"""First-step timing adapters for the existing owned Office converters."""
from functools import lru_cache
from pathlib import Path
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

from .first_step_conversion import (ConverterAttemptFailed, ConverterProcessUncertain,
                                    _check_stop, _wait_lock)

_state = threading.local()


def _remember_uncertain(message, processes=None):
    _state.uncertain = ConverterProcessUncertain(message, processes)
    return _state.uncertain


def _wait_job_empty(job, deadline):
    import win32job
    while True:
        try:
            active = win32job.QueryInformationJobObject(
                job, win32job.JobObjectBasicAccountingInformation)['ActiveProcesses']
        except Exception as exc:
            raise ConverterProcessUncertain('无法确认 LibreOffice 转换进程退出') from exc
        if active == 0:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(.05)


def _terminate_job(job, process):
    import win32job
    try:
        win32job.TerminateJobObject(job, 1)
        stdout, stderr = process.communicate(timeout=5)
        if not _wait_job_empty(job, time.monotonic() + 5):
            raise ConverterProcessUncertain('LibreOffice 转换进程尚未退出')
        return stdout, stderr
    except ConverterProcessUncertain:
        raise
    except Exception as exc:
        raise ConverterProcessUncertain('无法确认 LibreOffice 转换进程退出') from exc


def _run_libre_once(command, timeout, stop):
    """Use the native owned Job, retaining the slot until all its processes exit."""
    import psutil
    import win32job
    job = win32job.CreateJobObject(None, '')
    limits = win32job.QueryInformationJobObject(job, win32job.JobObjectExtendedLimitInformation)
    limits['BasicLimitInformation']['LimitFlags'] |= win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    win32job.SetInformationJobObject(job, win32job.JobObjectExtendedLimitInformation, limits)
    process, assigned, clean = None, False, False
    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   creationflags=subprocess.CREATE_NO_WINDOW | 0x4)
        try:
            win32job.AssignProcessToJobObject(job, process._handle)
            assigned = True
        except BaseException:
            process.kill()
            process.wait(timeout=5)
            clean = True
            raise
        psutil.Process(process.pid).resume()
        deadline = time.monotonic() + timeout
        while True:
            if stop is not None and stop.is_set():
                _terminate_job(job, process)
                clean = True
                raise InterruptedError('已停止 LibreOffice 转换，原材料保留')
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                stdout, stderr = _terminate_job(job, process)
                clean = True
                raise subprocess.TimeoutExpired(command, timeout, output=stdout, stderr=stderr)
            try:
                stdout, stderr = process.communicate(timeout=min(.1, remaining))
            except subprocess.TimeoutExpired:
                continue
            if not _wait_job_empty(job, deadline):
                stdout, stderr = _terminate_job(job, process)
                clean = True
                raise subprocess.TimeoutExpired(command, timeout, output=stdout, stderr=stderr)
            clean = True
            return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
    finally:
        try:
            if process is not None and assigned and not clean:
                _terminate_job(job, process)
        finally:
            job.Close()


def _run_libre(command, timeout, stop):
    try:
        return _run_libre_once(command, timeout, stop)
    except ConverterProcessUncertain as exc:
        _state.uncertain = exc
        raise


@lru_cache(maxsize=1)
def _libre_adapter():
    # Load the manifest-verified native module separately; do not replace its
    # runner for any other workflow that may use the original module.
    import office_libreoffice
    spec = importlib.util.spec_from_file_location('_paperflow_first_step_libre', office_libreoffice.__file__)
    adapter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(adapter)
    adapter._run = _run_libre
    return adapter


def convert_libre(source, output, *, target_format, timeout):
    _state.uncertain = None
    try:
        return _libre_adapter().convert(source, output, target_format=target_format, timeout=timeout)
    except ConverterProcessUncertain:
        raise
    except InterruptedError:
        raise
    except RuntimeError as exc:
        if _state.uncertain:
            raise _state.uncertain from exc
        raise ConverterAttemptFailed(str(exc)) from exc
    except Exception as exc:
        if _state.uncertain:
            raise _state.uncertain from exc
        raise
    finally:
        _state.uncertain = None


def _confirm_wps_exit(office_wps, state):
    if not state.is_file():
        raise _remember_uncertain('WPS 进程归属尚未确认，通道已保留')
    try:
        office_wps._wait_for_owned_exit(state)
    except Exception as exc:
        processes = []
        try:
            for item in json.loads(state.read_text(encoding='utf8')).get('processes', []):
                if isinstance(item.get('pid'), int) and isinstance(item.get('created_at'), (int, float)):
                    processes.append({'pid': item['pid'], 'created_at': item['created_at']})
        except (OSError, ValueError, TypeError, AttributeError):
            pass
        raise _remember_uncertain('WPS 转换进程尚未确认退出，通道已保留', processes) from exc


def _convert_wps(source, output, *, target_format, timeout, stop_requested=None):
    """Keep the native WPS worker, but start its deadline after the global lock."""
    import office_wps
    from processing_stop import check_stop, run_process
    source, output = Path(source).resolve(), Path(output).resolve()
    target = output / (source.stem + '.' + target_format)
    if source == target:
        raise ValueError('转换输出不能覆盖原稿')
    output.mkdir(parents=True, exist_ok=True)
    log = output.parent / (output.name + '_conversion.log')
    lock_path = office_wps._lock_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with _wait_lock(lock_path, stop_requested):
        with tempfile.TemporaryDirectory(prefix='up_wps_') as directory:
            scratch = Path(directory).resolve()
            local = scratch / ('source' + source.suffix.lower())
            shutil.copy2(source, local)
            converted, state = scratch / ('converted.' + target_format), scratch / 'worker.json'
            command = [sys.executable, '-X', 'utf8', '-B', str(Path(office_wps.__file__).resolve()),
                       '--worker', str(local), str(converted), str(state)]
            _check_stop(stop_requested)
            try:
                result = run_process(command, capture_output=True, timeout=timeout,
                                     creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            except BaseException as exc:
                _confirm_wps_exit(office_wps, state)
                if isinstance(exc, subprocess.TimeoutExpired):
                    log.write_bytes(b'engine=WPS\ntimeout\n' + (exc.stdout or b'') + (exc.stderr or b''))
                    raise ConverterAttemptFailed('WPS 转换超时') from exc
                raise
            _confirm_wps_exit(office_wps, state)
            log.write_bytes(b'engine=WPS\n' + result.stdout + b'\n' + result.stderr +
                            (state.read_bytes() if state.is_file() else b'') +
                            f'\nexit_code={result.returncode}'.encode())
            valid = office_wps._valid_pdf if target_format == 'pdf' else office_wps._valid_docx
            if result.returncode or not valid(converted):
                raise ConverterAttemptFailed('WPS 未产生有效转换结果')
            check_stop()
            with tempfile.NamedTemporaryFile(dir=output, prefix='.wps-', suffix='.tmp', delete=False) as stream:
                temporary = Path(stream.name)
            try:
                shutil.copy2(converted, temporary)
                temporary.replace(target)
            finally:
                temporary.unlink(missing_ok=True)
    return target


def convert_wps(source, output, *, target_format, timeout, stop_requested=None):
    _state.uncertain = None
    try:
        return _convert_wps(source, output, target_format=target_format, timeout=timeout,
                            stop_requested=stop_requested)
    except ConverterProcessUncertain:
        raise
    except Exception as exc:
        if _state.uncertain:
            raise _state.uncertain from exc
        raise
    finally:
        _state.uncertain = None
