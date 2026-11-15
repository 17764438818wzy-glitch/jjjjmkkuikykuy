"""Cooperative cancellation for this processing thread and its own helpers."""
from contextlib import contextmanager
import hashlib
from pathlib import Path
import subprocess
import threading
import time

_local = threading.local()


def current_stop():
    return getattr(_local, 'stop', None)


def record_failure(root, error, *, stage, run_id=None):
    """Keep failure type/location before joining; never persist arguments/locals."""
    import json
    import os
    import traceback
    try:
        folder=Path(root)/'private'/'processing-errors'
        folder.mkdir(parents=True,exist_ok=True)
        report=dict(stage=stage,run_id=run_id,exception_type=type(error).__name__,
            frames=[dict(file=frame.filename,line=frame.lineno,function=frame.name)
                    for frame in traceback.extract_tb(error.__traceback__)])
        from filelock import Timeout
        if isinstance(error,Timeout) or getattr(error,'lock_kind',None)=='global_stage_sql':
            report['lock']=dict(kind=getattr(error,'lock_kind','file_lock'),
                path=str(error.lock_file),wait_seconds=getattr(error,'lock_wait_seconds',None),
                location=getattr(error,'lock_location','see_frames'))
        for key in ('request_id','stop_reason'):
            value=getattr(error,key,None)
            if isinstance(value,str) and len(value)<=128:report[key]=value
        target=folder/f'{time.time_ns()}-{os.getpid()}-{threading.get_ident()}.json'
        with target.open('x',encoding='utf8') as stream:
            json.dump(report,stream,ensure_ascii=False,indent=2)
        return str(target)
    except Exception:
        # An unavailable disk must not replace the first processing failure.
        pass


def check_stop(stop=None):
    stop = stop if stop is not None else current_stop()
    if stop is not None and stop.is_set():
        raise InterruptedError('已停止，已有材料和结果保留')


def stop_after_failure(root,stop,*,run_id=None):
    """Keep cancellation/registration failures secondary to the real failure."""
    try:
        from preextract_delivery_control import LinkedStop
        if isinstance(stop,LinkedStop):stop.set(reason='batch_failure')
        else:stop.set()
    except Exception as secondary:
        record_failure(root,secondary,stage='processing_stop_registration',run_id=run_id)


@contextmanager
def stop_scope(stop):
    previous = current_stop()
    _local.stop = stop if stop is not None else previous
    try:
        yield
    finally:
        _local.stop = previous


def digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as stream:
        while True:
            check_stop()
            part = stream.read(1024 * 1024)
            if not part: break
            value.update(part)
    return value.hexdigest()


def run_process(command, *, timeout, stop=None, **kwargs):
    """Same result as subprocess.run; terminate only the Popen we created."""
    stop = stop if stop is not None else current_stop()
    if stop is None:
        return subprocess.run(command, timeout=timeout, **kwargs)
    check_stop(stop)
    capture = kwargs.pop('capture_output', False)
    if capture:
        kwargs['stdout'] = subprocess.PIPE
        kwargs['stderr'] = subprocess.PIPE
    deadline = time.monotonic() + timeout
    with subprocess.Popen(command, **kwargs) as process:
        try:
            while True:
                check_stop(stop)
                remaining = deadline - time.monotonic()
                if remaining <= 0: raise subprocess.TimeoutExpired(command, timeout)
                try:
                    stdout, stderr = process.communicate(timeout=min(.1, remaining))
                    check_stop(stop)
                    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
                except subprocess.TimeoutExpired:
                    if time.monotonic() >= deadline: raise
        except BaseException:
            if process.poll() is None: process.kill()
            process.communicate(timeout=5)
            raise


@contextmanager
def acquire_lock(lock, deadline):
    from filelock import Timeout
    if current_stop() is None:
        with lock:
            yield
        return
    while True:
        check_stop()
        remaining = deadline - time.monotonic()
        if remaining <= 0: raise Timeout(str(lock.lock_file))
        try:
            lock.acquire(timeout=min(.1, remaining))
            break
        except Timeout:
            continue
    try:
        check_stop()
        yield
    finally:
        lock.release()
