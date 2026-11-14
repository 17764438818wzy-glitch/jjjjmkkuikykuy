"""Existing LibreOffice CLI, isolated short paths and owned process cleanup."""
from pathlib import Path
import os
import shutil
import subprocess
import tempfile
import time


def executable():
    key = (os.getcwd(),) + tuple(os.environ.get(name) for name in (
        'LIBREOFFICE_PATH', 'DITY_LIBREOFFICE_PATH', 'PATH', 'PATHEXT',
        'NoDefaultCurrentDirectoryInExePath'))
    cached = getattr(executable, '_resolved_program', None)
    if (cached is not None and cached[0] == key and time.monotonic() < cached[1]
            and cached[2].is_file()):
        return cached[2]
    candidates = [os.environ.get('LIBREOFFICE_PATH'), os.environ.get('DITY_LIBREOFFICE_PATH'),
                  shutil.which('soffice.com'), r'C:\Program Files\LibreOffice\program\soffice.com',
                  r'C:\Program Files (x86)\LibreOffice\program\soffice.com']
    for value in candidates:
        if not value:
            continue
        path = Path(value)
        if path.name.lower() == 'soffice.exe' and path.with_suffix('.com').is_file():
            path = path.with_suffix('.com')
        if path.is_file():
            resolved = path.resolve()
            executable._resolved_program = (key, time.monotonic() + 5, resolved)
            return resolved
    return None


def _run(command, timeout, stop):
    """Assign before execution; closing this Job never targets another Office."""
    import psutil
    import win32job
    job = win32job.CreateJobObject(None, '')
    limits = win32job.QueryInformationJobObject(job, win32job.JobObjectExtendedLimitInformation)
    limits['BasicLimitInformation']['LimitFlags'] |= win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    win32job.SetInformationJobObject(job, win32job.JobObjectExtendedLimitInformation, limits)
    process = None
    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NO_WINDOW | 0x4)
        try:
            win32job.AssignProcessToJobObject(job, process._handle)
        except BaseException:
            process.kill();process.wait()
            raise
        psutil.Process(process.pid).resume()
        deadline = time.monotonic()+timeout
        while True:
            if stop is not None and stop.is_set():
                win32job.TerminateJobObject(job, 1)
                process.communicate(timeout=5)
                raise InterruptedError('已停止 LibreOffice 转换，原材料保留')
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                win32job.TerminateJobObject(job, 1)
                stdout, stderr = process.communicate(timeout=5)
                raise subprocess.TimeoutExpired(command, timeout, output=stdout, stderr=stderr)
            try:
                stdout, stderr = process.communicate(timeout=min(.1, remaining))
                return subprocess.CompletedProcess(command,process.returncode,stdout,stderr)
            except subprocess.TimeoutExpired:
                pass
    finally:
        job.Close()
        if process is not None and process.poll() is None:
            process.wait(timeout=5)


def convert(source, output_dir, *, target_format='pdf', timeout=120, stop=None):
    from office_wps import _valid_docx, _valid_pdf
    if target_format not in {'pdf','docx'}:
        raise ValueError('LibreOffice 转换格式无效')
    binary = executable()
    if binary is None:
        raise RuntimeError('未找到 LibreOffice 转换程序')
    source, output_dir = Path(source).resolve(), Path(output_dir).resolve()
    target = output_dir/(source.stem+'.'+target_format)
    if target == source:
        raise ValueError('转换输出不能覆盖原稿')
    output_dir.mkdir(parents=True,exist_ok=True)
    log = output_dir.parent/(output_dir.name+'_conversion.log')
    with tempfile.TemporaryDirectory(prefix='up_lo_') as name:
        folder = Path(name).resolve()
        local = folder/('source'+source.suffix.lower())
        shutil.copy2(source,local)
        command = [str(binary),'-env:UserInstallation='+(folder/'profile').as_uri(),
                   '--headless','--nologo','--norestore','--convert-to',target_format,
                   '--outdir',str(folder),str(local)]
        try:
            result = _run(command, timeout, stop)
        except subprocess.TimeoutExpired as exc:
            log.write_bytes(b'engine=LibreOffice\ntimeout\n'+(exc.stdout or b'')+(exc.stderr or b''))
            raise RuntimeError('LibreOffice 转换超时，原材料保留；查看转换日志') from exc
        except InterruptedError:
            log.write_text('engine=LibreOffice\nstopped\n',encoding='utf8')
            raise
        log.write_bytes(b'engine=LibreOffice\n'+result.stdout+b'\n'+result.stderr+
                        f'\nexit_code={result.returncode}'.encode())
        converted = folder/('source.'+target_format)
        valid = _valid_pdf if target_format == 'pdf' else _valid_docx
        if result.returncode or not valid(converted):
            raise RuntimeError('LibreOffice 未产生有效转换结果，原材料保留；查看转换日志')
        with tempfile.NamedTemporaryFile(dir=output_dir,prefix='.lo-',suffix='.tmp',delete=False) as stream:
            staged = Path(stream.name)
        try:
            shutil.copy2(converted,staged);staged.replace(target)
        finally:
            staged.unlink(missing_ok=True)
    return target
