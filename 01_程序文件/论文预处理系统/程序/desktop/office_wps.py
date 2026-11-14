"""WPS adapter for engine.convert_office; only private copies enter automation."""
from pathlib import Path
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile


def _registration():
    if os.name != 'nt':
        return None
    import winreg
    for view, activation in [(winreg.KEY_WOW64_32KEY, 0x40000),
                             (winreg.KEY_WOW64_64KEY, 0x80000)]:
        try:
            with winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, r'KWPS.Application\CLSID',
                                0, winreg.KEY_READ | view) as key:
                clsid = winreg.QueryValueEx(key, '')[0]
            with winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, f'CLSID\\{clsid}\\LocalServer32',
                                0, winreg.KEY_READ | view) as key:
                command = winreg.QueryValueEx(key, '')[0]
            match = re.match(r'^"?(.+?\.exe)"?(?:\s|$)', command, re.I)
            executable = Path(os.path.expandvars(match[1])) if match else None
            if executable and executable.name.lower() == 'wps.exe' and executable.is_file():
                return executable.resolve(), activation
        except OSError:
            continue
    return None


def available():
    return _registration() is not None


def _valid_docx(path):
    try:
        with zipfile.ZipFile(path) as archive:
            return {'[Content_Types].xml', 'word/document.xml'} <= set(archive.namelist()) and archive.testzip() is None
    except (OSError, zipfile.BadZipFile):
        return False


def _lock_path():
    # Other original callers assign a different TEMP/TMP to every paper.
    from project_paths import DATA_DIR
    return DATA_DIR / 'private/locks/wps-conversion.lock'


def _valid_pdf(path):
    import fitz
    try:
        with fitz.open(path) as document:
            return not document.needs_pass and len(document) > 0
    except Exception:
        return False


def _wait_for_owned_exit(state):
    """Job termination is asynchronous; keep the WPS lock until it settles."""
    import psutil
    if not state.is_file():
        return
    processes = []
    for identity in json.loads(state.read_text(encoding='utf8')).get('processes', []):
        try:
            process = psutil.Process(identity['pid'])
            if process.create_time() == identity['created_at']:
                processes.append(process)
        except psutil.NoSuchProcess:
            pass
    _, alive = psutil.wait_procs(processes, timeout=3)
    if alive:
        raise RuntimeError('WPS 转换进程尚未确认退出，原文件已保留；查看转换日志。')


def convert(source, output_dir, *, timeout=120, target_format='docx'):
    """Serialize WPS only, with one total deadline and atomic publication."""
    from filelock import FileLock
    if target_format not in {'docx', 'pdf'}:
        raise ValueError('WPS 转换只支持 DOCX 或 PDF 输出。')
    deadline = time.monotonic() + timeout
    source, output_dir = Path(source).resolve(), Path(output_dir).resolve()
    target = output_dir / (source.stem + '.' + target_format)
    if target == source:
        raise ValueError('转换输出不能覆盖原稿。')
    output_dir.mkdir(parents=True, exist_ok=True)
    log = output_dir.parent / (output_dir.name + '_conversion.log')
    # Shared by desktop processes, not by unrelated WPS documents. Native DOCX
    # and PDF preparation bypass this lock and keep their existing concurrency.
    lock_path = _lock_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(str(lock_path), timeout=max(0, deadline - time.monotonic())):
        with tempfile.TemporaryDirectory(prefix='up_wps_') as directory:
            scratch = Path(directory).resolve()
            if scratch.parent != Path(tempfile.gettempdir()).resolve() or not scratch.name.startswith('up_wps_'):
                raise RuntimeError('临时转换目录校验失败。')
            local = scratch / ('source' + source.suffix.lower())
            shutil.copy2(source, local)
            converted, state = scratch / ('converted.' + target_format), scratch / 'worker.json'
            command = [sys.executable, '-X', 'utf8', '-B', str(Path(__file__).resolve()),
                       '--worker', str(local), str(converted), str(state)]
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(command, timeout)
                result = subprocess.run(command, capture_output=True, timeout=remaining,
                                        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            except subprocess.TimeoutExpired as exc:
                log.write_bytes(b'engine=WPS\ntimeout\n' + (exc.stdout or b'') + (exc.stderr or b'')
                                + (state.read_bytes() if state.is_file() else b''))
                _wait_for_owned_exit(state)
                raise RuntimeError('WPS 转换超时，原文件已保留；查看转换日志。') from exc
            log.write_bytes(b'engine=WPS\n' + result.stdout + b'\n' + result.stderr
                            + (state.read_bytes() if state.is_file() else b'')
                            + f'\nexit_code={result.returncode}'.encode())
            _wait_for_owned_exit(state)
            valid = _valid_pdf if target_format == 'pdf' else _valid_docx
            if result.returncode or not valid(converted):
                raise RuntimeError(f'WPS 转换未产生有效 {target_format.upper()}，原文件已保留；查看转换日志。')
            # A failed attempt must not reuse or truncate an older output.
            with tempfile.NamedTemporaryFile(dir=output_dir, prefix='.wps-', suffix='.tmp', delete=False) as stream:
                temporary = Path(stream.name)
            try:
                shutil.copy2(converted, temporary)
                temporary.replace(target)
            finally:
                temporary.unlink(missing_ok=True)
    return target


def _bind_job(window, executable, previous, started):
    """Bind the process owning our hidden document, never a name-based kill."""
    import psutil
    import win32api
    import win32job
    import win32process
    pid = win32process.GetWindowThreadProcessId(int(window.Hwnd))[1]
    process = psutil.Process(pid)
    owners = []
    # WPS may have a newly created automation broker above its rendering process.
    while process and process.name().lower() == 'wps.exe':
        identity = (process.pid, process.create_time())
        if (identity in previous or identity[1] < started - 1
                or Path(process.exe()).resolve() != executable
                or '/automation' not in [arg.lower() for arg in process.cmdline()]):
            if not owners:
                raise RuntimeError('WPS 未建立独立转换进程，已保留现有文档。')
            break
        owners.append(process)
        process = process.parent()
    if not owners:
        raise RuntimeError('无法确认 WPS 转换进程归属。')
    job = win32job.CreateJobObject(None, '')
    try:
        limits = win32job.QueryInformationJobObject(job, win32job.JobObjectExtendedLimitInformation)
        limits['BasicLimitInformation']['LimitFlags'] |= win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        win32job.SetInformationJobObject(job, win32job.JobObjectExtendedLimitInformation, limits)
        for owner in owners:
            handle = win32api.OpenProcess(0x0100 | 0x0001 | 0x0400, False, owner.pid)
            try:
                # Recheck creation identity on the same handle assigned to the Job.
                created = win32process.GetProcessTimes(handle)['CreationTime'].timestamp()
                if abs(created - owner.create_time()) > 0.001:
                    raise RuntimeError('WPS 转换进程身份发生变化。')
                win32job.AssignProcessToJobObject(job, handle)
            finally:
                handle.Close()
        return job, [dict(pid=p.pid, created_at=p.create_time()) for p in owners]
    except BaseException:
        job.Close()
        raise


def _worker(source, target, state):
    import psutil
    import pythoncom
    import win32com.client
    registration = _registration()
    if registration is None:
        raise RuntimeError('未找到已注册的 WPS。')
    executable, activation = registration
    previous = set()
    for process in psutil.process_iter(['pid', 'create_time', 'name']):
        if (process.info['name'] or '').lower() == 'wps.exe':
            previous.add((process.pid, process.info['create_time']))
    started = time.time()
    app = document = seed = job = None
    pythoncom.CoInitialize()
    try:
        app = win32com.client.DispatchEx('KWPS.Application', clsctx=pythoncom.CLSCTX_LOCAL_SERVER | activation)
        if app.Visible or app.Documents.Count:
            raise RuntimeError('WPS 返回了已有文档的实例，已停止转换以保留当前文档。')
        seed = app.Documents.Add(pythoncom.Missing, False, pythoncom.Missing, False)
        job, owners = _bind_job(seed.Windows(1), executable, previous, started)
        state.write_text(json.dumps(dict(engine='WPS', phase='owned', processes=owners)), encoding='utf8')
        app.AutomationSecurity = 3  # Disable macros in this owned automation instance.
        app.DisplayAlerts = 0
        if source.suffix.lower() in {'.wmf', '.emf'}:
            document, seed = seed, None
            picture = document.InlineShapes.AddPicture(str(source), False, True)
            width, height = float(picture.Width), float(picture.Height)
            scale = min(1.0, 1500 / max(width, height, 1))
            picture.LockAspectRatio = -1
            picture.Width = max(1, width * scale)
            for margin in ('TopMargin', 'BottomMargin', 'LeftMargin', 'RightMargin'):
                setattr(document.PageSetup, margin, 0)
            document.PageSetup.PageWidth = max(72, width * scale + 2)
            document.PageSetup.PageHeight = max(72, height * scale + 24)
            document.Content.ParagraphFormat.SpaceBefore = 0
            document.Content.ParagraphFormat.SpaceAfter = 0
        else:
            seed.Close(0)
            seed = None
            document = app.Documents.Open(str(source), False, True, False, '', '', '', '', '',
                                          pythoncom.Missing, pythoncom.Missing, False, False,
                                          pythoncom.Missing, True)
        document.SaveAs(str(target), 17 if target.suffix == '.pdf' else 12, False, '', False)
        document.Close(0)
        document = None
        state.write_text(json.dumps(dict(engine='WPS', phase='converted', processes=owners)), encoding='utf8')
    finally:
        # Never close a document or application acquired from the user's session.
        for opened in (document, seed):
            if opened is not None:
                try:
                    opened.Close(0)
                except Exception:
                    pass
        if job is not None:
            try:
                if app.Documents.Count == 0 and not app.Visible:
                    app.Quit(0)
            except Exception:
                pass
            job.Close()
        document = seed = app = None
        pythoncom.CoUninitialize()


if __name__ == '__main__':
    if len(sys.argv) != 5 or sys.argv[1] != '--worker':
        raise SystemExit('office_wps is called by engine.convert_office')
    _worker(*(Path(value) for value in sys.argv[2:]))
