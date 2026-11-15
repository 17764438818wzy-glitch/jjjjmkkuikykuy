"""Start and verify the first-step local Nimble service without sending inference."""

from datetime import datetime
import filecmp
import http.client
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
from urllib.parse import urlsplit

from filelock import FileLock
import psutil


MODEL = 'nimble:9b-q4_K_M'
OLLAMA_EXE = Path(r'F:\nimble\runtime\ollama.exe')


class NimbleUnavailable(RuntimeError):
    """The first-step service or its configured model cannot be verified."""


def _listener_pid(port):
    listeners = {row.pid for row in psutil.net_connections(kind='tcp')
                 if row.status == psutil.CONN_LISTEN and row.laddr and row.laddr.port == port}
    if not listeners:
        return None
    if len(listeners) != 1 or None in listeners:
        raise NimbleUnavailable('Nimble 端口占用者无法确认')
    return listeners.pop()


def _verify_listener(pid, manager_root):
    try:
        record = json.loads((Path(manager_root) / 'runtime' / 'nimble-process.json').read_text(encoding='utf-8-sig'))
        started = datetime.fromisoformat(record['started_at']).timestamp()
        process = psutil.Process(pid)
        if (record['pid'] != pid or record['endpoint'] != 'http://127.0.0.1:11435'
                or os.path.normcase(str(Path(process.exe()).resolve())) != os.path.normcase(str(OLLAMA_EXE.resolve()))
                or abs(process.create_time() - started) > 30):
            raise NimbleUnavailable('Nimble 端口占用者与服务记录不一致')
    except (OSError, ValueError, KeyError, TypeError, psutil.Error) as exc:
        raise NimbleUnavailable('Nimble 端口占用者无法核验') from exc


def _get_json(host, port, path):
    conn = http.client.HTTPConnection(host, port, timeout=3)
    try:
        conn.request('GET', path)
        response = conn.getresponse()
        raw = response.read(1024 * 1024 + 1)
        if response.status != 200 or len(raw) > 1024 * 1024:
            raise NimbleUnavailable('Nimble 服务状态接口不可用')
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise NimbleUnavailable('Nimble 服务状态格式无效')
        return value
    except (OSError, ValueError, http.client.HTTPException) as exc:
        raise NimbleUnavailable('Nimble 服务状态不可核验') from exc
    finally:
        conn.close()


def probe_service(host, port, model=MODEL):
    version = _get_json(host, port, '/api/version')
    if not isinstance(version.get('version'), str) or not version['version']:
        raise NimbleUnavailable('Nimble 服务版本不可核验')
    tags = _get_json(host, port, '/api/tags')
    models = tags.get('models')
    if not isinstance(models, list) or not any(
            isinstance(item, dict) and str(item.get('name', item.get('model', ''))).casefold() == model.casefold()
            for item in models):
        raise NimbleUnavailable('第一步所需 Nimble 模型未注册')


def _archive_previous_logs(runtime):
    sources = [runtime / name for name in ('nimble.stdout.log', 'nimble.stderr.log', 'nimble-process.json')]
    sources = [path for path in sources if path.is_file()]
    if not sources:
        return
    destination = runtime / 'nimble-log-backups' / str(time.time_ns())
    destination.mkdir(parents=True, exist_ok=False)
    for source in sources:
        target = destination / source.name
        shutil.copy2(source, target)
        if not filecmp.cmp(source, target, shallow=False):
            raise NimbleUnavailable('原 Nimble 日志未能完整保存')


def _run_start_script(script):
    subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
                    '-File', str(script)], check=True, timeout=75, stdin=subprocess.DEVNULL,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                   creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))


def ensure_nimble(endpoint, manager_root, start_script, *, is_needed=None):
    """Ensure the dedicated local instance exists and reports the configured model."""
    try:
        url = urlsplit(endpoint)
        if (url.scheme != 'http' or url.hostname not in {'127.0.0.1', 'localhost'} or url.port != 11435
                or url.path != '/v1/systemone' or url.username or url.password or url.query or url.fragment):
            raise NimbleUnavailable('第一步 Nimble 地址与专用服务不一致')
        root, script = Path(manager_root).resolve(), Path(start_script).resolve()
        if script != root / 'start-nimble.ps1' or not script.is_file():
            raise NimbleUnavailable('第一步 Nimble 启动入口不可用')
        runtime = root / 'runtime'
        if not runtime.is_dir():
            raise NimbleUnavailable('第一步 Nimble 运行目录不可用')
        with FileLock(str(runtime / 'nimble-start.lock'), timeout=90):
            if is_needed is not None and not is_needed():
                return
            pid = _listener_pid(11435)
            if pid is None:
                _archive_previous_logs(runtime)
                if is_needed is not None and not is_needed():
                    return
                _run_start_script(script)
                pid = _listener_pid(11435)
                if pid is None:
                    raise NimbleUnavailable('Nimble 启动后未监听')
            _verify_listener(pid, root)
            probe_service('127.0.0.1', 11435, MODEL)
    except NimbleUnavailable:
        raise
    except Exception as exc:
        raise NimbleUnavailable('第一步 Nimble 服务未就绪') from exc
