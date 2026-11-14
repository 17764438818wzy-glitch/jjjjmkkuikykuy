"""Manually supervise the first-step API without touching Prefect or paper runs."""
from datetime import datetime
import json
import os
from pathlib import Path
import sys
import traceback

import psutil


ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / 'runtime'
CONFIG = RUNTIME / 'workspace-config.json'
PROCESS_STATE = RUNTIME / 'control-preview/processes.json'
LOG = RUNTIME / 'control-preview/api-only-supervisor.log'


def running_api():
    listeners = [connection for connection in psutil.net_connections(kind='tcp')
                 if connection.status == psutil.CONN_LISTEN and connection.laddr.ip == '127.0.0.1'
                 and connection.laddr.port == 14203]
    if not listeners:
        return None
    if len(listeners) != 1 or listeners[0].pid is None:
        raise RuntimeError('14203 监听归属无法确认')
    process = psutil.Process(listeners[0].pid)
    command = process.cmdline()
    if ('uvicorn' not in command or 'summary_api:app' not in command
            or Path(process.cwd()).resolve() != ROOT.resolve()
            or Path(process.environ().get('PAPERFLOW_CONFIG', '')).resolve() != CONFIG.resolve()):
        raise RuntimeError('14203 被其他进程占用')
    return process


def main():
    from filelock import FileLock, Timeout
    lock = FileLock(str(RUNTIME / 'control-preview/api-only-supervisor.lock'), timeout=0)
    try:
        lock.acquire()
    except Timeout:
        raise RuntimeError('第一步 API 单独管理程序已在运行') from None
    LOG.parent.mkdir(parents=True, exist_ok=True)
    try:
        with LOG.open('a', encoding='utf8', buffering=1) as log:
            sys.stdout = sys.stderr = log
            try:
                process = running_api()
                if process is not None:
                    print(f'{datetime.now().isoformat(timespec="seconds")} attached API pid={process.pid}', flush=True)
                    process.wait()
                    print(f'{datetime.now().isoformat(timespec="seconds")} API exited; no automatic restart', flush=True)
                    return 1
                settings = json.loads(CONFIG.read_text(encoding='utf8'))
                from managed_service_preflight import check_control_available, check_idle
                control_db = RUNTIME / 'workspace/output/group-task-summaries/summary-index.sqlite3'
                if settings.get('first_step_order_model') == 'v2':
                    check_control_available(control_db)
                    api = str(settings.get('prefect_api_url', '')).rstrip('/')
                    if not api or '127.0.0.1:4291' in api or 'localhost:4291' in api:
                        raise RuntimeError('统一 Prefect 地址未配置')
                else:
                    check_idle(control_db, RUNTIME / 'prefect-4291/prefect.db')
                queue = Path(settings.get('local_intake_queue_db', ''))
                if not queue.is_file():
                    raise RuntimeError('本地接收状态库未配置或不可用')
                state = json.loads(PROCESS_STATE.read_text(encoding='utf8'))
                port = state.get('tusd_port')
                if type(port) is not int or not 1 <= port <= 65535:
                    raise RuntimeError('上传服务端口不可用')
                os.environ.update(PAPERFLOW_CONFIG=str(CONFIG), PYTHONUTF8='1',
                    PAPERFLOW_TUS_PUBLIC_URL='http://127.0.0.1:14203/tus/files/',
                    PAPERFLOW_TUS_BACKEND_URL=f'http://127.0.0.1:{port}/files/',
                    PREFECT_API_URL=settings['prefect_api_url'])
                print(f'{datetime.now().isoformat(timespec="seconds")} starting API only', flush=True)
                import uvicorn
                uvicorn.run('summary_api:app', host='127.0.0.1', port=14203, access_log=False)
                print(f'{datetime.now().isoformat(timespec="seconds")} API exited; no automatic restart', flush=True)
                return 1
            except BaseException:
                traceback.print_exc(file=log)
                return 1
    finally:
        lock.release()


if __name__ == '__main__':
    raise SystemExit(main())
