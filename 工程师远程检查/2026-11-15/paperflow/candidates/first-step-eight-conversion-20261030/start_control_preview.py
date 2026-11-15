"""Start only the isolated API and tusd, without starting any paper or Prefect flow."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from urllib.request import Request, urlopen

from paperflow_prefect.settings import Settings


def port_available(port):
    with socket.socket() as sock:
        try:
            sock.bind(('127.0.0.1', port))
        except OSError:
            return False
        return True


def private_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def wait_url(url, method='GET'):
    for _ in range(100):
        try:
            with urlopen(Request(url, method=method), timeout=.3) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(.1)
    raise RuntimeError('候选服务启动失败，请检查本地日志')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=14203)
    args = parser.parse_args()
    port = args.port
    if not 1024 <= port <= 65535:
        raise RuntimeError('候选 API 端口无效')
    root = Path(__file__).resolve().parent
    settings = Settings.load()
    binary = root.parent / 'first-step-uppy-tusd-20261029' / 'tusd' / 'tusd.exe'
    if not binary.is_file():
        raise RuntimeError('未找到已有 tusd')
    if not port_available(port):
        raise RuntimeError(f'{port} 已被占用，未启动或修改现有服务')
    internal_port = private_port()
    runtime = root / 'runtime' / ('control-preview' if port == 14203 else f'control-preview-{port}')
    runtime.mkdir(parents=True, exist_ok=True)
    logs = [(runtime / name).open('ab', buffering=0) for name in ('api.log', 'tusd.log')]
    env = dict(os.environ, PAPERFLOW_TUS_PUBLIC_URL=f'http://127.0.0.1:{port}/tus/files/',
               PAPERFLOW_TUS_BACKEND_URL=f'http://127.0.0.1:{internal_port}/files/', PYTHONUTF8='1')
    flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    children = []
    try:
        children.append(subprocess.Popen([sys.executable, '-m', 'uvicorn', 'summary_api:app',
            '--host', '127.0.0.1', '--port', str(port), '--no-access-log'], cwd=root, env=env,
            stdin=subprocess.DEVNULL, stdout=logs[0], stderr=subprocess.STDOUT, creationflags=flags))
        wait_url(f'http://127.0.0.1:{port}/health')
        children.append(subprocess.Popen([str(binary), '-host', '127.0.0.1', '-port', str(internal_port),
            '-upload-dir', str(settings.path('data_root') / 'tusd'), '-hooks-http',
            f'http://127.0.0.1:{port}/api/v1/first-step/internal/tusd-hooks',
            '-hooks-enabled-events', 'pre-create,pre-finish', '-disable-download', '-disable-termination',
            '-disable-cors'], cwd=root, stdin=subprocess.DEVNULL, stdout=logs[1], stderr=subprocess.STDOUT,
            creationflags=flags))
        wait_url(f'http://127.0.0.1:{port}/tus/files/', 'OPTIONS')
        (runtime / 'processes.json').write_text(json.dumps({'api_pid': children[0].pid,
            'tusd_pid': children[1].pid, 'tusd_port': internal_port}, indent=2), encoding='utf8')
        print(f'candidate API ready: http://127.0.0.1:{port}')
        print(f'candidate tus endpoint: http://127.0.0.1:{port}/tus/files/')
    except BaseException:
        for child in children:
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=5)
        raise
    finally:
        for log in logs:
            log.close()


if __name__ == '__main__':
    main()
