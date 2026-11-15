"""Start a separate first-step Prefect and its worker, with no automatic input."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from urllib.parse import urlsplit
from urllib.request import urlopen

from paperflow_prefect.settings import Settings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=4291)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    settings = Settings.load()
    settings.require_local_classification()
    url = urlsplit(settings['prefect_api_url'])
    if url.hostname != '127.0.0.1' or url.port != args.port or url.path.rstrip('/') != '/api':
        raise RuntimeError('配置中的 Prefect 地址与独立服务不一致')
    with socket.socket() as sock:
        try:
            sock.bind(('127.0.0.1', args.port))
        except OSError:
            raise RuntimeError('端口被占用，未触碰既有服务') from None
    runtime = root / 'runtime' / f'prefect-{args.port}'
    runtime.mkdir(parents=True, exist_ok=True)
    db = (runtime / 'prefect.db').as_posix()
    env = dict(os.environ, PREFECT_HOME=str(runtime / 'home'),
        PREFECT_API_URL=f'http://127.0.0.1:{args.port}/api',
        PREFECT_SERVER_DATABASE_CONNECTION_URL='sqlite+aiosqlite:///' + db,
        PREFECT_SERVER_ANALYTICS_ENABLED='false', PYTHONUTF8='1')
    flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    logs = [(runtime / name).open('ab', buffering=0) for name in ('server.log', 'runner.log')]
    children = []
    try:
        children.append(subprocess.Popen([sys.executable, '-X', 'utf8', '-m', 'prefect', 'server', 'start',
            '--host', '127.0.0.1', '--port', str(args.port), '--analytics-off', '--no-scheduler'],
            cwd=root, env=env, stdin=subprocess.DEVNULL, stdout=logs[0], stderr=subprocess.STDOUT,
            creationflags=flags))
        for _ in range(300):
            if children[0].poll() is not None:
                raise RuntimeError('独立 Prefect 退出，请查看候选日志')
            try:
                with urlopen(f'http://127.0.0.1:{args.port}/api/health', timeout=.5) as response:
                    if response.status == 200:
                        break
            except OSError:
                time.sleep(.1)
        else:
            raise RuntimeError('独立 Prefect 未就绪，请查看候选日志')
        children.append(subprocess.Popen([sys.executable, '-X', 'utf8', 'serve_controlled.py'],
            cwd=root, env=env, stdin=subprocess.DEVNULL, stdout=logs[1], stderr=subprocess.STDOUT,
            creationflags=flags))
        (runtime / 'processes.json').write_text(json.dumps({'server_pid': children[0].pid,
            'runner_pid': children[1].pid, 'api_url': env['PREFECT_API_URL']}, indent=2), encoding='utf8')
        print(f'isolated Prefect server ready: http://127.0.0.1:{args.port}')
        print('runner started; deployment readiness must be checked separately')
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
