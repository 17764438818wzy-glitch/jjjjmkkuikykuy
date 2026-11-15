"""Host the independent first-step services without starting any paper.

The task has no trigger. The host retains process handles and records the
exit status, and deliberately never retries or resumes a paper run.
"""
from datetime import datetime
import json
import os
from pathlib import Path
import socket
import subprocess
import time

import httpx
import psutil

from managed_service_preflight import check_idle
from paperflow_prefect.records import write_json


def wait_url(client, url, process, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f'service exited during startup: {process.returncode}')
        try:
            if client.get(url).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(.15)
    raise RuntimeError('service startup timed out: ' + url)


def stop_created(process):
    try:
        parent = psutil.Process(process.pid)
        tree = [parent, *parent.children(recursive=True)]
        for item in reversed(tree):
            try:
                item.terminate()
            except psutil.NoSuchProcess:
                pass
        psutil.wait_procs(tree, timeout=5)
    except psutil.NoSuchProcess:
        pass


def main():
    root = Path(__file__).resolve().parent
    runtime = root / 'runtime'
    pyw = root.parents[1] / 'prefect-first-step-independent' / '.venv' / 'Scripts' / 'pythonw.exe'
    control_db = runtime / 'workspace/output/group-task-summaries/summary-index.sqlite3'
    prefect_db = runtime / 'prefect-4291/prefect.db'
    api_state_path = runtime / 'control-preview/processes.json'
    prefect_state_path = runtime / 'prefect-4291/processes.json'
    log_path = runtime / 'managed-service-host.log'
    api_log = (runtime / 'control-preview/api.log').open('ab', buffering=0)
    server_log = (runtime / 'prefect-4291/server.log').open('ab', buffering=0)
    runner_log = (runtime / 'prefect-4291/runner.log').open('ab', buffering=0)
    tus_log = (runtime / 'control-preview/tusd.log').open('ab', buffering=0)
    processes = {}
    with log_path.open('a', encoding='utf8', buffering=1) as log:
        def record(message):
            log.write(f'{datetime.now().isoformat(timespec="seconds")} {message}\n')

        try:
            check_idle(control_db, prefect_db)
            settings = json.loads((runtime / 'workspace-config.json').read_text(encoding='utf8'))
            v2 = settings.get('first_step_order_model') == 'v2'
            for port in ((14203,) if v2 else (4291, 14203)):
                with socket.socket() as sock:
                    sock.bind(('127.0.0.1', port))
            state = json.loads(api_state_path.read_text(encoding='utf8'))
            binary = (root.parent / 'first-step-uppy-tusd-20261029/tusd/tusd.exe').resolve()
            try:
                tus = psutil.Process(state['tusd_pid'])
                args = tus.cmdline()
                valid_tus = (Path(tus.exe()).resolve() == binary and args[args.index('-port') + 1] == str(state['tusd_port'])
                    and Path(args[args.index('-upload-dir') + 1]).resolve() == (runtime / 'workspace/data/tusd').resolve()
                    and args[args.index('-hooks-http') + 1] ==
                    'http://127.0.0.1:14203/api/v1/first-step/internal/tusd-hooks')
            except (psutil.Error, ValueError, IndexError):
                valid_tus = False
            client = httpx.Client(timeout=2, trust_env=False)
            if not valid_tus:
                with socket.socket() as sock:
                    sock.bind(('127.0.0.1', 0))
                    state['tusd_port'] = sock.getsockname()[1]
                tus = subprocess.Popen([str(binary), '-host', '127.0.0.1', '-port', str(state['tusd_port']),
                    '-upload-dir', str(runtime / 'workspace/data/tusd'), '-hooks-http',
                    'http://127.0.0.1:14203/api/v1/first-step/internal/tusd-hooks', '-hooks-enabled-events',
                    'pre-create,pre-finish', '-disable-download', '-disable-termination', '-disable-cors'],
                    cwd=root, stdin=subprocess.DEVNULL, stdout=tus_log, stderr=subprocess.STDOUT,
                    creationflags=subprocess.CREATE_NO_WINDOW)
                processes['upload_service'] = tus
                state['tusd_pid'] = tus.pid
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    if tus.poll() is not None:
                        raise RuntimeError('first-step upload service exited')
                    try:
                        if client.options(f"http://127.0.0.1:{state['tusd_port']}/files/").status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    time.sleep(.1)
                else:
                    raise RuntimeError('first-step upload service did not start')
            if client.options(f"http://127.0.0.1:{state['tusd_port']}/files/").status_code != 200:
                raise RuntimeError('first-step upload service is unavailable')
            prefect_url = settings['prefect_api_url'].rstrip('/') if v2 else 'http://127.0.0.1:4291/api'
            env = dict(os.environ, PAPERFLOW_CONFIG=str(runtime / 'workspace-config.json'), PYTHONUTF8='1',
                PREFECT_HOME=str(runtime / 'prefect-4291/home'),
                PREFECT_API_URL=prefect_url,
                PREFECT_SERVER_DATABASE_CONNECTION_URL='sqlite+aiosqlite:///' + prefect_db.as_posix(),
                PREFECT_SERVER_ANALYTICS_ENABLED='false',
                PAPERFLOW_TUS_PUBLIC_URL='http://127.0.0.1:14203/tus/files/',
                PAPERFLOW_TUS_BACKEND_URL=f"http://127.0.0.1:{state['tusd_port']}/files/")
            flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW

            def start(name, command, output):
                process = subprocess.Popen([str(pyw), '-X', 'utf8', *command], cwd=root, env=env,
                    stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT, creationflags=flags)
                processes[name] = process
                record(f'{name} started pid={process.pid}')
                return process

            if v2:
                response = client.get(prefect_url + '/health')
                if response.status_code != 200:
                    raise RuntimeError('统一 Prefect 服务不可用')
                intake = root.parent / 'local-intake-live-link-20261031'
                subprocess.run([str(pyw), '-X', 'utf8', str(intake / 'register_waiting_deployment.py'),
                                '--prefect-api', prefect_url], cwd=intake, env=env,
                               stdin=subprocess.DEVNULL, stdout=runner_log, stderr=subprocess.STDOUT,
                               creationflags=flags, check=True, timeout=120)
                server = None
            else:
                server = start('prefect_server', ['-m', 'prefect', 'server', 'start', '--host', '127.0.0.1',
                               '--port', '4291', '--analytics-off', '--no-scheduler'], server_log)
                wait_url(client, 'http://127.0.0.1:4291/api/health', server, timeout=60)
            runner = start('prefect_runner', ['serve_controlled.py'], runner_log)
            api = start('classification_api', ['-m', 'uvicorn', 'summary_api:app', '--host', '127.0.0.1',
                        '--port', '14203', '--no-access-log'], api_log)
            wait_url(client, 'http://127.0.0.1:14203/health', api)
            state['api_pid'] = api.pid
            write_json(api_state_path, state)
            if server is not None:
                write_json(prefect_state_path, {'server_pid': server.pid, 'runner_pid': runner.pid,
                                               'api_url': prefect_url})
            record('Services ready; no paper run was started.')
            reported = set()
            while True:
                for name, process in processes.items():
                    code = process.poll()
                    if code is not None and name not in reported:
                        record(f'{name} exited code={code} hex=0x{code & 0xffffffff:08x}; no restart attempted')
                        reported.add(name)
                time.sleep(2)
        except Exception as exc:
            record(f'Host startup failed: {type(exc).__name__}: {exc}')
            for process in processes.values():
                stop_created(process)
            raise
        finally:
            for file in (api_log, server_log, runner_log, tus_log):
                file.close()


if __name__ == '__main__':
    main()
