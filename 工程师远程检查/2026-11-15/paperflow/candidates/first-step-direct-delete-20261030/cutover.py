"""Install the verified first-step candidate while the independent workspace is idle."""
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time
from urllib.parse import quote
from urllib.request import ProxyHandler, build_opener

import psutil

HERE = Path(__file__).resolve().parent
BASE = HERE.parent
LIVE = BASE / 'first-step-control-20261029'
FRONT = BASE / 'first-step-preview-8795'
NEW = HERE / 'backend'
PYW = BASE.parent / 'prefect-first-step-independent' / '.venv' / 'Scripts' / 'pythonw.exe'
RUNTIME = LIVE / 'runtime'
sys.path.insert(0, str(LIVE))
from managed_service_preflight import check_idle

MODULES = ('attachment_evidence_handoff.py', 'controlled_first_step.py', 'first_step_control.py',
           'first_step_control_api.py', 'first_step_delete.py', 'group_classification.py',
           'group_classification_view.py', 'group_sort_cli.py', 'group_summary_store.py', 'group_task_summary.py')
FILES = [(NEW / 'paperflow_prefect' / name, LIVE / 'paperflow_prefect' / name, Path('backend/paperflow_prefect') / name)
         for name in MODULES]
FILES += [(NEW / 'summary_api.py', LIVE / 'summary_api.py', Path('backend/summary_api.py'))]
FILES += [(HERE / 'frontend' / name, FRONT / name, Path('frontend') / name) for name in ('app.js', 'styles.css', 'index.html')]
control_db = RUNTIME / 'workspace/output/group-task-summaries/summary-index.sqlite3'
prefect_db = RUNTIME / 'prefect-4291/prefect.db'
api_state_path = RUNTIME / 'control-preview/processes.json'
state = json.loads(api_state_path.read_text(encoding='utf8'))
prefect_state = json.loads((RUNTIME / 'prefect-4291/processes.json').read_text(encoding='utf8'))
opener = build_opener(ProxyHandler({}))


def check_process(pid, marker):
    process = psutil.Process(pid)
    command = ' '.join(process.cmdline())
    if marker not in command:
        raise RuntimeError('Service identity mismatch: ' + marker)
    return process


def stop_tree(process):
    tree = process.children(recursive=True) + [process]
    for item in reversed(tree):
        try:
            item.terminate()
        except psutil.NoSuchProcess:
            pass
    _, alive = psutil.wait_procs(tree, timeout=8)
    for item in alive:
        try:
            item.kill()
        except psutil.NoSuchProcess:
            pass
    psutil.wait_procs(alive, timeout=5)


def launch_host():
    log = (RUNTIME / 'managed-service-launch.log').open('ab', buffering=0)
    return subprocess.Popen([str(PYW), str(LIVE / 'managed_service_host.py')], cwd=LIVE,
        stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP)


def restore_api_without_restart():
    env = dict(os.environ, PAPERFLOW_CONFIG=str(RUNTIME / 'workspace-config.json'), PYTHONUTF8='1',
        PREFECT_HOME=str(RUNTIME / 'prefect-4291/home'), PREFECT_API_URL='http://127.0.0.1:4291/api',
        PAPERFLOW_TUS_PUBLIC_URL='http://127.0.0.1:14203/tus/files/',
        PAPERFLOW_TUS_BACKEND_URL=f"http://127.0.0.1:{state['tusd_port']}/files/")
    log = (RUNTIME / 'control-preview/api.log').open('ab', buffering=0)
    proc = subprocess.Popen([str(PYW), '-X', 'utf8', '-m', 'uvicorn', 'summary_api:app',
        '--host', '127.0.0.1', '--port', '14203', '--no-access-log'], cwd=LIVE, env=env,
        stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP)
    state['api_pid'] = proc.pid
    api_state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding='utf8')


def get_json(url):
    with opener.open(url, timeout=2) as response:
        return json.load(response)


def await_ready(host):
    deadline = time.monotonic() + 55
    deployment = quote('论文第一步-可控预处理', safe='') + '/' + quote('手动运行', safe='')
    while time.monotonic() < deadline:
        if host.poll() is not None:
            raise RuntimeError('First-step service host exited during startup')
        try:
            health = get_json('http://127.0.0.1:14203/health')
            schema = get_json('http://127.0.0.1:14203/openapi.json')
            prefect = get_json('http://127.0.0.1:4291/api/health')
            deployment_info = get_json('http://127.0.0.1:4291/api/deployments/name/' + deployment)
            paths = schema['paths']
            if (health.get('status') == 'ok' and prefect is True and not deployment_info.get('paused')
                    and paths.get('/api/v1/first-step/tasks/{task_id}/local-source', {}).get('post')
                    and paths.get('/api/v1/first-step/tasks/{task_id}', {}).get('delete')):
                return
        except Exception:
            pass
        time.sleep(.4)
    raise RuntimeError('First-step services did not become ready')


def main():
    for source, _, _ in FILES:
        if not source.is_file():
            raise RuntimeError('Candidate file is missing: ' + str(source))
    check_idle(control_db, prefect_db)
    api = check_process(state['api_pid'], 'summary_api:app')
    runner = check_process(prefect_state['runner_pid'], 'serve_controlled.py')
    server = check_process(prefect_state['server_pid'], 'prefect server start')
    host_inner = api.parent()
    if not host_inner or 'managed_service_host.py' not in ' '.join(host_inner.cmdline()):
        raise RuntimeError('Cannot confirm service supervisor')
    host_outer = host_inner.parent()
    if not host_outer or 'managed_service_host.py' not in ' '.join(host_outer.cmdline()):
        raise RuntimeError('Cannot confirm service supervisor wrapper')

    backup = HERE / ('backup-before-cutover-' + time.strftime('%Y%m%d-%H%M%S'))
    backup.mkdir()
    for _, destination, relative in FILES:
        if destination.exists():
            target = backup / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(destination, target)

    stop_tree(api)
    try:
        check_idle(control_db, prefect_db)
    except Exception:
        restore_api_without_restart()
        raise
    stop_tree(runner)
    stop_tree(server)
    for host in (host_inner, host_outer):
        try:
            host.terminate()
            host.wait(timeout=5)
        except psutil.NoSuchProcess:
            pass

    host = None
    try:
        with sqlite3.connect(control_db) as original, sqlite3.connect(backup / 'summary-index.sqlite3') as saved:
            original.backup(saved)
        for source, destination, _ in FILES:
            shutil.copy2(source, destination)
        host = launch_host()
        await_ready(host)
    except Exception:
        if host is not None:
            try:
                stop_tree(psutil.Process(host.pid))
            except psutil.NoSuchProcess:
                pass
        for _, destination, relative in FILES:
            original = backup / relative
            if original.exists():
                shutil.copy2(original, destination)
            else:
                destination.unlink(missing_ok=True)
        restored = launch_host()
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline and restored.poll() is None:
            try:
                if get_json('http://127.0.0.1:14203/health').get('status') == 'ok':
                    break
            except Exception:
                pass
            time.sleep(.4)
        raise
    print('INSTALLED', 'backend=14203', 'prefect=4291', 'frontend=8795', 'backup=' + str(backup), flush=True)


if __name__ == '__main__':
    main()
