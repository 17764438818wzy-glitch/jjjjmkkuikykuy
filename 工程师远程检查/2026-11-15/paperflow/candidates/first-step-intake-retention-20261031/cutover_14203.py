"""One-time idle-only switch of the first-step API; never starts a paper run."""
from datetime import datetime
import atexit
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import time
from urllib.request import urlopen

import psutil

ROOT = Path(__file__).resolve().parent.parent
LIVE = ROOT / 'first-step-control-20261029'
CANDIDATE = Path(__file__).resolve().parent
VALIDATED = ROOT / 'first-step-retention-merged-check-20261031'
RUNTIME = LIVE / 'runtime'
CONFIG = RUNTIME / 'workspace-config.json'
CONTROL_DB = RUNTIME / 'workspace/output/group-task-summaries/summary-index.sqlite3'
RECEIPT_DB = RUNTIME / 'workspace/receipts/receipts.sqlite3'
PREFECT_DB = RUNTIME / 'prefect-4291/prefect.db'
QUEUE_DB = ROOT.parent / 'local-intake-runtime/waiting-pool.sqlite3'
PROCESS_STATE = RUNTIME / 'control-preview/processes.json'
PYW = ROOT.parent / 'prefect-first-step-independent/.venv/Scripts/pythonw.exe'
BASELINE = {
    'summary_api.py': '78ed61b92f41b0755af47ce188b10b947205ee79a0674c586f75f7d416891b02',
    'paperflow_prefect/first_step_control.py': 'a78873904b7e2efb43da2548816c6b66bd6fda8fc6bac2d58865333fee3e8d47',
    'paperflow_prefect/first_step_control_api.py': '4fe8375e109f0b90348c9e4af1241013fa94943b2b45179f2e877d788548038b',
    'paperflow_prefect/first_step_delete.py': '8a0ef8f8de4c2445c6ae54e492b728294cccc7df31ed875d75279a0a672953b8',
    'paperflow_prefect/first_step_retention.py': None,
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None


def atomic_bytes(path, data):
    temporary = path.with_name(path.name + '.retention-switch.tmp')
    if temporary.exists():
        raise RuntimeError('previous temporary switch file exists: ' + str(temporary))
    with temporary.open('xb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def snapshot_sqlite(source, destination):
    if not source.is_file():
        raise RuntimeError('database missing: ' + str(source))
    with sqlite3.connect(source.as_uri() + '?mode=ro', uri=True, timeout=5) as src:
        with sqlite3.connect(destination) as dst:
            src.backup(dst)
            if dst.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                raise RuntimeError('backup integrity check failed: ' + str(source))


def task_snapshot():
    with sqlite3.connect(CONTROL_DB.as_uri() + '?mode=ro', uri=True, timeout=5) as db:
        tasks = db.execute('SELECT task_id,task_status FROM tasks ORDER BY task_id').fetchall()
        groups = db.execute('SELECT status,count(*) FROM group_results GROUP BY status ORDER BY status').fetchall()
    with sqlite3.connect(RECEIPT_DB.as_uri() + '?mode=ro', uri=True, timeout=5) as db:
        receipt_count = db.execute('SELECT count(*) FROM receipt_events').fetchone()[0]
    return {'tasks': tasks, 'group_results': groups, 'receipt_count': receipt_count}


def fetch(url):
    with urlopen(url, timeout=3) as response:
        return json.load(response)


def listener_pid(port):
    matches = [c.pid for c in psutil.net_connections(kind='tcp') if c.status == psutil.CONN_LISTEN
               and c.laddr.ip == '127.0.0.1' and c.laddr.port == port]
    if len(matches) != 1 or matches[0] is None:
        raise RuntimeError(f'expected exactly one local listener on {port}')
    return matches[0]


def preflight():
    spec = importlib.util.spec_from_file_location('retention_preflight', LIVE / 'managed_service_preflight.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.check_idle(CONTROL_DB, PREFECT_DB)
    with sqlite3.connect(QUEUE_DB.as_uri() + '?mode=ro', uri=True, timeout=5) as db:
        schema = db.execute("SELECT value FROM metadata WHERE key='queue_schema'").fetchone()
        if (schema != ('paperflow-local-waiting/1',)
                or any(db.execute('SELECT count(*) FROM ' + table).fetchone()[0]
                       for table in ('groups', 'retention_requests', 'claims'))):
            raise RuntimeError('local queue not empty or schema does not match')


def main():
    for name, expected in BASELINE.items():
        if digest(LIVE / name) != expected:
            raise RuntimeError('live file changed since review: ' + name)
        if digest(CANDIDATE / name) is None or digest(CANDIDATE / name) != digest(VALIDATED / name):
            raise RuntimeError('candidate differs from validated version: ' + name)
    if digest(LIVE / 'paperflow_prefect/group_classification.py') != digest(VALIDATED / 'paperflow_prefect/group_classification.py'):
        raise RuntimeError('live classification changed since combined testing')
    preflight()
    config = json.loads(CONFIG.read_text(encoding='utf8'))
    if config.get('local_intake_queue_db') not in (None, str(QUEUE_DB)):
        raise RuntimeError('another local queue is already configured')
    if (Path(config['classification_root']).resolve() != CONTROL_DB.parents[1]
            or Path(config['receipt_root']).resolve() != RECEIPT_DB.parent
            or config['prefect_api_url'] != 'http://127.0.0.1:4291/api'):
        raise RuntimeError('workspace identity does not match')
    old_pid = listener_pid(14203)
    api = psutil.Process(old_pid)
    command = api.cmdline()
    if ('uvicorn' not in command or 'summary_api:app' not in command or '14203' not in command
            or Path(api.cwd()).resolve() != LIVE.resolve()):
        raise RuntimeError('14203 process is not the expected first-step API')
    launcher = api.parent()
    if launcher is None or 'summary_api:app' not in launcher.cmdline():
        raise RuntimeError('14203 launcher identity is not confirmed')
    environment = api.environ()
    if (Path(environment['PAPERFLOW_CONFIG']).resolve() != CONFIG.resolve()
            or environment.get('PAPERFLOW_TUS_BACKEND_URL') != 'http://127.0.0.1:54785/files/'
            or environment.get('PAPERFLOW_LOCAL_INTAKE_DB') not in (None, '', str(QUEUE_DB))):
        raise RuntimeError('14203 environment does not match the reviewed configuration')
    state = json.loads(PROCESS_STATE.read_text(encoding='utf8'))
    if state.get('api_pid') != launcher.pid or state.get('tusd_port') != 54785:
        raise RuntimeError('recorded process state changed')
    before = task_snapshot()
    deployment_id = '87375d6f-d8c3-4068-b374-3a9f08fde37a'
    deployment_before = fetch('http://127.0.0.1:4291/api/deployments/' + deployment_id)
    if deployment_before.get('paused') is not False:
        raise RuntimeError('Prefect deployment is not currently enabled')
    backup = RUNTIME / ('retention-api-switch-' + datetime.now().strftime('%Y%m%d-%H%M%S'))
    backup.mkdir(parents=True, exist_ok=False)
    for name, expected in BASELINE.items():
        if expected is not None:
            target = backup / 'files' / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(LIVE / name, target)
    shutil.copy2(CONFIG, backup / 'workspace-config.json')
    shutil.copy2(PROCESS_STATE, backup / 'processes.json')
    for source, name in ((CONTROL_DB, 'control.sqlite3'), (RECEIPT_DB, 'receipts.sqlite3'),
                         (QUEUE_DB, 'waiting-pool.sqlite3')):
        snapshot_sqlite(source, backup / name)
    (backup / 'before.json').write_text(json.dumps({'tasks': before, 'api_pid': old_pid,
        'launcher_pid': launcher.pid, 'deployment_paused': deployment_before['paused']},
        ensure_ascii=False, indent=2), encoding='utf8')
    preflight()
    if listener_pid(14203) != old_pid or task_snapshot() != before:
        raise RuntimeError('work changed before stopping the API')
    print('backup complete; stopping only first-step API pid', old_pid, flush=True)
    api.terminate()
    api.wait(timeout=10)
    try:
        launcher.wait(timeout=10)
    except psutil.TimeoutExpired:
        if launcher.is_running() and 'summary_api:app' in launcher.cmdline():
            launcher.terminate()
            launcher.wait(timeout=10)
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 14203))
    preflight()
    if task_snapshot() != before:
        raise RuntimeError('task state changed during API switch; API remains stopped')
    for name in BASELINE:
        atomic_bytes(LIVE / name, (CANDIDATE / name).read_bytes())
    config['local_intake_queue_db'] = str(QUEUE_DB)
    atomic_bytes(CONFIG, (json.dumps(config, ensure_ascii=False, indent=2) + '\n').encode('utf8'))
    preflight()
    log_path = backup / 'api.log'
    environment['PYTHONUTF8'] = '1'
    with log_path.open('ab', buffering=0) as log:
        new_api = subprocess.Popen([str(PYW), '-X', 'utf8', '-m', 'uvicorn', 'summary_api:app',
            '--host', '127.0.0.1', '--port', '14203', '--no-access-log'], cwd=LIVE, env=environment,
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
            creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW)
    outcome = {'verified': False}
    def stop_unverified_api():
        if outcome['verified']:
            return
        try:
            parent = psutil.Process(new_api.pid)
            for process in reversed([parent, *parent.children(recursive=True)]):
                try:
                    process.terminate()
                except psutil.Error:
                    pass
        except psutil.Error:
            pass
    atexit.register(stop_unverified_api)
    deadline = time.monotonic() + 35
    while time.monotonic() < deadline:
        if new_api.poll() is not None:
            raise RuntimeError('new API exited; see ' + str(log_path))
        try:
            schema = fetch('http://127.0.0.1:14203/openapi.json')
            if '/api/v1/first-step/tasks/{task_id}/local-retentions' in schema.get('paths', {}):
                break
        except (OSError, ValueError):
            pass
        time.sleep(.25)
    else:
        raise RuntimeError('new API did not become ready; see ' + str(log_path))
    new_pid = listener_pid(14203)
    new_proc = psutil.Process(new_pid)
    if Path(new_proc.environ().get('PAPERFLOW_CONFIG', '')).resolve() != CONFIG.resolve():
        raise RuntimeError('new API has unexpected configuration')
    if before['tasks']:
        task_id = before['tasks'][0][0]
        refs = fetch('http://127.0.0.1:14203/api/v1/first-step/tasks/' + task_id + '/local-retentions?page=1&page_size=200')
        if refs.get('task_id') != task_id or not isinstance(refs.get('items'), list):
            raise RuntimeError('new API retention query failed')
    deployment_after = fetch('http://127.0.0.1:4291/api/deployments/' + deployment_id)
    if deployment_after.get('paused') != deployment_before.get('paused') or task_snapshot() != before:
        raise RuntimeError('Prefect or existing task state changed during switch')
    state['api_pid'] = new_api.pid
    state['api_mode'] = 'independent-retention-api'
    atomic_bytes(PROCESS_STATE, (json.dumps(state, ensure_ascii=False, indent=2) + '\n').encode('utf8'))
    result = {'api_launcher_pid': new_api.pid, 'api_listener_pid': new_pid, 'backup': str(backup),
              'task_snapshot': before, 'prefect_paused': deployment_after['paused'],
              'queue_configured': True, 'supervisor': 'independent; existing host does not restart this API'}
    (backup / 'after.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf8')
    outcome['verified'] = True
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
