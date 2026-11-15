"""Run a synthetic registration-only flow in its own local Prefect workspace."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
import uuid

import httpx
import psutil


PROJECT = Path(__file__).resolve().parents[1]
ROOT = PROJECT / 'runtime' / ('registration-acceptance-' + uuid.uuid4().hex[:10])
ROOT.mkdir(parents=True)
PROCESSES = []
LOGS = []
FLAGS = subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def launch(name, args, env):
    log = (ROOT / (name + '.log')).open('wb', buffering=0)
    LOGS.append(log)
    child = subprocess.Popen(args, cwd=PROJECT, env=env, stdin=subprocess.DEVNULL,
                             stdout=log, stderr=subprocess.STDOUT, creationflags=FLAGS)
    PROCESSES.append(child)
    return child


def wait_for(fn, label, seconds=75):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            last = fn()
            if last:
                return last
        except (OSError, httpx.HTTPError, ValueError, KeyError):
            pass
        for process in PROCESSES:
            if process.poll() is not None:
                raise RuntimeError(label + ' process exited; inspect ' + str(ROOT))
        time.sleep(.25)
    raise TimeoutError(label + ' not ready; last=' + str(last) + '; logs=' + str(ROOT))


def main():
    ports = [free_port() for _ in range(4)]
    if len(set(ports)) != 4:
        raise RuntimeError('synthetic ports were not unique')
    prefect_port, api_port, model_port, ui_port = ports
    api = f'http://127.0.0.1:{api_port}'
    prefect = f'http://127.0.0.1:{prefect_port}/api'
    source = ROOT / 'source'
    for i in range(2):
        group = source / f'person-{i}'
        group.mkdir(parents=True)
        (group / 'paper.txt').write_text(f'题目：合成论文{i}\n摘要：这是隔离测试资料。', encoding='utf8')
    before = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in source.rglob('*') if p.is_file()}
    data = ROOT / 'data'
    data.mkdir()
    config = json.loads((PROJECT.parent/'first-step-control-20261029'/'runtime'/'workspace-config.json').read_text(encoding='utf8'))
    config.update(data_root=str(data), classification_root=str(ROOT/'output'), receipt_root=str(ROOT/'receipts'),
                  library_root=str(source), prefect_api_url=prefect,
                  nimble_endpoint=f'http://127.0.0.1:{model_port}/v1/systemone', history_roots=[])
    config_path = ROOT / 'config.json'
    config_path.write_text(json.dumps(config, ensure_ascii=False), encoding='utf8')
    env = dict(os.environ, PAPERFLOW_CONFIG=str(config_path), PREFECT_HOME=str(ROOT/'prefect-home'),
               PREFECT_API_URL=prefect, PREFECT_SERVER_ANALYTICS_ENABLED='false', PYTHONUTF8='1',
               PREFECT_SERVER_DATABASE_CONNECTION_URL='sqlite+aiosqlite:///' + (ROOT/'prefect.db').as_posix())
    python = sys.executable
    client = httpx.Client(timeout=5, trust_env=False)
    launch('prefect-server', [python, '-m', 'prefect', 'server', 'start', '--host', '127.0.0.1',
           '--port', str(prefect_port), '--analytics-off', '--no-scheduler'], env)
    wait_for(lambda: client.get(prefect+'/health').status_code == 200, 'Prefect')
    launch('fake-nimble', [python, '-c', "import sys;sys.path.insert(0,sys.argv[1]);from synthetic_nimble_server import Handler,ThreadingHTTPServer;ThreadingHTTPServer(('127.0.0.1',int(sys.argv[2])),Handler).serve_forever()",
           str(PROJECT/'tests'), str(model_port)], env)
    launch('prefect-runner', [python, '-X', 'utf8', 'serve_controlled.py'], env)
    launch('api', [python, '-X', 'utf8', '-m', 'uvicorn', 'summary_api:app', '--host', '127.0.0.1',
           '--port', str(api_port), '--no-access-log'], env)
    wait_for(lambda: client.get(api+'/health').status_code == 200, 'first-step API')
    deployment = '/deployments/name/%E8%AE%BA%E6%96%87%E7%AC%AC%E4%B8%80%E6%AD%A5-%E5%8F%AF%E6%8E%A7%E9%A2%84%E5%A4%84%E7%90%86/%E6%89%8B%E5%8A%A8%E8%BF%90%E8%A1%8C'
    wait_for(lambda: client.get(prefect+deployment).status_code == 200, 'deployment')

    def checked(response):
        if response.status_code >= 300:
            raise RuntimeError(str(response.status_code) + ' ' + response.text[:400])
        return response.json()

    def run_task(name):
        task = checked(client.post(api+'/api/v1/first-step/tasks', json={'name': name}))['task_id']
        selected = checked(client.post(api+f'/api/v1/first-step/tasks/{task}/local-source', json={'path': str(source)}))
        checked(client.post(api+f'/api/v1/first-step/tasks/{task}/start',
                            json={'expected_revision': selected['control_revision']}))
        def finished():
            result = checked(client.get(api+f'/api/v1/first-step/tasks/{task}'))
            if result['task_status'] in ('failed', 'unknown', 'needs_review'):
                raise RuntimeError('synthetic flow did not complete: ' + json.dumps(result, ensure_ascii=False))
            return result if result['task_status'] == 'complete' else None
        result = wait_for(finished, name, seconds=90)
        with sqlite3.connect(ROOT/'output'/'group-task-summaries'/'summary-index.sqlite3') as db:
            run = db.execute('SELECT prefect_run_id FROM tasks WHERE task_id=?', (task,)).fetchone()[0]
        wait_for(lambda: checked(client.get(prefect+'/flow_runs/'+run))['state']['type'] == 'COMPLETED',
                 'Prefect flow state', seconds=40)
        return task, result

    first_id, first = run_task('合成登记一')
    assert first['passed_groups'] == first['registered_groups'] == 2 and first['registration_failed_groups'] == 0
    first_rows = checked(client.get(api+f'/api/v1/first-step/tasks/{first_id}?filter=passed'))['items']
    second_id, second = run_task('合成登记二')
    second_rows = checked(client.get(api+f'/api/v1/first-step/tasks/{second_id}?filter=passed'))['items']
    assert {x['group_id']: x['paper_id'] for x in first_rows} == {x['group_id']: x['paper_id'] for x in second_rows}
    assert checked(client.delete(api+f'/api/v1/first-step/tasks/{first_id}'))['deleted']
    assert all(not Path(row['receipt_path']).exists() for row in first_rows)
    assert all(Path(row['receipt_path']).is_file() for row in second_rows)

    config['history_roots'] = [str(ROOT/'missing-history')]
    config_path.write_text(json.dumps(config, ensure_ascii=False), encoding='utf8')
    failed_id, failed = run_task('合成登记失败')
    assert failed['passed_groups'] == failed['registration_failed_groups'] == 2
    failures = checked(client.get(api+f'/api/v1/first-step/tasks/{failed_id}?filter=registration_failed'))
    assert failures['list_total'] == 2 and all(row['registration_reason'] for row in failures['items'])
    after = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in source.rglob('*') if p.is_file()}
    assert before == after and not any((data/name).exists() for name in ('papers','unified-papers','stage5'))

    static = ROOT/'frontend'
    shutil.copytree(PROJECT.parent/'first-step-preview-8795', static)
    shutil.copy2(PROJECT/'frontend'/'app.js', static/'app.js')
    shutil.copy2(PROJECT/'frontend'/'index.html', static/'index.html')
    (static/'api-config.json').write_text(json.dumps({'api_base_url': api, 'open_current': True}), encoding='utf8')
    launch('frontend', [python, '-m', 'http.server', str(ui_port), '--bind', '127.0.0.1', '--directory', str(static)], env)
    wait_for(lambda: client.get(f'http://127.0.0.1:{ui_port}/').status_code == 200, 'frontend')
    print('ACCEPTANCE_READY ' + json.dumps(dict(ui=f'http://127.0.0.1:{ui_port}/', task=failed_id,
        api=api, registered=first['registered_groups'], registration_failed=failures['list_total'],
        same_local_source_identity=True, deleted_first_kept_second=True, sources_unchanged=True,
        downstream_started=False, fake_model=True), ensure_ascii=False), flush=True)
    sys.stdin.readline()


def cleanup():
    for child in reversed(PROCESSES):
        try:
            process = psutil.Process(child.pid)
            tree = process.children(recursive=True) + [process]
            for item in reversed(tree):
                try: item.terminate()
                except psutil.Error: pass
            _, alive = psutil.wait_procs(tree, timeout=4)
            for item in alive:
                try: item.kill()
                except psutil.Error: pass
        except psutil.Error:
            pass
    for log in LOGS:
        log.close()


if __name__ == '__main__':
    try:
        main()
    finally:
        cleanup()
