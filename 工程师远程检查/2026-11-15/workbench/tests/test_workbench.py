"""Browser controls must operate the real managed queue, not the retired service."""
import io
import json
from pathlib import Path
import sqlite3
import sys
import threading
import time

import pytest

from test_managed_entry import entry_case
from test_winjob import wait_until


def test_resume_cannot_revive_a_request_from_before_pause(tmp_path):
    from paperflow.control import Manager
    from paperflow.store import Store
    manager = Manager(Store(tmp_path / 'state.sqlite3'), tmp_path / 'manager').start()
    try:
        generation = manager.status()['generation']
        manager.pause('newer operator pause')
        wait_until(lambda: manager.status()['pause_persisted'])
        with pytest.raises(ValueError, match='stale'):
            manager.resume(expected_generation=generation)
        assert manager.stopped.is_set()
    finally:
        manager.shutdown()


@pytest.fixture
def web_case(entry_case):
    from paperflow.workbench import Workbench
    from webworkbench.api import create_app
    from webworkbench.auth import create_admin
    from fastapi.testclient import TestClient
    root, old, source, config = entry_case
    admin = root / 'auth' / 'admin.json'
    create_admin(admin, 'admin', 'isolated-workbench-password')
    workbench = Workbench(config, root, legacy_root=old)
    with TestClient(create_app(workbench, admin)) as client:
        yield workbench, client, source


def login(client):
    response = client.post('/api/login', json={'username': 'admin', 'password': '<REDACTED_SECRET>'})
    assert response.status_code == 200
    return {'x-csrf-token': response.json()['csrf']}


def select(client, headers, name='paper.txt', data=b'An explicitly selected source'):
    response = client.post('/api/inputs/upload', params={'name': name}, content=data, headers=headers)
    assert response.status_code == 200, response.text
    return response.json()['id']


def command(client, headers, path, **extra):
    snapshot = client.get('/api/status').json()
    status=snapshot['manager']
    return client.post(path, headers=headers, json={'epoch': status['epoch'], 'generation': status['generation'],
                        **({'batch_id':snapshot['batch_id']} if path=='/api/continue' else {}), **extra})


def test_login_csrf_and_upload_leave_original_and_admission_untouched(web_case):
    workbench, client, source = web_case
    assert client.get('/api/status').status_code == 401
    headers = login(client)
    assert client.post('/api/inputs/upload?name=x.txt', content=b'x').status_code == 403
    before = source.read_bytes()
    first = select(client, headers, data=before)
    assert select(client, headers, data=before) == first
    assert source.read_bytes() == before
    status = client.get('/api/status').json()
    assert status['manager']['state'] == 'paused'
    assert status['articles'] == []
    assert workbench.scheduler.ledger.rows('SELECT * FROM scheduler_attempts') == []
    assert client.post('/api/inputs/upload?name=../../x.txt', content=b'x', headers=headers).status_code == 400
    assert client.post('/api/inputs/upload?name=bad.unhandled', content=b'x', headers=headers).status_code == 400


def test_browser_start_pause_continue_controls_one_real_process_tree(web_case):
    workbench, client, _ = web_case
    headers = login(client)
    selected = select(client, headers)
    script = """import time
from paperflow.control_ipc import WorkerClient
c=WorkerClient.from_environment()
try:
 c.perform('llm', {'operation':'isolated-control-probe','stage':'round1'}, lambda cancel: cancel.wait(30), total_timeout=40)
except Exception: pass
c.report_execution('stopped', {'reason':'isolated probe stopped'})
c.close()
"""
    def launch(manager, config, key, *, schedule_attempt):
        return manager.launch('graph', key, [sys.executable, '-c', script], cwd=Path.cwd(),
                              schedule_attempt=schedule_attempt)
    workbench.scheduler.launchers['graph'] = launch
    response = command(client, headers, '/api/start', inputs=[selected], request_id='one-browser-click')
    assert response.status_code == 200, response.text
    wait_until(lambda: workbench.manager.status()['live_units'] == 1)
    wait_until(lambda: bool(workbench.manager.executions) and any(e.active_action for e in workbench.manager.executions.values()))
    status = client.get('/api/status').json()
    assert len(status['articles']) == 1
    assert status['articles'][0]['complete'] is False
    assert any(p['pid'] and p['task_id'] == status['articles'][0]['task_id'] for p in status['processes'])
    assert next(p for p in status['processes'] if p['state']=='running')['heartbeat_age_seconds'] < 5
    assert status['articles'][0]['elapsed_seconds'] >= 0
    running_pid=next(p['pid'] for p in status['processes'] if p['state']=='running')
    # Stop must not wait for the workflow SQLite writer.
    lock = sqlite3.connect(workbench.store.path)
    lock.execute('BEGIN IMMEDIATE')
    try:
        started = time.monotonic()
        paused = client.post('/api/pause', headers=headers, json={'epoch': workbench.manager.epoch})
        assert time.monotonic() - started < 1
        assert paused.status_code == 200 and paused.json()['manager']['admission_open'] is False
    finally:
        lock.rollback(); lock.close()
    wait_until(lambda: workbench.manager.status()['state'] == 'paused', seconds=15)
    assert client.get('/api/status').json()['processes'][0]['pid']==running_pid
    assert not client.get('/api/status').json()['manager']['admission_open']
    continued = command(client, headers, '/api/continue')
    assert continued.status_code == 200, continued.text
    wait_until(lambda: workbench.manager.status()['live_units'] == 1)
    assert len(client.get('/api/status').json()['articles']) == 1
    assert len(workbench.scheduler.ledger.rows('SELECT * FROM scheduler_jobs')) == 1


def test_stale_browser_continue_is_denied_and_restart_stays_paused(web_case):
    workbench, client, _ = web_case
    headers = login(client)
    old = client.get('/api/status').json()['manager']
    client.post('/api/pause', headers=headers, json={'epoch': old['epoch']})
    wait_until(lambda: workbench.manager.status()['pause_persisted'])
    stale = client.post('/api/continue', headers=headers,
                        json={'epoch': old['epoch'], 'generation': old['generation']})
    assert stale.status_code == 409
    wrong_epoch = client.post('/api/continue', headers=headers,
                              json={'epoch': 'old-manager', 'generation': workbench.manager.generation})
    assert wrong_epoch.status_code == 409
    assert workbench.manager.stopped.is_set()


def test_selected_files_are_a_bounded_batch_and_cannot_target_production(web_case):
    workbench, client, _ = web_case
    headers = login(client)
    selected = select(client, headers)
    workbench.config.rag_url = 'http://192.168.1.120:8088'
    response = command(client, headers, '/api/start', inputs=[selected], request_id='forbidden-target')
    assert response.status_code == 400
    assert workbench.store.db and not workbench.scheduler.ledger.rows('SELECT * FROM scheduler_attempts')


def test_pause_wins_over_start_waiting_for_sqlite_writer(web_case):
    workbench, client, _ = web_case
    headers = login(client)
    selected = select(client, headers)
    before = client.get('/api/status').json()['manager']
    entered = threading.Event()
    original = workbench.store.register_source_sql
    def registering(*args, **kwargs):
        entered.set()
        return original(*args, **kwargs)
    workbench.store.register_source_sql = registering
    lock = sqlite3.connect(workbench.store.path)
    lock.execute('BEGIN IMMEDIATE')
    results = []
    request = threading.Thread(target=lambda: results.append(client.post('/api/start',headers=headers,json={
        'inputs':[selected],'request_id':'blocked-start','epoch':before['epoch'],'generation':before['generation']})))
    try:
        request.start()
        assert entered.wait(3)
        response = client.post('/api/pause',headers=headers,json={'epoch':before['epoch']})
        assert response.status_code == 200 and not response.json()['manager']['admission_open']
    finally:
        lock.rollback();lock.close()
        request.join(5)
    assert not request.is_alive()
    assert results[0].status_code == 409
    assert workbench.manager.stopped.is_set()
    assert not workbench.scheduler.ledger.rows('SELECT * FROM scheduler_attempts')


def test_new_manager_keeps_selection_but_rejects_old_browser_epoch(entry_case):
    from paperflow.workbench import Workbench
    from webworkbench.api import create_app
    from webworkbench.auth import create_admin
    from fastapi.testclient import TestClient
    root, old, _, config = entry_case
    admin = root / 'auth' / 'admin.json'
    create_admin(admin,'admin','isolated-workbench-password')
    with TestClient(create_app(Workbench(config,root,legacy_root=old),admin)) as client:
        headers = login(client)
        select(client,headers)
        before = client.get('/api/status').json()['manager']
    second = Workbench(config,root,legacy_root=old)
    with TestClient(create_app(second,admin)) as client:
        headers = login(client)
        after = client.get('/api/status').json()['manager']
        assert after['epoch'] != before['epoch'] and after['state']=='paused'
        assert len(client.get('/api/inputs').json())==1
        assert client.post('/api/continue',headers=headers,json={'epoch':before['epoch'],'generation':before['generation']}).status_code==409
        assert not second.scheduler.ledger.rows('SELECT * FROM scheduler_attempts')


def test_malformed_selection_returns_validation_error_without_starting(web_case):
    workbench, client, _ = web_case
    headers = login(client)
    response = command(client,headers,'/api/start',inputs=[{'path':'anything'}],request_id='invalid')
    assert response.status_code==400
    assert workbench.manager.stopped.is_set()


def test_hashing_an_upload_does_not_block_the_pause_http_request(web_case):
    workbench, client, _ = web_case
    headers = login(client)
    entered, release = threading.Event(), threading.Event()
    original = workbench.record_upload
    def hashing(*args):
        entered.set()
        release.wait(3)
        return original(*args)
    workbench.record_upload = hashing
    results = []
    upload = threading.Thread(target=lambda: results.append(client.post('/api/inputs/upload?name=large.txt',content=b'input',headers=headers)))
    try:
        upload.start()
        assert entered.wait(3)
        started = time.monotonic()
        response = client.post('/api/pause',headers=headers,json={'epoch':workbench.manager.epoch})
        assert response.status_code==200
        assert time.monotonic()-started < 1
    finally:
        release.set();upload.join(5)
    assert results[0].status_code==200


def test_stop_file_during_registration_never_starts_business(web_case):
    workbench, client, _ = web_case
    headers = login(client)
    selected = select(client,headers)
    entered, release = threading.Event(), threading.Event()
    original = workbench.store.register_source_sql
    def registering(*args,**kwargs):
        entered.set();release.wait(3)
        return original(*args,**kwargs)
    workbench.store.register_source_sql = registering
    before = client.get('/api/status').json()['manager']
    results = []
    request = threading.Thread(target=lambda: results.append(client.post('/api/start',headers=headers,json={
        'inputs':[selected],'request_id':'stop-during-start','epoch':before['epoch'],'generation':before['generation']})))
    try:
        request.start();assert entered.wait(3)
        (workbench.root/'STOP').write_text('operator pause')
        time.sleep(.2)
    finally:
        release.set();request.join(5)
    assert results[0].status_code==409
    assert not workbench.manager.status()['admission_open']
    assert not workbench.scheduler.ledger.rows('SELECT * FROM scheduler_attempts')


def test_partial_selection_failure_rolls_back_tasks_and_batch_together(web_case):
    workbench,client,_=web_case
    headers=login(client)
    choices=[select(client,headers,name='first.txt',data=b'first'),select(client,headers,name='second.txt',data=b'second')]
    original=workbench.store.register_source_sql
    calls=[]
    def broken(*args,**kwargs):
        calls.append(True)
        if len(calls)==2:
            raise ValueError('interrupted selected source registration')
        return original(*args,**kwargs)
    workbench.store.register_source_sql=broken
    failed=command(client,headers,'/api/start',inputs=choices,request_id='atomic-selection')
    assert failed.status_code==400
    with workbench.store.db() as db:
        assert db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0]==0
        assert db.execute('SELECT COUNT(*) FROM web_batches').fetchone()[0]==0
    assert not workbench.scheduler.ledger.rows('SELECT * FROM scheduler_jobs')


def test_upload_keeps_attachment_name_and_deduplicates_same_bytes(web_case):
    workbench,client,_=web_case
    headers=login(client)
    first=select(client,headers,name='生产线图纸.dxf',data=b'offline drawing sample')
    second=select(client,headers,name='another-name.dxf',data=b'offline drawing sample')
    assert first==second
    assert len(client.get('/api/inputs').json())==1
    with workbench.store.db() as db:
        row=db.execute('SELECT * FROM web_inputs').fetchone()
        assert Path(row['path']).name=='生产线图纸.dxf'


def test_budget_failure_is_visible_instead_of_queued(web_case):
    workbench,client,_=web_case
    headers=login(client)
    selected=select(client,headers)
    workbench.config.max_infrastructure_retries=1
    script="from paperflow.control_ipc import WorkerClient;c=WorkerClient.from_environment();c.report_execution('failed',{'fault':{'category':'transient','message':'isolated upstream unavailable'}});c.close()"
    workbench.scheduler.launchers['graph']=lambda m,c,k,**kw:m.launch('graph',k,[sys.executable,'-c',script],cwd=Path.cwd(),**kw)
    assert command(client,headers,'/api/start',inputs=[selected],request_id='budget-test').status_code==200
    wait_until(lambda:bool(workbench.scheduler.ledger.rows("SELECT 1 FROM scheduler_jobs WHERE state='held_budget'")))
    article=client.get('/api/status').json()['articles'][0]
    assert article['state']=='held' and article['reason']=='isolated upstream unavailable'
    assert not article['complete']


def test_recovered_running_attempt_does_not_inherit_historical_failure(web_case):
    workbench,client,_=web_case
    headers=login(client);selected=select(client,headers)
    launched=[]
    def launch(manager,config,key,**kwargs):
        launched.append(True)
        report="c.report_execution('failed',{'fault':{'category':'transient','message':'previous offline fault'}})" if len(launched)==1 else "c.perform('llm',{'operation':'round1'},lambda cancel: {'cancelled':cancel.wait(30)},total_timeout=35);c.report_execution('stopped',{})"
        code="from paperflow.control_ipc import WorkerClient;c=WorkerClient.from_environment();"+report+";c.close()"
        return manager.launch('graph',key,[sys.executable,'-c',code],cwd=Path.cwd(),**kwargs)
    workbench.scheduler.launchers['graph']=launch
    assert command(client,headers,'/api/start',inputs=[selected],request_id='recover-state').status_code==200
    wait_until(lambda:bool(workbench.scheduler.ledger.rows("SELECT 1 FROM scheduler_jobs WHERE state='retry'")))
    assert client.get('/api/status').json()['articles'][0]['state']=='queued'
    workbench.scheduler.ledger._write(lambda db:db.execute("UPDATE scheduler_jobs SET due=0 WHERE state='retry'"))
    wait_until(lambda:len(launched)>=2 and any(e.active_action for e in workbench.manager.executions.values()),seconds=10)
    article=client.get('/api/status').json()['articles'][0]
    assert article['state']=='processing' and article['reason'] is None

