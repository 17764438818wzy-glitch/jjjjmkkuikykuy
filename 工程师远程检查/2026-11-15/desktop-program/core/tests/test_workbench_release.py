"""The deployed screen must project current pipeline facts and keep bounded admission."""
import json
from pathlib import Path
import time

import pytest

from paperflow.package import save, sha
from paperflow.store import Store
from test_managed_entry import entry_case
from test_workbench import web_case, login, select, command
from test_winjob import wait_until


def test_formats_follow_the_repaired_existing_intake(web_case):
    from paperflow.intake import EXTENSIONS
    workbench, client, _ = web_case
    headers = login(client)
    for suffix in ('.doc', '.docm', '.rtf', '.wps', '.odt'):
        assert suffix in EXTENSIONS
        select(client, headers, name='论文'+suffix, data=suffix.encode())


def test_atomic_registrar_keeps_source_version_supersession(tmp_path):
    store = Store(tmp_path / 'state.sqlite3')
    source = tmp_path / 'paper.doc'
    old = store.register_source(tmp_path/'old.doc', 'old', identity_path=source, supersede_identity=True)
    with store.db() as db:
        new = store.register_source_sql(db, tmp_path/'new.doc', 'new', identity_path=source, supersede_identity=True)
    assert not store.is_current(old['task_id'])
    assert store.is_current(new['task_id'])
    with store.db() as db:
        assert db.execute('SELECT COUNT(DISTINCT task_id) FROM sources').fetchone()[0] == 1
        assert db.execute('SELECT COUNT(*) FROM source_versions').fetchone()[0] == 3


def test_history_timing_and_failures_are_owned_by_task_not_shared_source(web_case):
    workbench, client, _ = web_case
    headers = login(client)
    source = workbench.root/'same-origin.txt'
    old = workbench.store.register_source(workbench.root/'old.txt', 'old-hash', identity_path=source)
    new = workbench.store.register_source(workbench.root/'new.txt', 'new-hash', identity_path=source, supersede_identity=True)
    now = time.time()
    ledger=workbench.scheduler.ledger
    service=workbench.scheduler.service.services['llm']['service_id']
    old_job=ledger.enqueue('graph',old['source_id'],old['task_id'],{},service)
    new_job=ledger.enqueue('graph',new['source_id'],new['task_id'],{},service)
    with workbench.store.db() as db:
        db.execute("UPDATE scheduler_jobs SET state='held' WHERE job_id=?",(old_job,))
        db.execute("UPDATE scheduler_jobs SET state='complete' WHERE job_id=?",(new_job,))
        for key, job, start, finish, outcome, data in [
            ('a-old',old_job,now-105,now-5,'failed',{'reason':'obsolete fault'}),
            ('a-new',new_job,now-3,now-1,'complete',{})]:
            db.execute('INSERT INTO scheduler_attempts VALUES(?,?,?,?,?,?,?,?)',
                       (key,job,None,'finished',start,finish,outcome,json.dumps(data)))
        for key,task in [('old-batch',old),('new-batch',new)]:
            db.execute('INSERT INTO web_batches VALUES(?,?,?,?,?)',(key,key,'[]',json.dumps([task['task_id']]),now))
    new_view=client.get('/api/status?batch_id=new-batch').json()['articles'][0]
    old_view=client.get('/api/status?batch_id=old-batch').json()['articles'][0]
    assert new_view['graph_seconds']==2 and new_view['attempts']==1
    assert new_view['reason'] is None
    assert old_view['graph_seconds']==100 and old_view['attempts']==1
    assert old_view['reason']=='obsolete fault'


def deployment_case(entry_case):
    root, old, source, config = entry_case
    config.rag_url='http://192.168.1.120:8088'
    config.production_pilot=str(root/'web-scope.json')
    report=json.loads(Path(config.delivery_acceptance_report).read_text())
    report['endpoint']=config.rag_url
    save(config.delivery_acceptance_report, report)
    path=root/'operator-deployment.json'
    save(path, dict(mode='operator-web',root=str(root),endpoint=config.rag_url,
        catalog_sha256=sha(Path(config.catalog_path)),
        acceptance_sha256=sha(Path(config.delivery_acceptance_report)),
        native_pipeline_id=config.native_pipeline_id, public_host='192.168.1.120',
        maximum_seconds=86400,maximum_selected=10))
    return root,old,config,path


def test_deployed_mode_starts_paused_without_issuing_a_selection(entry_case):
    from paperflow.workbench import Workbench
    root,old,config,path=deployment_case(entry_case)
    workbench=Workbench(config,root,legacy_root=old,deployment=path)
    try:
        workbench.open()
        assert workbench.control_state()['state']=='paused'
        assert not Path(config.production_pilot).exists()
        assert workbench.store.all_tasks()==[]
        assert workbench.status()['settings']['routing']=='automatic'
    finally:workbench.close()


def test_operator_batch_has_exact_expiring_scope_and_legacy_exclusion(entry_case):
    from paperflow.workbench import Workbench
    from paperflow.managed import legacy_guard
    from paperflow.instance_lock import InstanceLock
    from webworkbench.api import create_app
    from webworkbench.auth import create_admin
    from fastapi.testclient import TestClient
    root,old,config,path=deployment_case(entry_case)
    admin=root/'admin.json';create_admin(admin,'admin','isolated-workbench-password')
    workbench=Workbench(config,root,legacy_root=old,deployment=path)
    with TestClient(create_app(workbench,admin,allowed_hosts=['192.168.1.120','testserver'])) as client:
        headers=login(client)
        selected=select(client,headers)
        # This test exercises admission, not a live LLM request.
        workbench.scheduler.launchers['graph']=lambda *a,**k: (_ for _ in ()).throw(ValueError('no test worker'))
        assert command(client,headers,'/api/start',inputs=[selected],request_id='explicit-click').status_code==200
        scope=json.loads(Path(config.production_pilot).read_text())
        assert scope['mode']=='production-pilot' and len(scope['sources'])==1
        assert scope['sources'][0]['sha256']
        assert time.time()<scope['expires_at']<=time.time()+86400
        with pytest.raises(RuntimeError):
            with InstanceLock(legacy_guard(old).paths[0]):pass
        response=client.post('/api/pause',headers=headers,json={'epoch':workbench.manager.epoch})
        assert response.status_code==200
        wait_until(lambda:workbench.control_state()['pause_persisted'])
        # Any changed mapping invalidates further operator admission.
        save(config.catalog_path,{'majors':[]})
        response=command(client,headers,'/api/continue')
        assert response.status_code==400 and workbench.manager.stopped.is_set()


def test_frontend_projects_auto_routes_and_shared_budget_without_manual_select(web_case):
    workbench,client,_=web_case
    headers=login(client)
    task=workbench.store.register_source(workbench.root/'paper.txt','offline-hash',display_name='样例正文')
    with workbench.store.db() as db:
        db.execute('INSERT INTO web_batches VALUES(?,?,?,?,?)',('facts','facts','[]',json.dumps([task['task_id']]),time.time()))
        db.execute('INSERT INTO correction_budgets VALUES(?,?,?)',(task['source_id'],task['source_sha256'],1))
    status=client.get('/api/status').json()
    assert status['settings']['routing']=='automatic'
    assert status['settings']['specialty_count']==1
    article=status['articles'][0]
    assert article['correction_budget']=={'used':1,'limit':2}
    assert article['routes']==[] and article['artifacts']['profile']=='pending'
    assert article['detail']['cleanup']['closeout_pending']==0
    page=client.get('/').text
    # Input location and read-only state filters do not permit manual target-library selection.
    import re
    assert set(re.findall(r'<select\b[^>]*\bid="([^"]+)"',page)) <= {'nas-root','article-status-filter','process-status-filter'}
    assert 'Graph、Wiki' in page


def test_historical_continue_cannot_resume_a_different_latest_batch(web_case):
    workbench,client,_=web_case
    headers=login(client)
    keys=[select(client,headers,name='old.txt',data=b'old source'),
          select(client,headers,name='new.txt',data=b'new source')]
    workbench.scheduler.launchers['graph']=lambda *a,**k: (_ for _ in ()).throw(RuntimeError('no business worker in control acceptance'))
    service=workbench.scheduler.service.services['llm']['service_id']
    for index,key in enumerate(keys):
        with workbench.store.db() as db:
            row=dict(db.execute('SELECT * FROM web_inputs WHERE id=?',(key,)).fetchone())
        task=workbench.store.register_source(row['path'],row['sha256'])
        workbench.scheduler.ledger.enqueue('graph',task['source_id'],task['task_id'],{},service)
        with workbench.store.db() as db:
            db.execute('INSERT INTO web_batches VALUES(?,?,?,?,?)',
                       ('old' if index==0 else 'latest',key,json.dumps([key]),json.dumps([task['task_id']]),time.time()+index))
    current=client.get('/api/status?batch_id=old').json()
    manager=current['manager']
    for selected in ('old',None):
        result=client.post('/api/continue',headers=headers,json={
            'epoch':manager['epoch'],'generation':manager['generation'],'batch_id':selected})
        assert result.status_code==409,result.text
        assert workbench.manager.stopped.is_set()
    assert current['batch_id']=='old' and current['latest_batch_id']=='latest'
    assert not workbench.scheduler.ledger.rows('SELECT * FROM scheduler_attempts')


def test_queue_claim_cannot_carry_an_older_batch_into_a_new_selection(web_case):
    workbench,client,_=web_case
    old=workbench.store.register_source(workbench.root/'old.txt','old-hash')
    selected=workbench.store.register_source(workbench.root/'selected.txt','selected-hash')
    ledger=workbench.scheduler.ledger
    service=workbench.scheduler.service.services['llm']['service_id']
    old_job=ledger.enqueue('graph',old['source_id'],old['task_id'],dict(task_id=old['task_id']),service)
    selected_job=ledger.enqueue('graph',selected['source_id'],selected['task_id'],dict(task_id=selected['task_id']),service)
    claimed=ledger.claim(task_ids=[selected['task_id']])
    assert claimed['job_id']==selected_job
    assert ledger.claim(task_ids=[selected['task_id']]) is None
    assert ledger.job(old_job)['state']=='queued'


def test_sqlite_wait_cannot_release_an_operator_start_after_its_scope_expires(entry_case):
    import sqlite3
    import threading
    from paperflow.workbench import Workbench
    from webworkbench.api import create_app
    from webworkbench.auth import create_admin
    from fastapi.testclient import TestClient
    root,old,config,path=deployment_case(entry_case)
    deployment=json.loads(path.read_text());deployment['maximum_seconds']=.2;save(path,deployment)
    admin=root/'admin.json';create_admin(admin,'admin','isolated-workbench-password')
    workbench=Workbench(config,root,legacy_root=old,deployment=path)
    with TestClient(create_app(workbench,admin)) as client:
        headers=login(client);selected=select(client,headers)
        entered=threading.Event();original=workbench.store.register_source_sql
        def registering(*args,**kwargs):
            entered.set();return original(*args,**kwargs)
        workbench.store.register_source_sql=registering
        state=client.get('/api/status').json()['manager']
        writer=sqlite3.connect(workbench.store.path);writer.execute('BEGIN IMMEDIATE')
        results=[]
        thread=threading.Thread(target=lambda:results.append(client.post('/api/start',headers=headers,json={
            'inputs':[selected],'request_id':'expired-sql-wait','epoch':state['epoch'],'generation':state['generation']})))
        try:
            thread.start();assert entered.wait(3);time.sleep(.4)
            assert not workbench.manager.status()['admission_open']
        finally:
            writer.rollback();writer.close();thread.join(5)
        assert results[0].status_code==409
        assert not workbench.scheduler.ledger.rows('SELECT * FROM scheduler_attempts')
        assert workbench.manager.stopped.is_set()


def test_repaired_artifact_counts_follow_current_expected_manifest(tmp_path):
    from paperflow.workbench_view import artifacts
    before=tmp_path/'r01';current=tmp_path/'r02'
    before.mkdir();current.mkdir()
    save(before/'tree.json',{'nodes':[1]})
    save(current/'tree.json',{'nodes':[1,2,3]})
    view=artifacts({'package_dir':str(before)},tmp_path,True,
                   versions=[dict(manifest='r02',path=str(current),codes=['x'],complete=True)])
    assert view['tree_nodes']==3
    assert view['versions'][0]['manifest']=='r02'


def test_old_unselected_queue_does_not_prevent_settling_or_the_next_explicit_batch(entry_case):
    import sys
    from paperflow.workbench import Workbench
    from webworkbench.api import create_app
    from webworkbench.auth import create_admin
    from fastapi.testclient import TestClient
    root,old,config,path=deployment_case(entry_case)
    admin=root/'admin.json';create_admin(admin,'admin','isolated-workbench-password')
    workbench=Workbench(config,root,legacy_root=old,deployment=path)
    with TestClient(create_app(workbench,admin)) as client:
        headers=login(client)
        obsolete=workbench.store.register_source(root/'obsolete.txt','obsolete')
        ledger=workbench.scheduler.ledger
        service=workbench.scheduler.service.services['llm']['service_id']
        old_job=ledger.enqueue('graph',obsolete['source_id'],obsolete['task_id'],dict(task_id=obsolete['task_id']),service)
        ledger._write(lambda db:db.execute("UPDATE scheduler_jobs SET state='held_budget' WHERE job_id=?",(old_job,)))
        code="from paperflow.control_ipc import WorkerClient;c=WorkerClient.from_environment();c.report_execution('held',{'reason':'isolated held result'});c.close()"
        workbench.scheduler.launchers['graph']=lambda m,c,k,**kw:m.launch('graph',k,[sys.executable,'-c',code],cwd=Path.cwd(),**kw)
        selected=select(client,headers,data=b'first current selection')
        assert command(client,headers,'/api/start',inputs=[selected],request_id='current-one').status_code==200
        ledger._write(lambda db:db.execute("UPDATE scheduler_jobs SET state='queued' WHERE job_id=?",(old_job,)))
        wait_until(lambda:bool(ledger.rows("SELECT 1 FROM scheduler_jobs WHERE unit_key!=? AND state='held'",(obsolete['task_id'],))) and workbench.control_state()['state']=='paused')
        assert ledger.job(old_job)['state']=='queued'
        next_input=select(client,headers,data=b'next explicitly selected input')
        assert command(client,headers,'/api/start',inputs=[next_input],request_id='current-two').status_code==200
        assert not ledger.rows('SELECT 1 FROM scheduler_attempts WHERE job_id=?',(old_job,))


def test_manager_rejects_a_bypassed_queue_claim_outside_the_operator_selection(web_case):
    import sys
    from paperflow.scheduling_policy import ActionDeferred
    workbench,client,_=web_case
    selected=workbench.store.register_source(workbench.root/'selected.txt','selected')
    other=workbench.store.register_source(workbench.root/'other.txt','other')
    ledger=workbench.scheduler.ledger
    service=workbench.scheduler.service.services['llm']['service_id']
    ledger.enqueue('graph',other['source_id'],other['task_id'],dict(task_id=other['task_id']),service)
    with workbench.scheduler.tick_lock:
        # Background discovery can already have queued the selected article.
        # This regression must bypass selection using the *other* article's claim.
        claim=ledger.claim(task_ids=[other['task_id']])
        workbench.scheduler.service.select_tasks([selected['task_id']],time.time()+30)
        wait_until(lambda:workbench.manager.status()['pause_persisted'])
        workbench.manager.resume()
        with pytest.raises(ActionDeferred,match='outside_operator_selection'):
            workbench.manager.launch('graph',other['task_id'],[sys.executable,'-c','raise RuntimeError("must not execute")'],
                                     cwd=Path.cwd(),schedule_attempt=claim['attempt_id'])
    assert not ledger.rows('SELECT 1 FROM control_executions WHERE root_identity IS NOT NULL')
