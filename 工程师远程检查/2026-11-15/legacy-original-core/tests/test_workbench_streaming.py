"""Streaming selection uses synthetic local files and existing managed scheduling only."""
import json
import sqlite3
import threading
import time

from test_managed_entry import entry_case
from test_workbench import web_case, login, select, command
from test_winjob import wait_until


def manifest():
    return [dict(id='paper', name='paper.txt', relative_path='folder/paper.txt', bytes=5),
            dict(id='drawing', name='drawing.dwg', relative_path='folder/parts/drawing.dwg', bytes=7),
            dict(id='software', name='program.zip', relative_path='folder/program.zip', bytes=3),
            dict(id='unsupported', name='notes.unknown', relative_path='folder/notes.unknown', bytes=2)]


def begin(workbench, client, headers, entries=None):
    # No model, RAG connection or business process is run by these control tests.
    workbench.scheduler.launchers['graph'] = lambda *a, **kw: (_ for _ in ()).throw(ValueError('synthetic held worker'))
    key=prepare(client,headers,'paper','paper.txt',b'paper',(entries or manifest())[0]['relative_path'])
    response=command(client,headers,'/api/start',inputs=[key],request_id='stream-click',
                     manifest=entries or manifest(),first_item='paper')
    assert response.status_code==200,response.text
    return response.json()


def append(client, headers, batch, item, key):
    m=client.get('/api/control').json()['manager']
    return client.post('/api/intake/append',headers=headers,json=dict(batch_id=batch,
        epoch=m['epoch'],generation=m['generation'],items=[dict(item_id=item,input_id=key)]))


def prepare(client,headers,item,name,data,relative=None):
    response=client.post('/api/inputs/upload',params=dict(name=name,relative_path=relative or 'folder/parts/'+name,
        request_id='stream-click',item_id=item),content=data,headers=headers)
    assert response.status_code==200,response.text
    return response.json()['id']


def test_manifest_retains_paper_and_attachments_without_preparing_all_files(web_case):
    w,c,_=web_case;h=login(c);started=begin(w,c,h)
    status=c.get('/api/status').json()
    assert status.get('intake',{}).get('total')==4
    assert status['intake']['ready']==1 and status['intake']['pending']==2
    assert status['intake']['skipped']==1 and status['intake']['open']
    assert status['manager']['state']=='running'
    assert len(w.inputs())==1 and len(status['articles'])==1
    listing=c.get('/api/intake/'+started['batch_id']).json()
    assert {r['name'] for r in listing['items']}=={e['name'] for e in manifest()}
    assert next(r for r in listing['items'] if r['id']=='unsupported')['error']
    assert c.post('/api/intake/append',headers=h,json={}).status_code==400


def test_append_is_same_batch_deduplicated_and_does_not_repeat_or_widen_selection(web_case):
    w,c,_=web_case;h=login(c);started=begin(w,c,h);batch=started['batch_id']
    key=prepare(c,h,'drawing','drawing.dwg',b'drawing')
    a=append(c,h,batch,'drawing',key);assert a.status_code==200,a.text
    assert append(c,h,batch,'drawing',key).status_code==200
    assert c.get('/api/status').json()['intake']['ready']==2
    with w.store.db() as db:
        row=db.execute('SELECT task_ids FROM web_batches WHERE id=?',(batch,)).fetchone()
    assert len(json.loads(row['task_ids']))==2
    assert set(json.loads(row['task_ids']))==set(w.scheduler.service.selected_tasks)
    other=select(c,h,name='wrong.dwg',data=b'other!!')
    assert append(c,h,batch,'drawing',other).status_code==400
    assert append(c,h,batch,'not-in-manifest',other).status_code==400
    assert len(w.store.all_tasks())==2


def test_preparation_gap_does_not_auto_pause_and_pause_invalidates_old_append(web_case):
    w,c,_=web_case;h=login(c);batch=begin(w,c,h)['batch_id']
    wait_until(lambda: bool(w.scheduler.ledger.rows('SELECT * FROM scheduler_jobs')))
    with w.store.db() as db:db.execute("UPDATE scheduler_jobs SET state='complete'")
    time.sleep(.45)
    assert not w.manager.stopped.is_set(), 'first prepared subset must not settle the open manifest'
    key=prepare(c,h,'drawing','drawing.dwg',b'drawing')
    m=w.manager.status()
    c.post('/api/pause',headers=h,json={'epoch':m['epoch']})
    response=c.post('/api/intake/append',headers=h,json=dict(batch_id=batch,epoch=m['epoch'],
        generation=m['generation'],items=[dict(item_id='drawing',input_id=key)]))
    assert response.status_code==409 and w.manager.stopped.is_set()
    assert c.get('/api/status').json()['intake']['ready']==1


def test_append_database_wait_never_blocks_immediate_pause_or_reopens_admission(web_case):
    w,c,_=web_case;h=login(c);batch=begin(w,c,h)['batch_id']
    key=prepare(c,h,'drawing','drawing.dwg',b'drawing');m=w.manager.status()
    entered=threading.Event();original=w.store.register_source_sql
    def blocked(*a,**kw):
        entered.set();return original(*a,**kw)
    w.store.register_source_sql=blocked
    writer=sqlite3.connect(w.store.path);writer.execute('BEGIN IMMEDIATE')
    responses=[]
    thread=threading.Thread(target=lambda:responses.append(c.post('/api/intake/append',headers=h,json=dict(
        batch_id=batch,epoch=m['epoch'],generation=m['generation'],items=[dict(item_id='drawing',input_id=key)]))))
    try:
        thread.start();assert entered.wait(3)
        clock=time.monotonic();paused=c.post('/api/pause',headers=h,json={'epoch':m['epoch']})
        assert time.monotonic()-clock<1 and paused.status_code==200
        assert not w.manager.status()['admission_open']
    finally:
        writer.rollback();writer.close();thread.join(6)
    assert responses[0].status_code==409
    assert w.manager.stopped.is_set()
    assert len(w.scheduler.service.selected_tasks)==1


def test_same_content_items_keep_manifest_count_separate_from_unique_tasks(web_case):
    w,c,_=web_case;h=login(c)
    entries=[dict(id='paper',name='paper.txt',relative_path='a/paper.txt',bytes=5),
             dict(id='copy',name='copy.txt',relative_path='b/copy.txt',bytes=5)]
    batch=begin(w,c,h,entries)['batch_id']
    key=prepare(c,h,'copy','copy.txt',b'paper','b/copy.txt')
    response=append(c,h,batch,'copy',key);assert response.status_code==200,response.text
    status=c.get('/api/status').json()
    assert status['intake']['total']==2 and status['intake']['ready']==2
    assert status['intake']['unique_tasks']==1 and not status['intake']['open']
    assert len(status['articles'])==1


def test_new_batch_cannot_discard_an_open_manifest_and_failed_copy_is_visible(web_case):
    w,c,_=web_case;h=login(c);batch=begin(w,c,h)['batch_id']
    c.post('/api/pause',headers=h,json={'epoch':w.manager.epoch})
    wait_until(lambda:w.manager.status()['state']=='paused')
    another=select(c,h,name='next.txt',data=b'next')
    response=command(c,h,'/api/start',inputs=[another],request_id='other-batch')
    assert response.status_code==409
    assert c.get('/api/intake/'+batch).json()['summary']['pending']==2


def test_manifest_cannot_first_bind_a_same_size_historical_input(web_case):
    w,c,_=web_case;h=login(c)
    old=select(c,h,name='unrelated.txt',data=b'other')
    response=command(c,h,'/api/start',inputs=[old],request_id='stream-click',manifest=manifest(),first_item='paper')
    assert response.status_code==400 and not w.store.all_tasks()
    batch=begin(w,c,h)['batch_id']
    old=select(c,h,name='another.txt',data=b'1234567')
    response=append(c,h,batch,'drawing',old)
    assert response.status_code==400 and c.get('/api/status').json()['intake']['ready']==1


def test_last_append_and_settle_decision_are_serialized(web_case):
    w,c,_=web_case;h=login(c)
    entries=manifest()[:2];batch=begin(w,c,h,entries)['batch_id']
    key=prepare(c,h,'drawing','drawing.dwg',b'drawing')
    entered=threading.Event();release=threading.Event();original=w.scheduler.service.extend_selected_tasks
    def blocked(*a,**kw):entered.set();assert release.wait(4);return original(*a,**kw)
    w.scheduler.service.extend_selected_tasks=blocked
    results=[];thread=threading.Thread(target=lambda:results.append(append(c,h,batch,'drawing',key)))
    try:
        thread.start();assert entered.wait(3)
        with w.store.db() as db:db.execute("UPDATE scheduler_jobs SET state='complete'")
        time.sleep(.4)
        assert not w.manager.stopped.is_set(), 'last item cannot settle between SQL commit and selection extension'
    finally:release.set();thread.join(5)
    assert results[0].status_code==200,results[0].text


def test_continue_persists_intake_authorization_before_opening_admission(web_case):
    w,c,_=web_case;h=login(c);begin(w,c,h)
    c.post('/api/pause',headers=h,json={'epoch':w.manager.epoch});wait_until(lambda:w.manager.status()['state']=='paused')
    entered=threading.Event();release=threading.Event();original=w.intake.authorize_resume
    def blocked(*a,**kw):entered.set();assert release.wait(4);return original(*a,**kw)
    w.intake.authorize_resume=blocked
    results=[];thread=threading.Thread(target=lambda:results.append(command(c,h,'/api/continue')))
    try:
        thread.start();assert entered.wait(3)
        assert not w.manager.status()['admission_open']
        started=time.monotonic();c.post('/api/pause',headers=h,json={'epoch':w.manager.epoch})
        assert time.monotonic()-started<1
    finally:release.set();thread.join(5)
    assert results[0].status_code==409 and w.manager.stopped.is_set()


def test_conflicting_preparation_never_deletes_an_already_registered_input(web_case):
    from pathlib import Path
    w,c,_=web_case;h=login(c);begin(w,c,h)
    prepare(c,h,'drawing','drawing.dwg',b'drawing')
    response=c.post('/api/inputs/upload',params=dict(name='drawing.dwg',relative_path='folder/parts/drawing.dwg',
        request_id='stream-click',item_id='drawing'),content=b'changed',headers=h)
    assert response.status_code==400
    key=select(c,h,name='other.dwg',data=b'changed')
    with w.store.db() as db:row=db.execute('SELECT * FROM web_inputs WHERE id=?',(key,)).fetchone()
    assert Path(row['path']).read_bytes()==b'changed'


def test_restart_keeps_manifest_paused_and_rejects_previous_epoch(entry_case):
    from fastapi.testclient import TestClient
    from paperflow.workbench import Workbench
    from webworkbench.api import create_app
    from webworkbench.auth import create_admin
    root,old,_,config=entry_case;admin=root/'admin.json';create_admin(admin,'admin','isolated-workbench-password')
    first=Workbench(config,root,legacy_root=old)
    with TestClient(create_app(first,admin)) as c:
        h=login(c);batch=begin(first,c,h)['batch_id'];previous=first.manager.status()
        c.post('/api/pause',headers=h,json={'epoch':previous['epoch']})
    second=Workbench(config,root,legacy_root=old)
    with TestClient(create_app(second,admin)) as c:
        second.scheduler.launchers['graph']=lambda *a,**kw: (_ for _ in ()).throw(ValueError('synthetic worker'))
        h=login(c);status=c.get('/api/status').json()
        assert status['manager']['state']=='paused' and status['intake']['pending']==2
        assert status['manager']['epoch']!=previous['epoch']
        response=c.post('/api/intake/append',headers=h,json=dict(batch_id=batch,epoch=previous['epoch'],generation=previous['generation'],items=[dict(item_id='drawing',input_id='old')]))
        assert response.status_code==409
        assert command(c,h,'/api/continue').status_code==200
        key=prepare(c,h,'drawing','drawing.dwg',b'drawing')
        assert append(c,h,batch,'drawing',key).status_code==200


def test_append_keeps_fixed_production_deadline_and_excludes_old_tasks(entry_case):
    from pathlib import Path
    from fastapi.testclient import TestClient
    from paperflow.package import load
    from paperflow.workbench import Workbench
    from webworkbench.api import create_app
    from webworkbench.auth import create_admin
    from test_workbench_release import deployment_case
    root,old,config,deployment=deployment_case(entry_case)
    admin=root/'admin.json';create_admin(admin,'admin','isolated-workbench-password')
    w=Workbench(config,root,legacy_root=old,deployment=deployment)
    with TestClient(create_app(w,admin)) as c:
        h=login(c);old_key=select(c,h,name='old.txt',data=b'old historical selection')
        with w.store.db() as db:r=db.execute('SELECT * FROM web_inputs WHERE id=?',(old_key,)).fetchone()
        task=w.store.register_source(r['path'],r['sha256']);service=w.scheduler.service.services['llm']['service_id']
        old_job=w.scheduler.ledger.enqueue('graph',task['source_id'],task['task_id'],dict(task_id=task['task_id']),service)
        batch=begin(w,c,h)['batch_id'];before=load(config.production_pilot)
        key=prepare(c,h,'drawing','drawing.dwg',b'drawing');assert append(c,h,batch,'drawing',key).status_code==200
        after=load(config.production_pilot)
        assert before['expires_at']==after['expires_at'] and len(after['sources'])==2
        assert all(Path(r['path']).is_relative_to(root/'uploads') for r in after['sources'])
        assert task['task_id'] not in w.scheduler.service.selected_tasks
        assert not w.scheduler.ledger.rows('SELECT * FROM scheduler_attempts WHERE job_id=?',(old_job,))


def test_replaying_started_manifest_returns_receipt_and_never_resumes_it(web_case):
    w,c,_=web_case;h=login(c);batch=begin(w,c,h)['batch_id']
    with w.store.db() as db:key=json.loads(db.execute('SELECT selection FROM web_batches WHERE id=?',(batch,)).fetchone()[0])[0]
    c.post('/api/pause',headers=h,json={'epoch':w.manager.epoch});wait_until(lambda:w.manager.status()['state']=='paused')
    response=command(c,h,'/api/start',inputs=[key],request_id='stream-click',manifest=manifest(),first_item='paper')
    assert response.status_code==200 and response.json()['batch_id']==batch
    assert response.json().get('intake',{}).get('request_id')=='stream-click'
    assert w.manager.stopped.is_set()
