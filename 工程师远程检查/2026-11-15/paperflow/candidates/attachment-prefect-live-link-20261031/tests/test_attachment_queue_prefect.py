import asyncio
import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from test_registered_prepare import registered


@pytest.fixture
def waiting_pool(registered, tmp_path):
    group, receipt_root, receipt, item = registered
    entry = next(row for row in receipt['items'] if row['kind'] == 'attachment')
    record = {key: item[key] for key in ('group_id', 'paper_id', 'receipt_id', 'receipt_path', 'delivery_dir')}
    record.update(task_id='test-task', receipt_sha256=hashlib.sha256(Path(item['receipt_path']).read_bytes()).hexdigest(),
                  attachments=[{key: entry[key] for key in ('file_id', 'sha256', 'bytes', 'path')}])
    db = tmp_path/'queue.sqlite3'
    with sqlite3.connect(db) as conn:
        conn.executescript('''
          CREATE TABLE groups(task_id TEXT, group_id TEXT, paper_id TEXT, receipt_id TEXT, state TEXT, record TEXT, checked_session TEXT);
          CREATE TABLE retention_requests(task_id TEXT, group_id TEXT, paper_id TEXT, receipt_id TEXT,
                                          consumer_id TEXT, state TEXT, retention_id TEXT, last_error TEXT, checked_session TEXT);
          CREATE TABLE claims(state TEXT);
          CREATE TABLE metadata(key TEXT, value TEXT);
        ''')
        conn.execute('INSERT INTO groups VALUES (?,?,?,?,?,?,?)',
                     ('test-task', record['group_id'], record['paper_id'], record['receipt_id'], 'waiting', json.dumps(record), 'session-1'))
        conn.execute('INSERT INTO retention_requests VALUES (?,?,?,?,?,?,?,?,?)',
                     ('test-task', record['group_id'], record['paper_id'], record['receipt_id'],
                      'local-intake', 'accepted', 'protection-1', None, 'session-1'))
        conn.executemany('INSERT INTO metadata VALUES (?,?)', [('receiver_session_id','session-1'),
                         ('receiver_status','running'),('receiver_heartbeat_at',datetime.now(timezone.utc).isoformat())])
    return db, receipt_root, record, receipt


def test_authoritative_queue_is_read_without_claiming_body(waiting_pool):
    from paperflow_prefect.attachment_queue import AttachmentQueue
    db, root, record, receipt = waiting_pool
    queue = AttachmentQueue(db, root)
    assert [row['receipt_id'] for row in queue.eligible()] == [record['receipt_id']]
    result = queue.verified(record['receipt_id'])
    assert result['paper_id'] == record['paper_id']
    assert Path(result['library_root']) == Path(receipt['library_root'])
    assert result['receipt_path'] == record['receipt_path']
    with sqlite3.connect(db) as conn:
        assert conn.execute('SELECT state FROM groups').fetchone()[0] == 'waiting'
        assert conn.execute('SELECT COUNT(*) FROM claims').fetchone()[0] == 0


def test_queue_rejects_missing_upstream_protection_or_untrusted_receipt_root(waiting_pool, tmp_path):
    from paperflow_prefect.attachment_queue import AttachmentQueue
    db, root, record, _ = waiting_pool
    queue = AttachmentQueue(db, root)
    assert queue.verified(record['receipt_id'])['paper_id'] == record['paper_id']
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE retention_requests SET state='reserved'")
    assert queue.eligible() == []
    with pytest.raises(ValueError): queue.verified(record['receipt_id'])
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE retention_requests SET state='accepted'")
    other = tmp_path/'other'; other.mkdir()
    with pytest.raises(ValueError): AttachmentQueue(db, other).verified(record['receipt_id'])


def test_prefect_idempotency_survives_unknown_submit_and_repeated_scan(waiting_pool, tmp_path):
    from paperflow_prefect.attachment_queue import AttachmentQueue, schedule_ready
    db, root, record, _ = waiting_pool
    queue = AttachmentQueue(db, root)
    sent = []; server = {}
    async def submit(deployment_id, *, parameters, idempotency_key):
        sent.append((deployment_id, parameters, idempotency_key))
        run = server.setdefault(idempotency_key, 'synthetic-run-1')
        if len(sent) == 1: raise TimeoutError('response lost after create')
        return run
    async def observe(run_id): return 'RUNNING'
    journal = tmp_path/'attachment-dispatch'
    first = asyncio.run(schedule_ready(queue, journal, 'deployment-a', submit, observe))
    assert first['submitted'] == 0 and first['failed'] == 1
    second = asyncio.run(schedule_ready(queue, journal, 'deployment-a', submit, observe))
    assert second['submitted'] == 1 and second['failed'] == 0
    third = asyncio.run(schedule_ready(queue, journal, 'deployment-a', submit, observe))
    assert third['submitted'] == 0 and third['already_submitted'] == 1
    assert len(server) == 1 and len(sent) == 2 and sent[0] == sent[1]
    assert sent[0][1] == {'receipt_id': record['receipt_id'], 'run_mode': 'process'}
    with sqlite3.connect(db) as conn:
        assert conn.execute('SELECT state FROM groups').fetchone()[0] == 'waiting'


def test_failed_item_does_not_prevent_another_receipt_scheduling(waiting_pool, tmp_path):
    from paperflow_prefect.attachment_queue import AttachmentQueue, schedule_ready
    db, root, record, _ = waiting_pool
    broken = copy.deepcopy(record)
    broken['receipt_id'] = 'f'*64
    broken['receipt_path'] = str(root/'receipts'/(broken['receipt_id']+'.json'))
    broken['group_id'] = 'broken-group'; broken['paper_id'] = 'broken-paper'
    with sqlite3.connect(db) as conn:
        conn.execute('INSERT INTO groups VALUES (?,?,?,?,?,?,?)',
            ('test-task', broken['group_id'], broken['paper_id'], broken['receipt_id'], 'waiting', json.dumps(broken),'session-1'))
        conn.execute('INSERT INTO retention_requests VALUES (?,?,?,?,?,?,?,?,?)',
            ('test-task', broken['group_id'], broken['paper_id'], broken['receipt_id'], 'local-intake','accepted','protect-2',None,'session-1'))
    calls=[]
    async def submit(deployment_id, *, parameters, idempotency_key):
        calls.append(parameters['receipt_id']); return 'synthetic-run'
    async def observe(run_id): return 'RUNNING'
    result=asyncio.run(schedule_ready(AttachmentQueue(db,root),tmp_path/'dispatch','deployment-a',submit,observe))
    assert calls == [record['receipt_id']]
    assert result['submitted'] == 1 and result['failed'] == 1


def test_failed_prefect_run_gets_new_attempt_but_unknown_state_does_not(waiting_pool, tmp_path):
    from paperflow_prefect.attachment_queue import AttachmentQueue, schedule_ready
    db, root, record, _ = waiting_pool
    queue=AttachmentQueue(db,root); sent=[]; states={}
    async def submit(deployment_id, *, parameters, idempotency_key):
        sent.append(idempotency_key); run_id='run-'+str(len(sent)); states[run_id]='RUNNING'; return run_id
    async def observe(run_id): return states[run_id]
    journal=tmp_path/'dispatch'
    assert asyncio.run(schedule_ready(queue,journal,'deployment-a',submit,observe))['submitted']==1
    states['run-1']={'state':'FAILED','retryable':True,'run_id':'run-1','receipt_id':record['receipt_id']}
    assert asyncio.run(schedule_ready(queue,journal,'deployment-a',submit,observe))['submitted']==1
    assert len(sent)==2 and sent[0]!=sent[1]
    states['run-2']={'state':'CRASHED','retryable':False,'run_id':'run-2','receipt_id':record['receipt_id']}
    result=asyncio.run(schedule_ready(queue,journal,'deployment-a',submit,observe))
    assert result['failed']==1 and len(sent)==2


def test_bad_queue_record_does_not_hide_other_papers(waiting_pool, tmp_path):
    from paperflow_prefect.attachment_queue import AttachmentQueue, schedule_ready
    db,root,record,_=waiting_pool
    with sqlite3.connect(db) as conn:
        conn.execute('INSERT INTO groups VALUES (?,?,?,?,?,?,?)',
                     ('test-task','bad-group','bad-paper','e'*64,'waiting','{broken','session-1'))
        conn.execute('INSERT INTO retention_requests VALUES (?,?,?,?,?,?,?,?,?)',
                     ('test-task','bad-group','bad-paper','e'*64,'local-intake','accepted','p',None,'session-1'))
    sent=[]
    async def submit(deployment_id, *, parameters, idempotency_key):
        sent.append(parameters['receipt_id']); return 'run'
    async def observe(run_id): return 'RUNNING'
    result=asyncio.run(schedule_ready(AttachmentQueue(db,root),tmp_path/'dispatch','deployment-a',submit,observe))
    assert sent==[record['receipt_id']]
    assert result['submitted']==1 and result['failed']==1


def test_receiver_heartbeat_and_retention_must_be_current_at_execution(waiting_pool):
    from paperflow_prefect.attachment_queue import AttachmentQueue
    db,root,record,_=waiting_pool;queue=AttachmentQueue(db,root)
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE metadata SET value='2000-01-01T00:00:00+00:00' WHERE key='receiver_heartbeat_at'")
    with pytest.raises(ValueError): queue.verified(record['receipt_id'])
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE metadata SET value=? WHERE key='receiver_heartbeat_at'",(datetime.now(timezone.utc).isoformat(),))
        conn.execute("UPDATE retention_requests SET checked_session='old-session'")
    with pytest.raises(ValueError): queue.verified(record['receipt_id'])


def test_busy_paper_is_skipped_until_existing_processing_finishes(waiting_pool, tmp_path):
    from filelock import FileLock
    from paperflow_prefect.attachment_queue import AttachmentQueue, schedule_ready, paper_is_available
    from paperflow_prefect.records import identity
    db,root,record,receipt=waiting_pool
    queue=AttachmentQueue(db,root); observed=queue.verified(record['receipt_id'])
    data=tmp_path/'data';(data/'locks').mkdir(parents=True)
    lock=data/'locks'/(identity(str(Path(observed['paper']['person_dir']).resolve()).casefold())+'.lock')
    sent=[]
    async def submit(deployment_id, *, parameters, idempotency_key):
        sent.append(parameters); return 'run-1'
    async def observe(run_id): return 'RUNNING'
    def available(entry): return paper_is_available(entry,data)
    with FileLock(lock,timeout=0):
        result=asyncio.run(schedule_ready(queue,tmp_path/'dispatch','deployment',submit,observe,available=available))
        assert result['busy']==1 and sent==[]
    result=asyncio.run(schedule_ready(queue,tmp_path/'dispatch','deployment',submit,observe,available=available))
    assert result['submitted']==1 and len(sent)==1


def test_source_root_override_only_uses_verified_registered_queue_record(waiting_pool, tmp_path, monkeypatch):
    from paperflow_prefect import attachment_queue
    from paperflow_prefect.settings import Settings
    db, root, record, receipt = waiting_pool
    settings=Settings({'library_root':str(tmp_path/'different-source'), 'data_root':str(tmp_path/'data'),
                       'execution_enabled':True})
    observed=attachment_queue.AttachmentQueue(db,root).verified(record['receipt_id'])
    resolved=attachment_queue.settings_for_verified_receipt(settings, observed)
    assert resolved.path('library_root') == Path(receipt['library_root']).resolve()
    assert settings.path('library_root') != resolved.path('library_root')
    assert resolved.path('data_root') == settings.path('data_root')


def test_irrelevant_trash_copy_cannot_change_attachment_receipt_state(tmp_path):
    import subprocess
    import sys
    candidates=Path(__file__).resolve().parents[2]
    script="""
import json,sys
from pathlib import Path
sys.path[:0]=sys.argv[1:3]
from paperflow_prefect.first_step_registration import register_delivery
from paperflow_prefect.group_classification import process_one_group
from test_first_step_registration import FixtureJudge,evidence
base=Path(sys.argv[3]); group=base/'library'/'person';group.mkdir(parents=True)
(group/'paper.txt').write_text('摘要\\n测试正文',encoding='utf8')
(group/'data.csv').write_text('x,y\\n1,2',encoding='utf8')
(group/'开题报告.rtf').write_text('开题报告\\n合成过程材料',encoding='utf8')
delivery=process_one_group(group,group.parent,base/'task',FixtureJudge,evidence,verified_new=True)
receipt=register_delivery(delivery['delivery_dir'],receipt_root=base/'receipts',history_roots=(),lock_path=base/'receipt.lock')
trash=Path(delivery['delivery_dir'])/'files'/'开题报告.rtf';assert trash.is_relative_to(base);trash.write_text('changed',encoding='utf8')
print(json.dumps({'receipt':receipt['record_path']},ensure_ascii=False))
"""
    proc=subprocess.run([sys.executable,'-B','-X','utf8','-c',script,
        str(candidates/'first-step-control-20261029'),
        str(candidates/'first-step-auto-registration-20261030'/'tests'),str(tmp_path)],
        encoding='utf8',capture_output=True,timeout=30)
    assert proc.returncode==0,proc.stdout+proc.stderr
    from paperflow_prefect.stage3_handoff import read_receipt
    receipt,paper=read_receipt(json.loads(proc.stdout)['receipt'])
    assert receipt['state']=='已接收'
    assert len(paper['attachments'])==1


def test_prefect_attachment_job_rechecks_queue_and_uses_registered_source(waiting_pool, tmp_path, monkeypatch):
    from paperflow_prefect.settings import Settings
    db,root,record,receipt=waiting_pool
    settings=Settings({'library_root':str(tmp_path/'different-source'), 'data_root':str(tmp_path/'data'),
                       'execution_enabled':True,'prefect_api_url':'http://127.0.0.1:9/api'})
    monkeypatch.setattr(Settings,'load',classmethod(lambda cls,*args,**kwargs: settings))
    from paperflow_prefect import attachment_prefect_flows as attached
    monkeypatch.setattr(attached,'queue_settings',lambda: {'queue_db':str(db),'receipt_root':str(root),
                                                          'journal_root':str(tmp_path/'dispatch')})
    calls=[]
    async def fake_handoff(actual, receipt_path, *, attachments_only):
        calls.append((actual.path('library_root'),receipt_path,attachments_only))
        return {'status':'stage3_complete'}
    monkeypatch.setattr(attached.flows,'received_handoff_with_settings',fake_handoff)
    result=asyncio.run(attached.attachment_job.fn(record['receipt_id'],run_mode='process'))
    assert result['status']=='stage3_complete'
    assert calls==[(Path(receipt['library_root']).resolve(),record['receipt_path'],True)]
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE retention_requests SET state='reserved'")
    with pytest.raises(ValueError):
        asyncio.run(attached.attachment_job.fn(record['receipt_id'],run_mode='process'))
    assert len(calls)==1


def test_incomplete_attachment_business_result_fails_prefect_job(waiting_pool, tmp_path, monkeypatch):
    from paperflow_prefect.settings import Settings
    db,root,record,_=waiting_pool
    settings=Settings({'library_root':str(tmp_path/'source'), 'data_root':str(tmp_path/'data'),
                       'execution_enabled':True,'prefect_api_url':'http://127.0.0.1:9/api'})
    monkeypatch.setattr(Settings,'load',classmethod(lambda cls,*args,**kwargs: settings))
    from paperflow_prefect import attachment_prefect_flows as attached
    monkeypatch.setattr(attached,'queue_settings',lambda:{'queue_db':str(db),'receipt_root':str(root),
                                                         'journal_root':str(tmp_path/'dispatch')})
    async def incomplete(*args, **kwargs): return {'status':'incomplete'}
    monkeypatch.setattr(attached.flows,'received_handoff_with_settings',incomplete)
    with pytest.raises(RuntimeError):
        asyncio.run(attached.attachment_job.fn(record['receipt_id'],run_mode='process'))


def test_only_paper_lock_failure_is_marked_safe_for_retry(waiting_pool, tmp_path, monkeypatch):
    from filelock import Timeout
    from paperflow_prefect.settings import Settings
    db,root,record,_=waiting_pool
    settings=Settings({'library_root':str(tmp_path/'source'), 'data_root':str(tmp_path/'data'),
                       'execution_enabled':True,'prefect_api_url':'http://127.0.0.1:9/api'})
    monkeypatch.setattr(Settings,'load',classmethod(lambda cls,*args,**kwargs: settings))
    from paperflow_prefect import attachment_prefect_flows as attached
    monkeypatch.setattr(attached,'queue_settings',lambda:{'queue_db':str(db),'receipt_root':str(root),
                                                         'journal_root':str(tmp_path/'dispatch')})
    marked=[]
    monkeypatch.setattr(attached,'record_retryable_before_request',lambda receipt_id:marked.append(receipt_id))
    async def inner_lock_failure(*args, **kwargs): raise Timeout('later-render-lock')
    monkeypatch.setattr(attached.flows,'received_handoff_with_settings',inner_lock_failure)
    with pytest.raises(Timeout):
        asyncio.run(attached.attachment_job.fn(record['receipt_id'],run_mode='process'))
    assert marked==[]
    async def paper_lock_failure(*args, **kwargs): raise attached.flows.ReceivedPaperBusy('paper')
    monkeypatch.setattr(attached.flows,'received_handoff_with_settings',paper_lock_failure)
    with pytest.raises(attached.flows.ReceivedPaperBusy):
        asyncio.run(attached.attachment_job.fn(record['receipt_id'],run_mode='process'))
    assert marked==[record['receipt_id']]


def test_clean_process_initializes_prefect_from_selected_configuration():
    import os
    import subprocess
    import sys
    env=os.environ.copy()
    for key in ('PREFECT_API_URL','PREFECT_HOME','PREFECT_SERVER_ALLOW_EPHEMERAL_MODE','PREFECT_API_KEY'):
        env.pop(key,None)
    candidate=Path(__file__).resolve().parents[1]
    script="""
import json,os,sys
from pathlib import Path
p=Path(sys.argv[1]);p.write_text(json.dumps({'prefect_api_url':'http://127.0.0.1:48123/api'}),encoding='utf8')
os.environ['PAPERFLOW_CONFIG']=str(p)
import manage_attachment
from prefect.settings import get_current_settings
assert str(get_current_settings().api.url)=='http://127.0.0.1:48123/api'
print('configured')
"""
    import tempfile
    with tempfile.TemporaryDirectory() as folder:
        proc=subprocess.run([sys.executable,'-B','-X','utf8','-c',script,str(Path(folder)/'config.json')],
            cwd=candidate,env=env,encoding='utf8',capture_output=True,timeout=30)
    assert proc.returncode==0,proc.stdout+proc.stderr
    assert 'configured' in proc.stdout


def test_running_attachment_executor_detects_changed_configuration(tmp_path):
    import os
    import subprocess
    import sys
    candidate=Path(__file__).resolve().parents[1]
    config=tmp_path/'config.json'
    config.write_text(json.dumps({'prefect_api_url':'http://127.0.0.1:48123/api'}),encoding='utf8')
    env=os.environ.copy();env['PAPERFLOW_CONFIG']=str(config)
    script="""
from pathlib import Path
import sys
from paperflow_prefect import attachment_prefect_flows as f
f.runtime_identity()
p=Path(sys.argv[1]);p.write_text(p.read_text(encoding='utf8')+' ',encoding='utf8')
try: f.runtime_identity()
except RuntimeError: print('change detected')
else: raise AssertionError('未发现加载后的运行配置变更')
"""
    proc=subprocess.run([sys.executable,'-B','-X','utf8','-c',script,str(config)],
                        cwd=candidate,env=env,encoding='utf8',capture_output=True,timeout=30)
    assert proc.returncode==0,proc.stdout+proc.stderr
    assert 'change detected' in proc.stdout


def test_scan_failure_is_saved_and_not_reported_as_prefect_success(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from paperflow_prefect.settings import Settings
    monkeypatch.setattr(Settings,'load',classmethod(lambda cls,*args,**kwargs:
        Settings({'data_root':str(tmp_path/'data'),'prefect_api_url':'http://127.0.0.1:9/api'})))
    from paperflow_prefect import attachment_prefect_flows as attached
    monkeypatch.setattr(attached,'PROJECT',tmp_path)
    monkeypatch.setattr(attached,'runtime_identity',lambda:{'disk_matches_loaded':True})
    monkeypatch.setattr(attached,'queue_settings',lambda:{'journal_root':str(tmp_path/'dispatch')})
    monkeypatch.setattr(attached,'checked_queue',lambda:object())
    class Client:
        async def __aenter__(self): return self
        async def __aexit__(self,*args): pass
        async def read_deployment_by_name(self,name): return SimpleNamespace(id='deployment')
    monkeypatch.setattr(attached,'get_client',lambda:Client())
    async def failed(*args,**kwargs): return {'submitted':1,'failed':1,'errors':[{'error_type':'ValueError'}]}
    monkeypatch.setattr(attached,'schedule_ready',failed)
    with pytest.raises(RuntimeError): asyncio.run(attached.attachment_scan.fn(run_mode='process'))
    result=json.loads((tmp_path/'runtime'/'attachment-scan-last.json').read_text(encoding='utf8'))
    assert result['status']=='incomplete' and result['failed']==1 and result['submitted']==1


def test_scan_exception_still_saves_failure_summary(tmp_path, monkeypatch):
    from paperflow_prefect.settings import Settings
    monkeypatch.setattr(Settings,'load',classmethod(lambda cls,*args,**kwargs:
        Settings({'data_root':str(tmp_path/'data'),'prefect_api_url':'http://127.0.0.1:9/api'})))
    from paperflow_prefect import attachment_prefect_flows as attached
    monkeypatch.setattr(attached,'PROJECT',tmp_path)
    monkeypatch.setattr(attached,'runtime_identity',lambda:{'disk_matches_loaded':True})
    monkeypatch.setattr(attached,'queue_settings',lambda:(_ for _ in ()).throw(OSError('synthetic')))
    with pytest.raises(OSError): asyncio.run(attached.attachment_scan.fn(run_mode='process'))
    result=json.loads((tmp_path/'runtime'/'attachment-scan-last.json').read_text(encoding='utf8'))
    assert result['status']=='incomplete' and result['errors']==[{'error_type':'OSError'}]


def test_runner_records_actual_pid_and_rejects_duplicate(tmp_path):
    import os
    import subprocess
    import sys
    candidate=Path(__file__).resolve().parents[1]
    config=tmp_path/'config.json'
    config.write_text(json.dumps({'prefect_api_url':'http://127.0.0.1:48123/api'}),encoding='utf8')
    env=os.environ.copy();env['PAPERFLOW_CONFIG']=str(config)
    script="""
import json,sys
from pathlib import Path
from filelock import FileLock,Timeout
import manage_attachment as m
m.PROJECT=Path(sys.argv[1]);m.runtime_identity=lambda:{'source_sha256':'fixture'}
m.serve=lambda *args,**kwargs:None
m.run_runner(object(),object())
p=m.PROJECT/'runtime'/'runner-process.json';value=json.loads(p.read_text(encoding='utf8'))
assert value['pid']>0 and value['runtime']['source_sha256']=='fixture'
with FileLock(m.PROJECT/'runtime'/'runner.lock',timeout=0):
    try:m.run_runner(object(),object())
    except Timeout:pass
    else:raise AssertionError('重复执行器未阻止')
print('runner checked')
"""
    proc=subprocess.run([sys.executable,'-B','-X','utf8','-c',script,str(tmp_path)],
                        cwd=candidate,env=env,encoding='utf8',capture_output=True,timeout=30)
    assert proc.returncode==0,proc.stdout+proc.stderr
    assert 'runner checked' in proc.stdout


def test_check_flow_verifies_prefect_limits_without_running_a_model(tmp_path, monkeypatch):
    from paperflow_prefect.settings import Settings
    settings=Settings({'prefect_api_url':'http://127.0.0.1:9/api'})
    monkeypatch.setattr(Settings,'load',classmethod(lambda cls,*args,**kwargs:settings))
    from paperflow_prefect import attachment_prefect_flows as attached
    monkeypatch.setattr(attached,'PROJECT',tmp_path)
    monkeypatch.setattr(attached,'runtime_identity',lambda:{'disk_matches_loaded':True})
    class Queue:
        def eligible(self): return [{'receipt_id':'x'}, {'_invalid':True}]
    monkeypatch.setattr(attached,'checked_queue',lambda:Queue())
    verified=[]
    monkeypatch.setattr(attached.flows,'verify_limits',lambda actual:verified.append(actual))
    result=asyncio.run(attached.check_runtime())
    assert verified==[settings]
    assert result['prefect_limits_checked'] is True and result['real_model_requests']==0
    assert result['eligible_groups_with_attachments']==1 and result['invalid_queue_records']==1


def test_attachment_model_dispatch_reuses_current_cancellable_prefect_slots(monkeypatch, tmp_path):
    from contextlib import contextmanager
    from threading import Event
    from types import SimpleNamespace
    import sys
    from paperflow_prefect.settings import Settings
    monkeypatch.setattr(Settings,'load',classmethod(lambda cls,*args,**kwargs:
        Settings({'prefect_api_url':'http://127.0.0.1:9/api','limits':{'total_llm':50,'attachment_llm':20}})))
    from paperflow_prefect import flows
    observed=[];stop=Event()
    @contextmanager
    def cancellable(names, actual_stop, *, expected):
        observed.append((names,actual_stop,expected));yield
    def forbid_old(*args, **kwargs): raise AssertionError('必须使用现有可取消的名额等待')
    def request_once(body, connection, work, *, dispatch, **kwargs):
        return dispatch(lambda:'sent',None)
    monkeypatch.setitem(sys.modules,'preextract_request',SimpleNamespace(request_once=request_once))
    monkeypatch.setattr(flows,'_LOADED_LIMITS',{'total_llm':50,'attachment_llm':20},raising=False)
    monkeypatch.setattr(flows,'cancellable_concurrency',cancellable,raising=False)
    monkeypatch.setattr(flows,'concurrency',forbid_old)
    assert flows.invoke_model('attachment',{}, {},tmp_path,stop=stop)=='sent'
    assert observed==[(['paperflow-connected-total_llm','paperflow-connected-attachment_llm'],stop,
                      {'paperflow-connected-total_llm':50,'paperflow-connected-attachment_llm':20})]


def test_prefect_queue_to_attachment_model_end_to_end_offline(registered, tmp_path):
    import subprocess
    import sys
    group,_,receipt,item=registered
    helper=Path(__file__).parent/'fixtures'/'unified_attachment_offline.py'
    payload={'group':str(group),'tmp_path':str(tmp_path),'receipt':receipt,'item':item,
             'use_prefect_queue_entry':True}
    proc=subprocess.run([sys.executable,'-B','-X','utf8',str(helper)],
        input=json.dumps(payload,ensure_ascii=False),encoding='utf8',capture_output=True,timeout=30)
    assert proc.returncode==0,proc.stdout+proc.stderr
    assert json.loads(proc.stdout)=={'attachment_requests':1,'status':'stage3_complete'}
