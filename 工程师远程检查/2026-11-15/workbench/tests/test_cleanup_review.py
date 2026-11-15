import json
from pathlib import Path
import sys
import threading
from types import SimpleNamespace

import pytest

from paperflow.cleanup_control import CleanupClient,CleanupCommits
from paperflow.cleanup_store import CleanupLedger
from paperflow.handoff import PackageHandoff
from paperflow.handoff_store import HandoffLedger,canonical
from paperflow.package import save
from paperflow.repair_store import RepairLedger
from paperflow.scheduler import Scheduler
from paperflow.scheduling_policy import ActionDeferred,fault
from test_control import manager
from test_delivery_state import delivery_case,owner
from test_delivery_flow import advance
from test_cleanup_store import retired
from test_cleanup_control import controlled
from test_scheduling_retry import failing_http,Actions
from test_winjob import wait_until


@pytest.mark.parametrize('first',['delete','repair'])
def test_remote_delete_and_repair_reference_are_mutually_exclusive(delivery_case,tmp_path,first):
    case,_=retired(delivery_case,tmp_path);m,c,t,_,commits,*_=case
    ledger=CleanupLedger(m.store,c);ledger.discover_remote();item=ledger.items()[0]
    identity=dict(json.loads(commits.ledger.registration(delivery_case[3])['identity_json']),manifest_sha256=delivery_case[3])
    repair=RepairLedger(m.store)
    def request():return ledger._write(lambda db:repair.request_sql(db,identity,'round3_chunk','chunks_missing','120202',{'kind':'late defect'}))
    def begin():return ledger._write(lambda db:ledger.begin_sql(db,item['item_id'],now=item['eligible_at']))
    if first=='delete':
        begin()
        with pytest.raises(ActionDeferred,match='deleting'):request()
        assert not ledger.rows('SELECT * FROM repair_requests') and repair.used(identity)==0
        # Another specialty's independent active pointer does not refer to A's remote object.
        ledger._write(lambda db:HandoffLedger._reference(db,delivery_case[3],'active_specialty',canonical([t['source_id'],'120202']),'package',delivery_case[3]))
    else:
        request()
        with pytest.raises(ValueError,match='reference'):begin()
        assert ledger.item(item['item_id'])['state']=='waiting'


@pytest.mark.parametrize('waiting_kind',['cleanup','delivery'])
def test_specialty_contention_defers_queue_without_spending_budget(delivery_case,tmp_path,monkeypatch,waiting_kind):
    case,_=retired(delivery_case,tmp_path);m,c,t,d,*_=case
    c.min_free_gib=0;c.max_memory_percent=100
    report=tmp_path/'acceptance.json';save(report,dict(passed=True,endpoint=c.rag_url));c.delivery_acceptance_report=str(report)
    s=Scheduler(m,c,isolation_root=tmp_path);s.manager.cleanup.ledger.discover_remote();item=s.manager.cleanup.ledger.items()[0]
    with m.store.db() as db:db.execute('UPDATE cleanup_items SET eligible_at=0')
    if waiting_kind=='cleanup':
        ex,_=owner(case,tmp_path)
        key='cleanup:'+s.manager.cleanup.key(item);payload=dict(item_id=item['item_id'],resource_kind='remote')
    else:
        _,_,ex=controlled(case,tmp_path)
        case=advance(case);d=case[3]  # An unfinished newer target, not the already active version.
        identity=m.delivery.ledger.context(d,'120201')['identity']
        key=canonical([t['source_id'],'120201']);payload=dict(manifest_sha256=d,specialty_code='120201',generation=identity['generation'])
    job=s.ledger.enqueue(waiting_kind,t['source_id'],key,payload,s.service.services['rag']['service_id'])
    monkeypatch.setattr(s,'discover',lambda:None)
    snapshot=m.snapshot(ex.execution_id,'local');s.tick()
    assert s.ledger.job(job)['state']=='retry'
    assert m._valid(snapshot,ex.execution_id) and s.ledger.budget(job)['rounds']==0
    m.cancel_unit(ex.execution_id,'fixture owner release');wait_until(lambda:ex.exit_saved)
    with m.store.db() as db:db.execute('UPDATE scheduler_jobs SET due=0 WHERE job_id=?',(job,))
    claimed=s.ledger.claim(kinds=(waiting_kind,));assert claimed['job_id']==job
    s.ledger.finish(claimed['attempt_id'],'deferred',{'reason':'fixture completed'})


def test_cleanup_native_retry_after_is_preserved(failing_http):
    native,server=failing_http;actions=Actions();client=CleanupClient(native,actions)
    with pytest.raises(Exception) as error:client.request('GET','/datasets')
    assert server.calls==1 and actions.permits==['rag_read']
    assert fault(error.value)['retry_after']==45


def test_cleanup_native_authentication_is_service_configuration():
    import io
    sys.path.insert(0,r'E:\uv\ragflow_upstream')
    from ragflow_client import RagflowClient
    class Opener:
        def open(self,*a,**kw):return io.BytesIO(b'{"code":109,"message":"fixture authentication"}')
    native=RagflowClient('http://127.0.0.1:1','synthetic-key');native.opener=Opener()
    client=CleanupClient(native,Actions())
    with pytest.raises(Exception) as error:client.request('GET','/datasets')
    assert fault(error.value)['category']=='configuration'


def orphan(case,ledger,thread):
    m,c,t,_,_,material,catalog=case;handoff=PackageHandoff(m.store,c.handoff_root)
    intent=handoff.prepare(thread,dict(t,revision=3),material,catalog,mapping_version=thread)
    package=handoff.publish(thread,intent['input_fingerprint'])
    ledger.close_run(thread,t['task_id'],'abandoned',reason='isolated fixture')
    return intent,package


def recovery(case,thread,path):
    m,c,t,_,_,material,catalog=case;handoff=PackageHandoff(m.store,c.handoff_root)
    return handoff.prepare(thread,dict(t,revision=4),path,catalog,mapping_version=thread)


def test_orphan_retention_starts_after_last_recovery_reference_is_gone(delivery_case,monkeypatch):
    from paperflow import cleanup_store
    m,c,t,*_=delivery_case;ledger=CleanupLedger(m.store,c);c.orphan_package_retention_seconds=60
    now=[1000];monkeypatch.setattr(cleanup_store,'time',SimpleNamespace(time=lambda:now[0]))
    intent,path=orphan(delivery_case,ledger,'old-orphan')
    recovery(delivery_case,'other-recovery',path)
    with pytest.raises(ValueError,match='recoverable|closed'):ledger.plan_publication('old-orphan',intent['input_fingerprint'])
    now[0]=1600;ledger.close_run('other-recovery',t['task_id'],'abandoned',reason='recovery explicitly abandoned')
    item=ledger.plan_publication('old-orphan',intent['input_fingerprint'])
    assert item['created']==1600 and item['eligible_at']==1660
    now[0]=1610
    assert ledger.plan_publication('old-orphan',intent['input_fingerprint'])['eligible_at']==1660
    with pytest.raises(ValueError,match='retention'):ledger._write(lambda db:ledger.begin_sql(db,item['item_id']))


def test_orphan_scan_rotates_past_protected_publications(delivery_case):
    m,c,t,*_=delivery_case;ledger=CleanupLedger(m.store,c);c.orphan_package_retention_seconds=60
    for n in range(65):
        intent,path=orphan(delivery_case,ledger,f'orphan-{n}')
        if n<64:recovery(delivery_case,f'recovery-{n}',path)
    for _ in range(3):ledger.discover_local()
    assert any(json.loads(r['resource']).get('path')==str(path.resolve()) for r in ledger.items())


def test_run_closure_scan_rotates_past_unfinished_publications(delivery_case):
    m,c,t,d,commits,material,catalog=delivery_case;ledger=CleanupLedger(m.store,c)
    handoff=PackageHandoff(m.store,c.handoff_root);m.store.update(t['task_id'],stage='handed_off')
    # A run with one handed-off intent and another recoverable intent must stay open.
    base=ledger.rows('SELECT * FROM publication_intents')[0]
    with m.store.db() as db:
        for n in range(65):
            thread=f'run-{n}'
            data=json.loads(base['data']);data['thread_id']=thread
            columns=list(base);values=[dict(base,thread_id=thread,data=canonical(data))[key] for key in columns]
            db.execute('INSERT INTO publication_intents('+','.join(columns)+') VALUES('+','.join('?' for _ in columns)+')',values)
            if n<64:
                values=[dict(base,thread_id=thread,input_fingerprint='pending',lifecycle='open',data=canonical(data))[key] for key in columns]
                db.execute('INSERT INTO publication_intents('+','.join(columns)+') VALUES('+','.join('?' for _ in columns)+')',values)
    for _ in range(3):ledger.discover_local()
    assert ledger.rows('SELECT * FROM cleanup_runs WHERE thread_id=?',('run-64',))


def test_commit_rpc_preserves_deferred_classification():
    from paperflow.control_ipc import Peer
    peer=object.__new__(Peer);messages=[]
    def defer(*args):raise ActionDeferred('remote version deleting')
    peer.server=SimpleNamespace(manager=SimpleNamespace(handle=defer));peer.send=messages.append
    peer.graph_commit_lock=threading.Lock();peer.graph_commit_lock.acquire()
    peer.graph_commit({'id':'fixture'})
    assert messages[0].get('deferred')=='remote version deleting'
