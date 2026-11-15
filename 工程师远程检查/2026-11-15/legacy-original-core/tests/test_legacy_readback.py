import copy
import json
import uuid
import pytest

from paperflow.handoff_store import canonical
from paperflow.legacy_readback import readback,commit_readback,article_status
from paperflow.delivery_control import ControlledRagClient
from test_delivery_state import delivery_case,owner,begin
from test_control import manager
from test_delivery_flow import IndexedRag,flow,release,ManagedActions


def legacy(case,tmp_path,closed=True):
    m,c,t,d,commits,*_=case
    client=IndexedRag();f=flow(case,tmp_path,client);f.run();release(f)
    receipts=commits.ledger.progress(d,'120201')['receipts']
    with m.store.db() as conn:
        conn.execute('DELETE FROM specialty_active');conn.execute('DELETE FROM specialty_switches');conn.execute('DELETE FROM specialty_progress')
        conn.execute('INSERT INTO legacy_targets VALUES(?,?,?,?,?,?)',(d,'120201','snapshot',canonical(receipts['120201']),'pending_reconcile','{}'))
        conn.execute('INSERT INTO legacy_runs VALUES(?,?,?,?,?)',('snapshot',t['task_id'],canonical(dict(task=dict(t,stage='complete' if closed else 'ingest'))),'historical','{}'))
    actions=ManagedActions(case,tmp_path,'120201');client.calls=[]
    return actions,client


def test_old_verified_never_means_active_before_current_readback(delivery_case,tmp_path):
    actions,client=legacy(delivery_case,tmp_path)
    m,c,t,d,commits,*_=delivery_case
    assert commits.ledger.active(t['source_id'],'120201') is None
    result=readback(c,commits.ledger,actions,ControlledRagClient(client,actions),actions.identity)
    assert result['state']=='active'
    assert all(method=='GET' or path=='/retrieval' for method,path,_ in client.calls)
    status=article_status(m.store,t['task_id'],dict(closeout_pending=0,delayed=2))
    assert status['available']==1 and not status['complete'] # second specialty still absent


@pytest.mark.parametrize('failure',['disabled_doc','disabled_chunk','no_retrieval','bad_content','bad_identity'])
def test_incomplete_real_state_cannot_be_promoted(delivery_case,tmp_path,failure):
    actions,client=legacy(delivery_case,tmp_path)
    m,c,t,d,commits,*_=delivery_case
    doc=next(iter(client.docs.values()));did=doc['id']
    if failure=='disabled_doc':doc['status']='0'
    elif failure=='disabled_chunk':client.chunks[did][0]['available']=False
    elif failure=='bad_content':client.chunks[did][0]['content']='corrupted'
    elif failure=='bad_identity':doc['meta_fields']['paperflow_manifest_sha256']='0'*64
    else:
        original=client.request
        client.request=lambda method,path,*args:dict(chunks=[]) if path=='/retrieval' else original(method,path,*args)
    result=readback(c,commits.ledger,actions,ControlledRagClient(client,actions),actions.identity)
    assert result['state']=='pending_reconcile'
    assert commits.ledger.active(t['source_id'],'120201') is None


def test_interrupted_old_writer_cannot_be_cleared_by_readback(delivery_case,tmp_path):
    actions,client=legacy(delivery_case,tmp_path,closed=False)
    m,c,t,d,commits,*_=delivery_case
    result=readback(c,commits.ledger,actions,ControlledRagClient(client,actions),actions.identity)
    assert result['state']=='observed_pending'
    assert commits.ledger.active(t['source_id'],'120201') is None


@pytest.mark.parametrize('kind,operation',[('rag_write','http'),('local','switch'),('local','save'),('local','repair_report')])
def test_legacy_owner_cannot_borrow_write_or_repair_permissions(delivery_case,tmp_path,kind,operation):
    actions,client=legacy(delivery_case,tmp_path)
    with pytest.raises(ValueError,match='legacy'):
        begin(actions.m,actions.ex,actions.identity,kind=kind,operation=operation)


def test_imported_target_is_not_discovered_for_delivery(delivery_case,tmp_path):
    m,c,t,d,commits,*_=delivery_case
    from paperflow.scheduler import Scheduler
    c.cleanup=False
    scheduler=Scheduler(m,c,isolation_root=tmp_path)
    actions,client=legacy(delivery_case,tmp_path)
    scheduler.discover()
    rows=scheduler.ledger.rows("SELECT data FROM scheduler_jobs WHERE kind='delivery'")
    assert not any(json.loads(r['data'])['specialty_code']=='120201' for r in rows)


def test_pause_blocks_late_legacy_result(delivery_case,tmp_path):
    actions,client=legacy(delivery_case,tmp_path)
    original=actions.commit
    def pause_then_commit(op,value):
        actions.m.pause('late legacy result')
        return original(op,value)
    actions.commit=pause_then_commit
    with pytest.raises(Exception):
        readback(actions.config,actions.commits.ledger,actions,ControlledRagClient(client,actions),actions.identity)
    assert actions.commits.ledger.active(actions.identity['source_id'],'120201') is None


def test_normal_retention_does_not_block_complete_but_closeout_does(delivery_case,tmp_path):
    actions,client=legacy(delivery_case,tmp_path)
    m,c,t,d,commits,*_=delivery_case
    readback(c,commits.ledger,actions,ControlledRagClient(client,actions),actions.identity)
    with m.store.db() as conn:conn.execute("DELETE FROM target_versions WHERE specialty_code='120202'")
    assert article_status(m.store,t['task_id'],dict(closeout_pending=0,delayed=4))['complete']
    pending=article_status(m.store,t['task_id'],dict(closeout_pending=1,delayed=4))
    assert not pending['complete'] and pending['available']==1


def test_old_active_new_failed_and_frozen_old_view(delivery_case,tmp_path):
    from test_delivery_flow import advance
    actions,client=legacy(delivery_case,tmp_path)
    m,c,t,d,commits,*_=delivery_case
    readback(c,commits.ledger,actions,ControlledRagClient(client,actions),actions.identity)
    advance(delivery_case)
    status=article_status(m.store,t['task_id'],dict(closeout_pending=0))
    assert status['available']==1 and status['expected_complete']==0 and not status['complete']
    commits.freeze_active(t['source_id'],'120201','test old active bad')
    assert article_status(m.store,t['task_id'],dict(closeout_pending=0))['available']==0


def test_current_owner_can_only_read_older_active_version(delivery_case,tmp_path):
    from test_delivery_flow import advance
    from paperflow.legacy_readback import audit_active_version
    m,c,t,d,commits,*_=delivery_case
    client=IndexedRag();f=flow(delivery_case,tmp_path,client);f.run();release(f)
    newer=advance(delivery_case);actions=ManagedActions(newer,tmp_path,'120201');client.calls=[]
    result=audit_active_version(c,commits.ledger,actions,ControlledRagClient(client,actions),actions.identity)
    assert result['active_identity']['manifest_sha256']==d
    assert result['expected_identity']['manifest_sha256']==newer[3]
    assert all(method=='GET' or path=='/retrieval' for method,path,_ in client.calls)
    assert commits.ledger.active(t['source_id'],'120201')['identity']['manifest_sha256']==d
