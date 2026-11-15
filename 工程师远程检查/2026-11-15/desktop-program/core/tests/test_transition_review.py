"""Regressions from the single independent stage-8 review."""
import json
import uuid
from pathlib import Path

import pytest

from paperflow.handoff_store import canonical
from paperflow.legacy_readback import article_status,readback
from paperflow.delivery_control import ControlledRagClient
from paperflow.package import load,save,sha
from test_control import manager
from test_delivery_state import delivery_case,owner,begin
from test_delivery_flow import IndexedRag,flow,release,advance,ManagedActions
from test_legacy_readback import legacy
from test_winjob import wait_until


def stop_owner(actions):
    actions.m.cancel_unit(actions.ex.execution_id,'readback test ended')
    wait_until(lambda:actions.ex.exit_saved)


def test_failed_start_retains_every_old_lock(tmp_path):
    from paperflow.transition import TransitionGuard,TransitionSession
    from paperflow.instance_lock import InstanceLock
    stop=tmp_path/'STOP';stop.write_text('paused')
    locks=[tmp_path/'controller.lock',tmp_path/'worker.lock']
    class Unconfirmed:
        def start(self):raise RuntimeError('old process tree exit unconfirmed; takeover denied')
    guard=TransitionGuard(locks,[stop])
    try:
        with pytest.raises(RuntimeError,match='unconfirmed'):TransitionSession(Unconfirmed(),guard).__enter__()
        for lock in locks:
            with pytest.raises(RuntimeError,match='already running'):
                with InstanceLock(lock):pass
    finally:guard.__exit__() # Test-only cleanup after the simulated process is known absent.


def prepare_pending(case,tmp_path):
    actions,client=legacy(case,tmp_path,closed=False)
    result=readback(actions.config,actions.commits.ledger,actions,ControlledRagClient(client,actions),actions.identity)
    assert result['state']=='observed_pending'
    stop_owner(actions)
    return client


@pytest.mark.parametrize('changed_source',[False,True])
def test_legacy_pending_blocks_new_manifest_or_task_write(delivery_case,tmp_path,package_factory,changed_source):
    m,c,t,d,commits,*_=delivery_case
    prepare_pending(delivery_case,tmp_path)
    if changed_source:
        from paperflow.handoff import PackageHandoff
        Path(t['source_path']).write_text('new source bytes')
        task=m.store.register_source(t['source_path'],sha(t['source_path']))
        material,_,catalog=package_factory('new-source',task_id=task['task_id'],source_sha=task['source_sha256'])
        for name in ('profile.json','tree.json','assets.json','chunks.json','review.json','corrections.json'):
            value=load(material/name);value['source_id']=task['source_id'];save(material/name,value)
        (material/'manifest.json').unlink();(material/'READY.json').unlink()
        handoff=PackageHandoff(m.store,c.handoff_root)
        intent=handoff.prepare(task['task_id'],task,material,catalog,mapping_version='test')
        result=handoff.resume(task['task_id'],intent['input_fingerprint'])
        newer=(m,c,task,result['manifest_sha256'],commits,material,catalog)
        assert task['source_id']==t['source_id'] and task['task_id']!=t['task_id']
    else:newer=advance(delivery_case)
    ex,identity=owner(newer,tmp_path)
    with pytest.raises(ValueError,match='legacy.*pending'):
        begin(m,ex,identity,kind='rag_write',operation='http',method='POST',
            endpoint='http://127.0.0.1:18088/datasets/paper-ds/documents',object_manifest=newer[3])
    status=article_status(m.store,newer[2]['task_id'],dict(closeout_pending=0))
    assert status['pending_reconcile']==1 and not status['complete']


def test_pending_scope_excluded_before_scheduler_limit(delivery_case,tmp_path):
    from paperflow.scheduler import Scheduler
    m,c,t,d,commits,*_=delivery_case;c.cleanup=False
    scheduler=Scheduler(m,c,isolation_root=tmp_path)
    prepare_pending(delivery_case,tmp_path);newer=advance(delivery_case)
    scheduler.discover()
    codes={json.loads(r['data'])['specialty_code'] for r in scheduler.ledger.rows("SELECT data FROM scheduler_jobs WHERE kind='delivery'")}
    assert '120201' not in codes and '120202' in codes


def test_legacy_scope_rechecked_when_prepared_ticket_begins(delivery_case,tmp_path):
    m,c,t,d,commits,*_=delivery_case
    actions,client=legacy(delivery_case,tmp_path)
    readback(c,commits.ledger,actions,ControlledRagClient(client,actions),actions.identity);stop_owner(actions)
    newer=advance(delivery_case);ex,identity=owner(newer,tmp_path)
    resource=dict(identity=identity,operation='http',method='POST',
        endpoint='http://127.0.0.1:18088/datasets/paper-ds/documents',object_manifest=newer[3])
    ticket=m.request_action(ex.execution_id,uuid.uuid4().hex,'rag_write',resource,identity['generation'],60)['ticket']
    # Exercise the final DB recheck separately from the first preparation check.
    with m.store.db() as conn:conn.execute("UPDATE legacy_targets SET state='observed_pending'")
    with pytest.raises(ValueError,match='legacy.*pending'):m.check_begin(ex.execution_id,ticket)
    assert not ex.active_action


def test_historical_pending_protects_remote_versions_and_local_evidence(delivery_case,tmp_path):
    from paperflow.cleanup_store import CleanupLedger
    m,c,t,d,commits,*_=delivery_case
    prepare_pending(delivery_case,tmp_path);newer=advance(delivery_case)
    cleanup=CleanupLedger(m.store,c)
    with m.store.db() as conn:
        with pytest.raises(ValueError,match='legacy.*pending'):
            cleanup.remote_guard(conn,dict(manifest_sha256=newer[3],specialty_code='120201',source_id=t['source_id']))
        # Remove ordinary references to isolate the historical-evidence guard.
        conn.execute('DELETE FROM package_references WHERE package_sha256=?',(d,))
        with pytest.raises(ValueError,match='legacy.*pending'):cleanup.package_guard(conn,d)


def test_verified_closed_history_allows_new_version(delivery_case,tmp_path):
    actions,client=legacy(delivery_case,tmp_path)
    readback(actions.config,actions.commits.ledger,actions,ControlledRagClient(client,actions),actions.identity)
    stop_owner(actions)
    newer=flow(advance(delivery_case),tmp_path,client)
    assert newer.run()['state']=='active'


@pytest.mark.parametrize('phase,available',[('enabled',1),('retrieved',0),('old_disabled',0),('withdraw_pending',0),('withdrawn',1),('committed',1)])
def test_view_tracks_actual_switch_window(delivery_case,tmp_path,phase,available):
    m,c,t,d,commits,*_=delivery_case
    client=IndexedRag();previous=flow(delivery_case,tmp_path,client);previous.run();release(previous)
    candidate=flow(advance(delivery_case),tmp_path,client)
    stop_at=phase if phase in ('enabled','retrieved','old_disabled') else 'old_disabled'
    candidate.fault=lambda point:(_ for _ in ()).throw(RuntimeError('pause in switch')) if point==stop_at else None
    with pytest.raises(RuntimeError,match='pause in switch'):candidate.run()
    switch=candidate.ledger.switching(t['source_id'],'120201')
    if phase=='withdraw_pending':candidate.actions.commit('withdraw',dict(switch_id=switch['switch_id'],phase=phase))
    elif phase=='withdrawn':candidate.withdraw(switch)
    elif phase=='committed':
        candidate.fault=lambda point:None
        assert candidate.run()['state']=='active'
    result=article_status(m.store,t['task_id'],dict(closeout_pending=0))
    assert result['available']==available
    assert result['expected_complete']==int(phase=='committed')
    if phase=='withdraw_pending':assert candidate.ledger.active(t['source_id'],'120201')['state']=='restore_pending'
