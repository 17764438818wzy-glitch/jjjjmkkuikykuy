"""Durable metadata must not turn a successful upload into a second POST."""
import json
import sys
import time
from dataclasses import replace
from threading import Event

import pytest

from evidence import export_package, package_fingerprint
from engine import digest
from ingest_queue import QueueStore
from ingest_scheduler import WorkPool
from source_education import extract_source_education, source_education_fields
from test_ingest_contracts import spec
from test_ingest_scheduler import FakeGateway
from test_knowledge_package import sample


def source_spec(tmp_path, **changes):
    package = tmp_path / 'package'
    original = tmp_path / 'original.txt'
    original.write_text('本科毕业论文\n学习形式：函授\n摘要', encoding='utf8')
    meta = sample(package)
    meta.update(source_education_fields(extract_source_education(original)))
    export_package(package, meta)
    return spec(path=package/'ingest.md', package_path=package,
                package_sha256=package_fingerprint(package),
                content_sha256=digest(package/'ingest.md'),
                handoff_id=meta['id'], policy_key='finance', **changes)


def confirmed(task, remote, verification='verified'):
    payload = json.loads(task['source_metadata_payload'])
    return dict(status='confirmed', document_id=remote['id'],
                source_education_revision=payload['source_education_revision'],
                verification=verification, receipt_path='local-receipt.json', reason_code='ok')


def drive(pool, predicate, count=100):
    for _ in range(count):
        pool.tick()
        if predicate():
            return
        time.sleep(.005)
    assert predicate(), 'stage did not reach expected durable state'


def test_upload_only_stays_incomplete_until_metadata_and_restart(tmp_path):
    queue = QueueStore(tmp_path/'queue.db')
    key = queue.enqueue(source_spec(tmp_path, parse_required=False))
    gateway = FakeGateway()
    # Existing API cannot yet register or gate source-bearing tasks.
    assert queue.get(key).get('source_metadata_status') == 'pending'
    calls = []
    def timeout(task, remote):
        calls.append(task['remote_id'])
        assert queue.get(key)['remote_id'] == remote['id']
        raise TimeoutError('private server content must never be recorded')
    pool = WorkPool(queue, lambda: gateway, lambda *_: 'accepted', on_source_metadata=timeout)
    try:
        drive(pool, lambda: bool(calls) and not pool._has_future(key))
        row = queue.get(key)
        assert row['status'] == 'uploaded' and row['remote_id'] == 'remote-paper_123'
        assert pool.tick()['source_metadata']['pending'] == 1
        assert pool.tick()['source_metadata']['upload_only_complete'] == 0
        assert gateway.upload_calls == ['paper_123']
    finally:
        pool.close()
    queue = QueueStore(tmp_path/'queue.db')
    queue.transition(key, 'uploaded', 'uploaded', source_metadata_retry_at=0)
    pool = WorkPool(queue, lambda: gateway, lambda *_: 'accepted', on_source_metadata=confirmed)
    try:
        drive(pool, lambda: queue.get(key)['source_metadata_status'] == 'confirmed')
        assert pool.tick()['source_metadata']['upload_only_complete'] == 1
        assert gateway.upload_calls == ['paper_123'] and gateway.parse_calls == []
        assert 'private server' not in json.dumps(queue.attempts(key))
    finally:
        pool.close()


def test_metadata_wait_does_not_use_model_slot_and_conflict_is_legal(tmp_path):
    queue = QueueStore(tmp_path/'q.db')
    key = queue.enqueue(source_spec(tmp_path))
    queue.enqueue(spec(source_id='legacy'))
    gateway = FakeGateway()
    entered, release = Event(), Event()
    def receive(task, remote):
        entered.set()
        release.wait(2)
        return confirmed(task, remote, 'conflict')
    assert queue.get(key).get('source_metadata_status') == 'pending'
    pool = WorkPool(queue, lambda: gateway, lambda *_: 'accepted',
                    on_source_metadata=receive, parse_limit=1)
    try:
        drive(pool, entered.is_set)
        drive(pool, lambda: 'legacy' in gateway.parse_calls)
        assert 'paper_123' not in gateway.parse_calls
        gateway.docs['remote-legacy']['run'] = 'DONE'
        release.set()
        drive(pool, lambda: 'paper_123' in gateway.parse_calls)
        assert queue.get(key)['source_metadata_verification'] == 'conflict'
    finally:
        release.set()
        pool.close()


@pytest.mark.parametrize('initial', ['uploaded','upload_unknown'])
def test_imported_and_uncertain_receipts_run_metadata_without_upload(tmp_path, initial):
    queue = QueueStore(tmp_path/'q.db')
    artifact = source_spec(tmp_path, parse_required=False)
    key = queue.enqueue(artifact, {'status':initial, 'remote_id':'existing'})
    gateway = FakeGateway()
    gateway.docs['existing'] = {'id':'existing', 'name':artifact.remote_name, 'run':'UNSTART'}
    if initial == 'upload_unknown':
        # Existing conservative reconciliation waits 15s before trying again.
        import sqlite3
        with sqlite3.connect(queue.path) as db:
            db.execute('UPDATE tasks SET updated_at=0 WHERE id=?', (key,))
    pool = WorkPool(queue, lambda:gateway, lambda *_:'accepted', on_source_metadata=confirmed)
    try:
        drive(pool, lambda:queue.get(key)['source_metadata_status']=='confirmed')
        assert queue.get(key)['remote_id']=='existing'
        assert queue.get(key)['status']=='uploaded'
        assert gateway.upload_calls==[]
    finally:
        pool.close()


def test_unknown_upload_without_id_requires_existing_byte_reconciliation(tmp_path):
    queue = QueueStore(tmp_path/'q.db')
    artifact = source_spec(tmp_path, parse_required=False)
    key = queue.enqueue(artifact, {'status':'upload_unknown'})
    queue.transition(key,'upload_unknown','upload_intent',lease_until=0)
    gateway = FakeGateway()
    remote = {'id':'lost-reply', 'name':artifact.remote_name, 'run':'UNSTART'}
    gateway.lookup = lambda task:[remote.copy()]
    gateway.client_factory = lambda:None
    gateway.remote_content_reader = lambda *_:artifact.path.read_bytes()
    pool = WorkPool(queue,lambda:gateway,lambda *_:'accepted',on_source_metadata=confirmed)
    try:
        drive(pool,lambda:queue.get(key)['source_metadata_status']=='confirmed')
        assert queue.get(key)['remote_id']=='lost-reply'
        assert gateway.upload_calls==[]
    finally:
        pool.close()


def test_late_metadata_callback_cannot_overwrite_newer_review(tmp_path):
    queue = QueueStore(tmp_path/'q.db')
    key = queue.enqueue(source_spec(tmp_path,parse_required=False),{'status':'uploaded','remote_id':'doc'})
    gateway = FakeGateway()
    gateway.docs['doc']={'id':'doc','name':queue.get(key)['remote_name']}
    entered, release = Event(), Event()
    def receive(task, remote):
        entered.set(); release.wait(2)
        return confirmed(task,remote)
    pool=WorkPool(queue,lambda:gateway,lambda *_:'accepted',on_source_metadata=receive)
    try:
        drive(pool,entered.is_set)
        queue.transition(key,'uploaded','uploaded',source_metadata_status='blocked',error='new review')
        release.set()
        drive(pool,lambda:not pool._has_future(key))
        row=queue.get(key)
        assert row['source_metadata_status']=='blocked' and row['error']=='new review'
        assert not queue.attempts(key)
    finally:
        release.set(); pool.close()


@pytest.mark.parametrize('mismatch', ['local','remote','receipt_id','receipt_revision'])
def test_mismatch_blocks_metadata_without_quality_rejection_or_new_upload(tmp_path,mismatch):
    queue=QueueStore(tmp_path/'q.db')
    artifact=source_spec(tmp_path,parse_required=False)
    key=queue.enqueue(artifact,{'status':'uploaded','remote_id':'doc'})
    gateway=FakeGateway()
    gateway.docs['doc']={'id':'doc','name':artifact.remote_name}
    calls=[]
    def receive(task,remote):
        calls.append(task['id']); result=confirmed(task,remote)
        if mismatch=='receipt_id':result['document_id']='other'
        if mismatch=='receipt_revision':result['source_education_revision']='0'*64
        return result
    if mismatch=='local':artifact.path.write_text('changed approved body',encoding='utf8')
    if mismatch=='remote':gateway.docs['doc']['name']='another paper'
    pool=WorkPool(queue,lambda:gateway,lambda *_:'accepted',on_source_metadata=receive)
    try:
        drive(pool,lambda:queue.get(key)['source_metadata_status']=='blocked')
        assert queue.get(key)['status']=='uploaded' and queue.get(key)['remote_id']=='doc'
        assert gateway.upload_calls==gateway.parse_calls==[]
        if mismatch in ('local','remote'):assert calls==[]
    finally:
        pool.close()


def test_retry_budget_blocks_after_four_failures_without_uploading_again(tmp_path):
    queue=QueueStore(tmp_path/'q.db')
    key=queue.enqueue(source_spec(tmp_path,parse_required=False))
    gateway=FakeGateway(); calls=[]
    def unavailable(task,remote):
        calls.append(task['source_metadata_request_id']);raise TimeoutError('secret')
    pool=WorkPool(queue,lambda:gateway,lambda *_:'accepted',on_source_metadata=unavailable)
    try:
        for attempt in range(1,5):
            drive(pool,lambda:len(calls)==attempt and not pool._has_future(key))
            row=queue.get(key)
            if attempt<4:
                assert row['source_metadata_retry_at']>time.time()
                queue.transition(key,'uploaded','uploaded',source_metadata_retry_at=0)
        assert row['source_metadata_status']=='blocked'
        assert row['source_metadata_reason']=='retry_exhausted'
        for _ in range(5):pool.tick()
        assert len(calls)==4 and len(set(calls))==4
        assert gateway.upload_calls==['paper_123']
    finally:
        pool.close()


def test_legacy_and_assets_never_call_receiver(tmp_path):
    queue=QueueStore(tmp_path/'q.db')
    legacy=queue.enqueue(spec(parse_required=False))
    asset=queue.enqueue(source_spec(tmp_path,role='asset:original.png',parse_required=False))
    gateway=FakeGateway()
    def forbidden(*_):raise AssertionError('legacy/assets invoked source receiver')
    pool=WorkPool(queue,lambda:gateway,lambda *_:'accepted',on_source_metadata=forbidden)
    try:
        drive(pool,lambda:all(queue.get(key)['status']=='uploaded' for key in (legacy,asset)))
        assert queue.get(legacy)['source_metadata_status'] is None
        assert queue.get(asset)['source_metadata_status'] is None
    finally:
        pool.close()


def test_running_metadata_lease_recovers_after_process_death(tmp_path):
    queue=QueueStore(tmp_path/'q.db')
    key=queue.enqueue(source_spec(tmp_path,parse_required=False),{'status':'uploaded','remote_id':'doc'})
    old=queue.claim_source_metadata(time.time()-200,1)[0]
    newer=QueueStore(queue.path).claim_source_metadata(time.time(),1)[0]
    assert old['source_metadata_request_id']!=newer['source_metadata_request_id']
    assert not queue.finish_source_metadata(old,status='confirmed',reason='ok',receipt=confirmed(old,{'id':'doc'}))
    assert queue.finish_source_metadata(newer,status='confirmed',reason='ok',receipt=confirmed(newer,{'id':'doc'}))


def test_worker_configured_receiver_reads_trusted_host_config_only(tmp_path):
    from ingest_worker import configured_source_receiver
    root=tmp_path/'data';root.mkdir()
    assert configured_source_receiver(root) is None
    (root/'ai_settings.json').write_text(json.dumps({'source_receiver_command':[sys.executable,'fixed-host.py']}),encoding='utf8')
    receiver=configured_source_receiver(root)
    assert receiver.command==[sys.executable,'fixed-host.py']


def test_fixed_receiver_command_survives_save_and_load(tmp_path):
    from auto_config import save_config,load_config
    save_config(tmp_path,{'source_receiver_command':[sys.executable,'fixed-host.py']})
    assert load_config(tmp_path)['source_receiver_command']==[sys.executable,'fixed-host.py']


def test_native_routing_waiting_count_and_claim_exclude_metadata_pending(tmp_path):
    from test_ingest_routed_queue import route
    queue=QueueStore(tmp_path/'q.db')
    artifact=replace(source_spec(tmp_path),policy_key='education')
    key=queue.enqueue(artifact,{'status':'parse_waiting','remote_id':'doc'})
    assert queue.routed_waiting_count(route())==0
    assert queue.claim_routed_parse('owner',time.time(),120,route(),8,1,{}) is None
    task=queue.claim_source_metadata(time.time(),1)[0]
    assert queue.finish_source_metadata(task,status='confirmed',reason='ok',receipt=confirmed(task,{'id':'doc'},'pending'))
    assert queue.routed_waiting_count(route())==1
    assert queue.claim_routed_parse('owner',time.time(),120,route(),8,1,{})['id']==key


def test_missing_host_command_is_visible_and_never_claims_parse(tmp_path):
    queue=QueueStore(tmp_path/'q.db');key=queue.enqueue(source_spec(tmp_path))
    gateway=FakeGateway();pool=WorkPool(queue,lambda:gateway,lambda *_:'accepted')
    try:
        drive(pool,lambda:queue.get(key)['source_metadata_status']=='blocked')
        summary=pool.tick()['source_metadata']
        assert summary['blocked_reasons']=={'receiver_not_configured':1}
        assert summary['upload_only_complete']==0
        assert queue.get(key)['status']=='parse_waiting'
        assert gateway.parse_calls==[]
    finally:
        pool.close()


def test_confirmed_metadata_is_idempotent_on_reenqueue(tmp_path):
    queue=QueueStore(tmp_path/'q.db');artifact=source_spec(tmp_path,parse_required=False)
    key=queue.enqueue(artifact,{'status':'uploaded','remote_id':'doc'})
    task=queue.claim_source_metadata(time.time(),1)[0]
    assert queue.finish_source_metadata(task,status='confirmed',reason='ok',receipt=confirmed(task,{'id':'doc'}))
    assert queue.enqueue(artifact)==key
    assert queue.claim_source_metadata(time.time(),1)==[]
    assert queue.source_metadata_summary()['upload_only_complete']==1


@pytest.mark.parametrize('wrong', ['id','revision','status','missing'])
def test_queue_cannot_persist_confirmation_without_matching_receipt(tmp_path,wrong):
    queue=QueueStore(tmp_path/'q.db');key=queue.enqueue(source_spec(tmp_path,parse_required=False),{'status':'uploaded','remote_id':'doc'})
    task=queue.claim_source_metadata(time.time(),1)[0]
    result=confirmed(task,{'id':'doc'})
    if wrong=='id':result['document_id']='other'
    if wrong=='revision':result['source_education_revision']='0'*64
    if wrong=='status':result['status']='retryable_error'
    if wrong=='missing':result=None
    with pytest.raises(ValueError):
        queue.finish_source_metadata(task,status='confirmed',reason='ok',receipt=result)
    assert queue.get(key)['source_metadata_status']=='running'


def test_generic_transition_cannot_bypass_receiver_confirmation(tmp_path):
    queue=QueueStore(tmp_path/'q.db');key=queue.enqueue(source_spec(tmp_path))
    with pytest.raises(ValueError):
        queue.transition(key,'upload_waiting','upload_waiting',source_metadata_status='confirmed')


def test_operator_retry_is_cas_bound_rotates_request_and_stays_metadata_only(tmp_path):
    queue=QueueStore(tmp_path/'q.db');artifact=source_spec(tmp_path,parse_required=False)
    key=queue.enqueue(artifact,{'status':'uploaded','remote_id':'doc'})
    old=queue.claim_source_metadata(time.time(),1)[0]
    assert queue.finish_source_metadata(old,status='blocked',reason='retry_exhausted')
    blocked=queue.get(key)
    assert not queue.request_source_metadata_retry(key,blocked['updated_at']-1,'doc')
    assert not queue.request_source_metadata_retry(key,blocked['updated_at'],'other')
    assert queue.request_source_metadata_retry(key,blocked['updated_at'],'doc')
    renewed=queue.get(key)
    assert renewed['remote_id']=='doc' and renewed['status']=='uploaded'
    assert renewed['source_metadata_request_id']!=old['source_metadata_request_id']
    assert renewed['source_metadata_attempts']==0
    assert not queue.finish_source_metadata(old,status='confirmed',reason='ok',receipt=confirmed(old,{'id':'doc'}))
    assert not queue.request_source_metadata_retry(key,renewed['updated_at'],'doc')
    for _ in range(4):
        task=queue.claim_source_metadata(time.time()+1000,1)[0]
        assert queue.finish_source_metadata(task,status='pending',reason='receiver_unavailable')
    assert queue.get(key)['source_metadata_status']=='blocked'
    assert queue.claim_source_metadata(time.time()+1000,1)==[]
    assert any(attempt['operation']=='source_metadata_retry' for attempt in queue.attempts(key))


@pytest.mark.parametrize('state',['upload_intent','parse_intent','running'])
def test_verified_receipt_cannot_overwrite_live_inflight_task(tmp_path,state):
    queue=QueueStore(tmp_path/'q.db');artifact=source_spec(tmp_path,parse_required=False)
    key=queue.enqueue(artifact)
    queue.transition(key,'upload_waiting',state,owner='active',lease_until=time.time()+100)
    before=queue.get(key)
    with pytest.raises(ValueError):queue.enqueue(artifact,{'status':'uploaded','remote_id':'legacy-doc'})
    assert queue.get(key)==before


def test_conflicting_remote_receipt_cannot_replace_existing_document(tmp_path):
    queue=QueueStore(tmp_path/'q.db');artifact=source_spec(tmp_path,parse_required=False)
    key=queue.enqueue(artifact,{'status':'uploaded','remote_id':'current-doc'})
    before=queue.get(key)
    with pytest.raises(ValueError):queue.enqueue(artifact,{'status':'uploaded','remote_id':'other-doc'})
    assert queue.get(key)==before


def test_blocked_metadata_enables_existing_user_retry_without_gui_or_upload(tmp_path):
    from types import SimpleNamespace
    from rag_results_ui import RAGResultsView
    from upload_tracking import UploadIndex
    from rag_status import needs_recovery
    queue=QueueStore(tmp_path/'q.db');artifact=source_spec(tmp_path,parse_required=False)
    key=queue.enqueue(artifact,{'status':'uploaded','remote_id':'doc'})
    task=queue.claim_source_metadata(time.time(),1)[0]
    queue.finish_source_metadata(task,status='blocked',reason='receiver_not_configured')
    view=RAGResultsView()
    view.current={'run_id':'batch-1'}
    view.live_results_active=lambda:False
    view.url=SimpleNamespace(get=lambda:'http://localhost:9380')
    view._selected_target_dataset=lambda:None
    view._work_pool_snapshot=queue.get_many([key])
    state=view._remote_item_status({'id':'AUTO','queue_task_ids':[key]},UploadIndex([]))
    assert state['can_retry'] and needs_recovery(state)
    assert 'receiver_not_configured' in state['reason'] and '重试选中项' in state['reason']


@pytest.mark.parametrize('command', ['python script.py', ['relative.exe'], ['C:/fixed.cmd'], [sys.executable,'bad\x00arg']])
def test_cli_rejects_shell_or_untrusted_command_shape(command):
    from ingest_source_metadata import SourceReceiverCLI
    with pytest.raises(ValueError):SourceReceiverCLI(command)


@pytest.mark.parametrize('mode', ['confirmed','timeout','huge','multiline','wrong_id'])
def test_cli_real_process_limits_protocol_and_safe_failure(tmp_path, mode):
    from ingest_source_metadata import SourceReceiverCLI, SourceMetadataBlocked
    queue=QueueStore(tmp_path/'q.db');artifact=source_spec(tmp_path,parse_required=False)
    key=queue.enqueue(artifact,{'status':'uploaded','remote_id':'doc'})
    task=queue.claim_source_metadata(time.time(),1)[0]
    capture=tmp_path/'request.json'
    script=tmp_path/'receiver.py'
    revision=json.loads(task['source_metadata_payload'])['source_education_revision']
    result=confirmed(task,{'id':'doc'})
    if mode=='wrong_id':result['document_id']='wrong'
    script.write_text('import json,sys,time\nfrom pathlib import Path\nr=json.load(sys.stdin)\n'
        +f'Path({str(capture)!r}).write_text(json.dumps(r),encoding="utf8")\n'
        +('time.sleep(2)\n' if mode=='timeout' else '')
        +('print("x"*70000)\n' if mode=='huge' else f'print({json.dumps(result)!r})\n')
        +('print("secret extra line")\n' if mode=='multiline' else ''),encoding='utf8')
    receiver=SourceReceiverCLI([sys.executable,str(script)],timeout=.15 if mode=='timeout' else 3)
    if mode=='confirmed':
        assert receiver(task,{'id':'doc','name':task['remote_name']})['source_education_revision']==revision
        request=json.loads(capture.read_text('utf8'))
        assert set(request)=={'request_id','service_key','dataset_id','document_id','package_path','content_sha256','package_sha256','source_id','handoff_id','batch_id'}
        assert request['request_id']==task['source_metadata_request_id']
    else:
        with pytest.raises((TimeoutError,SourceMetadataBlocked)) as error:
            receiver(task,{'id':'doc','name':task['remote_name']})
        assert 'secret extra line' not in str(error.value)
