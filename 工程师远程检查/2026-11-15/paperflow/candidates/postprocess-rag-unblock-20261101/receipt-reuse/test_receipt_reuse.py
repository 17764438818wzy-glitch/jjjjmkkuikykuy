import asyncio
from contextlib import contextmanager
import copy
import json
import sqlite3
import threading

import pytest

from test_remote_runtime_adapter import make, sample
from test_remote_server_transport import setup as transport_setup
from folder_state import StageConflict
from postprocessing_results import ResultConflict
from remote_runtime_adapter import RemoteReceiptInspector


def run_with_result(sample, *, corrupt=False, pause=False):
    identity = sample['evidence'][0]['identity']
    inspections = []
    inspector = sample['stage_store'].inspect_receipt
    original_inspect = inspector._inspect_batch

    def inspect(requests):
        inspections.append(requests)
        return original_inspect(requests)

    inspector._inspect_batch = inspect

    def result(job):
        value = sample['remote_only_result'](job)
        request = dict(job=job, server_identity=sample['server'],
                       receipt_json=value['receipt_json'], receipt_sha256=value['receipt_sha256'])
        value['server_receipt'] = sample['attestation'](request)
        if corrupt:
            value['server_receipt']['artifact_map']['integrity_status'] = 'failed'
        if pause:
            with sample['task_gate'](identity['task_id']), sqlite3.connect(sample['database']) as db:
                db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (identity['task_id'],))
        return value

    sample['options']['wait_for_result'] = result
    def validate(job, value):
        request = dict(job=job, server_identity=sample['server'],
                       receipt_json=value['receipt_json'], receipt_sha256=value['receipt_sha256'])
        RemoteReceiptInspector._accepted(request, value['server_receipt'])
        return True

    sample['options']['validate_result'] = validate
    remote = make(sample)
    job = remote.reserve(identity, 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    return remote, job, identity, inspections


def test_current_result_is_reused_for_one_folder_and_discarded_afterward(sample):
    remote, job, identity, inspections = run_with_result(sample)
    result = remote._execute_sync(job['job_id'], job['job_id'])
    assert result['state'] == 'waiting_rag'
    assert inspections == []
    with sqlite3.connect(sample['database']) as db:
        raw = db.execute('SELECT receipt_json FROM postprocessing_receipts').fetchone()[0]
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] == 'passed'
    sample['stage_store'].inspect_receipt(json.loads(raw), identity, result['claim_id'])
    assert len(inspections) == 1


def test_corrupt_returned_evidence_is_not_replaced_by_a_later_fresh_read(sample):
    remote, job, _, _ = run_with_result(sample, corrupt=True)
    with pytest.raises(StageConflict):
        remote._execute_sync(job['job_id'], job['job_id'])
    with sqlite3.connect(sample['database']) as db:
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] != 'passed'
        assert db.execute('SELECT state FROM postprocessing_claims').fetchone()[0] != 'waiting_rag'


def test_pause_still_holds_the_completed_receipt(sample):
    remote, job, _, inspections = run_with_result(sample, pause=True)
    result = remote._execute_sync(job['job_id'], job['job_id'])
    assert result['state'] == 'receipt_held'
    assert inspections == []


def test_held_receipt_resumes_with_a_fresh_check_and_no_reprocessing(sample):
    remote, job, identity, inspections = run_with_result(sample, pause=True)
    held = remote._execute_sync(job['job_id'], job['job_id'])
    with sqlite3.connect(sample['database']) as db:
        db.row_factory = sqlite3.Row
        db.execute("UPDATE tasks SET desired_state='running',start_generation=2 WHERE task_id=?",
                   (identity['task_id'],))
        row = dict(db.execute('SELECT c.*,p.receipt_json,p.receipt_sha256 FROM postprocessing_claims c '
            'JOIN postprocessing_receipts p ON p.claim_id=c.claim_id WHERE c.claim_id=?',
            (held['claim_id'],)).fetchone())
    previous_events = list(sample['events'])
    inspector = sample['stage_store'].inspect_receipt
    with inspector.prefetched([row]):
        resumed = sample['base'].reconcile_held(job['job_id'], 2, store=sample['stage_store'],
            validator_factory=lambda *_: lambda _: True)
    assert resumed['state'] == 'waiting_rag'
    assert len(inspections) == 1
    assert sample['events'] == previous_events


def test_losing_the_original_prefect_folder_before_registration_is_not_passed(sample):
    remote, job, _, _ = run_with_result(sample)
    store = sample['stage_store']
    original_save = store._save

    def save_then_lose_observation(*args, **kwargs):
        result = original_save(*args, **kwargs)
        store.observe_run = lambda _: False
        return result

    store._save = save_then_lose_observation
    with pytest.raises((StageConflict, ResultConflict)):
        remote._execute_sync(job['job_id'], job['job_id'])
    with sqlite3.connect(sample['database']) as db:
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] != 'passed'
        assert db.execute('SELECT state FROM postprocessing_claims').fetchone()[0] != 'waiting_rag'


def test_transport_rejects_a_result_with_an_incomplete_artifact_map(transport_setup):
    x = transport_setup
    adapter = x['adapter']
    staged = x['prepared']()
    adapter.submit_once(x['job'], x['snapshot'], staged)
    result = adapter.wait_for_result(x['job'])
    with pytest.raises(StageConflict):
        adapter.validate_result(x['job'], result)


def test_transport_checks_the_returned_result_without_an_extra_inspect(transport_setup, sample):
    x = transport_setup
    adapter = x['adapter']
    result = sample['remote_only_result'](x['job'])
    request = dict(job=x['job'], server_identity=x['server'],
                   receipt_json=result['receipt_json'], receipt_sha256=result['receipt_sha256'])
    evidence = sample['attestation'](request)
    evidence['server_identity'] = x['server']
    result['server_identity'] = x['server']
    result['server_receipt'] = evidence
    x['result'].clear()
    x['result'].update(result)
    staged = x['prepared']()
    adapter.submit_once(x['job'], x['snapshot'], staged)
    result = adapter.wait_for_result(x['job'])
    assert adapter.validate_result(x['job'], result) is True
    assert [call[0] for call in x['calls']].count('inspect') == 0
    result['receipt_sha256'] = '0' * 64
    with pytest.raises(x['m'].TransportError):
        adapter.validate_result(x['job'], result)


def test_concurrent_folders_keep_their_returned_evidence_separate(sample):
    barrier = threading.Barrier(2)
    scope_barrier = threading.Barrier(2)
    inspections = []
    inspector = sample['stage_store'].inspect_receipt
    inspector._inspect_batch = lambda requests: inspections.append(requests)
    original_prefetched = inspector.prefetched

    @contextmanager
    def synchronized_scope(rows, **kwargs):
        with original_prefetched(rows, **kwargs):
            if kwargs.get('accepted_evidence') is not None:
                scope_barrier.wait(timeout=10)
            yield

    inspector.prefetched = synchronized_scope
    first, second = sample['evidence']
    old_task = second['identity']['task_id']
    parent = {key: first['identity'][key] for key in ('task_id', 'parent_run_id')}
    second['identity'].update(parent)
    second['body']['identity'].update(parent)
    for response in second['body']['responses']:
        response['identity'].update(parent)
    for attachment in second['attachments']:
        attachment['ticket'].update(parent)
    with sqlite3.connect(sample['database']) as db:
        db.execute('UPDATE llm_folder_queue SET task_id=?,parent_run_id=? WHERE task_id=?',
                   (parent['task_id'], parent['parent_run_id'], old_task))
        db.execute('UPDATE llm_request_admissions SET task_id=? WHERE task_id=?',
                   (parent['task_id'], old_task))

    def result(job):
        value = sample['remote_only_result'](job)
        request = dict(job=job, server_identity=sample['server'],
                       receipt_json=value['receipt_json'], receipt_sha256=value['receipt_sha256'])
        value['server_receipt'] = sample['attestation'](request)
        barrier.wait(timeout=10)
        return value

    sample['options']['wait_for_result'] = result
    def validate(job, value):
        request = dict(job=job, server_identity=sample['server'],
                       receipt_json=value['receipt_json'], receipt_sha256=value['receipt_sha256'])
        RemoteReceiptInspector._accepted(request, value['server_receipt'])
        return True

    sample['options']['validate_result'] = validate
    remote = make(sample)
    jobs = [remote.reserve(value['identity'], 1) for value in sample['evidence']]
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted'")

    async def execute():
        return await asyncio.gather(*(remote.execute(job['job_id'], job['job_id']) for job in jobs))

    results = asyncio.run(execute())
    assert {result['identity']['group_id'] for result in results} == {
        value['identity']['group_id'] for value in sample['evidence']}
    assert all(result['state'] == 'waiting_rag' for result in results)
    assert inspections == []


def test_full_result_to_registration_uses_one_result_and_no_extra_inspect(sample, transport_setup):
    adapter = transport_setup['adapter']
    adapter._server = sample['server']
    calls = []
    original_submit = sample['options']['submit_once']

    def submit(job, snapshot, staging):
        acknowledgement = original_submit(job, snapshot, staging)
        adapter._submitted[job['job_id']] = dict(job=copy.deepcopy(job), state='accepted')
        return acknowledgement

    def rpc(operation, payload, **kwargs):
        calls.append(operation)
        job = payload['job']
        if operation == 'status':
            return dict(job=job, server_identity=sample['server'], state='finished', receipt_ready=True)
        if operation == 'result':
            result = sample['remote_only_result'](job)
            request = dict(job=job, server_identity=sample['server'],
                           receipt_json=result['receipt_json'], receipt_sha256=result['receipt_sha256'])
            result['server_receipt'] = sample['attestation'](request)
            return result
        raise AssertionError('unexpected remote operation: ' + operation)

    adapter._rpc = rpc
    sample['options']['submit_once'] = submit
    sample['options']['wait_for_result'] = adapter.wait_for_result
    sample['options']['validate_result'] = adapter.validate_result
    sample['stage_store'].inspect_receipt._inspect_batch = lambda _: pytest.fail('unexpected extra inspection')
    remote = make(sample)
    job = remote.reserve(sample['evidence'][0]['identity'], 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    result = remote._execute_sync(job['job_id'], job['job_id'])
    assert result['state'] == 'waiting_rag'
    assert calls == ['status', 'result']
    with sqlite3.connect(sample['database']) as db:
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] == 'passed'
