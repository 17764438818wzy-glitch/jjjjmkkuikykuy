from pathlib import Path
import asyncio
import hashlib
import json
from contextlib import nullcontext
import sqlite3
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from live_runtime import ResultsHooks, body_validation_failure, resume_stopped, run_selected, select_folder
from test_stage_runtime import context


def test_selected_folder_uses_existing_identity_and_generation(context):
    selected, generation = select_folder(context.db, context.identity['task_id'], context.identity['group_id'])
    assert selected == context.identity
    assert generation == 1


def test_selected_folder_rejects_missing_or_noncompleted_rows(context):
    with sqlite3.connect(context.db) as db:
        db.execute("update llm_folder_queue set state='llm_processing'")
    with pytest.raises(ValueError):
        select_folder(context.db, context.identity['task_id'], context.identity['group_id'])


def test_result_hooks_bind_pass_to_existing_claim_and_real_validator(context):
    seen = []
    class Ledger:
        def reserve(self, identity, generation, manifest):
            seen.append(('reserve', identity, generation, manifest)); return 'result-id'
        def record_pass(self, identity, result_id, claim_id, *, validate_receipt):
            seen.append(('passed', identity, result_id, claim_id, validate_receipt('receipt')))
        def record_unresolved(self, identity, result_id, *, code):
            seen.append(('unresolved', identity, result_id, code))
    hooks = ResultsHooks(Ledger())
    result_id = hooks.reserve(context.identity, 1, 'c'*64)
    hooks.success(context.identity, result_id, {'claim_id': 'claim-id'}, lambda value: value == 'receipt')
    assert seen[-1] == ('passed', context.identity, 'result-id', 'claim-id', True)
    hooks.unknown(context.identity, result_id, RuntimeError())
    assert seen[-1] == ('unresolved', context.identity, 'result-id', 'outcome_unknown')


def test_result_hooks_refuse_unproven_failures(context):
    calls = []
    ledger = SimpleNamespace(record_failed=lambda *args, **kwargs: calls.append(args))
    with pytest.raises(ValueError): ResultsHooks(ledger).failure(context.identity, 'result-id', RuntimeError())
    assert calls == []


def test_body_validation_failure_requires_matching_saved_evidence(context, tmp_path):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from body_adapter import BodyValidationError
    from folder_processor import _hash
    identity = context.identity
    body = tmp_path / _hash(identity) / 'body'
    private = body / 'private'
    private.mkdir(parents=True)
    fingerprint = 'd' * 64
    (body / 'input-identity.json').write_text(json.dumps({'identity': identity,
        'input_fingerprint': fingerprint}), encoding='utf8')
    (body / 'diagnostic.json').write_text(json.dumps({'status': 'blocked', 'identity': identity,
        'input_fingerprint': fingerprint, 'error_type': 'BodyValidationError',
        'reason': 'candidate_validation_failed', 'model_requests_added': 0,
        'original_responses_retained': True}), encoding='utf8')
    report = private / 'part-001-validation.json'
    report.write_text('{"valid":false,"errors":[{"code":"learning_unusable"}]}', encoding='utf8')
    ref = {'path': str(report.resolve()), 'sha256': hashlib.sha256(report.read_bytes()).hexdigest()}
    cause = BodyValidationError('candidate_validation_failed', ref)
    failure = body_validation_failure(identity, 1, 'c' * 64, cause, tmp_path,
        verify_upstream=lambda actual, gen: {'identity': actual, 'generation': gen,
                                             'manifest_sha256': 'c' * 64, 'raw_complete': True,
                                             'session_current': True, 'not_deleted': True,
                                             'source_llm_generation': 1}, source_llm_generation=1)
    assert failure.code == 'body_validation'
    assert Path(failure.evidence_path) == report.resolve()
    assert failure.confirm(identity, 'body_validation') is True
    assert failure.confirm(dict(identity, paper_id='other'), 'body_validation') is False
    changed_retry = body_validation_failure(identity, 1, 'c' * 64, cause, tmp_path,
        verify_upstream=lambda actual, gen: {'identity': actual, 'generation': gen,
            'manifest_sha256': 'c' * 64, 'raw_complete': True, 'session_current': True, 'not_deleted': True,
            'source_llm_generation': 1, 'source_retry_sha256': 'e' * 64},
        source_llm_generation=1, source_retry_sha256='a' * 64)
    assert changed_retry.confirm(identity, 'body_validation') is False
    changed_source = body_validation_failure(identity, 2, 'c' * 64, cause, tmp_path,
        verify_upstream=lambda actual, gen: {'identity': actual, 'generation': gen, 'manifest_sha256': 'c' * 64,
            'raw_complete': True, 'session_current': True, 'not_deleted': True, 'source_llm_generation': 2},
        source_llm_generation=1)
    assert changed_source.confirm(identity, 'body_validation') is False
    missing_proof = body_validation_failure(identity, 1, 'c' * 64, cause, tmp_path,
        verify_upstream=lambda actual, gen: {'identity': actual, 'generation': gen, 'manifest_sha256': 'c' * 64},
        source_llm_generation=1)
    assert missing_proof.confirm(identity, 'body_validation') is False
    report.write_text('{"valid":false,"errors":[{"code":"learning_unusable"},{"code":"image_version"}]}', encoding='utf8')
    changed_ref = {'path': str(report.resolve()), 'sha256': hashlib.sha256(report.read_bytes()).hexdigest()}
    resource_error = BodyValidationError('candidate_validation_failed', changed_ref)
    resource_failure = body_validation_failure(identity, 1, 'c' * 64, resource_error, tmp_path,
        verify_upstream=lambda actual, gen: {'identity': actual, 'generation': gen,
            'manifest_sha256': 'c' * 64, 'raw_complete': True, 'session_current': True, 'not_deleted': True,
            'source_llm_generation': 1}, source_llm_generation=1)
    assert resource_failure.confirm(identity, 'body_validation') is False
    report.write_text('{"valid":true}', encoding='utf8')
    assert failure.confirm(identity, 'body_validation') is False


def test_run_selected_submits_only_the_chosen_folder(context):
    item = dict(job_id='ba630c45-6019-447c-9ec0-5eb4480ef225', state='reserved')
    calls = []
    class Coordinator:
        def reserve(self, identity, generation):
            calls.append(('reserve', identity, generation)); return item
        def submit(self, job_id, client, task):
            calls.append(('submit', job_id, task.task_key)); item['state'] = 'finished'
        def job(self, job_id): return dict(item)
    class Worker:
        finished_task_runs = {}
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        async def astart(self): await asyncio.Event().wait()
    def factory(stage, job_id):
        calls.append(('worker', job_id, stage.task_key)); return Worker()
    result = asyncio.run(run_selected(Coordinator(), context.identity, 1,
        client_factory=lambda: nullcontext(object()), runtime_factory=lambda: None,
        worker_factory=factory, poll_seconds=.01))
    assert result['state'] == 'finished'
    assert [call[0] for call in calls] == ['reserve', 'worker', 'submit']
    assert calls[1][1] == calls[2][1] == item['job_id']
    assert calls[1][2] == calls[2][2]


def test_resume_stopped_uses_rearmed_job_id_without_reserving_again(context):
    job = {'job_id': 'ba630c45-6019-447c-9ec0-5eb4480ef225', 'state': 'reserved'}
    calls = []
    class Coordinator:
        def rearm_stopped(self, job_id, generation, *, client, expected_api_url):
            calls.append(('rearm', job_id, generation, expected_api_url)); return job
        def reserve(self, *_args): pytest.fail('must preserve the original reservation')
        def submit(self, job_id, client, task):
            calls.append(('submit', job_id)); job['state'] = 'finished'
        def job(self, _job_id): return dict(job)
    class Worker:
        finished_task_runs = {}
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        async def astart(self): await asyncio.Event().wait()
    result = asyncio.run(resume_stopped(Coordinator(), job['job_id'], 2,
        client_factory=lambda: nullcontext(object()), expected_api_url='http://prefect.test/api',
        runtime_factory=lambda: None, worker_factory=lambda _task, _id: Worker(), poll_seconds=.01))
    assert result['job_id'] == job['job_id'] and result['state'] == 'finished'
    assert calls == [('rearm', job['job_id'], 2, 'http://prefect.test/api'), ('submit', job['job_id'])]
