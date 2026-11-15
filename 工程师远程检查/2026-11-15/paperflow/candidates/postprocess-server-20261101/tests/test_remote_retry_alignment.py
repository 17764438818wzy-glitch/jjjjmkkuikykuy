"""Original-ticket manual retry -> remote coordinator, with synthetic evidence only."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys

import psutil
import pytest

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM = ROOT.parent / 'postprocess-prefect-20261031'
sys.path.insert(0, str(UPSTREAM / 'generation_next' / 'tests'))
from test_remote_runtime_adapter import sample, make
from test_retry_source import install_retry_records
from folder_state import StageConflict as ReceiptConflict
from stage_runtime import StageConflict, reserved_source_matches, retry_source_snapshot


@pytest.mark.parametrize('change', ['none', 'answer', 'prefect_state'])
def test_completed_manual_retry_keeps_original_ticket_across_remote_handoff(sample, change):
    identity = sample['evidence'][0]['identity']
    _, _, _, retry_job, old_state = install_retry_records(sample['database'], identity,
                                                          existing_admissions=True)
    # This represents a completed Prefect state in an in-memory test double;
    # this integration test does not contact a Prefect or server service.
    prefect_state = {'retry_job_id': retry_job, 'state_id': old_state, 'state': 'completed'}
    original_verify = sample['base'].verify_upstream
    def verify(item, generation):
        base = original_verify(item, generation)
        selected = retry_source_snapshot(sample['database'], item, 1)
        if selected is None or prefect_state['state'] != 'completed':
            raise ValueError('synthetic retry is not completed')
        raw = json.dumps(prefect_state, sort_keys=True, separators=(',', ':')).encode()
        return dict(base, source_retry_sha256=selected['sha256'],
                    source_retry_prefect_sha256=hashlib.sha256(raw).hexdigest())
    def observe(item):
        try:
            return reserved_source_matches(sample['database'], item, verify(item, 1))
        except Exception:
            return False
    sample['base'].verify_upstream = verify
    sample['stage_store'].observe_run = observe
    wait = sample['options']['wait_for_result']
    def finish(job):
        result = wait(job)
        if change == 'answer':
            with sqlite3.connect(sample['database']) as db:
                db.execute('UPDATE llm_request_admissions SET response_sha256=? WHERE task_run_id=?',
                           ('f' * 64, identity['task_run_id']))
        elif change == 'prefect_state':
            prefect_state['state_id'] = 'synthetic-changed-state'
        return result
    sample['options']['wait_for_result'] = finish
    remote = make(sample)
    job = remote.reserve(identity, 1)
    assert job['source_retry_sha256'] == retry_source_snapshot(sample['database'], identity, 1)['sha256']
    assert job['source_retry_prefect_sha256'] == verify(identity, 1)['source_retry_prefect_sha256']
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    sample['wait_release'].set()
    if change == 'none':
        result = asyncio.run(remote.execute(job['job_id'], job['job_id']))
        assert result['state'] == 'waiting_rag'
        assert result['identity'] == identity
    else:
        with pytest.raises(Exception):
            asyncio.run(remote.execute(job['job_id'], job['job_id']))
        with sqlite3.connect(sample['database']) as db:
            assert db.execute("SELECT COUNT(*) FROM postprocessing_claims WHERE state='waiting_rag'").fetchone()[0] == 0
            assert db.execute("SELECT COUNT(*) FROM postprocessing_results WHERE state='passed'").fetchone()[0] == 0
    assert sample['journal'].get_transfer(job['job_id'])['job']['identity'] == identity
    assert sample['journal'].get(job['job_id'])['job']['identity'] == identity


@pytest.mark.parametrize('field,value', [
    ('source_llm_generation', 2),
    ('source_retry_sha256', 'a' * 64),
    ('source_retry_prefect_sha256', 'b' * 64),
])
def test_stage_binding_rejects_changed_frozen_retry_identity(sample, field, value):
    remote = make(sample)
    identity = sample['evidence'][0]['identity']
    stage = remote.reserve(identity, 1)
    server_job = dict(job_id=stage['job_id'], identity=identity, generation=1,
                      manifest_sha256=stage['manifest_sha256'])
    with sqlite3.connect(sample['database']) as db:
        db.execute('UPDATE paperflow_postprocess_stage_jobs SET state=?,owner_pid=?,owner_started=?,'
                   + field + '=? WHERE job_id=?',
                   ('executing', os.getpid(), psutil.Process(os.getpid()).create_time(), value, stage['job_id']))
    with pytest.raises(StageConflict):
        remote._stage_binding(stage, server_job)


def test_non_retry_ticket_with_migrated_empty_proofs_keeps_original_binding(sample):
    remote = make(sample)
    identity = sample['evidence'][0]['identity']
    stage = remote.reserve(identity, 1)
    assert stage['source_retry_sha256'] is None and stage['source_retry_prefect_sha256'] is None
    server_job = dict(job_id=stage['job_id'], identity=identity, generation=1,
                      manifest_sha256=stage['manifest_sha256'])
    with sqlite3.connect(sample['database']) as db:
        db.execute('UPDATE paperflow_postprocess_stage_jobs SET state=?,owner_pid=?,owner_started=? WHERE job_id=?',
                   ('executing', os.getpid(), psutil.Process(os.getpid()).create_time(), stage['job_id']))
    assert remote._stage_binding(stage, server_job)['job_id'] == stage['job_id']


def test_unmigrated_stage_without_retry_proofs_is_not_assumed_safe(sample):
    remote = make(sample)
    identity = sample['evidence'][0]['identity']
    stage = remote.reserve(identity, 1)
    server_job = dict(job_id=stage['job_id'], identity=identity, generation=1,
                      manifest_sha256=stage['manifest_sha256'])
    with sqlite3.connect(sample['database']) as db:
        db.execute('UPDATE paperflow_postprocess_stage_jobs SET state=?,owner_pid=?,owner_started=? WHERE job_id=?',
                   ('executing', os.getpid(), psutil.Process(os.getpid()).create_time(), stage['job_id']))
        db.execute('ALTER TABLE paperflow_postprocess_stage_jobs DROP COLUMN source_retry_prefect_sha256')
    with pytest.raises(StageConflict):
        remote._stage_binding(stage, server_job)


def test_legacy_non_retry_stage_keeps_original_order_after_null_proof_migration(sample):
    remote = make(sample)
    identity = sample['evidence'][0]['identity']
    stage = remote.reserve(identity, 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute('ALTER TABLE paperflow_postprocess_stage_jobs DROP COLUMN source_retry_sha256')
        db.execute('ALTER TABLE paperflow_postprocess_stage_jobs DROP COLUMN source_retry_prefect_sha256')
    # The database is the disposable synthetic fixture, not the live control DB.
    sample['base'].initialize()
    migrated = remote.job(stage['job_id'])
    assert all(migrated[key] == stage[key] for key in ('job_id', 'task_id', 'group_id', 'paper_id',
        'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id', 'folder_run_id', 'generation', 'manifest_sha256'))
    assert migrated['source_retry_sha256'] is None and migrated['source_retry_prefect_sha256'] is None
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (stage['job_id'],))
    sample['wait_release'].set()
    result = asyncio.run(remote.execute(stage['job_id'], stage['job_id']))
    assert result['state'] == 'waiting_rag' and result['identity'] == identity
    assert [kind for kind, _ in sample['events']].count('submit') == 1


def test_nonempty_legacy_stage_without_source_generation_is_not_guessed(sample):
    remote = make(sample)
    identity = sample['evidence'][0]['identity']
    stage = remote.reserve(identity, 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute('ALTER TABLE paperflow_postprocess_stage_jobs DROP COLUMN source_retry_sha256')
        db.execute('ALTER TABLE paperflow_postprocess_stage_jobs DROP COLUMN source_retry_prefect_sha256')
        db.execute('ALTER TABLE paperflow_postprocess_stage_jobs DROP COLUMN source_llm_generation')
    with pytest.raises(StageConflict):
        sample['base'].initialize()
    with sqlite3.connect(sample['database']) as db:
        columns = {row[1] for row in db.execute('PRAGMA table_info(paperflow_postprocess_stage_jobs)')}
        assert 'source_llm_generation' not in columns and 'source_retry_sha256' not in columns
        assert db.execute('SELECT job_id FROM paperflow_postprocess_stage_jobs').fetchone() == (stage['job_id'],)
    assert sample['events'] == []


def test_retry_source_changed_after_execution_check_never_reaches_remote_start(sample):
    identity = sample['evidence'][0]['identity']
    install_retry_records(sample['database'], identity, existing_admissions=True)
    original_verify = sample['base'].verify_upstream
    def verify(item, generation):
        base = original_verify(item, generation)
        selected = retry_source_snapshot(sample['database'], item, 1)
        return dict(base, source_retry_sha256=selected['sha256'], source_retry_prefect_sha256='9' * 64)
    def observe(item):
        try:
            return reserved_source_matches(sample['database'], item, verify(item, 1))
        except Exception:
            return False
    sample['base'].verify_upstream = verify
    sample['stage_store'].observe_run = observe
    changed = [False]
    def verify_execution(item, context):
        assert context == item['job_id']
        if not changed[0] and any(event[0] == 'transfer' for event in sample['events']):
            # Simulate the source changing immediately after an otherwise
            # successful external Prefect observation. The synthetic stage
            # row is also changed to match, without changing its timestamp.
            with sqlite3.connect(sample['database']) as db:
                db.execute('UPDATE llm_request_admissions SET response_sha256=? WHERE task_run_id=?',
                           ('f' * 64, identity['task_run_id']))
            current = retry_source_snapshot(sample['database'], identity, 1)['sha256']
            with sqlite3.connect(sample['database']) as db:
                db.execute('UPDATE paperflow_postprocess_stage_jobs SET source_retry_sha256=? WHERE job_id=?',
                           (current, item['job_id']))
            changed[0] = True
        return True
    sample['base'].verify_execution = verify_execution
    remote = make(sample)
    stage = remote.reserve(identity, 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (stage['job_id'],))
    sample['wait_release'].set()
    with pytest.raises(StageConflict):
        asyncio.run(remote.execute(stage['job_id'], stage['job_id']))
    assert changed[0] is True
    assert [kind for kind, _ in sample['events']].count('transfer') == 1
    assert not any(kind == 'submit' for kind, _ in sample['events'])
    assert sample['journal'].get(stage['job_id']) is None


def test_prefect_change_after_observation_may_start_but_never_accepts_stale_receipt(sample):
    """Independent services cannot be made atomic by comparing local fields."""
    identity = sample['evidence'][0]['identity']
    install_retry_records(sample['database'], identity, existing_admissions=True)
    prefect_state = {'state_id': 'before'}
    original_verify = sample['base'].verify_upstream
    def verify(item, generation):
        base = original_verify(item, generation)
        selected = retry_source_snapshot(sample['database'], item, 1)
        proof = hashlib.sha256(prefect_state['state_id'].encode()).hexdigest()
        return dict(base, source_retry_sha256=selected['sha256'], source_retry_prefect_sha256=proof)
    def observe(item):
        try:
            return reserved_source_matches(sample['database'], item, verify(item, 1))
        except Exception:
            return False
    sample['base'].verify_upstream = verify
    sample['stage_store'].observe_run = observe
    changed = [False]
    def verify_execution(item, context):
        assert context == item['job_id']
        if not changed[0] and any(event[0] == 'transfer' for event in sample['events']):
            # Simulate a separate Prefect state changing just after an
            # otherwise successful external observation returns.
            prefect_state['state_id'] = 'after'
            changed[0] = True
        return True
    sample['base'].verify_execution = verify_execution
    remote = make(sample)
    stage = remote.reserve(identity, 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (stage['job_id'],))
    sample['wait_release'].set()
    with pytest.raises(ReceiptConflict):
        asyncio.run(remote.execute(stage['job_id'], stage['job_id']))
    assert changed[0] is True
    assert [kind for kind, _ in sample['events']].count('submit') == 1
    with sqlite3.connect(sample['database']) as db:
        assert db.execute("SELECT COUNT(*) FROM postprocessing_claims WHERE state='waiting_rag'").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM postprocessing_results WHERE state='passed'").fetchone()[0] == 0
