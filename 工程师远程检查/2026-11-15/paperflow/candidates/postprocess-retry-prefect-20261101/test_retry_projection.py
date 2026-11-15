import importlib.util
from pathlib import Path
import sqlite3
import sys
from uuid import UUID, uuid4, uuid5
from types import SimpleNamespace

import pytest

root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(root / 'candidates/postprocess-prefect-20261031/projection_next/tests'))
from test_projections import modules as base_modules, sample, insert_job, State


@pytest.fixture
def modules():
    result = base_modules.__wrapped__()
    spec = importlib.util.spec_from_file_location('retry_projections', Path(__file__).with_name('projections.py'))
    result.projection = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result.projection)
    return result


def test_retried_small_order_is_visible_during_pause_while_current_stage_finishes(sample):
    State.id = uuid4()
    State.is_failed = lambda self: self.kind == 'FAILED'
    State.is_crashed = lambda self: self.kind == 'CRASHED'
    old_id = insert_job(sample, 'executing')
    new_id = str(uuid4())
    stage = sample.client.stages.pop(old_id)
    stage.id = UUID(new_id)
    stage.task_key = sample.modules.runtime.task_key_for_job(new_id)
    stage.dynamic_key = 'paperflow-postprocess-' + new_id
    sample.client.stages[new_id] = stage
    with sqlite3.connect(sample.control) as db:
        db.execute('UPDATE paperflow_postprocess_stage_jobs SET job_id=?, task_key=?', (new_id, stage.task_key))
        db.execute("UPDATE tasks SET desired_state='paused'")
    sample.projector._observe(sample.queue.snapshot())
    projection = sample.projector.stage_projection(sample.queue.snapshot())
    assert projection['available'] is True, projection
    assert projection['counts']['postprocessing_in_progress'] == 1
    assert projection['counts']['waiting_postprocessing'] == 0


@pytest.mark.parametrize(('kind', 'state', 'expected'), [
    ('rag', 'waiting', 'waiting_rag_upload'), ('rag', 'running', 'rag_in_progress'),
    ('nas', 'waiting', 'waiting_nas_archive_delete'), ('nas', 'running', 'nas_in_progress')])
def test_submitted_but_unclaimed_downstream_stage_is_shown_waiting(kind, state, expected):
    spec = importlib.util.spec_from_file_location('retry_positions', Path(__file__).with_name('ticket_positions.py'))
    positions = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(positions)
    identity = dict(task_id=str(uuid4()), group_id='a' * 32, paper_id='paper', receipt_id='b' * 64,
                    attempt_id='attempt', parent_run_id=str(uuid4()), task_run_id=str(uuid4()))
    job = dict(generation=1, job_id=str(uuid4()), manifest_sha256='c' * 64)
    claim, result = {'claim_id': str(uuid4())}, {'stage_receipt_sha256': 'd' * 64}
    row = {**identity, 'source_generation': 1, 'claim_id': claim['claim_id'], 'postprocess_job_id': job['job_id'],
           'upstream_manifest_sha256': job['manifest_sha256'], 'postprocess_receipt_sha256': result['stage_receipt_sha256'],
           'stage': kind, 'state': state, 'rag_receipt_path': 'receipt' if kind == 'nas' else None}
    class FakeState:
        def __init__(self, name): self.name = name
        def is_running(self): return self.name == 'running'
        def is_completed(self): return self.name == 'completed'
        def is_scheduled(self): return self.name == 'scheduled'
        def is_pending(self): return False
        def is_paused(self): return False
    runs = {}
    for stage in ('rag', 'nas'):
        run_id = str(uuid5(positions.DOWNSTREAM_NAMESPACE, identity['task_run_id'] + ':' + stage))
        row[stage + '_task_run_id'] = run_id
        if stage == kind or (kind == 'nas' and stage == 'rag'):
            runs[run_id] = SimpleNamespace(id=UUID(run_id), flow_run_id=UUID(identity['task_run_id']),
                task_key=f'paperflow-folder-{stage}-stage-v1-{UUID(run_id).hex}',
                dynamic_key=f'paperflow-{stage}-{run_id}', tags=[f'paperflow-{stage}-stage-v1'],
                state=FakeState('completed' if kind == 'nas' and stage == 'rag' else 'running'))
    assert positions._downstream_position(row, identity, job, claim, result, runs) == expected
