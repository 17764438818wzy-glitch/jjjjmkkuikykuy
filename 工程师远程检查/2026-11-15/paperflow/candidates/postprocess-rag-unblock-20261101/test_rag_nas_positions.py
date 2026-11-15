"""The same Prefect folder appears in exactly one downstream position."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid5

import pytest

spec = importlib.util.spec_from_file_location('positions_candidate', Path(__file__).with_name('ticket_positions.py'))
positions = importlib.util.module_from_spec(spec)
spec.loader.exec_module(positions)

FIELDS = positions.FIELDS
NAMESPACE = UUID('ccf886ef-7347-4204-9a83-4fa33586d046')
identity = dict(zip(FIELDS, [str(UUID(int=i)) for i in range(1, 8)]))
job = {'generation': 4, 'job_id': 'post-job', 'manifest_sha256': 'manifest'}
claim = {'claim_id': 'claim'}
result = {'stage_receipt_sha256': 'receipt'}


def task(kind, state):
    task_id = uuid5(NAMESPACE, identity['task_run_id'] + ':' + kind)
    class State:
        def is_running(self): return state == 'running'
        def is_completed(self): return state == 'completed'
        def is_scheduled(self): return state == 'scheduled'
        def is_pending(self): return state == 'pending'
        def is_paused(self): return state == 'paused'
    return SimpleNamespace(id=task_id, flow_run_id=None,
        task_key=f'paperflow-folder-{kind}-stage-v1-{task_id.hex}',
        dynamic_key=f'paperflow-{kind}-{task_id}', tags=[f'paperflow-{kind}-stage-v1'], state=State())


def row(stage, state):
    return {**identity, 'source_generation': 4, 'stage': stage, 'state': state,
        'claim_id': 'claim', 'postprocess_job_id': 'post-job',
        'upstream_manifest_sha256': 'manifest', 'postprocess_receipt_sha256': 'receipt',
        'rag_task_run_id': str(uuid5(NAMESPACE, identity['task_run_id'] + ':rag')),
        'nas_task_run_id': str(uuid5(NAMESPACE, identity['task_run_id'] + ':nas')),
        'rag_receipt_path': 'rag.json', 'nas_receipt_path': 'nas.json',
        'cleanup_receipt_path': 'cleanup.json', 'completed_at_ns': 1}


@pytest.mark.parametrize(('stage','state','rag','nas','expected'), [
    ('rag', 'waiting', None, None, 'waiting_rag_upload'),
    ('rag', 'waiting', 'scheduled', None, 'waiting_rag_upload'),
    ('rag', 'running', 'running', None, 'rag_in_progress'),
    ('rag', 'paused', 'running', None, 'waiting_rag_upload'),
    ('rag', 'waiting', None, 'running', None),
    ('rag', 'waiting', None, 'failed', None),
    ('rag', 'running', 'running', 'running', None),
    ('rag', 'completed', 'completed', 'running', 'nas_in_progress'),
    ('rag', 'completed', 'completed', 'completed', None),
    ('nas', 'waiting', 'completed', None, 'waiting_nas_archive_delete'),
    ('nas', 'running', 'completed', 'running', 'nas_in_progress'),
    ('nas', 'paused', 'completed', 'running', 'waiting_nas_archive_delete'),
    ('completed', 'completed', 'completed', 'completed', 'completed'),
    ('rag', 'unknown', None, None, None),
    ('rag', 'needs_review', 'failed', None, None),
    ('completed', 'completed', 'running', 'completed', None),
])
def test_downstream_position(stage, state, rag, nas, expected):
    runs = {}
    for kind, status in (('rag', rag), ('nas', nas)):
        if status:
            value = task(kind, status)
            runs[str(value.id)] = value
    assert positions._downstream_position(row(stage, state), identity, job, claim, result, runs) == expected


def test_mismatched_identity_and_missing_receipt_are_not_completed():
    rag, nas = task('rag', 'completed'), task('nas', 'completed')
    runs = {str(rag.id): rag, str(nas.id): nas}
    value = row('completed', 'completed')
    value['cleanup_receipt_path'] = None
    assert positions._downstream_position(value, identity, job, claim, result, runs) is None
    value = row('completed', 'completed')
    value['group_id'] = 'another-folder'
    assert positions._downstream_position(value, identity, job, claim, result, runs) is None


def test_prefect_rag_position_keeps_original_postprocessing_pass():
    class FlowState:
        def __init__(self, paused=False):
            self.paused = paused
            self.state_details = SimpleNamespace(pause_reschedule=paused)
        def is_final(self): return False
        def is_cancelling(self): return False
        def is_failed(self): return False
        def is_crashed(self): return False
        def is_paused(self): return self.paused

    root_deployment, folder_deployment = SimpleNamespace(id='root'), SimpleNamespace(id='folder')
    parent = SimpleNamespace(deployment_id='root', parent_task_run_id=None, state=FlowState(),
        idempotency_key='first-step-order-v2-' + identity['task_id'], parameters={'task_id': identity['task_id']})
    folder = SimpleNamespace(deployment_id='folder', parent_task_run_id=None, state=FlowState(paused=True),
        idempotency_key='folder-waiting-v2-' + identity['task_id'] + '-' + identity['group_id'],
        work_queue_name='task-' + identity['task_id'], parameters={key: identity[key] for key in
            ('task_id', 'group_id', 'receipt_id', 'attempt_id')})
    runtime = SimpleNamespace(_identity=lambda item: item, _job_id=lambda _: job['job_id'],
        task_key_for_job=lambda _: 'post-key', _task_identity=lambda *_: True, StageConflict=ValueError)
    record = {'folder': {**identity, 'state': 'waiting_postprocessing', 'generation': 2,
                         'manifest_sha256': 'manifest'},
        'task': {'prefect_order_model': 'v2', 'prefect_run_id': identity['parent_run_id']},
        'order': {'prefect_run_id': identity['task_run_id'], 'receipt_id': identity['receipt_id'],
                  'attempt_id': identity['attempt_id']},
        'job': {**identity, **job, 'folder_run_id': identity['task_run_id'], 'task_key': 'post-key',
                'state': 'finished', 'source_llm_generation': 2},
        'claim': {**identity, **claim, 'state': 'waiting_rag', 'manifest_sha256': 'manifest'},
        'result': {**identity, **result, 'claim_id': 'claim', 'state': 'passed', 'code': 'passed',
                   'manifest_sha256': 'manifest'},
        'admissions': [{'kind': 'body', 'state': 'raw_complete', 'receipt_id': identity['receipt_id'],
                        'generation': 2}], 'related': [], 'retries': [], 'downstream': row('rag', 'running')}
    rag = task('rag', 'running')
    stage = SimpleNamespace(state=SimpleNamespace(is_running=lambda: False, is_completed=lambda: True))
    current, outcome = positions._position(SimpleNamespace(runtime=runtime), record,
        {identity['parent_run_id']: parent, identity['task_run_id']: folder},
        {job['job_id']: stage, str(rag.id): rag}, root_deployment, folder_deployment)
    assert (current, outcome) == ('rag_in_progress', 'passed')
    record['downstream'] = row('rag', 'needs_review')
    failed_rag = task('rag', 'failed')
    current, outcome = positions._position(SimpleNamespace(runtime=runtime), record,
        {identity['parent_run_id']: parent, identity['task_run_id']: folder},
        {job['job_id']: stage, str(failed_rag.id): failed_rag}, root_deployment, folder_deployment)
    assert (current, outcome) == (None, 'passed')
