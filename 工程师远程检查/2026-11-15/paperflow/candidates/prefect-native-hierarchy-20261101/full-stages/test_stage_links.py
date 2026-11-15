import importlib.util
from contextlib import nullcontext
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import pytest

here = Path(__file__).resolve().parent
root = here.parents[2]
sys.path.insert(0, str(root / 'prefect-paperflow'))
sys.path.insert(0, str(root / 'candidates/llm-new-prefect-20261031/tests'))
from test_upstream_binding import bound
from test_upstream_flow_binding import actual


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, here / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


post = load('candidate_stage_runtime', 'stage_runtime.py')
llm = load('paperflow_prefect.llm_queue.hierarchy_isolated_stage', 'isolated_stage.py')
positions = load('candidate_ticket_positions', 'ticket_positions.py')
upstream = load('paperflow_prefect.llm_queue.hierarchy_upstream_binding', 'upstream_flow_binding.py')


def test_postprocess_stage_accepts_original_folder_and_legacy_history():
    job_id, folder_id = str(uuid4()), str(uuid4())
    item = {'job_id': job_id, 'folder_run_id': folder_id, 'task_key': post.task_key_for_job(job_id)}
    stage = SimpleNamespace(id=UUID(job_id), flow_run_id=UUID(folder_id), task_key=item['task_key'],
                            dynamic_key='paperflow-postprocess-' + job_id, tags=[post.TAG])
    assert post._task_identity(stage, item)
    stage.flow_run_id = None
    assert post._task_identity(stage, item)
    stage.flow_run_id = uuid4()
    assert not post._task_identity(stage, item)


def test_new_postprocess_stage_is_submitted_to_original_folder(tmp_path, monkeypatch):
    database = tmp_path / 'control.db'
    job_id, folder_id = str(uuid4()), str(uuid4())
    item = dict(job_id=job_id, folder_run_id=folder_id, task_run_id=folder_id,
                task_id=str(uuid4()), group_id='a' * 32, paper_id='p', receipt_id='b' * 64,
                attempt_id='a', parent_run_id=str(uuid4()), generation=1, manifest_sha256='c' * 64,
                source_llm_generation=1, source_retry_sha256=None, task_key=post.task_key_for_job(job_id))
    with sqlite3.connect(database) as db:
        db.execute('CREATE TABLE paperflow_postprocess_stage_jobs(job_id TEXT, state TEXT, updated_at_ns INTEGER)')
        db.execute("INSERT INTO paperflow_postprocess_stage_jobs VALUES(?, 'reserved', 0)", (job_id,))
    coordinator = post.Coordinator.__new__(post.Coordinator)
    coordinator.database = database
    coordinator.job = lambda _: item
    coordinator._gates = lambda: None
    coordinator.task_gate = lambda _: nullcontext()
    coordinator._move = lambda *_: None
    monkeypatch.setattr(post.Coordinator, '_check_db', lambda *_args, **_kwargs: None)
    calls = []
    class Client:
        def create_task_run(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(id=kwargs['id'], flow_run_id=kwargs['flow_run_id'],
                                   task_key=item['task_key'], dynamic_key=kwargs['dynamic_key'],
                                   tags=[post.TAG])
    coordinator.submit(job_id, Client(), object())
    assert calls[0]['flow_run_id'] == UUID(folder_id)


def test_llm_stage_accepts_original_folder_and_legacy_history():
    job_id, folder_id = str(uuid4()), str(uuid4())
    item = {'job_id': job_id, 'folder_run_id': folder_id, 'task_key': llm.STAGE_TASK_KEY}
    stage = SimpleNamespace(id=UUID(job_id), flow_run_id=UUID(folder_id), task_key=item['task_key'],
                            dynamic_key='paperflow-llm-' + job_id, tags=[llm.TAG], state=object())
    llm._validate_stage_run(stage, item)
    stage.flow_run_id = None
    llm._validate_stage_run(stage, item)
    stage.flow_run_id = uuid4()
    with pytest.raises(ValueError):
        llm._validate_stage_run(stage, item)


def test_new_llm_stage_is_submitted_to_original_folder(tmp_path, monkeypatch):
    import paperflow_prefect.local_preprocessing as local
    job_id, folder_id = str(uuid4()), str(uuid4())
    item = dict(job_id=job_id, folder_run_id=folder_id, task_id=str(uuid4()), generation=1,
                retry_key='', task_key=llm.STAGE_TASK_KEY)
    monkeypatch.setattr(local, '_control_paths', lambda: (tmp_path / 'control.db', tmp_path))
    monkeypatch.setattr(llm, 'ControlStop', lambda *_: SimpleNamespace(is_set=lambda: False))
    monkeypatch.setattr(llm, '_move', lambda *_: True)
    calls = []
    class Client:
        def create_task_run(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(id=kwargs['id'], flow_run_id=kwargs['flow_run_id'],
                                   task_key=item['task_key'], dynamic_key=kwargs['dynamic_key'], tags=[llm.TAG])
    assert llm._submit(Client(), tmp_path / 'control.db', item)
    assert calls[0]['flow_run_id'] == UUID(folder_id)


def test_folder_display_recognizes_native_llm_stage_and_legacy_history():
    folder_id = str(uuid4())
    identity = dict(task_id=str(uuid4()), group_id='a' * 32, paper_id='p', receipt_id='b' * 64,
                    attempt_id='a', parent_run_id=str(uuid4()), task_run_id=folder_id)
    job_id = str(uuid5(positions.RETRY_NAMESPACE, folder_id + ':1'))
    related = {key: value for key, value in identity.items() if key != 'task_run_id'}
    related.update(job_id=job_id, folder_run_id=folder_id, generation=1, created_at_ns=1,
                   state='finished', retry_key='', task_key='paperflow-folder-llm-stage-v1')
    record = {'related': [related], 'folder': {'generation': 1}}
    stage = SimpleNamespace(id=UUID(job_id), flow_run_id=UUID(folder_id), task_key=related['task_key'],
        dynamic_key='paperflow-llm-' + job_id, tags=['paperflow-llm-stage-v1'],
        state=SimpleNamespace(is_running=lambda: False, is_completed=lambda: True))
    assert positions._llm_handoff(record, identity, None, {job_id: stage}) == 'completed'
    stage.flow_run_id = None
    assert positions._llm_handoff(record, identity, None, {job_id: stage}) == 'completed'
    stage.flow_run_id = uuid4()
    assert positions._llm_handoff(record, identity, None, {job_id: stage}) is None


def test_llm_retry_history_accepts_original_folder_and_rejects_other_parent():
    folder_id, job_id, state_id = str(uuid4()), str(uuid4()), str(uuid4())
    stage = SimpleNamespace(id=UUID(job_id), flow_run_id=UUID(folder_id),
        task_key='paperflow-folder-llm-stage-v1', dynamic_key='paperflow-llm-' + job_id,
        tags=['paperflow-llm-stage-v1'], state=SimpleNamespace(id=UUID(state_id),
            is_failed=lambda: True, is_crashed=lambda: False, is_completed=lambda: True))
    binder = post.RetryAwareUpstream.__new__(post.RetryAwareUpstream)
    binder.upstream = SimpleNamespace(client=SimpleNamespace(read_task_run=lambda _: stage))
    selected = {'prior_failed': [{'job_id': job_id, 'state_id': state_id}], 'prior_stopped': [job_id]}
    binder._verify_prior_failed(selected, folder_id)
    assert binder._verify_prior_stopped(selected, folder_id) == ((job_id, state_id),)
    stage.flow_run_id = None
    binder._verify_prior_failed(selected, folder_id)
    stage.flow_run_id = uuid4()
    with pytest.raises(post.StageConflict):
        binder._verify_prior_failed(selected, folder_id)


def test_running_llm_stage_retains_original_folder_binding(actual):
    base, database, identity, manifest, client, folder, _ = actual
    folder.name = '资料文件夹 · 等待 LLM'
    folder.state = SimpleNamespace(is_final=lambda: False, is_cancelling=lambda: False,
        is_paused=lambda: True, state_details=SimpleNamespace(pause_reschedule=True))
    job_id = str(uuid4())
    stage = SimpleNamespace(id=UUID(job_id), flow_run_id=UUID(identity['task_run_id']),
        task_key=llm.STAGE_TASK_KEY, tags=[llm.TAG], state=SimpleNamespace(is_running=lambda: True))
    client.read_task_run = lambda _: stage
    verifier = upstream.UpstreamFlowBinding(database, database, client, str(client.api_url), 'local-intake',
        base.verify_receipt, receiver_session=base.receiver_session,
        execution_task_run_id=job_id, execution_task_key=llm.STAGE_TASK_KEY)
    assert verifier.load(identity, require_running=True)['manifest'] == manifest
    stage.flow_run_id = None
    assert verifier.load(identity, require_running=True)['manifest'] == manifest
    stage.flow_run_id = uuid4()
    with pytest.raises(InterruptedError):
        verifier.load(identity, require_running=True)
