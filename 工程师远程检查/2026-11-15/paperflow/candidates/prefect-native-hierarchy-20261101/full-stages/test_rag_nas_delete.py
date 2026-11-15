import importlib
from pathlib import Path
import sqlite3
import sys
from types import ModuleType, SimpleNamespace
from uuid import UUID, uuid4, uuid5

import pytest


here = Path(__file__).resolve().parent
root = here.parents[2]
package = ModuleType('_downstream_control_candidate')
package.__path__ = [str(here), str(root / 'candidates/first-step-control-20261029/paperflow_prefect_v2'),
                    str(root / 'candidates/first-step-control-20261029/paperflow_prefect')]
sys.modules[package.__name__] = package
scheduler = importlib.import_module(package.__name__ + '.first_step_scheduler')
delete = importlib.import_module(package.__name__ + '.first_step_delete')
NAMESPACE = UUID('ccf886ef-7347-4204-9a83-4fa33586d046')


@pytest.mark.parametrize('scenario', ['normal', 'native', 'wrong_folder', 'wrong_llm_folder',
                                      'wrong_post_folder', 'running', 'wrong_id', 'unknown_missing'])
def test_delete_cancels_only_the_original_folder_downstream_tasks(monkeypatch, scenario):
    from prefect.exceptions import ObjectNotFound

    task_id, root_id, folder_id = str(uuid4()), str(uuid4()), str(uuid4())
    group, receipt, attempt = uuid4().hex, 'a' * 64, 'attempt'
    order = dict(task_id=task_id, group_id=group, receipt_id=receipt, attempt_id=attempt,
                 prefect_run_id=folder_id, folder_label='synthetic')
    ids = {kind: str(uuid5(NAMESPACE, folder_id + ':' + kind)) for kind in ('rag', 'nas')}
    job = dict(task_id=task_id, group_id=group, receipt_id=receipt, attempt_id=attempt,
               parent_run_id=root_id, task_run_id=folder_id, rag_task_run_id=ids['rag'],
               nas_task_run_id=ids['nas'], rag_submission='submitted', nas_submission='reserved')
    if scenario == 'wrong_id':
        job['rag_task_run_id'] = str(uuid4())
    if scenario == 'unknown_missing':
        job['rag_submission'] = 'unknown'

    class State:
        def __init__(self, value): self.value = value
        def is_final(self): return self.value == 'final'
        def is_scheduled(self): return self.value == 'scheduled'
        def is_pending(self): return False
        def is_paused(self): return self.value == 'paused'

    root_deploy, folder_deploy = uuid4(), uuid4()
    parent = SimpleNamespace(id=UUID(root_id), deployment_id=root_deploy, parameters={'task_id': task_id},
        idempotency_key='first-step-order-v2-' + task_id, parent_task_run_id=None, state=State('paused'))
    folder = SimpleNamespace(id=UUID(folder_id), deployment_id=folder_deploy, parameters={
        key: order[key] for key in ('task_id', 'group_id', 'receipt_id', 'attempt_id')},
        idempotency_key='folder-waiting-v2-' + task_id + '-' + group, parent_task_run_id=None,
        work_queue_name='task-' + task_id, state=State('paused'))
    rag_id = UUID(ids['rag'])
    rag = SimpleNamespace(id=rag_id, flow_run_id=None, task_key='paperflow-folder-rag-stage-v1-' + rag_id.hex,
        dynamic_key='paperflow-rag-' + ids['rag'], tags=['paperflow-rag-stage-v1'],
        state=State('running' if scenario == 'running' else 'scheduled'))
    if scenario == 'native':
        rag.flow_run_id = UUID(folder_id)
    elif scenario == 'wrong_folder':
        rag.flow_run_id = uuid4()
    llm_id = uuid4()
    post_id = uuid5(UUID('5e9b7732-458e-50cb-8916-6b53007c204f'), folder_id)
    llm_job = dict(task_id=task_id, group_id=group, receipt_id=receipt, attempt_id=attempt,
        parent_run_id=root_id, folder_run_id=folder_id, job_id=str(llm_id),
        task_key='paperflow-folder-llm-stage-v1', state='submitted')
    post_job = dict(task_id=task_id, group_id=group, receipt_id=receipt, attempt_id=attempt,
        parent_run_id=root_id, folder_run_id=folder_id, task_run_id=folder_id, job_id=str(post_id),
        task_key=scheduler.postprocess_task_key(str(post_id)), state='submitted')
    parent_id = UUID(folder_id) if scenario == 'native' else None
    llm = SimpleNamespace(id=llm_id, flow_run_id=uuid4() if scenario == 'wrong_llm_folder' else parent_id,
        task_key=llm_job['task_key'], dynamic_key='paperflow-llm-' + str(llm_id),
        tags=['paperflow-llm-stage-v1'], state=State('scheduled'))
    post = SimpleNamespace(id=post_id, flow_run_id=uuid4() if scenario == 'wrong_post_folder' else parent_id,
        task_key=post_job['task_key'], dynamic_key='paperflow-postprocess-' + str(post_id),
        tags=['paperflow-postprocess-stage-v1'], state=State('scheduled'))
    stages = {stage.id: stage for stage in (llm, post, rag)}
    cancelled = []

    class Client:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def read_deployment_by_name(self, name):
            return (SimpleNamespace(id=folder_deploy, work_pool_name='paperflow-unified-preprocessing',
                    tags=['paperflow-order-v2-live-20261031']) if '资料文件夹' in name else
                    SimpleNamespace(id=root_deploy, tags=['paperflow-order-v2-live-20261031']))
        def read_flow_run(self, run_id): return parent if run_id == parent.id else folder
        def read_work_pool(self, name):
            return SimpleNamespace(type='process', description='paperflow-order-v2-live-20261031')
        def read_work_queue_by_name(self, *args, **kwargs): return SimpleNamespace(is_paused=True)
        def read_flow_runs(self, flow_run_filter, limit):
            return [run for run in (parent, folder) if run.id in flow_run_filter.id.any_]
        def read_task_run(self, run_id):
            if run_id not in stages or (run_id == rag_id and scenario == 'unknown_missing'):
                raise ObjectNotFound(Exception('synthetic missing task'))
            return stages[run_id]
        def set_task_run_state(self, run_id, state, force=False):
            cancelled.append(run_id)
            stages[run_id].state = State('final')

    sched = scheduler.PrefectScheduler('http://synthetic.invalid')
    monkeypatch.setattr(sched, '_client', lambda *args, **kwargs: Client())
    if scenario == 'running':
        monkeypatch.setattr(scheduler.time, 'sleep', lambda _: setattr(rag, 'state', State('final')))
    def request(path, payload, timeout):
        for run in (parent, folder): run.state = State('final')
        return {'results': [{'flow_run_id': value} for value in payload['flow_runs']['id']['any_']]}
    monkeypatch.setattr(sched, '_request', request)
    if scenario in ('normal', 'native', 'running'):
        sched.cancel_task(task_id, root_id, [order], stage_jobs=[llm_job], postprocess_jobs=[post_job],
                          downstream_jobs=[job])
        assert cancelled == ([llm_id, post_id] if scenario == 'running' else [llm_id, post_id, rag_id])
    else:
        with pytest.raises(scheduler.SchedulerNotSubmitted):
            sched.cancel_task(task_id, root_id, [order], stage_jobs=[llm_job], postprocess_jobs=[post_job],
                              downstream_jobs=[job])
        assert not cancelled


def test_existing_delete_passes_downstream_orders_to_scheduler(tmp_path):
    database, local = tmp_path / 'control.sqlite3', tmp_path / 'local.sqlite3'
    with sqlite3.connect(database) as db:
        db.executescript('''CREATE TABLE paperflow_postprocess_stage_jobs(task_id TEXT, state TEXT, updated_at_ns INTEGER);
            CREATE TABLE paperflow_rag_nas_jobs(task_id TEXT, group_id TEXT);
            INSERT INTO paperflow_rag_nas_jobs VALUES('task','group');''')
    with sqlite3.connect(local) as db:
        db.execute('''CREATE TABLE preprocessing_folder_orders(task_id TEXT,group_id TEXT,receipt_id TEXT,
                   attempt_id TEXT,prefect_run_id TEXT,folder_label TEXT)''')
    def connect():
        db = sqlite3.connect(database)
        db.row_factory = sqlite3.Row
        return db
    calls = []
    control = SimpleNamespace(summary=SimpleNamespace(_connect=connect),
        _task=lambda db, task_id: {'prefect_run_id': 'parent'}, local_intake_queue_db=local,
        scheduler=SimpleNamespace(cancel_task=lambda *args, **kwargs: calls.append(kwargs)))
    delete._cancel_prefect_orders(control, 'task')
    assert calls[0]['downstream_jobs'] == [{'task_id': 'task', 'group_id': 'group'}]


@pytest.mark.parametrize(('state', 'submission'), [('running', 'submitted'), ('waiting', 'submitting')])
def test_delete_waits_for_downstream_effects_to_finish(tmp_path, monkeypatch, state, submission):
    database = tmp_path / 'control.sqlite3'
    with sqlite3.connect(database) as db:
        db.executescript('''CREATE TABLE control_groups(task_id TEXT,state TEXT);
            CREATE TABLE model_decisions(task_id TEXT,state TEXT);
            CREATE TABLE paperflow_rag_nas_jobs(task_id TEXT,state TEXT,rag_submission TEXT,nas_submission TEXT);''')
        db.execute('INSERT INTO paperflow_rag_nas_jobs VALUES(?,?,?,?)', ('task', state, submission, 'reserved'))
    def connect():
        return sqlite3.connect(database)
    control = SimpleNamespace(summary=SimpleNamespace(_connect=connect),
        _recover_stale_groups=lambda *_: None, _task=lambda *_: {'start_pending': False},
        has_upload_transfers=lambda *_: False)
    admission = importlib.import_module(package.__name__ + '.llm_admission_state')
    monkeypatch.setattr(admission, 'counts', lambda *_: dict.fromkeys(
        ('unknown', 'stage_unknown', 'recovery_unknown', 'inflight', 'stage_inflight'), 0))
    waits = []
    def finish(_):
        waits.append(True)
        with sqlite3.connect(database) as db:
            db.execute("UPDATE paperflow_rag_nas_jobs SET state='cancelled',rag_submission='submitted'")
    monkeypatch.setattr(delete.time, 'sleep', finish)
    delete._wait_first_step(control, 'task')
    assert waits == [True]


def test_delete_keeps_unconfirmed_downstream_records(tmp_path, monkeypatch):
    database = tmp_path / 'control.sqlite3'
    with sqlite3.connect(database) as db:
        db.execute('CREATE TABLE paperflow_rag_nas_jobs(task_id TEXT,state TEXT,rag_submission TEXT,nas_submission TEXT)')
        db.execute("INSERT INTO paperflow_rag_nas_jobs VALUES('task','unknown','submitted','reserved')")
    control = SimpleNamespace(summary=SimpleNamespace(_connect=lambda: sqlite3.connect(database)),
        _recover_stale_groups=lambda *_: None, _task=lambda *_: {'start_pending': False})
    admission = importlib.import_module(package.__name__ + '.llm_admission_state')
    monkeypatch.setattr(admission, 'counts', lambda *_: dict.fromkeys(
        ('unknown', 'stage_unknown', 'recovery_unknown', 'inflight', 'stage_inflight'), 0))
    with pytest.raises(delete.ControlError, match='RAG'):
        delete._wait_first_step(control, 'task')
    with sqlite3.connect(database) as db:
        assert db.execute('SELECT state FROM paperflow_rag_nas_jobs').fetchone() == ('unknown',)


