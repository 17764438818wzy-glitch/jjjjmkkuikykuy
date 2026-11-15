"""Resume only the same real folder on an explicit task start."""
from contextlib import closing
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from uuid import uuid4

from prefect.client.schemas.responses import SetStateStatus
from prefect.states import Paused

from test_real_folder_no_placeholder import FakePrefect, FakeQueue, HERE, module

orders = module('flow_folder_orders', HERE / 'flow_prefect_orders.py')


def context(tmp_path, monkeypatch, phase='等待 LLM'):
    task_id, group_id = str(uuid4()), uuid4().hex
    client, queue = FakePrefect(task_id), FakeQueue(tmp_path / 'local.db')
    control = tmp_path / 'control.db'
    with closing(sqlite3.connect(control)) as db, db:
        db.executescript('CREATE TABLE tasks(task_id TEXT,prefect_run_id TEXT,prefect_order_model TEXT);'
                         'CREATE TABLE task_deletions(task_id TEXT);')
        db.execute('INSERT INTO tasks VALUES(?,?,?)', (task_id, str(client.root.id), 'v2'))
    (tmp_path / 'control-locks').mkdir()
    params = {'task_id':task_id, 'group_id':group_id, 'receipt_id':'a' * 64, 'attempt_id':'attempt-1'}
    run = client.create_flow_run_from_deployment(client.folder_deployment.id, parameters=params,
        idempotency_key='folder-waiting-v2-' + task_id + '-' + group_id,
        work_queue_name='task-' + task_id, name='资料文件夹 甲 · ' + phase,
        state=Paused(name='Suspended', reschedule=True))
    item = {**params, 'prefect_run_id':str(run.id)}
    service = orders.FolderOrderService(queue, prefect_api_url='http://new-prefect/api',
        first_step_index=control, first_step_data_root=tmp_path, first_step_receipt_root=tmp_path)
    current = {'prefect_run_id':str(client.root.id), 'desired_state':'paused',
               'start_pending':0, 'control_revision':1}
    monkeypatch.setattr(service, '_client', lambda: client)
    monkeypatch.setattr(service, '_task', lambda _: dict(current))
    resumed = []
    client.resume_flow_run = lambda value: resumed.append(value) or SimpleNamespace(status=SetStateStatus.ACCEPT)
    return service, client, item, current, resumed, run


def test_pause_prevents_resume_then_new_task_revision_wakes_same_real_folder(tmp_path, monkeypatch):
    service, _, item, current, resumed, run = context(tmp_path, monkeypatch, '等待任务开始')
    service._reconcile_ready([item])
    assert not resumed
    current.update(desired_state='running', control_revision=2)
    service._reconcile_ready([item])
    assert resumed == [run.id]


def test_waiting_postprocessing_is_never_resumed(tmp_path, monkeypatch):
    service, _, item, current, resumed, _ = context(tmp_path, monkeypatch, '等待后处理')
    current.update(desired_state='running', control_revision=2)
    service._reconcile_ready([item])
    assert resumed == []


def test_pause_winning_the_resume_gate_blocks_dispatch(tmp_path, monkeypatch):
    service, _, item, current, resumed, _ = context(tmp_path, monkeypatch)
    current.update(desired_state='running', control_revision=2)
    calls = []
    def changing(_):
        calls.append(1)
        if len(calls) > 1:
            current.update(desired_state='paused', control_revision=3)
        return dict(current)
    monkeypatch.setattr(service, '_task', changing)
    service._reconcile_ready([item])
    assert not resumed and len(calls) >= 2
