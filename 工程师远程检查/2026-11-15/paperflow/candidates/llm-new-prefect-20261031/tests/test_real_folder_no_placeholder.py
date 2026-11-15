"""Folder orders use actual flows and never create fake TaskRuns."""
from contextlib import closing, nullcontext
import importlib.util
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from uuid import uuid4

import pytest
from prefect.states import Cancelled, Paused, Scheduled
from prefect.client.schemas.responses import SetStateStatus


HERE = Path(__file__).resolve().parents[1]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


orders = module('folder_orders_no_placeholder', HERE / 'prefect_orders_no_placeholder.py')
scheduler_module = module('scheduler_no_placeholder', HERE / 'control_candidate/first_step_scheduler.py')


class FakeQueue:
    def __init__(self, path):
        self.path = path
        self.consumer_id = 'test'
        with self._connect() as db:
            db.executescript('''
                CREATE TABLE cancelled_tasks (task_id TEXT PRIMARY KEY);
                CREATE TABLE preprocessing_folder_orders (
                    task_id TEXT, group_id TEXT, receipt_id TEXT, attempt_id TEXT,
                    folder_label TEXT, created_at TEXT, prefect_run_id TEXT,
                    UNIQUE(task_id,group_id));
            ''')

    def _connect(self):
        db = sqlite3.connect(self.path)
        db.row_factory = sqlite3.Row
        return db


class FakePrefect:
    def __init__(self, task_id):
        self.task_id = task_id
        self.root_deployment = SimpleNamespace(id=uuid4(), tags=[orders.MARKER])
        self.folder_deployment = SimpleNamespace(id=uuid4(), tags=[orders.MARKER],
                                                 work_pool_name=orders.POOL)
        self.root = SimpleNamespace(id=uuid4(), deployment_id=self.root_deployment.id,
            idempotency_key='first-step-order-v2-' + task_id, parameters={'task_id': task_id},
            parent_task_run_id=None, state=Paused(name='Suspended', reschedule=True))
        self.flows = {self.root.id: self.root}
        self.created = []
        self.queue = SimpleNamespace(id=uuid4(), is_paused=True)

    def __enter__(self): return self
    def __exit__(self, *_): return False
    def read_flow_run(self, run_id): return self.flows[run_id]
    def read_deployment_by_name(self, name):
        return self.folder_deployment if name == orders.DEPLOYMENT else self.root_deployment
    def read_work_pool(self, _name):
        return SimpleNamespace(type='process', description=orders.MARKER, is_paused=False)
    def read_work_queue_by_name(self, _name, work_pool_name=None): return self.queue
    def update_work_queue(self, _id, *, is_paused): self.queue.is_paused = is_paused
    def set_flow_run_state(self, run_id, state):
        self.flows[run_id].state = state
        return SimpleNamespace(status=SetStateStatus.ACCEPT, state=state)

    def create_flow_run_from_deployment(self, deployment_id, **kwargs):
        assert 'tags' not in kwargs
        key = kwargs['idempotency_key']
        for run in self.created:
            if run.idempotency_key == key:
                return run
        run = SimpleNamespace(id=uuid4(), deployment_id=deployment_id, parent_task_run_id=None,
            parameters=kwargs['parameters'], idempotency_key=key,
            work_queue_name=kwargs['work_queue_name'], name=kwargs['name'], state=kwargs['state'])
        self.flows[run.id] = run
        self.created.append(run)
        return run

    def create_task_run(self, *_args, **_kwargs):
        raise AssertionError('a placeholder TaskRun must not be created')

    def read_task_runs(self, *_args, **_kwargs):
        raise AssertionError('real folder observation must not depend on a placeholder TaskRun')

    def read_flow_runs(self, flow_run_filter, limit):
        identity = flow_run_filter.id
        keys = flow_run_filter.idempotency_key
        result = list(self.flows.values())
        if identity is not None and identity.any_ is not None:
            result = [row for row in result if row.id in identity.any_]
        if keys is not None and keys.any_ is not None:
            result = [row for row in result if row.idempotency_key in keys.any_]
        state = flow_run_filter.state
        if state is not None and state.type is not None and state.type.any_ is not None:
            result = [row for row in result if row.state.type in state.type.any_]
        return result[:limit]


def test_one_real_flow_per_folder_without_placeholder_and_identity_checked(tmp_path, monkeypatch):
    task_id, group_id = str(uuid4()), uuid4().hex
    receipt = 'a' * 64
    queue = FakeQueue(tmp_path / 'local.db')
    client = FakePrefect(task_id)
    service = orders.FolderOrderService(queue, prefect_api_url='http://new-prefect/api',
        first_step_index=tmp_path / 'index', first_step_data_root=tmp_path,
        first_step_receipt_root=tmp_path)
    monkeypatch.setattr(service, '_client', lambda: client)
    monkeypatch.setattr(service, '_task', lambda _: {'prefect_run_id': str(client.root.id),
        'desired_state': 'paused', 'start_pending': 0})
    monkeypatch.setattr(service, '_sync_queue', lambda *_: None)
    def eligible(_task, _group):
        with closing(queue._connect()) as db:
            row = db.execute('SELECT * FROM preprocessing_folder_orders').fetchone()
        return [{'task_id': task_id, 'group_id': group_id, 'receipt_id': receipt,
                 'record': '{}', 'order_receipt_id': row['receipt_id'] if row else None,
                 'attempt_id': row['attempt_id'] if row else None,
                 'folder_label': row['folder_label'] if row else '文件夹甲',
                 'prefect_run_id': row['prefect_run_id'] if row else None}]
    monkeypatch.setattr(service, '_eligible', eligible)
    first = service.sync_one(task_id, group_id)
    assert service.sync_one(task_id, group_id) == first
    assert len(client.created) == 1
    actual = client.created[0]
    assert actual.parent_task_run_id is None and actual.work_queue_name == 'task-' + task_id
    assert orders._result_folder_matches(actual, client.root.id, actual.parameters, client.folder_deployment.id)
    wrong = dict(actual.parameters, receipt_id='b' * 64)
    assert not orders._result_folder_matches(actual, client.root.id, wrong, client.folder_deployment.id)
    with closing(queue._connect()) as db:
        assert db.execute('SELECT COUNT(*) FROM preprocessing_folder_orders').fetchone()[0] == 1


@pytest.mark.parametrize('remote_exists', [True, False])
def test_delete_cancels_real_flow_and_does_not_create_placeholders(tmp_path, monkeypatch, remote_exists):
    task_id, group_id = str(uuid4()), uuid4().hex
    client = FakePrefect(task_id)
    item = {'task_id': task_id, 'group_id': group_id, 'receipt_id': 'a' * 64,
            'attempt_id': 'attempt-1', 'folder_label': '文件夹甲', 'prefect_run_id': None}
    if remote_exists:
        flow = client.create_flow_run_from_deployment(client.folder_deployment.id, parameters=item,
            idempotency_key='folder-waiting-v2-' + task_id + '-' + group_id,
            work_queue_name='task-' + task_id, name='真实文件夹工单', state=Scheduled())
        item['prefect_run_id'] = str(flow.id)
    scheduler = scheduler_module.PrefectScheduler('http://new-prefect/api')
    monkeypatch.setattr(scheduler, '_client', lambda: client)
    def request(path, payload=None, timeout=3):
        assert path == '/flow_runs/bulk_set_state'
        ids = payload['flow_runs']['id']['any_']
        for run_id in ids:
            from uuid import UUID
            client.flows[UUID(run_id)].state = Cancelled()
        return {'results': [{'flow_run_id': run_id} for run_id in ids]}
    monkeypatch.setattr(scheduler, '_request', request)
    scheduler.cancel_task(task_id, str(client.root.id), [item])
    assert client.root.state.is_cancelled()
    assert len(client.created) == int(remote_exists)
    if remote_exists:
        assert flow.state.is_cancelled()


def test_waiting_llm_is_observed_from_real_folder_flow(tmp_path, monkeypatch):
    task_id, group_id = str(uuid4()), uuid4().hex
    client = FakePrefect(task_id)
    queue = FakeQueue(tmp_path / 'local.db')
    control_path = tmp_path / 'control.db'
    with sqlite3.connect(control_path) as db:
        db.executescript('CREATE TABLE tasks(task_id TEXT, prefect_run_id TEXT, prefect_order_model TEXT);'
                         'CREATE TABLE task_deletions(task_id TEXT);')
        db.execute('INSERT INTO tasks VALUES(?,?,?)', (task_id, str(client.root.id), 'v2'))
    params = {'task_id': task_id, 'group_id': group_id, 'receipt_id': 'a' * 64,
              'attempt_id': 'attempt-1'}
    run = client.create_flow_run_from_deployment(client.folder_deployment.id, parameters=params,
        idempotency_key='folder-waiting-v2-' + task_id + '-' + group_id,
        work_queue_name='task-' + task_id, name='资料文件夹 甲 · 等待 LLM',
        state=Paused(name='Suspended', reschedule=True))
    item = {**params, 'prefect_run_id': str(run.id)}
    service = orders.FolderOrderService(queue, prefect_api_url='http://new-prefect/api',
        first_step_index=control_path, first_step_data_root=tmp_path, first_step_receipt_root=tmp_path)
    monkeypatch.setattr(service, '_client', lambda: client)
    monkeypatch.setattr(service, '_task', lambda _: {'prefect_run_id': str(client.root.id)})
    service._reconcile_ready([item])
    assert (task_id, group_id, params['receipt_id'], params['attempt_id'], str(run.id)) in service._ready_sync_done


def test_delete_tombstone_cancels_late_real_flow_without_creating_any_ticket(tmp_path, monkeypatch):
    task_id, group_id = str(uuid4()), uuid4().hex
    client = FakePrefect(task_id)
    client.root.state = Cancelled()
    queue = FakeQueue(tmp_path / 'local.db')
    with queue._connect() as db:
        db.execute('INSERT INTO cancelled_tasks VALUES(?)', (task_id,))
        db.execute('INSERT INTO preprocessing_folder_orders VALUES(?,?,?,?,?,?,?)',
                   (task_id, group_id, 'a' * 64, 'attempt-1', '文件夹甲', 'now', None))
    control_path = tmp_path / 'control.db'
    with sqlite3.connect(control_path) as db:
        db.executescript('CREATE TABLE tasks(task_id TEXT); CREATE TABLE task_deletions(task_id TEXT);')
        db.execute('INSERT INTO task_deletions VALUES(?)', (task_id,))
    params = {'task_id': task_id, 'group_id': group_id, 'receipt_id': 'a' * 64, 'attempt_id': 'attempt-1'}
    late = client.create_flow_run_from_deployment(client.folder_deployment.id, parameters=params,
        idempotency_key='folder-waiting-v2-' + task_id + '-' + group_id,
        work_queue_name='task-' + task_id, name='late', state=Scheduled())
    service = orders.FolderOrderService(queue, prefect_api_url='http://new-prefect/api',
        first_step_index=control_path, first_step_data_root=tmp_path, first_step_receipt_root=tmp_path)
    monkeypatch.setattr(service, '_client', lambda: client)
    assert service.reconcile_cancelled() == 0
    assert late.state.is_cancelling()
    assert len(client.created) == 1


def test_real_prefect_server_creates_one_actual_folder_flow_without_taskrun(tmp_path, monkeypatch):
    """A temporary Prefect server exercises the real create/identity API."""
    from prefect import flow
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import WorkPoolCreate
    from prefect.client.schemas.filters import TaskRunFilter, TaskRunFilterFlowRunId
    from prefect.states import Running
    from prefect.testing.utilities import prefect_test_harness
    from prefect.workers.process import ProcessWorker

    @flow(name='论文第一步-任务工单')
    def parent_flow(task_id: str):
        raise AssertionError('synthetic parent must never execute')

    @flow(name='资料文件夹工单')
    def folder_flow(task_id: str, group_id: str, receipt_id: str, attempt_id: str, folder_label: str):
        raise AssertionError('synthetic folder must never execute')

    task_id, group_id = str(uuid4()), uuid4().hex
    queue = FakeQueue(tmp_path / 'local.db')
    with prefect_test_harness(server_startup_timeout=60):
        with get_client(sync_client=True) as client:
            client.create_work_pool(WorkPoolCreate(name=orders.POOL, type='process',
                description=orders.MARKER, base_job_template=ProcessWorker.get_default_base_job_template()))
            parent_flow_id = client.create_flow(parent_flow)
            parent_deployment = client.create_deployment(parent_flow_id, '手动运行', tags=[orders.MARKER])
            folder_flow_id = client.create_flow(folder_flow)
            folder_deployment = client.create_deployment(folder_flow_id, '前处理',
                work_pool_name=orders.POOL, tags=[orders.MARKER])
            root = client.create_flow_run_from_deployment(parent_deployment,
                parameters={'task_id': task_id}, idempotency_key='first-step-order-v2-' + task_id,
                state=Running())
            service = orders.FolderOrderService(queue, prefect_api_url=str(client.api_url),
                first_step_index=tmp_path / 'index', first_step_data_root=tmp_path,
                first_step_receipt_root=tmp_path)
            monkeypatch.setattr(service, '_client', lambda: nullcontext(client))
            monkeypatch.setattr(service, '_task', lambda _: {'prefect_run_id': str(root.id),
                'desired_state': 'paused', 'start_pending': 0, 'control_revision': 1})
            def eligible(_task, _group):
                with closing(queue._connect()) as db:
                    row = db.execute('SELECT * FROM preprocessing_folder_orders').fetchone()
                return [{'task_id': task_id, 'group_id': group_id, 'receipt_id': 'a' * 64,
                    'record': '{}', 'order_receipt_id': row['receipt_id'] if row else None,
                    'attempt_id': row['attempt_id'] if row else None,
                    'folder_label': row['folder_label'] if row else '文件夹甲',
                    'prefect_run_id': row['prefect_run_id'] if row else None}]
            monkeypatch.setattr(service, '_eligible', eligible)
            first = service.sync_one(task_id, group_id)
            assert service.sync_one(task_id, group_id) == first
            run = client.read_flow_run(__import__('uuid').UUID(first))
            assert run.deployment_id == folder_deployment and run.parent_task_run_id is None
            assert not any(tag.startswith('paperflow-folder-ticket-') for tag in run.tags)
            assert client.read_work_queue_by_name('task-' + task_id, work_pool_name=orders.POOL).is_paused
            assert not client.read_task_runs(task_run_filter=TaskRunFilter(
                flow_run_id=TaskRunFilterFlowRunId(any_=[root.id])))
