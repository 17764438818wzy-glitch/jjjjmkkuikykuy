"""Offline test of a manual LLM stage attached to an already failed folder."""
from contextlib import closing
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest


SOURCE = Path(__file__).resolve().parents[1] / 'retry_service.py'
spec = importlib.util.spec_from_file_location('retry_terminal_candidate', SOURCE)
retry = importlib.util.module_from_spec(spec)
spec.loader.exec_module(retry)

from paperflow_prefect.llm_queue import isolated_stage, upstream_flow_binding
from paperflow_prefect.llm_queue.pause_admission import AdmissionGuard
from paperflow_prefect import local_preprocessing


class MissingTaskRun(Exception):
    pass


def flow_state(kind):
    return SimpleNamespace(
        id=uuid4(),
        is_failed=lambda: kind == 'FAILED',
        is_crashed=lambda: kind == 'CRASHED',
        is_paused=lambda: kind == 'PAUSED',
        is_running=lambda: kind == 'RUNNING',
        is_final=lambda: kind in {'FAILED', 'CRASHED', 'COMPLETED'},
        is_cancelling=lambda: False,
    )


class FakePrefect:
    api_url = 'http://prefect.test/api'

    def __init__(self, task_id, group_id, receipt_id, attempt_id):
        self.created = 0
        self.parent = SimpleNamespace(
            id=uuid4(), deployment_id=uuid4(), parent_task_run_id=None,
            state=flow_state('RUNNING'), idempotency_key='first-step-order-v2-' + task_id,
            parameters={'task_id': task_id},
        )
        self.folder = SimpleNamespace(
            id=uuid4(), deployment_id=uuid4(), parent_task_run_id=None,
            state=flow_state('FAILED'),
            idempotency_key='folder-waiting-v2-' + task_id + '-' + group_id,
            work_queue_name='task-' + task_id,
            parameters=dict(task_id=task_id, group_id=group_id,
                            receipt_id=receipt_id, attempt_id=attempt_id),
            name='folder · LLM 失败待核对',
        )
        self.stage = None

    def read_flow_run(self, run_id):
        if run_id == self.parent.id:
            return self.parent
        assert run_id == self.folder.id
        return self.folder

    def read_deployment(self, deployment_id):
        assert deployment_id in {self.parent.deployment_id, self.folder.deployment_id}
        return SimpleNamespace(tags=[upstream_flow_binding.MARKER],
                               work_pool_name='paperflow-unified-preprocessing')

    def read_task_run(self, run_id):
        if self.stage is None:
            raise MissingTaskRun()
        assert run_id == self.stage.id
        return self.stage

    def create_task_run(self, *, task, flow_run_id, dynamic_key, id, name, state):
        assert flow_run_id is None
        assert dynamic_key == 'paperflow-llm-' + str(id)
        assert name == '资料文件夹 LLM 阶段'
        self.created += 1
        self.stage = SimpleNamespace(id=id, flow_run_id=None, dynamic_key=dynamic_key,
                                     task_key=task.task_key, tags=[isolated_stage.TAG], state=state)
        return self.stage


@pytest.fixture
def case(tmp_path, monkeypatch):
    database, queue = tmp_path / 'control.sqlite3', tmp_path / 'queue.sqlite3'
    locks = tmp_path / 'locks'
    locks.mkdir()
    task_id, request_id = str(uuid4()), str(uuid4())
    item = dict(task_id=task_id, group_id='a' * 32, paper_id='paper',
                receipt_id='b' * 64, attempt_id='attempt-1')
    client = FakePrefect(task_id, item['group_id'], item['receipt_id'], item['attempt_id'])
    identity = {**item, 'parent_run_id': str(client.parent.id), 'task_run_id': str(client.folder.id)}
    with sqlite3.connect(database) as db:
        db.executescript('''
            CREATE TABLE tasks(task_id TEXT PRIMARY KEY, prefect_run_id TEXT,
                prefect_order_model TEXT, desired_state TEXT, start_generation INTEGER,
                start_pending INTEGER, task_status TEXT);
            CREATE TABLE task_deletions(task_id TEXT);
            CREATE TABLE llm_folder_queue(task_id TEXT, group_id TEXT, state TEXT);
            CREATE TABLE paperflow_llm_retry_commands(request_id TEXT PRIMARY KEY,
                state TEXT, message TEXT);
            CREATE TABLE paperflow_llm_retry_items(request_id TEXT, task_id TEXT, group_id TEXT,
                paper_id TEXT, receipt_id TEXT, attempt_id TEXT, folder_label TEXT, paper_title TEXT,
                folder_run_id TEXT, job_id TEXT, generation INTEGER, stage_mode TEXT,
                old_flow_state_id TEXT, old_stage_state_id TEXT, old_job_json TEXT,
                state TEXT, message TEXT, updated_at_ns INTEGER);
        ''')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?)',
                   (task_id, str(client.parent.id), 'v2', 'running', 2, 0, 'complete'))
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?)', (task_id, item['group_id'], 'llm_processing'))
        db.execute("INSERT INTO paperflow_llm_retry_commands VALUES(?,'running','')", (request_id,))
        db.execute('INSERT INTO paperflow_llm_retry_items '
                   '(request_id,task_id,group_id,paper_id,receipt_id,attempt_id,state) VALUES(?,?,?,?,?,?,?)',
                   (request_id, task_id, item['group_id'], item['paper_id'], item['receipt_id'],
                    item['attempt_id'], 'checking'))
    isolated_stage.initialize(database)
    guard = AdmissionGuard(database, locks)
    guard.initialize()
    failed_work, good_work = tmp_path / 'failed-body', tmp_path / 'good-attachment'
    failed_model, good_model = failed_work / 'model', good_work / 'model'
    failed_model.mkdir(parents=True)
    good_model.mkdir(parents=True)
    (failed_model / 'request.json').write_text('failed', encoding='utf8')
    (good_model / 'response.txt').write_text('completed attachment', encoding='utf8')
    with sqlite3.connect(database) as db:
        for component, state, kind, work, model in (
                ('c' * 64, 'unknown', 'body', failed_work, failed_model),
                ('d' * 64, 'raw_complete', 'attachment', good_work, good_model)):
            db.execute('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (task_id, component, 1, 'e' * 64, state, None, kind, str(work), str(model),
                 item['receipt_id'], str(client.folder.id), 'f' * 64, 1, 2))
    with sqlite3.connect(queue) as db:
        db.executescript('''
            CREATE TABLE metadata(key TEXT, value TEXT);
            CREATE TABLE cancelled_tasks(task_id TEXT);
            CREATE TABLE preprocessing_folder_orders(task_id TEXT,group_id TEXT,receipt_id TEXT,
                attempt_id TEXT,prefect_run_id TEXT);
            CREATE TABLE groups(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,
                state TEXT,checked_session TEXT);
            CREATE TABLE claims(consumer_id TEXT,attempt_id TEXT,task_id TEXT,group_id TEXT,
                receipt_id TEXT,state TEXT);
            CREATE TABLE preprocessing_handoffs(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,
                consumer_id TEXT,attempt_id TEXT,state TEXT,handoff TEXT);
            CREATE TABLE retention_requests(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,
                consumer_id TEXT,state TEXT,retention_id TEXT,last_error TEXT,checked_session TEXT);
        ''')
        db.execute('INSERT INTO metadata VALUES(?,?)', ('queue_schema', 'paperflow-local-waiting/1'))
        db.execute('INSERT INTO preprocessing_folder_orders VALUES(?,?,?,?,?)',
                   (task_id, item['group_id'], item['receipt_id'], item['attempt_id'], str(client.folder.id)))
        db.execute('INSERT INTO groups VALUES(?,?,?,?,?,?)',
                   (task_id, item['group_id'], item['paper_id'], item['receipt_id'], 'ready_for_llm', 'session'))
        db.execute('INSERT INTO claims VALUES(?,?,?,?,?,?)',
                   ('prefect-local-preprocessing', item['attempt_id'], task_id, item['group_id'],
                    item['receipt_id'], 'completed'))
        db.execute('INSERT INTO preprocessing_handoffs VALUES(?,?,?,?,?,?,?,?)',
                   (task_id, item['group_id'], item['paper_id'], item['receipt_id'],
                    'prefect-local-preprocessing', item['attempt_id'], 'waiting', json.dumps({'fake': True})))
        db.execute('INSERT INTO retention_requests VALUES(?,?,?,?,?,?,?,?,?)',
                   (task_id, item['group_id'], item['paper_id'], item['receipt_id'],
                    'local-intake', 'accepted', 'retention', None, 'session'))
    service = retry.RetryService.__new__(retry.RetryService)
    service.database, service.control_locks = database, locks
    service.settings, service.queue = object(), object()
    service._evidence = lambda *_: None  # This test isolates orchestration, not receipt contents.
    monkeypatch.setattr(retry, 'ObjectNotFound', MissingTaskRun)
    monkeypatch.setattr(local_preprocessing, '_control_paths', lambda: (database, locks))
    monkeypatch.setattr(upstream_flow_binding, 'load_frozen_handoff', lambda *_: {'manifest': 'synthetic'})
    def binding(_settings, _queue, control, fake_client, job):
        return upstream_flow_binding.UpstreamFlowBinding(
            queue, control, fake_client, fake_client.api_url, 'local-intake', lambda *_: None,
            receiver_session=lambda _: 'session', execution_task_run_id=job['job_id'],
            execution_task_key=job['task_key'], allow_terminal_retry=True)
    monkeypatch.setattr(retry, '_binding', binding)
    return SimpleNamespace(service=service, database=database, request_id=request_id,
                           identity=identity, item=item, client=client, binding=binding,
                           failed_model=failed_model, good_model=good_model)


def reserve_terminal(case):
    current = {'start_generation': 2, 'prefect_run_id': case.identity['parent_run_id']}
    job_id, mode = case.service._hold_stage(case.request_id, case.item, current, case.client.folder, None, None)
    assert mode == 'create'
    with closing(retry._connect(case.database)) as db:
        assert db.execute('SELECT state FROM paperflow_llm_stage_jobs WHERE job_id=?', (job_id,)).fetchone()[0] == 'recovering_folder'
        assert db.execute("SELECT COUNT(*) FROM llm_request_admissions WHERE state='unknown'").fetchone()[0] == 1
    case.service._reset_failed(case.item, current, str(case.client.folder.id))
    assert not (case.failed_model / 'request.json').exists()
    assert (case.good_model / 'response.txt').read_text(encoding='utf8') == 'completed attachment'
    with closing(retry._connect(case.database)) as db:
        assert db.execute("SELECT COUNT(*) FROM llm_request_admissions WHERE state='raw_complete'").fetchone()[0] == 1
    case.service._item(case.request_id, case.item, 'recovered', 'ready')
    case.service._send_stage(case.client, case.request_id, case.item, current, str(case.client.folder.id), job_id,
                             {'state_id': str(case.client.folder.state.id), 'terminal': True})
    with closing(retry._connect(case.database)) as db:
        job = dict(db.execute('SELECT * FROM paperflow_llm_stage_jobs WHERE job_id=?', (job_id,)).fetchone())
    assert job['state'] == 'reserved'
    return job


def test_failed_folder_manual_stage_reserves_submits_and_executes(case):
    job = reserve_terminal(case)
    binding = case.binding(None, None, case.database, case.client, job)
    assert binding._manual_retry(case.identity, case.client.folder, require_running=False)
    with pytest.raises(InterruptedError):
        binding._manual_retry(case.identity, case.client.folder, require_running=True)
    assert case.service.status(case.request_id)['items'][0]['state'] == 'queued'
    assert isolated_stage._submit(case.client, case.database, job)
    assert case.client.created == 1
    assert case.service.status(case.request_id)['items'][0]['state'] == 'submitted'
    assert isolated_stage._claim_job(case.database, job['job_id'])
    case.client.stage.state = flow_state('RUNNING')
    assert binding._manual_retry(case.identity, case.client.folder, require_running=True)
    assert binding.load(case.identity, require_running=True) == {'manifest': 'synthetic'}
    assert case.service.status(case.request_id)['items'][0]['state'] == 'submitted'
    assert case.client.folder.state.is_failed()  # The old FlowRun is never rewritten.


def test_pause_before_submit_keeps_the_stage_out_of_prefect(case):
    job = reserve_terminal(case)
    with sqlite3.connect(case.database) as db:
        db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (case.item['task_id'],))
    assert not isolated_stage._submit(case.client, case.database, job)
    assert case.client.created == 0
    assert case.service.status(case.request_id)['items'][0]['state'] == 'not_sent'


def test_pause_after_submit_prevents_worker_claim(case):
    job = reserve_terminal(case)
    assert isolated_stage._submit(case.client, case.database, job)
    with sqlite3.connect(case.database) as db:
        db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (case.item['task_id'],))
    assert not isolated_stage._claim_job(case.database, job['job_id'])
    assert case.service.status(case.request_id)['items'][0]['state'] == 'paused'


@pytest.mark.parametrize(('stage_state', 'visible_state'), [
    ('finished', 'completed'), ('failed', 'failed'), ('stopped', 'paused'),
    ('stopped_before_submit', 'not_sent'),
])
def test_user_visible_state_matches_recorded_stage(case, stage_state, visible_state):
    job = reserve_terminal(case)
    with sqlite3.connect(case.database) as db:
        db.execute('UPDATE paperflow_llm_stage_jobs SET state=? WHERE job_id=?', (stage_state, job['job_id']))
        if stage_state == 'finished':
            db.execute("UPDATE llm_folder_queue SET state='waiting_postprocessing' WHERE task_id=?",
                       (case.item['task_id'],))
    assert case.service.status(case.request_id)['items'][0]['state'] == visible_state
