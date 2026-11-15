"""Synthetic Prefect positions; no real tickets, requests or workflows are run."""
from contextlib import closing
import importlib.util
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest


HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('llm_ticket_positions_candidate', HERE / 'llm_queue_summary.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class State:
    def __init__(self, name, state_id=None):
        self.name = name
        self.id = state_id or uuid4()
        self.state_details = SimpleNamespace(pause_reschedule=True)
    def is_running(self): return self.name == 'running'
    def is_paused(self): return self.name == 'paused'
    def is_failed(self): return self.name == 'failed'
    def is_crashed(self): return self.name == 'crashed'
    def is_completed(self): return self.name == 'completed'
    def is_cancelled(self): return self.name == 'cancelled'
    def is_cancelling(self): return self.name == 'cancelling'
    def is_final(self): return self.name in {'failed', 'crashed', 'completed', 'cancelled'}
    def is_pending(self): return self.name == 'pending'
    def is_scheduled(self): return self.name == 'scheduled'


def scenario(tmp_path, *, job_state, task_state, folder_state='llm_processing',
             admission_state=None, retry=True, folder_generation=1):
    task_id, group_id, folder_id, parent_id, retry_id = [str(uuid4()) for _ in range(5)]
    receipt_id, attempt_id, paper_id = 'a' * 64, str(uuid4()), str(uuid4())
    old_state_id = uuid4()
    job_id = module._stage_id(folder_id, 1, retry_id if retry else '')
    control, local = tmp_path / 'control.sqlite', tmp_path / 'local.sqlite'
    with closing(sqlite3.connect(control)) as db, db:
        db.executescript('''
        CREATE TABLE tasks(task_id TEXT, prefect_run_id TEXT, start_generation INTEGER, prefect_order_model TEXT);
        CREATE TABLE task_deletions(task_id TEXT);
        CREATE TABLE llm_folder_queue(task_id TEXT, group_id TEXT, paper_id TEXT, receipt_id TEXT,
            attempt_id TEXT, parent_run_id TEXT, task_run_id TEXT, state TEXT, generation INTEGER);
        CREATE TABLE llm_request_admissions(task_id TEXT, task_run_id TEXT, component_id TEXT,
            kind TEXT, state TEXT, receipt_id TEXT);
        CREATE TABLE paperflow_llm_stage_jobs(job_id TEXT, task_id TEXT, group_id TEXT, paper_id TEXT,
            receipt_id TEXT, attempt_id TEXT, parent_run_id TEXT, folder_run_id TEXT, generation INTEGER,
            task_key TEXT, state TEXT, created_at_ns INTEGER, retry_key TEXT);
        CREATE TABLE paperflow_llm_retry_items(job_id TEXT, request_id TEXT, old_flow_state_id TEXT, state TEXT);
        ''')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?)', (task_id, parent_id, 1, 'v2'))
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?)',
                   (task_id, group_id, paper_id, receipt_id, attempt_id, parent_id, folder_id, folder_state, folder_generation))
        if admission_state:
            db.execute('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?,?)',
                       (task_id, folder_id, 'body-1', 'body', admission_state, receipt_id))
        db.execute('INSERT INTO paperflow_llm_stage_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (job_id, task_id, group_id, paper_id, receipt_id, attempt_id, parent_id, folder_id,
                    1, 'paperflow-folder-llm-stage-v1', job_state, 1, retry_id if retry else ''))
        if retry:
            db.execute('INSERT INTO paperflow_llm_retry_items VALUES(?,?,?,?)',
                       (job_id, retry_id, str(old_state_id), 'submitted'))
    with closing(sqlite3.connect(local)) as db, db:
        db.execute('CREATE TABLE preprocessing_folder_orders(task_id TEXT, group_id TEXT, receipt_id TEXT, '
                   'attempt_id TEXT, prefect_run_id TEXT)')
        db.execute('INSERT INTO preprocessing_folder_orders VALUES(?,?,?,?,?)',
                   (task_id, group_id, receipt_id, attempt_id, folder_id))
    item = dict(task_id=task_id, group_id=group_id, paper_id=paper_id, receipt_id=receipt_id, attempt_id=attempt_id)
    snapshot = {'receiver_status': 'running', 'waiting_available': True}
    class Queue:
        def list_waiting_llm(self): return {'items': [dict(item)], 'unavailable_count': 0}
        def _connect(self):
            db = sqlite3.connect(local)
            db.row_factory = sqlite3.Row
            return db
    folder_deployment, root_deployment = uuid4(), uuid4()
    parent = SimpleNamespace(id=UUID(parent_id), deployment_id=root_deployment, parent_task_run_id=None,
        state=State('paused'), idempotency_key='first-step-order-v2-' + task_id, parameters={'task_id': task_id})
    folder = SimpleNamespace(id=UUID(folder_id), deployment_id=folder_deployment, parent_task_run_id=None,
        state=State('failed' if retry else 'paused', old_state_id),
        idempotency_key='folder-waiting-v2-' + task_id + '-' + group_id,
        work_queue_name='task-' + task_id, name='folder · 等待 LLM',
        parameters={k: item[k] for k in ('task_id', 'group_id', 'receipt_id', 'attempt_id')})
    stage = None if task_state is None else SimpleNamespace(id=UUID(job_id), flow_run_id=None,
        task_key='paperflow-folder-llm-stage-v1', dynamic_key='paperflow-llm-' + job_id,
        tags=['paperflow-llm-stage-v1'], state=State(task_state))
    class Observer:
        def _client(self): return self
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def _safe_deployment(self, _): return SimpleNamespace(id=folder_deployment)
        def read_deployment_by_name(self, _):
            return SimpleNamespace(id=root_deployment, tags=['paperflow-order-v2-live-20261031'])
        def read_flow_runs(self, **_): return [parent, folder]
        def read_task_runs(self, **_): return [stage] if stage else []
    return Queue(), control, Observer(), snapshot


def test_failed_task_is_not_shown_as_running_even_when_response_is_unknown(tmp_path):
    args = scenario(tmp_path, job_state='failed', task_state='failed', admission_state='unknown')
    result = module.summarize_llm(*args)
    assert result['llm_body_in_progress'] == 0
    assert result['llm_processing_folders'] == 0


def test_running_prefect_task_counts_once_even_before_a_request_is_admitted(tmp_path):
    args = scenario(tmp_path, job_state='executing', task_state='running')
    result = module.summarize_llm(*args)
    assert result['llm_processing_folders'] == 1


def test_reserved_ticket_is_still_waiting_until_a_task_starts(tmp_path):
    args = scenario(tmp_path, job_state='reserved', task_state=None, folder_state='waiting_llm', retry=False)
    result = module.summarize_llm(*args)
    assert result['waiting_llm'] == 1
    assert result['llm_processing_folders'] == 0


def test_actual_running_stage_wins_over_local_reserved_transition(tmp_path):
    args = scenario(tmp_path, job_state='reserved', task_state='running', folder_state='waiting_llm', retry=False)
    result = module.summarize_llm(*args)
    assert result['waiting_llm'] == 0
    assert result['llm_processing_folders'] == 1


def test_same_folder_running_body_and_attachment_counts_once(tmp_path):
    args = scenario(tmp_path, job_state='executing', task_state='running', admission_state='admitted')
    with closing(sqlite3.connect(args[1])) as db, db:
        row = db.execute('SELECT task_id,task_run_id,receipt_id FROM llm_request_admissions').fetchone()
        db.execute('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?,?)',
                   (row[0], row[1], 'attachment-1', 'attachment', 'admitted', row[2]))
    result = module.summarize_llm(*args)
    assert result['llm_processing_folders'] == 1
    assert result['llm_body_in_progress'] == result['llm_attachment_in_progress'] == 1


def test_unreachable_prefect_does_not_claim_a_failed_stage_is_inactive(tmp_path):
    args = scenario(tmp_path, job_state='failed', task_state='failed', admission_state='unknown')
    args[2].read_flow_runs = lambda **_: (_ for _ in ()).throw(OSError('offline'))
    result = module.summarize_llm(*args)
    assert result['llm_body_in_progress'] is None
    assert result['llm_processing_folders'] is None


def test_unreachable_prefect_does_not_claim_empty_subtypes_before_admission(tmp_path):
    args = scenario(tmp_path, job_state='executing', task_state='running')
    args[2].read_flow_runs = lambda **_: (_ for _ in ()).throw(OSError('offline'))
    result = module.summarize_llm(*args)
    assert result['llm_processing_folders'] is None
    assert result['llm_body_in_progress'] is None
    assert result['llm_attachment_in_progress'] is None


def test_missing_task_position_does_not_claim_waiting_queue_is_empty(tmp_path):
    args = scenario(tmp_path, job_state='submission_unknown', task_state=None)
    result = module.summarize_llm(*args)
    assert result['waiting_llm'] is None
    assert result['waiting_llm_partial'] is True


def test_manual_retry_stopped_before_submit_is_not_an_queued_ticket(tmp_path):
    args = scenario(tmp_path, job_state='stopped_before_submit', task_state=None, retry=True)
    result = module.summarize_llm(*args)
    assert result['waiting_llm'] == 0
    assert result['llm_processing_folders'] == 0
    assert result['llm_folder_positions'] == []


def test_manual_retry_stopped_after_execution_is_not_a_scheduled_ticket(tmp_path):
    args = scenario(tmp_path, job_state='stopped', task_state='completed', retry=True)
    result = module.summarize_llm(*args)
    assert result['waiting_llm'] == 0
    assert result['llm_processing_folders'] == 0
    assert result['llm_folder_positions'] == []


def test_stopping_retry_remains_in_processing_until_prefect_task_stops(tmp_path):
    args = scenario(tmp_path, job_state='stopped', task_state='running', retry=True)
    result = module.summarize_llm(*args)
    assert result['waiting_llm'] == 0
    assert result['llm_processing_folders'] == 1


def test_task_still_running_during_postprocessing_handoff_remains_in_llm(tmp_path):
    args = scenario(tmp_path, job_state='executing', task_state='running',
                    folder_state='waiting_postprocessing', retry=False)
    result = module.summarize_llm(*args)
    assert result['llm_processing_folders'] == 1


def test_new_waiting_folder_without_generation_is_still_counted_when_reserved(tmp_path):
    args = scenario(tmp_path, job_state='reserved', task_state=None, folder_state='waiting_llm',
                    retry=False, folder_generation=None)
    result = module.summarize_llm(*args)
    assert result['waiting_llm'] == 1
    assert result['llm_processing_folders'] == 0


def test_retry_of_failed_original_counts_as_waiting_before_submission(tmp_path):
    args = scenario(tmp_path, job_state='reserved', task_state=None, folder_state='waiting_llm',
                    retry=True, folder_generation=None)
    result = module.summarize_llm(*args)
    assert result['waiting_llm'] == 1
    assert result['llm_processing_folders'] == 0


def test_scheduled_prefect_task_is_waiting_until_it_runs(tmp_path):
    args = scenario(tmp_path, job_state='submitted', task_state='scheduled',
                    folder_state='waiting_llm', retry=False, folder_generation=None)
    result = module.summarize_llm(*args)
    assert result['waiting_llm'] == 1
    assert result['llm_processing_folders'] == 0


def test_running_prefect_task_wins_when_local_job_status_is_uncertain(tmp_path):
    args = scenario(tmp_path, job_state='execution_unknown', task_state='running',
                    folder_state='waiting_llm', retry=False)
    result = module.summarize_llm(*args)
    assert result['waiting_llm'] == 0
    assert result['llm_processing_folders'] == 1


def test_unavailable_handoff_cannot_masquerade_as_empty_processing(tmp_path):
    args = scenario(tmp_path, job_state='executing', task_state='running')
    args[0].list_waiting_llm = lambda: {'items': [], 'unavailable_count': 1}
    result = module.summarize_llm(*args)
    assert result['llm_processing_folders'] is None


def test_unknown_body_result_is_not_claimed_as_active_request(tmp_path):
    args = scenario(tmp_path, job_state='executing', task_state='running', admission_state='unknown')
    result = module.summarize_llm(*args)
    assert result['llm_processing_folders'] == 1
    assert result['llm_body_in_progress'] is None


def test_scheduled_retry_with_old_processing_ledger_is_waiting(tmp_path):
    args = scenario(tmp_path, job_state='submitted', task_state='scheduled',
                    folder_state='llm_processing', retry=True)
    result = module.summarize_llm(*args)
    assert result['waiting_llm'] == 1
    assert result['llm_processing_folders'] == 0


def test_newer_running_retry_wins_before_ledger_generation_updates(tmp_path):
    args = scenario(tmp_path, job_state='failed', task_state='failed', admission_state='unknown')
    with closing(sqlite3.connect(args[1])) as db, db:
        old = db.execute('SELECT task_id,group_id,paper_id,receipt_id,attempt_id,parent_run_id,folder_run_id '
                         'FROM paperflow_llm_stage_jobs').fetchone()
        old_state_id = db.execute('SELECT old_flow_state_id FROM paperflow_llm_retry_items').fetchone()[0]
        new_retry = str(uuid4())
        new_job = module._stage_id(old[6], 2, new_retry)
        db.execute('UPDATE tasks SET start_generation=2')
        db.execute('INSERT INTO paperflow_llm_stage_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (new_job, *old, 2, 'paperflow-folder-llm-stage-v1', 'executing', 2, new_retry))
        db.execute('INSERT INTO paperflow_llm_retry_items VALUES(?,?,?,?)',
                   (new_job, new_retry, old_state_id, 'submitted'))
    stage = SimpleNamespace(id=UUID(new_job), flow_run_id=None, task_key='paperflow-folder-llm-stage-v1',
        dynamic_key='paperflow-llm-' + new_job, tags=['paperflow-llm-stage-v1'], state=State('running'))
    args[2].read_task_runs = lambda **_: [stage]
    result = module.summarize_llm(*args)
    assert result['llm_processing_folders'] == 1
