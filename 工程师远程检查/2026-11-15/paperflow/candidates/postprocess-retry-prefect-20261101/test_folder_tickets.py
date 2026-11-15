from pathlib import Path
import sqlite3
from types import SimpleNamespace
from uuid import uuid4

import pytest

import folder_tickets


class State:
    def __init__(self, value): self.value = value
    def is_running(self): return self.value == 'running'
    def is_completed(self): return self.value == 'completed'
    def is_failed(self): return self.value == 'failed'
    def is_scheduled(self): return self.value == 'scheduled'
    def is_pending(self): return self.value == 'pending'
    def is_paused(self): return self.value == 'paused'


@pytest.fixture
def example(tmp_path):
    local, control = tmp_path / 'local.sqlite', tmp_path / 'control.sqlite'
    task, group, folder, llm_job, post_job, rag_job, nas_job = [str(uuid4()) for _ in range(7)]
    with sqlite3.connect(local) as db:
        db.executescript('''CREATE TABLE groups(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,state TEXT,review_reason TEXT);
            CREATE TABLE preprocessing_folder_orders(task_id TEXT,group_id TEXT,receipt_id TEXT,prefect_run_id TEXT,folder_label TEXT,attempt_id TEXT);
            CREATE TABLE claims(task_id TEXT,group_id TEXT,receipt_id TEXT,attempt_id TEXT,consumer_id TEXT,state TEXT);
            CREATE TABLE preprocessing_failures(attempt_id TEXT,consumer_id TEXT,error_type TEXT);
            CREATE TABLE preprocessing_handoffs(task_id TEXT,group_id TEXT,receipt_id TEXT,attempt_id TEXT,state TEXT);
            CREATE TABLE cancelled_tasks(task_id TEXT);''')
        db.execute('INSERT INTO groups VALUES(?,?,?,?,?,?)', (task, group, 'paper', 'receipt', 'ready_for_llm', None))
        db.execute('INSERT INTO preprocessing_folder_orders VALUES(?,?,?,?,?,?)', (task, group, 'receipt', folder, '资料组', 'attempt'))
        db.execute('INSERT INTO claims VALUES(?,?,?,?,?,?)', (task, group, 'receipt', 'attempt', 'prefect-local-preprocessing', 'completed'))
        db.execute('INSERT INTO preprocessing_handoffs VALUES(?,?,?,?,?)', (task, group, 'receipt', 'attempt', 'waiting'))
    parent = str(uuid4())
    with sqlite3.connect(control) as db:
        db.executescript('''CREATE TABLE tasks(task_id TEXT,name TEXT,prefect_run_id TEXT,prefect_order_model TEXT);
            CREATE TABLE task_deletions(task_id TEXT);
            CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,state TEXT);
            CREATE TABLE paperflow_postprocess_stage_jobs(task_id TEXT,group_id TEXT,job_id TEXT,state TEXT);
            CREATE TABLE postprocessing_results(task_id TEXT,group_id TEXT,state TEXT,code TEXT);
            CREATE TABLE paperflow_rag_nas_jobs(task_id TEXT,group_id TEXT,stage TEXT,state TEXT,rag_task_run_id TEXT,nas_task_run_id TEXT);
            CREATE TABLE paperflow_llm_stage_jobs(task_id TEXT,group_id TEXT,job_id TEXT,state TEXT,generation INTEGER,created_at_ns INTEGER);
            CREATE TABLE llm_request_admissions(task_id TEXT,task_run_id TEXT,kind TEXT,state TEXT,response_sha256 TEXT);''')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?)', (task, '总任务', parent, 'v2'))
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?)', (task, group, 'llm_processing'))
        db.execute('INSERT INTO paperflow_llm_stage_jobs VALUES(?,?,?,?,?,?)', (task, group, llm_job, 'executing', 1, 1))
        db.executemany('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?)',
                       [(task, folder, 'body', 'admitted', '1' * 64), (task, folder, 'attachment', 'admitted', '2' * 64)])
    states = {llm_job: State('running'), post_job: State('running'), rag_job: State('running'), nas_job: State('completed')}
    class Projector:
        observer = SimpleNamespace(_add_result_titles=lambda items: None)
        def __init__(self): self.local, self.control = local, control
        def _prefect(self, folders, jobs, stages):
            flows = {folder: SimpleNamespace(state=State('paused'), parameters={'task_id': task, 'group_id': group}),
                     parent: SimpleNamespace(state=State('paused'))}
            runs = {key: SimpleNamespace(state=value) for key, value in states.items()}
            return flows, runs, None, None, None
    return SimpleNamespace(projector=Projector(), control=control, task=task, group=group, folder=folder,
                           llm=llm_job, post=post_job, rag=rag_job, nas=nas_job, states=states)


def test_simultaneous_body_and_attachment_are_one_running_folder(example):
    value = folder_tickets.observe(example.projector)
    assert value['complete'] is True and len(value['folders']) == 1
    row = value['folders'][0]
    assert row['ticket_id'] == example.folder and row['stage'] == 'llm_in_progress'
    assert row['llm_body_running'] and row['llm_attachment_running']


def test_empty_saved_body_is_not_counted_as_a_successful_recognition(example):
    import hashlib
    with sqlite3.connect(example.control) as db:
        db.execute("UPDATE llm_folder_queue SET state='waiting_postprocessing'")
        db.execute("UPDATE llm_request_admissions SET state='raw_complete'")
        db.execute("UPDATE llm_request_admissions SET response_sha256=? WHERE kind='body'", (hashlib.sha256(b'').hexdigest(),))
        db.execute("UPDATE paperflow_llm_stage_jobs SET state='finished'")
    example.states[example.llm] = State('completed')
    row = folder_tickets.observe(example.projector)['folders'][0]
    assert row['stage'] == 'waiting_postprocessing'
    assert row['llm_result'] == 'unrecognized'
    assert '正文原始回答为空' in row['llm_detail']


def test_saved_answer_is_not_marked_passed_until_postprocessing_checks_it(example):
    with sqlite3.connect(example.control) as db:
        db.execute("UPDATE llm_folder_queue SET state='waiting_postprocessing'")
        db.execute("UPDATE llm_request_admissions SET state='raw_complete'")
        db.execute("UPDATE paperflow_llm_stage_jobs SET state='finished'")
    example.states[example.llm] = State('completed')
    row = folder_tickets.observe(example.projector)['folders'][0]
    assert row['stage'] == 'waiting_postprocessing' and row['llm_result'] is None
    assert row['llm_response_saved'] is True
    assert '画像校验在后处理进行' in row['llm_detail']


def test_saved_answer_stays_in_llm_until_prefect_stage_finishes(example):
    with sqlite3.connect(example.control) as db:
        db.execute("UPDATE llm_folder_queue SET state='waiting_postprocessing'")
        db.execute("UPDATE llm_request_admissions SET state='raw_complete'")
        db.execute("UPDATE paperflow_llm_stage_jobs SET state='finished'")
    row = folder_tickets.observe(example.projector)['folders'][0]
    assert row['stage'] == 'llm_in_progress'
    example.states[example.llm] = State('completed')
    row = folder_tickets.observe(example.projector)['folders'][0]
    assert row['stage'] == 'waiting_postprocessing'


def test_postprocessing_display_uses_the_existing_prefect_identity_projection(example):
    with sqlite3.connect(example.control) as db:
        db.execute("UPDATE llm_folder_queue SET state='waiting_postprocessing'")
        db.execute("UPDATE llm_request_admissions SET state='raw_complete'")
        db.execute("INSERT INTO paperflow_postprocess_stage_jobs VALUES(?,?,?,?)",
                   (example.task, example.group, example.post, 'executing'))
    class Positions:
        def _snapshot(self, projector):
            return {(example.task, example.group): {'job': {'state': 'executing'}, 'result': None,
                    'downstream': None, 'related': []}}, True
        def _position(self, *args): return None, None
    value = folder_tickets.observe(example.projector, Positions())
    assert value['folders'][0]['stage'] == 'unresolved' and value['complete'] is False


@pytest.mark.parametrize(('stage', 'state', 'expected', 'complete'), [
    ('post', 'executing', 'postprocessing_in_progress', True),
    ('post', 'finished', 'waiting_rag_upload', True),
    ('rag', 'waiting', 'waiting_rag_upload', True),
    ('rag', 'unknown', 'unresolved', False),
    ('completed', 'completed', 'completed', True)])
def test_position_uses_the_original_folder_and_current_stage(example, stage, state, expected, complete):
    e = example
    with sqlite3.connect(e.control) as db:
        db.execute("UPDATE llm_folder_queue SET state='waiting_postprocessing'")
        db.execute("UPDATE llm_request_admissions SET state='raw_complete'")
        db.execute("INSERT INTO paperflow_postprocess_stage_jobs VALUES(?,?,?,?)",
                   (e.task, e.group, e.post, 'executing' if stage == 'post' and state == 'executing' else 'finished'))
        if state != 'executing':
            db.execute("INSERT INTO postprocessing_results VALUES(?,?,?,?)", (e.task, e.group, 'passed', 'passed'))
        if stage != 'post':
            db.execute('INSERT INTO paperflow_rag_nas_jobs VALUES(?,?,?,?,?,?)',
                       (e.task, e.group, stage, state, e.rag, e.nas))
    if stage == 'post' and state == 'finished': e.states[e.post] = State('completed')
    value = folder_tickets.observe(e.projector)
    assert value['complete'] is complete and value['folders'][0]['stage'] == expected
    assert value['folders'][0]['ticket_id'] == e.folder
