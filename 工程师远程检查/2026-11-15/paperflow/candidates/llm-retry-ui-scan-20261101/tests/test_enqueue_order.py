from contextlib import nullcontext
import importlib.util
from pathlib import Path
import sqlite3
import threading
from types import SimpleNamespace
from uuid import uuid4

import pytest

source = Path(__file__).resolve().parents[1] / 'retry_service.py'
spec = importlib.util.spec_from_file_location('retry_enqueue_candidate', source)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_real_reservation_precedes_reset(tmp_path):
    db = tmp_path / 'control.db'
    task, parent, folder, request_id = (str(uuid4()) for _ in range(4))
    item = dict(task_id=task, group_id='a' * 32, paper_id='paper', receipt_id='b' * 64, attempt_id='first')
    with sqlite3.connect(db) as conn:
        conn.executescript('''
        CREATE TABLE llm_request_admissions(task_id TEXT,task_run_id TEXT,state TEXT);
        CREATE TABLE paperflow_llm_stage_jobs(job_id TEXT,task_id TEXT,group_id TEXT,paper_id TEXT,
            receipt_id TEXT,attempt_id TEXT,parent_run_id TEXT,folder_run_id TEXT,generation INTEGER,
            task_key TEXT,state TEXT,error_type TEXT,created_at_ns INTEGER,updated_at_ns INTEGER,retry_key TEXT);
        CREATE TABLE paperflow_llm_retry_items(request_id TEXT,task_id TEXT,group_id TEXT,folder_run_id TEXT,
            job_id TEXT,generation INTEGER,stage_mode TEXT,old_flow_state_id TEXT,old_stage_state_id TEXT,
            old_job_json TEXT,state TEXT,message TEXT,updated_at_ns INTEGER);
        ''')
        conn.execute("INSERT INTO llm_request_admissions VALUES(?,?,'unknown')", (task, folder))
        conn.execute('INSERT INTO paperflow_llm_retry_items(request_id,task_id,group_id) VALUES(?,?,?)',
                     (request_id, task, item['group_id']))
    service = module.RetryService.__new__(module.RetryService)
    service.database, service.control_locks = db, tmp_path
    service._task = lambda *_: {'start_generation': 2, 'prefect_run_id': parent}
    current = {'start_generation': 2, 'prefect_run_id': parent}
    job_id, _ = service._hold_stage(request_id, item, current,
        SimpleNamespace(id=folder, state=SimpleNamespace(id=uuid4())), None, None)
    with sqlite3.connect(db) as conn:
        assert conn.execute('SELECT state,retry_key FROM paperflow_llm_stage_jobs WHERE job_id=?',
                            (job_id,)).fetchone() == ('recovering_folder', request_id)
        assert conn.execute("SELECT count(*) FROM llm_request_admissions WHERE state='unknown'").fetchone()[0] == 1


def test_process_creates_job_before_clearing_failed_component(monkeypatch, tmp_path):
    (tmp_path / 'locks').mkdir()
    service = module.RetryService.__new__(module.RetryService)
    service.settings = SimpleNamespace(path=lambda _: tmp_path)
    service.database = tmp_path / 'control.db'
    sqlite3.connect(service.database).close()
    task, parent, folder, request_id = (str(uuid4()) for _ in range(4))
    item = dict(task_id=task, group_id='a' * 32, paper_id='paper', receipt_id='b' * 64, attempt_id='first')
    class Missing(Exception): pass
    monkeypatch.setattr(module, 'ObjectNotFound', Missing)
    class Client:
        def read_task_run(self, _): raise Missing()
    state = SimpleNamespace(id=uuid4(), is_paused=lambda: False,
                            is_failed=lambda: True, is_crashed=lambda: False)
    service._client = lambda: nullcontext(Client())
    service._item = lambda *_: None
    service._handoff = lambda _: (folder, {})
    service._task = lambda *_: {'start_generation': 2, 'prefect_run_id': parent}
    service._prefect_identity = lambda *_: SimpleNamespace(id=folder, state=state)
    service._stage_before = lambda *_: (None, None)
    service._evidence = lambda *_: None
    order = []
    service._hold_stage = lambda *args: (order.append('reserve') or str(uuid4()), 'create')
    service._reset_failed = lambda *args: order.append('reset')
    service._send_stage = lambda *args: order.append('enqueue')
    service._process_folder(request_id, item)
    assert order == ['reserve', 'reset', 'enqueue']


def test_paused_task_reports_not_submitted_without_creating_a_job(tmp_path):
    database = tmp_path / 'control.db'
    request_id = str(uuid4())
    row = dict(task_id=str(uuid4()), group_id='a' * 32, paper_id='paper', receipt_id='b' * 64,
               attempt_id='first', folder_label='folder', paper_title=None)
    with sqlite3.connect(database) as conn:
        conn.executescript('''
        CREATE TABLE paperflow_llm_retry_commands(request_id TEXT,state TEXT,message TEXT,updated_at_ns INTEGER);
        CREATE TABLE paperflow_llm_retry_items(request_id TEXT,task_id TEXT,group_id TEXT,paper_id TEXT,
            receipt_id TEXT,attempt_id TEXT,folder_label TEXT,paper_title TEXT,state TEXT,message TEXT,
            updated_at_ns INTEGER,job_id TEXT);
        ''')
        conn.execute("INSERT INTO paperflow_llm_retry_commands VALUES(?,'queued','',0)", (request_id,))
    service = module.RetryService.__new__(module.RetryService)
    service.database, service.gate = database, threading.Lock()
    service._failed_snapshot = lambda _: [row]
    def stopped(*_): raise module.RetryStopped('大任务当前已暂停，本次未提交')
    service._process_folder = stopped
    service._run(request_id, {'scope': 'all'})
    with sqlite3.connect(database) as conn:
        assert conn.execute('SELECT state,job_id FROM paperflow_llm_retry_items').fetchone() == ('paused', None)
