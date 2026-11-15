import importlib.util
import sqlite3
from pathlib import Path

import pytest


HERE = Path(__file__).resolve().parent


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, HERE / file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_releasing_rag_does_not_close_postprocessing(tmp_path):
    dbpath = tmp_path / 'control.sqlite'
    sqlite3.connect(dbpath).close()
    gates = load('candidate_gates', 'stage_gates.py').QueueBarriers(dbpath)
    gates.initialize()
    gates.open_postprocessing()
    with sqlite3.connect(dbpath) as db:
        db.execute("UPDATE paperflow_queue_barriers SET blocked=0 WHERE name='rag_upload'")
    assert gates.postprocess_is_open()
    gates.require_rag_upload_allowed()
    gates.hold_postprocessing()
    assert not gates.postprocess_is_open()
    gates.open_postprocessing()
    assert gates.postprocess_is_open()


def test_postprocess_coordinator_does_not_require_rag_hold():
    stage = load('candidate_stage', 'stage_runtime.py')
    coordinator = stage.Coordinator.__new__(stage.Coordinator)
    coordinator.postprocess_is_open = lambda: True
    coordinator.rag_is_held = lambda: False
    coordinator._gates()
    coordinator.postprocess_is_open = lambda: False
    with pytest.raises(stage.StageConflict):
        coordinator._gates()


def test_rag_open_still_obeys_task_pause(tmp_path):
    worker = load('candidate_worker', 'run_remote_postprocess_worker.py')
    dbpath = tmp_path / 'control.sqlite'
    with sqlite3.connect(dbpath) as db:
        db.executescript('''
            CREATE TABLE paperflow_queue_barriers (name TEXT, blocked INTEGER);
            INSERT INTO paperflow_queue_barriers VALUES ('postprocessing', 0), ('rag_upload', 0);
            CREATE TABLE tasks (task_id TEXT, desired_state TEXT, start_generation INTEGER,
                start_pending INTEGER, prefect_order_model TEXT, prefect_run_id TEXT,
                task_status TEXT, started_at_ns INTEGER);
            CREATE TABLE task_deletions (task_id TEXT);
            INSERT INTO tasks VALUES ('task', 'paused', 5, 0, 'v2', 'parent', 'complete', 1);
        ''')
    assert worker.running_tasks(dbpath) == ([], [])
    with sqlite3.connect(dbpath) as db:
        db.execute("UPDATE tasks SET desired_state='running'")
    assert worker.running_tasks(dbpath) == ([('task', 5)], [])
    with sqlite3.connect(dbpath) as db:
        db.execute("UPDATE paperflow_queue_barriers SET blocked=1 WHERE name='postprocessing'")
    assert worker.running_tasks(dbpath) == ([], [])
