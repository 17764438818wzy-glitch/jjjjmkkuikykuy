"""Task control reports downstream work using the existing status counts."""
import importlib.util
from pathlib import Path
import sqlite3

spec = importlib.util.spec_from_file_location('control_candidate', Path(__file__).with_name('llm_admission_state.py'))
control = importlib.util.module_from_spec(spec)
spec.loader.exec_module(control)


def test_downstream_running_and_unknown_join_existing_task_counts(monkeypatch):
    db = sqlite3.connect(':memory:')
    db.executescript('''CREATE TABLE llm_request_admissions(task_id TEXT, state TEXT);
        CREATE TABLE llm_folder_queue(task_id TEXT);
        CREATE TABLE paperflow_llm_stage_jobs(task_id TEXT, state TEXT);
        CREATE TABLE task_deletions(task_id TEXT);
        CREATE TABLE paperflow_rag_nas_jobs(task_id TEXT, state TEXT, rag_submission TEXT, nas_submission TEXT);
        INSERT INTO paperflow_rag_nas_jobs VALUES ('task','running','submitted','reserved'),
          ('task','unknown','unknown','reserved'), ('task','waiting','reserved','reserved'),
          ('task','completed','finished','finished'), ('other','running','submitted','reserved');''')
    monkeypatch.setattr(control, '_postprocessing_counts', lambda *_: (0, 0))
    result = control.counts(db, 'task')
    assert result['inflight'] == 1
    assert result['unknown'] == 1


def test_submitting_is_inflight_and_unknown_submission_is_unresolved(monkeypatch):
    db = sqlite3.connect(':memory:')
    db.executescript('''CREATE TABLE llm_request_admissions(task_id TEXT, state TEXT);
        CREATE TABLE llm_folder_queue(task_id TEXT);
        CREATE TABLE paperflow_llm_stage_jobs(task_id TEXT, state TEXT);
        CREATE TABLE task_deletions(task_id TEXT);
        CREATE TABLE paperflow_rag_nas_jobs(task_id TEXT, state TEXT, rag_submission TEXT, nas_submission TEXT);
        INSERT INTO paperflow_rag_nas_jobs VALUES ('task','waiting','submitting','reserved'),
          ('task','waiting','finished','unknown');''')
    monkeypatch.setattr(control, '_postprocessing_counts', lambda *_: (0, 0))
    result = control.counts(db, 'task')
    assert result['inflight'] == 1
    assert result['unknown'] == 1
