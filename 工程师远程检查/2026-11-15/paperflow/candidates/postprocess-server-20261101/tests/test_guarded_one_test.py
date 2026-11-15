import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import pytest


SPEC = importlib.util.spec_from_file_location('guarded_one_test', Path(__file__).parents[1] / 'guarded_one_test.py')
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)


@pytest.fixture
def ticket(tmp_path, monkeypatch):
    workspace = tmp_path / 'workspace'
    classified = workspace / 'classification'
    database = classified / 'group-task-summaries/summary-index.sqlite3'
    database.parent.mkdir(parents=True)
    with sqlite3.connect(database) as db:
        db.executescript('''
        CREATE TABLE tasks(task_id TEXT, desired_state TEXT, start_pending INTEGER, task_status TEXT, start_generation INTEGER);
        INSERT INTO tasks VALUES('task', 'running', 0, 'running', 3);
        CREATE TABLE task_deletions(task_id TEXT);
        CREATE TABLE postprocessing_claims(task_id TEXT, group_id TEXT);
        CREATE TABLE postprocessing_results(task_id TEXT, group_id TEXT);
        CREATE TABLE paperflow_postprocess_stage_jobs(task_id TEXT, group_id TEXT);
        ''')
    config_dir = workspace / 'candidates/first-step-control-20261029/runtime'
    config_dir.mkdir(parents=True)
    (config_dir / 'workspace-config.json').write_text(json.dumps({'classification_root': str(classified)}))
    config = tmp_path / 'config.json'
    config.write_text(json.dumps({'expected_prefect_api_url': 'http://prefect/api'}))
    evidence = tmp_path / 'evidence.json'
    evidence.write_text(json.dumps({'schema': 'paperflow-selected-ticket-evidence/1', 'claim_created': False,
                                    'identity': {'task_id': 'task', 'group_id': 'group', 'task_run_id': 'folder'},
                                    'generation': 3}))
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    progress, log = tmp_path / 'progress.json', tmp_path / 'job.log'
    monkeypatch.setattr(sys, 'argv', ['guarded_one_test.py', str(config), digest(config), str(evidence),
                                      digest(evidence), str(progress), str(log), str(workspace)])

    class Barriers:
        state = {'rag_upload': 1, 'postprocessing': 1}
        def __init__(self, path):
            assert path == database
        def _read(self):
            return dict(self.state)
        def open_postprocessing(self):
            self.state['postprocessing'] = 0
        def hold_postprocessing(self):
            self.state['postprocessing'] = 1

    response = SimpleNamespace(raise_for_status=lambda: None,
        json=lambda: {'id': 'folder', 'name': '原文件夹等待后处理',
                      'state': {'type': 'PAUSED', 'state_details': {'pause_reschedule': True}}})
    monkeypatch.setitem(sys.modules, 'stage_gates', SimpleNamespace(QueueBarriers=Barriers))
    monkeypatch.setitem(sys.modules, 'requests', SimpleNamespace(get=lambda *a, **kw: response))
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        if 'verify' in argv:
            return SimpleNamespace(returncode=0, stdout=b'{"same_original_identity":true}')
        assert Barriers.state['postprocessing'] == 0 and Barriers.state['rag_upload'] == 1
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(guard.subprocess, 'run', run)
    return SimpleNamespace(db=database, barriers=Barriers, progress=progress, log=log, calls=calls, run=run)


def test_one_original_ticket_and_close_existing_gate(ticket):
    assert guard.main() == 0
    result = json.loads(ticket.progress.read_text())
    assert result['state'] == 'exited' and result['gate_reclosed']
    assert ticket.barriers.state == {'rag_upload': 1, 'postprocessing': 1}
    assert len(ticket.calls) == 2 and 'run-one' in ticket.calls[1]
    assert ticket.calls[1][-4:] == ['--task-id', 'task', '--group-id', 'group']


def test_runner_timeout_holds_gate_and_keeps_unknown(ticket, monkeypatch):
    def failing(argv, **kwargs):
        if 'verify' in argv:
            return ticket.run(argv, **kwargs)
        raise subprocess.TimeoutExpired(argv, 5000)
    monkeypatch.setattr(guard.subprocess, 'run', failing)
    assert guard.main() == 1
    result = json.loads(ticket.progress.read_text())
    assert result['state'] == 'unconfirmed' and result['error_type'] == 'TimeoutExpired'
    assert result['gate_reclosed'] and ticket.barriers.state['postprocessing'] == 1


def test_paused_parent_never_opens_gate(ticket):
    with sqlite3.connect(ticket.db) as db:
        db.execute("UPDATE tasks SET desired_state='paused'")
    with pytest.raises(ValueError):
        guard.main()
    assert ticket.barriers.state['postprocessing'] == 1
    assert len(ticket.calls) == 1 and not ticket.log.exists()
