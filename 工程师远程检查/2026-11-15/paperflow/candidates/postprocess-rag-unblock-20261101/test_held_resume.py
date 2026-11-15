"""Exercise the original completed receipt and Prefect task when resuming."""
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID
import json
import importlib.util
import sqlite3
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
ORIGINAL = ROOT / 'candidates' / 'postprocess-prefect-20261031'
sys.path[:0] = [str(Path(__file__).resolve().parent), str(ORIGINAL / 'runtime_next' / 'tests'),
                str(ROOT / 'prefect-paperflow')]
from test_held_recovery import held
from receipt_access import ReceiptAccess
from stage_runtime import TAG, StageConflict

sys.path.insert(0, str(ROOT / 'candidates' / 'postprocess-server-20261101'))
spec = importlib.util.spec_from_file_location('held_worker_candidate', Path(__file__).with_name('run_remote_postprocess_worker.py'))
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)
reconcile_held = worker.reconcile_held


class Inspector:
    @contextmanager
    def prefetched(self, rows, *, recheck_journal=False):
        assert len(rows) == 1 and recheck_journal is False
        self.row = rows[0]
        yield

    def __call__(self, receipt, identity, claim_id):
        assert receipt == json.loads(self.row['receipt_json'])
        assert claim_id == self.row['claim_id']
        assert all(identity[key] == self.row[key] for key in identity)
        return True


def test_completed_prefect_receipt_resumes_without_reprocessing(held, monkeypatch):
    with sqlite3.connect(held['database']) as db:
        db.execute('ALTER TABLE tasks ADD COLUMN started_at_ns INTEGER DEFAULT 1')
    coordinator = held['coordinator']
    item = coordinator.job(held['job']['job_id'])
    task = SimpleNamespace(id=UUID(item['job_id']), flow_run_id=None, task_key=item['task_key'],
        dynamic_key='paperflow-postprocess-' + item['job_id'], tags=[TAG],
        state=SimpleNamespace(is_completed=lambda: True))

    class Client:
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def read_task_run(self, job_id):
            assert job_id == task.id
            return task

    runtime = SimpleNamespace(original_runtime={'coordinator': coordinator, 'database': held['database'],
                                                'observe_run': lambda identity: identity == held['identity'],
                                                'client': Client})
    access = ReceiptAccess(held['database'], Inspector())
    monkeypatch.setattr(coordinator, 'process_folder', lambda *_: pytest.fail('business work repeated'))
    assert reconcile_held(runtime, access, item['job_id'], 2) == 'finished'
    with sqlite3.connect(held['database']) as db:
        assert db.execute('SELECT state,generation FROM postprocessing_claims').fetchone() == ('waiting_rag', 2)
        assert db.execute('SELECT COUNT(*),generation FROM paperflow_postprocess_stage_jobs').fetchone() == (1, 1)
        assert db.execute('SELECT state FROM postprocessing_results').fetchone() == ('passed',)


def test_paused_parent_does_not_resume_held_receipt(held):
    coordinator = held['coordinator']
    item = coordinator.job(held['job']['job_id'])
    task = SimpleNamespace(id=UUID(item['job_id']), flow_run_id=None, task_key=item['task_key'],
        dynamic_key='paperflow-postprocess-' + item['job_id'], tags=[TAG],
        state=SimpleNamespace(is_completed=lambda: True))
    class Client:
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def read_task_run(self, _): pytest.fail('paused task queried Prefect')
    runtime = SimpleNamespace(original_runtime={'coordinator': coordinator, 'database': held['database'],
                                                'observe_run': lambda _: True, 'client': Client})
    with sqlite3.connect(held['database']) as db:
        db.execute('ALTER TABLE tasks ADD COLUMN started_at_ns INTEGER DEFAULT 1')
        db.execute("UPDATE tasks SET desired_state='paused'")
    with pytest.raises(StageConflict):
        reconcile_held(runtime, ReceiptAccess(held['database'], Inspector()), item['job_id'], 2)
    with sqlite3.connect(held['database']) as db:
        assert db.execute('SELECT state FROM postprocessing_claims').fetchone()[0] == 'receipt_held'
