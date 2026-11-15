"""Synthetic-only checks for the Windows/server postprocessing boundary.

The fixtures create temporary databases and files. They do not contact Prefect,
process real papers, operate the gates, or verify an actual server.
"""
import asyncio
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    'isolated_projection_test_helpers', ROOT / 'projection_next' / 'tests' / 'test_projections.py'
)
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)


@pytest.fixture
def modules():
    return helpers.modules.__wrapped__()


@pytest.fixture
def sample(tmp_path, modules):
    return helpers.sample.__wrapped__(tmp_path, modules)


def test_task_execution_uses_worker_factory_not_outer_coordinator(modules, monkeypatch):
    from prefect.context import TaskRunContext

    class Probe(modules.runtime.Coordinator):
        def __init__(self, name):
            self.name, self.calls = name, 0

        async def execute(self, job_id, task_run):
            self.calls += 1
            return self.name

    run_id = uuid4()
    monkeypatch.setattr(TaskRunContext, 'get', lambda: SimpleNamespace(task_run=SimpleNamespace(id=run_id)))
    outer_remote = Probe('outer remote')
    worker_local = Probe('worker local')
    task = modules.runtime.build_task(lambda: worker_local, str(run_id))

    assert asyncio.run(task.fn()) == 'worker local'
    assert worker_local.calls == 1
    assert outer_remote.calls == 0

    worker_remote = Probe('worker remote')
    task = modules.runtime.build_task(lambda: worker_remote, str(run_id))
    assert asyncio.run(task.fn()) == 'worker remote'
    assert worker_remote.calls == 1


def test_projection_cache_tracks_server_only_artifact_locally(sample, monkeypatch):
    helpers.save_pass(sample)
    helpers.insert_job(sample, 'finished', task_state='COMPLETED')
    with sqlite3.connect(sample.queue.path) as db:
        db.execute('ALTER TABLE groups ADD COLUMN record TEXT')
        db.execute('INSERT INTO groups VALUES(?,?,?,?,?,?,?)',
                   (sample.item['task_id'], sample.item['group_id'], sample.item['paper_id'],
                    sample.item['receipt_id'], 'accepted', 'receiver-test', '{}'))

    server_only_path = str(sample.tmp_path / 'not-mounted-server' / 'synthetic-artifact.json')
    with sqlite3.connect(sample.control) as db:
        claim_id, raw = db.execute(
            'SELECT claim_id,receipt_json FROM postprocessing_receipts'
        ).fetchone()
        receipt = json.loads(raw)
        receipt['artifacts'][0]['path'] = server_only_path
        raw = json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        digest = hashlib.sha256(raw.encode('utf8')).hexdigest()
        db.execute('UPDATE postprocessing_receipts SET receipt_json=?,receipt_sha256=? WHERE claim_id=?',
                   (raw, digest, claim_id))
        db.execute('UPDATE postprocessing_results SET stage_receipt_sha256=? WHERE claim_id=?',
                   (digest, claim_id))

    remote_calls = []
    monkeypatch.setattr(sample.projector.stage_store, '_inspect_receipt',
                        lambda *_: remote_calls.append('simulated remote inspector'))
    monkeypatch.setattr(sample.projector.result_store, 'summary', lambda *_: {'available': True})

    # Control: with local file caching disabled, the simulated remote inspector is reached.
    control = sample.projector.stage_projection(sample.queue.snapshot())
    assert control['available'] is True
    assert control['counts']['waiting_rag_upload'] == 1
    assert remote_calls == ['simulated remote inspector']

    # The configured projection's cache witness independently stats artifacts locally.
    original_basis = sample.modules.projection._file_basis
    failed_paths = []

    def tracked_basis(paths):
        paths = list(paths)
        try:
            return original_basis(paths)
        except FileNotFoundError:
            failed_paths.extend(paths)
            raise

    monkeypatch.setattr(sample.modules.projection, '_file_basis', tracked_basis)
    sample.projector.delivery_fingerprint = lambda *_: 'synthetic-unchanged'
    remote_calls.clear()
    observed = sample.projector.stage_projection(sample.queue.snapshot())
    assert observed['available'] is False
    assert observed['counts']['waiting_rag_upload'] is None
    assert server_only_path in failed_paths
    assert remote_calls == []
