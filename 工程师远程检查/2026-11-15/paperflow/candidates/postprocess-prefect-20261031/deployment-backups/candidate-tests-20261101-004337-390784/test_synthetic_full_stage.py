"""Real business scripts plus the candidate stage and results ledger, no network."""
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import sqlite3
import sys

import pytest
from filelock import FileLock

CANDIDATE = Path(__file__).resolve().parents[2]
for path in (CANDIDATE, CANDIDATE / 'tests', CANDIDATE / 'results_next',
             CANDIDATE.parent.parent / 'prefect-paperflow', CANDIDATE / 'runtime_next'):
    sys.path.insert(0, str(path))

from test_synthetic_mainline_integration import (synthetic_folder, compact, RELEASE, FixtureCounter,
    expected_components, inspect_folder, request_observation, frozen_plan_evidence)
from folder_state import FolderStageStore
from mainline_bridge import process_mainline_folder
from postprocessing_results import ResultsStore
from stage_gates import QueueBarriers
from live_runtime import ResultsHooks, body_validation_failure
from stage_runtime import ConfirmedFailure, Coordinator
from body_adapter import BodyValidationError


@pytest.mark.parametrize('parts', [1, 2])
@pytest.mark.parametrize('invalid_profile', [False, True])
def test_selected_folder_produces_a_verified_result_without_network(tmp_path, monkeypatch, parts, invalid_profile):
    import socket
    identity, manifest, paper, admissions, catalog, attachment_work = synthetic_folder(tmp_path, parts)
    database, digest = tmp_path / 'control.db', compact(manifest, sort=True)
    with sqlite3.connect(database) as db:
        db.execute('CREATE TABLE tasks(task_id TEXT PRIMARY KEY,desired_state TEXT,start_generation INTEGER,'
                   'start_pending INTEGER,task_status TEXT,prefect_run_id TEXT,prefect_order_model TEXT,name TEXT)')
        db.execute('CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY)')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?)',
                   (identity['task_id'], 'running', 1, 0, 'complete', identity['parent_run_id'], 'v2', 'test'))
        db.execute('CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,'
                   'attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,manifest_sha256 TEXT,state TEXT,generation INTEGER)')
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?)', tuple(identity.values()) +
                   (digest, 'waiting_postprocessing', 1))
        columns = list(admissions[0])
        db.execute('CREATE TABLE llm_request_admissions(' + ','.join(k + ' TEXT' for k in columns) + ')')
        for row in admissions:
            db.execute('INSERT INTO llm_request_admissions VALUES(' + ','.join('?' for _ in columns) + ')',
                       tuple(row[k] for k in columns))
    task_gate = lambda task: FileLock(tmp_path / ('task-' + task + '.lock'), timeout=3)
    folder_gate = lambda item: FileLock(tmp_path / ('folder-' + item['attempt_id'] + '.lock'), timeout=3)
    barriers = QueueBarriers(database); barriers.initialize(); barriers.open_postprocessing()
    stage_store = FolderStageStore(database, observe_run=lambda item: item == identity, admission_gate=task_gate)
    stage_store.initialize()
    result_store = ResultsStore(database, admission_gate=task_gate, inspect_receipt=FolderStageStore._inspect_receipt)
    result_store.initialize()
    class Binding:
        def load(self, item, *, require_running):
            assert item == identity and isinstance(require_running, bool)
            return {'manifest': manifest, 'paper': paper}
    class Backend:
        def attachment_work(self, p, r): return Path(attachment_work)
    @asynccontextmanager
    async def slot(): yield
    def verify(item, generation):
        return dict(identity=item, generation=generation, manifest_sha256=digest,
                    raw_complete=True, session_current=True, not_deleted=True)
    def process(item, generation, job, accept, context):
        try:
            return process_mainline_folder(identity=item, generation=generation, control_database=database,
                binding=Binding(), backend=Backend(), store=stage_store, catalog=catalog,
                paperflow_root=RELEASE / 'core', counter=FixtureCounter(), output_root=tmp_path / 'output',
                expected_components=expected_components, inspect_folder=inspect_folder,
                observe_request=request_observation, frozen_plan=frozen_plan_evidence, accepted_hook=accept)
        except BodyValidationError as exc:
            raise body_validation_failure(item, generation, digest, exc, tmp_path / 'output',
                                          verify_upstream=verify) from exc
    coordinator = Coordinator(database, task_gate=task_gate, folder_gate=folder_gate,
        postprocess_is_open=barriers.postprocess_is_open, rag_is_held=barriers.rag_is_held,
        verify_upstream=verify,
        verify_execution=lambda job, context: context == job['job_id'], slot=slot,
        process_folder=process, results=ResultsHooks(result_store))
    coordinator.initialize()
    job = coordinator.reserve(identity, 1)
    with sqlite3.connect(database) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted'")
    if invalid_profile:
        import preextract_contract
        monkeypatch.setattr(preextract_contract, 'validate_candidate', lambda *a, **kw:
            {'valid': False, 'errors': [{'code': 'learning_unusable'}]})
    async def run_without_network():
        # Windows asyncio creates a local socket pair when the loop starts.
        # Block network only after that internal pair has been created.
        monkeypatch.setattr(socket.socket, 'connect', lambda *_args, **_kwargs: pytest.fail('network forbidden'))
        return await coordinator.execute(job['job_id'], job['job_id'])
    if invalid_profile:
        with pytest.raises(ConfirmedFailure):
            asyncio.run(run_without_network())
        assert coordinator.job(job['job_id'])['state'] == 'failed'
        assert result_store.summary('failed')['counts']['failed'] == 1
        with sqlite3.connect(database) as db:
            assert db.execute('SELECT state FROM postprocessing_claims').fetchone()[0] == 'unknown'
            assert db.execute('SELECT state FROM llm_folder_queue').fetchone()[0] == 'waiting_postprocessing'
        assert barriers.rag_is_held() is True
        return
    output = asyncio.run(run_without_network())
    assert output['state'] == 'waiting_rag'
    assert coordinator.job(job['job_id'])['state'] == 'finished'
    with sqlite3.connect(database) as db:
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] == 'passed'
        assert db.execute('SELECT state FROM postprocessing_claims').fetchone()[0] == 'waiting_rag'
        assert db.execute('SELECT state FROM llm_folder_queue').fetchone()[0] == 'waiting_postprocessing'
    assert barriers.rag_is_held() is True
