"""Real business scripts plus the candidate stage and results ledger, no network."""
import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
from uuid import UUID

import pytest
from filelock import FileLock

CANDIDATE = Path(__file__).resolve().parents[2]
for path in (CANDIDATE, CANDIDATE / 'tests', CANDIDATE / 'results_next',
             CANDIDATE.parent.parent / 'prefect-paperflow', CANDIDATE / 'generation_next'):
    sys.path.insert(0, str(path))

from test_synthetic_mainline_integration import (synthetic_folder, compact, RELEASE, FixtureCounter,
    expected_components, inspect_folder, request_observation, frozen_plan_evidence)
from folder_state import FolderStageStore
from generation_mainline_bridge import process_mainline_folder
from postprocessing_results import ResultsStore
from stage_gates import QueueBarriers
from live_runtime import ResultsHooks, body_validation_failure
from stage_runtime import (ConfirmedFailure, Coordinator, StageBoundHandoff, retry_source_snapshot,
                           reserved_source_matches, task_key_for_job, validate_prefect_stage, TAG)
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
        return dict(identity=item, generation=generation, source_llm_generation=1, manifest_sha256=digest,
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
                                          verify_upstream=verify, source_llm_generation=job['source_llm_generation']) from exc
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


@pytest.mark.parametrize('change_point', ['none', 'before_receipt', 'during_result_acceptance',
                                          'rejected_result_validation'])
def test_completed_retry_runs_real_folder_packaging_without_reissuing_model_requests(tmp_path, monkeypatch, change_point):
    """Two body parts and an attachment retain the original failed folder identity."""
    import socket
    from handoff_inputs import build_inputs
    from test_retry_source import install_retry_records

    identity, manifest, paper, admissions, catalog, attachment_work = synthetic_folder(tmp_path, 2)
    database, digest = tmp_path / 'control.db', compact(manifest, sort=True)
    with sqlite3.connect(database) as db:
        db.executescript('''CREATE TABLE tasks(task_id TEXT PRIMARY KEY,desired_state TEXT,start_generation INTEGER,
                start_pending INTEGER,task_status TEXT,prefect_run_id TEXT,prefect_order_model TEXT,name TEXT);
            CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY);
            CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,
                attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,manifest_sha256 TEXT,state TEXT,generation INTEGER);''')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?)',
            (identity['task_id'], 'running', 1, 0, 'running', identity['parent_run_id'], 'v2', 'synthetic'))
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?)',
                   tuple(identity.values()) + (digest, 'waiting_postprocessing', 1))
        columns = list(admissions[0])
        db.execute('CREATE TABLE llm_request_admissions(' + ','.join(k + ' TEXT' for k in columns) + ')')
        for row in admissions:
            db.execute('INSERT INTO llm_request_admissions VALUES(' + ','.join('?' for _ in columns) + ')',
                       tuple(row[k] for k in columns))
    _, _, _, _, old_state_id = install_retry_records(database, identity, existing_admissions=True,
                                                     preserve_admissions=True)
    task_gate = lambda task: FileLock(tmp_path / ('task-' + task + '.lock'), timeout=3)
    folder_gate = lambda item: FileLock(tmp_path / ('folder-' + item['attempt_id'] + '.lock'), timeout=3)
    barriers = QueueBarriers(database); barriers.initialize(); barriers.open_postprocessing()
    failed = SimpleNamespace(id=UUID(old_state_id), is_paused=lambda: False,
                             is_failed=lambda: True, is_crashed=lambda: False)
    folder = SimpleNamespace(id=identity['task_run_id'], name='原文件夹 · 失败', state=failed)
    stages = {}
    class Client:
        api_url = 'http://synthetic-prefect/api'
        def read_task_run(self, key): return stages[str(key)]
        def read_flow_run(self, _): return folder
    client = Client()
    class Upstream:
        api_url = client.api_url
        def load(self, item, *, require_running):
            assert item == identity and require_running is False
            selected = retry_source_snapshot(database, item, 1)
            if str(folder.state.id) != selected['old_flow_state_id']:
                raise ValueError('old failed folder changed')
            return {'manifest': manifest, 'paper': paper, 'source_retry_sha256': selected['sha256'],
                    'source_retry_prefect_sha256': '9' * 64}
    upstream = Upstream()
    class Backend:
        def attachment_work(self, p, r): return Path(attachment_work)
    backend = Backend()
    def verify(item, generation):
        checked = upstream.load(item, require_running=False)
        with sqlite3.connect(database) as db:
            db.row_factory = sqlite3.Row
            current = [dict(row) for row in db.execute('SELECT * FROM llm_request_admissions')]
        build_inputs(identity=item, verified=checked, admissions=current, manifest_sha256=digest,
            expected_components=expected_components, inspect_folder=inspect_folder,
            observe_request=request_observation, frozen_plan=frozen_plan_evidence,
            attachment_work=backend.attachment_work)
        return dict(identity=item, generation=generation, source_llm_generation=1, manifest_sha256=digest,
                    source_retry_sha256=checked['source_retry_sha256'],
                    source_retry_prefect_sha256=checked['source_retry_prefect_sha256'], raw_complete=True,
                    session_current=True, not_deleted=True)
    def observe(item):
        try: return reserved_source_matches(database, item, verify(item, 1))
        except Exception: return False
    store = FolderStageStore(database, observe_run=observe, admission_gate=task_gate); store.initialize()
    if change_point == 'before_receipt':
        original_complete = store.complete
        def changed_source(*args, **kwargs):
            with sqlite3.connect(database) as db:
                db.execute("UPDATE llm_request_admissions SET response_sha256=? WHERE kind='body'", ('f' * 64,))
            return original_complete(*args, **kwargs)
        store.complete = changed_source
    results = ResultsStore(database, admission_gate=task_gate, inspect_receipt=FolderStageStore._inspect_receipt)
    results.initialize()
    if change_point in {'during_result_acceptance', 'rejected_result_validation'}:
        original_pass = results.record_pass
        def changed_source_during_result(item, result_id, claim_id, *, validate_receipt):
            def validate(value):
                accepted = validate_receipt(value)
                with sqlite3.connect(database) as db:
                    db.execute("UPDATE llm_request_admissions SET response_sha256=? WHERE kind='body'", ('f' * 64,))
                return accepted if change_point == 'during_result_acceptance' else False
            return original_pass(item, result_id, claim_id, validate_receipt=validate)
        results.record_pass = changed_source_during_result
    @asynccontextmanager
    async def slot(): yield
    def verify_execution(job, context):
        return validate_prefect_stage(job, context, client, expected_api_url=client.api_url, retry_upstream=upstream)
    def process(item, generation, job, accept, context):
        binding = StageBoundHandoff(upstream, client, job, context)
        return process_mainline_folder(identity=item, generation=generation, control_database=database,
            binding=binding, backend=backend, store=store, catalog=catalog, paperflow_root=RELEASE / 'core',
            counter=FixtureCounter(), output_root=tmp_path / 'output', expected_components=expected_components,
            inspect_folder=inspect_folder, observe_request=request_observation, frozen_plan=frozen_plan_evidence,
            accepted_hook=accept)
    coordinator = Coordinator(database, task_gate=task_gate, folder_gate=folder_gate,
        postprocess_is_open=barriers.postprocess_is_open, rag_is_held=barriers.rag_is_held,
        verify_upstream=verify, verify_execution=verify_execution, slot=slot,
        process_folder=process, results=ResultsHooks(results))
    coordinator.initialize()
    job = coordinator.reserve(identity, 1)
    running = SimpleNamespace(is_running=lambda: True)
    stage = SimpleNamespace(id=job['job_id'], flow_run_id=None, task_key=task_key_for_job(job['job_id']),
        dynamic_key='paperflow-postprocess-' + job['job_id'], tags=[TAG], state=running)
    stages[job['job_id']] = stage
    with sqlite3.connect(database) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted'")
    async def run():
        monkeypatch.setattr(socket.socket, 'connect', lambda *_args, **_kwargs: pytest.fail('network forbidden'))
        return await coordinator.execute(job['job_id'], stage)
    if change_point != 'none':
        with pytest.raises(Exception):
            asyncio.run(run())
        with sqlite3.connect(database) as db:
            assert db.execute("SELECT COUNT(*) FROM postprocessing_claims WHERE state='waiting_rag'").fetchone()[0] == 0
            assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] != 'passed'
        assert folder.state is failed and barriers.rag_is_held() is True
        return
    output = asyncio.run(run())
    assert output['state'] == 'waiting_rag' and output['identity'] == identity
    assert coordinator.job(job['job_id'])['state'] == 'finished'
    with sqlite3.connect(database) as db:
        receipt = json.loads(db.execute('SELECT receipt_json FROM postprocessing_receipts').fetchone()[0])
        assert db.execute('SELECT COUNT(*) FROM postprocessing_claims').fetchone()[0] == 1
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] == 'passed'
    assert receipt['identity'] == identity
    assert Path(receipt['body']['package_path'], 'article.redacted.md').is_file()
    assert Path(receipt['attachments'][0]['md_path']).is_file()
    assert folder.state is failed and barriers.rag_is_held() is True
