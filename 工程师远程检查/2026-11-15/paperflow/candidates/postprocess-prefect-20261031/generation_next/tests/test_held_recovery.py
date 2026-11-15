"""A held real synthetic receipt is rechecked; only its queue state changes."""
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import sqlite3
import sys

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
from live_runtime import ResultsHooks
from stage_runtime import Coordinator, StageConflict
from paperflow_prefect.records import sha_file


@pytest.fixture(params=['normal', 'retry'])
def held(tmp_path, monkeypatch, request):
    identity, manifest, paper, admissions, catalog, attachment_work = synthetic_folder(tmp_path)
    database, digest = tmp_path / 'control.db', compact(manifest, sort=True)
    with sqlite3.connect(database) as db:
        db.executescript('''CREATE TABLE tasks(task_id TEXT PRIMARY KEY,desired_state TEXT,start_generation INTEGER,
            start_pending INTEGER,task_status TEXT,prefect_run_id TEXT,prefect_order_model TEXT,name TEXT);
            CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY);
            CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,
            attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,manifest_sha256 TEXT,state TEXT,generation INTEGER);''')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?)',
                   (identity['task_id'], 'running', 1, 0, 'complete', identity['parent_run_id'], 'v2', 'test'))
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?)', tuple(identity.values()) +
                   (digest, 'waiting_postprocessing', 1))
        columns = list(admissions[0])
        db.execute('CREATE TABLE llm_request_admissions(' + ','.join(k + ' TEXT' for k in columns) + ')')
        for row in admissions:
            db.execute('INSERT INTO llm_request_admissions VALUES(' + ','.join('?' for _ in columns) + ')',
                       tuple(row[k] for k in columns))
    if request.param == 'retry':
        from test_retry_source import install_retry_records
        install_retry_records(database, identity, existing_admissions=True, preserve_admissions=True)
    prefect_proof = ['9' * 64] if request.param == 'retry' else [None]
    task_gate = lambda task: FileLock(tmp_path / ('task-' + task + '.lock'), timeout=3)
    folder_gate = lambda item: FileLock(tmp_path / ('folder-' + item['attempt_id'] + '.lock'), timeout=3)
    barriers = QueueBarriers(database); barriers.initialize(); barriers.open_postprocessing()
    store = FolderStageStore(database, observe_run=lambda item: item == identity, admission_gate=task_gate)
    store.initialize()
    results = ResultsStore(database, admission_gate=task_gate, inspect_receipt=FolderStageStore._inspect_receipt)
    results.initialize()
    class Binding:
        def load(self, item, *, require_running):
            assert item == identity and isinstance(require_running, bool)
            return {'manifest': manifest, 'paper': paper}
    class Backend:
        def attachment_work(self, _paper, _row): return Path(attachment_work)
    @asynccontextmanager
    async def slot(): yield
    def verify(item, generation):
        with sqlite3.connect(database) as db:
            source = db.execute('SELECT generation FROM llm_folder_queue').fetchone()[0]
        if request.param == 'retry':
            from stage_runtime import retry_source_snapshot
            proof = retry_source_snapshot(database, item, source)['sha256']
        else:
            proof = None
        return dict(identity=item, generation=generation, source_llm_generation=source, manifest_sha256=digest,
                    source_retry_sha256=proof, source_retry_prefect_sha256=prefect_proof[0],
                    raw_complete=True, session_current=True, not_deleted=True)
    arguments = dict(identity=identity, control_database=database, binding=Binding(), backend=Backend(),
        store=store, catalog=catalog, paperflow_root=RELEASE / 'core', counter=FixtureCounter(),
        output_root=tmp_path / 'output', expected_components=expected_components, inspect_folder=inspect_folder,
        observe_request=request_observation, frozen_plan=frozen_plan_evidence)
    def process(item, generation, job, accept, _context):
        return process_mainline_folder(generation=generation, accepted_hook=accept, **arguments)
    coordinator = Coordinator(database, task_gate=task_gate, folder_gate=folder_gate,
        postprocess_is_open=barriers.postprocess_is_open, rag_is_held=barriers.rag_is_held,
        verify_upstream=verify, verify_execution=lambda job, context: context == job['job_id'],
        slot=slot, process_folder=process, results=ResultsHooks(results))
    coordinator.initialize()
    job = coordinator.reserve(identity, 1)
    with sqlite3.connect(database) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted'")
    complete = store.complete
    def pause_and_complete(*args, **kwargs):
        with sqlite3.connect(database) as db:
            db.execute("UPDATE tasks SET desired_state='paused',task_status='paused'")
        return complete(*args, **kwargs)
    monkeypatch.setattr(store, 'complete', pause_and_complete)
    output = asyncio.run(coordinator.execute(job['job_id'], job['job_id']))
    assert output['state'] == 'receipt_held'
    with sqlite3.connect(database) as db:
        db.execute("UPDATE tasks SET desired_state='running',task_status='running',start_generation=2")
    def validator_factory(_item, claim):
        return process_mainline_folder(generation=2, reconcile_claim_id=claim, validator_only=True, **arguments)
    return dict(coordinator=coordinator, store=store, job=job, identity=identity, database=database, root=tmp_path,
                output=tmp_path / 'output', validator_factory=validator_factory, barriers=barriers,
                retry=request.param == 'retry', prefect_proof=prefect_proof)


def test_held_receipt_promotes_without_rewriting_outputs_or_creating_new_identity(held, monkeypatch):
    before = {p: (sha_file(p), p.stat().st_mtime_ns) for p in held['root'].rglob('*') if p.is_file()
              and p.suffix not in {'.lock', '.db'} and not p.name.startswith('control.db-')}
    with sqlite3.connect(held['database']) as db:
        receipt_before = db.execute('SELECT receipt_json,receipt_sha256 FROM postprocessing_receipts').fetchone()
        source_before = db.execute('SELECT generation FROM llm_folder_queue').fetchone()
    monkeypatch.setattr(held['coordinator'], 'process_folder', lambda *_: pytest.fail('business work repeated'))
    old_open = Path.open
    def no_write(path, mode='r', *args, **kwargs):
        if any(char in mode for char in 'wax+'):
            pytest.fail('receipt validation attempted to write')
        return old_open(path, mode, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', no_write)
    def no_new_directory(path, *_args, **_kw):
        if path == held['root'] and path.is_dir():  # FileLock checks its existing directory.
            return None
        pytest.fail('validation attempted to create a directory')
    monkeypatch.setattr(Path, 'mkdir', no_new_directory)
    result = held['coordinator'].reconcile_held(held['job']['job_id'], 2, store=held['store'],
                                               validator_factory=held['validator_factory'])
    assert result['state'] == 'waiting_rag'
    assert {p: (sha_file(p), p.stat().st_mtime_ns) for p in before} == before
    with sqlite3.connect(held['database']) as db:
        assert db.execute('SELECT receipt_json,receipt_sha256 FROM postprocessing_receipts').fetchone() == receipt_before
        assert db.execute('SELECT generation FROM llm_folder_queue').fetchone() == source_before == (1,)
        assert db.execute('SELECT count(*),generation,source_llm_generation FROM paperflow_postprocess_stage_jobs').fetchone() == (1, 1, 1)
        assert db.execute('SELECT state,generation FROM postprocessing_claims').fetchone() == ('waiting_rag', 2)
        assert db.execute('SELECT state FROM postprocessing_results').fetchone() == ('passed',)
    assert held['barriers'].rag_is_held() is True


@pytest.mark.parametrize('change', ['pause', 'source'])
def test_concurrent_pause_or_changed_original_generation_keeps_receipt_held(held, change):
    def racing(item, claim):
        validate = held['validator_factory'](item, claim)
        def run(receipt):
            valid = validate(receipt)
            with sqlite3.connect(held['database']) as db:
                if change == 'pause': db.execute("UPDATE tasks SET desired_state='paused'")
                else: db.execute('UPDATE llm_folder_queue SET generation=2')
            return valid
        return run
    with pytest.raises((StageConflict, ValueError)):
        held['coordinator'].reconcile_held(held['job']['job_id'], 2, store=held['store'], validator_factory=racing)
    with sqlite3.connect(held['database']) as db:
        assert db.execute('SELECT state FROM postprocessing_claims').fetchone()[0] == 'receipt_held'


def test_changed_retry_prefect_completion_during_held_recovery_is_rejected(held):
    if not held['retry']:
        pytest.skip('only a retry has a Prefect completion fingerprint')
    def racing(item, claim):
        validate = held['validator_factory'](item, claim)
        def run(receipt):
            valid = validate(receipt)
            held['prefect_proof'][0] = '8' * 64
            return valid
        return run
    with pytest.raises(StageConflict):
        held['coordinator'].reconcile_held(held['job']['job_id'], 2, store=held['store'], validator_factory=racing)
    with sqlite3.connect(held['database']) as db:
        assert db.execute('SELECT state FROM postprocessing_claims').fetchone()[0] == 'receipt_held'
