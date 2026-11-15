"""One-folder Prefect/remote wiring, using only temporary synthetic evidence."""
import asyncio
import copy
from contextlib import asynccontextmanager, contextmanager, nullcontext
import hashlib
import importlib
import json
from pathlib import Path, PureWindowsPath
import sqlite3
import sys
import threading
from types import SimpleNamespace
from uuid import uuid4, uuid5

import pytest
from filelock import FileLock

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM = ROOT.parent / 'postprocess-prefect-20261031'
for path in (ROOT, ROOT / 'tests', UPSTREAM, UPSTREAM / 'tests', UPSTREAM / 'runtime_next',
             UPSTREAM / 'results_next', UPSTREAM.parent.parent / 'prefect-paperflow'):
    sys.path.insert(0, str(path))

from folder_state import FolderStageStore, StageConflict as ReceiptConflict
from live_runtime import ResultsHooks
from postprocessing_results import ResultsStore
from remote_coordinator import RemoteSubmissionJournal
from snapshot_wire import snapshot_sha
from stage_runtime import Coordinator, StageConflict, TAG, _job_id, task_key_for_job
from test_snapshot_collect import fixture as synthetic_inputs, bind_catalog
from test_synthetic_mainline_integration import compact, synthetic_folder


def adapter():
    try:
        return importlib.import_module('remote_runtime_adapter')
    except ModuleNotFoundError as exc:
        if exc.name == 'remote_runtime_adapter':
            pytest.fail('remote Prefect runtime adapter has not been implemented')
        raise


@pytest.fixture
def sample(tmp_path):
    evidence = []
    catalog = tmp_path / 'trusted-catalog.json'
    catalog.write_text(json.dumps({'synthetic': 1}), encoding='utf8')
    for n in range(2):
        root = tmp_path / ('evidence-' + str(n))
        root.mkdir()
        values = synthetic_inputs(root, 1)
        bind_catalog(values, catalog)
        evidence.append(values)
    inputs_by_task = {item['identity']['task_id']: item for item in evidence}
    def folder_inputs(identity):
        return next(value for value in evidence
                    if value['identity']['task_run_id'] == identity['task_run_id'])
    database = tmp_path / 'control.sqlite3'
    with sqlite3.connect(database) as db:
        db.executescript('''CREATE TABLE tasks(task_id TEXT PRIMARY KEY,desired_state TEXT,
                start_generation INTEGER,start_pending INTEGER,task_status TEXT,
                prefect_run_id TEXT,prefect_order_model TEXT,name TEXT);
            CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY);
            CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,
                attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,manifest_sha256 TEXT,state TEXT,generation INTEGER);
            CREATE TABLE llm_request_admissions(task_id TEXT,task_run_id TEXT,component_id TEXT,state TEXT,kind TEXT);
        ''')
        for values in evidence:
            identity = values['identity']
            db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?)',
                       (identity['task_id'], 'running', 1, 0, 'running', identity['parent_run_id'], 'v2', 'synthetic'))
            db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?)',
                       tuple(identity.values()) + (values['manifest_sha256'], 'waiting_postprocessing', 1))
            db.execute('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?)',
                       (identity['task_id'], identity['task_run_id'], 'body', 'raw_complete', 'body'))
    gate_state = threading.local()
    entered_folder = threading.Event()
    events, wait_release = [], threading.Event()
    two_waiting, eight_waiting = threading.Event(), threading.Event()
    guard = threading.Lock()
    waiting = [0]
    slot_state = {'active': 0, 'maximum': 0, 'semaphore': None}

    @contextmanager
    def task_gate(task_id):
        with FileLock(tmp_path / ('task-' + task_id + '.lock'), timeout=3, thread_local=False):
            assert not getattr(gate_state, 'held', False)
            gate_state.held = True
            try:
                yield
            finally:
                gate_state.held = False

    @contextmanager
    def folder_gate(item):
        with FileLock(tmp_path / ('folder-' + item['task_run_id'] + '.lock'), timeout=3, thread_local=False):
            entered_folder.set()
            yield

    @asynccontextmanager
    async def slot():
        if slot_state['semaphore'] is None:
            slot_state['semaphore'] = asyncio.Semaphore(8)
        async with slot_state['semaphore']:
            slot_state['active'] += 1
            slot_state['maximum'] = max(slot_state['maximum'], slot_state['active'])
            try:
                yield
            finally:
                slot_state['active'] -= 1

    def verify(identity, generation):
        assert not getattr(gate_state, 'held', False)
        original = folder_inputs(identity)
        return dict(identity=dict(identity), generation=generation, source_llm_generation=1,
                    manifest_sha256=original['manifest_sha256'], raw_complete=True,
                    session_current=True, not_deleted=True)

    results = ResultsStore(database, admission_gate=task_gate, inspect_receipt=FolderStageStore._inspect_receipt)
    results.initialize()
    stage_store = FolderStageStore(database, observe_run=lambda identity: identity == folder_inputs(
        identity)['identity'], admission_gate=task_gate)
    stage_store.initialize()
    base = Coordinator(database, task_gate=task_gate, folder_gate=folder_gate,
        postprocess_is_open=lambda: True, rag_is_held=lambda: True,
        verify_upstream=verify, verify_execution=lambda item, context: context == item['job_id'],
        slot=slot, process_folder=lambda *_: pytest.fail('the local business path must not run'),
        results=ResultsHooks(results))
    base.initialize()
    journal = RemoteSubmissionJournal(tmp_path / 'remote-intents.sqlite3',
                                      control_database=database, admission_gate=task_gate)
    journal.initialize()
    server = {'server_id': 'synthetic-server', 'ledger_id': '3e00b559-9035-4747-84a8-6dbf319f9864'}
    runtime = dict(embedding_model_budget=1777, embedding_admission=dict(policy=dict(
        model_sha256='1' * 64, libllama_sha256='2' * 64, cl100k_sha256='3' * 64,
        pipeline_limit=1600, adapter_limit=2100)))

    def load_inputs(identity, generation, item, context):
        assert generation == 1 and context == item['job_id']
        assert not getattr(gate_state, 'held', False)
        from snapshot_prepare import counter_contract_from_runtime
        return dict(inputs=copy.deepcopy(folder_inputs(identity)),
                    counter_contract=counter_contract_from_runtime(runtime), trusted_catalog_path=catalog)

    def transfer(job, snapshot, manifest, *, verify_locally, trusted_files):
        assert verify_locally is False and manifest['binary_hash_scope'] == 'server'
        assert not getattr(gate_state, 'held', False)
        assert snapshot_sha(manifest) == snapshot['snapshot_sha256'] == job['snapshot_sha256']
        assert manifest['counter_contract'] == dict(model_sha256='1' * 64, libllama_sha256='2' * 64,
            cl100k_sha256='3' * 64, embedding_model_budget=1777, cl_limit=1600)
        assert journal.get_transfer(job['job_id'])['job'] == job
        assert trusted_files == [dict(path=str(catalog),
            sha256=hashlib.sha256(catalog.read_bytes()).hexdigest())]
        events.append(('transfer', job['job_id']))
        return dict(job=copy.deepcopy(job), server_identity=copy.deepcopy(server), state='staged',
                    snapshot_sha256=job['snapshot_sha256'], code_sha256=job['code_sha256'],
                    payload=copy.deepcopy(snapshot['payload']), staging_receipt={'synthetic': True})

    def validate_staging(job, snapshot, staging):
        assert not getattr(gate_state, 'held', False)
        return staging['staging_receipt'] == {'synthetic': True}

    def submit(job, snapshot, staging):
        assert getattr(gate_state, 'held', False)
        assert journal.get(job['job_id'])['job'] == job
        events.append(('submit', job['job_id']))
        return dict(job=copy.deepcopy(job), server_identity=copy.deepcopy(server), accepted=True)

    def wait(job):
        assert not getattr(gate_state, 'held', False)
        with guard:
            waiting[0] += 1
            if waiting[0] == 2:
                two_waiting.set()
            if waiting[0] == 8:
                eight_waiting.set()
        if not wait_release.wait(30):
            raise TimeoutError('synthetic server wait timed out')
        return remote_only_result(job)

    def validate_result(job, result):
        assert not getattr(gate_state, 'held', False)
        return result['server_receipt'] == {'synthetic': True}

    last_remote_receipt, package_seals = [], {}
    def remote_only_result(job, *, package_members=None):
        publication = tmp_path / 'server-only' / job['job_id'] / 'publication'
        names = package_members if package_members is not None else [
            'article.redacted.md', 'profile.json', 'tree.json', 'assets.json', 'chunks.json',
            'split-plan.json', 'source-map.json', 'review.json', 'processing.json', 'assets/figure.png']
        artifacts = [dict(path=str(publication.joinpath(*name.split('/'))), sha256=hashlib.sha256(name.encode()).hexdigest())
                     for name in names]
        sizes = {ref['path']: len(name.encode()) for name, ref in zip(names, artifacts)}
        identity = dict(task_id='PRE_' + job['job_id'], source_id='synthetic', source_sha256='5' * 64)
        manifest = dict(schema='paperflow/1.0', **identity, files=[dict(path=name,
            sha256=ref['sha256'], bytes=sizes[ref['path']]) for name, ref in zip(names, artifacts)],
            publication_files=[name for name in names if name not in
                {'split-plan.json', 'source-map.json', 'review.json', 'processing.json'}])
        encode = lambda value: json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        manifest_json = encode(manifest)
        manifest_sha = hashlib.sha256(manifest_json.encode()).hexdigest()
        ready_json = encode(dict(status='ready', manifest='manifest.json', manifest_sha256=manifest_sha, **identity))
        for name, text in [('manifest.json', manifest_json), ('READY.json', ready_json)]:
            path = str(publication / name)
            artifacts.append(dict(path=path, sha256=hashlib.sha256(text.encode()).hexdigest()))
            sizes[path] = len(text.encode())
        package_seals[job['job_id']] = dict(manifest_json=manifest_json, ready_json=ready_json,
            sizes=sizes, publications=manifest['publication_files'])
        receipt = dict(schema='paperflow-postprocessing-receipt/1', identity=copy.deepcopy(job['identity']),
            manifest_sha256=job['manifest_sha256'], claim_id=job['claim_id'], validation_status='passed',
            body=dict(package_path=str(publication), manifest_sha256=manifest_sha),
            attachments=[], artifacts=artifacts)
        raw = json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
        last_remote_receipt[:] = [raw]
        return dict(job=copy.deepcopy(job), state='finished', server_validation='passed', receipt=receipt,
                    receipt_json=raw, receipt_sha256=hashlib.sha256(raw.encode('utf8')).hexdigest(),
                    server_identity=copy.deepcopy(server), server_receipt={'synthetic': True})

    def attestation(request):
        job, raw, digest = request['job'], request['receipt_json'], request['receipt_sha256']
        original = json.loads(raw)
        seals = package_seals[job['job_id']]
        files = [dict(path=ref['path'], linux_path='/synthetic/server/' + str(n),
                      sha256=ref['sha256'], bytes=seals['sizes'][ref['path']], kind='output')
                 for n, ref in enumerate(original['artifacts'])]
        package = [dict(relative_path=PureWindowsPath(row['path']).relative_to(
                            PureWindowsPath(original['body']['package_path'])).as_posix(),
                        **{k: row[k] for k in ('path', 'linux_path', 'sha256', 'bytes')}) for row in files]
        publications = [copy.deepcopy(row) for row in files if PureWindowsPath(row['path']).relative_to(
            PureWindowsPath(original['body']['package_path'])).as_posix() in seals['publications']]
        evidence = dict(schema='paperflow-linux-artifact-map/1', integrity_status='passed',
            job=copy.deepcopy(job), identity=copy.deepcopy(job['identity']), receipt_canonical_sha256=digest,
            files=files, body_package=package, body_publications=publications,
            body_manifest_json=seals['manifest_json'], body_ready_json=seals['ready_json'],
            attachments=[], rag_admission='not_granted',
            semantic_validation='depends_on_reviewed_business_processor',
            privacy_validation='depends_on_reviewed_business_processor', worker_termination='caller_must_verify',
            original_receipt_rewritten=False)
        return dict(schema='paperflow-authenticated-remote-result/1', server_identity=copy.deepcopy(server),
            job=copy.deepcopy(job), receipt_json=raw, receipt_sha256=digest, ledger_state='finished',
            worker_terminated=True, business_validation='processor_reported_pass',
            business_code_sha256=job['code_sha256'], artifact_map=evidence)

    stage_store, results = adapter().make_remote_receipt_stores(
        database=database, admission_gate=task_gate, journal=journal, server_identity=server,
        observe_run=lambda actual: actual == folder_inputs(actual)['identity'],
        inspect_batch=lambda requests: [attestation(request) for request in requests],
        max_summary_receipts=50)
    base.results = ResultsHooks(results)

    return dict(database=database, evidence=evidence, catalog=catalog, task_gate=task_gate,
        entered_folder=entered_folder, events=events, wait_release=wait_release, two_waiting=two_waiting,
        eight_waiting=eight_waiting, inputs_by_task=inputs_by_task,
        base=base, stage_store=stage_store, results=results, journal=journal, server=server,
        gate_held=lambda: getattr(gate_state, 'held', False), remote_only_result=remote_only_result,
        last_remote_receipt=last_remote_receipt, attestation=attestation,
        slot_state=slot_state, options=dict(
            base=base, stage_store=stage_store, journal=journal, server_identity=server,
            load_inputs=load_inputs,
            snapshot_options=dict(catalog={'synthetic': 1}, code_sha256='d' * 64,
                paperflow_root_windows=r'F:\release\core', business_workspace_windows=r'F:\workspace',
                output_root_windows=r'F:\pfo', runtime_template_sha256='e' * 64,
                allowed_roots=[tmp_path], max_json_file_bytes=8 * 1024 * 1024,
                max_total_json_bytes=64 * 1024 * 1024),
            transfer_snapshot=transfer, validate_staging=validate_staging, submit_once=submit,
            wait_for_result=wait, validate_result=validate_result))


def make(sample):
    return adapter().make_remote_coordinator(**sample['options'])


def counts(database):
    with sqlite3.connect(database) as db:
        return tuple(db.execute('SELECT count(*) FROM ' + name).fetchone()[0] for name in
                     ('paperflow_postprocess_stage_jobs', 'postprocessing_results', 'postprocessing_claims'))


def test_paused_reservation_creates_no_stage_claim_or_result(sample):
    remote = make(sample)
    identity = sample['evidence'][0]['identity']
    with sample['task_gate'](identity['task_id']), sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (identity['task_id'],))
    with pytest.raises(StageConflict):
        remote.reserve(identity, 1)
    assert counts(sample['database']) == (0, 0, 0)


@pytest.mark.parametrize('stop', ['paused', 'deleted'])
def test_paused_or_deleted_before_execution_does_not_create_result_or_claim(sample, stop):
    remote = make(sample)
    identity = sample['evidence'][0]['identity']
    job = remote.reserve(identity, 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    with sample['task_gate'](identity['task_id']), sqlite3.connect(sample['database']) as db:
        if stop == 'paused':
            db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (identity['task_id'],))
        else:
            db.execute('INSERT INTO task_deletions VALUES(?)', (identity['task_id'],))
    with pytest.raises(StageConflict):
        remote._execute_sync(job['job_id'], job['job_id'])
    assert counts(sample['database']) == (1, 0, 0)
    assert remote.job(job['job_id'])['state'] == 'submitted'
    assert sample['events'] == []


@pytest.mark.parametrize('phase', ['preparation', 'transfer'])
@pytest.mark.parametrize('same_parent', [False, True])
def test_distinct_folder_preparation_and_transfer_are_concurrent_in_existing_slots(sample, phase, same_parent):
    if same_parent:
        first, second = sample['evidence']
        old_task = second['identity']['task_id']
        update = {key: first['identity'][key] for key in ('task_id', 'parent_run_id')}
        second['identity'].update(update)
        second['body']['identity'].update(update)
        for response in second['body']['responses']: response['identity'].update(update)
        for attachment in second['attachments']: attachment['ticket'].update(update)
        with sqlite3.connect(sample['database']) as db:
            db.execute('UPDATE llm_folder_queue SET task_id=?,parent_run_id=? WHERE task_id=?',
                       (update['task_id'], update['parent_run_id'], old_task))
            db.execute('UPDATE llm_request_admissions SET task_id=? WHERE task_id=?',
                       (update['task_id'], old_task))
    barrier = threading.Barrier(2)
    key = 'load_inputs' if phase == 'preparation' else 'transfer_snapshot'
    original = sample['options'][key]
    def overlapping(*args, **kwargs):
        barrier.wait(timeout=10)  # a host-global serial gate makes this fail
        return original(*args, **kwargs)
    sample['options'][key] = overlapping
    remote = make(sample)
    assert remote.slot is sample['base'].slot
    assert remote.folder_gate is sample['base'].folder_gate
    assert remote.task_gate is sample['base'].task_gate
    jobs = [remote.reserve(value['identity'], 1) for value in sample['evidence']]
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted'")
    async def exercise():
        tasks = [asyncio.create_task(remote.execute(job['job_id'], job['job_id'])) for job in jobs]
        try:
            assert await asyncio.to_thread(sample['two_waiting'].wait, 10)
        finally:
            sample['wait_release'].set()
        return await asyncio.gather(*tasks)
    results = asyncio.run(exercise())
    assert all(result['state'] == 'waiting_rag' for result in results)
    assert sample['slot_state']['maximum'] == 2
    assert [kind for kind, _ in sample['events']].count('transfer') == 2
    assert [kind for kind, _ in sample['events']].count('submit') == 2
    assert counts(sample['database']) == (2, 2, 2)
    with sqlite3.connect(sample['database']) as db:
        assert set(row[0] for row in db.execute('SELECT state FROM postprocessing_results')) == {'passed'}


def test_eight_synthetic_server_waits_can_use_all_existing_slots(sample, tmp_path):
    for n in range(2, 8):
        root = tmp_path / ('evidence-' + str(n))
        root.mkdir()
        item = synthetic_inputs(root, 1)
        bind_catalog(item, sample['catalog'])
        sample['evidence'].append(item)
        identity = item['identity']
        sample['inputs_by_task'][identity['task_id']] = item
        with sqlite3.connect(sample['database']) as db:
            db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?)',
                (identity['task_id'], 'running', 1, 0, 'running', identity['parent_run_id'], 'v2', 'synthetic'))
            db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?)',
                tuple(identity.values()) + (item['manifest_sha256'], 'waiting_postprocessing', 1))
            db.execute('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?)',
                (identity['task_id'], identity['task_run_id'], 'body', 'raw_complete', 'body'))
    remote = make(sample)
    jobs = [remote.reserve(item['identity'], 1) for item in sample['evidence']]
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted'")
    async def exercise():
        tasks = [asyncio.create_task(remote.execute(job['job_id'], job['job_id'])) for job in jobs]
        try:
            assert await asyncio.to_thread(sample['eight_waiting'].wait, 20)
        finally:
            sample['wait_release'].set()
        return await asyncio.gather(*tasks)
    results = asyncio.run(exercise())
    assert sample['slot_state']['maximum'] == 8
    assert all(result['state'] == 'waiting_rag' for result in results)
    assert counts(sample['database']) == (8, 8, 8)


def test_ambiguous_transfer_never_retries(sample):
    identity = sample['evidence'][0]['identity']
    calls = []
    def transfer(*args, **kwargs):
        calls.append('transfer')
        raise ConnectionError('synthetic transfer connection lost')
    sample['options']['transfer_snapshot'] = transfer
    remote = make(sample)
    job = remote.reserve(identity, 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    with pytest.raises(ConnectionError):
        remote._execute_sync(job['job_id'], job['job_id'])
    assert remote.job(job['job_id'])['state'] == 'execution_unknown'
    with sqlite3.connect(sample['database']) as db:
        assert db.execute('SELECT state FROM postprocessing_claims').fetchone()[0] == 'unknown'
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] == 'unresolved'
    with pytest.raises(StageConflict):
        remote._execute_sync(job['job_id'], job['job_id'])
    assert calls == ['transfer']


@pytest.mark.parametrize('phase', ['preparation', 'transfer'])
def test_pause_during_preparation_or_upload_still_blocks_server_start(sample, phase):
    key = 'load_inputs' if phase == 'preparation' else 'transfer_snapshot'
    original = sample['options'][key]
    identity = sample['evidence'][0]['identity']
    def pause_after(*args, **kwargs):
        value = original(*args, **kwargs)
        assert not sample['gate_held']()
        with sample['task_gate'](identity['task_id']), sqlite3.connect(sample['database']) as db:
            db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (identity['task_id'],))
        return value
    sample['options'][key] = pause_after
    remote = make(sample)
    job = remote.reserve(identity, 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    with pytest.raises(ReceiptConflict, match='paused'):
        remote._execute_sync(job['job_id'], job['job_id'])
    assert all(kind != 'submit' for kind, _ in sample['events'])
    assert sample['journal'].get(job['job_id']) is None
    assert bool(sample['journal'].get_transfer(job['job_id'])) is (phase == 'transfer')
    assert remote.job(job['job_id'])['state'] == 'execution_unknown'


def test_pause_after_authorized_start_retains_completed_receipt_as_held(sample):
    original = sample['options']['wait_for_result']
    identity = sample['evidence'][0]['identity']
    def pause_then_result(job):
        with sample['task_gate'](identity['task_id']), sqlite3.connect(sample['database']) as db:
            db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (identity['task_id'],))
        return original(job)
    sample['options']['wait_for_result'] = pause_then_result
    sample['wait_release'].set()
    remote = make(sample)
    job = remote.reserve(identity, 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    assert remote._execute_sync(job['job_id'], job['job_id'])['state'] == 'receipt_held'
    with sqlite3.connect(sample['database']) as db:
        assert db.execute('SELECT count(*) FROM postprocessing_receipts').fetchone()[0] == 1
        assert db.execute('SELECT state FROM postprocessing_claims').fetchone()[0] == 'receipt_held'


def test_input_loader_reuses_stage_bound_handoff_and_existing_build_inputs(tmp_path):
    identity, manifest, paper, admissions, _catalog, work = synthetic_folder(tmp_path, 1)
    digest = compact(manifest, sort=True)
    database = tmp_path / 'loader-control.sqlite3'
    with sqlite3.connect(database) as db:
        db.execute('CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,'
                   'attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,manifest_sha256 TEXT,state TEXT,generation INTEGER)')
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?)',
                   tuple(identity.values()) + (digest, 'waiting_postprocessing', 1))
        fields = list(admissions[0])
        db.execute('CREATE TABLE llm_request_admissions(' + ','.join(name + ' TEXT' for name in fields) + ')')
        for row in admissions:
            db.execute('INSERT INTO llm_request_admissions VALUES(' + ','.join('?' for _ in fields) + ')',
                       tuple(row[name] for name in fields))
    job_id = _job_id(identity['task_run_id'])
    item = dict(identity, job_id=job_id, folder_run_id=identity['task_run_id'], generation=1,
                source_llm_generation=1, manifest_sha256=digest, task_key=task_key_for_job(job_id))
    stage = SimpleNamespace(id=job_id, flow_run_id=None, task_key=item['task_key'],
        dynamic_key='paperflow-postprocess-' + job_id, tags=[TAG],
        state=SimpleNamespace(is_running=lambda: True))
    paused = SimpleNamespace(is_paused=lambda: True, state_details=SimpleNamespace(pause_reschedule=True))
    calls = []
    class Client:
        api_url = 'http://synthetic-prefect/api'
        def read_task_run(self, actual):
            calls.append(('stage', str(actual)))
            return stage
        def read_flow_run(self, actual):
            calls.append(('folder', str(actual)))
            return SimpleNamespace(id=identity['task_run_id'], state=paused, name='synthetic等待后处理')
    client = Client()
    class Upstream:
        api_url = client.api_url
        def load(self, actual, *, require_running):
            calls.append(('upstream', require_running))
            assert actual == identity and require_running is False
            return {'manifest': manifest, 'paper': paper}
    runtime = dict(embedding_model_budget=2500, embedding_admission=dict(policy=dict(
        model_sha256='1' * 64, libllama_sha256='2' * 64, cl100k_sha256='3' * 64,
        pipeline_limit=1800, adapter_limit=2000)))
    catalog_path = tmp_path / 'trusted-backend-catalog.json'
    catalog_path.write_text('{"synthetic":1}', encoding='utf8')
    settings = SimpleNamespace(path=lambda key: catalog_path if key == 'catalog_path' else None)
    loader = adapter().make_stage_input_loader(database=database, client_factory=lambda: nullcontext(client),
        upstream_factory=lambda _client: Upstream(),
        backend_factory=lambda _verified: SimpleNamespace(invoke=None, runtime=runtime, settings=settings,
            attachment_work=lambda *_args: work))
    result = loader(identity, 1, item, stage)
    assert result['inputs']['identity'] == identity and result['inputs']['manifest_sha256'] == digest
    assert len(result['inputs']['body']['responses']) == 1 and len(result['inputs']['attachments']) == 1
    assert result['counter_contract'] == dict(model_sha256='1' * 64, libllama_sha256='2' * 64,
        cl100k_sha256='3' * 64, embedding_model_budget=2500, cl_limit=1800)
    assert result['trusted_catalog_path'] == catalog_path
    assert [kind for kind, _ in calls].count('stage') == 2
    assert [kind for kind, _ in calls].count('upstream') == 1
    with pytest.raises(StageConflict):
        loader(identity, 2, item, stage)
    unsafe_loader = adapter().make_stage_input_loader(database=database, client_factory=lambda: nullcontext(client),
        upstream_factory=lambda _client: Upstream(), backend_factory=lambda _verified:
            SimpleNamespace(invoke=lambda *_args: None, attachment_work=lambda *_args: work))
    with pytest.raises(StageConflict, match='model invocation must be disabled'):
        unsafe_loader(identity, 1, item, stage)
    unconfigured_loader = adapter().make_stage_input_loader(database=database,
        client_factory=lambda: nullcontext(client), upstream_factory=lambda _client: Upstream(),
        backend_factory=lambda _verified: SimpleNamespace(invoke=None, runtime=runtime,
            attachment_work=lambda *_args: work))
    with pytest.raises(StageConflict, match='trusted backend catalog configuration'):
        unconfigured_loader(identity, 1, item, stage)


def test_rearm_never_submitted_preserves_original_prefect_identity(sample):
    from prefect.exceptions import ObjectNotFound

    remote = make(sample)
    identity = sample['evidence'][0]['identity']
    job = remote.reserve(identity, 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='stopped_before_submit' WHERE job_id=?",
                   (job['job_id'],))
        db.execute('UPDATE tasks SET start_generation=2 WHERE task_id=?', (identity['task_id'],))

    class Client:
        api_url = 'http://synthetic-prefect/api'
        def read_task_run(self, unused):
            raise ObjectNotFound('synthetic task was never submitted')

    reopened = remote.rearm_stopped(job['job_id'], 2, client=Client(),
                                    expected_api_url='http://synthetic-prefect/api')
    assert reopened['job_id'] == job['job_id'] and reopened['state'] == 'reserved'
    assert reopened['generation'] == 2 and counts(sample['database']) == (1, 0, 0)


def test_remote_held_recovery_is_explicitly_blocked_until_reconciled(sample):
    remote = make(sample)
    identity = sample['evidence'][0]['identity']
    job = remote.reserve(identity, 1)
    with pytest.raises(StageConflict, match='held receipt reconciliation is not connected'):
        remote.reconcile_held(job['job_id'], 2, store=sample['stage_store'], validator_factory=lambda *_: None)
    assert counts(sample['database']) == (1, 0, 0)


def test_trusted_remote_stores_share_one_inspector_and_do_not_read_server_files(sample, tmp_path):
    identity = sample['evidence'][0]['identity']
    calls = []
    def inspect_batch(requests):
        assert not sample['gate_held']()
        calls.append(copy.deepcopy(requests))
        return [sample['attestation'](request) for request in requests]

    stage_store, results = adapter().make_remote_receipt_stores(
        database=sample['database'], admission_gate=sample['task_gate'],
        journal=sample['journal'], server_identity=sample['server'],
        observe_run=lambda actual: any(actual == row['identity'] for row in sample['evidence']), inspect_batch=inspect_batch,
        max_summary_receipts=50)
    assert stage_store.inspect_receipt is results.inspect_receipt
    sample['base'].results = ResultsHooks(results)
    sample['options']['stage_store'] = stage_store
    sample['options']['wait_for_result'] = sample['remote_only_result']
    remote = make(sample)
    job = remote.reserve(identity, 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    result = remote._execute_sync(job['job_id'], job['job_id'])
    assert result['state'] == 'waiting_rag'
    with sqlite3.connect(sample['database']) as db:
        raw, digest = db.execute('SELECT receipt_json,receipt_sha256 FROM postprocessing_receipts').fetchone()
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] == 'passed'
    assert raw == sample['last_remote_receipt'][0]
    assert hashlib.sha256(raw.encode('utf8')).hexdigest() == digest
    assert not Path(json.loads(raw)['artifacts'][0]['path']).exists()
    assert len(calls) >= 3


def test_remote_summary_batch_inspection_does_not_hold_task_or_database_locks(sample):
    identity = sample['evidence'][0]['identity']
    during_summary = [False]
    calls = []
    def inspect_batch(requests):
        assert not sample['gate_held']()
        calls.append(len(requests))
        if during_summary[0]:
            # This COMMIT must be possible while the remote call is in progress.
            with sample['task_gate'](identity['task_id']), sqlite3.connect(sample['database'], timeout=.2) as db:
                db.execute("UPDATE tasks SET name=name WHERE task_id=?", (identity['task_id'],))
        return [sample['attestation'](request) for request in requests]

    stage_store, results = adapter().make_remote_receipt_stores(
        database=sample['database'], admission_gate=sample['task_gate'],
        journal=sample['journal'], server_identity=sample['server'],
        observe_run=lambda actual: any(actual == row['identity'] for row in sample['evidence']), inspect_batch=inspect_batch,
        max_summary_receipts=50)
    sample['base'].results = ResultsHooks(results)
    sample['options']['stage_store'] = stage_store
    sample['options']['wait_for_result'] = sample['remote_only_result']
    remote = make(sample)
    job = remote.reserve(identity, 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    remote._execute_sync(job['job_id'], job['job_id'])
    calls.clear()
    during_summary[0] = True
    summary = results.summary()
    assert summary['available'] is True and summary['counts']['passed'] == 1
    assert calls == [1]
    calls.clear()
    assert stage_store.summary()['waiting_rag'] == 1
    assert calls == [1]


def test_remote_coordinator_rejects_nonremote_or_mismatched_inspector(sample):
    legacy = FolderStageStore(sample['database'], observe_run=lambda _: True, admission_gate=sample['task_gate'])
    options = dict(sample['options'], stage_store=legacy)
    with pytest.raises(ValueError):
        adapter().make_remote_coordinator(**options)


def inspection_case(sample, *, package_members=None):
    identity = sample['evidence'][0]['identity']
    job = dict(job_id=str(uuid5(__import__('remote_jobs').NAMESPACE, identity['task_run_id'])),
        claim_id=str(uuid4()), identity=identity, generation=1, manifest_sha256='c' * 64,
        snapshot_sha256='d' * 64, code_sha256='e' * 64)
    result = sample['remote_only_result'](job, package_members=package_members)
    request = {key: result[key] for key in ('job', 'server_identity', 'receipt_json', 'receipt_sha256')}
    return request, sample['attestation'](request)


def reseal_inspection(request, result, *, manifest=None, ready=None):
    """Make synthetic seal hashes internally consistent to test structure, not drift."""
    encode = lambda value: json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    evidence = result['artifact_map']; original = json.loads(request['receipt_json'])
    manifest = json.loads(evidence['body_manifest_json']) if manifest is None else manifest
    manifest_json = encode(manifest); manifest_sha = hashlib.sha256(manifest_json.encode()).hexdigest()
    if ready is None:
        ready = json.loads(evidence['body_ready_json']); ready['manifest_sha256'] = manifest_sha
    ready_json = encode(ready)
    original['body']['manifest_sha256'] = manifest_sha
    package = PureWindowsPath(original['body']['package_path'])
    for name, text in [('manifest.json', manifest_json), ('READY.json', ready_json)]:
        path = package / name; digest = hashlib.sha256(text.encode()).hexdigest()
        for ref in original['artifacts']:
            if PureWindowsPath(ref['path']) == path: ref['sha256'] = digest
        for rows in (evidence['files'], evidence['body_package'], evidence['body_publications']):
            for row in rows:
                if PureWindowsPath(row['path']) == path:
                    row.update(sha256=digest, bytes=len(text.encode()))
    evidence.update(body_manifest_json=manifest_json, body_ready_json=ready_json)
    raw = encode(original); digest = hashlib.sha256(raw.encode()).hexdigest()
    request.update(receipt_json=raw, receipt_sha256=digest)
    result.update(receipt_json=raw, receipt_sha256=digest)
    evidence['receipt_canonical_sha256'] = digest


@pytest.mark.parametrize('relative', ['article.redacted.md', 'source-map.json', 'assets/figure.png'])
def test_receiver_rejects_body_package_mapping_omission_even_when_global_file_map_has_it(sample, relative):
    request, result = inspection_case(sample)
    result['artifact_map']['body_package'] = [row for row in result['artifact_map']['body_package']
        if row['relative_path'] != relative]
    with pytest.raises(ReceiptConflict): adapter().RemoteReceiptInspector._accepted(request, result)


@pytest.mark.parametrize('change', ['omitted', 'duplicate', 'wrong_hash', 'wrong_path', 'unmapped', 'missing_list'])
def test_receiver_rejects_incomplete_or_forged_publication_mappings(sample, change):
    request, result = inspection_case(sample)
    evidence = result['artifact_map']; publications = evidence['body_publications']
    if change == 'omitted': publications.pop()
    if change == 'duplicate': publications.append(copy.deepcopy(publications[0]))
    if change == 'wrong_hash': publications[0]['sha256'] = '0' * 64
    if change == 'wrong_path': publications[0]['path'] = r'F:\another\article.redacted.md'
    if change == 'unmapped': publications[0]['linux_path'] = '/different/unverified/file'
    if change == 'missing_list': evidence.pop('body_publications')
    with pytest.raises(ReceiptConflict): adapter().RemoteReceiptInspector._accepted(request, result)


@pytest.mark.parametrize('field', ['body_manifest_json', 'body_ready_json'])
@pytest.mark.parametrize('change', ['missing', 'changed', 'oversize'])
def test_receiver_requires_exact_original_manifest_and_ready_text(sample, field, change):
    request, result = inspection_case(sample)
    evidence = result['artifact_map']
    if change == 'missing': evidence.pop(field)
    if change == 'changed': evidence[field] += ' '
    if change == 'oversize': evidence[field] = ' ' * (4 * 1024 * 1024 + 1)
    with pytest.raises(ReceiptConflict): adapter().RemoteReceiptInspector._accepted(request, result)


@pytest.mark.parametrize('missing', ['article.redacted.md', 'profile.json', 'tree.json', 'assets.json',
                                  'chunks.json', 'split-plan.json', 'source-map.json'])
def test_receiver_requires_mandatory_members_even_when_receipt_and_manifest_consistently_omit_them(sample, missing):
    names = ['article.redacted.md', 'profile.json', 'tree.json', 'assets.json', 'chunks.json',
             'split-plan.json', 'source-map.json', 'assets/deep/figure.png']
    request, result = inspection_case(sample, package_members=[name for name in names if name != missing])
    with pytest.raises(ReceiptConflict): adapter().RemoteReceiptInspector._accepted(request, result)


def test_receiver_accepts_complete_variable_package_and_nested_asset_publications(sample):
    request, result = inspection_case(sample, package_members=[
        'article.redacted.md', 'profile.json', 'tree.json', 'assets.json', 'chunks.json',
        'split-plan.json', 'source-map.json', 'review.json', 'assets/chapter/figure.png', 'assets/another/table.csv'])
    evidence = adapter().RemoteReceiptInspector._accepted(request, result)
    assert len(evidence['body_package']) == 12
    assert any(row['path'].endswith('assets\\chapter\\figure.png') for row in evidence['body_publications'])
    assert evidence['semantic_validation'] == evidence['privacy_validation'] == 'depends_on_reviewed_business_processor'


@pytest.mark.parametrize('change', ['missing_asset', 'extra_asset', 'duplicate', 'case_alias', 'wrong_bytes', 'wrong_hash',
                                  'self_reference', 'unsafe_path', 'publication_duplicate', 'publication_undeclared'])
def test_receiver_checks_original_manifest_structure_after_all_synthetic_hashes_are_consistent(sample, change):
    request, result = inspection_case(sample); evidence = result['artifact_map']
    manifest = json.loads(evidence['body_manifest_json']); entries = manifest['files']
    if change == 'missing_asset': entries[:] = [row for row in entries if row['path'] != 'assets/figure.png']
    if change == 'extra_asset': entries.append(dict(path='assets/unmapped.png', sha256='0' * 64, bytes=3))
    if change == 'duplicate': entries.append(copy.deepcopy(entries[0]))
    if change == 'case_alias': entries.append(dict(entries[0], path='ARTICLE.REDACTED.MD'))
    if change == 'wrong_bytes': entries[0]['bytes'] += 1
    if change == 'wrong_hash': entries[0]['sha256'] = '0' * 64
    if change == 'self_reference': entries.append(dict(path='manifest.json', sha256='0' * 64, bytes=3))
    if change == 'unsafe_path': entries[0]['path'] = '../article.redacted.md'
    if change in {'publication_duplicate', 'publication_undeclared'}:
        manifest['publication_files'].append(manifest['publication_files'][0] if change == 'publication_duplicate' else 'manifest.json')
        evidence['body_publications'].append(copy.deepcopy(evidence['body_publications'][0]))
    reseal_inspection(request, result, manifest=manifest)
    with pytest.raises(ReceiptConflict): adapter().RemoteReceiptInspector._accepted(request, result)


@pytest.mark.parametrize('field,value', [('status', 'not_ready'), ('manifest', 'another.json'),
                                       ('manifest_sha256', '0' * 64), ('source_id', 'another-source')])
def test_receiver_checks_ready_binding_even_after_ready_artifact_is_rehashed(sample, field, value):
    request, result = inspection_case(sample)
    ready = json.loads(result['artifact_map']['body_ready_json']); ready[field] = value
    reseal_inspection(request, result, ready=ready)
    with pytest.raises(ReceiptConflict): adapter().RemoteReceiptInspector._accepted(request, result)


@pytest.mark.parametrize('change', ['duplicate_package', 'extra_global_mapping', 'extra_package_mapping', 'duplicate_artifact'])
def test_receiver_rejects_ambiguous_or_unreceipted_package_mapping(sample, change):
    request, result = inspection_case(sample); evidence = result['artifact_map']
    if change == 'duplicate_package': evidence['body_package'].append(copy.deepcopy(evidence['body_package'][0]))
    elif change == 'duplicate_artifact':
        original = json.loads(request['receipt_json']); original['artifacts'].append(copy.deepcopy(original['artifacts'][0]))
        raw = json.dumps(original, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        digest = hashlib.sha256(raw.encode()).hexdigest()
        request.update(receipt_json=raw, receipt_sha256=digest); result.update(receipt_json=raw, receipt_sha256=digest)
        evidence['receipt_canonical_sha256'] = digest
    else:
        extra = dict(evidence['files'][0], path=str(PureWindowsPath(json.loads(request['receipt_json'])['body']['package_path'])
            / 'assets/unreceipted.png'), linux_path='/synthetic/another-file')
        evidence['files'].append(extra)
        if change == 'extra_package_mapping': evidence['body_package'].append(dict(extra, relative_path='assets/unreceipted.png'))
    with pytest.raises(ReceiptConflict): adapter().RemoteReceiptInspector._accepted(request, result)


@pytest.mark.parametrize('field', ['server', 'job', 'receipt', 'worker', 'business', 'code', 'integrity',
                                  'package', 'package_path', 'package_manifest'])
def test_untrusted_or_incomplete_remote_attestation_does_not_publish(sample, field):
    identity = sample['evidence'][0]['identity']
    def inspect_batch(requests):
        result = sample['attestation'](requests[0])
        if field == 'server': result['server_identity']['server_id'] = 'another-server'
        if field == 'job': result['job']['snapshot_sha256'] = '0' * 64
        if field == 'receipt': result['receipt_json'] += ' '
        if field == 'worker': result['worker_terminated'] = False
        if field == 'business': result['business_validation'] = 'integrity_only'
        if field == 'code': result['business_code_sha256'] = '0' * 64
        if field == 'integrity': result['artifact_map']['integrity_status'] = 'unknown'
        if field == 'package': result['artifact_map']['body_package'] = []
        if field == 'package_path': result['artifact_map']['body_package'][0]['relative_path'] = 'wrong.md'
        if field == 'package_manifest':
            next(row for row in result['artifact_map']['body_package']
                 if row['relative_path'] == 'manifest.json')['relative_path'] = 'wrong.json'
        return [result]

    stage_store, results = adapter().make_remote_receipt_stores(
        database=sample['database'], admission_gate=sample['task_gate'], journal=sample['journal'],
        server_identity=sample['server'], observe_run=lambda actual: actual == identity,
        inspect_batch=inspect_batch, max_summary_receipts=50)
    sample['base'].results = ResultsHooks(results)
    sample['options']['stage_store'] = stage_store
    sample['options']['wait_for_result'] = sample['remote_only_result']
    remote = make(sample)
    job = remote.reserve(identity, 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    with pytest.raises(ReceiptConflict):
        remote._execute_sync(job['job_id'], job['job_id'])
    with sqlite3.connect(sample['database']) as db:
        assert db.execute('SELECT state FROM postprocessing_claims').fetchone()[0] == 'unknown'
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] == 'unresolved'
        assert db.execute('SELECT count(*) FROM postprocessing_receipts').fetchone()[0] == 0
    assert [kind for kind, _ in sample['events']].count('submit') == 1


def test_changed_receipt_during_unlocked_summary_makes_summary_unavailable(sample):
    identity = sample['evidence'][0]['identity']
    change = [False]
    def inspect_batch(requests):
        result = [sample['attestation'](request) for request in requests]
        if change[0]:
            with sample['task_gate'](identity['task_id']), sqlite3.connect(sample['database']) as db:
                db.execute("UPDATE postprocessing_receipts SET receipt_json=receipt_json || ' '")
        return result

    stage_store, results = adapter().make_remote_receipt_stores(
        database=sample['database'], admission_gate=sample['task_gate'], journal=sample['journal'],
        server_identity=sample['server'], observe_run=lambda actual: actual == identity,
        inspect_batch=inspect_batch, max_summary_receipts=50)
    sample['base'].results = ResultsHooks(results)
    sample['options']['stage_store'] = stage_store
    sample['options']['wait_for_result'] = sample['remote_only_result']
    remote = make(sample)
    job = remote.reserve(identity, 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    remote._execute_sync(job['job_id'], job['job_id'])
    change[0] = True
    assert results.summary()['available'] is False


def test_remote_acceptance_checks_never_hold_task_or_database_locks(sample):
    identity = sample['evidence'][0]['identity']
    calls = []
    def inspect_batch(requests):
        assert not sample['gate_held']()
        with sample['task_gate'](identity['task_id']), sqlite3.connect(sample['database'], timeout=.2) as db:
            db.execute("UPDATE tasks SET name=name WHERE task_id=?", (identity['task_id'],))
        calls.append(len(requests))
        return [sample['attestation'](request) for request in requests]

    stage_store, results = adapter().make_remote_receipt_stores(
        database=sample['database'], admission_gate=sample['task_gate'], journal=sample['journal'],
        server_identity=sample['server'], observe_run=lambda actual: actual == identity,
        inspect_batch=inspect_batch, max_summary_receipts=50)
    sample['base'].results = ResultsHooks(results)
    sample['options']['stage_store'] = stage_store
    sample['options']['wait_for_result'] = sample['remote_only_result']
    remote = make(sample)
    job = remote.reserve(identity, 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    assert remote._execute_sync(job['job_id'], job['job_id'])['state'] == 'waiting_rag'
    assert len(calls) >= 3


def test_remote_inspection_timeout_after_receipt_save_fences_unaccepted_claim(sample):
    identity = sample['evidence'][0]['identity']
    def inspect_batch(requests):
        with sqlite3.connect(sample['database']) as db:
            row = db.execute('SELECT state FROM postprocessing_claims').fetchone()
        if row and row[0] == 'waiting_rag':
            raise TimeoutError('synthetic result prefetch timed out')
        return [sample['attestation'](request) for request in requests]
    stage_store, results = adapter().make_remote_receipt_stores(
        database=sample['database'], admission_gate=sample['task_gate'], journal=sample['journal'],
        server_identity=sample['server'], observe_run=lambda actual: actual == identity,
        inspect_batch=inspect_batch, max_summary_receipts=50)
    sample['base'].results = ResultsHooks(results)
    sample['options']['stage_store'] = stage_store
    sample['options']['wait_for_result'] = sample['remote_only_result']
    remote = make(sample)
    job = remote.reserve(identity, 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    with pytest.raises(TimeoutError):
        remote._execute_sync(job['job_id'], job['job_id'])
    with sqlite3.connect(sample['database']) as db:
        assert db.execute('SELECT state FROM postprocessing_claims').fetchone()[0] == 'unknown'
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] == 'unresolved'
        assert db.execute('SELECT count(*) FROM postprocessing_receipts').fetchone()[0] == 1


def _synthetic_passed_receipts(sample, monkeypatch, count):
    """Populate only the temporary ledger; no workers, models or network."""
    from remote_coordinator import NAMESPACE
    intents, claims = {}, []
    with sqlite3.connect(sample['database']) as db:
        for n in range(count):
            identity = dict(task_id=str(uuid4()), group_id=uuid4().hex, paper_id='synthetic-' + str(n),
                receipt_id='b' * 64, attempt_id='synthetic', parent_run_id=str(uuid4()), task_run_id=str(uuid4()))
            claim = str(uuid4())
            job = dict(job_id=str(uuid5(NAMESPACE, identity['task_run_id'])), claim_id=claim,
                identity=identity, generation=1, manifest_sha256='c' * 64,
                snapshot_sha256='d' * 64, code_sha256='e' * 64)
            result = sample['remote_only_result'](job)
            intents[job['job_id']] = dict(job=copy.deepcopy(job), server_identity=sample['server'])
            claims.append(claim)
            values = tuple(identity.values())
            db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?)',
                (identity['task_id'], 'running', 1, 0, 'running', identity['parent_run_id'], 'v2', 'synthetic'))
            db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?)',
                values + (job['manifest_sha256'], 'waiting_postprocessing', 1))
            db.execute('INSERT INTO postprocessing_claims VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                values + (job['manifest_sha256'], claim, 1, 'waiting_rag', None, n, n))
            db.execute('INSERT INTO postprocessing_receipts VALUES(?,?,?,?)',
                (claim, result['receipt_json'], result['receipt_sha256'], n))
            db.execute('INSERT INTO postprocessing_results VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                values + (job['manifest_sha256'], 1, str(uuid4()), 'passed', 'passed', claim,
                          result['receipt_sha256'], None, None, n, n))
    monkeypatch.setattr(sample['journal'], 'get', lambda job_id: copy.deepcopy(intents.get(job_id)))
    return claims


def test_remote_summaries_check_all_65_receipts_in_bounded_batches_without_locks(sample, monkeypatch):
    _synthetic_passed_receipts(sample, monkeypatch, 65)
    calls = []
    def inspect_batch(requests):
        assert not sample['gate_held']()
        assert 1 <= len(requests) <= 32
        task_id = requests[0]['job']['identity']['task_id']
        with sample['task_gate'](task_id), sqlite3.connect(sample['database'], timeout=.2) as db:
            db.execute('UPDATE tasks SET name=name WHERE task_id=?', (task_id,))
        calls.append(len(requests))
        return [sample['attestation'](request) for request in requests]

    stage, results = adapter().make_remote_receipt_stores(
        database=sample['database'], admission_gate=sample['task_gate'], journal=sample['journal'],
        server_identity=sample['server'], observe_run=lambda _: True, inspect_batch=inspect_batch,
        max_summary_receipts=100)
    summary = results.summary()
    assert summary['available'] is True and summary['counts']['passed'] == 65
    assert calls == [32, 32, 1]
    calls.clear()
    summary = stage.summary()
    assert summary['waiting_rag'] == 65 and calls == [32, 32, 1]


@pytest.mark.parametrize('failure', ['connection', 'missing', 'reordered', 'changed_local_receipt'])
def test_remote_summaries_fail_closed_on_later_batch_or_changed_snapshot(sample, monkeypatch, failure):
    claims = _synthetic_passed_receipts(sample, monkeypatch, 65)
    calls = []
    def inspect_batch(requests):
        assert not sample['gate_held']() and len(requests) <= 32
        calls.append(len(requests))
        values = [sample['attestation'](request) for request in requests]
        if len(calls) == 2:
            if failure == 'connection': raise ConnectionError('synthetic interrupted inspection')
            if failure == 'missing': return values[:-1]
            if failure == 'reordered': return list(reversed(values))
            if failure == 'changed_local_receipt':
                with sample['task_gate'](requests[0]['job']['identity']['task_id']), sqlite3.connect(sample['database']) as db:
                    db.execute("UPDATE postprocessing_receipts SET receipt_json=receipt_json || ' ' WHERE claim_id=?", (claims[0],))
        return values

    stage, results = adapter().make_remote_receipt_stores(
        database=sample['database'], admission_gate=sample['task_gate'], journal=sample['journal'],
        server_identity=sample['server'], observe_run=lambda _: True, inspect_batch=inspect_batch,
        max_summary_receipts=100)
    summary = results.summary()
    assert summary['available'] is False
    assert calls[:2] == [32, 32]
    # A later successful inspection starts from scratch; earlier partial evidence
    # must not be exposed as a count or retained as an implicit success cache.
    if failure != 'changed_local_receipt':
        calls.clear()
        results.inspect_receipt._inspect_batch = lambda requests: [sample['attestation'](request) for request in requests]
        assert results.summary()['counts']['passed'] == 65
    else:
        assert stage.summary()['waiting_rag'] is None


@pytest.mark.parametrize('changed', ['result_state', 'claim_state', 'task_metadata'])
def test_remote_summary_rejects_local_snapshot_changes_during_inspection(sample, monkeypatch, changed):
    claims = _synthetic_passed_receipts(sample, monkeypatch, 65)
    calls = []

    def inspect_batch(requests):
        assert not sample['gate_held']()
        calls.append(len(requests))
        if len(calls) == 2:
            with sqlite3.connect(sample['database'], timeout=.2) as db:
                if changed == 'result_state':
                    db.execute("UPDATE postprocessing_results SET state='unresolved',code='processing' WHERE claim_id=?", (claims[0],))
                elif changed == 'claim_state':
                    db.execute("UPDATE postprocessing_claims SET state='unknown' WHERE claim_id=?", (claims[0],))
                else:
                    db.execute("UPDATE tasks SET name='changed' WHERE task_id=?",
                               (requests[0]['job']['identity']['task_id'],))
        return [sample['attestation'](request) for request in requests]

    stage, results = adapter().make_remote_receipt_stores(
        database=sample['database'], admission_gate=sample['task_gate'], journal=sample['journal'],
        server_identity=sample['server'], observe_run=lambda _: True, inspect_batch=inspect_batch,
        max_summary_receipts=100)
    if changed == 'claim_state':
        assert stage.summary()['waiting_rag'] is None
    else:
        assert results.summary()['available'] is False
    assert calls[:2] == [32, 32]


def test_stage_summary_rejects_changes_during_original_summary(sample, monkeypatch):
    _synthetic_passed_receipts(sample, monkeypatch, 1)
    seen = []

    def observe_run(identity):
        if not seen:
            with sqlite3.connect(sample['database'], timeout=.2) as db:
                db.execute("UPDATE tasks SET name='changed' WHERE task_id=?", (identity['task_id'],))
            seen.append(True)
        return True

    stage, _ = adapter().make_remote_receipt_stores(
        database=sample['database'], admission_gate=sample['task_gate'], journal=sample['journal'],
        server_identity=sample['server'], observe_run=observe_run,
        inspect_batch=lambda requests: [sample['attestation'](request) for request in requests],
        max_summary_receipts=100)
    assert stage.summary()['waiting_rag'] is None
    assert seen == [True]


@pytest.mark.parametrize('changed_at', ['remote_batch', 'original_summary'])
def test_stage_summary_rejects_changed_local_journal_binding(sample, monkeypatch, changed_at):
    _synthetic_passed_receipts(sample, monkeypatch, 1)
    original_get = sample['journal'].get
    changed = []
    monkeypatch.setattr(sample['journal'], 'get', lambda job_id: None if changed else original_get(job_id))

    def inspect_batch(requests):
        if changed_at == 'remote_batch':
            changed.append(True)
        return [sample['attestation'](request) for request in requests]

    def observe_run(_):
        if changed_at == 'original_summary':
            changed.append(True)
        return True

    stage, _ = adapter().make_remote_receipt_stores(
        database=sample['database'], admission_gate=sample['task_gate'], journal=sample['journal'],
        server_identity=sample['server'], observe_run=observe_run, inspect_batch=inspect_batch,
        max_summary_receipts=100)
    assert stage.summary()['waiting_rag'] is None
    assert changed


def test_empty_summary_does_not_claim_zero_if_journal_disappears(sample):
    stage, results = adapter().make_remote_receipt_stores(
        database=sample['database'], admission_gate=sample['task_gate'], journal=sample['journal'],
        server_identity=sample['server'], observe_run=lambda _: True,
        inspect_batch=lambda requests: pytest.fail('there are no passed receipts'),
        max_summary_receipts=100)
    sample['journal'].database.rename(sample['journal'].database.with_suffix('.removed'))
    assert results.summary()['available'] is False
    assert stage.summary()['waiting_rag'] is None


def test_stage_summary_rechecks_control_after_journal_recheck(sample, monkeypatch):
    _synthetic_passed_receipts(sample, monkeypatch, 1)
    get = sample['journal'].get
    reads = []

    def journal_get(job_id):
        reads.append(job_id)
        result = get(job_id)
        if len(reads) == 3:
            with sqlite3.connect(sample['database'], timeout=.2) as db:
                db.execute("UPDATE tasks SET name='changed' WHERE task_id=?",
                           (result['job']['identity']['task_id'],))
        return result

    monkeypatch.setattr(sample['journal'], 'get', journal_get)
    stage, _ = adapter().make_remote_receipt_stores(
        database=sample['database'], admission_gate=sample['task_gate'], journal=sample['journal'],
        server_identity=sample['server'], observe_run=lambda _: True,
        inspect_batch=lambda requests: [sample['attestation'](request) for request in requests],
        max_summary_receipts=100)
    assert stage.summary()['waiting_rag'] is None
    assert len(reads) == 3


def test_remote_batch_size_can_be_lowered_but_not_exceed_server_limit(sample, monkeypatch):
    _synthetic_passed_receipts(sample, monkeypatch, 33)
    calls = []
    def inspect_batch(requests):
        calls.append(len(requests))
        return [sample['attestation'](request) for request in requests]
    options = dict(database=sample['database'], admission_gate=sample['task_gate'], journal=sample['journal'],
        server_identity=sample['server'], observe_run=lambda _: True, inspect_batch=inspect_batch,
        max_summary_receipts=100)
    stage, results = adapter().make_remote_receipt_stores(**options, inspection_batch_size=16)
    assert results.summary()['counts']['passed'] == 33
    assert calls == [16, 16, 1]
    with pytest.raises(ValueError):
        adapter().make_remote_receipt_stores(**options, inspection_batch_size=33)
