"""Synthetic source, Prefect and frozen-handoff checks; never schedules work."""
import importlib.util
import importlib
import copy
import json
import hashlib
from pathlib import Path
import sqlite3
import sys
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[2]
for path in (ROOT.parent / 'postprocess-server-20261101', ROOT, ROOT / 'runtime_next',
             ROOT / 'results_next', ROOT.parents[1] / 'prefect-paperflow'):
    sys.path.insert(0, str(path))


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


previous = load('scope_existing_projection_tests', ROOT / 'projection_next' / 'tests' / 'test_projections.py')
modules = previous.modules
sample = previous.sample


@pytest.fixture
def scope(sample):
    # The existing fixture models the Prefect records but did not need IDs on
    # states before source observations started comparing state transitions.
    for run in sample.client.flows.values():
        run.state.id = uuid4()
    source = load('synthetic_scope_verifier', ROOT / 'remote_projection_next' / 'scope_verifier.py')
    server = {'server_id': 'synthetic-server', 'ledger_id': str(uuid4())}
    verifier = source.ScopeVerifier(sample.projector, server_identity=server, check_original_files=lambda: None)
    request = dict(schema='paperflow-server-projection-scope/1', database=str(sample.control.resolve()),
        ledger_revision='a' * 64, server_identity=server,
        folders=[dict(identity=dict(sample.item), manifest_sha256=sample.digest, generation=1)])
    return sample, verifier, request


def test_scope_rechecks_prefect_folder_and_exact_original_identities(scope):
    sample, verifier, request = scope
    assert verifier.verify(request) is True
    request['folders'][0]['generation'] = 2
    assert verifier(request) is False


def test_parent_ticket_must_use_the_original_root_deployment(scope):
    sample, verifier, request = scope
    sample.client.flows[sample.item['parent_run_id']].deployment_id = sample.client.folder_deployment.id
    assert verifier(request) is False


def test_registered_postprocess_job_must_match_its_actual_prefect_stage(scope):
    sample, verifier, request = scope
    job = previous.insert_job(sample, 'finished', task_state='COMPLETED')
    sample.client.stages[job].state.id = uuid4()
    assert verifier.verify(request) is True
    sample.client.stages[job].dynamic_key = 'unrelated-stage'
    assert verifier(request) is False


@pytest.mark.parametrize('field', ['root_tags', 'folder_tags', 'folder_work_pool'])
def test_deployment_identity_changes_during_observation_are_unknown(scope, field):
    sample, verifier, request = scope
    original = sample.client.read_flow_runs
    calls = []
    def read(*args, **kwargs):
        calls.append(True)
        if len(calls) == 2:
            if field == 'root_tags':
                sample.client.root.tags.append('synthetic-changed')
            elif field == 'folder_tags':
                sample.client.folder_deployment.tags.append('synthetic-changed')
            else:
                sample.client.folder_deployment.work_pool_name = 'different-pool'
        return original(*args, **kwargs)
    sample.client.read_flow_runs = read
    assert verifier(request) is False


@pytest.mark.parametrize('state', ['FAILED', 'CRASHED', 'RUNNING'])
def test_terminal_or_running_folder_is_never_admitted_as_waiting(scope, state):
    sample, verifier, request = scope
    sample.client.flows[sample.item['task_run_id']].state.kind = state
    assert verifier(request) is False


def test_source_deletion_and_prefect_change_are_unknown(scope):
    sample, verifier, request = scope
    with sqlite3.connect(sample.control) as conn:
        conn.execute('INSERT INTO task_deletions VALUES(?)', (sample.item['task_id'],))
    assert verifier(request) is False


def test_manual_retry_is_not_promoted_to_an_executable_postprocess_folder(scope):
    sample, verifier, request = scope
    with sqlite3.connect(sample.control) as conn:
        conn.execute('CREATE TABLE paperflow_llm_stage_jobs(job_id TEXT,task_id TEXT,group_id TEXT,'
                     'retry_key TEXT,state TEXT,generation INTEGER,created_at_ns INTEGER)')
        conn.execute('INSERT INTO paperflow_llm_stage_jobs VALUES(?,?,?,?,?,?,?)',
                     (str(uuid4()), sample.item['task_id'], sample.item['group_id'], str(uuid4()), 'finished', 1, 1))
    assert verifier(request) is False


def test_source_handoff_changed_after_registration_is_unknown(scope):
    sample, verifier, request = scope
    sample.path.write_text('{"changed":true}', encoding='utf8')
    assert verifier(request) is False


def populate_real_frozen_bindings(sample, count):
    """Use the actual UpstreamFlowBinding on small, synthetic source files."""
    def ref(path, value):
        raw = json.dumps(value, ensure_ascii=False).encode('utf8')
        path.write_bytes(raw)
        return dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest())

    body = dict(file_id='synthetic-body', sha256='f' * 64)
    receipt = ref(sample.tmp_path / 'synthetic-receipt.json', {'synthetic': True})
    material = ref(sample.tmp_path / 'synthetic-material.json', {'source': {'source_sha256': body['sha256']}})
    reading = ref(sample.tmp_path / 'synthetic-reading.json', {'source': {'source_sha256': body['sha256']}})
    verified, entries, folders = {}, [], []
    template = sample.client.flows[sample.item['task_run_id']]
    with sqlite3.connect(sample.control) as control, sqlite3.connect(sample.queue.path) as queue:
        queue.execute('INSERT INTO metadata VALUES(?,?)', ('queue_schema', 'paperflow-local-waiting/1'))
        control.execute('CREATE TABLE llm_request_admissions(task_id TEXT,component_id TEXT,generation INTEGER,'
            'request_sha256 TEXT,state TEXT,response_sha256 TEXT,kind TEXT,receipt_id TEXT,task_run_id TEXT,'
            'source_sha256 TEXT)')
        for n in range(count):
            item = dict(sample.item)
            if n:
                item.update(group_id=f'{n:032x}', paper_id=f'synthetic-paper-{n}', receipt_id=f'{n:064x}',
                            attempt_id=f'synthetic-attempt-{n}', task_run_id=str(uuid4()))
                folder = copy.copy(template)
                folder.id = previous.UUID(item['task_run_id'])
                folder.state = previous.State('PAUSED')
                folder.state.id = uuid4()
                folder.idempotency_key = 'folder-waiting-v2-' + item['task_id'] + '-' + item['group_id']
                folder.parameters = {key: item[key] for key in ('task_id', 'group_id', 'receipt_id', 'attempt_id')}
                sample.client.flows[item['task_run_id']] = folder
            paper = dict(paper_id=item['paper_id'], revision='synthetic-revision', body=body, attachments=[])
            manifest = dict(schema='paperflow-local-preprocessing-handoff/1', consumer_id='prefect-local-preprocessing',
                **{key: item[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
                revision=paper['revision'], body=body, attachments=[], receipt=receipt, material=material,
                reading_material=reading, body_work=str(sample.tmp_path), body_text=None, index=None, gaps=[])
            path = sample.path if not n else sample.tmp_path / f'synthetic-manifest-{n}.json'
            raw = json.dumps(manifest, ensure_ascii=False).encode('utf8')
            path.write_bytes(raw)
            reference = dict(manifest_path=str(path), manifest_sha256=hashlib.sha256(raw).hexdigest())
            digest = hashlib.sha256(json.dumps(manifest, ensure_ascii=False, sort_keys=True,
                                    separators=(',', ':'), allow_nan=False).encode('utf8')).hexdigest()
            if not n:
                control.execute('UPDATE llm_folder_queue SET manifest_sha256=?', (digest,))
                sample.digest = digest
            else:
                control.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?)',
                                (*item.values(), digest, 'waiting_postprocessing', 1))
                queue.execute('INSERT INTO preprocessing_folder_orders VALUES(?,?,?,?,?)',
                    (item['task_id'], item['group_id'], item['receipt_id'], item['attempt_id'], item['task_run_id']))
            queue.execute('INSERT INTO groups VALUES(?,?,?,?,?,?)', (item['task_id'], item['group_id'],
                item['paper_id'], item['receipt_id'], 'ready_for_llm', 'receiver-test'))
            queue.execute('INSERT INTO claims VALUES(?,?,?,?,?,?)', ('prefect-local-preprocessing', item['attempt_id'],
                item['task_id'], item['group_id'], item['receipt_id'], 'completed'))
            queue.execute('INSERT INTO preprocessing_handoffs VALUES(?,?,?,?,?,?,?,?)',
                (item['task_id'], item['group_id'], item['paper_id'], item['receipt_id'],
                 'prefect-local-preprocessing', item['attempt_id'], 'waiting', json.dumps(reference)))
            queue.execute('INSERT INTO retention_requests VALUES(?,?,?,?,?,?,?,?,?,?)',
                (item['task_id'], item['group_id'], item['paper_id'], item['receipt_id'],
                 'local-intake', 'accepted', 'retained', None, 'receiver-test', 'early'))
            control.execute('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?,?,?,?,?,?)',
                (item['task_id'], f'component-{n}', 1, 'a' * 64, 'raw_complete', 'b' * 64,
                 'body', item['receipt_id'], item['task_run_id'], body['sha256']))
            verified[item['receipt_id']] = {**item, 'receipt_path': receipt['path'],
                                          'receipt_sha256': receipt['sha256'], 'paper': paper}
            entries.append({**{key: item[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id')},
                            **reference})
            folders.append(dict(identity=item, manifest_sha256=digest, generation=1))
    sample.queue.snap['waiting_llm_total'] = count
    sample.queue.list_waiting_llm = lambda: dict(waiting_count=count, unavailable_count=0, items=entries)
    package = ROOT.parents[1] / 'prefect-paperflow' / 'paperflow_prefect'
    sample.modules.projection._load_module('_synthetic_scope_upstream', package / '__init__.py', package_dir=package)
    upstream = importlib.import_module('_synthetic_scope_upstream.llm_queue.upstream_flow_binding')
    sample.projector.binding_factory = lambda client: upstream.UpstreamFlowBinding(
        sample.queue.path, sample.control, client, sample.observer.api_url, sample.queue.consumer_id,
        lambda _path, receipt_id: verified[receipt_id], receiver_session=lambda _: 'receiver-test')
    return sorted(folders, key=lambda value: (value['identity']['task_id'], value['identity']['group_id'])), reading


def test_actual_frozen_binding_rejects_changed_material(scope):
    sample, verifier, request = scope
    request['folders'], reading = populate_real_frozen_bindings(sample, 1)
    assert verifier.verify(request) is True
    Path(reading['path']).write_text('{}', encoding='utf8')
    assert verifier(request) is False


def test_268_synthetic_registered_folders_end_to_end(scope):
    sample, verifier, request = scope
    request['folders'], _ = populate_real_frozen_bindings(sample, 268)
    fixtures = load('scope_benchmark_server_fixtures', ROOT / 'remote_projection_next' / 'tests' /
                    'test_server_projection.py')
    db = SimpleNamespace(path=sample.control)
    inspector, _ = fixtures.make_inspector(db, max_rows=300)
    verifier.server_identity = copy.deepcopy(inspector.server)
    started = time.perf_counter()
    value = fixtures.candidate().snapshot(sample.control, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, verify_scope=verifier, probe_server=lambda _: True)
    elapsed = time.perf_counter() - started
    print(f'268 synthetic waiting folders; two actual frozen-source observations; elapsed={elapsed:.3f}s')
    assert value['server_observation']['available'] is True, value['server_observation'].get('reason')
    assert value['stage']['counts']['waiting_postprocessing'] == 268
    assert value['stage']['counts']['waiting_rag_upload'] == 0
    assert value['stage']['counts']['postprocessing_in_progress'] is None


def test_original_prefect_stage_and_complete_server_receipt_are_both_required(scope):
    sample, verifier, request = scope
    request['folders'], _ = populate_real_frozen_bindings(sample, 1)
    fixtures = load('scope_pass_server_fixtures', ROOT / 'remote_projection_next' / 'tests' /
                    'test_server_projection.py')
    identity, manifest_sha = sample.item, request['folders'][0]['manifest_sha256']
    job = previous.insert_job(sample, 'finished', task_state='COMPLETED')
    sample.client.stages[job].state.id = uuid4()
    claim_id = str(uuid4())
    body_dir = sample.tmp_path / 'synthetic-remote-body'
    package = body_dir / 'publication'
    artifacts = [dict(path=str(package / name), sha256=f'{index:x}' * 64)
                 for index, name in enumerate(('article.redacted.md', 'profile.json', 'tree.json', 'assets.json',
                    'chunks.json', 'split-plan.json', 'source-map.json', 'manifest.json', 'READY.json'), 1)]
    artifacts.append(dict(path=str(body_dir / 'body-result.json'), sha256='a' * 64))
    receipt = dict(schema='paperflow-postprocessing-receipt/1', identity=identity, claim_id=claim_id,
        manifest_sha256=manifest_sha, validation_status='passed',
        body=dict(package_path=str(package), manifest_sha256='b' * 64), attachments=[], artifacts=artifacts)
    manifest, ready = fixtures.synthetic_seals(receipt)
    manifest_digest, ready_digest = (hashlib.sha256(raw.encode('utf8')).hexdigest() for raw in (manifest, ready))
    receipt['body']['manifest_sha256'] = manifest_digest
    for artifact in artifacts:
        name = Path(artifact['path']).name.casefold()
        if name == 'manifest.json': artifact['sha256'] = manifest_digest
        if name == 'ready.json': artifact['sha256'] = ready_digest
    raw = json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    receipt_digest = hashlib.sha256(raw.encode('utf8')).hexdigest()
    values = tuple(identity[key] for key in fixtures.helpers.FIELDS)
    with sqlite3.connect(sample.control) as conn:
        conn.execute('INSERT INTO postprocessing_claims VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                     (*values, manifest_sha, claim_id, 1, 'waiting_rag', None, 1, 1))
        conn.execute('INSERT INTO postprocessing_results VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                     (*values, manifest_sha, 1, str(uuid4()), 'passed', 'passed', claim_id, receipt_digest,
                      None, None, 1, 1))
        conn.execute('INSERT INTO postprocessing_receipts VALUES(?,?,?,?)', (claim_id, raw, receipt_digest, 1))
    inspector, calls = fixtures.make_inspector(SimpleNamespace(path=sample.control), max_rows=300)
    verifier.server_identity = copy.deepcopy(inspector.server)
    kwargs = dict(ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, verify_scope=verifier)
    value = fixtures.candidate().snapshot(sample.control, **kwargs)
    assert value['server_observation']['available'] is True, value['server_observation'].get('reason')
    assert value['stage']['counts']['waiting_rag_upload'] == 1 and len(calls) == 1
    sample.client.stages[job].dynamic_key = 'wrong-stage'
    value = fixtures.candidate().snapshot(sample.control, **kwargs)
    assert value['server_observation']['available'] is False
    assert value['stage']['counts']['waiting_rag_upload'] is None


def test_full_candidate_page_assembly_uses_bound_probe_and_original_source(scope, monkeypatch):
    from contextlib import nullcontext
    import shlex
    from fastapi.testclient import TestClient
    from remote_coordinator import RemoteSubmissionJournal
    from remote_jobs import RemoteJobs
    from remote_live_runtime import RemoteLiveRuntime
    from remote_prefect_entry import RemoteCoordinatorFactory
    from remote_runtime_adapter import make_remote_receipt_stores
    from remote_server_transport import RemoteServerTransport
    from stage_runtime import Coordinator
    import server_rpc

    sample, _, _ = scope
    populate_real_frozen_bindings(sample, 1)
    gate = lambda _: nullcontext()
    noop = lambda *args, **kwargs: None
    upstream = sample.projector.binding_factory
    prefect_client = lambda: sample.client
    def make_verify_upstream(upstream, client):
        def verify(*_):
            return upstream(client())
        return verify
    hooks = SimpleNamespace(reserve=noop, success=noop, failure=noop, unknown=noop)
    base = Coordinator(sample.control, task_gate=gate, folder_gate=noop, postprocess_is_open=noop,
                       rag_is_held=noop, verify_upstream=make_verify_upstream(upstream, prefect_client), verify_execution=noop,
                       slot=noop, process_folder=noop, results=hooks)
    journal = RemoteSubmissionJournal(sample.tmp_path / 'assembly-journal.sqlite3',
                                      control_database=sample.control, admission_gate=gate)
    journal.initialize()
    server = {'server_id': 'synthetic-server', 'ledger_id': str(uuid4())}
    remote_ledger = RemoteJobs(sample.tmp_path / 'assembly-server.sqlite3', capacity=1, ledger_id=server['ledger_id'])
    remote_ledger.initialize()
    monkeypatch.setattr(server_rpc.service, '_ledger', lambda _: remote_ledger)
    calls = []
    def rpc(_connection, command, payload, **_):
        assert shlex.split(command)[-1] == 'probe'
        calls.append(payload)
        return server_rpc.execute('probe', payload, {'server_identity': server})
    config = dict(schema='paperflow-server-transport/1', server_identity=server,
        connection=dict(host='synthetic.invalid', port=22, username='synthetic',
            private_key=str(sample.tmp_path / 'never-read-key'), server_key_sha256='1' * 64),
        server_python='/synthetic/python', staging_script='/synthetic/stage.py',
        rpc_script='/synthetic/rpc.py', service_config='/synthetic/service.json',
        service_config_sha256='2' * 64, staging_root='/synthetic/staging', allowed_roots=[str(sample.tmp_path)],
        staging_timeout_seconds=60, metadata_timeout_seconds=5, admit_timeout_seconds=2,
        wait_timeout_seconds=20, poll_seconds=.1)
    raw = json.dumps(config, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
    transport = RemoteServerTransport(config, config_sha256=hashlib.sha256(raw.encode()).hexdigest(),
                                      journal=journal, rpc_call=rpc)
    stage, results = make_remote_receipt_stores(database=sample.control, admission_gate=gate, journal=journal,
        server_identity=server, observe_run=lambda _: True, inspect_batch=transport.inspect_batch,
        max_summary_receipts=300)
    factory = RemoteCoordinatorFactory(server_identity=server, control_database=sample.control,
        journal_database=journal.database, journal_id=journal.journal_id, original_base=base,
        components_factory=lambda: {}, transport=transport.coordinator_callbacks())
    remote = RemoteLiveRuntime(None, factory, stage, results, journal, transport)
    original = dict(coordinator=base, database=sample.control, api=sample.observer.api_url,
                    client=prefect_client, upstream_factory=upstream)
    sample.observer.summary = lambda _: dict(source='prefect', scope='all_tasks', available=True, counts={})
    sample.observer.preprocessing_results = lambda *_: {'available': True}
    sample.observer.llm_results = lambda *_: {'available': True}
    assembly = load('synthetic_page_assembly', ROOT / 'remote_projection_next' / 'page_assembly.py')
    app, registered = assembly.build_page(original_runtime=original, remote_runtime=remote,
                                           queue=sample.queue, observer=sample.observer)
    assert calls == []  # Constructing the app does not contact the server.
    with TestClient(app) as client:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            response = client.get('/api/v1/local-intake/preprocessing/summary').json()
            value = response['postprocessing_registered_ledger']
            if value.get('server_observation', {}).get('available') is True:
                break
            time.sleep(.01)
        else:
            pytest.fail('assembled candidate page did not observe synthetic source and server')
    assert value['stage']['counts']['waiting_postprocessing'] == 1
    assert value['stage']['counts']['waiting_rag_upload'] == 0
    assert len(calls) == 1
    sample.client.flows[sample.item['task_run_id']].state.kind = 'FAILED'
    changed = registered()
    assert changed['server_observation']['available'] is False
    assert changed['stage']['counts']['waiting_postprocessing'] is None
    base.task_gate = lambda _: nullcontext()
    with pytest.raises(ValueError):
        assembly.build_page(original_runtime=original, remote_runtime=remote,
                            queue=sample.queue, observer=sample.observer)
