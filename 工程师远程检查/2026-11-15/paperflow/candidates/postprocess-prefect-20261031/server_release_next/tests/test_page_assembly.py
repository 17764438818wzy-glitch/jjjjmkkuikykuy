"""Full page assembly using temporary databases and an in-process fake server."""
from contextlib import nullcontext
import hashlib
import importlib
import importlib.util
import json
from pathlib import Path
import shlex
import sys
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parent
sys.path.insert(0, str(HERE))


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fixtures = load('synthetic_prior_scope_fixtures', ROOT / 'remote_projection_next' / 'tests' / 'test_scope_verifier.py')
modules = fixtures.modules
sample = fixtures.sample
scope = fixtures.scope


def test_release_page_assembles_without_dispatch_and_detects_configuration_drift(scope, monkeypatch):
    from bounded_summary import BoundedSummary
    from remote_coordinator import RemoteSubmissionJournal
    from remote_jobs import RemoteJobs
    from remote_live_launch import LoadedRemoteRuntime
    from remote_live_runtime import RemoteLiveRuntime
    from remote_prefect_entry import RemoteCoordinatorFactory
    from remote_runtime_adapter import make_remote_receipt_stores
    from remote_server_transport import RemoteServerTransport
    from release_guard import ReleaseGuard, file_hashes, inventory
    from stage_runtime import Coordinator
    import server_rpc

    sample, _, _ = scope
    fixtures.populate_real_frozen_bindings(sample, 1)
    sample.queue.snap['upstream_scan_completed_at'] = '2026-11-01T00:00:00Z'
    gate = lambda _: nullcontext()
    noop = lambda *args, **kwargs: None
    upstream, prefect_client = sample.projector.binding_factory, lambda: sample.client
    def make_verify_upstream(upstream, client):
        def verify(*_): return upstream(client())
        return verify
    hooks = SimpleNamespace(reserve=noop, success=noop, failure=noop, unknown=noop)
    base = Coordinator(sample.control, task_gate=gate, folder_gate=noop, postprocess_is_open=noop,
        rag_is_held=noop, verify_upstream=make_verify_upstream(upstream, prefect_client),
        verify_execution=noop, slot=noop, process_folder=noop, results=hooks)
    journal = RemoteSubmissionJournal(sample.tmp_path / 'page-journal.sqlite3',
                                      control_database=sample.control, admission_gate=gate)
    journal.initialize()
    server = {'server_id': 'synthetic-server', 'ledger_id': str(uuid4())}
    remote_ledger = RemoteJobs(sample.tmp_path / 'page-server.sqlite3', capacity=1, ledger_id=server['ledger_id'])
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

    fixed = sample.tmp_path / 'synthetic-release-input.json'
    fixed.write_text('{"approved":true}', encoding='utf8')
    roots = [(HERE, ('.py',), False), (ROOT / 'projection_next', ('.py',), False),
             (ROOT / 'ledger_projection_next', ('.py',), False)]
    manifest = {'schema': 'paperflow-page-release/1', 'files': file_hashes(inventory(roots, [fixed])),
                'python': None, 'packages': {}}
    encoded = json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode('utf8')
    manifest_path = sample.tmp_path / 'page-manifest.json'
    manifest_path.write_bytes(encoded)
    guard = ReleaseGuard(manifest_path, hashlib.sha256(encoded).hexdigest(), roots=roots, required_files=[fixed])
    state = {'approved': True}
    def check_configuration():
        if not state['approved']: raise ValueError('synthetic remote config changed')
    loaded = LoadedRemoteRuntime(original, remote, None, None, original['api'], False, check_configuration)
    assembly = load('synthetic_server_release_page_assembly', HERE / 'page_assembly.py')
    app, registered = assembly.build_page(loaded_runtime=loaded, release_guard=guard,
                                           queue=sample.queue, observer=sample.observer)
    assert calls == []
    with TestClient(BoundedSummary(app)) as client:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            response = client.get('/api/v1/local-intake/preprocessing/summary')
            if response.status_code == 200:
                value = response.json()['postprocessing_registered_ledger']
                if value.get('server_observation', {}).get('available') is True: break
            time.sleep(.03)
        else:
            pytest.fail('assembled page did not observe the synthetic source and server')
        assert value['stage']['counts']['waiting_postprocessing'] == 1
        assert value['stage']['counts']['waiting_rag_upload'] == 0
    assert calls
    calls.clear()
    for name in ('_release_page_existing_projections', '_release_page_scope_verifier',
                 '_release_page_registered_ledger', '_release_page_server_projection', '_release_page_waiting_api',
                 '_release_page_revision_witness'):
        sys.modules.pop(name, None)  # The first synthetic application is closed.
    partial_app, partial_projection = assembly.build_page(loaded_runtime=loaded, release_guard=guard,
        queue=sample.queue, observer=sample.observer, partial_observation=True)
    partial_value = partial_projection()
    assert partial_value['available'] is True
    assert partial_value['server_observation']['available'] is False
    assert partial_value['stage']['counts']['waiting_rag_upload'] is None
    assert calls == [], 'an empty registered subset must not dispatch or assume a server result'
    assert partial_app.state.postprocessing_observation_revision()[0] == partial_app.state.postprocessing_revision.current()
    with TestClient(BoundedSummary(partial_app,
            revision_witness=partial_app.state.postprocessing_observation_revision)) as client:
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            response = client.get('/api/v1/local-intake/preprocessing/summary')
            if (response.status_code == 200 and
                    response.json()['postprocessing_registered_ledger'].get('available') is True): break
            time.sleep(.03)
        assert response.status_code == 200 and response.json()['postprocessing_registered_ledger']['available'] is True
        assert response.json()['postprocessing_registered_ledger']['results']['counts']['passed'] is None
    assert partial_projection()['available'] is False, 'ASGI shutdown must stop candidate observations'
    fixed.write_text('{"changed":true}', encoding='utf8')
    assert registered()['server_observation']['available'] is False
    assert partial_projection()['available'] is False
    fixed.write_text('{"approved":true}', encoding='utf8')
    state['approved'] = False
    assert registered()['server_observation']['available'] is False
