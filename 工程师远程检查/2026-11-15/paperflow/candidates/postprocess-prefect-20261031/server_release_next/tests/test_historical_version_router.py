"""Synthetic RPC routing; no real server, SSH, paper, or job."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import sqlite3
import subprocess
import sys
import time

import pytest

HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parent


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fixtures = load('synthetic_history_fixtures', ROOT / 'remote_projection_next' / 'tests' / 'test_server_projection.py')


@pytest.fixture
def sample(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    first, second = fixtures.add_complete_pass(db, 1), fixtures.add_complete_pass(db, 2)
    inspector, _ = fixtures.make_inspector(db)
    requests = []
    with sqlite3.connect(db.path) as conn:
        for identity in (first, second):
            raw, claim = conn.execute('SELECT p.receipt_json,p.claim_id FROM postprocessing_receipts p '
                'JOIN postprocessing_claims c ON c.claim_id=p.claim_id WHERE c.group_id=?', (identity['group_id'],)).fetchone()
            requests.append(inspector._request(json.loads(raw), identity, claim))
    config = {'connection': {'host': 'synthetic'}, 'server_python': '/python', 'rpc_script': '/rpc.py',
              'service_config': '/versions/current.json', 'service_config_sha256': 'c' * 64}
    versions = [dict(canonical_sha256='a' * 64, service_config='/versions/old.json', service_config_sha256='1' * 64,
                     server_python='/old/python', rpc_script='/old/app/server_rpc.py', rpc_script_sha256='3' * 64,
                     access_ref='base'),
                dict(canonical_sha256='b' * 64, service_config='/versions/new.json', service_config_sha256='2' * 64,
                     server_python='/new/python', rpc_script='/new/app/server_rpc.py', rpc_script_sha256='4' * 64,
                     access_ref='base')]
    registry = dict(schema='paperflow-historical-service-versions/1', server_identity=inspector.server, versions=versions)
    path = tmp_path / 'registry.json'
    data = json.dumps(registry, sort_keys=True, separators=(',', ':')).encode()
    path.write_bytes(data)
    return db, inspector, requests, config, path, hashlib.sha256(data).hexdigest(), versions


def router(module, sample):
    assert hasattr(module, 'HistoricalVersionRouter'), 'history routing is not implemented'
    _, inspector, _, _, path, sha, _ = sample
    return module.HistoricalVersionRouter(path, sha, server_identity=inspector.server,
        remote_script='/opt/synthetic/server_version_query.py', remote_script_sha256='d' * 64)


def test_each_registered_receipt_uses_its_server_recorded_config(sample):
    db, inspector, requests, config, _, _, versions = sample
    module = load('synthetic_version_router', HERE / 'read_only_rpc.py')
    routing = router(module, sample)
    calls = []
    def delegate(connection, command, payload, **kwargs):
        calls.append((shlex.split(command), copy.deepcopy(payload), kwargs))
        if 'nonce' in payload:
            index = next(i for i, request in enumerate(requests) if request['job'] == payload['job'])
            return dict(schema='paperflow-server-job-config/1', server_identity=inspector.server,
                job_id=payload['job']['job_id'], receipt_sha256=payload['receipt_sha256'], nonce=payload['nonce'],
                service_config_canonical_sha256=versions[index]['canonical_sha256'])
        return [{'synthetic': True}]
    rpc = module.ReadOnlyRPC(delegate)
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    result = rpc(config['connection'], module.command_for(config, 'inspect'), {'requests': requests}, timeout=5, max_bytes=100000)
    assert result == [{'synthetic': True}, {'synthetic': True}]
    inspections = [call for call in calls if 'requests' in call[1]]
    assert [row[0][-3:] for row in inspections] == [
        [versions[0]['service_config'], versions[0]['service_config_sha256'], 'inspect'],
        [versions[1]['service_config'], versions[1]['service_config_sha256'], 'inspect']]
    assert [(row[0][0], row[0][4], row[0][5]) for row in inspections] == [
        (versions[0]['server_python'], versions[0]['rpc_script'], versions[0]['rpc_script_sha256']),
        (versions[1]['server_python'], versions[1]['rpc_script'], versions[1]['rpc_script_sha256'])]
    lookups = [call for call in calls if 'nonce' in call[1]]
    assert all(row[0][-3:] == ['/', config['service_config'], config['service_config_sha256']]
               for row in lookups), 'lookup must import the reviewed base RPC directory'
    assert all(row[2]['timeout'] <= 5 for row in calls)


@pytest.mark.parametrize('change', ['unknown_version', 'nonce', 'receipt', 'server', 'job', 'registry'])
def test_no_inspection_occurs_when_the_version_or_binding_is_unavailable(sample, change):
    db, inspector, requests, config, path, _, versions = sample
    module = load('synthetic_bad_version_' + change, HERE / 'read_only_rpc.py')
    routing = router(module, sample)
    calls = []
    def delegate(_connection, _command, payload, **_kwargs):
        calls.append(copy.deepcopy(payload))
        assert 'nonce' in payload, 'a failed version query must not inspect a guessed configuration'
        result = dict(schema='paperflow-server-job-config/1', server_identity=copy.deepcopy(inspector.server),
            job_id=payload['job']['job_id'], receipt_sha256=payload['receipt_sha256'], nonce=payload['nonce'],
            service_config_canonical_sha256=versions[0]['canonical_sha256'])
        if change == 'unknown_version': result['service_config_canonical_sha256'] = 'f' * 64
        elif change == 'nonce': result['nonce'] = 'different'
        elif change == 'receipt': result['receipt_sha256'] = 'f' * 64
        elif change == 'server': result['server_identity']['server_id'] = 'different'
        elif change == 'job': result['job_id'] = 'different'
        return result
    rpc = module.ReadOnlyRPC(delegate)
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    if change == 'registry': path.write_text('{}', encoding='utf8')
    with pytest.raises(ValueError):
        rpc(config['connection'], module.command_for(config, 'inspect'), {'requests': [requests[0]]}, timeout=5)
    assert len(calls) <= 1


def test_unregistered_item_is_rejected_before_querying_the_server(sample):
    db, inspector, requests, config, _, _, _ = sample
    module = load('synthetic_unregistered_version', HERE / 'read_only_rpc.py')
    routing = router(module, sample)
    rpc = module.ReadOnlyRPC(lambda *a, **kw: pytest.fail('must not query server'))
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    with sqlite3.connect(db.path) as conn:
        conn.execute("UPDATE postprocessing_results SET state='unresolved' WHERE group_id=?",
                     (requests[0]['job']['identity']['group_id'],))
    with pytest.raises(ValueError):
        rpc(config['connection'], module.command_for(config, 'inspect'), {'requests': [requests[0]]}, timeout=5)


def test_deletion_during_remote_inspection_invalidates_the_whole_result(sample):
    db, inspector, requests, config, _, _, versions = sample
    module = load('synthetic_version_deleted_during_query', HERE / 'read_only_rpc.py')
    routing = router(module, sample)
    def delegate(_connection, _command, payload, **_kwargs):
        if 'nonce' in payload:
            return dict(schema='paperflow-server-job-config/1', server_identity=copy.deepcopy(inspector.server),
                job_id=payload['job']['job_id'], receipt_sha256=payload['receipt_sha256'], nonce=payload['nonce'],
                service_config_canonical_sha256=versions[0]['canonical_sha256'])
        with sqlite3.connect(db.path) as conn:
            conn.execute('INSERT INTO task_deletions VALUES(?)', (requests[0]['job']['identity']['task_id'],))
        return [{'synthetic': True}]
    rpc = module.ReadOnlyRPC(delegate)
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    with pytest.raises(ValueError):
        rpc(config['connection'], module.command_for(config, 'inspect'), {'requests': [requests[0]]}, timeout=5)


def test_version_bootstrap_obeys_ssh_command_policy_and_executes_only_pinned_bytes(tmp_path):
    module = load('synthetic_version_bootstrap', HERE / 'read_only_rpc.py')
    assert '\n' not in module.VERSION_BOOTSTRAP and '\r' not in module.VERSION_BOOTSTRAP
    script = tmp_path / 'version helper.py'
    data = b'import json,sys; print(json.dumps(sys.argv[1:]))\n'
    script.write_bytes(data)
    digest = hashlib.sha256(data).hexdigest()
    args = [sys.executable, '-B', '-c', module.VERSION_BOOTSTRAP, str(script), digest, 'file.json', 'a' * 64]
    success = subprocess.run(args, capture_output=True, text=True, check=False, timeout=5)
    assert success.returncode == 0 and json.loads(success.stdout) == ['file.json', 'a' * 64]
    args[5] = 'f' * 64
    rejected = subprocess.run(args, capture_output=True, text=True, check=False, timeout=5)
    assert rejected.returncode != 0 and not rejected.stdout


def test_timeout_includes_the_initial_local_registration_check(sample):
    db, inspector, requests, config, _, _, _ = sample
    module = load('synthetic_version_timeout', HERE / 'read_only_rpc.py')
    routing = router(module, sample)
    rpc = module.ReadOnlyRPC(lambda *a, **kw: pytest.fail('expired local check must not call server'))
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    rpc._registered = lambda _: time.sleep(.05)
    with pytest.raises(ValueError, match='timed out'):
        rpc(config['connection'], module.command_for(config, 'inspect'), {'requests': [requests[0]]}, timeout=.01)


def test_timeout_during_final_recheck_cannot_return_success(sample):
    db, inspector, requests, config, _, _, versions = sample
    module = load('synthetic_version_final_timeout', HERE / 'read_only_rpc.py')
    routing = router(module, sample)
    def delegate(_connection, _command, payload, **_kwargs):
        if 'nonce' in payload:
            return dict(schema='paperflow-server-job-config/1', server_identity=copy.deepcopy(inspector.server),
                job_id=payload['job']['job_id'], receipt_sha256=payload['receipt_sha256'], nonce=payload['nonce'],
                service_config_canonical_sha256=versions[0]['canonical_sha256'])
        return [{'synthetic': True}]
    rpc = module.ReadOnlyRPC(delegate)
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    checks = []
    def slow_final(_):
        checks.append(True)
        if len(checks) == 3: time.sleep(.06)
    rpc._registered = slow_final
    with pytest.raises(ValueError, match='timed out'):
        rpc(config['connection'], module.command_for(config, 'inspect'), {'requests': [requests[0]]}, timeout=.04)


def test_version_query_budget_includes_a_large_receipt_and_ssh_envelope(sample):
    db, inspector, requests, config, _, _, versions = sample
    module = load('synthetic_large_version_request', HERE / 'read_only_rpc.py')
    routing = router(module, sample)
    request = copy.deepcopy(requests[0])
    request['receipt_json'] += 'x' * 9000
    def delegate(connection, command, payload, **kwargs):
        envelope = dict(connection=connection, command=command, payload=payload,
                        io_timeout=kwargs['timeout'], max_bytes=kwargs['max_bytes'])
        size = len(json.dumps(envelope, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf8'))
        assert size <= kwargs['max_bytes'], 'the real SSH helper bounds the entire request envelope'
        if 'nonce' in payload:
            return dict(schema='paperflow-server-job-config/1', server_identity=copy.deepcopy(inspector.server),
                job_id=payload['job']['job_id'], receipt_sha256=payload['receipt_sha256'], nonce=payload['nonce'],
                service_config_canonical_sha256=versions[0]['canonical_sha256'])
        return [{'synthetic': True}]
    rpc = module.ReadOnlyRPC(delegate)
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    rpc._registered = lambda _: None
    assert rpc(config['connection'], module.command_for(config, 'inspect'), {'requests': [request]},
               timeout=5, max_bytes=16 * 1024 * 1024) == [{'synthetic': True}]


def test_new_version_uses_its_own_pinned_access_reference(sample, tmp_path):
    db, inspector, requests, config, registry, _, versions = sample
    module = load('synthetic_version_access_bindings', HERE / 'read_only_rpc.py')
    assert hasattr(module, 'PinnedAccessBindings'), 'separate historical access bindings are missing'
    key = tmp_path / 'synthetic-key'
    key.write_bytes(b'synthetic key, never real credentials')
    connection = dict(host='synthetic', port=22, username='test', private_key=str(key), server_key_sha256='e' * 64)
    old_key = tmp_path / 'synthetic-old-key'
    old_key.write_bytes(b'old synthetic credential')
    base = dict(connection, private_key=str(old_key))
    config['connection'] = base
    transport = {'server_identity': inspector.server, 'connection': connection}
    transport_sha = hashlib.sha256(json.dumps(transport, ensure_ascii=False, sort_keys=True,
                                             separators=(',', ':')).encode()).hexdigest()
    runtime = dict(schema='paperflow-remote-live/1', server_identity=inspector.server,
                   transport_config=transport, transport_config_sha256=transport_sha)
    runtime_file = tmp_path / 'new-runtime.json'
    runtime_raw = json.dumps(runtime, sort_keys=True, separators=(',', ':')).encode()
    runtime_file.write_bytes(runtime_raw)
    base_transport = {'server_identity': inspector.server, 'connection': base}
    base_transport_sha = hashlib.sha256(json.dumps(base_transport, ensure_ascii=False, sort_keys=True,
                                                  separators=(',', ':')).encode()).hexdigest()
    base_runtime = dict(schema='paperflow-remote-live/1', server_identity=inspector.server,
                        transport_config=base_transport, transport_config_sha256=base_transport_sha)
    base_file = tmp_path / 'base-runtime.json'
    base_raw = json.dumps(base_runtime, sort_keys=True, separators=(',', ':')).encode()
    base_file.write_bytes(base_raw)
    access = dict(schema='paperflow-server-access-bindings/1', server_identity=inspector.server,
        sources=[dict(ref='new-gate', config_path=str(runtime_file),
                      config_sha256=hashlib.sha256(runtime_raw).hexdigest(),
                      private_key_sha256=hashlib.sha256(key.read_bytes()).hexdigest()),
                 dict(ref='base', config_path=str(base_file), config_sha256=hashlib.sha256(base_raw).hexdigest(),
                      private_key_sha256=hashlib.sha256(old_key.read_bytes()).hexdigest())])
    access_file = tmp_path / 'access.json'
    access_raw = json.dumps(access, sort_keys=True, separators=(',', ':')).encode()
    access_file.write_bytes(access_raw)
    bindings = module.PinnedAccessBindings(access_file, hashlib.sha256(access_raw).hexdigest(),
                                           server_identity=inspector.server)
    doc = json.loads(registry.read_text())
    doc['versions'][1]['access_ref'] = 'new-gate'
    registry_raw = json.dumps(doc, sort_keys=True, separators=(',', ':')).encode()
    registry.write_bytes(registry_raw)
    routing = module.HistoricalVersionRouter(registry, hashlib.sha256(registry_raw).hexdigest(),
        server_identity=inspector.server, remote_script='/opt/synthetic/version.py', remote_script_sha256='d' * 64,
        access_bindings=bindings)
    used = []
    def delegate(actual, _command, payload, **_kwargs):
        if 'nonce' in payload:
            return dict(schema='paperflow-server-job-config/1', server_identity=copy.deepcopy(inspector.server),
                job_id=payload['job']['job_id'], receipt_sha256=payload['receipt_sha256'], nonce=payload['nonce'],
                service_config_canonical_sha256=versions[1]['canonical_sha256'])
        used.append(actual)
        return [{'synthetic': True}]
    rpc = module.ReadOnlyRPC(delegate)
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    assert rpc(base, module.command_for(config, 'inspect'), {'requests': [requests[1]]}, timeout=5) == [{'synthetic': True}]
    assert used == [connection]
    key.write_bytes(b'changed synthetic key')
    with pytest.raises(ValueError):
        rpc(base, module.command_for(config, 'inspect'), {'requests': [requests[1]]}, timeout=5)
    key.write_bytes(b'synthetic key, never real credentials')
    old_key.write_bytes(b'changed old credential')
    with pytest.raises(ValueError):
        rpc(base, module.command_for(config, 'inspect'), {'requests': [requests[1]]}, timeout=5)


@pytest.mark.parametrize('verdict,permitted', [('passed', True), ('review_required', False), ('missing', False)])
def test_each_receipt_requires_its_own_unread_disclosure_check(sample, verdict, permitted):
    db, inspector, requests, config, registry, sha, versions = sample
    module = load('synthetic_disclosure_gate_' + verdict, HERE / 'read_only_rpc.py')
    assert hasattr(module, 'UnreadDisclosureGate'), 'the independent disclosure gate is missing'
    gate = module.UnreadDisclosureGate('/candidate/unread-query.py', 'f' * 64)
    routing = module.HistoricalVersionRouter(registry, sha, server_identity=inspector.server,
        remote_script='/candidate/version-query.py', remote_script_sha256='d' * 64, disclosure_gate=gate)
    def delegate(_connection, command, payload, **_kwargs):
        if 'nonce' in payload and '/candidate/unread-query.py' in command:
            if verdict == 'missing': return {}
            return dict(schema='paperflow-unread-disclosure-check/1', server_identity=copy.deepcopy(inspector.server),
                job_id=payload['job']['job_id'], receipt_sha256=payload['receipt_sha256'], nonce=payload['nonce'],
                status=verdict, reason='unread_disclosure_checked' if verdict == 'passed'
                else 'unlocated_unread_missing_public_limit')
        if 'nonce' in payload:
            return dict(schema='paperflow-server-job-config/1', server_identity=copy.deepcopy(inspector.server),
                job_id=payload['job']['job_id'], receipt_sha256=payload['receipt_sha256'], nonce=payload['nonce'],
                service_config_canonical_sha256=versions[0]['canonical_sha256'])
        return [{'synthetic': True}]
    rpc = module.ReadOnlyRPC(delegate)
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    if permitted:
        assert rpc(config['connection'], module.command_for(config, 'inspect'), {'requests': [requests[0]]},
                   timeout=5) == [{'synthetic': True}]
    else:
        expected_error = module.UnreadReviewRequired if verdict == 'review_required' else ValueError
        with pytest.raises(expected_error) as caught:
            rpc(config['connection'], module.command_for(config, 'inspect'), {'requests': [requests[0]]}, timeout=5)
        if verdict == 'missing':
            assert not isinstance(caught.value, module.UnreadReviewRequired)
