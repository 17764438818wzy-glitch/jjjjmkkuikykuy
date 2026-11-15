"""Synthetic history routing using the already established inspect operation."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import sqlite3
import time

import pytest

HERE = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


history = load('existing_history_fixtures', HERE / 'tests' / 'test_historical_version_router.py')


@pytest.fixture
def example(tmp_path):
    db, inspector, requests, config, registry, sha, versions = history.sample.__wrapped__(tmp_path)
    key = tmp_path / 'synthetic-key'
    key.write_bytes(b'fake key')
    connection = dict(host='synthetic', port=22, username='test', private_key=str(key), server_key_sha256='e' * 64)
    config['connection'] = connection
    transport = {'server_identity': inspector.server, 'connection': connection}
    canonical = lambda value: json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
    runtime = {'schema': 'paperflow-remote-live/1', 'server_identity': inspector.server,
               'transport_config': transport, 'transport_config_sha256': hashlib.sha256(canonical(transport)).hexdigest()}
    runtime_path = tmp_path / 'synthetic-runtime.json'
    runtime_path.write_bytes(canonical(runtime))
    access = {'schema': 'paperflow-server-access-bindings/1', 'server_identity': inspector.server,
              'sources': [{'ref': 'base', 'config_path': str(runtime_path),
                           'config_sha256': hashlib.sha256(runtime_path.read_bytes()).hexdigest(),
                           'private_key_sha256': hashlib.sha256(key.read_bytes()).hexdigest()}]}
    access_path = tmp_path / 'synthetic-access.json'
    access_path.write_bytes(canonical(access))
    module = load('synthetic_existing_inspect_router', HERE / 'read_only_rpc.py')
    bindings = module.PinnedAccessBindings(access_path, hashlib.sha256(access_path.read_bytes()).hexdigest(),
                                           server_identity=inspector.server)
    gate = module.UnreadDisclosureGate('/candidate/unread-query.py', 'f' * 64)
    assert hasattr(module, 'ExistingInspectVersionRouter'), 'existing inspect routing is missing'
    routing = module.ExistingInspectVersionRouter(registry, sha, server_identity=inspector.server,
                                                  access_bindings=bindings, disclosure_gate=gate)
    return module, routing, db, inspector, requests, config, versions


@pytest.mark.parametrize('outcome', ['passed', 'review', 'all_fail', 'wrong_receipt', 'first_bad_map'])
def test_only_a_matching_existing_inspect_and_disclosure_can_produce_a_result(example, outcome):
    module, routing, db, inspector, requests, config, versions = example
    calls = []
    request = requests[0]
    valid = inspector._inspect_batch([request])[0]
    def delegate(_connection, command, payload, **_kwargs):
        args = shlex.split(command)
        calls.append((args, copy.deepcopy(payload)))
        if '/candidate/unread-query.py' in args:
            return dict(schema='paperflow-unread-disclosure-check/1', server_identity=inspector.server,
                job_id=request['job']['job_id'], receipt_sha256=request['receipt_sha256'], nonce=payload['nonce'],
                status='review_required' if outcome == 'review' else 'passed',
                reason='unlocated_unread_missing_public_limit' if outcome == 'review' else 'unread_disclosure_checked')
        assert args[-1] == 'inspect' and '-c' not in args
        if args[-3] == versions[0]['service_config'] and outcome == 'first_bad_map':
            return [dict(copy.deepcopy(valid), artifact_map={})]
        if args[-3] == versions[0]['service_config'] or outcome == 'all_fail':
            raise ValueError('synthetic wrong or unavailable historical configuration')
        result = copy.deepcopy(valid)
        if outcome == 'wrong_receipt': result['receipt_sha256'] = 'a' * 64
        return [result]
    rpc = module.ReadOnlyRPC(delegate)
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    if outcome in {'passed', 'first_bad_map'}:
        assert rpc(config['connection'], module.command_for(config, 'inspect'), {'requests': [request]},
                   timeout=5, max_bytes=100000) == [valid]
        assert [call[0][-3] for call in calls[:2]] == [v['service_config'] for v in versions]
        assert len(calls) == 3
    else:
        with pytest.raises(ValueError):
            rpc(config['connection'], module.command_for(config, 'inspect'), {'requests': [request]},
                timeout=5, max_bytes=100000)
        assert len(calls) <= 3


def test_access_resolution_is_inside_the_total_timeout(example, monkeypatch):
    module, routing, db, inspector, requests, config, _ = example
    original = routing.access_bindings.resolve
    def slow(reference):
        time.sleep(.08)
        return original(reference)
    monkeypatch.setattr(routing.access_bindings, 'resolve', slow)
    calls = []
    def delegate(*_args, **_kwargs):
        calls.append(True)
        return []
    rpc = module.ReadOnlyRPC(delegate)
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    with pytest.raises(ValueError, match='timed out'):
        rpc(config['connection'], module.command_for(config, 'inspect'), {'requests': [requests[0]]}, timeout=.02)
    assert not calls


def test_probe_uses_pinned_base_key_and_allows_unchanged_access(example):
    module, routing, db, inspector, _, config, _ = example
    calls = []
    rpc = module.ReadOnlyRPC(lambda *args, **kwargs: calls.append(args) or {'status': 'synthetic-ok'})
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    payload = {'server_identity': inspector.server, 'nonce': 'synthetic-probe'}
    assert rpc(config['connection'], module.command_for(config, 'probe'), payload, timeout=5) == {
        'status': 'synthetic-ok'}
    assert len(calls) == 1


def test_probe_never_calls_server_after_pinned_key_changes(example):
    module, routing, db, inspector, _, config, _ = example
    calls = []
    rpc = module.ReadOnlyRPC(lambda *args, **kwargs: calls.append(args) or {'status': 'synthetic-ok'})
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    Path(config['connection']['private_key']).write_bytes(b'changed synthetic key')
    with pytest.raises(ValueError):
        rpc(config['connection'], module.command_for(config, 'probe'),
            {'server_identity': inspector.server, 'nonce': 'synthetic-probe'}, timeout=5)
    assert not calls


def test_probe_rejects_result_when_pinned_key_changes_during_call(example):
    module, routing, db, inspector, _, config, _ = example
    calls = []
    def delegate(*args, **_kwargs):
        calls.append(args)
        Path(config['connection']['private_key']).write_bytes(b'changed synthetic key')
        return {'status': 'synthetic-ok'}
    rpc = module.ReadOnlyRPC(delegate)
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    with pytest.raises(ValueError):
        rpc(config['connection'], module.command_for(config, 'probe'),
            {'server_identity': inspector.server, 'nonce': 'synthetic-probe'}, timeout=5)
    assert len(calls) == 1


def test_probe_base_access_resolution_is_inside_the_total_timeout(example, monkeypatch):
    module, routing, db, inspector, _, config, _ = example
    original = routing.access_bindings.resolve
    def slow(reference):
        time.sleep(.08)
        return original(reference)
    monkeypatch.setattr(routing.access_bindings, 'resolve', slow)
    calls = []
    rpc = module.ReadOnlyRPC(lambda *args, **kwargs: calls.append(args) or {'status': 'synthetic-ok'})
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    with pytest.raises(ValueError):
        rpc(config['connection'], module.command_for(config, 'probe'),
            {'server_identity': inspector.server, 'nonce': 'synthetic-probe'}, timeout=.02)
    assert not calls


def test_probe_rejects_result_when_post_call_access_check_exceeds_timeout(example, monkeypatch):
    module, routing, db, inspector, _, config, _ = example
    original = routing.access_bindings.resolve
    checks = []
    def slow_after_call(reference):
        checks.append(reference)
        if len(checks) == 2:
            time.sleep(.4)
        return original(reference)
    monkeypatch.setattr(routing.access_bindings, 'resolve', slow_after_call)
    calls = []
    rpc = module.ReadOnlyRPC(lambda *args, **kwargs: calls.append(args) or {'status': 'synthetic-ok'})
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    with pytest.raises(ValueError):
        rpc(config['connection'], module.command_for(config, 'probe'),
            {'server_identity': inspector.server, 'nonce': 'synthetic-probe'}, timeout=.3)
    assert len(calls) == 1


def test_probe_without_access_bindings_retains_previous_behavior(example):
    module, _, db, inspector, _, config, _ = example
    calls = []
    rpc = module.ReadOnlyRPC(lambda *args, **kwargs: calls.append(args) or {'status': 'synthetic-ok'})
    rpc.bind(database=db.path, inspector=inspector, transport_config=config)
    assert rpc(config['connection'], module.command_for(config, 'probe'),
               {'server_identity': inspector.server, 'nonce': 'synthetic-probe'}, timeout=5) == {
        'status': 'synthetic-ok'}
    assert len(calls) == 1


def test_probe_never_calls_server_when_base_access_names_another_connection(example):
    module, routing, db, inspector, _, config, _ = example
    config['connection']['username'] = 'different-synthetic-user'
    calls = []
    rpc = module.ReadOnlyRPC(lambda *args, **kwargs: calls.append(args) or {'status': 'synthetic-ok'})
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    with pytest.raises(ValueError):
        rpc(config['connection'], module.command_for(config, 'probe'),
            {'server_identity': inspector.server, 'nonce': 'synthetic-probe'}, timeout=5)
    assert not calls


def test_review_is_only_reported_after_registration_is_rechecked(example):
    module, routing, db, inspector, requests, config, versions = example
    request = requests[0]
    valid = inspector._inspect_batch([request])[0]

    def delegate(_connection, command, payload, **_kwargs):
        args = shlex.split(command)
        if '/candidate/unread-query.py' in args:
            with sqlite3.connect(db.path) as conn:
                conn.execute('INSERT INTO task_deletions VALUES(?)', (request['job']['identity']['task_id'],))
            return dict(schema='paperflow-unread-disclosure-check/1', server_identity=inspector.server,
                job_id=request['job']['job_id'], receipt_sha256=request['receipt_sha256'], nonce=payload['nonce'],
                status='review_required', reason='unlocated_unread_missing_public_limit')
        if args[-3] == versions[0]['service_config']:
            raise ValueError('synthetic wrong historical configuration')
        return [copy.deepcopy(valid)]

    rpc = module.ReadOnlyRPC(delegate)
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    with pytest.raises(ValueError, match='deleted task') as failure:
        rpc(config['connection'], module.command_for(config, 'inspect'), {'requests': [request]}, timeout=5)
    assert not isinstance(failure.value, module.UnreadReviewRequired)


def test_expired_outer_deadline_invalidates_review(example, monkeypatch):
    module, routing, db, inspector, requests, config, _ = example

    def slow_review(*_args, **_kwargs):
        time.sleep(.04)
        raise module.UnreadReviewRequired('synthetic delayed review')

    monkeypatch.setattr(routing, 'inspect', slow_review)
    rpc = module.ReadOnlyRPC(lambda *_args, **_kwargs: None)
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    with pytest.raises(ValueError, match='timed out') as failure:
        rpc(config['connection'], module.command_for(config, 'inspect'), {'requests': [requests[0]]}, timeout=.02)
    assert not isinstance(failure.value, module.UnreadReviewRequired)
