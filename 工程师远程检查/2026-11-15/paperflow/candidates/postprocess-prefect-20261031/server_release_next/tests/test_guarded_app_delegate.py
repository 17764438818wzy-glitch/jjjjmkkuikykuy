"""Synthetic command rewriting for a fixed, read-only server entry."""
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath
import shlex
import time

import pytest

HERE = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fixtures = load('synthetic_guard_delegate_fixtures', HERE / 'tests' / 'test_existing_inspect_router.py')


@pytest.fixture
def sample(tmp_path):
    module, routing, db, inspector, requests, config, versions = fixtures.example.__wrapped__(tmp_path)
    index = {'schema': 'paperflow-guarded-app-versions/1', 'server_identity': inspector.server,
        'versions': [dict(canonical_sha256=v['canonical_sha256'], app=str(PurePosixPath(v['rpc_script']).parent),
                          manifest_path='/observer/manifest-' + str(i) + '.json', manifest_sha256=str(i + 5) * 64)
                     for i, v in enumerate(versions)]}
    encode = lambda value: json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
    index_path = tmp_path / 'index.json'
    index_path.write_bytes(encode(index))
    bundle = dict(schema='paperflow-server-guard-bundle/1', server_identity=inspector.server,
        index_path=str(index_path), index_sha256=hashlib.sha256(index_path.read_bytes()).hexdigest(),
        entry_path='/observer/entry.py', entry_sha256='a' * 64,
        guard_path='/observer/guard.py', guard_sha256='b' * 64,
        unread_path='/candidate/unread-query.py', unread_sha256='f' * 64)
    bundle_path = tmp_path / 'bundle.json'
    bundle_path.write_bytes(encode(bundle))
    return module, routing, db, inspector, requests, config, versions, index_path, bundle_path, bundle


def test_only_exact_reviewed_inspect_and_unread_commands_are_rewritten(sample):
    module, routing, _, _, requests, config, versions, _, bundle_path, bundle = sample
    assert hasattr(module, 'GuardedAppDelegate'), 'guarded historical command routing is missing'
    calls = []
    def delegate(connection, command, payload, **kwargs):
        calls.append((connection, shlex.split(command), payload, kwargs))
        return {'ok': True}
    guarded = module.GuardedAppDelegate(delegate, routing, bundle_path,
                                        hashlib.sha256(bundle_path.read_bytes()).hexdigest())
    chosen = versions[0]
    assert guarded(config['connection'], module.command_for(chosen, 'inspect'), {'requests': [requests[0]]},
                   timeout=5) == {'ok': True}
    args = calls[0][1]
    assert args[:3] == [chosen['server_python'], '-I', '-B']
    assert args[-1] == 'inspect' and chosen['rpc_script'] in args and chosen['rpc_script_sha256'] in args
    assert '/observer/manifest-0.json' in args and bundle['entry_sha256'] in args
    assert '\r' not in shlex.join(args) and '\n' not in shlex.join(args)
    gate = routing.disclosure_gate
    unread = shlex.join([chosen['server_python'], '-B', '-c', module.VERSION_BOOTSTRAP,
        gate.remote_script, gate.remote_script_sha256, str(PurePosixPath(chosen['rpc_script']).parent),
        chosen['service_config'], chosen['service_config_sha256']])
    guarded(config['connection'], unread, {'nonce': 'synthetic'}, timeout=5)
    assert calls[-1][1][-1] == 'unread' and gate.remote_script_sha256 in calls[-1][1]
    with pytest.raises(ValueError):
        guarded(config['connection'], module.command_for(chosen, 'admit'), {}, timeout=5)
    assert len(calls) == 2


def test_guarded_delegate_rejects_changed_version_index_before_remote_call(sample):
    module, routing, _, _, _, _, _, index_path, bundle_path, _ = sample
    guarded = module.GuardedAppDelegate(lambda *_a, **_k: pytest.fail('must not call remote'), routing, bundle_path,
                                        hashlib.sha256(bundle_path.read_bytes()).hexdigest())
    index_path.write_text('{}', encoding='utf8')
    with pytest.raises(ValueError):
        guarded({}, 'not-a-command', {}, timeout=5)


def test_final_binding_recheck_is_inside_the_total_timeout(sample, monkeypatch):
    module, routing, _, _, requests, config, versions, _, bundle_path, _ = sample
    guarded = module.GuardedAppDelegate(lambda *_a, **_k: {'ok': True}, routing, bundle_path,
                                        hashlib.sha256(bundle_path.read_bytes()).hexdigest())
    original = guarded._bindings
    calls = []
    def slow_final():
        calls.append(True)
        if len(calls) == 2: time.sleep(.08)
        return original()
    monkeypatch.setattr(guarded, '_bindings', slow_final)
    with pytest.raises(ValueError, match='expired'):
        guarded(config['connection'], module.command_for(versions[0], 'inspect'),
                {'requests': [requests[0]]}, timeout=.02)


def test_registered_folder_uses_guarded_inspect_and_guarded_unread_in_order(sample):
    module, routing, db, inspector, requests, config, versions, _, bundle_path, _ = sample
    request = requests[0]
    valid = inspector._inspect_batch([request])[0]
    calls = []
    def remote(_connection, command, payload, **_kwargs):
        args = shlex.split(command)
        calls.append(args)
        if args[-1] == 'inspect' and args[-3] == versions[0]['service_config']:
            raise ValueError('wrong historical config')
        if args[-1] == 'inspect':
            return [valid]
        assert args[-1] == 'unread'
        return dict(schema='paperflow-unread-disclosure-check/1', server_identity=inspector.server,
            job_id=request['job']['job_id'], receipt_sha256=request['receipt_sha256'], nonce=payload['nonce'],
            status='passed', reason='unread_disclosure_checked')
    guarded = module.GuardedAppDelegate(remote, routing, bundle_path,
                                        hashlib.sha256(bundle_path.read_bytes()).hexdigest())
    rpc = module.ReadOnlyRPC(guarded)
    rpc.bind(database=db.path, inspector=inspector, transport_config=config, version_router=routing)
    assert rpc(config['connection'], module.command_for(config, 'inspect'), {'requests': [request]},
               timeout=5, max_bytes=100000) == [valid]
    assert [row[-1] for row in calls] == ['inspect', 'inspect', 'unread']
    assert '/observer/manifest-1.json' in calls[-2] and '/observer/manifest-1.json' in calls[-1]
