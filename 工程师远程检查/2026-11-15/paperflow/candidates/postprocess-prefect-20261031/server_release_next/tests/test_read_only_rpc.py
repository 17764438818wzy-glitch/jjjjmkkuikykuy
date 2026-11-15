"""Only synthetic receipts, control databases and a fake RPC are used."""
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parent


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fixtures = load('synthetic_readonly_rpc_fixtures', ROOT / 'remote_projection_next' / 'tests' / 'test_server_projection.py')


def test_readonly_rpc_automatically_accepts_only_actual_registered_receipts(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    identity = fixtures.add_complete_pass(db, 1)
    inspector, _ = fixtures.make_inspector(db)
    with sqlite3.connect(db.path) as conn:
        raw, claim_id = conn.execute('SELECT receipt_json,claim_id FROM postprocessing_receipts').fetchone()
    request = inspector._request(json.loads(raw), identity, claim_id)
    calls = []
    config = {'connection': {'host': 'synthetic'}, 'server_python': '/python', 'rpc_script': '/rpc.py',
              'service_config': '/service.json', 'service_config_sha256': 'a' * 64}
    module = load('synthetic_readonly_rpc', HERE / 'read_only_rpc.py')
    rpc = module.ReadOnlyRPC(lambda *args, **kwargs: calls.append((args, kwargs)) or ['ok'])
    rpc.bind(database=db.path, inspector=inspector, transport_config=config)
    command = module.command_for(config, 'inspect')
    assert rpc(config['connection'], command, {'requests': [request]}, timeout=2) == ['ok']
    assert len(calls) == 1
    altered = json.loads(json.dumps(request))
    altered['job']['identity']['group_id'] = 'different-folder'
    with pytest.raises(ValueError):
        rpc(config['connection'], command, {'requests': [altered]}, timeout=2)
    with pytest.raises(ValueError):
        rpc(config['connection'], module.command_for(config, 'admit'), {'requests': [request]}, timeout=2)
    with sqlite3.connect(db.path) as conn:
        conn.execute("UPDATE postprocessing_results SET state='unresolved'")
    with pytest.raises(ValueError):
        rpc(config['connection'], command, {'requests': [request]}, timeout=2)
    assert len(calls) == 1


def test_readonly_rpc_does_not_allow_a_changed_connection_or_command(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    identity = fixtures.add_complete_pass(db, 1)
    inspector, _ = fixtures.make_inspector(db)
    with sqlite3.connect(db.path) as conn:
        raw, claim_id = conn.execute('SELECT receipt_json,claim_id FROM postprocessing_receipts').fetchone()
    request = inspector._request(json.loads(raw), identity, claim_id)
    config = {'connection': {'host': 'synthetic'}, 'server_python': '/python', 'rpc_script': '/rpc.py',
              'service_config': '/service.json', 'service_config_sha256': 'a' * 64}
    module = load('synthetic_readonly_rpc_config', HERE / 'read_only_rpc.py')
    rpc = module.ReadOnlyRPC(lambda *args, **kwargs: pytest.fail('must not call remote'))
    rpc.bind(database=db.path, inspector=inspector, transport_config=config)
    with pytest.raises(ValueError):
        rpc({'host': 'different'}, module.command_for(config, 'inspect'), {'requests': [request]}, timeout=2)
    with pytest.raises(ValueError):
        rpc(config['connection'], '/different/command inspect', {'requests': [request]}, timeout=2)


def test_deleted_parent_is_never_sent_to_remote_observation(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    identity = fixtures.add_complete_pass(db, 1)
    inspector, _ = fixtures.make_inspector(db)
    with sqlite3.connect(db.path) as conn:
        raw, claim_id = conn.execute('SELECT receipt_json,claim_id FROM postprocessing_receipts').fetchone()
        conn.execute('INSERT INTO task_deletions VALUES(?)', (identity['task_id'],))
    request = inspector._request(json.loads(raw), identity, claim_id)
    config = {'connection': {'host': 'synthetic'}, 'server_python': '/python', 'rpc_script': '/rpc.py',
              'service_config': '/service.json', 'service_config_sha256': 'a' * 64}
    module = load('synthetic_deleted_rpc', HERE / 'read_only_rpc.py')
    rpc = module.ReadOnlyRPC(lambda *args, **kwargs: pytest.fail('must not call remote'))
    rpc.bind(database=db.path, inspector=inspector, transport_config=config)
    with pytest.raises(ValueError):
        rpc(config['connection'], module.command_for(config, 'inspect'), {'requests': [request]}, timeout=2)


def test_direct_inspect_rechecks_deletion_after_the_remote_call(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    identity = fixtures.add_complete_pass(db, 1)
    inspector, _ = fixtures.make_inspector(db)
    with sqlite3.connect(db.path) as conn:
        raw, claim_id = conn.execute('SELECT receipt_json,claim_id FROM postprocessing_receipts').fetchone()
    request = inspector._request(json.loads(raw), identity, claim_id)
    config = {'connection': {'host': 'synthetic'}, 'server_python': '/python', 'rpc_script': '/rpc.py',
              'service_config': '/service.json', 'service_config_sha256': 'a' * 64}
    module = load('synthetic_direct_deleted_during_inspection', HERE / 'read_only_rpc.py')
    def delegate(*_args, **_kwargs):
        with sqlite3.connect(db.path) as conn:
            conn.execute('INSERT INTO task_deletions VALUES(?)', (identity['task_id'],))
        return ['ok']
    rpc = module.ReadOnlyRPC(delegate)
    rpc.bind(database=db.path, inspector=inspector, transport_config=config)
    with pytest.raises(ValueError):
        rpc(config['connection'], module.command_for(config, 'inspect'), {'requests': [request]}, timeout=2)
