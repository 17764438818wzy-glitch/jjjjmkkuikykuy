"""Wire the candidate probe to the page using a synthetic local RPC only."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import sqlite3
from types import SimpleNamespace
from uuid import uuid4

import pytest


HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('synthetic_server_projection_helpers', HERE / 'test_server_projection.py')
fixtures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixtures)

from remote_jobs import RemoteJobs
from remote_server_transport import RemoteServerTransport
import server_rpc


@pytest.mark.parametrize('case', ['valid', 'wrong_server', 'wrong_ledger', 'wrong_nonce', 'timeout'])
def test_empty_projection_uses_fixed_transport_probe_with_synthetic_rpc(tmp_path, monkeypatch, case):
    control = fixtures.helpers.db.__wrapped__(tmp_path)
    inspector, _ = fixtures.make_inspector(control)
    server = copy.deepcopy(inspector.server)
    if case == 'wrong_server':
        server['server_id'] = 'different-synthetic-server'
    ledger = RemoteJobs(tmp_path / 'synthetic-server-ledger.sqlite3', capacity=1, ledger_id=server['ledger_id'])
    ledger.initialize()
    if case == 'wrong_ledger':
        with sqlite3.connect(ledger.database) as db:
            db.execute("UPDATE executor_settings SET value=? WHERE name='ledger_id'", (str(uuid4()),))
    monkeypatch.setattr(server_rpc.service, '_ledger', lambda _config: ledger)
    calls = []

    def rpc(_connection, command, payload, **kwargs):
        assert shlex.split(command)[-1] == 'probe'
        calls.append(copy.deepcopy(payload))
        assert kwargs['timeout'] == 5 and kwargs['max_bytes'] == 65536
        if case == 'timeout':
            raise TimeoutError('synthetic metadata timeout')
        value = server_rpc.execute('probe', payload, {'server_identity': server})
        if case == 'wrong_nonce':
            value['nonce'] = str(uuid4())
        return value

    config = dict(schema='paperflow-server-transport/1', server_identity=server,
        connection=dict(host='synthetic.invalid', port=22, username='synthetic',
            private_key=str(tmp_path / 'never-read-key'), server_key_sha256='1' * 64),
        server_python='/synthetic/python', staging_script='/synthetic/stage.py',
        rpc_script='/synthetic/rpc.py', service_config='/synthetic/service.json',
        service_config_sha256='2' * 64, staging_root='/synthetic/staging', allowed_roots=[str(tmp_path)],
        staging_timeout_seconds=60, metadata_timeout_seconds=5, admit_timeout_seconds=2,
        wait_timeout_seconds=20, poll_seconds=.1)
    raw = json.dumps(config, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
    journal = SimpleNamespace(journal_id=str(uuid4()), get_transfer=lambda _: None, get=lambda _: None)
    transport = RemoteServerTransport(config, config_sha256=hashlib.sha256(raw.encode()).hexdigest(),
                                      journal=journal, rpc_call=rpc)
    # verify_scope is only a synthetic stand-in: this test does not establish
    # the authoritative source, Prefect identity, network authentication or RAG readiness.
    result = fixtures.observe(control, inspector=inspector, verify_scope=lambda _: True,
                              probe_server=transport.probe_server)
    with sqlite3.connect(ledger.database) as db:
        jobs = db.execute('SELECT count(*) FROM executor_jobs').fetchone()[0]
    assert jobs == 0
    if case == 'valid':
        assert result['server_observation']['available'] is True
        assert result['stage']['counts']['waiting_rag_upload'] == 0
        assert len(calls) == 1
    else:
        assert result['server_observation']['available'] is False
        assert result['stage']['counts']['waiting_rag_upload'] is None
        assert len(calls) == (0 if case == 'wrong_server' else 1)
