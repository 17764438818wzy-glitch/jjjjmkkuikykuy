"""Read-only, authenticated empty-queue observations with synthetic ledgers."""
import copy
from pathlib import Path
import sqlite3
import sys
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from remote_jobs import RemoteJobs
import server_rpc
from test_remote_server_transport import setup


def test_server_probe_verifies_the_original_ledger_without_writing(tmp_path, monkeypatch):
    ledger_id = str(uuid4())
    ledger = RemoteJobs(tmp_path / 'ledger.db', capacity=1, ledger_id=ledger_id)
    ledger.initialize()
    config = {'server_identity': {'server_id': 'synthetic-server', 'ledger_id': ledger_id}}
    monkeypatch.setattr(server_rpc.service, '_ledger', lambda value: ledger)
    nonce = str(uuid4())
    before = ledger.database.read_bytes()
    result = server_rpc.execute('probe', {'server_identity': config['server_identity'], 'nonce': nonce}, config)
    assert result == {'schema': 'paperflow-server-probe/1', 'server_identity': config['server_identity'],
                      'nonce': nonce, 'ledger_identity_verified': True}
    assert ledger.database.read_bytes() == before


@pytest.mark.parametrize('damage', ['server', 'ledger', 'nonce', 'missing', 'extra'])
def test_server_probe_rejects_wrong_identity_or_ledger(tmp_path, monkeypatch, damage):
    ledger_id = str(uuid4())
    ledger = RemoteJobs(tmp_path / 'ledger.db', capacity=1, ledger_id=ledger_id)
    ledger.initialize()
    config = {'server_identity': {'server_id': 'synthetic-server', 'ledger_id': ledger_id}}
    monkeypatch.setattr(server_rpc.service, '_ledger', lambda value: ledger)
    request = {'server_identity': copy.deepcopy(config['server_identity']), 'nonce': str(uuid4())}
    if damage == 'server': request['server_identity']['server_id'] = 'other'
    if damage == 'nonce': request['nonce'] = 'not-a-uuid'
    if damage == 'missing': request.pop('nonce')
    if damage == 'extra': request['extra'] = True
    if damage == 'ledger':
        with sqlite3.connect(ledger.database) as db:
            db.execute("UPDATE executor_settings SET value=? WHERE name='ledger_id'", (str(uuid4()),))
    with pytest.raises((ValueError, sqlite3.Error)):
        server_rpc.execute('probe', request, config)


def test_transport_probe_requires_fresh_exact_response_and_original_server(setup):
    x = setup
    requests = []
    def rpc(_connection, command, payload, **_kwargs):
        requests.append((command, copy.deepcopy(payload)))
        return dict(schema='paperflow-server-probe/1', server_identity=copy.deepcopy(x['server']),
                    nonce=payload['nonce'], ledger_identity_verified=True)
    transport = x['construct'](rpc_call=rpc)
    assert transport.probe_server(copy.deepcopy(x['server'])) is True
    assert requests[0][0].endswith(' probe')
    with pytest.raises(x['m'].TransportError):
        transport.probe_server(dict(x['server'], server_id='other'))
    assert len(requests) == 1


@pytest.mark.parametrize('field', ['schema', 'server_identity', 'nonce', 'ledger_identity_verified'])
def test_transport_probe_rejects_changed_reply(setup, field):
    x = setup
    def rpc(_connection, _command, payload, **_kwargs):
        reply = dict(schema='paperflow-server-probe/1', server_identity=copy.deepcopy(x['server']),
                     nonce=payload['nonce'], ledger_identity_verified=True)
        reply[field] = None
        return reply
    with pytest.raises(x['m'].TransportError):
        x['construct'](rpc_call=rpc).probe_server(x['server'])


def test_transport_probe_rejects_integer_in_place_of_boolean(setup):
    x = setup
    def rpc(_connection, _command, payload, **_kwargs):
        return dict(schema='paperflow-server-probe/1', server_identity=copy.deepcopy(x['server']),
                    nonce=payload['nonce'], ledger_identity_verified=1)
    with pytest.raises(x['m'].TransportError):
        x['construct'](rpc_call=rpc).probe_server(x['server'])
