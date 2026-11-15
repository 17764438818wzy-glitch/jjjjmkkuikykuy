import sqlite3
from uuid import uuid4

import pytest

from test_terminated_capacity_release import ledger_with_recorded_termination


def test_capacity_query_counts_only_proven_stopped_workers_as_free(tmp_path):
    from server_capacity_query import query_capacity
    store, item, config = ledger_with_recorded_termination(tmp_path)
    store.initialize_capacity_reconciliation()
    server = {'server_id': 'synthetic', 'ledger_id': store.ledger_id}
    nonce = str(uuid4())
    first = query_capacity(store.database, capacity=1, server_identity=server, nonce=nonce)
    assert first['occupied'] == 1 and first['available'] == 0 and first['nonce'] == nonce
    store.release_terminated_capacity(item['job_id'], item['claim_id'], config)
    second = query_capacity(store.database, capacity=1, server_identity=server, nonce=nonce)
    assert second['occupied'] == 0 and second['available'] == 1
    assert store.status(item['job_id'])['state'] == 'unknown'


def test_changed_termination_evidence_is_conservatively_occupied(tmp_path):
    from server_capacity_query import query_capacity
    store, item, config = ledger_with_recorded_termination(tmp_path)
    store.initialize_capacity_reconciliation()
    store.release_terminated_capacity(item['job_id'], item['claim_id'], config)
    with sqlite3.connect(store.database) as db:
        db.execute("update service_jobs set detail_json='{}' where job_id=?", (item['job_id'],))
    server = {'server_id': 'synthetic', 'ledger_id': store.ledger_id}
    result = query_capacity(store.database, capacity=1, server_identity=server, nonce=str(uuid4()))
    assert result['occupied'] == 1 and result['available'] == 0


def test_wrong_server_ledger_identity_never_returns_an_available_count(tmp_path):
    from server_capacity_query import query_capacity
    store, _, _ = ledger_with_recorded_termination(tmp_path)
    with pytest.raises(ValueError):
        query_capacity(store.database, capacity=1,
                       server_identity={'server_id': 'synthetic', 'ledger_id': str(uuid4())}, nonce=str(uuid4()))
