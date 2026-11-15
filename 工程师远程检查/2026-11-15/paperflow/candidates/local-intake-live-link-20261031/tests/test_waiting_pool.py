from concurrent.futures import ThreadPoolExecutor
import sqlite3
from pathlib import Path
from threading import Event

import pytest
from fastapi.testclient import TestClient
from test_registered_input import registered
from test_retention_flow import RetentionService
from test_upstream_discovery import TwoPageSource


def received_queue(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'queue.sqlite3', require_retention=True)
    result = discover_once(TwoPageSource(item), queue, receipt_root=receipt_root,
                           retention_client=RetentionService())
    assert result['complete'] is True
    return queue, item


def test_actual_handoff_removes_one_waiting_group_and_is_idempotent(registered, tmp_path):
    queue, item = received_queue(registered, tmp_path)
    assert queue.snapshot()['waiting_count'] == 1
    first = queue.claim_next(consumer_id='preprocessor', attempt_id='attempt-1')
    assert first['group_id'] == item['group_id']
    assert first['state'] == 'processing'
    assert queue.snapshot()['waiting_count'] == 0
    repeated = queue.claim_next(consumer_id='preprocessor', attempt_id='attempt-1')
    assert repeated['receipt_id'] == first['receipt_id']
    assert queue.snapshot()['waiting_count'] == 0
    assert queue.claim_next(consumer_id='preprocessor', attempt_id='attempt-2') is None


def test_two_consumers_cannot_claim_the_same_waiting_group(registered, tmp_path):
    queue, item = received_queue(registered, tmp_path)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda n: queue.claim_next(consumer_id='preprocessor',
                                                          attempt_id=f'attempt-{n}'), (1, 2)))
    claimed = [row for row in results if row is not None]
    assert len(claimed) == 1
    assert claimed[0]['group_id'] == item['group_id']
    assert queue.snapshot()['waiting_count'] == 0
    with sqlite3.connect(queue.path) as db:
        assert db.execute('SELECT COUNT(*) FROM claims').fetchone()[0] == 1


def test_pool_http_handoff_is_separate_from_waiting_summary(registered, tmp_path):
    from waiting_api import create_app

    queue, item = received_queue(registered, tmp_path)
    with TestClient(create_app(queue, enable_claims=True)) as client:
        before = client.get('/api/v1/local-intake/preprocessing/summary').json()
        claim = client.post('/api/v1/local-intake/preprocessing/claims',
                            json={'consumer_id': 'preprocessor', 'attempt_id': 'attempt-http'})
        after = client.get('/api/v1/local-intake/preprocessing/summary').json()
        duplicate = client.post('/api/v1/local-intake/preprocessing/claims',
                                json={'consumer_id': 'preprocessor', 'attempt_id': 'attempt-http'})
        empty = client.post('/api/v1/local-intake/preprocessing/claims',
                            json={'consumer_id': 'preprocessor', 'attempt_id': 'attempt-other'})
    assert before['waiting_count'] == 1
    assert claim.status_code == duplicate.status_code == 200
    assert claim.json()['group_id'] == duplicate.json()['group_id'] == item['group_id']
    assert after['waiting_count'] == 0
    assert empty.status_code == 204
    assert after['preprocessing_handoff_connected'] is False


def test_reception_api_does_not_enable_claims_by_default(registered, tmp_path):
    from waiting_api import create_app

    queue, _ = received_queue(registered, tmp_path)
    with TestClient(create_app(queue)) as client:
        response = client.post('/api/v1/local-intake/preprocessing/claims',
                               json={'consumer_id': 'preprocessor', 'attempt_id': 'attempt-http'})
        assert response.status_code == 404
        assert client.get('/api/v1/local-intake/preprocessing/summary').json()['waiting_count'] == 1


def test_repeated_claim_does_not_return_a_group_now_requiring_review(registered, tmp_path):
    queue, _ = received_queue(registered, tmp_path)
    queue.claim_next(consumer_id='preprocessor', attempt_id='attempt-1')
    with sqlite3.connect(queue.path) as db:
        db.execute("UPDATE groups SET state='needs_review'")
    with pytest.raises(ValueError, match='核对'):
        queue.claim_next(consumer_id='preprocessor', attempt_id='attempt-1')


def test_changed_delivery_is_held_instead_of_being_claimed(registered, tmp_path):
    queue, _ = received_queue(registered, tmp_path)
    body = queue.list_waiting()[0]['body']
    Path(body['path']).write_bytes(b'modified after reception')
    with pytest.raises(ValueError, match='核对'):
        queue.claim_next(consumer_id='preprocessor', attempt_id='attempt-1')
    assert queue.list_waiting() == []
    assert len(queue.list_review()) == 1


def test_failed_pre_handoff_check_can_recover_with_a_new_attempt(registered, tmp_path):
    _, receipt_root, receipt, item = registered
    queue, _ = received_queue(registered, tmp_path)
    body = Path(receipt['inventory']['body']['path'])
    original = body.read_bytes()
    body.write_bytes(b'temporary invalid data')
    with pytest.raises(ValueError, match='核对'):
        queue.claim_next(consumer_id='preprocessor', attempt_id='attempt-failed')
    body.write_bytes(original)
    assert queue.receive('task-one', item, receipt_root=receipt_root)['state'] == 'waiting'
    with pytest.raises(ValueError, match='核对'):
        queue.claim_next(consumer_id='preprocessor', attempt_id='attempt-failed')
    assert queue.claim_next(consumer_id='preprocessor', attempt_id='attempt-new')['state'] == 'processing'


def test_replayed_processing_attempt_rechecks_the_delivery(registered, tmp_path):
    queue, _ = received_queue(registered, tmp_path)
    first = queue.claim_next(consumer_id='preprocessor', attempt_id='attempt-1')
    Path(first['body']['path']).write_bytes(b'changed after first response')
    with pytest.raises(ValueError, match='核对'):
        queue.claim_next(consumer_id='preprocessor', attempt_id='attempt-1')
    assert len(queue.list_review()) == 1


def test_handoff_rechecks_full_sha_even_if_lightweight_metadata_is_unchanged(monkeypatch, registered, tmp_path):
    import local_queue

    queue, _ = received_queue(registered, tmp_path)
    verified = []
    original = local_queue.read_registered_item

    def recording_reader(*args, **kwargs):
        verified.append(True)
        return original(*args, **kwargs)

    monkeypatch.setattr(local_queue, 'read_registered_item', recording_reader)
    result = queue.claim_next(consumer_id='preprocessor', attempt_id='attempt-1')
    assert result is not None
    assert verified == [True]


def test_interrupted_verification_keeps_the_same_claim_recoverable(monkeypatch, registered, tmp_path):
    import local_queue

    queue, _ = received_queue(registered, tmp_path)
    original = local_queue.read_registered_item
    monkeypatch.setattr(local_queue, 'read_registered_item',
                        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt()))
    with pytest.raises(KeyboardInterrupt):
        queue.claim_next(consumer_id='preprocessor', attempt_id='attempt-1')
    assert queue.snapshot()['waiting_count'] == 1
    assert queue.claim_next(consumer_id='preprocessor', attempt_id='attempt-2') is None
    monkeypatch.setattr(local_queue, 'read_registered_item', original)
    assert queue.claim_next(consumer_id='preprocessor', attempt_id='attempt-1')['state'] == 'processing'
    assert queue.snapshot()['waiting_count'] == 0


def test_claim_hashing_does_not_hold_the_local_database_write_lock(monkeypatch, registered, tmp_path):
    import local_queue

    queue, _ = received_queue(registered, tmp_path)
    entered, release = Event(), Event()
    original = local_queue.read_registered_item

    def slow_reader(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(local_queue, 'read_registered_item', slow_reader)
    with ThreadPoolExecutor(max_workers=2) as pool:
        claim = pool.submit(queue.claim_next, consumer_id='preprocessor', attempt_id='attempt-1')
        assert entered.wait(5)
        try:
            status_write = pool.submit(queue.record_scan, complete=False, reason='synthetic')
            status_write.result(timeout=2)
        finally:
            release.set()
        assert claim.result(timeout=5)['state'] == 'processing'
