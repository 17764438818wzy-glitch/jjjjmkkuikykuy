from fastapi.testclient import TestClient
import sqlite3

from test_registered_input import registered
from test_upstream_discovery import TwoPageSource


def test_waiting_api_reports_unknown_before_any_upstream_scan(tmp_path):
    from local_queue import LocalQueue
    from waiting_api import create_app

    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    with TestClient(create_app(queue)) as client:
        response = client.get('/api/v1/local-intake/preprocessing/summary')
    assert response.status_code == 200
    assert response.headers['cache-control'] == 'no-store'
    result = response.json()
    assert result['scope'] == 'all_tasks'
    assert result['waiting_count'] is None
    assert result['waiting_available'] is False
    assert result['upstream_scan_completed_at'] is None
    assert result['preprocessing_handoff_connected'] is False
    assert 'processing_count' not in result


def test_waiting_api_counts_only_verified_local_receipts(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once
    from waiting_api import create_app

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    source = TwoPageSource(item)
    assert discover_once(source, queue, receipt_root=receipt_root)['complete'] is True
    with TestClient(create_app(queue)) as client:
        first = client.get('/api/v1/local-intake/preprocessing/summary').json()
        second = client.get('/api/v1/local-intake/preprocessing/summary').json()
    assert first['waiting_count'] == second['waiting_count'] == 1
    assert first['waiting_available'] is True
    assert first['upstream_scan_completed_at'] is not None
    assert first['preprocessing_handoff_connected'] is False
    assert 'processing_count' not in first


def test_waiting_api_reports_received_count_after_upstream_scan_failure(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once
    from waiting_api import create_app

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    source = TwoPageSource(item)
    assert discover_once(source, queue, receipt_root=receipt_root)['complete'] is True
    source.fail = True
    assert discover_once(source, queue, receipt_root=receipt_root)['complete'] is False
    with TestClient(create_app(queue)) as client:
        data = client.get('/api/v1/local-intake/preprocessing/summary').json()
    assert data['waiting_available'] is True
    assert data['waiting_count'] == 1
    assert data['upstream_scan_status'] == 'incomplete'


def test_waiting_api_keeps_optional_field_shape_when_local_state_cannot_be_read():
    from waiting_api import create_app

    class BrokenQueue:
        def snapshot(self, **kwargs):
            raise sqlite3.OperationalError('synthetic failure')

    with TestClient(create_app(BrokenQueue())) as client:
        response = client.get('/api/v1/local-intake/preprocessing/summary')
    assert response.status_code == 503
    data = response.json()
    assert data['waiting_count'] is None
    assert data['waiting_available'] is False
    assert data['oldest_verified_at'] is None
    assert data['needs_review_count'] is None
