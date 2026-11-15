import json
import pytest
from unittest.mock import patch

from ingest_queue import QueueStore
from ingest_scheduler import WorkPool
from ingest_service import ensure_work_pool_worker
from test_ingest_contracts import spec
from test_ingest_model_routes import route_config


def test_explicit_one_is_not_raised_by_legacy_route(tmp_path):
    (tmp_path / 'settings.json').write_text(json.dumps({'ragflow_url': 'http://localhost:9380'}))
    path = tmp_path / 'private/education_dual_channel/routes.json'
    path.parent.mkdir(parents=True)
    value = route_config()
    value['total_limit'] = 18
    path.write_text(json.dumps(value))
    with patch('ingest_service._worker_has_lock', side_effect=[False, True]), patch('ingest_service.subprocess.Popen') as launched:
        ensure_work_pool_worker(tmp_path, tmp_path / 'queue.db', 'http://localhost:9380', parse_limit=1)
    args = launched.call_args.args[0]
    assert args[args.index('--parse-limit') + 1] == '1'


def waiting(queue, source, status='parse_waiting'):
    ident = queue.enqueue(spec(source_id=source, remote_name=source + '_aaaaaaaaaaaaaaaa.md'))
    with queue._connect() as db:
        db.execute('UPDATE tasks SET status=?,remote_id=? WHERE id=?', (status, 'remote-' + source, ident))
    return ident


def test_unconfirmed_remote_work_keeps_slot_without_router(tmp_path):
    queue = QueueStore(tmp_path / 'q.db')
    waiting(queue, 'uncertain', 'parse_unknown')
    waiting(queue, 'next')
    pool = WorkPool(queue, lambda: None, lambda *args: 'accepted', parse_limit=1)
    try:
        with patch.object(pool, '_submit') as submitted:
            pool._dispatch_parses()
        submitted.assert_not_called()
    finally:
        pool.close()


def test_external_native_work_keeps_slot_without_router(tmp_path):
    queue = QueueStore(tmp_path / 'q.db')
    waiting(queue, 'next')
    pool = WorkPool(queue, lambda: None, lambda *args: 'accepted', parse_limit=1,
                    external_counts_reader=lambda: {'total': 1})
    try:
        with patch.object(pool, '_submit') as submitted:
            pool._dispatch_parses()
        submitted.assert_not_called()
    finally:
        pool.close()


def test_free_capacity_is_actual_limit_minus_owned_and_external(tmp_path):
    queue = QueueStore(tmp_path / 'q.db')
    waiting(queue, 'owned', 'running')
    for i in range(5):
        waiting(queue, 'next' + str(i))
    pool = WorkPool(queue, lambda: None, lambda *args: 'accepted', parse_limit=4,
                    external_counts_reader=lambda: {'total': 2})
    try:
        with patch.object(pool, '_submit') as submitted:
            pool._dispatch_parses()
        assert submitted.call_count == 1
    finally:
        pool.close()


@pytest.mark.parametrize('response', [{}, {'other': 1}, None, {'total': -1}, {'total': True}])
def test_missing_or_invalid_external_inventory_never_dispatches(tmp_path, response):
    queue = QueueStore(tmp_path / 'q.db')
    waiting(queue, 'next')
    pool = WorkPool(queue, lambda: None, lambda *args: 'accepted', parse_limit=1,
                    external_counts_reader=lambda: response)
    try:
        with patch.object(pool, '_submit') as submitted:
            with pytest.raises(ValueError, match='unverified'):
                pool._dispatch_parses()
        submitted.assert_not_called()
    finally:
        pool.close()
