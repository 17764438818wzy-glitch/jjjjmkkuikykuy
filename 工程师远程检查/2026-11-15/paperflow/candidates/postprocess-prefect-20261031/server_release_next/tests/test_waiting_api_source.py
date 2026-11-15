"""Only synthetic source, Prefect and server evidence are used."""
import importlib.util
from pathlib import Path
import sys
import threading
import time

from fastapi.testclient import TestClient

HERE = Path(__file__).resolve().parents[1]
for path in (HERE.parents[1] / 'postprocess-server-20261101', HERE.parent,
             HERE.parent / 'runtime_next', HERE.parent / 'results_next'):
    sys.path.insert(0, str(path))
PREVIOUS = HERE.parent / 'remote_projection_next' / 'tests' / 'test_cache_order.py'
BASE = '/api/v1/local-intake/preprocessing/summary'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_registered_evidence_becomes_unknown_when_source_is_lost():
    fixtures = load('synthetic_previous_server_projection', PREVIOUS)
    api = load('synthetic_bound_source_api', HERE / 'waiting_api.py')
    class Queue:
        available = True
        def snapshot(self, **_):
            return {'receiver_status': 'running' if self.available else 'unknown',
                    'receiver_session_id': 'synthetic', 'scan_source_instance': 'synthetic',
                    'waiting_available': self.available, 'upstream_scan_verified': self.available,
                    'upstream_scan_completed_at': '2026-11-01T00:00:00Z', 'waiting_llm_total': 0}
    class Observer:
        def summary(self, _):
            return {'source': 'prefect', 'scope': 'all_tasks', 'available': True, 'counts': {}}
        def preprocessing_results(self, *_): return {'available': True, 'counts': {}}
        def llm_results(self, *_): return {'available': True, 'counts': {}}
    queue = Queue()
    with TestClient(api.create_app(queue, ticket_observer=Observer(), registered_projection=fixtures.Projection())) as client:
        limit = time.monotonic() + 3
        while time.monotonic() < limit:
            ledger = client.get(BASE).json()['postprocessing_registered_ledger']
            if ledger.get('server_observation', {}).get('available') is True:
                break
            time.sleep(.01)
        else:
            raise AssertionError('synthetic server evidence did not warm')
        queue.available = False
        response = client.get(BASE).json()
        assert response['receiver_status'] == 'unknown'
        ledger = response['postprocessing_registered_ledger']
        assert ledger['available'] is False or ledger.get('server_observation', {}).get('available') is False


def test_late_server_success_cannot_restore_a_lost_source():
    fixtures = load('synthetic_delayed_server_projection', PREVIOUS)
    api = load('synthetic_delayed_source_api', HERE / 'waiting_api.py')
    started, release = threading.Event(), threading.Event()
    class Queue:
        available = True
        def snapshot(self, **_):
            return {'receiver_status': 'running' if self.available else 'unknown',
                    'receiver_session_id': 'synthetic', 'scan_source_instance': 'synthetic',
                    'waiting_available': self.available, 'upstream_scan_verified': self.available,
                    'upstream_scan_completed_at': '2026-11-01T00:00:00Z', 'waiting_llm_total': 0}
    class Observer:
        def summary(self, _): return {'source': 'prefect', 'scope': 'all_tasks', 'available': True, 'counts': {}}
        def preprocessing_results(self, *_): return {'available': True, 'counts': {}}
        def llm_results(self, *_): return {'available': True, 'counts': {}}
    original = fixtures.Projection()
    def projection(**kwargs):
        started.set()
        assert release.wait(3)
        return original(**kwargs)
    queue = Queue()
    try:
        with TestClient(api.create_app(queue, ticket_observer=Observer(), registered_projection=projection)) as client:
            assert client.get(BASE).json()['postprocessing_registered_ledger']['available'] is False
            assert started.wait(1)
            queue.available = False
            assert client.get(BASE).json()['postprocessing_registered_ledger']['available'] is False
            release.set()
            time.sleep(.05)
            assert client.get(BASE).json()['postprocessing_registered_ledger']['available'] is False
    finally:
        release.set()


def test_inner_cache_passes_its_earliest_deadline_to_the_outer_adapter():
    fixtures = load('synthetic_deadline_projection', PREVIOUS)
    api = load('synthetic_deadline_source_api', HERE / 'waiting_api.py')
    api.PROJECTION_MAX_AGE_SECONDS = .5
    class Queue:
        def snapshot(self, **_):
            return {'receiver_status': 'running', 'receiver_session_id': 'synthetic',
                    'scan_source_instance': 'synthetic', 'waiting_available': True,
                    'upstream_scan_verified': True, 'upstream_scan_completed_at': '2026-11-01T00:00:00Z',
                    'waiting_llm_total': 0}
    class Observer:
        def summary(self, _): return {'source': 'prefect', 'scope': 'all_tasks', 'available': True, 'counts': {}}
        def preprocessing_results(self, *_): return {'available': True, 'counts': {}}
        def llm_results(self, *_): return {'available': True, 'counts': {}}
    with TestClient(api.create_app(Queue(), ticket_observer=Observer(), registered_projection=fixtures.Projection())) as client:
        limit = time.monotonic() + 3
        while time.monotonic() < limit:
            response = client.get(BASE)
            if response.json()['postprocessing_registered_ledger'].get('server_observation', {}).get('available') is True:
                break
            time.sleep(.01)
        else:
            raise AssertionError('synthetic server evidence did not warm')
        remaining = float(response.headers['x-paperflow-observation-deadline']) - time.monotonic()
        assert 0 < remaining <= .5


def test_completed_partial_snapshot_replaces_a_later_pending_poll():
    api = load('synthetic_pending_order_api', HERE / 'waiting_api.py')
    server = load('synthetic_pending_order_server', HERE / 'server_projection.py')
    api.PROJECTION_REFRESH_SECONDS = .01
    cache = api._ProjectionCache(invalidate_server_failures=True)
    epoch = cache.bind_source('synthetic-source', 1)
    started = time.monotonic()
    time.sleep(.01)
    pending_at = time.monotonic()
    pending = server.Observation({'available': True, 'server_observation': {
        'available': False, 'source_revision': 'a' * 64}}, pending_at, 30)
    pending._server_cache_order_monotonic = pending_at
    cache.get('summary', lambda: pending, expected_epoch=epoch)
    deadline = time.monotonic() + 2
    while cache.inflight and time.monotonic() < deadline:
        time.sleep(.01)
    assert cache.entries['summary']['value']['server_observation']['available'] is False
    time.sleep(.02)
    completed = server.Observation({'available': True, 'server_observation': {
        'available': True, 'source_revision': 'a' * 64, 'observation_revision': 'b' * 64}}, started, 30)
    completed._server_cache_order_monotonic = time.monotonic()
    cache.get('summary', lambda: completed, expected_epoch=epoch)
    deadline = time.monotonic() + 2
    while cache.inflight and time.monotonic() < deadline:
        time.sleep(.01)
    assert cache.entries['summary']['value']['server_observation']['available'] is True


def test_newer_partial_failure_invalidates_and_cannot_be_overwritten_by_older_success():
    api = load('synthetic_partial_failure_order_api', HERE / 'waiting_api.py')
    server = load('synthetic_partial_failure_order_server', HERE / 'server_projection.py')
    api.PROJECTION_REFRESH_SECONDS = .01
    cache = api._ProjectionCache(invalidate_server_failures=True)
    epoch = cache.bind_source('synthetic-source', 1)
    started = time.monotonic() - 2
    good = server.Observation({'available': True, 'server_observation': {
        'available': True, 'source_revision': 'a' * 64, 'observation_revision': 'b' * 64}}, started, 30)
    good._server_cache_order_monotonic = time.monotonic()
    cache.get('passed-list', lambda: good, expected_epoch=epoch)
    deadline = time.monotonic() + 2
    while cache.inflight and time.monotonic() < deadline:
        time.sleep(.01)
    assert cache.entries['passed-list']['value']['server_observation']['available'] is True
    started_old, release_old = threading.Event(), threading.Event()
    old = server.Observation(dict(good), started, 30)
    old._server_cache_order_monotonic = time.monotonic()
    def delayed_old():
        started_old.set()
        assert release_old.wait(timeout=2)
        return old
    try:
        cache.get('older-list', delayed_old, expected_epoch=epoch)
        assert started_old.wait(timeout=2)
        failure = server.Observation({'available': True, 'server_observation': {
            'available': False, 'source_revision': 'a' * 64}}, started - 1, 30)
        failure._server_cache_order_monotonic = time.monotonic()
        cache.get('summary', lambda: failure, expected_epoch=epoch)
        deadline = time.monotonic() + 2
        while 'summary' in cache.inflight and time.monotonic() < deadline:
            time.sleep(.01)
        assert 'passed-list' not in cache.entries
        assert cache.entries['summary']['value']['server_observation']['available'] is False
        release_old.set()
        deadline = time.monotonic() + 2
        while 'older-list' in cache.inflight and time.monotonic() < deadline:
            time.sleep(.01)
        assert 'older-list' not in cache.entries
    finally:
        release_old.set()


def test_partial_cache_order_does_not_extend_receipt_freshness():
    api = load('synthetic_partial_expiry_api', HERE / 'waiting_api.py')
    server = load('synthetic_partial_expiry_server', HERE / 'server_projection.py')
    as_of = '2026-11-01T00:00:00Z'
    revision = 'a' * 64
    value = server.Observation({'available': True, 'revision': revision, 'as_of': as_of,
        'stage': {'counts': {'waiting_rag_upload': None}},
        'results': {'counts': {'passed': 1}, 'partial': True},
        'server_observation': {'available': True, 'basis': 'paperflow-server-receipt-observation/1',
            'observation_revision': revision, 'as_of': as_of, 'waiting_rag_upload': None}},
        time.monotonic() - 31, 30)
    value._server_cache_order_monotonic = time.monotonic()
    result = api._normalize_server_observation(value)
    assert result['results']['counts']['passed'] is None
    assert result['server_observation']['available'] is False


def test_unmarked_late_failure_invalidates_pass_and_older_inflight_success():
    api = load('synthetic_unmarked_failure_api', HERE / 'waiting_api.py')
    server = load('synthetic_unmarked_failure_server', HERE / 'server_projection.py')
    for failure_kind in ('exception', 'unavailable'):
        cache = api._ProjectionCache(invalidate_server_failures=True)
        epoch = cache.bind_source('synthetic-source', 1)
        failure_started, release_failure = threading.Event(), threading.Event()
        success_started, release_success = threading.Event(), threading.Event()
        def delayed_failure():
            failure_started.set()
            assert release_failure.wait(timeout=3)
            if failure_kind == 'exception':
                raise ValueError('synthetic projection failed after starting')
            return api._unavailable_registered('ledger_unavailable')
        try:
            cache.get('older-failure', delayed_failure, expected_epoch=epoch)
            assert failure_started.wait(timeout=2)
            started = time.monotonic() - 1
            good = server.Observation({'available': True, 'server_observation': {
                'available': True, 'source_revision': 'a' * 64, 'observation_revision': 'b' * 64}}, started, 30)
            good._server_cache_order_monotonic = time.monotonic()
            cache.get('passed-list', lambda: good, expected_epoch=epoch)
            deadline = time.monotonic() + 2
            while 'passed-list' in cache.inflight and time.monotonic() < deadline:
                time.sleep(.01)
            assert cache.entries['passed-list']['value']['server_observation']['available'] is True
            old = server.Observation(dict(good), started, 30)
            old._server_cache_order_monotonic = time.monotonic()
            def delayed_old_success():
                success_started.set()
                assert release_success.wait(timeout=3)
                return old
            cache.get('inflight-success', delayed_old_success, expected_epoch=epoch)
            assert success_started.wait(timeout=2)
            release_failure.set()
            deadline = time.monotonic() + 2
            while 'older-failure' in cache.inflight and time.monotonic() < deadline:
                time.sleep(.01)
            assert 'passed-list' not in cache.entries
            assert cache.latest_server_state == ('unavailable',)
            release_success.set()
            deadline = time.monotonic() + 2
            while 'inflight-success' in cache.inflight and time.monotonic() < deadline:
                time.sleep(.01)
            assert 'inflight-success' not in cache.entries
        finally:
            release_failure.set()
            release_success.set()


def test_late_failure_invalidates_pass_even_if_its_old_cache_entry_was_evicted():
    api = load('synthetic_evicted_failure_api', HERE / 'waiting_api.py')
    server = load('synthetic_evicted_failure_server', HERE / 'server_projection.py')
    api.PROJECTION_REFRESH_SECONDS = .01
    cache = api._ProjectionCache(invalidate_server_failures=True)
    epoch = cache.bind_source('synthetic-source', 1)
    pending_at = time.monotonic()
    pending = server.Observation({'available': True, 'server_observation': {
        'available': False, 'source_revision': 'a' * 64}}, pending_at, 30)
    pending._server_cache_order_monotonic = pending_at
    cache.get('older-filter', lambda: pending, expected_epoch=epoch)
    deadline = time.monotonic() + 2
    while cache.inflight and time.monotonic() < deadline:
        time.sleep(.01)
    assert 'older-filter' in cache.entries
    time.sleep(.02)
    entered, release = threading.Event(), threading.Event()
    def delayed_failure():
        entered.set()
        assert release.wait(timeout=3)
        raise ValueError('synthetic projection failed after cache eviction')
    try:
        cache.get('older-filter', delayed_failure, expected_epoch=epoch)
        assert entered.wait(timeout=2)
        good_at = time.monotonic()
        good = server.Observation({'available': True, 'server_observation': {
            'available': True, 'source_revision': 'a' * 64, 'observation_revision': 'b' * 64}}, good_at, 30)
        good._server_cache_order_monotonic = good_at
        cache.get('newer-filter', lambda: good, expected_epoch=epoch)
        deadline = time.monotonic() + 2
        while 'newer-filter' in cache.inflight and time.monotonic() < deadline:
            time.sleep(.01)
        assert 'older-filter' not in cache.entries
        assert cache.entries['newer-filter']['value']['server_observation']['available'] is True
        release.set()
        deadline = time.monotonic() + 2
        while 'older-filter' in cache.inflight and time.monotonic() < deadline:
            time.sleep(.01)
        assert 'newer-filter' not in cache.entries
        assert cache.latest_server_state == ('unavailable',)
    finally:
        release.set()


def test_inner_server_failure_clears_other_filter_from_outer_http_cache():
    fixtures = load('synthetic_cross_layer_projection', PREVIOUS)
    api = load('synthetic_cross_layer_api', HERE / 'waiting_api.py')
    bounded = load('synthetic_cross_layer_bounded', HERE / 'bounded_summary.py')
    api.PROJECTION_REFRESH_SECONDS = .03
    class Queue:
        def snapshot(self, **_):
            return {'receiver_status': 'running', 'receiver_session_id': 'synthetic',
                    'scan_source_instance': 'synthetic', 'waiting_available': True,
                    'upstream_scan_verified': True, 'upstream_scan_completed_at': '2026-11-01T00:00:00Z',
                    'waiting_llm_total': 0}
    class Observer:
        def summary(self, _): return {'source': 'prefect', 'scope': 'all_tasks', 'available': True, 'counts': {}}
    projection = fixtures.Projection()
    app = api.create_app(Queue(), ticket_observer=Observer(), registered_projection=projection,
                         postprocess_only=True, registered_revision=lambda: 'same-control-revision')
    witness = lambda: ('same-control-revision', app.state.postprocessing_cache_revision())
    passed = {'postprocessing_result_filter': 'passed'}
    try:
        with TestClient(bounded.BoundedSummary(app, max_age_seconds=2, revision_witness=witness)) as client:
            for params in ({}, passed):
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    response = client.get(BASE, params=params)
                    if (response.status_code == 200 and response.json().get('postprocessing_registered_ledger', {})
                            .get('server_observation', {}).get('available') is True):
                        break
                    time.sleep(.02)
                else:
                    raise AssertionError('synthetic verified cache did not warm')
            old_revision = app.state.postprocessing_cache_revision()
            projection.hold_bad_list = True
            projection.online = False
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                client.get(BASE)
                if projection.summary_failed.is_set() and app.state.postprocessing_cache_revision() != old_revision:
                    break
                time.sleep(.02)
            else:
                raise AssertionError('synthetic inner failure did not commit')
            response = client.get(BASE, params=passed)
            if response.status_code == 200:
                assert response.json()['postprocessing_registered_ledger'].get('server_observation', {}).get('available') is not True
            else:
                assert response.status_code == 503
    finally:
        projection.release_old.set()
        projection.release_bad.set()
