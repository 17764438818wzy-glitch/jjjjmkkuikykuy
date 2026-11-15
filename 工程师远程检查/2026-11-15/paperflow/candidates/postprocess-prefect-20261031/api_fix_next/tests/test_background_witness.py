import importlib.util
from pathlib import Path
from threading import Barrier, Event, Lock, Thread
from types import SimpleNamespace
import sys
import time

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def api(monkeypatch):
    path = Path(__file__).resolve().parents[1] / 'waiting_api.py'
    spec = importlib.util.spec_from_file_location('isolated_async_waiting_api', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Witness:
    def __init__(self):
        self.local_started, self.local_gate = Event(), Event()
        self.remote_started, self.remote_gate = Event(), Event()
        self.local_gate.set()
        self.remote_gate.set()
        self.local_ok = self.remote_ok = True
        self.full_observed_at = 0.0
        self.remote_observed_at = 0.0
        self.local_calls = 0
        self.lock = Lock()

    def local(self, _snapshot):
        with self.lock:
            self.local_calls += 1
        self.local_started.set()
        if not self.local_gate.wait(4):
            return False
        return self.local_ok

    def remote(self):
        self.remote_started.set()
        if not self.remote_gate.wait(4):
            return False
        return self.remote_ok


class Value(dict):
    def __init__(self, witness, version=1):
        super().__init__(count=3, version=version, as_of='2026-11-01T00:00:00Z')
        self._cache_witness = witness

    def __deepcopy__(self, _memo):
        return Value(self._cache_witness, self['version'])


class GenericValue(dict):
    def __init__(self, value, witness):
        super().__init__(value)
        self._cache_witness = witness

    def __deepcopy__(self, _memo):
        return GenericValue(dict(self), self._cache_witness)


def ready(cache, key):
    until = time.monotonic() + 3
    while time.monotonic() < until:
        with cache.lock:
            if key in cache.entries and key not in cache.inflight:
                return
        time.sleep(.005)
    pytest.fail('background full verification did not complete')


def idle(cache, kind, key):
    until = time.monotonic() + 3
    while time.monotonic() < until:
        with cache.lock:
            if key not in getattr(cache, kind):
                return
        time.sleep(.005)
    pytest.fail('background witness did not complete')


def test_overlapping_source_reads_are_unsequenced_and_do_not_wait(api):
    started, release = Event(), Event()
    class Queue:
        count = 0
        source = 'a'
        def snapshot(self, **_):
            self.count += 1
            if self.count == 1:
                started.set()
                assert release.wait(3)
            return {'source': self.source}
    queue = Queue()
    captures = api._SourceSnapshotCapture()
    older = []
    thread = Thread(target=lambda: older.append(captures.take(queue, 60)))
    thread.start()
    assert started.wait(1)
    begin = time.perf_counter()
    newer, newer_sequence = captures.take(queue, 60)
    assert time.perf_counter() - begin < .5
    assert newer['source'] == 'a' and newer_sequence is None
    queue.source = 'b'
    release.set()
    thread.join(2)
    assert not thread.is_alive()
    assert older[0][0]['source'] == 'b' and older[0][1] is not None
    current, sequence = captures.take(queue, 60)
    assert current['source'] == 'b' and sequence is not None


def test_concurrent_fast_reads_share_a_source_snapshot_and_sequence(api):
    started, release = Event(), Event()
    class Queue:
        calls = 0
        def snapshot(self, **_):
            self.calls += 1
            started.set()
            assert release.wait(1)
            return {'source': 'a', 'nested': {'value': 1}}
    queue = Queue()
    captures = api._SourceSnapshotCapture()
    captured = []
    leader = Thread(target=lambda: captured.append(captures.take(queue, 60)))
    leader.start()
    assert started.wait(1)
    followers = [Thread(target=lambda: captured.append(captures.take(queue, 60))) for _ in range(2)]
    for thread in followers: thread.start()
    time.sleep(.03)
    release.set()
    for thread in [leader, *followers]: thread.join(2)
    assert queue.calls == 1 and len(captured) == 3
    assert all(sequence == captured[0][1] and sequence is not None for _, sequence in captured)
    captured[0][0]['nested']['value'] = 2
    assert captured[1][0]['nested']['value'] == captured[2][0]['nested']['value'] == 1


def test_slow_overlapping_http_snapshot_does_not_block_other_summary(api, monkeypatch):
    monkeypatch.setitem(sys.modules, 'llm_queue_summary', SimpleNamespace(summarize_llm=lambda *_: {}))
    started, release = Event(), Event()
    class Queue:
        block_next = False
        def snapshot(self, **_):
            if self.block_next:
                self.block_next = False
                started.set()
                assert release.wait(3)
            return dict(receiver_status='running', receiver_session_id='receiver', scan_source_instance='source',
                        waiting_available=True, upstream_scan_verified=True, waiting_llm_total=1,
                        upstream_scan_completed_at='2026-11-01T00:00:00Z')
    class Observer:
        index = 'synthetic'
        def summary(self, _): return dict(source='prefect', scope='all_tasks', available=True, counts={})
        def preprocessing_results(self, *_): return {'available': False}
        def llm_results(self, *_): return {'available': False}
    def stage(_):
        return dict(available=True, basis='folder-stage-receipts/1', as_of='2026-11-01T00:00:00Z',
                    counts=dict(waiting_postprocessing=1, postprocessing_in_progress=0, waiting_rag_upload=0))
    queue = Queue()
    app = api.create_app(queue, ticket_observer=Observer(), stage_projection=stage)
    endpoint = '/api/v1/local-intake/preprocessing/summary'
    results = []
    with TestClient(app) as client:
        queue.block_next = True
        thread = Thread(target=lambda: results.append(client.get(endpoint).json()))
        thread.start()
        assert started.wait(1)
        begin = time.perf_counter()
        response = client.get(endpoint).json()
        assert time.perf_counter() - begin < .8
        assert response['prefect_ticket_summary'].get('postprocessing_basis') is None
        assert response['postprocessing_results']['available'] is False
        release.set()
        thread.join(2)
        assert not thread.is_alive()
        # The leader may bind its own complete snapshot; only the timed-out
        # follower is ineligible to rebind the source.


def test_multiple_expanded_ui_lists_can_refresh_together_over_many_rounds(api, monkeypatch):
    monkeypatch.setitem(sys.modules, 'llm_queue_summary', SimpleNamespace(summarize_llm=lambda *_: {}))
    class Queue:
        def snapshot(self, **_):
            time.sleep(.03)
            return dict(receiver_status='running', receiver_session_id='receiver', scan_source_instance='source',
                        waiting_available=True, upstream_scan_verified=True, waiting_llm_total=1,
                        upstream_scan_completed_at='2026-11-01T00:00:00Z')
    class Observer:
        index = 'synthetic'
        def summary(self, _): return dict(source='prefect', scope='all_tasks', available=True, counts={})
        def preprocessing_results(self, *_): return {'available': False}
        def llm_results(self, *_): return {'available': False}
    def stage(_):
        return dict(available=True, basis='folder-stage-receipts/1', as_of='2026-11-01T00:00:00Z',
                    counts=dict(waiting_postprocessing=1, postprocessing_in_progress=0, waiting_rag_upload=0))
    def results(result_filter, page, page_size):
        value = dict(available=True, as_of='2026-11-01T00:00:00Z', partial=False,
                     counts=dict(passed=1, failed=0, unrecognized=0))
        if result_filter:
            value['list'] = dict(filter=result_filter, page=page, page_size=page_size, total=1, items=[{}])
        return value
    app = api.create_app(Queue(), ticket_observer=Observer(), stage_projection=stage, results_projection=results)
    endpoint = '/api/v1/local-intake/preprocessing/summary'
    urls = [endpoint + '?result_filter=passed', endpoint + '?llm_result_filter=failed',
            endpoint + '?postprocessing_result_filter=passed']
    with TestClient(app) as client:
        for cycle in range(5):
            start = Barrier(4)
            outputs = [None, None, None]
            def fetch(index):
                start.wait()
                outputs[index] = client.get(urls[index]).json()
            threads = [Thread(target=fetch, args=(index,)) for index in range(3)]
            for thread in threads: thread.start()
            start.wait()
            for thread in threads: thread.join(3)
            assert all(not thread.is_alive() for thread in threads)
            if cycle >= 2:
                assert all(value['prefect_ticket_summary'].get('postprocessing_basis') for value in outputs)
                assert all(value['postprocessing_results']['available'] for value in outputs)
                assert outputs[2]['postprocessing_results']['list']['filter'] == 'passed'
            time.sleep(.04)


def test_older_request_cannot_rebind_or_clear_newer_source(api, monkeypatch):
    monkeypatch.setattr(api, 'projection_clock', lambda: 0.0)
    cache = api._ProjectionCache(max_inflight=1)
    first = cache.bind_source(('source-a',), 1)
    second = cache.bind_source(('source-b',), 2)
    calls = []
    key = (('source-b',),)
    cache.get(key, lambda: calls.append(1) or Value(Witness()), snapshot={}, expected_epoch=second)
    ready(cache, key)
    assert cache.bind_source(('source-a',), 1) is None
    assert cache.source == ('source-b',)
    assert cache.get(key, lambda: None, snapshot={}, expected_epoch=second)['count'] == 3
    assert cache.get((('source-a',),), lambda: calls.append(2),
                     snapshot={}, expected_epoch=first) is None
    assert calls == [1]


def test_late_local_failure_cannot_evict_new_full_verification(api, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    old, new = Witness(), Witness()
    new.full_observed_at = new.remote_observed_at = 15.0
    cache, key = api._ProjectionCache(max_inflight=1), ('one',)
    calls = []

    def compute():
        calls.append(1)
        return Value(old if len(calls) == 1 else new, version=len(calls))

    cache.get(key, compute, snapshot={})
    ready(cache, key)
    old.local_started.clear()
    old.local_gate.clear()
    old.local_ok = False
    clock[0] = 3.0
    cache.get(key, compute, snapshot={})
    assert old.local_started.wait(1)
    clock[0] = 15.0
    cache.get(key, compute, snapshot={})
    ready(cache, key)
    old.local_gate.set()
    idle(cache, 'local_inflight', key)
    assert cache.get(key, compute, snapshot={})['version'] == 2


def test_slow_local_witness_never_blocks_http_path_and_then_expires(api, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    w = Witness()
    cache = api._ProjectionCache(max_inflight=1)
    key = ('same',)
    compute = lambda: Value(w)
    assert cache.get(key, compute, snapshot={}) is None
    ready(cache, key)
    w.local_gate.clear()
    w.local_started.clear()
    clock[0] = 3.0
    started = time.perf_counter()
    assert cache.get(key, compute, snapshot={})['count'] == 3
    assert time.perf_counter() - started < .5
    assert w.local_started.wait(1)
    clock[0] = 9.0
    started = time.perf_counter()
    assert cache.get(key, compute, snapshot={}) is None
    assert time.perf_counter() - started < .5
    w.local_gate.set()
    idle(cache, 'local_inflight', key)


def test_detected_local_change_invalidates_and_does_not_refresh_as_of(api, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    w, cache, key = Witness(), api._ProjectionCache(max_inflight=1), ('one',)
    gate, calls = Event(), []
    def compute():
        calls.append(True)
        if len(calls) > 1:
            gate.wait(3)
        return Value(w)
    cache.get(key, compute, snapshot={})
    ready(cache, key)
    w.local_ok = False
    w.local_started.clear()
    clock[0] = 3.0
    cache.get(key, compute, snapshot={})
    assert w.local_started.wait(1)
    idle(cache, 'local_inflight', key)
    assert cache.get(key, compute, snapshot={}) is None
    gate.set()


def test_slow_successful_remote_witness_has_bounded_observation_age(api, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    monkeypatch.setattr(api, 'GUARDED_PROJECTION_REFRESH_SECONDS', 100)
    w, cache, key = Witness(), api._ProjectionCache(max_inflight=1), ('one',)
    compute = lambda: Value(w)
    cache.get(key, compute, snapshot={})
    ready(cache, key)
    for start, finish in [(5.0, 11.0), (15.0, 21.0), (25.0, 31.0)]:
        w.remote_gate.clear()
        w.remote_started.clear()
        clock[0] = start
        assert cache.get(key, compute, snapshot={})['count'] == 3
        assert w.remote_started.wait(1)
        idle(cache, 'local_inflight', key)
        clock[0] = start + 5
        assert cache.get(key, compute, snapshot={})['count'] == 3
        idle(cache, 'local_inflight', key)
        clock[0] = finish
        w.remote_gate.set()
        idle(cache, 'remote_inflight', key)
        with cache.lock:
            assert cache.entries[key]['remote_at'] == start


def test_remote_result_after_observation_deadline_cannot_revive_old_state(api, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    w, cache, key = Witness(), api._ProjectionCache(max_inflight=1), ('one',)
    compute = lambda: Value(w)
    cache.get(key, compute, snapshot={})
    ready(cache, key)
    w.remote_gate.clear()
    clock[0] = 3.0
    cache.get(key, compute, snapshot={})
    assert w.remote_started.wait(1)
    clock[0] = 19.0
    w.remote_gate.set()
    idle(cache, 'remote_inflight', key)
    with cache.lock:
        assert cache.entries[key]['remote_at'] == 3.0
    assert cache.get(key, compute, snapshot={}) is None


def test_full_completion_does_not_reset_old_remote_observation_time(api, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    w, cache, key = Witness(), api._ProjectionCache(max_inflight=1), ('one',)
    started, finish = Event(), Event()
    def compute():
        started.set()
        assert finish.wait(3)
        return Value(w)
    cache.get(key, compute, snapshot={})
    assert started.wait(1)
    clock[0] = 16.0
    finish.set()
    ready(cache, key)
    with cache.lock:
        assert cache.entries[key]['at'] == 0.0
        assert cache.entries[key]['remote_at'] == 0.0
    assert cache.get(key, compute, snapshot={}) is None


def test_full_hash_older_than_limit_is_not_fresh_at_completion(api, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    w, cache, key = Witness(), api._ProjectionCache(max_inflight=1), ('one',)
    started, finish = Event(), Event()
    def compute():
        started.set()
        assert finish.wait(3)
        return Value(w)
    cache.get(key, compute, snapshot={})
    assert started.wait(1)
    clock[0] = 46.0
    w.remote_observed_at = 45.0
    finish.set()
    ready(cache, key)
    with cache.lock:
        assert cache.entries[key]['at'] == 0.0
    assert cache.get(key, compute, snapshot={}) is None


def test_full_refresh_local_witness_uses_its_observation_start(api, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    w, cache, key = Witness(), api._ProjectionCache(max_inflight=1), ('one',)
    def slow_local(_snapshot):
        clock[0] = 9.0
        return True
    w.local = slow_local
    cache.get(key, lambda: Value(w), snapshot={})
    ready(cache, key)
    with cache.lock:
        assert cache.entries[key]['local_at'] == 0.0
    assert cache.get(key, lambda: Value(w), snapshot={}) is None


def test_multiple_requests_share_one_local_witness_per_key(api, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    w, cache, key = Witness(), api._ProjectionCache(max_inflight=1), ('one',)
    compute = lambda: Value(w)
    cache.get(key, compute, snapshot={})
    ready(cache, key)
    w.local_gate.clear()
    w.local_started.clear()
    before = w.local_calls
    clock[0] = 3.0
    results = []
    threads = [Thread(target=lambda: results.append(cache.get(key, compute, snapshot={}))) for _ in range(20)]
    start = time.perf_counter()
    for thread in threads: thread.start()
    for thread in threads: thread.join(.7)
    assert all(not thread.is_alive() for thread in threads)
    assert time.perf_counter() - start < 1
    assert w.local_started.wait(1)
    assert w.local_calls - before == 1
    assert len(results) == 20 and all(value['count'] == 3 for value in results)
    w.local_gate.set()
    idle(cache, 'local_inflight', key)


def test_stage_and_results_slow_witnesses_do_not_block_http(api, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    monkeypatch.setitem(sys.modules, 'llm_queue_summary', SimpleNamespace(summarize_llm=lambda *_: {}))
    class Queue:
        def snapshot(self, **_):
            return dict(receiver_status='running', receiver_session_id='receiver', scan_source_instance='source',
                        waiting_available=True, upstream_scan_verified=True, waiting_llm_total=1,
                        upstream_scan_completed_at='2026-11-01T00:00:00Z')
    class Observer:
        index = 'synthetic'
        def summary(self, _): return dict(source='prefect', scope='all_tasks', available=True, counts={})
        def preprocessing_results(self, *_): return {'available': False}
        def llm_results(self, *_): return {'available': False}
    stage_witness, result_witness = Witness(), Witness()
    def stage(_):
        return GenericValue(dict(available=True, basis='folder-stage-receipts/1',
            as_of='2026-11-01T00:00:00Z', counts=dict(waiting_postprocessing=1,
            postprocessing_in_progress=0, waiting_rag_upload=0)), stage_witness)
    def results(*_):
        return GenericValue(dict(available=True, as_of='2026-11-01T00:00:00Z', partial=False,
            counts=dict(passed=0, failed=0, unrecognized=None)), result_witness)
    app = api.create_app(Queue(), ticket_observer=Observer(), stage_projection=stage, results_projection=results)
    endpoint = '/api/v1/local-intake/preprocessing/summary'
    with TestClient(app) as client:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            data = client.get(endpoint).json()
            if data['prefect_ticket_summary'].get('postprocessing_basis') and data['postprocessing_results']['available']:
                break
            time.sleep(.01)
        else:
            pytest.fail('synthetic guarded stage and results not available')
        for witness in (stage_witness, result_witness):
            witness.local_started.clear()
            witness.local_gate.clear()
        clock[0] = 3.0
        started = time.perf_counter()
        data = client.get(endpoint).json()
        assert time.perf_counter() - started < .8
        assert data['prefect_ticket_summary'].get('postprocessing_basis')
        assert data['postprocessing_results']['available']
        assert stage_witness.local_started.wait(1) and result_witness.local_started.wait(1)
        clock[0] = 9.0
        started = time.perf_counter()
        data = client.get(endpoint).json()
        assert time.perf_counter() - started < .8
        assert data['prefect_ticket_summary'].get('postprocessing_basis') is None
        assert data['postprocessing_results']['available'] is False
        stage_witness.local_gate.set()
        result_witness.local_gate.set()


def test_scan_transition_fences_old_inflight_verification(api, monkeypatch):
    monkeypatch.setitem(sys.modules, 'llm_queue_summary', SimpleNamespace(summarize_llm=lambda *_: {}))
    class Queue:
        state = dict(receiver_status='running', receiver_session_id='receiver', scan_source_instance='source',
                     waiting_available=True, upstream_scan_verified=True,
                     upstream_scan_completed_at='2026-11-01T00:00:00Z', waiting_llm_total=1)
        def snapshot(self, **_): return dict(self.state)
    class Observer:
        index = 'synthetic'
        def summary(self, _): return dict(source='prefect', scope='all_tasks', available=True, counts={})
        def preprocessing_results(self, *_): return {'available': False}
        def llm_results(self, *_): return {'available': False}
    started, release = Event(), Event()
    calls = []
    def stage(_snapshot):
        calls.append(True)
        number = len(calls)
        if number == 1:
            started.set()
            assert release.wait(3)
        return dict(available=True, basis='folder-stage-receipts/1',
                    as_of='2026-11-01T00:00:00Z', counts=dict(waiting_postprocessing=number,
                    postprocessing_in_progress=0, waiting_rag_upload=0))
    queue = Queue()
    app = api.create_app(queue, ticket_observer=Observer(), stage_projection=stage)
    endpoint = '/api/v1/local-intake/preprocessing/summary'
    with TestClient(app) as client:
        client.get(endpoint)
        assert started.wait(1)
        queue.state.update(waiting_available=False, upstream_scan_verified=False)
        assert client.get(endpoint).json()['prefect_ticket_summary'].get('postprocessing_basis') is None
        queue.state.update(waiting_available=True, upstream_scan_verified=True)
        assert client.get(endpoint).json()['prefect_ticket_summary'].get('postprocessing_basis') is None
        release.set()
        deadline = time.monotonic() + 3
        values = []
        while time.monotonic() < deadline:
            p = client.get(endpoint).json()['prefect_ticket_summary']
            if p.get('postprocessing_basis'):
                values.append(p['counts']['waiting_postprocessing'])
                if len(calls) >= 2:
                    break
            time.sleep(.01)
    assert calls and len(calls) >= 2
    assert values and all(value == 2 for value in values)


def test_delayed_http_request_does_not_clear_newer_scan_cache(api, monkeypatch):
    monkeypatch.setitem(sys.modules, 'llm_queue_summary', SimpleNamespace(summarize_llm=lambda *_: {}))
    class Queue:
        state = dict(receiver_status='running', receiver_session_id='receiver', scan_source_instance='source',
                     waiting_available=True, upstream_scan_verified=True, waiting_llm_total=1,
                     upstream_scan_completed_at='2026-11-01T00:00:00Z')
        def snapshot(self, **_): return dict(self.state)
    class Observer:
        index = 'synthetic'
        block_next = False
        def summary(self, _): return dict(source='prefect', scope='all_tasks', available=True, counts={})
        def preprocessing_results(self, *_):
            if self.block_next:
                self.block_next = False
                blocked.set()
                assert release.wait(3)
            return {'available': False}
        def llm_results(self, *_): return {'available': False}
    def stage(snapshot):
        number = 1 if snapshot['upstream_scan_completed_at'].endswith('00Z') else 2
        return dict(available=True, basis='folder-stage-receipts/1',
                    as_of='2026-11-01T00:00:00Z', counts=dict(waiting_postprocessing=number,
                    postprocessing_in_progress=0, waiting_rag_upload=0))
    def await_count(client, expected):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            result = client.get(endpoint).json()['prefect_ticket_summary']
            if result.get('postprocessing_basis') and result['counts']['waiting_postprocessing'] == expected:
                return
            time.sleep(.01)
        pytest.fail('synthetic scan count unavailable')
    queue, observer, blocked, release = Queue(), Observer(), Event(), Event()
    app = api.create_app(queue, ticket_observer=observer, stage_projection=stage)
    endpoint = '/api/v1/local-intake/preprocessing/summary'
    old_responses = []
    with TestClient(app) as client:
        await_count(client, 1)
        observer.block_next = True
        thread = Thread(target=lambda: old_responses.append(client.get(endpoint).json()))
        thread.start()
        assert blocked.wait(1)
        queue.state['upstream_scan_completed_at'] = '2026-11-01T00:00:01Z'
        await_count(client, 2)
        release.set()
        thread.join(2)
        assert not thread.is_alive()
        assert old_responses[0]['prefect_ticket_summary'].get('postprocessing_basis') is None
        await_count(client, 2)


def test_completed_scan_between_polls_changes_source_generation(api):
    base = dict(receiver_status='running', receiver_session_id='receiver', scan_source_instance='source',
                waiting_available=True, upstream_scan_verified=True, waiting_llm_total=1,
                upstream_scan_completed_at='2026-11-01T00:00:00Z')
    newer = dict(base, upstream_scan_completed_at='2026-11-01T00:00:01Z')
    assert api._usable_source(base)
    assert api._snapshot_key(base) != api._snapshot_key(newer)
    assert not api._usable_source({key: value for key, value in base.items()
                                   if key != 'upstream_scan_completed_at'})


def test_35_second_refresh_shows_gap_after_full_hash_deadline(api, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    first, second = Witness(), Witness()
    cache, key, release, refreshing = api._ProjectionCache(max_inflight=1), ('one',), Event(), Event()
    third_release = Event()
    calls = []
    def compute():
        calls.append(True)
        if len(calls) == 1:
            return Value(first, version=1)
        if len(calls) > 2:
            assert third_release.wait(3)
            return Value(second, version=2)
        refreshing.set()
        assert release.wait(3)
        second.full_observed_at = 15.0
        second.remote_observed_at = 44.0
        return Value(second, version=2)
    cache.get(key, compute, snapshot={})
    ready(cache, key)
    for point in range(5, 50, 5):
        clock[0] = float(point)
        assert cache.get(key, compute, snapshot={})['version'] == 1
        idle(cache, 'local_inflight', key)
        idle(cache, 'remote_inflight', key)
        if point == 15:
            assert refreshing.wait(1)
    clock[0] = 50.0
    assert cache.get(key, compute, snapshot={}) is None
    release.set()
    ready(cache, key)
    assert cache.get(key, compute, snapshot={})['version'] == 2
    third_release.set()


def test_35_second_verification_cannot_publish_across_30_second_scan_window(api, monkeypatch):
    monkeypatch.setitem(sys.modules, 'llm_queue_summary', SimpleNamespace(summarize_llm=lambda *_: {}))
    class Queue:
        state = dict(receiver_status='running', receiver_session_id='receiver', scan_source_instance='source',
                     waiting_available=True, upstream_scan_verified=True, waiting_llm_total=1,
                     upstream_scan_completed_at='2026-11-01T00:00:00Z')
        def snapshot(self, **_): return dict(self.state)
    class Observer:
        index = 'synthetic'
        def summary(self, _): return dict(source='prefect', scope='all_tasks', available=True, counts={})
        def preprocessing_results(self, *_): return {'available': False}
        def llm_results(self, *_): return {'available': False}
    releases, starts = [Event(), Event()], [Event(), Event()]
    calls = []
    def stage(_snapshot):
        index = len(calls)
        calls.append(index)
        if index < 2:
            starts[index].set()
            assert releases[index].wait(3)
        return dict(available=True, basis='folder-stage-receipts/1', as_of='2026-11-01T00:00:00Z',
                    counts=dict(waiting_postprocessing=1, postprocessing_in_progress=0, waiting_rag_upload=0))
    queue = Queue()
    app = api.create_app(queue, ticket_observer=Observer(), stage_projection=stage)
    endpoint = '/api/v1/local-intake/preprocessing/summary'
    with TestClient(app) as client:
        # Each callback models a 35-second check; the following source scan
        # begins after only 30 seconds and remains incomplete for 10 seconds.
        for point in range(0, 81, 5):
            if point in (30, 70):
                queue.state.update(waiting_available=False, upstream_scan_verified=False)
            if point == 40:
                queue.state.update(waiting_available=True, upstream_scan_verified=True,
                                   upstream_scan_completed_at='2026-11-01T00:00:40Z')
            if point == 80:
                queue.state.update(waiting_available=True, upstream_scan_verified=True,
                                   upstream_scan_completed_at='2026-11-01T00:01:20Z')
            if point in (35, 75):
                releases[(point - 35) // 40].set()
            data = client.get(endpoint).json()
            assert data['prefect_ticket_summary'].get('postprocessing_basis') is None
            if point in (0, 40):
                assert starts[point // 40].wait(1)
            time.sleep(.01)
