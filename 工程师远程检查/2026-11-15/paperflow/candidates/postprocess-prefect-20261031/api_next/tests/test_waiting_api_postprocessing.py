import importlib.util
from contextlib import nullcontext
from pathlib import Path
import sqlite3
import sys
from threading import Event, Lock, current_thread
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient


API_PATH = Path(__file__).resolve().parents[1] / 'waiting_api.py'
BASE = '/api/v1/local-intake/preprocessing/summary'
STAGES = ('waiting_postprocessing', 'postprocessing_in_progress', 'waiting_rag_upload')


@pytest.fixture
def api(monkeypatch):
    spec = importlib.util.spec_from_file_location('isolated_waiting_api_next', API_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    llm = dict(waiting_llm=4, llm_body_in_progress=1, llm_attachment_in_progress=2,
               waiting_postprocessing=99, waiting_llm_partial=False,
               llm_body_in_progress_partial=False, llm_attachment_in_progress_partial=False,
               unavailable_count=0, waiting_items=[], as_of='2026-11-01T00:00:00Z')
    monkeypatch.setitem(sys.modules, 'llm_queue_summary', SimpleNamespace(summarize_llm=lambda *_: dict(llm)))
    return module


class Queue:
    def __init__(self):
        self.state = dict(receiver_status='running', receiver_session_id='receiver-one',
                          scan_source_instance='source-one', waiting_available=True,
                          upstream_scan_verified=True, waiting_llm_total=8,
                          in_progress_count=2, as_of='2026-11-01T00:00:00Z')

    def snapshot(self, **_):
        return dict(self.state)


class Observer:
    index = 'synthetic'

    def summary(self, _snapshot):
        return dict(source='prefect', scope='all_tasks', available=True,
                    counts=dict(waiting_preprocessing=3, waiting_postprocessing=111,
                                postprocessing_in_progress=222, waiting_rag_upload=333,
                                postprocessing_held=444, postprocessing_unresolved=555))

    def preprocessing_results(self, _snapshot, result_filter, page, page_size):
        return dict(available=True, counts={'passed': 7}, filter=result_filter, page=page, page_size=page_size)

    def llm_results(self, _snapshot, result_filter, page, page_size):
        return dict(available=True, counts={'passed': 5}, filter=result_filter, page=page, page_size=page_size)


def result(filter=None, page=1, page_size=20, *, prefix='合成'):
    value = dict(available=True, counts=dict(passed=21, failed=1, unrecognized=None),
                 partial=False, as_of='2026-11-01T00:00:00Z')
    if filter:
        value['list'] = dict(filter=filter, page=page, page_size=page_size, total=21,
                             items=[dict(task_id='test', group_id=f'{prefix}-{page}')], message='合成记录')
    return value


def stage(_snapshot):
    return dict(available=True, basis='folder-stage-receipts/1', counts=dict(
        waiting_postprocessing=3, postprocessing_in_progress=2, waiting_rag_upload=1,
        postprocessing_held=1, postprocessing_unresolved=2), as_of='2026-11-01T00:00:00Z')


def poll(client, predicate, params=None):
    until = time.monotonic() + 3
    while time.monotonic() < until:
        value = client.get(BASE, params=params).json()
        if predicate(value):
            return value
        time.sleep(.01)
    pytest.fail('synthetic projection did not become available within 3 seconds')


def test_no_projections_are_read_only_and_do_not_inherit_old_counts(api):
    queue, tickets = Queue(), Observer()
    with TestClient(api.create_app(queue, ticket_observer=tickets)) as client:
        response = client.get(BASE, params={'result_filter': 'passed', 'result_page': 2,
            'llm_result_filter': 'failed', 'llm_result_page': 3})
        assert response.status_code == 200
        snapshot = response.json()
        assert snapshot['preprocessing_results']['page'] == 2
        assert snapshot['llm_results']['page'] == 3
        assert snapshot['prefect_ticket_summary']['counts']['preprocessing_in_progress'] == 2
        counts = snapshot['prefect_ticket_summary']['counts']
        assert all(counts.get(key) is None for key in (*STAGES, 'postprocessing_held', 'postprocessing_unresolved'))
        assert 'postprocessing_basis' not in snapshot['prefect_ticket_summary']
        assert snapshot['postprocessing_results']['available'] is False
        later = poll(client, lambda x: x['prefect_ticket_summary']['counts'].get('waiting_llm') == 4)
        assert later['prefect_ticket_summary']['counts']['llm_body_in_progress'] == 1
        assert all(later['prefect_ticket_summary']['counts'].get(key) is None for key in STAGES)


def test_projection_is_nonblocking_then_available_with_trusted_basis(api):
    gate, started = Event(), Event()
    def slow_results(filter, page, page_size):
        started.set()
        assert gate.wait(3)
        return result(filter, page, page_size)
    app = api.create_app(Queue(), ticket_observer=Observer(), stage_projection=stage,
                         results_projection=slow_results)
    with TestClient(app) as client:
        before = time.monotonic()
        first = client.get(BASE).json()
        assert time.monotonic() - before < 1
        assert first['postprocessing_results']['available'] is False
        assert started.wait(1)
        gate.set()
        value = poll(client, lambda x: x['postprocessing_results']['available'] is True and
                     x['prefect_ticket_summary'].get('postprocessing_basis') == 'folder-stage-receipts/1')
        counts = value['prefect_ticket_summary']['counts']
        assert [counts[key] for key in STAGES] == [3, 2, 1]
        assert counts['postprocessing_held'] == 1 and counts['postprocessing_unresolved'] == 2
        assert value['prefect_ticket_summary']['postprocessing_as_of'] == '2026-11-01T00:00:00Z'
        assert value['preprocessing_results']['counts']['passed'] == 7
        assert value['llm_results']['counts']['passed'] == 5


def test_page_caches_are_isolated_even_when_older_page_finishes_last(api):
    page_one_started, finish_page_one = Event(), Event()
    def projection(filter, page, page_size):
        if filter == 'passed' and page == 1:
            page_one_started.set()
            assert finish_page_one.wait(3)
        return result(filter, page, page_size)
    with TestClient(api.create_app(Queue(), ticket_observer=Observer(), results_projection=projection)) as client:
        first = {'postprocessing_result_filter': 'passed', 'postprocessing_result_page': 1}
        second = {'postprocessing_result_filter': 'passed', 'postprocessing_result_page': 2}
        assert client.get(BASE, params=first).json()['postprocessing_results']['available'] is False
        assert page_one_started.wait(1)
        second_result = poll(client, lambda x: x['postprocessing_results']['available'] is True, second)
        assert second_result['postprocessing_results']['list']['page'] == 2
        assert second_result['postprocessing_results']['list']['items'][0]['group_id'] == '合成-2'
        finish_page_one.set()
        poll(client, lambda x: x['postprocessing_results']['available'] is True, first)
        second_again = client.get(BASE, params=second).json()['postprocessing_results']['list']
        assert second_again['page'] == 2 and second_again['items'][0]['group_id'] == '合成-2'


def test_expired_projection_is_unknown_while_refresh_is_blocked(api, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    gate, refreshing = Event(), Event()
    calls = [0]
    def projection(filter, page, page_size):
        calls[0] += 1
        if calls[0] > 1:
            refreshing.set()
            assert gate.wait(3)
        return result(filter, page, page_size)
    with TestClient(api.create_app(Queue(), ticket_observer=Observer(), results_projection=projection)) as client:
        client.get(BASE)
        poll(client, lambda x: x['postprocessing_results']['available'] is True)
        clock[0] += 9
        assert client.get(BASE).json()['postprocessing_results']['available'] is False
        assert refreshing.wait(1)
        gate.set()
        poll(client, lambda x: x['postprocessing_results']['available'] is True)


def test_snapshot_identity_change_during_read_discards_old_data(api):
    checked = Event()
    class QueueObserved(Queue):
        def snapshot(self, **kwargs):
            value = super().snapshot(**kwargs)
            if current_thread().name == 'postprocessing-projection':
                checked.set()
            return value
    queue = QueueObserved()
    gate, started = Event(), Event()
    def projection(filter, page, page_size):
        started.set()
        assert gate.wait(3)
        return result(filter, page, page_size, prefix='旧来源')
    with TestClient(api.create_app(queue, ticket_observer=Observer(), results_projection=projection)) as client:
        assert client.get(BASE).json()['postprocessing_results']['available'] is False
        assert started.wait(1)
        queue.state['scan_source_instance'] = 'source-two'
        gate.set()
        assert checked.wait(1)
        # A different source must not reuse the old cache entry even if that
        # background read completed after the source changed.
        first_new = client.get(BASE).json()
        assert first_new['postprocessing_results']['available'] is False
        queue.state['scan_source_instance'] = 'source-one'
        assert client.get(BASE).json()['postprocessing_results']['available'] is False


@pytest.mark.parametrize('broken_stage', [
    lambda _: dict(available=True, counts={key: 0 for key in STAGES}),
    lambda _: dict(available=True, basis='wrong', counts={key: 0 for key in STAGES}),
    lambda _: dict(available=True, basis='folder-stage-receipts/1',
                   counts=dict(waiting_postprocessing=True, postprocessing_in_progress=0, waiting_rag_upload=0)),
])
def test_invalid_stage_provenance_never_promotes_legacy_counts(api, broken_stage):
    with TestClient(api.create_app(Queue(), ticket_observer=Observer(), stage_projection=broken_stage)) as client:
        client.get(BASE)
        value = poll(client, lambda x: x['prefect_ticket_summary']['counts'].get('waiting_llm') == 4)
        assert all(value['prefect_ticket_summary']['counts'].get(key) is None for key in STAGES)
        assert 'postprocessing_basis' not in value['prefect_ticket_summary']


def test_missing_verification_time_is_not_filled_with_receiver_time(api):
    def missing_time(snapshot):
        value = stage(snapshot)
        value.pop('as_of')
        return value
    assert api._valid_stage(missing_time({})) is False
    class OldObserver(Observer):
        def summary(self, snapshot):
            return {**super().summary(snapshot), 'postprocessing_as_of': '2020-01-01T00:00:00Z'}
    with TestClient(api.create_app(Queue(), ticket_observer=OldObserver(), stage_projection=missing_time)) as client:
        client.get(BASE)
        value = poll(client, lambda item: item['prefect_ticket_summary']['counts'].get('waiting_llm') == 4)
    assert value['prefect_ticket_summary']['counts'].get('waiting_postprocessing') is None
    assert 'postprocessing_as_of' not in value['prefect_ticket_summary']


def test_projection_errors_are_isolated_from_preprocessing_and_llm(api):
    def broken(*_):
        raise RuntimeError('synthetic failure')
    with TestClient(api.create_app(Queue(), ticket_observer=Observer(), stage_projection=broken,
                                  results_projection=broken)) as client:
        value = poll(client, lambda x: x['prefect_ticket_summary']['counts'].get('waiting_llm') == 4)
        assert value['postprocessing_results']['available'] is False
        assert value['preprocessing_results']['counts']['passed'] == 7
        assert value['llm_results']['counts']['passed'] == 5
        assert all(value['prefect_ticket_summary']['counts'].get(key) is None for key in STAGES)


def test_a_mismatched_page_result_is_unavailable(api):
    def mismatched(filter, page, page_size):
        value = result(filter, page, page_size)
        value['list']['page'] += 1
        return value
    with TestClient(api.create_app(Queue(), ticket_observer=Observer(), results_projection=mismatched)) as client:
        params = {'postprocessing_result_filter': 'failed', 'postprocessing_result_page': 1}
        first = client.get(BASE, params=params).json()
        assert first['postprocessing_results']['available'] is False
        later = poll(client, lambda x: x['prefect_ticket_summary']['counts'].get('waiting_llm') == 4, params)
        assert later['postprocessing_results']['available'] is False
        assert later['postprocessing_results']['list']['page'] == 1


def test_unverified_source_is_unknown_without_invoking_projection(api):
    queue = Queue()
    queue.state['upstream_scan_verified'] = False
    called = []
    def never(*_):
        called.append(True)
        return result()
    with TestClient(api.create_app(queue, ticket_observer=Observer(), stage_projection=never,
                                  results_projection=never)) as client:
        snapshot = client.get(BASE).json()
        assert snapshot['postprocessing_results']['available'] is False
        assert all(snapshot['prefect_ticket_summary']['counts'].get(key) is None for key in STAGES)
        assert called == []


def test_projection_work_is_bounded_when_requests_span_many_pages(api):
    lock, started, gate = Lock(), [], Event()
    def projection(filter, page, page_size):
        with lock:
            started.append((filter, page))
        assert gate.wait(3)
        return result(filter, page, page_size)
    with TestClient(api.create_app(Queue(), ticket_observer=Observer(), results_projection=projection)) as client:
        try:
            for page in range(1, 20):
                assert client.get(BASE, params={'postprocessing_result_filter': 'passed',
                                                'postprocessing_result_page': page}).status_code == 200
            assert 1 <= len(started) <= 2
        finally:
            gate.set()


def test_projection_cache_evicts_old_pages(api):
    cache = api._ProjectionCache(max_entries=2, max_inflight=1)
    for key in range(5):
        cache.get((key,), lambda key=key: {'key': key})
        until = time.monotonic() + 1
        while time.monotonic() < until:
            with cache.lock:
                if (key,) in cache.entries:
                    break
            time.sleep(.005)
        else:
            pytest.fail('synthetic cache entry was not populated')
    with cache.lock:
        assert len(cache.entries) == 2
        assert set(cache.entries) == {(3,), (4,)}


class SyntheticWitness:
    def __init__(self):
        self.local_ok = True
        self.remote_ok = True
        self.remote_started = Event()
        self.remote_gate = Event()
        self.remote_gate.set()

    def local(self, _snapshot):
        return self.local_ok

    def remote(self):
        self.remote_started.set()
        assert self.remote_gate.wait(2)
        return self.remote_ok


class GuardedValue(dict):
    def __init__(self, value, witness):
        super().__init__(value)
        self._cache_witness = witness

    def __deepcopy__(self, _memo):
        return GuardedValue(dict(self), self._cache_witness)


def cache_populated(cache, key):
    until = time.monotonic() + 2
    while time.monotonic() < until:
        with cache.lock:
            if key in cache.entries and key not in cache.inflight:
                return
        time.sleep(.005)
    pytest.fail('guarded projection did not become available')


def test_guarded_cache_rechecks_and_expires_without_rehash(api, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    witness = SyntheticWitness()
    cache = api._ProjectionCache(max_inflight=1)
    key = ('guarded',)
    compute = lambda: GuardedValue({'count': 3}, witness)
    assert cache.get(key, compute, snapshot={}) is None
    cache_populated(cache, key)
    assert cache.get(key, compute, snapshot={}) == {'count': 3}
    clock[0] = 3.1
    assert cache.get(key, compute, snapshot={}) == {'count': 3}
    assert witness.remote_started.wait(1)
    until = time.monotonic() + 1
    while time.monotonic() < until and cache.remote_inflight:
        time.sleep(.005)
    clock[0] = 9.0
    assert cache.get(key, compute, snapshot={}) == {'count': 3}
    clock[0] = 46.0
    assert cache.get(key, compute, snapshot={}) is None


def test_guarded_cache_invalidates_local_change_immediately(api):
    witness = SyntheticWitness()
    cache = api._ProjectionCache(max_inflight=1)
    key = ('guarded',)
    gate = Event()
    calls = []
    def compute():
        calls.append(True)
        if len(calls) > 1:
            assert gate.wait(2)
        return GuardedValue({'count': 3}, witness)
    cache.get(key, compute, snapshot={})
    cache_populated(cache, key)
    witness.local_ok = False
    assert cache.get(key, compute, snapshot={}) is None
    assert cache.get(key, compute, snapshot={}) is None
    gate.set()


def test_guarded_cache_invalidates_failed_or_expired_prefect_check(api, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    witness = SyntheticWitness()
    witness.remote_gate.clear()
    cache = api._ProjectionCache(max_inflight=1)
    key = ('guarded',)
    compute = lambda: GuardedValue({'count': 3}, witness)
    cache.get(key, compute, snapshot={})
    cache_populated(cache, key)
    clock[0] = 3.1
    assert cache.get(key, compute, snapshot={}) == {'count': 3}
    assert witness.remote_started.wait(1)
    clock[0] = 9.0
    assert cache.get(key, compute, snapshot={}) is None
    witness.remote_ok = False
    witness.remote_gate.set()
    until = time.monotonic() + 1
    while time.monotonic() < until and cache.remote_inflight:
        time.sleep(.005)
    assert cache.get(key, compute, snapshot={}) is None


def test_slow_prefect_check_cannot_make_old_evidence_new(api, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    witness = SyntheticWitness()
    witness.remote_gate.clear()
    cache = api._ProjectionCache(max_inflight=1)
    key = ('guarded',)
    compute = lambda: GuardedValue({'count': 3}, witness)
    cache.get(key, compute, snapshot={})
    cache_populated(cache, key)
    clock[0] = 3.1
    assert cache.get(key, compute, snapshot={}) == {'count': 3}
    assert witness.remote_started.wait(1)
    clock[0] = 12.0
    witness.remote_gate.set()
    until = time.monotonic() + 1
    while time.monotonic() < until and cache.remote_inflight:
        time.sleep(.005)
    assert cache.get(key, compute, snapshot={}) is None


def test_full_verification_can_expire_while_local_check_is_running(api, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    witness = SyntheticWitness()
    cache = api._ProjectionCache(max_inflight=1)
    key = ('guarded',)
    compute = lambda: GuardedValue({'count': 3}, witness)
    cache.get(key, compute, snapshot={})
    cache_populated(cache, key)
    def slow_local(_snapshot):
        clock[0] = 46.0
        return True
    witness.local = slow_local
    assert cache.get(key, compute, snapshot={}) is None


def test_witness_is_private_to_cache_and_not_returned_in_api_json(api):
    witness = SyntheticWitness()
    def projection(filter, page, page_size):
        return GuardedValue(result(filter, page, page_size), witness)
    with TestClient(api.create_app(Queue(), ticket_observer=Observer(), results_projection=projection)) as client:
        client.get(BASE)
        value = poll(client, lambda item: item['postprocessing_results']['available'] is True)
    assert '_cache_witness' not in value['postprocessing_results']


def test_real_candidate_result_store_can_supply_a_synthetic_filtered_page(api, tmp_path):
    root = API_PATH.parents[1]
    result_spec = importlib.util.spec_from_file_location('synthetic_results_store',
                                                          root / 'results_next' / 'postprocessing_results.py')
    result_module = importlib.util.module_from_spec(result_spec)
    result_spec.loader.exec_module(result_module)
    stage_spec = importlib.util.spec_from_file_location('synthetic_folder_stage', root / 'folder_state.py')
    stage_module = importlib.util.module_from_spec(stage_spec)
    stage_spec.loader.exec_module(stage_module)
    database = tmp_path / 'synthetic.sqlite3'
    item = dict(task_id=str(uuid4()), group_id='a' * 32, paper_id='synthetic-paper', receipt_id='b' * 64,
                attempt_id='synthetic-attempt', parent_run_id=str(uuid4()), task_run_id=str(uuid4()))
    with sqlite3.connect(database) as db:
        db.execute('CREATE TABLE tasks(task_id TEXT PRIMARY KEY,desired_state TEXT,start_generation INTEGER,'
                   'start_pending INTEGER,task_status TEXT,prefect_run_id TEXT,prefect_order_model TEXT,name TEXT)')
        db.execute('CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY)')
        db.execute('CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,'
                   'attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,manifest_sha256 TEXT,state TEXT,'
                   'generation INTEGER,PRIMARY KEY(task_id,group_id))')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?)',
                   (item['task_id'], 'running', 1, 0, 'running', item['parent_run_id'], 'v2', '合成任务'))
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?)',
                   (*item.values(), 'c' * 64, 'waiting_postprocessing', 1))
    stage_module.FolderStageStore(database, observe_run=lambda _: True).initialize()
    store = result_module.ResultsStore(database, admission_gate=lambda _: nullcontext(),
                                        inspect_receipt=stage_module.FolderStageStore._inspect_receipt)
    store.initialize()
    ticket = store.reserve(item, 1, 'c' * 64)
    proof = tmp_path / 'synthetic-validation.json'
    proof.write_text('{"valid":false}', encoding='utf8')
    store.record_failed(item, ticket, code='body_validation', evidence_path=proof, confirm=lambda *_: True)
    params = {'postprocessing_result_filter': 'failed', 'postprocessing_result_page': 1,
              'postprocessing_result_page_size': 20}
    with TestClient(api.create_app(Queue(), ticket_observer=Observer(), results_projection=store.summary)) as client:
        client.get(BASE, params=params)
        value = poll(client, lambda x: x['postprocessing_results']['available'] is True, params)
    results = value['postprocessing_results']
    assert results['counts']['failed'] == 1
    assert results['list']['page_size'] == 20 and results['list']['items'][0]['group_id'] == 'a' * 32
    assert results['list']['items'][0]['reason'] == '正文画像校验未通过'
