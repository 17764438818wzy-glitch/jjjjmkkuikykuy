"""Synthetic API checks for ordering independent server observation caches.

No real control database, remote server, queue, or business worker is used.
"""
import copy
from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import threading
import time

from fastapi.testclient import TestClient


HERE = Path(__file__).resolve().parents[1]
BASE = '/api/v1/local-intake/preprocessing/summary'
BASIS = 'postprocessing-registered-ledger/1'
PASSED = {'postprocessing_result_filter': 'passed'}


def load_api():
    spec = importlib.util.spec_from_file_location('remote_projection_cache_order_api', HERE / 'waiting_api.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.PROJECTION_REFRESH_SECONDS = .05
    return module


class Queue:
    def snapshot(self, **_):
        return {'receiver_status': 'unknown', 'waiting_available': False}


class Observation(dict):
    def __init__(self, value, observed=None):
        super().__init__(value)
        self._server_observed_monotonic = time.monotonic() if observed is None else observed
        self._server_valid_for_seconds = 30

    def __deepcopy__(self, memo):
        return Observation(copy.deepcopy(dict(self), memo), self._server_observed_monotonic)


class Projection:
    def __init__(self):
        self.online = True
        self.version = 1
        self.hold_good_list = False
        self.hold_bad_list = False
        self.old_started = threading.Event()
        self.old_finished = threading.Event()
        self.release_old = threading.Event()
        self.release_bad = threading.Event()
        self.summary_failed = threading.Event()

    def __call__(self, *, result_filter=None, page=1, page_size=20, expected_revision=None):
        # Match server_projection.snapshot: freshness begins before remote I/O.
        observed = time.monotonic()
        as_of = datetime.now(timezone.utc).isoformat()
        good = self.online
        version = self.version
        if result_filter and good and self.hold_good_list:
            self.hold_good_list = False
            self.old_started.set()
            if not self.release_old.wait(4):
                raise TimeoutError('synthetic old observation was not released')
            self.old_finished.set()
        if result_filter and not good and self.hold_bad_list:
            if not self.release_bad.wait(4):
                raise TimeoutError('synthetic failed observation was not released')
        if not result_filter and not good:
            self.summary_failed.set()
        revision = (('a' if version == 1 else 'd') if good else 'b') * 64
        count = version if good else None
        result = {'available': True, 'basis': BASIS, 'as_of': as_of, 'partial': not good,
                  'counts': {'passed': count, 'failed': None, 'unrecognized': None}}
        if result_filter:
            result['list'] = {'filter': result_filter, 'page': page, 'page_size': page_size,
                              'total': count, 'has_next': False,
                              'items': [{'group_id': f'synthetic-{n}'} for n in range(count)] if good else []}
        remote = {'available': good, 'basis': 'paperflow-server-receipt-observation/1',
                  'as_of': as_of, 'source_revision': ('c' if version == 1 else 'e') * 64,
                  'observation_revision': revision, 'waiting_rag_upload': count}
        return Observation({'available': True, 'basis': BASIS, 'as_of': as_of,
            'revision': revision, 'current_delivery_verified': False,
            'stage': {'available': True, 'basis': BASIS, 'as_of': as_of, 'partial': False,
                      'counts': {'waiting_postprocessing': 0, 'postprocessing_in_progress': None,
                                 'waiting_rag_upload': count}},
            'results': result, 'server_observation': remote}, observed=observed)


def get(client, params=None):
    response = client.get(BASE, params=params or {})
    assert response.status_code == 200
    return response.json()['postprocessing_registered_ledger']


def remote_available(value):
    return value.get('available') is True and value.get('server_observation', {}).get('available') is True


def poll(client, params, predicate):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        value = get(client, params)
        if predicate(value):
            return value
        time.sleep(.01)
    raise AssertionError('synthetic observation did not reach the expected state')


def warm(client):
    poll(client, {}, remote_available)
    poll(client, PASSED, remote_available)
    time.sleep(.1)


def test_new_failure_invalidates_success_cached_under_a_different_filter():
    api, projection = load_api(), Projection()
    try:
        with TestClient(api.create_app(Queue(), registered_projection=projection)) as client:
            warm(client)
            projection.hold_bad_list = True
            projection.online = False
            assert projection.summary_failed.wait(.01) is False
            poll(client, {}, lambda value: projection.summary_failed.is_set() and not remote_available(value))
            stale = get(client, PASSED)
            assert not remote_available(stale), 'a newer failed observation must invalidate older list evidence'
    finally:
        projection.release_old.set()
        projection.release_bad.set()


def test_late_success_cannot_overwrite_a_newer_failed_observation():
    api, projection = load_api(), Projection()
    try:
        with TestClient(api.create_app(Queue(), registered_projection=projection)) as client:
            warm(client)
            projection.hold_good_list = True
            get(client, PASSED)
            assert projection.old_started.wait(2)
            projection.hold_bad_list = True
            projection.online = False
            poll(client, {}, lambda value: projection.summary_failed.is_set() and not remote_available(value))
            projection.release_old.set()
            assert projection.old_finished.wait(2)
            # Give the asynchronous cache writer a chance to commit the old result.
            time.sleep(.05)
            deadline = time.monotonic() + .2
            while time.monotonic() < deadline:
                stale = get(client, PASSED)
                assert not remote_available(stale), 'an older in-flight success must not reverse newer failure'
                time.sleep(.01)
    finally:
        projection.release_old.set()
        projection.release_bad.set()


def test_new_success_invalidates_a_cached_list_from_an_older_ledger_revision():
    api, projection = load_api(), Projection()
    try:
        with TestClient(api.create_app(Queue(), registered_projection=projection)) as client:
            warm(client)
            projection.hold_good_list = True
            projection.version = 2
            newer = poll(client, {}, lambda value: value.get('results', {}).get('counts', {}).get('passed') == 2)
            old_list = get(client, PASSED)
            assert not remote_available(old_list) or old_list['revision'] == newer['revision'], (
                'an older cached list must not overwrite a newer successful ledger snapshot')
    finally:
        projection.release_old.set()
        projection.release_bad.set()


def test_late_old_success_cannot_replace_a_newer_successful_revision():
    api, projection = load_api(), Projection()
    try:
        with TestClient(api.create_app(Queue(), registered_projection=projection)) as client:
            warm(client)
            projection.hold_good_list = True
            get(client, PASSED)
            assert projection.old_started.wait(2)
            projection.version = 2
            newer = poll(client, {}, lambda value: value.get('results', {}).get('counts', {}).get('passed') == 2)
            projection.release_old.set()
            assert projection.old_finished.wait(2)
            time.sleep(.05)
            old_list = get(client, PASSED)
            assert not remote_available(old_list) or old_list['revision'] == newer['revision']
    finally:
        projection.release_old.set()
        projection.release_bad.set()
