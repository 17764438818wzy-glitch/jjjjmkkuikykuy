"""Isolated API integration: only synthetic ledgers and remote metadata."""
import functools
import importlib.util
from pathlib import Path
import time

from fastapi.testclient import TestClient

HERE = Path(__file__).resolve().parents[1]
BASE = '/api/v1/local-intake/preprocessing/summary'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fixtures = load('remote_projection_api_fixtures', HERE / 'tests' / 'test_server_projection.py')


class Queue:
    def snapshot(self, **_):
        return {'receiver_status': 'unknown', 'waiting_available': False}


def poll(client, predicate, params=None):
    deadline = time.monotonic() + 4
    while time.monotonic() < deadline:
        response = client.get(BASE, params=params or {})
        assert response.status_code == 200
        value = response.json()['postprocessing_registered_ledger']
        if predicate(value):
            return value
        time.sleep(.01)
    raise AssertionError('synthetic server projection did not become available')


def test_api_displays_only_server_verified_waiting_receipts(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    fixtures.add_complete_pass(db, 1)
    inspector, _ = fixtures.make_inspector(db)
    projection = functools.partial(fixtures.observe, db, inspector=inspector)
    api = load('remote_projection_api', HERE / 'waiting_api.py')
    with TestClient(api.create_app(Queue(), registered_projection=projection)) as client:
        value = poll(client, lambda item: item.get('available') is True)
    assert value['stage']['counts']['waiting_rag_upload'] == 1
    assert value['stage']['counts']['postprocessing_in_progress'] is None
    assert value['server_observation']['available'] is True
    assert value['server_observation']['observation_revision'] == value['revision']
    assert value['results']['counts']['passed'] == 1
    assert value['results']['counts']['failed'] is None
    assert value['current_delivery_verified'] is False
    assert '_server_observed_monotonic' not in value


def test_api_strips_expired_or_unwitnessed_server_counts(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    fixtures.add_complete_pass(db, 1)
    inspector, _ = fixtures.make_inspector(db)
    value = fixtures.observe(db, inspector=inspector)
    api = load('remote_projection_api_expiry', HERE / 'waiting_api.py')
    expired = api._normalize_server_observation(value, now=value._server_observed_monotonic + 31)
    assert expired['server_observation']['available'] is False
    assert expired['stage']['counts']['waiting_postprocessing'] is None
    assert expired['stage']['counts']['waiting_rag_upload'] is None
    assert expired['results']['counts']['passed'] is None
    assert expired['revision'] != value['revision']
    unwitnessed = api._normalize_server_observation(dict(value), now=value._server_observed_monotonic)
    assert unwitnessed['stage']['counts']['waiting_rag_upload'] is None


def test_api_does_not_treat_plain_registered_receipt_as_server_verified(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    fixtures.add_complete_pass(db, 1)
    projection = functools.partial(fixtures.helpers.projection.snapshot, db.path,
                                  component_id_fn=fixtures.helpers.upstream.component_id)
    api = load('remote_projection_api_plain_ledger', HERE / 'waiting_api.py')
    with TestClient(api.create_app(Queue(), registered_projection=projection)) as client:
        value = poll(client, lambda item: item.get('available') is True)
    assert value['stage']['counts']['waiting_rag_upload'] is None
    assert value['stage']['counts']['postprocessing_in_progress'] is None
    assert value['results']['counts']['passed'] is None


def test_unavailable_server_keeps_consistent_revision_for_registered_pagination(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    for n in (1, 2):
        fixtures.add_complete_pass(db, n)
    inspector, _ = fixtures.make_inspector(db, tamper='offline')
    first = fixtures.observe(db, inspector=inspector, result_filter='passed', page=1, page_size=1)
    api = load('remote_projection_api_unknown_revision', HERE / 'waiting_api.py')
    normalized = api._normalize_server_observation(first, now=first._server_observed_monotonic)
    second = fixtures.observe(db, inspector=inspector, result_filter='passed', page=2, page_size=1,
                              expected_revision=normalized['revision'])
    assert second['available'] is True
    assert second['results']['list']['items'][0]['group_id'] != first['results']['list']['items'][0]['group_id']
    assert second['stage']['counts']['waiting_rag_upload'] is None
