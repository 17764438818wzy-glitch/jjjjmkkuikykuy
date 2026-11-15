import importlib.util
from pathlib import Path

from fastapi import FastAPI


here = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('positions_freshness_api', here / 'waiting_api.py')
api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api)


def test_slow_refresh_keeps_timestamped_positions_only_until_the_new_deadline(monkeypatch):
    clock = [9.0]
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    key = ('positions',)
    value = {'available': True, 'as_of': '2026-11-02T04:00:00Z'}
    cache = api._ProjectionCache(max_inflight=1, max_age_seconds=15)
    epoch = cache.bind_source(('source',), 1)
    cache.entries[key] = {'at': 0.0, 'value': value, 'witness': None}
    cache.inflight.add(key)
    assert cache.get(key, lambda: None, expected_epoch=epoch) == value
    clock[0] = 16.0
    assert cache.get(key, lambda: None, expected_epoch=epoch) is None


def test_other_projection_caches_keep_their_existing_deadline(monkeypatch):
    monkeypatch.setattr(api, 'projection_clock', lambda: 9.0)
    key = ('other',)
    cache = api._ProjectionCache(max_inflight=1)
    epoch = cache.bind_source(('source',), 1)
    cache.entries[key] = {'at': 0.0, 'value': {'available': True}, 'witness': None}
    cache.inflight.add(key)
    assert cache.get(key, lambda: None, expected_epoch=epoch) is None


def test_positions_endpoint_uses_the_adjusted_deadline():
    spec = importlib.util.spec_from_file_location('positions_freshness_candidate', here / 'ticket_positions.py')
    positions = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(positions)
    options = []

    def factory(**kwargs):
        options.append(kwargs)
        return api._ProjectionCache(**kwargs)

    positions.install(FastAPI(), object(), factory)
    assert options == [{'max_inflight': 1, 'max_age_seconds': 15}]
