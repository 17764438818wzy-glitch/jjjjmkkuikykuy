"""Exercise only synthetic ASGI services; no real queue or network is used."""
import asyncio
import importlib.util
import json
from pathlib import Path
import time
import threading

import httpx
import pytest


HERE = Path(__file__).resolve().parents[1]
BASE = '/api/v1/local-intake/preprocessing/summary'


def load():
    spec = importlib.util.spec_from_file_location('synthetic_bounded_summary', HERE / 'bounded_summary.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def payload(available=True, revision='a' * 64):
    return json.dumps({'receiver_status': 'running', 'receiver_session_id': 'synthetic-session',
        'scan_source_instance': 'synthetic-source', 'waiting_available': True,
        'postprocessing_registered_ledger': {'available': True, 'revision': revision,
            'server_observation': {'available': available}}}).encode()


async def send_json(send, body, status=200, deadline=None):
    if deadline is None:
        deadline = time.monotonic() + 29
    await send({'type': 'http.response.start', 'status': status,
                'headers': [(b'content-type', b'application/json'),
                            (b'x-paperflow-observation-deadline', repr(deadline).encode('ascii'))]})
    await send({'type': 'http.response.body', 'body': body})


def test_slow_summary_fails_fast_then_returns_a_completed_observation():
    async def exercise():
        release = asyncio.Event()
        calls = []
        async def original(scope, receive, send):
            calls.append(scope['query_string'])
            await release.wait()  # Would remain blocked longer than the browser's 5 seconds.
            await send_json(send, payload())
        app = load().BoundedSummary(original, max_inflight=2, wait_seconds=.05, max_age_seconds=15)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
            started = time.monotonic()
            response = await client.get(BASE)
            assert time.monotonic() - started < .5 and response.status_code == 503
            assert response.json()['status'] == 'observation_pending'
            release.set()
            await asyncio.sleep(.03)
            response = await client.get(BASE)
            assert response.status_code == 200 and response.json()['receiver_status'] == 'running'
            assert calls
    asyncio.run(exercise())


def test_inner_failure_invalidates_outer_cached_pass_before_next_http_response():
    async def exercise():
        spec = importlib.util.spec_from_file_location('synthetic_shared_cache_api', HERE / 'waiting_api.py')
        api = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(api)
        cache = api._ProjectionCache(invalidate_server_failures=True)
        epoch = cache.bind_source('synthetic-source', 1)
        class Observation(dict):
            def __init__(self, available):
                super().__init__({'available': True, 'server_observation': {
                    'available': available, 'source_revision': 'a' * 64,
                    'observation_revision': 'b' * 64}})
                self._server_observed_monotonic = time.monotonic()
            def __deepcopy__(self, _memo):
                result = Observation(self['server_observation']['available'])
                result._server_observed_monotonic = self._server_observed_monotonic
                return result
        cache.get('passed-list', lambda: Observation(True), expected_epoch=epoch)
        deadline = time.monotonic() + 2
        while cache.inflight and time.monotonic() < deadline:
            await asyncio.sleep(.01)
        assert cache.entries['passed-list']['value']['server_observation']['available'] is True
        release = asyncio.Event()
        calls = 0
        async def original(scope, receive, send):
            nonlocal calls
            calls += 1
            if calls > 1:
                await release.wait()
            await send_json(send, payload(True))
        def shared_revision():
            revision = getattr(cache, 'current_revision', None)
            return ('same-control-revision', revision() if callable(revision) else 0)
        app = load().BoundedSummary(original, wait_seconds=.02, revision_witness=shared_revision)
        try:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
                first = await client.get(BASE)
                assert first.status_code == 200
                assert first.json()['postprocessing_registered_ledger']['server_observation']['available'] is True
                def failure():
                    raise ValueError('synthetic new inner observation failed')
                cache.get('other-filter', failure, expected_epoch=epoch)
                deadline = time.monotonic() + 2
                while 'other-filter' in cache.inflight and time.monotonic() < deadline:
                    await asyncio.sleep(.01)
                assert 'passed-list' not in cache.entries
                after = await client.get(BASE)
                assert (after.status_code == 503 or
                        after.json()['postprocessing_registered_ledger'].get('server_observation', {}).get('available') is not True)
        finally:
            release.set()
    asyncio.run(exercise())


def test_blocked_requests_are_bounded_and_do_not_create_unlimited_background_work():
    async def exercise():
        release = asyncio.Event()
        calls = []
        async def original(scope, receive, send):
            calls.append(scope['query_string'])
            await release.wait()
            await send_json(send, payload())
        app = load().BoundedSummary(original, max_inflight=2, wait_seconds=.01, max_age_seconds=15)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
            results = await asyncio.gather(*(client.get(BASE, params={'page': n}) for n in range(8)))
            assert all(result.status_code == 503 for result in results)
            assert len(calls) == 2
            release.set()
            await asyncio.sleep(.03)
    asyncio.run(exercise())


def test_over_five_second_observation_returns_pending_without_dispatch():
    async def exercise():
        release, entered = asyncio.Event(), asyncio.Event()
        async def original(scope, receive, send):
            entered.set()
            await release.wait()
            await send_json(send, payload())
        app = load().BoundedSummary(original, wait_seconds=.05)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
            start = time.monotonic()
            first = await client.get(BASE)
            assert first.status_code == 503 and time.monotonic() - start < 1
            assert entered.is_set()
            await asyncio.sleep(5.1)
            assert time.monotonic() - start >= 5
            start = time.monotonic()
            assert (await client.get(BASE)).status_code == 503
            assert time.monotonic() - start < 1
            release.set()
            await asyncio.sleep(.03)
            assert (await client.get(BASE)).status_code == 200
    asyncio.run(exercise())


def test_newer_failed_observation_prevents_an_older_success_from_reappearing():
    async def exercise():
        older, newer, fresh = asyncio.Event(), asyncio.Event(), asyncio.Event()
        older_calls = []
        async def original(scope, receive, send):
            if scope['query_string'] == b'view=older':
                older_calls.append(True)
                await (older if len(older_calls) == 1 else fresh).wait()
                await send_json(send, payload())
            else:
                await newer.wait()
                await send_json(send, payload(False))
        app = load().BoundedSummary(original, max_inflight=2, wait_seconds=.01, max_age_seconds=15)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
            assert (await client.get(BASE, params={'view': 'older'})).status_code == 503
            assert (await client.get(BASE, params={'view': 'newer'})).status_code == 503
            newer.set()
            await asyncio.sleep(.03)
            older.set()
            await asyncio.sleep(.03)
            result = await client.get(BASE, params={'view': 'older'})
            assert result.status_code == 503
            fresh.set()
    asyncio.run(exercise())


def test_source_loss_on_summary_invalidates_an_older_cached_list():
    async def exercise():
        hold = asyncio.Event()
        online = True
        async def original(scope, receive, send):
            if not online and scope['query_string'] == b'view=list':
                await hold.wait()
            body = json.loads(payload(online))
            if not online:
                body['receiver_status'] = 'unknown'
                body['waiting_available'] = False
            await send_json(send, json.dumps(body).encode())
        app = load().BoundedSummary(original, wait_seconds=.01, max_age_seconds=15)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
            assert (await client.get(BASE, params={'view': 'list'})).json()['postprocessing_registered_ledger']['server_observation']['available']
            online = False
            observed = await client.get(BASE, params={'view': 'summary'})
            assert observed.status_code == 503 or not observed.json()['postprocessing_registered_ledger']['server_observation']['available']
            assert (await client.get(BASE, params={'view': 'list'})).status_code == 503
            hold.set()
    asyncio.run(exercise())


def test_late_failed_observation_invalidates_a_newer_started_success():
    async def exercise():
        observe_failure, hold_refresh = asyncio.Event(), asyncio.Event()
        newer_calls = 0
        async def original(scope, receive, send):
            nonlocal newer_calls
            if scope['query_string'] == b'view=older':
                await observe_failure.wait()
                await send_json(send, payload(False))
            else:
                newer_calls += 1
                if newer_calls > 1:
                    await hold_refresh.wait()
                await send_json(send, payload(True))
        app = load().BoundedSummary(original, wait_seconds=.01, max_age_seconds=15)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
            assert (await client.get(BASE, params={'view': 'older'})).status_code == 503
            assert (await client.get(BASE, params={'view': 'newer'})).json()['postprocessing_registered_ledger']['server_observation']['available']
            observe_failure.set()
            await asyncio.sleep(.03)
            after = await client.get(BASE, params={'view': 'newer'})
            assert after.status_code == 503 or not after.json()['postprocessing_registered_ledger']['server_observation']['available']
            hold_refresh.set()
    asyncio.run(exercise())


def test_conflict_invalidates_an_already_inflight_later_request_too():
    async def exercise():
        fail, stale_success, fresh = asyncio.Event(), asyncio.Event(), asyncio.Event()
        summary_calls = 0
        async def original(scope, receive, send):
            nonlocal summary_calls
            if scope['query_string'] == b'view=older':
                await fail.wait()
                await send_json(send, payload(False))
            else:
                summary_calls += 1
                if summary_calls == 2: await stale_success.wait()
                if summary_calls >= 3: await fresh.wait()
                await send_json(send, payload(True))
        app = load().BoundedSummary(original, wait_seconds=.01, max_age_seconds=15)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
            assert (await client.get(BASE, params={'view': 'older'})).status_code == 503
            assert (await client.get(BASE, params={'view': 'summary'})).status_code == 200
            assert (await client.get(BASE, params={'view': 'summary'})).status_code == 200
            await asyncio.sleep(.03)
            assert summary_calls == 2
            fail.set()
            await asyncio.sleep(.03)
            stale_success.set()
            await asyncio.sleep(.03)
            after = await client.get(BASE, params={'view': 'summary'})
            assert after.status_code == 503 or not after.json()['postprocessing_registered_ledger']['server_observation']['available']
            fresh.set()
    asyncio.run(exercise())


def test_older_success_cannot_replace_failure_when_response_order_is_reversed():
    async def exercise():
        failure, send_success, fresh = asyncio.Event(), asyncio.Event(), asyncio.Event()
        newer_calls = 0
        async def original(scope, receive, send):
            nonlocal newer_calls
            if scope['query_string'] == b'view=older':
                await failure.wait()
                await send_json(send, payload(False))
            else:
                newer_calls += 1
                if newer_calls == 1:
                    observed_success = payload(True)
                    await send_success.wait()
                    await send_json(send, observed_success)
                else:
                    await fresh.wait()
                    await send_json(send, payload(True))
        app = load().BoundedSummary(original, wait_seconds=.01, max_age_seconds=15)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
            assert (await client.get(BASE, params={'view': 'older'})).status_code == 503
            assert (await client.get(BASE, params={'view': 'newer'})).status_code == 503
            failure.set()
            await asyncio.sleep(.03)
            send_success.set()
            await asyncio.sleep(.03)
            after = await client.get(BASE, params={'view': 'newer'})
            assert after.status_code == 503 or not after.json()['postprocessing_registered_ledger']['server_observation']['available']
            fresh.set()
    asyncio.run(exercise())


def test_slow_list_with_same_verified_source_and_revision_can_finish():
    async def exercise():
        older, next_list = asyncio.Event(), asyncio.Event()
        list_calls = 0
        async def original(scope, receive, send):
            nonlocal list_calls
            if scope['query_string'] == b'view=list':
                list_calls += 1
                await (older if list_calls == 1 else next_list).wait()
            await send_json(send, payload())
        app = load().BoundedSummary(original, max_inflight=2, wait_seconds=.01, max_age_seconds=15)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
            assert (await client.get(BASE, params={'view': 'list'})).status_code == 503
            assert (await client.get(BASE, params={'view': 'summary'})).status_code == 200
            older.set()
            await asyncio.sleep(.03)
            assert (await client.get(BASE, params={'view': 'list'})).status_code == 200
            next_list.set()
    asyncio.run(exercise())


def test_cached_response_is_scoped_to_its_cors_origin():
    async def exercise():
        async def original(scope, receive, send):
            origin = dict(scope['headers']).get(b'origin', b'')
            await send({'type': 'http.response.start', 'status': 200,
                        'headers': [(b'access-control-allow-origin', origin)]})
            await send({'type': 'http.response.body', 'body': payload()})
        app = load().BoundedSummary(original, wait_seconds=.01)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
            first = await client.get(BASE, headers={'Origin': 'http://localhost:8795'})
            second = await client.get(BASE, headers={'Origin': 'http://127.0.0.1:8795'})
            assert first.headers['access-control-allow-origin'] == 'http://localhost:8795'
            assert second.headers['access-control-allow-origin'] == 'http://127.0.0.1:8795'
    asyncio.run(exercise())


def test_outer_cache_never_extends_server_evidence_deadline():
    async def exercise():
        release = asyncio.Event()
        calls = 0
        async def original(scope, receive, send):
            nonlocal calls
            calls += 1
            if calls > 1:
                await release.wait()
            body = json.loads(payload())
            body['preprocessing_results'] = {'available': True, 'counts': {'passed': 2}}
            body['llm_results'] = {'available': True, 'counts': {'passed': 3}}
            await send_json(send, json.dumps(body).encode(), deadline=time.monotonic() + .1)
        app = load().BoundedSummary(original, wait_seconds=.01, max_age_seconds=15)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
            first = await client.get(BASE)
            assert first.json()['postprocessing_registered_ledger']['server_observation']['available'] is True
            assert 'x-paperflow-observation-deadline' not in first.headers
            await asyncio.sleep(.15)
            second = await client.get(BASE)
            assert second.status_code == 200
            result = second.json()
            assert result['postprocessing_registered_ledger']['available'] is False
            assert result['preprocessing_results']['counts']['passed'] == 2
            assert result['llm_results']['counts']['passed'] == 3
            release.set()
    asyncio.run(exercise())


def test_browser_receives_only_the_remaining_observation_lifetime():
    async def exercise():
        release = asyncio.Event()
        calls = 0
        async def original(scope, receive, send):
            nonlocal calls
            calls += 1
            if calls > 1:
                await release.wait()
            await send_json(send, payload(), deadline=time.monotonic() + .4)
        app = load().BoundedSummary(original, wait_seconds=.01)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
            first = (await client.get(BASE)).json()
            assert 0 < first['postprocessing_observation_remaining_ms'] <= 400
            await asyncio.sleep(.12)
            second = (await client.get(BASE)).json()
            assert 0 < second['postprocessing_observation_remaining_ms'] < first['postprocessing_observation_remaining_ms']
            await asyncio.sleep(.32)
            expired = (await client.get(BASE)).json()
            assert expired['postprocessing_registered_ledger']['available'] is False
            assert expired['postprocessing_observation_remaining_ms'] == 0
            release.set()
    asyncio.run(exercise())


def test_control_revision_change_discards_cached_server_observations():
    async def exercise():
        revision = [1]
        hold = asyncio.Event()
        calls = 0
        async def original(scope, receive, send):
            nonlocal calls
            calls += 1
            if calls > 1:
                await hold.wait()
            await send_json(send, payload())
        app = load().BoundedSummary(original, wait_seconds=.01, revision_witness=lambda: revision[0])
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
            first = await client.get(BASE)
            assert first.json()['postprocessing_registered_ledger']['server_observation']['available'] is True
            revision[0] = 2  # A synthetic deletion or other control-DB change.
            second = await client.get(BASE)
            assert second.status_code == 503 or second.json()['postprocessing_registered_ledger']['available'] is False
            hold.set()
    asyncio.run(exercise())


def test_verified_response_without_valid_internal_deadline_fails_closed():
    async def exercise():
        async def original(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': payload()})
        app = load().BoundedSummary(original, wait_seconds=.01)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
            result = await client.get(BASE)
            assert result.json()['postprocessing_registered_ledger']['available'] is False
    asyncio.run(exercise())


def test_other_routes_pass_through_without_summary_caching():
    async def exercise():
        async def original(scope, receive, send):
            await send_json(send, b'{"synthetic":true}')
        app = load().BoundedSummary(original, max_inflight=2, wait_seconds=.01, max_age_seconds=15)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
            assert (await client.get('/another-route')).json() == {'synthetic': True}
    asyncio.run(exercise())


@pytest.mark.parametrize('phase', ['queue', 'summary', 'preprocessing', 'llm'])
def test_real_candidate_api_stays_bounded_when_each_source_blocks(phase):
    async def exercise():
        blocked = threading.Event()
        entered = threading.Event()
        def maybe_wait(name):
            if phase == name:
                entered.set()
                blocked.wait(timeout=6)
        class Queue:
            def snapshot(self, **_):
                maybe_wait('queue')
                return dict(receiver_status='running', receiver_session_id='synthetic',
                    scan_source_instance='synthetic-source', waiting_available=True,
                    upstream_scan_verified=True, upstream_scan_completed_at='2026-11-01T00:00:00Z',
                    waiting_llm_total=0, in_progress_count=0)
        class Observer:
            def summary(self, _):
                maybe_wait('summary')
                return dict(source='prefect', scope='all_tasks', available=True, counts={})
            def preprocessing_results(self, *_):
                maybe_wait('preprocessing')
                return {'available': True, 'counts': {'passed': 2}}
            def llm_results(self, *_):
                maybe_wait('llm')
                return {'available': True, 'counts': {'passed': 3}}
        spec = importlib.util.spec_from_file_location('synthetic_bounded_candidate_api_' + phase, HERE / 'waiting_api.py')
        api = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(api)
        app = load().BoundedSummary(api.create_app(Queue(), ticket_observer=Observer()),
                                    max_inflight=2, wait_seconds=.05, max_age_seconds=15)
        try:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
                start = time.monotonic()
                result = await client.get(BASE, headers={'Origin': 'http://127.0.0.1:8795'})
                assert result.status_code == 503 and time.monotonic() - start < 1
                assert result.headers['access-control-allow-origin'] == 'http://127.0.0.1:8795'
                assert entered.wait(timeout=1)
                blocked.set()
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline:
                    await asyncio.sleep(.03)
                    result = await client.get(BASE)
                    if result.status_code == 200:
                        break
                assert result.status_code == 200
                assert result.json()['preprocessing_results']['counts']['passed'] == 2
                assert result.json()['llm_results']['counts']['passed'] == 3
        finally:
            blocked.set()
    asyncio.run(exercise())
