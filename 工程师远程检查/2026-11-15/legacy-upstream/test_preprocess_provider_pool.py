import io
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError

import pytest

from agent_client import AgentClient


def config():
    return {'base_url': 'https://lan.example.test/v1', 'text_model': 'lan-text',
            'concurrency': {'enabled': True, 'max_retries': 2, 'backoff_base_seconds': 0},
            'preprocess_routing': {'enabled': True, 'lan_limit': 2, 'official_limit': 1},
            'vision_model': {'primary': {'base_url': 'https://lan.example.test/v1', 'model': 'lan-vision'},
                             'fallback': {'enabled': True, 'base_url': 'https://official.example.test/v1', 'model': 'official-model'},
                             'retry': {'max_tries': 1, 'backoff_base_seconds': 0}}}


class Response:
    headers = {'Content-Type': 'application/json'}
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def read(self, _):
        return json.dumps({'choices': [{'finish_reason': 'stop', 'message': {'content': json.dumps(
            {'decision': 'pass', 'reason': 'fixture', 'colors': ['red', 'green', 'blue', 'yellow']})}}]}).encode()


@pytest.mark.parametrize('vision', [False, True])
def test_independent_papers_share_lan_cap_and_overflow_uses_official_key(tmp_path, vision):
    barrier = threading.Barrier(3)
    calls = []
    class Opener:
        def open(self, request, timeout):
            calls.append((request.full_url, request.get_header('Authorization'), json.loads(request.data)['model']))
            barrier.wait(timeout=3)
            return Response()
    clients = [AgentClient(config(), 'lan-key', vision_key='lan-vision-key',
                           fallback_key='official-key', state_root=tmp_path) for _ in range(3)]
    for client in clients: client.opener = Opener()
    images = ()
    if vision:
        from PIL import Image
        out = io.BytesIO(); Image.new('RGB', (10, 10)).save(out, format='PNG'); images = (out.getvalue(),)
    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(lambda c: c.call('vision_probe' if vision else 'ping', {}, images), clients))
    official = [row for row in calls if 'official.example.test' in row[0]]
    assert len(official) == 1
    assert official[0][1:] == ('Bearer official-key', 'official-model')
    assert all(r['decision'] == 'pass' for r in results)
    assert clients[0].provider_pool.snapshot()['active'] == {'lan': 0, 'official': 0}


def test_primary_vision_service_fallback_moves_capacity_and_releases_both(tmp_path):
    client = AgentClient(config(), 'lan-key', vision_key='vision-key', fallback_key='official-key', state_root=tmp_path)
    calls = []
    class Opener:
        def open(self, request, timeout):
            calls.append(request.full_url)
            if 'lan.example.test' in request.full_url:
                raise HTTPError(request.full_url, 503, 'fixture', {}, io.BytesIO(b'busy'))
            return Response()
    client.opener = Opener()
    from PIL import Image
    out = io.BytesIO(); Image.new('RGB', (10, 10)).save(out, format='PNG')
    assert client.call('vision_probe', {}, [out.getvalue()])['decision'] == 'pass'
    assert len(calls) == 2 and 'official' in calls[-1]
    assert client.provider_pool.snapshot()['active'] == {'lan': 0, 'official': 0}


def test_cancelled_wait_does_not_leak_capacity(tmp_path):
    from provider_pool import ProviderPool, ProviderCancelled
    pool = ProviderPool(1, 1)
    first = pool.reserve(); second = pool.reserve()
    stop = threading.Event(); stop.set()
    with pytest.raises(ProviderCancelled): pool.reserve(stop=stop)
    first.release(); second.release()
    assert pool.snapshot()['active'] == {'lan': 0, 'official': 0}


def test_routing_settings_round_trip_and_default_remains_opt_in(tmp_path):
    from auto_config import load_config, save_config
    cfg = load_config(tmp_path)
    assert cfg['preprocess_routing']['enabled'] is False
    cfg['preprocess_routing'] = {'enabled': True, 'lan_limit': 10, 'official_limit': 4, 'strategy': 'lan_first'}
    save_config(tmp_path, cfg)
    assert load_config(tmp_path)['preprocess_routing'] == cfg['preprocess_routing']


def test_disabled_fallback_never_receives_overflow(tmp_path):
    cfg = config(); cfg['vision_model']['fallback']['enabled'] = False
    client = AgentClient(cfg, 'lan-key', fallback_key='official-key', state_root=tmp_path)
    stop = threading.Event(); client.stop = stop
    slots = [client.provider_pool.reserve(exact='lan') for _ in range(2)]
    stop.set()
    from agent_client import AgentUnavailable
    with pytest.raises(AgentUnavailable): client.call('ping', {})
    for slot in slots: slot.release()
    assert client.provider_pool.snapshot()['selected']['official'] == 0


@pytest.mark.parametrize('vision', [False, True])
def test_connection_probe_pins_the_selected_channel(tmp_path, monkeypatch, vision):
    monkeypatch.setattr('random.SystemRandom', lambda: type('Fixed', (), {'shuffle': lambda self, values: None})())
    client = AgentClient(config(), 'lan-key', fallback_key='official-key', state_root=tmp_path)
    seen = []
    def call(task, payload, images=(), channel=None):
        seen.append(channel)
        return {'decision': 'pass', 'reason': 'fixture', 'colors': ['red', 'green', 'blue', 'yellow']}
    client.call = call
    assert ('lan-vision' if vision else 'lan-text') in client.probe(vision=vision)
    assert seen == ['primary']


@pytest.mark.parametrize('vision', [False, True])
def test_balanced_routing_uses_official_before_lan_fills(tmp_path, vision):
    cfg = config()
    cfg['preprocess_routing'].update(lan_limit=8, official_limit=16, strategy='balanced')
    barrier = threading.Barrier(2)
    calls = []
    class Opener:
        def open(self, request, timeout):
            calls.append((request.full_url, request.get_header('Authorization')))
            barrier.wait(timeout=3)
            return Response()
    clients = [AgentClient(cfg, 'lan-key', vision_key='lan-vision-key',
                           fallback_key='official-key', state_root=tmp_path) for _ in range(2)]
    for client in clients: client.opener = Opener()
    images = ()
    if vision:
        from PIL import Image
        out = io.BytesIO(); Image.new('RGB', (10, 10)).save(out, format='PNG'); images = (out.getvalue(),)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda c: c.call('vision_probe' if vision else 'ping', {}, images), clients))
    assert all(r['decision'] == 'pass' for r in results)
    assert sum('official.example.test' in url for url, _ in calls) == 1
    assert ('https://official.example.test/v1/chat/completions', 'Bearer official-key') in calls
    assert clients[0].provider_pool.snapshot()['active'] == {'lan': 0, 'official': 0}


def test_balanced_pool_never_exceeds_either_capacity_and_honors_pin():
    from provider_pool import ProviderPool
    pool = ProviderPool(2, 4, strategy='balanced')
    leases = [pool.reserve() for _ in range(6)]
    assert [lease.lane for lease in leases[:3]] == ['official', 'lan', 'official']
    assert pool.snapshot()['active'] == {'lan': 2, 'official': 4}
    stop = threading.Event(); stop.set()
    from provider_pool import ProviderCancelled
    with pytest.raises(ProviderCancelled): pool.reserve(stop=stop)
    for lease in leases: lease.release()
    with pool.reserve(exact='lan') as pinned: assert pinned.lane == 'lan'
    with pool.reserve(allow_official=False) as disabled: assert disabled.lane == 'lan'
    assert pool.snapshot()['active'] == {'lan': 0, 'official': 0}


@pytest.mark.parametrize('strategy', ['balanced', 'official_first'])
def test_balanced_strategy_persists_and_invalid_value_is_rejected(tmp_path, strategy):
    from auto_config import load_config, save_config, normalize_preprocess_routing
    cfg = load_config(tmp_path)
    cfg['preprocess_routing'].update(enabled=True, strategy=strategy)
    save_config(tmp_path, cfg)
    assert load_config(tmp_path)['preprocess_routing']['strategy'] == strategy
    with pytest.raises(ValueError): normalize_preprocess_routing({'strategy': 'typo'})


def test_official_first_keeps_slow_lan_for_overflow_and_honors_explicit_lanes():
    from provider_pool import ProviderPool
    pool = ProviderPool(2, 16, strategy='official_first')
    leases = [pool.reserve() for _ in range(18)]
    assert [lease.lane for lease in leases] == ['official']*16 + ['lan']*2
    for lease in leases: lease.release()
    with pool.reserve(exact='lan') as pinned: assert pinned.lane == 'lan'
    with pool.reserve(allow_official=False) as disabled: assert disabled.lane == 'lan'
    assert pool.snapshot()['active'] == {'lan': 0, 'official': 0}


@pytest.mark.parametrize('vision', [False, True], ids=['text', 'vision'])
def test_64_official_reservations_still_limit_complete_http_lifetimes_to_16(tmp_path, vision):
    from auto_pipeline import _ObservedGate

    cfg = config()
    cfg['concurrency'].update(papers=8, images_per_paper=8, global_llm=16, max_retries=1)
    cfg['preprocess_routing'].update(official_limit=64, strategy='official_first')
    gate = _ObservedGate(16)
    clients = [AgentClient(cfg, 'lan-key', vision_key='lan-vision-key',
                           fallback_key='official-key', state_root=tmp_path) for _ in range(64)]
    provider_pool = clients[0].provider_pool
    assert all(client.provider_pool is provider_pool for client in clients)

    lock = threading.Lock()
    all_reserved = threading.Event()
    first_wave_reading = threading.Event()
    allow_responses = threading.Event()
    open_wave = threading.Barrier(16)
    read_wave = threading.Barrier(16)
    close_wave = threading.Barrier(16)
    calls = []
    counts = {'gate_attempts': 0, 'active': 0, 'peak': 0, 'read': 0, 'closed': 0}
    acquire = gate.acquire

    def observed_acquire(*args, **kwargs):
        # Observe the existing gate boundary without changing its semaphore.
        # Provider leases have already been obtained when all 64 arrive here.
        with lock:
            counts['gate_attempts'] += 1
            if counts['gate_attempts'] == 64:
                all_reserved.set()
        return acquire(*args, **kwargs)

    gate.acquire = observed_acquire

    class HeldResponse(Response):
        def read(self, limit):
            with lock:
                counts['read'] += 1
                assert gate.current >= counts['active']
                if counts['read'] == 16:
                    first_wave_reading.set()
            read_wave.wait(timeout=10)
            assert allow_responses.wait(timeout=10), 'test controller did not release responses'
            return super().read(limit)

        def __exit__(self, *args):
            try:
                # All 16 remain active through response closure, not just open().
                close_wave.wait(timeout=10)
                with lock:
                    assert gate.current >= counts['active']
            finally:
                with lock:
                    counts['active'] -= 1
                    counts['closed'] += 1

    class Opener:
        def open(self, request, timeout):
            with lock:
                counts['active'] += 1
                counts['peak'] = max(counts['peak'], counts['active'])
                calls.append((request.full_url, request.get_header('Authorization')))
            try:
                open_wave.wait(timeout=10)
            except BaseException:
                with lock:
                    counts['active'] -= 1
                raise
            return HeldResponse()

    for client in clients:
        client.llm_gate = gate
        client.opener = Opener()
    images = ()
    if vision:
        from test_vision_failover import _png
        images = (_png(),)

    with ThreadPoolExecutor(max_workers=64) as executor:
        futures = [executor.submit(client.call, 'vision_probe' if vision else 'ping', {}, images)
                   for client in clients]
        try:
            assert all_reserved.wait(timeout=10), 'not all 64 callers reached the HTTP gate'
            assert first_wave_reading.wait(timeout=10), 'actual HTTP concurrency never reached 16'
            assert provider_pool.snapshot()['active'] == {'lan': 0, 'official': 64}
            assert gate.current == 16
            with lock:
                assert counts['active'] == counts['peak'] == 16
                assert len(calls) == counts['read'] == 16
        finally:
            allow_responses.set()
        results = [future.result(timeout=10) for future in futures]

    assert all(result['decision'] == 'pass' for result in results)
    assert calls == [('https://official.example.test/v1/chat/completions', 'Bearer official-key')] * 64
    assert counts == {'gate_attempts': 64, 'active': 0, 'peak': 16, 'read': 64, 'closed': 64}
    snapshot = provider_pool.snapshot()
    assert snapshot['selected'] == {'lan': 0, 'official': 64}
    assert snapshot['peak'] == {'lan': 0, 'official': 64}
    assert snapshot['active'] == {'lan': 0, 'official': 0}
    assert gate.current == 0 and gate.peak == 16
    # Confirm the real semaphore capacity was returned as well as its counters.
    permits = [gate.acquire(blocking=False) for _ in range(16)]
    try:
        assert all(permits) and gate.acquire(blocking=False) is False
    finally:
        for acquired in permits:
            if acquired:
                gate.release()
