"""Audited request boundaries and shared circuit state; synthetic HTTP only."""
import copy
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import pytest

from agent_client import AgentClient, AgentError, AgentUnavailable
from stage_cache import StageCache
from vision_circuit import VisionCircuit
from test_vision_failover import _cfg, _http, _ok, _png, _Opener, _Response


def client(tmp_path, opener, *, vision=False, stop=None, config=None):
    cfg = copy.deepcopy(config or _cfg()) if vision else {
        'base_url': 'https://synthetic.invalid/v1', 'text_model': 'synthetic',
        'attempts': 1, 'concurrency': {'enabled': True, 'max_retries': 1,
                                     'backoff_base_seconds': 0}}
    result = AgentClient(cfg, 'synthetic-text-key', stop,
                         'synthetic-primary-key', 'synthetic-fallback-key', state_root=tmp_path)
    result.opener = opener
    return result


def invoke(agent, vision):
    return agent.call('screen', {'text': 'synthetic'}, [_png()] if vision else ())


@pytest.mark.parametrize('vision', [False, True])
def test_stop_interrupts_wait_for_llm_slot(tmp_path, vision):
    waiting = threading.Event()
    stop = threading.Event()

    class Gate:
        def __init__(self):
            self.semaphore = threading.BoundedSemaphore(1)
            self.semaphore.acquire()

        def acquire(self, *args, **kwargs):
            waiting.set()
            return self.semaphore.acquire(*args, **kwargs)

        def release(self):
            self.semaphore.release()

    agent = client(tmp_path, _Opener(_ok()), vision=vision, stop=stop)
    gate = agent.llm_gate = Gate()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(invoke, agent, vision)
        assert waiting.wait(2), 'request did not reach the full real semaphore'
        stop.set()
        try:
            with pytest.raises(AgentUnavailable, match='停止'):
                future.result(timeout=0.5)
        finally:
            gate.release()


@pytest.mark.parametrize('vision', [False, True])
def test_stop_after_slot_granted_blocks_http_send(tmp_path, vision):
    stop = threading.Event()

    class Gate:
        def acquire(self, *args, **kwargs):
            stop.set()
            return True

        def release(self):
            pass

    agent = client(tmp_path, _Opener(_ok()), vision=vision, stop=stop)
    agent.llm_gate = Gate()
    with pytest.raises(AgentUnavailable, match='停止'):
        invoke(agent, vision)


@pytest.mark.parametrize('vision', [False, True])
def test_canceled_before_send_has_no_request_or_unknown_usage(tmp_path, vision):
    from billing import Ledger
    stop = threading.Event()

    class Gate:
        def acquire(self, *args, **kwargs):
            stop.set()
            return True

        def release(self):
            pass

    agent = client(tmp_path, _Opener(_ok()), vision=vision, stop=stop)
    agent.ledger = Ledger(tmp_path)
    agent.llm_gate = Gate()
    with pytest.raises(AgentUnavailable):
        invoke(agent, vision)
    assert not agent.opener.requests
    summary = agent.ledger.summary()
    assert summary['requests'] == summary['unknown_usage'] == summary['primary_requests'] == 0


@pytest.mark.parametrize('vision', [False, True])
def test_stop_during_durable_billing_begin_blocks_send(tmp_path, monkeypatch, vision):
    from billing import Ledger
    stop = threading.Event()
    agent = client(tmp_path, _Opener(_ok()), vision=vision, stop=stop)
    agent.ledger = Ledger(tmp_path)
    begin = agent.ledger.begin

    def stop_after_durable_begin(*args, **kwargs):
        receipt = begin(*args, **kwargs)
        stop.set()
        return receipt

    monkeypatch.setattr(agent.ledger, 'begin', stop_after_durable_begin)
    with pytest.raises(AgentUnavailable):
        invoke(agent, vision)
    assert not agent.opener.requests
    records = agent.ledger.records()
    assert len(records) == 1
    assert records[0]['outcome'] == 'cancelled_before_send'
    assert records[0]['total_tokens'] == 0
    summary = agent.ledger.summary()
    assert summary['requests'] == summary['unknown_usage'] == summary['primary_requests'] == 0


@pytest.mark.parametrize('vision', [False, True])
def test_billing_begin_failure_releases_slot_without_sending(tmp_path, vision):
    from billing import Ledger

    class BrokenLedger(Ledger):
        def begin(self, *args, **kwargs):
            raise PermissionError('synthetic ledger write failure')

    agent = client(tmp_path, _Opener(_ok()), vision=vision)
    agent.ledger = BrokenLedger(tmp_path)
    agent.llm_gate = threading.BoundedSemaphore(1)
    with pytest.raises(AgentError, match='费用记录'):
        invoke(agent, vision)
    assert agent.llm_gate.acquire(blocking=False)
    agent.llm_gate.release()
    assert not agent.opener.requests
    assert agent.ledger.summary()['requests'] == 0


@pytest.mark.parametrize('attempts', [3, 4, 8])
def test_text_transient_attempts_match_configured_total(tmp_path, monkeypatch, attempts):
    monkeypatch.setattr('agent_client.time.sleep', lambda _: None)
    opener = _Opener(*[_http(503) for _ in range(10)])
    agent = client(tmp_path, opener)
    agent.config['concurrency']['max_retries'] = attempts
    with pytest.raises(AgentError):
        invoke(agent, False)
    assert len(opener.requests) == attempts


def test_vision_uses_smaller_local_attempt_limit(tmp_path, monkeypatch):
    monkeypatch.setattr('agent_client.time.sleep', lambda _: None)
    cfg = _cfg(concurrency={'enabled': True, 'max_retries': 8},
               vision_model={'fallback': {'enabled': False},
                             'retry': {'max_tries': 3, 'backoff_base_seconds': 0}})
    opener = _Opener(*[_http(503) for _ in range(10)])
    agent = client(tmp_path, opener, vision=True, config=cfg)
    with pytest.raises(AgentError):
        invoke(agent, True)
    assert len(opener.requests) == 3


def test_legacy_vision_uses_smaller_local_attempt_limit(tmp_path, monkeypatch):
    monkeypatch.setattr('agent_client.time.sleep', lambda _: None)
    cfg = {'base_url': 'https://synthetic.invalid/v1', 'vision_model': 'synthetic',
           'attempts': 3, 'concurrency': {'enabled': True, 'max_retries': 8,
                                        'backoff_base_seconds': 0}}
    opener = _Opener(*[_http(503) for _ in range(10)])
    agent = client(tmp_path, opener, vision=True, config=cfg)
    with pytest.raises(AgentError):
        invoke(agent, True)
    assert len(opener.requests) == 3


def test_vision_total_budget_covers_primary_and_fallback(tmp_path, monkeypatch):
    monkeypatch.setattr('agent_client.time.sleep', lambda _: None)
    cfg = _cfg(concurrency={'enabled': True, 'max_retries': 3},
               vision_model={'retry': {'max_tries': 3, 'backoff_base_seconds': 0}})
    opener = _Opener(_http(503), _http(503), _http(503), _ok())
    agent = client(tmp_path, opener, vision=True, config=cfg)
    with pytest.raises(AgentError):
        invoke(agent, True)
    assert len(opener.requests) == 3


def test_vision_fallback_can_use_remaining_total_budget(tmp_path, monkeypatch):
    monkeypatch.setattr('agent_client.time.sleep', lambda _: None)
    cfg = _cfg(concurrency={'enabled': True, 'max_retries': 4},
               vision_model={'retry': {'max_tries': 3, 'backoff_base_seconds': 0}})
    opener = _Opener(_http(503), _http(503), _http(503), _ok(result={'decision': 'pass', 'reason': 'fallback'}))
    agent = client(tmp_path, opener, vision=True, config=cfg)
    assert invoke(agent, True)['reason'] == 'fallback'
    assert len(opener.requests) == 4


@pytest.mark.parametrize('threshold', [1, 3])
def test_selected_fallback_channel_reuses_its_accepted_cache(tmp_path, threshold):
    cfg = _cfg(vision_model={'circuit_breaker': {'failure_threshold': threshold, 'cooldown_seconds': 300}})
    first = client(tmp_path, _Opener(_http(503), _ok(result={'decision': 'pass', 'reason': 'fallback result'})),
                   vision=True, config=cfg)
    cache = StageCache(tmp_path)
    first.bind_stage_cache(cache, 'a' * 64, 'fixture-v1')
    result = invoke(first, True)
    first.accept_cached_result(result)
    if threshold == 3:
        assert first.vision_circuit.snapshot()['circuit']['primary']['state'] == 'closed'
    resumed = client(tmp_path, _Opener(), vision=True, config=cfg)
    resumed.bind_stage_cache(cache, 'a' * 64, 'fixture-v1')
    assert invoke(resumed, True)['reason'] == 'fallback result'
    assert resumed.last_call_cache_hit
    assert not resumed.opener.requests


def test_automatic_cache_prefers_selected_primary_over_fallback(tmp_path):
    cache = StageCache(tmp_path)
    agent = client(tmp_path, _Opener(_ok(result={'decision': 'pass', 'reason': 'fallback'})), vision=True)
    agent.bind_stage_cache(cache, 'a' * 64, 'fixture-v1')
    result = agent.call('screen', {'text': 'synthetic'}, [_png()], channel='fallback')
    agent.accept_cached_result(result)
    agent.opener = _Opener(_ok(result={'decision': 'pass', 'reason': 'primary'}))
    result = agent.call('screen', {'text': 'synthetic'}, [_png()], channel='primary')
    agent.accept_cached_result(result)
    resumed = client(tmp_path, _Opener(), vision=True)
    resumed.bind_stage_cache(cache, 'a' * 64, 'fixture-v1')
    assert invoke(resumed, True)['reason'] == 'primary'
    assert resumed.last_call_cache_hit


@pytest.mark.parametrize('mode', ['forced_primary', 'fallback_disabled'])
def test_unallowed_fallback_cache_does_not_replace_primary_request(tmp_path, mode):
    cfg = _cfg(vision_model={'circuit_breaker': {'failure_threshold': 3}})
    cache = StageCache(tmp_path)
    first = client(tmp_path, _Opener(_http(503), _ok(result={'decision': 'pass', 'reason': 'cached fallback'})),
                   vision=True, config=cfg)
    first.bind_stage_cache(cache, 'a' * 64, 'fixture-v1')
    result = invoke(first, True)
    first.accept_cached_result(result)
    if mode == 'fallback_disabled':
        cfg['vision_model']['fallback']['enabled'] = False
    resumed = client(tmp_path, _Opener(_ok(result={'decision': 'pass', 'reason': 'fresh primary'})),
                     vision=True, config=cfg)
    resumed.bind_stage_cache(cache, 'a' * 64, 'fixture-v1')
    result = (resumed.call('screen', {'text': 'synthetic'}, [_png()], channel='primary')
              if mode == 'forced_primary' else invoke(resumed, True))
    assert result['reason'] == 'fresh primary'
    assert not resumed.last_call_cache_hit


def test_malformed_fallback_cache_preflight_does_not_block_primary(tmp_path):
    cfg = _cfg(vision_model={'fallback': {'enabled': True, 'base_url': 'not-a-url', 'model': ''}})
    resumed = client(tmp_path, _Opener(_ok(result={'decision': 'pass', 'reason': 'fresh primary'})),
                     vision=True, config=cfg)
    resumed.bind_stage_cache(StageCache(tmp_path), 'a' * 64, 'fixture-v1')
    assert invoke(resumed, True)['reason'] == 'fresh primary'
    assert not resumed.last_call_cache_hit


def test_forced_fallback_does_not_use_primary_cache(tmp_path):
    first = client(tmp_path, _Opener(_ok(result={'decision': 'pass', 'reason': 'primary result'})), vision=True)
    cache = StageCache(tmp_path)
    first.bind_stage_cache(cache, 'a' * 64, 'fixture-v1')
    result = invoke(first, True)
    first.accept_cached_result(result)
    first.opener = _Opener(_ok(result={'decision': 'pass', 'reason': 'fallback result'}))
    result = first.call('screen', {'text': 'synthetic'}, [_png()], channel='fallback')
    assert result['reason'] == 'fallback result'
    assert not first.last_call_cache_hit


@pytest.mark.parametrize('threshold', [1, 3])
@pytest.mark.parametrize('changed', ['source', 'version', 'model'])
def test_fallback_cache_does_not_cross_request_identity(tmp_path, changed, threshold):
    cfg = _cfg(vision_model={'circuit_breaker': {'failure_threshold': threshold}})
    cache = StageCache(tmp_path)
    first = client(tmp_path, _Opener(_http(503), _ok(result={'decision': 'pass', 'reason': 'old'})),
                   vision=True, config=cfg)
    first.bind_stage_cache(cache, 'a' * 64, 'fixture-v1')
    result = invoke(first, True)
    first.accept_cached_result(result)
    if changed == 'model':
        cfg['vision_model']['fallback']['model'] = 'changed-fallback-model'
    resumed = client(tmp_path, _Opener(_ok(result={'decision': 'pass', 'reason': 'fresh'})), vision=True, config=cfg)
    resumed.bind_stage_cache(cache, 'b' * 64 if changed == 'source' else 'a' * 64,
                            'fixture-v2' if changed == 'version' else 'fixture-v1')
    assert invoke(resumed, True)['reason'] == 'fresh'
    assert not resumed.last_call_cache_hit


@pytest.mark.parametrize('finish_reason', ['content_filter', 'tool_calls'])
def test_vision_rejects_non_normal_finish_reason(tmp_path, finish_reason):
    import json
    response = _Response(json.dumps({'choices': [{'finish_reason': finish_reason,
        'message': {'content': json.dumps({'decision': 'pass', 'reason': 'synthetic'})}}]}).encode())
    agent = client(tmp_path, _Opener(response), vision=True)
    with pytest.raises(AgentError):
        invoke(agent, True)


def test_same_path_circuit_instances_accumulate_failures(tmp_path):
    cfg = {'failure_threshold': 2, 'cooldown_seconds': 300}
    first, second = VisionCircuit(tmp_path, cfg), VisionCircuit(tmp_path, cfg)
    assert not first.failure('primary')
    assert second.failure('primary')
    assert first.before_call(True) == ('fallback', False)
    assert first.snapshot()['circuit']['primary']['consecutive_failures'] == 2


def test_same_path_circuits_admit_only_one_probe(tmp_path):
    cfg = {'failure_threshold': 1, 'cooldown_seconds': 0}
    first, second = VisionCircuit(tmp_path, cfg), VisionCircuit(tmp_path, cfg)
    first.failure('primary')
    assert first.before_call(True) == ('primary', True)
    assert second.before_call(True) == ('fallback', False)


def test_abandoned_probe_lease_recovers_after_restart(tmp_path, monkeypatch):
    import vision_circuit
    moment = datetime(2026, 9, 27, 12, 0, 0)

    class Clock(datetime):
        @classmethod
        def now(cls):
            return moment

    monkeypatch.setattr(vision_circuit, 'datetime', Clock)
    cfg = {'failure_threshold': 1, 'cooldown_seconds': 0, 'probe_lease_seconds': 30}
    circuit = VisionCircuit(tmp_path, cfg)
    circuit.failure('primary')
    assert circuit.before_call(True) == ('primary', True)
    moment += timedelta(seconds=31)
    restarted = VisionCircuit(tmp_path, cfg)
    assert restarted.before_call(True) == ('primary', True)


def test_circuit_roots_remain_isolated(tmp_path):
    cfg = {'failure_threshold': 1, 'cooldown_seconds': 300}
    one, two = VisionCircuit(tmp_path / 'one', cfg), VisionCircuit(tmp_path / 'two', cfg)
    one.failure('primary')
    assert two.before_call(True) == ('primary', False)


def test_cached_primary_result_does_not_hold_half_open_probe(tmp_path):
    cfg = _cfg(vision_model={'circuit_breaker': {'failure_threshold': 1, 'cooldown_seconds': 0}})
    agent = client(tmp_path, _Opener(_ok()), vision=True, config=cfg)
    cache = StageCache(tmp_path)
    agent.bind_stage_cache(cache, 'a' * 64, 'fixture-v1')
    result = invoke(agent, True)
    agent.accept_cached_result(result)
    agent.vision_circuit.failure('primary')
    resumed = client(tmp_path, _Opener(), vision=True, config=cfg)
    resumed.bind_stage_cache(cache, 'a' * 64, 'fixture-v1')
    assert invoke(resumed, True)['decision'] == 'pass'
    assert resumed.last_call_cache_hit
    assert VisionCircuit(tmp_path, cfg['vision_model']['circuit_breaker']).before_call(True) == ('primary', True)


def test_canceled_primary_probe_releases_its_lease(tmp_path):
    cfg = _cfg(vision_model={'circuit_breaker': {'failure_threshold': 1, 'cooldown_seconds': 0}})
    stop = threading.Event()
    agent = client(tmp_path, _Opener(_ok()), vision=True, stop=stop, config=cfg)
    agent.vision_circuit.failure('primary')

    class Gate:
        def acquire(self, *args, **kwargs):
            stop.set()
            return True

        def release(self):
            pass

    agent.llm_gate = Gate()
    with pytest.raises(AgentUnavailable):
        invoke(agent, True)
    assert VisionCircuit(tmp_path, cfg['vision_model']['circuit_breaker']).before_call(True) == ('primary', True)


def test_nontransient_probe_error_releases_its_lease(tmp_path):
    cfg = _cfg(vision_model={'circuit_breaker': {'failure_threshold': 1, 'cooldown_seconds': 0}})
    agent = client(tmp_path, _Opener(_http(400, 'synthetic bad request')), vision=True, config=cfg)
    agent.vision_circuit.failure('primary')
    with pytest.raises(AgentError):
        invoke(agent, True)
    assert VisionCircuit(tmp_path, cfg['vision_model']['circuit_breaker']).before_call(True) == ('primary', True)


def test_fallback_completion_keeps_other_threads_probe_lease(tmp_path):
    cfg = _cfg(vision_model={'circuit_breaker': {'failure_threshold': 1, 'cooldown_seconds': 0}})
    started, release = threading.Event(), threading.Event()

    class Transport:
        def open(self, request, timeout):
            if request.full_url.startswith('https://primary.test'):
                started.set()
                assert release.wait(3)
            return _ok()

    agent = client(tmp_path, Transport(), vision=True, config=cfg)
    agent.vision_circuit.failure('primary')
    with ThreadPoolExecutor(max_workers=1) as pool:
        primary = pool.submit(invoke, agent, True)
        assert started.wait(2)
        try:
            assert invoke(agent, True)['decision'] == 'pass'
            assert VisionCircuit(tmp_path, cfg['vision_model']['circuit_breaker']).before_call(True) == ('fallback', False)
        finally:
            release.set()
        assert primary.result()['decision'] == 'pass'


def test_expired_probe_cannot_close_newer_probe(tmp_path, monkeypatch):
    import vision_circuit
    moment = datetime(2026, 9, 27, 12, 0, 0)

    class Clock(datetime):
        @classmethod
        def now(cls):
            return moment

    monkeypatch.setattr(vision_circuit, 'datetime', Clock)
    cfg = {'failure_threshold': 1, 'cooldown_seconds': 0, 'probe_lease_seconds': 30}
    first, newer = VisionCircuit(tmp_path, cfg), VisionCircuit(tmp_path, cfg)
    first.failure('primary')
    assert first.before_call(True) == ('primary', True)
    moment += timedelta(seconds=31)
    assert newer.before_call(True) == ('primary', True)
    first.success('primary')
    assert VisionCircuit(tmp_path, cfg).before_call(True) == ('fallback', False)
    newer.success('primary')
    assert VisionCircuit(tmp_path, cfg).before_call(True) == ('primary', False)
