"""Exercise the manual self-test's real health gate with synthetic samples.

The samples and case directory are local only; no RAG or NAS call is made.
"""
import ast
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import math
import os
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import time

import pytest
from native_ingest import FreshSubmissionProof


SCRIPT = Path(__file__).resolve().parents[1] / 'live_rag_selftest.py'


def original_gate(tmp_path, samples, *, runtime=None, return_namespace=False):
    """Load the exact nested function used by the manual test coordinator."""
    tree = ast.parse(SCRIPT.read_text(encoding='utf8'), filename=str(SCRIPT))
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'main')
    gate = next(n for n in main.body if isinstance(n, ast.FunctionDef) and n.name == 'refresh_runtime')
    executable = ast.fix_missing_locations(ast.Module(body=[gate], type_ignores=[]))
    cache = tmp_path / 'observer.json'

    class Sftp:
        def close(self): pass
        def get_channel(self): return self
        def settimeout(self, _timeout): pass

    class SSH:
        channel_timeout = 10
        def open_sftp(self): return Sftp()
        def get_transport(self): return self

    class Monitor:
        def __init__(self): self.calls = 0
        def publish_native_runtime(self, _ssh, config_file, **_kwargs):
            value = samples[min(self.calls, len(samples)-1)]
            self.calls += 1
            output = Path(json.loads(Path(config_file).read_text(encoding='utf8'))['output_path'])
            temporary = output.with_name(output.name + '.fake')
            temporary.write_text(json.dumps(value), encoding='utf8')
            temporary.replace(output)

    class Memory:
        last_sample = {'ready': True, 'available_gib': 32.0, 'observed_at': time.time()}
        def can_admit(self): return True

    monitor = Monitor()
    state = {}
    namespace = {'__builtins__': __builtins__, 'json': json, 'os': os, 'time': time,
                 'math': math, 'Path': Path, 'tempfile': tempfile,
                 'ssh': SSH(), 'expected_observer': {}, 'monitor_config': {},
                 'monitor': monitor, 'case': tmp_path, 'password': 'synthetic-not-a-credential',
                 'observer_cache': cache, 'native_runtime_readiness': lambda *_args, **_kwargs: runtime or {
                     'state': 'ready', 'ready': True, 'reason_code': 'executor_ready', 'observed_at': time.time(),
                     'ready_valid_until': time.time() + 5},
                 'raw': SimpleNamespace(base_url='http://fake'),
                 'config': SimpleNamespace(native_service='http://fake', native_pipeline_id='test'),
                 'memory_config': object(), '_local_native_target': lambda _config: 4,
                 'runtime_snapshot': {'observation': None}, 'memory': Memory(),
                 'state': state, 'update': lambda: None,
                 'health_wait_seconds': .1, 'health_poll_seconds': .001}
    exec(compile(executable, str(SCRIPT), 'exec'), namespace)
    result = (namespace['refresh_runtime'], monitor, state)
    return (*result, namespace) if return_namespace else result


def sample(*, active=None, pending=0, lag=0):
    return {'active_documents': [] if active is None else active,
            'broker': {'group': {'pending': pending, 'lag': lag}}}


def test_transient_busy_is_resampled_before_any_native_submission(tmp_path):
    check, monitor, _ = original_gate(tmp_path, [sample(active=[{'document_id': 'finished-test'}]), sample()])
    check()
    assert monitor.calls == 2


def test_failed_observation_is_persisted_instead_of_reusing_old_success(tmp_path):
    check, _monitor, state = original_gate(tmp_path, [sample(pending=None)])
    with pytest.raises((RuntimeError, TimeoutError)):
        check()
    assert state['last_runtime']['reason_code'] == 'broker_observation_unknown'
    events = [json.loads(line) for line in (tmp_path / 'rag-runtime-events.jsonl').read_text(encoding='utf8').splitlines()]
    assert events[-1]['reason_code'] == 'broker_observation_unknown'


def test_busy_final_recheck_returns_immediately_and_keeps_its_reason(tmp_path):
    check, monitor, state = original_gate(tmp_path, [sample(pending=1), sample()])
    with pytest.raises((RuntimeError, TimeoutError)):
        check(wait=False)
    assert monitor.calls == 1
    assert state['last_runtime']['reason_code'] == 'broker_busy'


def test_paused_executor_is_not_automatically_waited_or_resumed(tmp_path):
    runtime = {'state': 'paused', 'ready': False, 'reason_code': 'executor_frozen', 'observed_at': time.time()}
    check, monitor, state = original_gate(tmp_path, [sample()], runtime=runtime)
    with pytest.raises(InterruptedError):
        check()
    assert monitor.calls == 1
    assert state['last_runtime']['reason_code'] == 'executor_paused'


def test_paused_upload_policy_is_not_automatically_waited_or_resumed(tmp_path):
    check, monitor, state, ns = original_gate(tmp_path, [sample()], return_namespace=True)
    class Memory:
        last_sample = None
        def can_admit(self):
            if self.last_sample is None:
                self.last_sample = {'ready': False, 'reason': 'admission_paused', 'observed_at': time.time()}
                return False
            self.last_sample = {'ready': True, 'available_gib': 32, 'observed_at': time.time()}
            return True
    ns['memory'] = Memory()
    with pytest.raises(InterruptedError):
        check()
    assert monitor.calls == 1
    assert state['last_runtime']['reason_code'] == 'upload_admission_paused'


@pytest.mark.parametrize('initial_target,final_target,expected_collections', [(0, 4, 0), (4, 0, 1)])
def test_upload_pause_is_checked_even_if_runtime_is_unknown(
        tmp_path, initial_target, final_target, expected_collections):
    runtime = {'state': 'unknown', 'ready': False, 'reason_code': 'unavailable', 'observed_at': time.time()}
    check, monitor, state, ns = original_gate(tmp_path, [sample()], runtime=runtime, return_namespace=True)
    targets = iter([initial_target, final_target, 4])
    ns['_local_native_target'] = lambda _config: next(targets, 4)
    with pytest.raises(InterruptedError):
        check()
    assert monitor.calls == expected_collections
    assert state['last_runtime']['reason_code'] == 'upload_admission_paused'


def test_real_local_pause_policy_is_checked_without_runtime_readiness(tmp_path):
    import live_rag_selftest as app
    check, monitor, state, ns = original_gate(tmp_path, [sample()], return_namespace=True)
    policy = tmp_path / 'synthetic-policy.json'
    policy.write_text(json.dumps({'operator_upload_paused': True, 'native_workers': 4}), encoding='utf8')
    ns['memory_config'] = SimpleNamespace(policy_path=policy, policy_identity={}, max_native_workers=8,
                                           native_transport_verified=True)
    ns['_local_native_target'] = app._local_native_target
    with pytest.raises(InterruptedError):
        check()
    assert monitor.calls == 0
    assert state['last_runtime']['reason_code'] == 'upload_admission_paused'


def test_interrupted_observer_records_failure_instead_of_stale_success(tmp_path):
    check, _monitor, state, ns = original_gate(tmp_path, [sample()], return_namespace=True)
    state['last_runtime'] = {'reason_code': 'admitted'}
    def interrupted():
        raise InterruptedError('synthetic observer interruption')
    ns['ssh'].open_sftp = interrupted
    with pytest.raises(InterruptedError):
        check(wait=False)
    assert state['last_runtime']['reason_code'] == 'observer_error'
    assert state['last_runtime']['error_type'] == 'InterruptedError'


def test_busy_wait_is_bounded_and_records_each_sample(tmp_path):
    check, monitor, state = original_gate(tmp_path, [sample(lag=1)])
    started = time.monotonic()
    with pytest.raises((RuntimeError, TimeoutError)):
        check()
    assert 1 < monitor.calls < 500
    assert time.monotonic() - started < 2
    events = (tmp_path / 'rag-runtime-events.jsonl').read_text(encoding='utf8').splitlines()
    assert len(events) == monitor.calls
    assert state['last_runtime']['reason_code'] == 'broker_busy'


@pytest.mark.parametrize('malformed', [[], {'broker': []}, {'active_documents': [], 'broker': {'group': []}}])
def test_malformed_observation_is_recorded_as_unknown_and_never_admitted(tmp_path, malformed):
    check, monitor, state = original_gate(tmp_path, [malformed])
    with pytest.raises((RuntimeError, TimeoutError)):
        check(wait=False)
    assert monitor.calls == 1
    assert state['last_runtime']['reason_code'] == 'broker_observation_unknown'


def test_collector_must_replace_cache_in_this_sample(tmp_path):
    check, monitor, _state, ns = original_gate(tmp_path, [sample()], return_namespace=True)
    ns['observer_cache'].write_text(json.dumps(sample()), encoding='utf8')
    monitor.publish_native_runtime = lambda *_args, **_kwargs: None
    with pytest.raises((RuntimeError, TimeoutError, ValueError)):
        check(wait=False)


def test_wait_deadline_is_checked_before_starting_another_collection(tmp_path):
    check, monitor, _state, ns = original_gate(tmp_path, [sample(pending=1)], return_namespace=True)
    ns['health_wait_seconds'] = .01
    ns['health_poll_seconds'] = .05
    with pytest.raises((RuntimeError, TimeoutError)):
        check()
    assert monitor.calls == 1


def test_executor_and_memory_gate_use_the_same_snapshot(tmp_path):
    check, _monitor, _state, ns = original_gate(tmp_path, [sample()], return_namespace=True)
    captured = []
    def readiness(observation, **_kwargs):
        captured.append(('runtime', observation))
        # Simulate a concurrent writer changing the cache after collection.
        ns['observer_cache'].write_text(json.dumps(sample(pending=1)), encoding='utf8')
        return {'state': 'ready', 'ready': True, 'observed_at': time.time(),
                'ready_valid_until': time.time() + 5}
    class Memory:
        last_sample = None
        def can_admit(self):
            captured.append(('memory', ns['runtime_snapshot']['observation']))
            self.last_sample = {'ready': True, 'observed_at': time.time(), 'available_gib': 32}
            return True
    ns['native_runtime_readiness'] = readiness
    ns['memory'] = Memory()
    ns['config'] = SimpleNamespace(native_service='http://fake', native_pipeline_id='test')
    check(wait=False)
    assert [row[0] for row in captured] == ['runtime', 'memory']
    assert captured[0][1] is captured[1][1]
    assert captured[1][1]['broker']['group']['pending'] == 0


def test_expired_memory_sample_cannot_be_admitted(tmp_path):
    check, _monitor, _state, ns = original_gate(tmp_path, [sample()], return_namespace=True)
    ns['memory'].last_sample['observed_at'] = time.time() - 20
    with pytest.raises((RuntimeError, TimeoutError, ValueError)):
        check(wait=False)


def test_bypassed_memory_is_not_an_isolated_selftest_admission(tmp_path):
    check, _monitor, _state, ns = original_gate(tmp_path, [sample()], return_namespace=True)
    ns['memory'].last_sample.update(observed_at=time.time(), memory_admission_bypassed=True)
    with pytest.raises((RuntimeError, TimeoutError, ValueError)):
        check(wait=False)


def test_final_health_sample_follows_slow_document_identity_reads():
    tree = ast.parse(SCRIPT.read_text(encoding='utf8'), filename=str(SCRIPT))
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'main')
    fresh = next(n for n in main.body if isinstance(n, ast.FunctionDef) and n.name == 'fresh')
    module = ast.fix_missing_locations(ast.Module(body=[fresh], type_ignores=[]))
    events = []
    config = SimpleNamespace(policy_path='synthetic', temporary_policy_sha256='hash',
        native_transport_verified=True, policy_identity={'deployment': 'test'},
        memory_settings={'test': True}, native_pipeline_id='pipeline')
    class Publisher:
        def documents(self, *_args, **_kwargs):
            events.append('documents')
            return [{'id': 'document', 'pipeline_id': 'pipeline', 'status': '0',
                     'run': 'UNSTART', 'progress': 0, 'meta_fields': {'owner': 'test'}}]
        def chunks(self, *_args):
            events.append('chunks')
            return []
        def document_bytes(self, *_args):
            return b'sealed'
        def _download(self, *_args):
            events.append('download')
            return b'sealed'
    def health(*, wait):
        assert wait is False
        events.append('health')
        return {'valid_until_monotonic': time.monotonic() + 5}
    ns = {'__builtins__': __builtins__, 'Path': Path, 'targets': {'test-dataset': 'S1'},
          'receipts': {'S1': {'body': {'document_id': 'document', 'identity': {'owner': 'test'}}}},
          'formal': set(), 'state': {'datasets': {'S1': {'state': 'created_and_verified'}}},
          'config': config, 'digest': lambda _path: 'hash', 'load_native_batch_config': lambda *_: config,
          'raw': object(), 'pub': Publisher(), 'task': {}, 'packet': {},
          'refresh_runtime': health, 'FreshSubmissionProof': FreshSubmissionProof}
    exec(compile(module, str(SCRIPT), 'exec'), ns)
    binding = {'dataset_id': 'test-dataset', 'document_id': 'document'}
    proof = ns['fresh'](binding)
    assert proof.binding == tuple(sorted(binding.items()))
    assert events[-1] == 'health'
    assert events.index('download') < len(events) - 1
    assert proof.valid_until > time.monotonic()


def test_manual_coordinator_rejects_a_second_concurrent_ingest():
    tree = ast.parse(SCRIPT.read_text(encoding='utf8'), filename=str(SCRIPT))
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'main')
    cls = next(n for n in main.body if isinstance(n, ast.ClassDef) and n.name == 'RecordingNativeClient')
    cls.body = [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == 'ingest']
    executable = ast.fix_missing_locations(ast.Module(body=[cls], type_ignores=[]))
    entered, release = threading.Event(), threading.Event()
    class Base:
        def ingest(self, *_args, **_kwargs):
            entered.set()
            assert release.wait(3)
            return 'done'
    ns = {'__builtins__': __builtins__, 'NativeIngestClient': Base,
          'selftest_ingest_guard': threading.Lock()}
    exec(compile(executable, str(SCRIPT), 'exec'), ns)
    client = ns['RecordingNativeClient']()
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(client.ingest)
        try:
            assert entered.wait(2)
            with pytest.raises(RuntimeError, match='already submitting'):
                client.ingest()
        finally:
            release.set()
        assert first.result(timeout=3) == 'done'


def test_real_observer_accepts_the_isolated_sample_cache(tmp_path):
    check, _mock_monitor, state, ns = original_gate(tmp_path, [sample()], return_namespace=True)
    observer_path = Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py')
    spec = importlib.util.spec_from_file_location('synthetic_runtime_observer', observer_path)
    observer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(observer)
    rows = [sample(pending=1), sample()]
    sent = []
    class Channel:
        def __init__(self, row):
            self.data = json.dumps({'schema': 'rag-native-runtime-observation/1',
                'service': 'http://fake', 'pipeline_id': 'test',
                'identity': {'container_id': 'container-test'}, **row}).encode('utf8')
            self.payload = bytearray()
            self.closed = False
        def settimeout(self, _value): pass
        def exec_command(self, _command): pass
        def send(self, data): self.payload.extend(data); return len(data)
        def shutdown_write(self): sent.append(json.loads(self.payload))
        def recv_ready(self): return bool(self.data)
        def recv(self, _size): result, self.data = self.data, b''; return result
        def recv_stderr_ready(self): return False
        def exit_status_ready(self): return not self.data
        def recv_exit_status(self): return 0
        def close(self): self.closed = True
    class SSH:
        channel_timeout = 10
        def get_transport(self): return self
        def open_sftp(self): return self
        def close(self): pass
        def open_session(self, timeout):
            assert 0 < timeout <= 25
            return Channel(rows[len(sent)])
    ns.update(monitor=observer, ssh=SSH(), monitor_config={
        'schema': 'rag-runtime-monitor/1', 'enabled': True, 'container': 'test',
        'container_id': 'container-test', 'service': 'http://fake', 'pipeline_id': 'test',
        'stream': 'stream-test', 'consumer_group': 'group-test', 'observer_script': '/tmp/observer.py'})
    check()
    assert len(sent) == 2
    assert sent[0]['previous'] is None
    assert sent[1]['previous']['broker']['group']['pending'] == 1
    assert state['last_runtime']['reason_code'] == 'admitted'


def test_real_readiness_and_memory_gate_share_bound_synthetic_observation(tmp_path):
    import copy
    import live_rag_selftest as app
    from preextract_native_runtime import HEARTBEAT_SOURCE_SHA256
    now = time.time()
    observation = {**sample(), 'schema': 'rag-native-runtime-observation/1',
        'service': 'http://fake', 'pipeline_id': 'test', 'observed_at': now,
        'startup_enabled': True, 'collection_complete': True, 'collection_errors': [],
        'heartbeat_source_sha256': HEARTBEAT_SOURCE_SHA256,
        'identity': {'server': 'fake', 'container_id': 'container', 'container_started_at': 'start',
                     'boot_id': 'boot', 'pid_namespace': 'namespace', 'stream': 'stream',
                     'consumer_group': 'group'},
        'workers': [{'pid': 1, 'worker_id': 'worker', 'starttime': 1, 'state': 'S',
                     'thread_scan_complete': True, 'broker_consumer_registered': True,
                     'thread_states': {'1': 'S'}, 'heartbeat_at': now,
                     'heartbeat_interval_seconds': 1, 'heartbeat_max_age_seconds': 10,
                     'heartbeat_process_binding': 'pid_name_boot_at_after_kernel_start'}]}
    check, _monitor, state, ns = original_gate(tmp_path, [observation], return_namespace=True)
    config = SimpleNamespace(native_service='http://fake', native_pipeline_id='test', policy_path=None,
        native_transport_verified=True, memory_profile='normal', min_available_gib=1, max_native_workers=8)
    memory_config = copy.copy(config)
    memory_config.native_runtime_observation = lambda: ns['runtime_snapshot']['observation']
    class MemoryMonitor:
        def read(self):
            return {'state': 'available', 'total_bytes': 32 * 2**30,
                    'used_bytes': 4 * 2**30, 'observed_at': time.time()}
    ns.update(config=config, native_runtime_readiness=app.native_runtime_readiness,
              memory=app.NativeMemoryGate(memory_config, monitor=MemoryMonitor()))
    result = check(wait=False)
    assert result['reason_code'] == 'admitted'
    assert result['valid_until_monotonic'] > time.monotonic()
    assert state['last_runtime']['memory_ready'] is True
    assert not hasattr(config, 'native_runtime_observation')
