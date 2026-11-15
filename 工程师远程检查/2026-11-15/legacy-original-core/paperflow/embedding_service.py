"""One exact vocabulary owner, reused through the existing authenticated pipe.

The bounded coordinator is not a graph/LLM actor or a resource action. Its locks
only protect short in-memory queue/cache operations, never IO/counting/waiting.
Cache facts qualify mathematical counts, not remote writes or publication.
"""
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass, field
import copy
import hashlib
import json
from pathlib import Path
import queue
import threading
import time
import uuid

from .package import save
from .private_files import read_owned_bytes, resolve_private_path
from .scheduling_policy import ActionDeferred
from .embedding_recovery import (RecoveryState, helper_exited, helper_identity_valid,
                                 CounterProcessExited, CounterProtocolError)

MAX_BYTES = 8 * 1024 * 1024
MAX_TEXTS = 20000


class CountsPending(ActionDeferred):
    def __init__(self):
        super().__init__('embedding_counts_not_ready')


class CountBatch(list):
    """Mathematical counts bound to one live service generation, never a permit."""
    def __init__(self, rows, counter_epoch):
        super().__init__(rows)
        self.counter_epoch = counter_epoch


def input_hash(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def validate_counts(rows, size):
    if (not isinstance(rows, list) or len(rows) != size or any(
            not isinstance(row, dict) or set(row) != {'model_tokens', 'cl100k_tokens'} or
            any(type(row[k]) is not int or row[k] < 0 for k in row) for row in rows)):
        raise CounterProtocolError('invalid exact-counter reply')


def validate_texts(texts):
    if (not isinstance(texts, list) or len(texts) > MAX_TEXTS or
            any(not isinstance(text, str) for text in texts) or
            sum(len(text.encode('utf-8')) for text in texts) > MAX_BYTES):
        raise ValueError('bounded exact-counter input required')


def owned_path(root, path):
    root, path = Path(root).absolute(), Path(path).absolute()
    # Reject links before resolving, including a junction at an ancestor.
    for part in (path, *path.parents):
        if part.is_symlink() or part.is_junction():
            raise ValueError('linked counter descriptor')
    if not resolve_private_path(path).is_relative_to(resolve_private_path(root)):
        raise ValueError('counter descriptor outside owner')
    return path


def write_descriptor(root, value):
    root = Path(root)
    path = owned_path(root, root / (uuid.uuid4().hex + '.json'))
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
    if len(raw) > MAX_BYTES:
        raise ValueError('counter descriptor too large')
    root.mkdir(parents=True, exist_ok=True)
    # Reuse atomic temporary-file publication. The file is never a pass bit.
    save(path, value)
    raw = path.read_bytes()
    if len(raw) > MAX_BYTES:
        path.unlink(); raise ValueError('counter descriptor too large')
    return dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest())


def read_descriptor(root, descriptor, identity):
    if not isinstance(descriptor, dict) or set(descriptor) != {'path', 'sha256'}:
        raise ValueError('invalid counter descriptor')
    path = owned_path(root, descriptor['path'])
    raw = read_owned_bytes(root,path,MAX_BYTES)
    if len(raw) > MAX_BYTES or hashlib.sha256(raw).hexdigest() != descriptor['sha256']:
        raise ValueError('counter descriptor hash or size mismatch')
    value = json.loads(raw)
    if not isinstance(value, dict) or any(value.get(k) != v for k, v in identity.items()):
        raise ValueError('counter descriptor identity mismatch')
    return value


@dataclass
class _Request:
    texts: list
    counter_epoch: str
    maintenance: object = None
    done: threading.Event = field(default_factory=threading.Event)
    result: list | None = None
    error: Exception | None = None
    abandoned: bool = False


class CountBroker:
    def __init__(self, policy, settings, *, factory=None, max_entries=50000, timeout=110,
                 recovery=False, restart_delays=(1, 2, 4), allowed=None):
        if not 1 <= max_entries <= 100000 or not 0 < timeout <= 110:
            raise ValueError('invalid counter bounds')
        self.policy, self.settings = policy, copy.deepcopy(settings)
        self.identity = {k: getattr(policy, k) for k in
                         ('model_sha256', 'libllama_sha256', 'cl100k_sha256')}
        self.max_entries, self.timeout = max_entries, timeout
        self.lock = threading.Lock()
        self.cache = OrderedDict()
        self.queue = queue.Queue(128)
        self.closed = threading.Event()
        self.counter, self.failure = None, None
        self.owner_process = None
        self.maintenance_scope = None
        self.lifecycle = RecoveryState(recovery, restart_delays)
        self.service_id = uuid.uuid4().hex
        self.allowed = allowed or (lambda: True)
        self.injected = factory is not None
        self.loads, self.counted_inputs, self.cache_hits = 0, 0, 0
        self.runtime = {}
        self.factory = factory or self._factory
        self.artifacts = self._artifact_state()
        self.thread = threading.Thread(target=self._run, daemon=True, name='paperflow-exact-counter')
        self.thread.start()

    def _factory(self):
        from .embedding_counter import OfflineCounter,file_sha
        if (not self.policy.counter_runtime_sha256 or not self.settings.get('runtime_manifest') or
                file_sha(self.settings['runtime_manifest']) != self.policy.counter_runtime_sha256):
            raise ValueError('accepted exact counter runtime fingerprint required')
        if not 0 < self.settings.get('timeout',30) <= 30:
            raise ValueError('managed counter backend timeout must be at most 30 seconds')
        return OfflineCounter(**self.settings, **self.identity, unified=True)

    def _artifact_state(self):
        if not self.settings:
            return ()  # Only explicitly injected test backends have no artifact paths.
        from .embedding_counter import CL100K_CACHE
        paths = [Path(self.settings['model_path']), Path(self.settings['library_path']),
                 Path(self.settings['cache_dir']) / CL100K_CACHE]
        if self.settings.get('runtime_manifest'):
            paths.append(Path(self.settings['runtime_manifest']))
            # The loaded process owns immutable module state. The complete runtime
            # file manifest is SHA checked once before load, not per cache lookup.
        if self.lifecycle.enabled:
            paths.extend(Path(__file__).with_name(name) for name in
                         ('embedding_service.py', 'embedding_recovery.py', 'embedding_counter.py',
                          'embedding_counter_worker.py', 'private_files.py'))
        return tuple((str(path.resolve()), path.stat().st_dev, path.stat().st_ino,
                      path.stat().st_size, path.stat().st_mtime_ns, path.stat().st_ctime_ns) for path in paths)

    def _epoch(self):
        return self.service_id + ':' + str(self.lifecycle.generation)

    def _invalidate(self, category, *, terminal=False, error_type=None, epoch=None):
        with self.lock:
            if epoch is not None and epoch != self._epoch():
                return
            if self.lifecycle.invalidate(category, time.monotonic(), terminal=terminal):
                self.cache.clear()
            if error_type is not None:
                self.failure = error_type  # Only a type, never exception/manuscript text.

    def poll_health(self):
        """Observe idle owner death without counting, waiting, restarting or SQL."""
        if self.closed.is_set():
            return
        with self.lock:
            counter, epoch = self.counter, self._epoch()
            state = self.lifecycle.state
        if state == 'failed':
            return
        try:
            if self._artifact_state() != self.artifacts:
                self._invalidate('artifact_drift', terminal=True, epoch=epoch)
                return
        except Exception:
            self._invalidate('artifact_drift', terminal=True, epoch=epoch)
            return
        try:
            process = getattr(counter, 'process', None)
            if state in ('ready', 'starting') and counter is not None and hasattr(counter, 'process') and (
                    process is None or process.poll() is not None):
                self._invalidate('process_exited', epoch=epoch)
        except Exception:
            self._invalidate('health_unconfirmed', terminal=True, epoch=epoch)

    def health_snapshot(self):
        allowed = self.allowed() and not self.closed.is_set()
        with self.lock:
            life = self.lifecycle
            state = 'closed' if self.closed.is_set() else life.state
            if not allowed and state not in ('closed', 'failed'):
                state = 'paused'
            return dict(state=state, available=state == 'ready', generation=life.generation,
                        restarts=life.restarts, max_restarts=len(life.delays) if life.enabled else 0,
                        failure_category=life.failure_category,
                        retry_in_seconds=max(0, life.retry_at-time.monotonic()) if life.retry_at else None)

    def _unavailable(self):
        if self.lifecycle.enabled:
            return ActionDeferred('embedding_counter_' + self.lifecycle.state +
                                  (':' + self.lifecycle.failure_category if self.lifecycle.failure_category else ''))
        category = self.lifecycle.failure_category
        detail = {'artifact_drift': 'artifacts changed', 'process_exited': 'process exited'}.get(category, 'unavailable')
        return ValueError('exact counter ' + detail)

    def _count_allowed(self, maintenance=None):
        if maintenance is None:
            return self.allowed()
        with self.lock:
            current = self.maintenance_scope
        return maintenance is current and maintenance.valid_memory()

    def _healthy(self, maintenance=None, *, queueing=False):
        self.poll_health()
        if self.closed.is_set() or not self._count_allowed(maintenance):
            if self.lifecycle.enabled:
                raise ActionDeferred('embedding_counter_closed' if self.closed.is_set() else 'embedding_counter_paused')
            raise ValueError('exact counter unavailable')
        with self.lock:
            life = self.lifecycle
            explicit_retry = maintenance is not None and queueing and (
                (life.state == 'backoff' and time.monotonic() >= life.retry_at) or
                (life.state == 'starting' and life.restarts and self.counter is None))
            if not explicit_retry and (life.state not in ('ready', 'starting') or (
                    life.state == 'starting' and life.restarts)):
                raise self._unavailable()
            return self._epoch()

    def validate_epoch(self, counter_epoch, *, _maintenance=None):
        current = self._healthy(_maintenance)
        with self.lock:
            if (not isinstance(counter_epoch, str) or counter_epoch != current or current != self._epoch()
                    or self.lifecycle.state != 'ready'):
                raise CountsPending()
        return counter_epoch

    def _verify_artifacts(self):
        """A new generation verifies content SHA again, even under the same policy."""
        if not self.settings:
            return
        from .embedding_counter import CL100K_CACHE, file_sha
        paths = dict(model_sha256=self.settings['model_path'], libllama_sha256=self.settings['library_path'],
                     cl100k_sha256=Path(self.settings['cache_dir']) / CL100K_CACHE)
        if any(file_sha(path) != self.identity[key] for key, path in paths.items()):
            raise ValueError('counter artifact fingerprint mismatch')
        manifest_path = self.settings.get('runtime_manifest')
        if not manifest_path or file_sha(manifest_path) != self.policy.counter_runtime_sha256:
            raise ValueError('counter runtime fingerprint mismatch')
        if self.settings.get('python_path'):
            root = Path(self.settings['python_path']).resolve()
            manifest = json.loads(Path(manifest_path).read_text(encoding='utf8'))
            if manifest.get('tiktoken') != '0.12.0' or not manifest.get('files'):
                raise ValueError('counter runtime manifest invalid')
            for row in manifest['files']:
                path = (root / row['path']).resolve()
                if not path.is_relative_to(root) or file_sha(path) != row['sha256']:
                    raise ValueError('counter runtime dependency drift')
        if not self.injected:
            from .embedding_admission import renderer_fingerprint
            if renderer_fingerprint() != self.policy.renderer_fingerprint:
                raise ValueError('counter renderer fingerprint mismatch')

    def _start(self, maintenance=None):
        if not self._count_allowed(maintenance) or self.closed.is_set():
            raise ActionDeferred('embedding_counter_paused')
        if self.lifecycle.enabled:
            try:
                self._verify_artifacts()
            except (OSError, ValueError, KeyError, TypeError):
                self._invalidate('artifact_drift', terminal=True)
                raise self._unavailable() from None
        if not self._count_allowed(maintenance) or self.closed.is_set():
            raise ActionDeferred('embedding_counter_paused')
        try:
            counter = self.factory()
        except Exception as exc:
            # A constructor that never delivered the helper identity cannot prove
            # cleanup of a partially started WSL process. Do not guess/restart it.
            self._invalidate('startup_unconfirmed', terminal=True, error_type=type(exc).__name__)
            raise self._unavailable() from None
        with self.lock:
            self.counter = counter
            self.owner_process = getattr(counter, 'process', None)
        if getattr(counter, 'identity', None) != self.identity:
            self._invalidate('identity_mismatch', terminal=True)
            raise self._unavailable()
        runtime = dict(counter.runtime)
        if self.lifecycle.enabled and not self.injected:
            from .embedding_counter import file_sha
            if (not helper_identity_valid(runtime) or
                    runtime['worker_sha256'] != file_sha(Path(__file__).with_name('embedding_counter_worker.py'))):
                self._invalidate('identity_mismatch', terminal=True)
                raise self._unavailable()
        with self.lock:
            self.loads += 1
            self.runtime = runtime
            if self.lifecycle.state != 'starting':
                raise self._unavailable()
            self.lifecycle.state = 'ready'
            self.failure = None

    def _retire(self):
        counter, process = self.counter, self.owner_process
        if counter is None:
            return True
        try:
            counter.close()
            confirmed = (not hasattr(counter, 'process') or
                         (process is not None and process.poll() is not None))
            if confirmed and self.lifecycle.enabled and not self.injected:
                confirmed = helper_exited(self.settings, dict(getattr(counter, 'runtime', {})))
        except Exception:
            confirmed = False
        if not confirmed:
            self._invalidate('exit_unconfirmed', terminal=True)
            return False
        with self.lock:
            self.counter, self.owner_process = None, None
        return True

    def _maintenance(self):
        self.poll_health()
        if not self.lifecycle.enabled:
            return
        with self.lock:
            failed = self.lifecycle.state in ('backoff', 'failed')
            unconfirmed = self.lifecycle.failure_category == 'exit_unconfirmed'
        if unconfirmed:
            return  # A missing exit proof needs intervention, not repeated probes.
        if failed and not self._retire():
            return
        if not self.allowed() or self.closed.is_set():
            return
        with self.lock:
            restart = (self.lifecycle.state == 'starting' and self.lifecycle.restarts > 0 and self.counter is None)
            restart = restart or self.lifecycle.start_retry(time.monotonic())
        if restart:
            try:
                self._start()
            except ActionDeferred:
                pass
            except Exception as exc:
                self._invalidate('startup_unconfirmed', terminal=True, error_type=type(exc).__name__)

    def cached_many(self, texts, *, _maintenance=None):
        texts = list(texts); validate_texts(texts)
        epoch = self._healthy(_maintenance)
        keys = [(self.policy.fingerprint, input_hash(text)) for text in texts]
        with self.lock:
            if epoch != self._epoch() or self.lifecycle.state not in ('ready', 'starting'):
                raise self._unavailable()
            if any(key not in self.cache for key in keys):
                raise CountsPending()
            values = [dict(self.cache[key]) for key in keys]
            for key in keys: self.cache.move_to_end(key)
            self.cache_hits += len(keys)
        return CountBatch(values, epoch)

    def count_many(self, texts, *, policy_id=None, cancelled=None, _maintenance=None):
        if policy_id is not None and policy_id != self.policy.fingerprint:
            raise ValueError('counter policy mismatch')
        texts = list(texts); validate_texts(texts)
        epoch = self._healthy(_maintenance, queueing=_maintenance is not None)
        with self.lock:
            explicit_retry = _maintenance is not None and self.lifecycle.state != 'ready' and self.lifecycle.restarts > 0
            explicit_retry = explicit_retry or (_maintenance is not None and self.lifecycle.state == 'backoff')
        if not explicit_retry:
            try: return self.cached_many(texts, _maintenance=_maintenance)
            except CountsPending: pass
        request = _Request(texts, epoch, maintenance=_maintenance)
        try: self.queue.put_nowait(request)
        except queue.Full: raise ActionDeferred('embedding_counter_busy') from None
        deadline = time.monotonic() + self.timeout
        while not request.done.wait(.05):
            if (self.closed.is_set() or (cancelled is not None and cancelled()) or
                    (_maintenance is not None and not self._count_allowed(_maintenance))):
                request.abandoned = True
                raise ActionDeferred('embedding_counter_cancelled')
            if time.monotonic() >= deadline:
                request.abandoned = True
                if self.lifecycle.enabled:
                    self._invalidate('timeout', epoch=epoch)
                    raise ActionDeferred('embedding_counter_timeout')
                raise TimeoutError('exact counter request deadline')
        if request.error is not None: raise request.error
        self.validate_epoch(request.counter_epoch, _maintenance=_maintenance)
        return request.result

    def _run(self):
        try:
            while not self.closed.is_set():
                self._maintenance()
                try: request = self.queue.get(timeout=.1)
                except queue.Empty: continue
                if request.abandoned:
                    request.error = ValueError('abandoned counter request'); request.done.set(); continue
                try:
                    epoch = self._healthy(request.maintenance, queueing=request.maintenance is not None)
                    if request.counter_epoch != epoch:
                        raise CountsPending()
                    if request.maintenance is not None and self.lifecycle.state == 'backoff':
                        # Paused maintenance cannot wake background recovery. A new
                        # explicit session may use the same bounded retry after exit
                        # proof and backoff, on this original owner thread only.
                        if not self._retire() or not self._count_allowed(request.maintenance):
                            raise self._unavailable()
                        with self.lock:
                            if not self.lifecycle.start_retry(time.monotonic()):
                                raise self._unavailable()
                    if self.counter is None:
                        self._start(request.maintenance)
                    keys = [(self.policy.fingerprint, input_hash(text)) for text in request.texts]
                    with self.lock:
                        if epoch != self._epoch() or self.lifecycle.state != 'ready':
                            raise self._unavailable()
                        found = {key: dict(self.cache[key]) for key in keys if key in self.cache}
                    missing = list(dict.fromkeys(key for key in keys if key not in found))
                    texts_by_key = dict(zip(keys, request.texts))
                    rows = self.counter.count_many([texts_by_key[key] for key in missing]) if missing else []
                    validate_counts(rows, len(missing))
                    self._healthy(request.maintenance)
                    found.update(zip(missing, rows))
                    with self.lock:
                        if epoch != self._epoch() or self.lifecycle.state != 'ready':
                            raise self._unavailable()
                        for key in keys:
                            self.cache[key] = dict(found[key]); self.cache.move_to_end(key)
                        while len(self.cache) > self.max_entries: self.cache.popitem(last=False)
                        self.counted_inputs += len(missing)
                    request.result = CountBatch([dict(found[key]) for key in keys], epoch)
                except Exception as exc:
                    if isinstance(exc, ActionDeferred):
                        request.error = exc
                    else:
                        process = getattr(self.counter, 'process', None)
                        exited = self.counter is not None and hasattr(self.counter, 'process') and (
                            process is None or process.poll() is not None)
                        category = ('invalid_reply' if isinstance(exc, CounterProtocolError) else
                                    'timeout' if isinstance(exc, TimeoutError) else
                                    'process_exited' if isinstance(exc, CounterProcessExited) or exited else 'invalid_reply')
                        self._invalidate(category, terminal=category == 'invalid_reply', error_type=type(exc).__name__)
                        request.error = self._unavailable() if self.lifecycle.enabled else ValueError('exact counter failed: ' + type(exc).__name__)
                finally: request.done.set()
        finally:
            self._retire()
            while True:
                try: request = self.queue.get_nowait()
                except queue.Empty: break
                request.error = ValueError('exact counter closed'); request.done.set()

    def close(self):
        self.closed.set()
        self.thread.join(timeout=40)
        if self.thread.is_alive(): raise TimeoutError('exact counter shutdown incomplete')
        if self.counter is not None: raise TimeoutError('exact counter helper exit unconfirmed')

    def __enter__(self): return self
    def __exit__(self, *args): self.close()


class CachedCounter:
    """Manager publication adapter: never queues, starts or waits for a count."""
    def __init__(self, broker):
        self.broker, self.identity = broker, broker.identity
    def count_many(self, texts): return self.broker.cached_many(texts)
    def validate_epoch(self, counter_epoch): return self.broker.validate_epoch(counter_epoch)


class PipeCounter:
    def __init__(self, client, root, policy):
        self.client, self.root, self.policy = client, Path(root), policy
        self.identity = {k: getattr(policy, k) for k in
                         ('model_sha256', 'libllama_sha256', 'cl100k_sha256')}

    def count_many(self, texts):
        client = self.client
        if client.active_kind is not None:
            raise ValueError('exact counting must occur outside resource actions')
        texts = list(texts); validate_texts(texts)
        identity = dict(epoch=client.epoch, execution_id=client.execution_id, policy_id=self.policy.fingerprint)
        root = self.root / 'private' / 'embedding' / client.epoch / client.execution_id
        request = write_descriptor(root, dict(**identity, texts=texts))
        reply = None
        try:
            reply = client.rpc('embedding_count', dict(descriptor=request))
            value = read_descriptor(root, reply, identity)
            if (value.get('hashes') != [input_hash(text) for text in texts] or
                    value.get('counter_identity') != self.identity):
                raise ValueError('exact counter response identity mismatch')
            validate_counts(value.get('counts'), len(texts))
            epoch = value.get('counter_epoch')
            if not isinstance(epoch, str) or not epoch:
                raise ActionDeferred('embedding_counter_epoch_missing')
            return CountBatch(value['counts'], epoch)
        except (TimeoutError, ConnectionError):
            raise ActionDeferred('embedding_counter_transport_unavailable') from None
        finally:
            # These are execution-private temporary transport files, not evidence/packages.
            for descriptor in (request, reply):
                if descriptor is not None:
                    try: owned_path(root, descriptor['path']).unlink(missing_ok=True)
                    except (OSError, KeyError, ValueError): pass

    def validate_epoch(self, counter_epoch):
        if not isinstance(counter_epoch, str) or not counter_epoch:
            raise CountsPending()
        try:
            value = self.client.rpc('embedding_epoch', dict(counter_epoch=counter_epoch))
        except (TimeoutError, ConnectionError):
            raise ActionDeferred('embedding_counter_transport_unavailable') from None
        if value != dict(counter_epoch=counter_epoch):
            raise ActionDeferred('embedding_counter_epoch_mismatch')
        return counter_epoch


class _MaintenanceCounter:
    """One non-renewable, thread-owned migration capability; never a worker permit."""
    def __init__(self, endpoint, run, validate_scope, timeout):
        if (type(timeout) not in (int, float) or not 0 < timeout <= 300 or
                not callable(validate_scope) or not isinstance(run, dict)):
            raise ValueError('bounded explicit maintenance session required')
        control = run.get('control')
        if (not isinstance(control, dict) or set(control) != {'epoch', 'generation'} or
                not isinstance(control['epoch'], str) or not control['epoch'] or
                type(control['generation']) is not int or
                any(not isinstance(run.get(key), str) or not 0 < len(run[key]) <= 512
                    for key in ('migration_id', 'task_id', 'policy_id'))):
            raise ValueError('bound migration identity required')
        self.endpoint, self.identity = endpoint, dict(endpoint.broker.identity)
        self.binding = {key: copy.deepcopy(run[key]) for key in ('migration_id', 'task_id', 'policy_id', 'control')}
        self.validate_scope = validate_scope
        self.deadline, self.owner = time.monotonic()+timeout, threading.get_ident()
        self.closed, self.nonce = False, uuid.uuid4().hex
        self.service_epoch = endpoint.broker._epoch()

    def valid_memory(self):
        endpoint, m = self.endpoint, self.endpoint.manager
        with m.gate_lock:
            return bool(not self.closed and endpoint._maintenance_session is self and
                time.monotonic() < self.deadline and not endpoint.broker.closed.is_set() and m.stopped.is_set() and
                not m.closing and not m.pause_pending and
                self.binding['control'] == dict(epoch=m.epoch, generation=m.generation) and
                m.saved_generation == m.generation and not m.starting and
                not m.pending_control_changes and not m.publications and
                all(ex.exit_saved for ex in m.executions.values()) and
                self.binding['policy_id'] == endpoint.policy.fingerprint == endpoint.broker.policy.fingerprint and
                self.service_epoch == endpoint.broker._epoch())

    def _check(self):
        if threading.get_ident() != self.owner or not self.valid_memory():
            raise ActionDeferred('embedding_maintenance_scope_invalid')
        # The migration's ledger/target guard runs in the calling thread, outside
        # broker and Manager locks. It is never a callback on the owner thread.
        try:
            if self.validate_scope() is False:
                raise ValueError('maintenance scope denied')
        except Exception:
            self.closed = True
            raise ActionDeferred('embedding_maintenance_scope_invalid') from None
        if not self.valid_memory():
            raise ActionDeferred('embedding_maintenance_scope_invalid')

    def count_many(self, texts):
        self._check()
        rows = self.endpoint.broker.count_many(texts, policy_id=self.binding['policy_id'], _maintenance=self)
        self._check()
        return CountBatch(rows, rows.counter_epoch + ':maintenance:' + self.nonce)

    def validate_epoch(self, counter_epoch):
        self._check()
        suffix = ':maintenance:' + self.nonce
        if not isinstance(counter_epoch, str) or not counter_epoch.endswith(suffix):
            raise CountsPending()
        self.endpoint.broker.validate_epoch(counter_epoch[:-len(suffix)], _maintenance=self)
        self._check()
        return counter_epoch


class CounterEndpoint:
    def __init__(self, manager, config, policy, settings, *, factory=None):
        self.manager, self.config, self.policy = manager, config, policy
        self._maintenance_session = None
        self.broker = CountBroker(policy, settings, factory=factory, recovery=True, allowed=self.recovery_allowed)

    def maintenance_active(self):
        """Pure in-memory resume barrier. Expired sessions never keep access alive."""
        with self.manager.gate_lock:
            scope = getattr(self, '_maintenance_session', None)
            return bool(scope is not None and not scope.closed and time.monotonic() < scope.deadline)

    @contextmanager
    def maintenance_counter(self, run, validate_scope, *, timeout=300):
        """Explicit paused migration only; context exit revokes without rechecking SQL."""
        self.broker.poll_health()
        scope = _MaintenanceCounter(self, run, validate_scope, timeout)
        with self.manager.gate_lock:
            if self._maintenance_session is not None:
                raise ActionDeferred('embedding_maintenance_busy')
            self._maintenance_session = scope
            with self.broker.lock:
                self.broker.maintenance_scope = scope
        try:
            scope._check()
            yield scope
        finally:
            # register() has already changed reserved -> complete on success.
            # Revocation must not invoke a reserved-state guard after commit.
            with self.manager.gate_lock:
                scope.closed = True
                if self._maintenance_session is scope:
                    self._maintenance_session = None
                with self.broker.lock:
                    if self.broker.maintenance_scope is scope:
                        self.broker.maintenance_scope = None

    def recovery_allowed(self):
        m = self.manager
        with m.gate_lock:
            return not m.stopped.is_set() and not m.closing and not m.pause_pending

    def poll_health(self): self.broker.poll_health()
    def health_snapshot(self): return self.broker.health_snapshot()

    def epoch_eligible(self, execution_id):
        m = self.manager
        now = time.monotonic()
        with m.gate_lock:
            execution = m.executions.get(execution_id)
            return bool(not m.stopped.is_set() and not m.closing and not m.pause_pending and
                execution is not None and execution.eligible and not execution.exit_saved and
                execution.stop_at is None and execution.kind in ('graph', 'delivery') and
                now < execution.deadline and now-execution.last_heartbeat < m.lease_seconds and
                (execution.active_action is None or now < execution.active_action['deadline']))

    def validate_epoch(self, execution_id, counter_epoch):
        if not self.epoch_eligible(execution_id):
            raise ActionDeferred('embedding_counter_not_admitted')
        self.broker.validate_epoch(counter_epoch)
        if not self.epoch_eligible(execution_id):
            raise ActionDeferred('embedding_counter_not_admitted')
        return dict(counter_epoch=counter_epoch)

    def eligible(self, execution_id):
        m = self.manager
        with m.gate_lock:
            execution = m.executions.get(execution_id)
            return (not m.stopped.is_set() and not m.closing and execution is not None and
                    execution.eligible and not execution.exit_saved and execution.stop_at is None and
                    execution.kind in ('graph', 'delivery') and execution.active_action is None)

    def handle(self, execution_id, descriptor):
        if not self.eligible(execution_id): raise ActionDeferred('embedding_counter_not_admitted')
        identity = dict(epoch=self.manager.epoch, execution_id=execution_id, policy_id=self.policy.fingerprint)
        root = self.config.root / 'private' / 'embedding' / self.manager.epoch / execution_id
        value = read_descriptor(root, descriptor, identity)
        if set(value) != {*identity, 'texts'}: raise ValueError('unknown counter request fields')
        validate_texts(value['texts'])
        counts = self.broker.count_many(value['texts'], policy_id=value['policy_id'],
                                       cancelled=lambda: not self.eligible(execution_id))
        if not self.eligible(execution_id): raise ActionDeferred('embedding_counter_not_admitted')
        return write_descriptor(root, dict(**identity, hashes=[input_hash(t) for t in value['texts']],
                                counter_identity=self.broker.identity, counts=counts, counter_epoch=counts.counter_epoch))

    def close(self):
        with self.manager.gate_lock:
            scope = getattr(self, '_maintenance_session', None)
            if scope is not None:
                scope.closed = True
        self.broker.close()


def install(manager, config):
    """Explicit manager attachment; never discovers or changes production config."""
    from .embedding_admission import ConfiguredAdmission, admission_from_config
    admission = admission_from_config(config)
    if not isinstance(admission, ConfiguredAdmission): return None
    existing = getattr(manager, 'embedding', None)
    if existing is not None:
        if existing.policy != admission.policy or existing.config.root != config.root:
            raise ValueError('one manager requires one accepted counter policy')
        return existing
    manager.embedding = CounterEndpoint(manager, config, admission.policy, admission.settings)
    return manager.embedding
