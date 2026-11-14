"""First-step Office conversion with shared channels and task-local results."""
from collections import deque
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from uuid import uuid4
import hashlib
import json
import math
import shutil
import tempfile
import threading
import time

from filelock import FileLock, Timeout

from .first_step_limits import (LIBREOFFICE_CHANNELS, LIBREOFFICE_TIMEOUT_SECONDS,
                                WPS_TIMEOUT_SECONDS)


class ConverterAttemptFailed(RuntimeError):
    """An engine completed or stopped and did not produce a valid document."""


class ConverterProcessUncertain(RuntimeError):
    """The converter's own processes could not be confirmed stopped."""
    def __init__(self, message, processes=None):
        super().__init__(message)
        self.processes = processes or []


class ConvertersExhausted(RuntimeError):
    """All appropriate engines failed for this file."""


class ConverterUnavailable(RuntimeError):
    """A required converter is unavailable, so results are not conclusive."""


def _check_stop(stop_requested):
    if stop_requested is not None and stop_requested():
        from .first_step_control import PauseRequested
        raise PauseRequested()


@contextmanager
def _wait_lock(path, stop_requested):
    lock = FileLock(str(path))
    while True:
        _check_stop(stop_requested)
        try:
            lock.acquire(timeout=.2)
            break
        except Timeout:
            pass
    try:
        _check_stop(stop_requested)
        yield
    finally:
        lock.release()


class _Channels:
    def __init__(self, root):
        self.root = Path(root)
        self.condition = threading.Condition()
        self.waiting = deque()
        self.busy = set()

    @contextmanager
    def claim(self, engine, *, task_id=None, offline=False, stop_requested=None):
        names = ([f'lo-{i + 1}' for i in range(LIBREOFFICE_CHANNELS)]
                 if engine == 'libreoffice' else ['wps'])
        self.root.mkdir(parents=True, exist_ok=True)
        ticket = (object(), frozenset(names))
        held = name = None
        with self.condition:
            self.waiting.append(ticket)
        try:
            while held is None:
                _check_stop(stop_requested)
                with self.condition:
                    earlier = list(self.waiting)[:self.waiting.index(ticket)]
                    can_wait = False
                    for candidate in names:
                        if candidate in self.busy or any(candidate in item[1] for item in earlier):
                            can_wait = True
                            continue
                        lock = FileLock(str(self.root / (candidate + '.lock')))
                        try:
                            lock.acquire(timeout=0)
                        except Timeout:
                            can_wait = True
                            continue
                        # Another process may have died after our initial scan.
                        if (self.root / (candidate + '.uncertain')).exists():
                            lock.release()
                            continue
                        held, name = lock, candidate
                        self.busy.add(name)
                        self.waiting.remove(ticket)
                        self.condition.notify_all()
                        break
                    if held is None:
                        if not can_wait:
                            raise ConverterProcessUncertain('转换通道的前一进程尚未确认退出')
                        self.condition.wait(timeout=.2)
            marker = self.root / (name + '.uncertain')
            # A crash must not make a still-owned converter appear idle.
            owner = dict(task_id=task_id, scope='offline' if offline else 'task', engine=engine,
                         state='active', started_at=time.time())
            temporary = self.root / (name + '-' + uuid4().hex + '.tmp')
            temporary.write_text(json.dumps(owner, ensure_ascii=False), encoding='utf8')
            temporary.replace(marker)
            uncertain = False
            try:
                _check_stop(stop_requested)
                yield name
            except ConverterProcessUncertain as exc:
                uncertain = True
                owner.update(state='uncertain', reason=str(exc))
                if exc.processes:
                    owner['processes'] = exc.processes
                marker.write_text(json.dumps(owner, ensure_ascii=False), encoding='utf8')
                raise
            finally:
                if not uncertain:
                    marker.unlink(missing_ok=True)
        finally:
            if held is not None:
                held.release()
            with self.condition:
                if name is not None:
                    self.busy.discard(name)
                if ticket in self.waiting:
                    self.waiting.remove(ticket)
                self.condition.notify_all()


_CHANNELS = {}
_CHANNELS_LOCK = threading.Lock()


def _channels(root):
    key = str(Path(root).resolve())
    with _CHANNELS_LOCK:
        if key not in _CHANNELS:
            _CHANNELS[key] = _Channels(key)
        return _CHANNELS[key]


class FirstStepConverter:
    """Reuse native content checks/cache with first-step-only scheduling."""
    def __init__(self, cache_root, channel_root, *, libre=None, wps=None,
                 task_id=None, offline=False, stop_requested=None):
        import material_conversion
        from .first_step_office import convert_libre, convert_wps
        self.native = material_conversion
        self.cache = material_conversion.ConversionPool(
            Path(cache_root).resolve() / 'private' / 'material-conversion')
        self.channels = _channels(channel_root)
        self.libre = libre or convert_libre
        self.wps = wps or partial(convert_wps, stop_requested=stop_requested)
        self.task_id = str(task_id) if task_id is not None else None
        self.offline = offline
        self.stop_requested = stop_requested

    def _attempt(self, engine, source, output, target_format):
        import office_libreoffice
        import office_wps
        label = 'LibreOffice' if engine == 'libreoffice' else 'WPS'
        available = office_libreoffice.executable() if engine == 'libreoffice' else office_wps.available()
        if not available:
            raise ConverterUnavailable(label + ' 不可用')
        timeout = LIBREOFFICE_TIMEOUT_SECONDS if engine == 'libreoffice' else WPS_TIMEOUT_SECONDS
        digest = self.native._sha(source)
        key = hashlib.sha256((self.cache._identity(engine) + source.suffix.lower() + ':' +
                              target_format + ':' + digest).encode()).hexdigest()
        cache = self.cache.root / 'cache' / engine / key
        cache.parent.mkdir(parents=True, exist_ok=True)
        # Wait for an identical conversion before occupying a channel.
        with _wait_lock(str(cache) + '.lock', self.stop_requested):
            cached = self.cache._cached(engine, key, target_format)
            if cached is None:
                cache.mkdir(parents=True, exist_ok=True)
                with self.channels.claim(engine, task_id=self.task_id, offline=self.offline,
                                         stop_requested=self.stop_requested):
                    with tempfile.TemporaryDirectory(prefix='attempt-', dir=cache) as name:
                        attempt = Path(name)
                        try:
                            runner = self.libre if engine == 'libreoffice' else self.wps
                            converted = runner(source, attempt / 'out', target_format=target_format,
                                               timeout=timeout)
                            valid = office_wps._valid_pdf if target_format == 'pdf' else office_wps._valid_docx
                            if not valid(converted):
                                raise ConverterAttemptFailed(label + ' 未产生有效文件')
                            if self.native._sha(source) != digest:
                                raise RuntimeError('转换期间原材料发生变化')
                            cached = cache / ('content.' + target_format)
                            self.native._publish(converted, cached)
                            log = attempt / 'out_conversion.log'
                            if log.exists():
                                self.native._publish(log, cache / 'conversion.log')
                            report = dict(version=self.native.VERSION, engine=engine,
                                          source_sha256=digest, sha256=self.native._sha(cached),
                                          completed_at=time.time())
                            temporary = cache / ('identity-' + uuid4().hex + '.tmp')
                            temporary.write_text(json.dumps(report, ensure_ascii=False), encoding='utf8')
                            temporary.replace(cache / 'identity.json')
                        except BaseException as exc:
                            failed = cache / 'failed' / uuid4().hex
                            try:
                                shutil.copytree(attempt, failed)
                                (failed / 'failure.json').write_text(json.dumps(dict(
                                    error_type=type(exc).__name__, source_sha256=digest,
                                    engine=engine, failed_at=time.time())), encoding='utf8')
                            except OSError as archive_error:
                                if isinstance(exc, ConverterProcessUncertain):
                                    raise exc from archive_error
                                raise
                            raise
            target = output / (source.stem + '.' + target_format)
            self.native._publish(cached, target)
            return target

    def convert(self, source, output, *, target_format='docx'):
        source, output = Path(source).resolve(), Path(output).resolve()
        if target_format not in {'docx', 'pdf'}:
            raise ValueError('格式转换目标无效')
        if source == output / (source.stem + '.' + target_format):
            raise ValueError('转换输出不能覆盖原稿')
        failures = []
        unavailable = False
        engines = ['wps'] if source.suffix.lower() == '.wps' else ['libreoffice', 'wps']
        # A later successful engine must not replace the material used by the
        # original classification/title proof. Reuse the first valid successful
        # cache for this exact source, using the native cache hashes/validator.
        digest = self.native._sha(source)
        cached_results = []
        for engine in engines:
            _check_stop(self.stop_requested)
            try:
                key = hashlib.sha256((self.cache._identity(engine) + source.suffix.lower() + ':' +
                                      target_format + ':' + digest).encode()).hexdigest()
            except (OSError, AttributeError):
                continue
            with _wait_lock(str(self.cache.root / 'cache' / engine / key) + '.lock', self.stop_requested):
                cached = self.cache._cached(engine, key, target_format)
                if cached is None:
                    continue
                try:
                    report = json.loads((cached.parent / 'identity.json').read_text(encoding='utf8'))
                    completed = float(report['completed_at'])
                    if (report.get('source_sha256') == digest and report.get('engine') == engine
                            and math.isfinite(completed)):
                        cached_results.append((completed, engine, key, cached, report['sha256']))
                except (OSError, ValueError, KeyError, TypeError):
                    continue
        if cached_results:
            completed, engine, key, cached, output_sha = min(cached_results)
            with _wait_lock(str(cached.parent) + '.lock', self.stop_requested):
                current = self.cache._cached(engine, key, target_format)
                try:
                    report = json.loads((cached.parent / 'identity.json').read_text(encoding='utf8'))
                    unchanged = (current == cached and report['sha256'] == output_sha
                                 and report['source_sha256'] == digest and report['engine'] == engine
                                 and float(report['completed_at']) == completed)
                except (OSError, ValueError, KeyError, TypeError):
                    unchanged = False
                if not unchanged:
                    raise RuntimeError('已选转换缓存发生变化，需重新核对')
                if self.native._sha(source) != digest:
                    raise RuntimeError('读取缓存期间原材料发生变化')
                _check_stop(self.stop_requested)
                target = output / (source.stem + '.' + target_format)
                self.native._publish(cached, target)
                return target
        for engine in engines:
            _check_stop(self.stop_requested)
            try:
                return self._attempt(engine, source, output, target_format)
            except ConverterAttemptFailed as exc:
                failures.append(str(exc))
            except ConverterUnavailable as exc:
                unavailable = True
                failures.append(str(exc))
        if unavailable:
            raise ConverterUnavailable('；'.join(failures))
        raise ConvertersExhausted('；'.join(failures))
