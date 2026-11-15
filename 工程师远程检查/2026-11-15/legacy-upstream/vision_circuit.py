"""Crash-safe primary vision-channel circuit breaker."""
import json
import os
import tempfile
import threading
import uuid
from datetime import datetime, timedelta
from pathlib import Path


_LOCKS = {}
_LOCKS_GUARD = threading.Lock()


def _lock_for(path):
    key = os.path.normcase(str(path.resolve()))
    with _LOCKS_GUARD:
        return _LOCKS.setdefault(key, threading.RLock())


class CircuitOpen(RuntimeError):
    pass


class VisionCircuit:
    def __init__(self, root, config=None, filename='vision_circuit.json'):
        self.path = Path(root or '.').resolve() / filename
        self.config = config if isinstance(config, dict) else {}
        self.lock = _lock_for(self.path)
        self._probe = threading.local()
        with self.lock:
            self.data = self._load()

    def _load(self):
        try:
            value = json.loads(self.path.read_text(encoding='utf-8'))
            if isinstance(value, dict) and isinstance(value.get('circuit'), dict):
                return value
        except (OSError, ValueError, UnicodeError):
            pass
        return {'circuit': {'primary': self._closed()}}

    @staticmethod
    def _closed():
        return {'state': 'closed', 'consecutive_failures': 0,
                'opened_at': None, 'cooldown_seconds': 0, 'next_probe_at': None,
                'probe_in_flight': False, 'probe_expires_at': None, 'probe_id': None}

    def _write(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=self.path.parent,
                                             prefix='.vision-circuit-', suffix='.tmp', delete=False)
        temp = Path(handle.name)
        try:
            with handle:
                json.dump(self.data, handle, ensure_ascii=False, indent=2)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, self.path)
        finally:
            if temp.exists():
                try: temp.unlink()
                except OSError: pass

    def _settings(self):
        defaults = {'failure_threshold': 3, 'cooldown_seconds': 300,
                    'cooldown_multiplier': 2.0, 'max_cooldown_seconds': 3600,
                    'probe_lease_seconds': 3600}
        result = {**defaults, **(self.config if isinstance(self.config, dict) else {})}
        try: result['failure_threshold'] = max(1, int(result['failure_threshold']))
        except (TypeError, ValueError): result['failure_threshold'] = 3
        try: result['cooldown_seconds'] = max(0, float(result['cooldown_seconds']))
        except (TypeError, ValueError): result['cooldown_seconds'] = 300
        try: result['cooldown_multiplier'] = max(1.0, float(result['cooldown_multiplier']))
        except (TypeError, ValueError): result['cooldown_multiplier'] = 2.0
        try: result['max_cooldown_seconds'] = max(result['cooldown_seconds'], float(result['max_cooldown_seconds']))
        except (TypeError, ValueError): result['max_cooldown_seconds'] = 3600
        try: result['probe_lease_seconds'] = max(1.0, float(result['probe_lease_seconds']))
        except (TypeError, ValueError): result['probe_lease_seconds'] = 3600
        return result

    def _start_probe(self, primary):
        self._probe.token = uuid.uuid4().hex
        primary.update(state='half_open', probe_in_flight=True,
                       probe_id=self._probe.token,
                       probe_expires_at=(datetime.now() + timedelta(
                           seconds=self._settings()['probe_lease_seconds'])).isoformat())
        self._write()
        return 'primary', True

    def release_probe(self):
        """Return an unfinished probe without claiming the endpoint recovered."""
        token = getattr(self._probe, 'token', None)
        if token is None:
            return
        try:
            with self.lock:
                self.data = self._load()
                primary = self.data.setdefault('circuit', {}).setdefault('primary', self._closed())
                if primary.get('state') == 'half_open' and primary.get('probe_id') == token:
                    primary.update(state='open', probe_in_flight=False,
                                   probe_expires_at=None, probe_id=None)
                    self._write()
        finally:
            self._probe.token = None

    def _stale_probe(self, primary):
        token = getattr(self._probe, 'token', None)
        return token is not None and primary.get('probe_id') != token

    def before_call(self, fallback_enabled):
        """Return (channel, is_probe). A half-open circuit admits one probe."""
        with self.lock:
            self.data = self._load()
            primary = self.data.setdefault('circuit', {}).setdefault('primary', self._closed())
            if primary.get('state') == 'half_open':
                try:
                    live = (primary.get('probe_in_flight') and
                            datetime.now() < datetime.fromisoformat(primary['probe_expires_at']))
                except (KeyError, TypeError, ValueError):
                    # Old half-open states without a lease cannot block a
                    # restarted workbench forever.
                    live = False
                if live:
                    if fallback_enabled:
                        return 'fallback', False
                    raise CircuitOpen('主通道探针正在进行，请稍后重试')
                return self._start_probe(primary)
            if primary.get('state') != 'open':
                return 'primary', False
            if not fallback_enabled:
                return 'primary', False
            next_probe = primary.get('next_probe_at')
            try: due = not next_probe or datetime.now() >= datetime.fromisoformat(next_probe)
            except (TypeError, ValueError): due = True
            if not due:
                return 'fallback', False
            # Persist half-open before releasing the lock so concurrent workers
            # cannot all turn the cooldown probe into a thundering herd.
            return self._start_probe(primary)

    def success(self, channel):
        if channel != 'primary': return
        with self.lock:
            self.data = self._load()
            if self._stale_probe(self.data.get('circuit', {}).get('primary', {})):
                self._probe.token = None
                return
            self.data.setdefault('circuit', {})['primary'] = self._closed()
            self._write()
            self._probe.token = None

    def failure(self, channel):
        if channel != 'primary': return False
        with self.lock:
            self.data = self._load()
            primary = self.data.setdefault('circuit', {}).setdefault('primary', self._closed())
            if self._stale_probe(primary):
                self._probe.token = None
                return False
            settings = self._settings()
            was_half_open = primary.get('state') == 'half_open'
            previous = float(primary.get('cooldown_seconds') or settings['cooldown_seconds'])
            count = int(primary.get('consecutive_failures') or 0) + 1
            primary['consecutive_failures'] = count
            opened = primary.get('state') == 'half_open' or count >= settings['failure_threshold']
            if opened:
                base = previous * settings['cooldown_multiplier'] if was_half_open else settings['cooldown_seconds']
                cooldown = min(settings['max_cooldown_seconds'], max(settings['cooldown_seconds'], base))
                now = datetime.now()
                primary.update(state='open', opened_at=now.isoformat(timespec='seconds'),
                               cooldown_seconds=cooldown,
                               next_probe_at=(now + timedelta(seconds=cooldown)).isoformat(timespec='seconds'),
                               probe_in_flight=False, probe_expires_at=None, probe_id=None)
                self._write()
                self._probe.token = None
                return True
            primary['state'] = 'closed';primary['probe_in_flight']=False;primary['probe_expires_at']=None;primary['probe_id']=None
            self._write()
            self._probe.token = None
            return False

    def snapshot(self):
        with self.lock:
            self.data = self._load()
            return json.loads(json.dumps(self.data))
