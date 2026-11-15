"""Small durable stage receipts; no scheduler, leases or background workers."""
import hashlib
import json
import os
from pathlib import Path
import time
import uuid


def sha_file(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def identity(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(',', ':')).encode()).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def write_json(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    data = json.dumps(value, ensure_ascii=False, indent=2).encode('utf8')
    try:
        with temporary.open('xb') as stream:
            stream.write(data); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        # On failure preserve the old durable record and remove only our temp.
        temporary.unlink(missing_ok=True)


class Records:
    def __init__(self, root):
        self.root = Path(root)

    def path(self, stage):
        if not stage or any(c not in 'abcdefghijklmnopqrstuvwxyz0123456789-_' for c in stage):
            raise ValueError('Invalid stage name')
        return self.root / 'stages' / (stage + '.json')

    def status(self, stage):
        path = self.path(stage)
        return read_json(path) if path.is_file() else {'state': 'pending'}

    def cached(self, stage):
        record = self.status(stage)
        if record['state'] != 'complete': return None
        if identity(record['result']) != record['result_sha256']:
            raise ValueError('Stage result changed: ' + stage)
        for name, digest in record['artifacts'].items():
            if not Path(name).is_file() or sha_file(name) != digest:
                raise ValueError('Saved stage artifact changed: ' + stage)
        return record['result']

    def started(self, stage):
        self._save(stage, {'state': 'running', 'started_at': time.time()})

    def complete(self, stage, result, artifacts=()):
        self._save(stage, {'state': 'complete', 'completed_at': time.time(),
            'result': result, 'result_sha256': identity(result),
            'artifacts': {str(Path(p).resolve()): sha_file(p) for p in artifacts}})

    def failed(self, stage, error):
        # Exception text can contain provider payloads or private material.
        value = {'state': 'failed', 'failed_at': time.time(), 'error_type': type(error).__name__}
        if stage == 'profile':
            from .profile_recovery import safe_error
            value['error'] = safe_error(error)
        elif stage in {'identify', 'attachment-card'}:
            from .attachments import safe_error
            value['error'] = safe_error(error)
        self._save(stage, value)

    def _save(self, stage, value):
        path = self.path(stage)
        if stage in {'profile', 'identify', 'attachment-card'}:
            history = path.parent / 'history' / stage
            history.mkdir(parents=True, exist_ok=True)
            if path.exists():
                original = path.read_bytes()
                old = history / ('prior-' + hashlib.sha256(original).hexdigest() + '.json')
                if not old.exists():
                    with old.open('xb') as stream:
                        stream.write(original); stream.flush(); os.fsync(stream.fileno())
            event = history / (str(time.time_ns()) + '-' + uuid.uuid4().hex + '.json')
            with event.open('x', encoding='utf8') as stream:
                json.dump(value, stream, ensure_ascii=False, indent=2)
                stream.flush(); os.fsync(stream.fileno())
        write_json(path, value)
