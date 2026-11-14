"""Crash-safe per-image progress for long-running preprocessing batches."""
import copy
import json
import os
import tempfile
import threading
from datetime import datetime
from pathlib import Path


_LOCKS = {}
_LOCKS_GUARD = threading.Lock()


def _lock_for(path):
    key = str(path.resolve())
    with _LOCKS_GUARD:
        return _LOCKS.setdefault(key, threading.RLock())


class ProcessingState:
    VERSION = 1

    def __init__(self, root, state_file='processing_state.json', processing_version=''):
        root = Path(root).resolve()
        path = Path(state_file)
        self.path = path if path.is_absolute() else root / path
        self.failures_path = self.path.with_name('failures.json')
        self.processing_version = str(processing_version or '')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = _lock_for(self.path)

    def _empty(self):
        return {'version': self.VERSION, 'processing_version': self.processing_version,
                'papers': {}, 'failures': []}

    def _read(self):
        try:
            value = json.loads(self.path.read_text(encoding='utf-8'))
        except (OSError, ValueError, UnicodeError):
            return self._empty()
        if not isinstance(value, dict) or value.get('version') != self.VERSION:
            return self._empty()
        if self.processing_version and value.get('processing_version') != self.processing_version:
            return self._empty()
        if not isinstance(value.get('papers'), dict):
            value['papers'] = {}
        if not isinstance(value.get('failures'), list):
            value['failures'] = []
        if self.processing_version:
            value['papers'] = {key: record for key, record in value['papers'].items()
                               if isinstance(record, dict) and record.get('processing_version') == self.processing_version}
        return value

    def _write(self, value):
        encoded = json.dumps(value, ensure_ascii=False, indent=2).encode('utf-8')
        temp = None
        try:
            with tempfile.NamedTemporaryFile(dir=self.path.parent, prefix='.processing-state-',
                                             suffix='.tmp', delete=False) as stream:
                temp = Path(stream.name)
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, self.path)
            failures_path = self.failures_path
            failure_temp = failures_path.with_name(failures_path.name + f'.{os.getpid()}.{threading.get_ident()}.tmp')
            try:
                failure_temp.write_text(json.dumps(value.get('failures', []), ensure_ascii=False, indent=2), encoding='utf-8')
                failure_temp.replace(failures_path)
            finally:
                failure_temp.unlink(missing_ok=True)
        finally:
            if temp is not None:
                try:
                    temp.unlink(missing_ok=True)
                except OSError:
                    pass

    def snapshot(self):
        with self._lock:
            return copy.deepcopy(self._read())

    def _paper(self, data, document_id, paper_id='', image_ids=()):
        now = datetime.now().isoformat(timespec='seconds')
        existing = data['papers'].get(document_id, {})
        done = set(existing.get('images_done', []))
        failed = set(existing.get('images_failed', []))
        total_ids = [str(value) for value in image_ids if value]
        done.intersection_update(total_ids or done)
        failed.intersection_update(total_ids or failed)
        record = {
            'paper_id': paper_id or existing.get('paper_id', ''),
            'stage': existing.get('stage', 'pending'),
            'images_total': len(total_ids) if total_ids else int(existing.get('images_total', 0) or 0),
            'images_done': sorted(done),
            'images_failed': sorted(failed),
            'completed_stages': list(existing.get('completed_stages', [])),
            'processing_version': self.processing_version or existing.get('processing_version', ''),
            'updated_at': now,
        }
        if record['images_total'] and len(record['images_done']) >= record['images_total']:
            record['stage'] = 'images_done'
        elif record['images_done']:
            record['stage'] = 'images_partial'
        data['papers'][document_id] = record
        return record

    def begin_paper(self, document_id, paper_id, image_ids, processing_version=None):
        with self._lock:
            data = self._read()
            if processing_version is not None:
                self.processing_version = str(processing_version or '')
                data['processing_version'] = self.processing_version
            self._paper(data, document_id, paper_id, image_ids)
            self._write(data)

    def image_done(self, document_id, image_id):
        with self._lock:
            data = self._read()
            record = data['papers'].setdefault(document_id, {
                'paper_id': '', 'stage': 'pending', 'images_total': 0,
                'images_done': [], 'images_failed': [], 'completed_stages': [],
                'processing_version': self.processing_version, 'updated_at': '',
            })
            if image_id not in record['images_done']:
                record['images_done'].append(image_id)
                record['images_done'].sort()
            record['images_failed'] = [x for x in record.get('images_failed', []) if x != image_id]
            record['stage'] = ('images_done' if record.get('images_total') and
                               len(record['images_done']) >= record['images_total']
                               else 'images_partial')
            record['updated_at'] = datetime.now().isoformat(timespec='seconds')
            self._write(data)

    def image_failed(self, document_id, image_id, reason):
        with self._lock:
            data = self._read()
            record = data['papers'].setdefault(document_id, {
                'paper_id': '', 'stage': 'pending', 'images_total': 0,
                'images_done': [], 'images_failed': [], 'completed_stages': [],
                'processing_version': self.processing_version, 'updated_at': '',
            })
            if image_id not in record.get('images_failed', []):
                record.setdefault('images_failed', []).append(image_id)
                record['images_failed'].sort()
            if image_id not in record.get('images_done', []):
                record['stage'] = 'images_partial'
            record['updated_at'] = datetime.now().isoformat(timespec='seconds')
            data.setdefault('failures', []).append({
                'paper': document_id, 'stage': 'image', 'image': image_id,
                'reason': str(reason), 'at': record['updated_at'],
            })
            self._write(data)

    def paper_stage(self, document_id, stage):
        with self._lock:
            data = self._read()
            record = data['papers'].setdefault(document_id, {
                'paper_id': '', 'stage': 'pending', 'images_total': 0,
                'images_done': [], 'images_failed': [], 'completed_stages': [],
                'processing_version': self.processing_version, 'updated_at': '',
            })
            record['stage'] = stage
            completed = record.setdefault('completed_stages', [])
            if stage not in completed: completed.append(stage)
            record['processing_version'] = self.processing_version or record.get('processing_version', '')
            record['updated_at'] = datetime.now().isoformat(timespec='seconds')
            self._write(data)

    def stage_done(self, document_id, stage):
        with self._lock:
            record = self._read().get('papers', {}).get(document_id, {})
            return stage in record.get('completed_stages', [])
