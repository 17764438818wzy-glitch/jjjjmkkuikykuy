"""One local recovery file; only whole-folder completion is sent to Prefect."""

import copy
from datetime import datetime, timezone
import os
import json
from pathlib import Path
import time
import uuid

from paperflow_prefect.records import identity as digest, read_json


PROGRESS_KINDS = {'rag-body', 'rag-attachments', 'rag-backend',
                  'rag-qdrant-body', 'rag-qdrant-attachments', 'rag-qdrant-backend',
                  'nas', 'cleanup', 'delivery_stage',
                  'rag_effect_intent', 'rag_effect_returned', 'rag_effect_not_sent', 'rag_effect_failed',
                  'folder_completion_pending', 'delivery_error'}


def write_json(path, value):
    """Keep one atomic file; wait briefly for transient Windows reader sharing."""
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    data=json.dumps(value,ensure_ascii=False,indent=2).encode('utf8')
    try:
        with temporary.open('xb') as stream:
            stream.write(data);stream.flush();os.fsync(stream.fileno())
        deadline=time.monotonic()+2
        while True:
            try:
                os.replace(temporary,path)
                break
            except PermissionError as exc:
                if os.name!='nt' or getattr(exc,'winerror',None) not in (5,32,33) or time.monotonic()>=deadline:
                    raise
                time.sleep(.05)
    finally:
        temporary.unlink(missing_ok=True)


def progress_path(data_root, run_id):
    # This directory is outside the per-paper work trees removed by cleanup.
    return Path(data_root).resolve().parent / 'folder-delivery-state' / (digest(str(run_id)) + '.json')


def record_key(row):
    kind = row['kind']
    if kind == 'folder_completion_pending':
        return 'completion:' + row['data']['stage']
    return ('effect:' + row['data']['effect_id']) if kind.startswith('rag_effect_') else kind


def local_records(run_id):
    config = os.environ.get('PAPERFLOW_V3_PROJECT_CONFIG')
    if not config:
        return []
    path = progress_path(read_json(config)['data_root'], run_id)
    if not path.is_file():
        return []
    saved = read_json(path)
    if saved['identity']['task_run_id'] != str(run_id):
        raise ValueError('local delivery state belongs to another paper')
    return sorted(({**row, 'local_only': True} for row in saved['records'].values()),
                  key=lambda row: row.get('recorded_at', ''), reverse=True)


def merge_records(local, remote, *, copy_rows=True):
    # Keep retry authorizations in their actual order relative to local failures.
    rows = sorted(remote + local, key=lambda row: row.get('recorded_at', ''), reverse=True)
    selected, seen = [], set()
    for row in rows:
        if row['kind'] in PROGRESS_KINDS:
            key = record_key(row)
            # Final remote receipts contain no recovery details; retain the local state too.
            if not row.get('folder_completion'):
                if key in seen:
                    continue
                seen.add(key)
        selected.append(copy.deepcopy(row) if copy_rows else row)
    return selected


class FolderProgress:
    def __init__(self, settings, identity, records, publish):
        self.identity = copy.deepcopy(identity)
        self.path = progress_path(settings.path('data_root'), identity['task_run_id'])
        self.publish = publish
        self.value = read_json(self.path) if self.path.is_file() else {
            'identity': self.identity, 'records': {}, 'published': {}}
        if self.value['identity'] != identity:
            raise ValueError('local delivery state belongs to another paper')
        self.records = merge_records(sorted(self.value['records'].values(),
            key=lambda row: row.get('recorded_at', ''), reverse=True), records)
        self.remote_complete = {row['kind']: row for row in records
            if row['kind'] in {'rag_complete', 'nas', 'cleanup'} and row.get('folder_completion')
            and row['data'].get('status') == 'complete' and row['data'].get('identity') == identity}
        # Runtime reads detailed local recovery state; server completions are tracked separately.
        self.records = [row for row in self.records if not row.get('folder_completion')]
        self.value['records'] = {record_key(row): copy.deepcopy(row) for row in reversed(self.records)
                                if row['kind'] in PROGRESS_KINDS}
        for kind in self.remote_complete:
            self.value['published'][kind] = True
            self.value['records'].pop('completion:' + ('rag' if kind == 'rag_complete' else kind), None)
        completed_stages = {'rag' if kind == 'rag_complete' else kind for kind in self.remote_complete}
        self.records = [row for row in self.records if row['kind'] != 'folder_completion_pending'
                        or row['data']['stage'] not in completed_stages]
        if self.value['records'] or self.value['published']:
            write_json(self.path, self.value)

    def save(self, kind, data, event_id=None):
        from paperflow_v3.control.receipts import _processing_attempt
        row = {'task_id': self.identity['task_id'], 'folder_id': self.identity['group_id'],
               'kind': kind, 'data': copy.deepcopy(data),
               'event_id': event_id or kind + '-' + digest(data),
               'recorded_at': datetime.now(timezone.utc).isoformat()}
        if _processing_attempt.get():
            row['processing_attempt_id'] = _processing_attempt.get()
        value = copy.deepcopy(self.value)
        value['records'][record_key(row)] = row
        write_json(self.path, value)
        self.value = value
        return row['event_id']

    def writer(self, kind):
        return lambda data: self.save(kind, data)

    def complete(self, kind, data):
        if data.get('status') != 'complete' or data.get('identity') != self.identity:
            raise ValueError('folder stage has not completed for this paper')
        if kind in self.value['published'] or kind in self.remote_complete:
            return
        try:
            if not self.publish(task_id=self.identity['task_id'], folder_id=self.identity['group_id'],
                    child_flow_run_id=self.identity['task_run_id'],
                    event_id='folder-' + kind + '-' + digest(self.identity), kind=kind,
                    data={'identity': self.identity, 'status': 'complete'}):
                raise ValueError('folder completion was not saved')
        except Exception:
            self.save('folder_completion_pending', {'identity': self.identity, 'status': 'pending',
                       'stage': 'rag' if kind == 'rag_complete' else kind})
            raise
        value = copy.deepcopy(self.value)
        value['published'][kind] = True
        value['records'].pop('completion:' + ('rag' if kind == 'rag_complete' else kind), None)
        write_json(self.path, value)
        self.value = value
