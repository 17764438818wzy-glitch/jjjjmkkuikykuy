"""Durable follow-up evidence for new native queue deliveries only.

No worker, network client, task grant, or independent upload path is created here.
The original queue, its controls, and the original receipt remain authoritative.
"""
from __future__ import annotations

import copy
import hashlib
import json
import time
from pathlib import Path

SCHEMA = 'native-queue-followup/1'
REQUEST_SCHEMA = 'native-ingest-intent/1'
MAX_TRACK_SECONDS = 7 * 24 * 60 * 60
MAX_DELAY_SECONDS = 300


def request_id(dataset_id, document_id, pipeline_id):
    return hashlib.sha256('\0'.join((dataset_id, document_id, pipeline_id)).encode('utf8')).hexdigest()


def _binding(job, marker):
    payload = job['payload']
    item = payload['item']
    return (isinstance(marker, dict) and marker.get('schema') == SCHEMA
            and marker.get('delivery_id') == job['id']
            and marker.get('service') == payload['service']
            and marker.get('task') == payload['task']
            and marker.get('manifest_sha256') == payload['manifest_sha256']
            and marker.get('source_sha256') == item.get('source_hash')
            and marker.get('source_sha256') == payload['task'].get('source_sha256')
            and isinstance(marker.get('requests'), dict))


def arm(queue, job):
    """Mark only a new, claimed job before any of this candidate's writes."""
    current = queue.get(job['id'])
    marker = (current.get('projection') or {}).get('preextract_native_followup')
    if marker is not None:
        if not _binding(current, marker):
            raise ValueError('native follow-up binding changed')
        return copy.deepcopy(marker)
    work = Path(job['payload']['item']['preextract_work_dir'])
    if any((work / name).exists() for name in ('delivery-binding.json', 'delivery-receipt.json')):
        return None  # Never adopt an existing intent or an old receipt.
    marker = dict(schema=SCHEMA, phase='armed', delivery_id=job['id'],
                  service=job['payload']['service'], task=copy.deepcopy(job['payload']['task']),
                  source_sha256=job['payload']['item'].get('source_hash'),
                  manifest_sha256=job['payload']['manifest_sha256'],
                  created_at=time.time(), observed_at=None, next_attempt_at=None,
                  initial_dispatch_scope=copy.deepcopy(job.get('dispatch_scope')),
                  initial_global_task_generation=(job.get('delivery_authority') or {}).get('global_task_generation'),
                  initial_upload_generations=copy.deepcopy(job.get('control_generations')),
                  attempts=0, requests={})
    if not _binding(job, marker):
        raise ValueError('native follow-up source identity missing')
    if not queue.update(job['id'], expected_status='uploading',
                        projection={'preextract_native_followup': marker}):
        raise InterruptedError('native follow-up queue ownership changed')
    return marker


class _FollowTransport:
    def __init__(self, opener, stop):
        self.opener, self.stop = opener, stop

    def __getattr__(self, name):
        value = getattr(self.opener, name)
        if name == 'open':
            def checked(*args, **kwargs):
                if self.stop.is_set():
                    raise InterruptedError('delivery scope paused; native follow-up retained')
                return value(*args, **kwargs)
            return checked
        return value


class FollowClient:
    """Per-job wrapper; its durable write-ahead hook runs before the ingest POST."""
    native_auto_follow = True
    native_unscoped_tail_blocked = True

    def __init__(self, client, queue, job, stop):
        self.client, self.queue, self.job, self.stop = client, queue, job, stop

    def __getattr__(self, name):
        value = getattr(self.client, name)
        if name == 'opener':
            return _FollowTransport(value, self.stop)
        if name in {'request', 'upload_bytes', 'upload_file', 'download_file', 'download_bytes', 'download_image'}:
            def checked(*args, **kwargs):
                if self.stop.is_set():
                    raise InterruptedError('delivery scope paused; native follow-up retained')
                return value(*args, **kwargs)
            return checked
        return value

    def native_before_ingest(self, identity):
        if self.stop.is_set():
            raise InterruptedError('delivery scope stopped before native intent')
        current = self.queue.get(self.job['id'])
        marker = copy.deepcopy((current.get('projection') or {}).get('preextract_native_followup'))
        if current['status'] != 'uploading' or not _binding(current, marker):
            raise ValueError('native follow-up authority or job identity changed')
        task = current['payload']['task']
        if (identity.get('paperflow_task') != task['task_id']
                or identity.get('paperflow_source_sha256') != task['source_sha256']
                or identity.get('paperflow_manifest_sha256') != current['payload']['manifest_sha256']
                or identity.get('paperflow_revision') != str(task['revision'])
                or identity.get('paperflow_role') != 'body'):
            raise ValueError('native request identity differs from queue')
        keys = ('dataset_id', 'document_id', 'pipeline_id')
        if any(not isinstance(identity.get(k), str) or not identity[k] for k in keys):
            raise ValueError('native request identity incomplete')
        rid = request_id(*(identity[k] for k in keys))
        if rid in marker['requests']:
            raise ValueError('native ingest already has an intent; never resend')
        marker['requests'][rid] = dict(schema=REQUEST_SCHEMA, request_id=rid,
                                       identity=copy.deepcopy(identity), intended_at=time.time(),
                                       dispatch_scope=copy.deepcopy(self.job.get('dispatch_scope')),
                                       global_task_generation=(self.job.get('delivery_authority') or {}).get('global_task_generation'),
                                       upload_generations=copy.deepcopy(self.job.get('control_generations')))
        marker.update(phase='submitted_or_unknown', attempts=0)
        if not self.queue.update(self.job['id'], expected_status='uploading',
                                 projection={'preextract_native_followup': marker}):
            raise InterruptedError('native follow-up queue ownership changed')
        if self.stop.is_set():
            raise InterruptedError('delivery scope stopped after native intent')
        return rid


def evidence(queue, job):
    """Prove that every follow-up request still matches its original receipt."""
    current = queue.get(job['id'])
    marker = copy.deepcopy((current.get('projection') or {}).get('preextract_native_followup'))
    if not _binding(current, marker) or not marker['requests']:
        raise ValueError('candidate native intent is missing')
    work = Path(current['payload']['item']['preextract_work_dir'])
    binding = json.loads((work / 'delivery-binding.json').read_text(encoding='utf8'))
    receipt = json.loads((work / 'delivery-receipt.json').read_text(encoding='utf8'))
    payload = current['payload']
    if (binding.get('service') != payload['service']
            or any(binding.get(k) != payload[k] or receipt.get(k) != payload[k]
                   for k in ('task', 'manifest_sha256'))
            or receipt.get('transport') != 'native_body_batch/1'
            or not isinstance(receipt.get('targets'), dict)):
        raise ValueError('original native receipt or binding changed')
    targets = receipt['targets']
    for rid, intent in marker['requests'].items():
        ident = intent.get('identity') or {}
        keys = ('dataset_id', 'document_id', 'pipeline_id')
        if (intent.get('schema') != REQUEST_SCHEMA or intent.get('request_id') != rid
                or any(not isinstance(ident.get(k), str) or not ident[k] for k in keys)
                or request_id(*(ident[k] for k in keys)) != rid):
            raise ValueError('native request identity changed')
        target = targets.get(ident.get('paperflow_specialty'), {})
        obj = target.get('body') or {}
        native = obj.get('native_batch') or {}
        if (obj.get('dataset_id') != ident['dataset_id'] or obj.get('document_id') != ident['document_id']
                or native.get('pipeline_id') != ident['pipeline_id']
                or native.get('follow_request_id') != rid
                or any((obj.get('identity') or {}).get(k) != ident.get(k) for k in (
                    'paperflow_source_sha256','paperflow_manifest_sha256','paperflow_revision',
                    'paperflow_specialty','paperflow_role','paperflow_task'))):
            raise ValueError('native receipt target no longer matches request intent')
    return marker


def wait_projection(queue, job, reason, *, now=None, observed_at=None):
    """Return queue changes; never admit work or call the server here."""
    now = time.time() if now is None else now
    marker = evidence(queue, job)
    first = min(row['intended_at'] for row in marker['requests'].values())
    if now < first or now-first > MAX_TRACK_SECONDS:
        raise ValueError('native outcome remained unknown past the bounded tracking period')
    attempt = int(marker.get('attempts', 0)) + 1
    delay = min(MAX_DELAY_SECONDS, 15 * (2 ** min(attempt-1, 5)))
    delay += int(job['id'][:2], 16) % 8  # deterministic spread across jobs
    if observed_at is not None:
        if not isinstance(observed_at, (int, float)) or not 0 <= observed_at <= now:
            raise ValueError('invalid native observation timestamp')
        marker['observed_at'] = observed_at
    marker.update(phase='waiting', attempts=attempt, last_decision_at=now,
                  next_attempt_at=now + delay, reason=reason)
    return dict(preextract_native_followup=marker,
                preextract_delivery_wait=dict(reason='native_followup', next_attempt_at=now+delay,
                                              owner='original_queue'),
                handoff_status='pending', preextract_handoff_blocked=True,
                handoff_reason='服务器处理结果待按原文档身份继续核对；未重复上传')


def recovered_projection(queue, job):
    """Called only for a claimed, candidate-marked job after owner loss."""
    try:
        return wait_projection(queue, job, 'owner_interrupted')
    except (OSError, ValueError, KeyError, TypeError, OverflowError):
        return None


def observed_terminal(outcome):
    """A remote DONE on one target is not a complete paper."""
    for target in (outcome.get('targets') or {}).values():
        if not isinstance(target, dict):
            return 'invalid_observation'
        for role in ('body', 'images'):
            row = target.get(role)
            if not isinstance(row, dict):
                continue
            if row.get('state') in {'confirmed_failed', 'content_conflict'}:
                return row['state']
            if row.get('error_type') in {'RemoteConflict', 'ValueError', 'KeyError', 'TypeError'}:
                return 'identity_or_evidence_conflict'
    return None
