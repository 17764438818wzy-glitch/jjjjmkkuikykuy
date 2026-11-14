"""Opt-in reuse of the installed native body pipeline, selected by a new worker."""
from __future__ import annotations

import hashlib
import copy
import json
import math
import os
import re
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import quote

from upload_tracking import service_key
from preextract_native_runtime import (native_runtime_readiness, native_document_execution,
    readiness_for_client, runtime_observation_for_client, require_runtime_ready, NativeReadbackPending)


def _digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(',', ':')).encode('utf8')).hexdigest()


def _native_limits(policy, maximum):
    """A pause closes admission while retaining the configured drain capacity."""
    paused = policy.get('operator_upload_paused', False)
    slots = policy.get('native_workers', 4)
    if type(paused) is not bool or type(slots) is not int:
        raise ValueError('invalid native admission control')
    if not 0 <= slots <= maximum or (slots == 0 and not paused):
        raise ValueError(f'native concurrency must be within 1..{maximum}, or explicitly paused')
    capacity = policy.get('native_workers_target', slots or maximum)
    if type(capacity) is not int or not 1 <= capacity <= maximum:
        raise ValueError('invalid native target capacity')
    return (0 if paused else slots), capacity


def _verify_native_transport(policy, client):
    agent = client.request('GET', '/agents/' + policy['pipeline_id'])
    dsl = agent.get('dsl') if isinstance(agent, dict) else None
    if isinstance(dsl, str):
        dsl = json.loads(dsl)
    if (not isinstance(dsl, dict) or agent.get('id') != policy['pipeline_id']
            or _digest(dsl.get('components')) != policy['components_sha256']):
        raise ValueError('native pipeline differs from the verified transport; no delivery admitted')
    deployment = client.request('GET', '/paperflow/deployment')
    if (not isinstance(deployment, dict)
            or deployment.get('chunk_api_sha256') != policy['chunk_api_sha256']
            or service_key(deployment.get('endpoint', '')) != service_key(client.base_url)):
        raise ValueError('native candidate visibility deployment changed; no delivery admitted')


def load_native_batch_config(root, client):
    """No policy means the unchanged direct transport; probe before claiming work."""
    path = Path(root) / 'private' / 'preextract_delivery' / 'native-batch.json'
    if not path.exists():
        return None
    policy_bytes = path.read_bytes()
    policy = json.loads(policy_bytes.decode('utf8'))
    if policy.get('enabled') is not True:
        return None
    if (policy.get('schema') != 'preextract-native-batch/1'
            or service_key(policy.get('service', '')) != service_key(client.base_url)
            or not re.fullmatch('[0-9a-f]{32}', policy.get('pipeline_id', ''))
            or not re.fullmatch('[0-9a-f]{64}', policy.get('components_sha256', ''))
            or not re.fullmatch('[0-9a-f]{64}', policy.get('chunk_api_sha256', ''))):
        raise ValueError('native batch policy identity is invalid; no delivery admitted')
    timeout = policy.get('timeout_seconds', 600)
    if type(timeout) not in (int, float) or not 0 < timeout <= 3600:
        raise ValueError('invalid native batch deadline')
    memory_path = Path(policy.get('memory_settings_path', ''))
    profile = policy.get('memory_profile', 'standard')
    profiles = {'standard': (8, 8), 'two_slot_recovery': (2, 3),
                'four_slot_balanced': (4, 4), 'eight_slot_upload': (8, 4), 'twenty_slot_upload': (20, 8),
                'fifty_slot_backlog': (50, 8)}
    if profile not in profiles:
        raise ValueError('native memory safety profile is invalid')
    maximum, floor = profiles[profile]
    admission, capacity = _native_limits(policy, maximum)
    minimum = policy.get('min_available_gib', 8)
    if (not memory_path.is_file() or type(minimum) not in (int, float)
            or not math.isfinite(minimum) or minimum < floor):
        raise ValueError('native memory safety policy is missing')
    memory_settings = json.loads(memory_path.read_text(encoding='utf8'))
    if memory_settings.get('host') != '192.168.1.120':
        raise ValueError('native memory endpoint does not match this deployment')
    reserve = policy.get('per_slot_reserve_gib', 1.0)
    minimum_reserve = .2 if profile in ('eight_slot_upload', 'twenty_slot_upload', 'fifty_slot_backlog') else .5
    if type(reserve) not in (int, float) or not math.isfinite(reserve) or not minimum_reserve <= reserve <= 2:
        raise ValueError('native per-slot memory reserve is invalid')
    restore = None
    if profile == 'fifty_slot_backlog':
        restore = policy.get('restore_after_drain')
        if not isinstance(restore, dict) or not re.fullmatch('[0-9a-f]{64}', str(restore.get('sha256', ''))):
            raise ValueError('temporary native policy requires a verified restore policy')
        backup = Path(restore.get('path', '')).resolve()
        if backup.parent != path.resolve().parent or backup == path.resolve() or not backup.is_file():
            raise ValueError('temporary native restore path is invalid')
        original_bytes = backup.read_bytes()
        if hashlib.sha256(original_bytes).hexdigest() != restore['sha256']:
            raise ValueError('temporary native restore policy changed')
        original = json.loads(original_bytes.decode('utf8'))
        identity_fields = ('schema', 'enabled', 'service', 'pipeline_id', 'components_sha256',
                           'chunk_api_sha256', 'memory_settings_path', 'paperflow_root',
                           'min_available_gib', 'per_slot_reserve_gib')
        if (any(original.get(k) != policy.get(k) for k in identity_fields)
                or original.get('memory_profile') != 'twenty_slot_upload'
                or type(original.get('native_workers')) is not int
                or not 1 <= original['native_workers'] <= 20
                or original.get('restore_after_drain') is not None):
            raise ValueError('temporary native restore policy identity is invalid')
    if admission:
        _verify_native_transport(policy, client)
    return SimpleNamespace(native_pipeline_id=policy['pipeline_id'], native_ingest_timeout=timeout,
        native_transport_verified=bool(admission),
        native_service=client.base_url,
        native_runtime_observation=lambda: runtime_observation_for_client(
            client, path.with_name('native-runtime-observation.json')),
        min_available_gib=minimum, memory_settings=memory_settings,
        per_slot_reserve_gib=reserve, memory_profile=profile, max_native_workers=maximum,
        paperflow_root=policy['paperflow_root'], native_workers_capacity=capacity,
        status_path=path.with_name('native-memory.json'),
        policy_path=path, temporary_policy_sha256=hashlib.sha256(policy_bytes).hexdigest(),
        restore_after_drain=restore, policy_identity={k:policy.get(k) for k in (
            'schema','enabled','service','pipeline_id','components_sha256','chunk_api_sha256',
            'memory_profile','min_available_gib','per_slot_reserve_gib','restore_after_drain')})


def restore_temporary_native_policy(config):
    """Restore the opt-in backlog policy on cooperative exit, without overwriting edits."""
    if getattr(config, 'memory_profile', None) != 'fifty_slot_backlog':
        return False
    path = config.policy_path
    if hashlib.sha256(path.read_bytes()).hexdigest() != config.temporary_policy_sha256:
        return False
    restore = config.restore_after_drain
    original = Path(restore['path']).read_bytes()
    if hashlib.sha256(original).hexdigest() != restore['sha256']:
        raise ValueError('temporary native restore backup changed; policy retained')
    fd, temporary = tempfile.mkstemp(prefix='.native-batch-restore-', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(original)
            stream.flush()
            os.fsync(stream.fileno())
        if hashlib.sha256(path.read_bytes()).hexdigest() != config.temporary_policy_sha256:
            return False
        os.replace(temporary, path)
        return True
    finally:
        Path(temporary).unlink(missing_ok=True)


def _local_native_target(config):
    """Only the local slot limit is live-adjustable; transport identity stays pinned."""
    if getattr(config, 'policy_path', None) is None:
        return 4
    policy = json.loads(config.policy_path.read_text(encoding='utf8'))
    if any(policy.get(k) != v for k,v in config.policy_identity.items()):
        raise ValueError('native transport identity changed during admission')
    target = _native_limits(policy, getattr(config, 'max_native_workers', 8))[0]
    return target if getattr(config, 'native_transport_verified', True) is True else 0


def native_worker_target(config):
    """Preserve capacity; admit only when both local control and executor are ready."""
    target = _local_native_target(config)
    return target if target and readiness_for_client(config=config)['state'] == 'ready' else 0


def native_worker_capacity(config):
    """Capacity is telemetry, independent of the current admission permission."""
    if getattr(config, 'policy_path', None) is None:
        return getattr(config, 'native_workers_capacity', 4)
    policy = json.loads(config.policy_path.read_text(encoding='utf8'))
    if any(policy.get(k) != v for k, v in config.policy_identity.items()):
        raise ValueError('native transport identity changed during admission')
    return _native_limits(policy, getattr(config, 'max_native_workers', 8))[1]


class NativeMemoryGate:
    """Read existing pinned host health; unknown/low memory admits no new work."""
    def __init__(self, config, *, monitor=None):
        if monitor is None and getattr(config, 'memory_profile', None) != 'fifty_slot_backlog':
            from preextract_publish import _paperflow
            _paperflow(config.paperflow_root)
            from paperflow.workbench_resources import ServerMemory
            monitor = ServerMemory(config.memory_settings)
        self.config, self.monitor = config, monitor
        self.last_sample = None

    def can_admit(self, pending_slots=0):
        if native_worker_target(self.config) == 0:
            runtime = readiness_for_client(config=self.config)
            self.last_sample = dict(ready=False, observed_at=time.time(),
                available_gib=None, reason=('admission_paused' if _local_native_target(self.config) == 0 else runtime['reason_code']),
                runtime=runtime)
            return False
        if getattr(self.config, 'memory_profile', None) == 'fifty_slot_backlog':
            ready = type(pending_slots) is int and 0 <= pending_slots < 50
            self.last_sample = dict(ready=ready, observed_at=time.time(),
                available_gib=None, memory_admission_bypassed=True,
                reason='user_authorized_temporary_backlog')
            return ready
        try:
            sample = self.monitor.read()
            total, used, observed = (sample.get(k) for k in ('total_bytes', 'used_bytes', 'observed_at'))
            valid = (sample.get('state') == 'available'
                and type(total) is int and type(used) is int and 0 <= used <= total
                and type(observed) in (int, float) and 0 <= time.time()-observed <= 10)
            available = (total-used) / 2**30 if valid else None
            reserve = (pending_slots + 1) * getattr(self.config, 'per_slot_reserve_gib', 1.0)
            ready = (type(pending_slots) is int
                     and 0 <= pending_slots < getattr(self.config, 'max_native_workers', 8)
                     and valid and available - reserve >= self.config.min_available_gib)
            self.last_sample = dict(ready=bool(ready), observed_at=observed,
                available_gib=available, minimum_gib=self.config.min_available_gib,
                reserved_gib=reserve)
        except Exception:
            ready = False
            self.last_sample = dict(ready=False, observed_at=time.time(),
                available_gib=None, minimum_gib=self.config.min_available_gib)
        return bool(ready)


def _qualified(prepared, packet, publisher):
    """The native tokenizer has a narrower limit; never let it truncate records."""
    evidence = prepared.get('validation', {}).get('embedding', {})
    cl_limit, model_budget = evidence.get('cl_limit'), evidence.get('model_budget')
    if (evidence.get('admission_verified') is not True
            or type(cl_limit) is not int or not 0 < cl_limit <= 2038
            or type(model_budget) is not int or not 0 < model_budget <= 2000):
        return False
    records = publisher.records(packet)['body']
    if not 1 <= len(records) <= 1000:
        return False
    rows = evidence.get('records', [])
    checked = {(r.get('role'), r.get('record_id')): r for r in rows if isinstance(r, dict)}
    if len(checked) != len(rows):
        return False
    for record in records:
        row = checked.get(('body', record[0]), {})
        text = publisher.render(prepared['task'], packet, record)
        if (row.get('input_sha256') != hashlib.sha256(text.encode('utf8')).hexdigest()
                or any(type(row.get(k)) is not int or not 0 <= row[k] <= limit
                       for k, limit in [('model_tokens', model_budget), ('cl100k_tokens', cl_limit)])):
            return False
    return True


class _NativeClient:
    def __init__(self, client, config, task, packet, *, submitted_documents=()):
        self.client, self.config = client, config
        self.submitted_documents = set(submitted_documents)
        self.identities = {route['paper_dataset_id']: dict(
            paperflow_source_sha256=task['source_sha256'],
            paperflow_manifest_sha256=packet['manifest_sha256'],
            paperflow_revision=str(task['revision']), paperflow_specialty=route['code'],
            paperflow_role='body', paperflow_task=task['task_id']) for route in packet['routes']}

    def __getattr__(self, name):
        return getattr(self.client, name)

    def ingest(self, dataset_id, document_id, pipeline_id, *, timeout=600):
        if (dataset_id not in self.identities or pipeline_id != self.config.native_pipeline_id
                or not 0 < timeout <= 3600):
            raise ValueError('unbound native submission')
        endpoint = (f'/datasets/{quote(dataset_id, safe="")}/documents'
                    f'?id={quote(document_id, safe="")}&page=1&page_size=100')

        def observe():
            data = self.client.request('GET', endpoint)
            docs = data.get('docs', []) if isinstance(data, dict) else data
            if not isinstance(docs, list) or len(docs) != 1:
                raise ValueError('native document identity is missing')
            doc = docs[0]
            if (doc.get('id') != document_id or doc.get('pipeline_id') != pipeline_id
                    or str(doc.get('status')) != '0'
                    or any((doc.get('meta_fields') or {}).get(k) != v
                           for k, v in self.identities[dataset_id].items())):
                raise ValueError('native document ownership, pipeline or visibility changed')
            return doc

        identity = dict(self.identities[dataset_id], dataset_id=dataset_id,
            document_id=document_id, pipeline_id=pipeline_id,
            task_id=self.identities[dataset_id]['paperflow_task'],
            source_sha256=self.identities[dataset_id]['paperflow_source_sha256'],
            manifest_sha256=self.identities[dataset_id]['paperflow_manifest_sha256'])
        # A queue-owned job gives its slot back quickly after a durable submit.
        # Standalone and managed publishers retain their original wait behavior.
        if getattr(self.client, 'native_auto_follow', False):
            timeout = min(timeout, 2.0)
        started = time.monotonic()
        doc = observe()
        run = str(doc.get('run'))
        if run in ('0', 'UNSTART'):
            if (dataset_id, document_id) in self.submitted_documents:
                raise ValueError('recorded native intent has an unknown outcome; no resubmission')
            require_runtime_ready(self.client, self.config)
            # SQLite queue intent is durable before the external effect. A
            # lost response never authorizes another POST for this identity.
            if getattr(self.client, 'native_auto_follow', False):
                hook = getattr(self.client, 'native_before_ingest', None)
                if not callable(hook):
                    raise ValueError('native follow-up intent hook unavailable')
                hook(identity)
            self.submitted_documents.add((dataset_id, document_id))
            self.client.request('POST', '/documents/ingest',
                                dict(doc_ids=[document_id], run='1', delete=False))
            doc = observe()
        elif run not in ('1', 'RUNNING', '3', 'DONE', 'FAIL', '4', '-1', '2', 'CANCEL'):
            raise ValueError('native outcome is unknown; reconciliation required')
        polls = 0
        while time.monotonic() - started < timeout:
            polls += 1
            if str(doc.get('run')) in ('3', 'DONE') and float(doc.get('progress', 0)) >= 1:
                return dict(document_id=document_id, pipeline_id=pipeline_id, run=doc['run'],
                            progress=doc['progress'], polls=polls,
                            elapsed_seconds=time.monotonic() - started)
            if str(doc.get('run')) in ('FAIL', '4', '-1', '2', 'CANCEL') or float(doc.get('progress', 0)) < 0:
                raise RuntimeError('native job failed; reconciliation required')
            time.sleep(.25)
            doc = observe()
        execution = native_document_execution(doc, readiness_for_client(self.client, self.config),
                                              dataset_id=dataset_id, document_id=document_id)
        raise NativeReadbackPending(identity, execution)


def delivery_publisher(client, on_change, prepared, packet):
    from paperflow.rag import RagPublisher
    config = getattr(client, 'native_batch_config', None)
    active = _ContinuationClient(client, config, lambda: True) if config is not None else client
    direct = RagPublisher(active, on_change=on_change)
    if config is None or not _qualified(prepared, packet, direct):
        return direct
    from paperflow.native_delivery import NativeDeliveryPublisher
    return NativeDeliveryPublisher(_NativeClient(active, config, prepared['task'], packet),
                                   on_change, config)


class _ReadOnlyClient:
    """Observe stopped deliveries without clearing the business stop event."""
    def __init__(self, client):
        self.client = client

    def request(self, method, endpoint, body=None, *args, **kwargs):
        if method != 'GET':
            raise ValueError('reconciliation observation permits GET only')
        return self.client.request(method, endpoint, body, *args, **kwargs)

    def __getattr__(self, name):
        if name in {'upload_bytes', 'upload_file', 'ingest'}:
            raise AttributeError(name)
        return getattr(self.client, name)


class _ContinuationClient:
    """The lock/control owner supplies a live permission, checked at every write."""
    def __init__(self, client, config, control, *, submitted_chunks=()):
        self.client, self.config, self.control = client, config, control
        self.submitted_chunks = set(submitted_chunks)

    def permit(self):
        if self.control() is not True or _local_native_target(self.config) <= 0:
            raise InterruptedError('delivery continuation is paused; no new write admitted')
        require_runtime_ready(self.client, self.config)
        if self.control() is not True or _local_native_target(self.config) <= 0:
            raise InterruptedError('delivery continuation is paused; no new write admitted')

    def request(self, method, endpoint, body=None, *args, **kwargs):
        if method != 'GET':
            self.permit()
        parts = endpoint.split('?', 1)[0].strip('/').split('/')
        if (method == 'POST' and len(parts) == 5 and parts[0] == 'datasets'
                and parts[2] == 'documents' and parts[4] == 'chunks'):
            marker = (body or {}).get('content', '').split('\n', 1)[0]
            if not (marker.startswith('[PAPERFLOW]') and marker.endswith('[/PAPERFLOW]')):
                raise ValueError('chunk write has no bound record identity')
            identity = json.loads(marker[len('[PAPERFLOW]'):-len('[/PAPERFLOW]')])
            key = (parts[1], parts[3], identity['record'])
            if key in self.submitted_chunks:
                raise ValueError('previous chunk intent has an unknown outcome; no replay')
            # The publisher persisted its per-record intent before reaching this boundary.
            self.submitted_chunks.add(key)
        return self.client.request(method, endpoint, body, *args, **kwargs)

    def upload_bytes(self, *args, **kwargs):
        self.permit()
        return self.client.upload_bytes(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.client, name)


class _OriginalBindingClient:
    """Reuse the native binder for one original ID, without upload authority."""
    def __init__(self, client, dataset, document):
        self.client, self.dataset, self.document = client, dataset, document

    def request(self, method, endpoint, body=None):
        from urllib.parse import parse_qs, urlsplit
        parsed = urlsplit(endpoint)
        query = parse_qs(parsed.query)
        expected = f'/datasets/{quote(self.dataset, safe="")}/documents'
        if (method == 'GET' and parsed.path == expected and query.get('id') == [self.document]
                and not (parsed.scheme or parsed.netloc or parsed.fragment)):
            result = self.client.request(method, endpoint, body)
            docs = result.get('docs', []) if isinstance(result, dict) else result
            if not isinstance(docs, list) or any(d.get('id') != self.document for d in docs):
                raise ValueError('original binding document identity changed')
            return result
        if method == 'PATCH' and endpoint == f'{expected}/{self.document}':
            return self.client.request(method, endpoint, body)
        raise ValueError('original binding only permits the saved document')

    def download_bytes(self, dataset, document):
        if (dataset, document) != (self.dataset, self.document):
            raise ValueError('original binding download identity changed')
        from paperflow.rag import RagPublisher
        return RagPublisher(self.client)._download(dataset, document)

    def upload_bytes(self, *args, **kwargs):
        raise ValueError('original binding cannot upload a document')


def _observe_native_targets(task, packet, existing, client):
    """Build a separate projection; never change the persisted original receipts."""
    from paperflow.rag import RagPublisher, RemoteConflict, compact
    from paperflow.native_delivery import NativeDeliveryPublisher
    reader = RagPublisher(_ReadOnlyClient(client))
    records_by_role = {k: v for k, v in reader.records(packet).items() if v}
    shadow = copy.deepcopy(existing['targets'])
    observed = {}
    config = getattr(client, 'native_batch_config', None)
    if config is None:
        pipelines = {obj.get('native_batch', {}).get('pipeline_id') for target in existing['targets'].values()
                     for role, obj in target.items() if role == 'body' and isinstance(obj, dict) and obj.get('native_batch')}
        if len(pipelines) == 1:
            config = SimpleNamespace(native_service=client.base_url, native_pipeline_id=next(iter(pipelines)))
    runtime = readiness_for_client(client, config)
    for route in packet['routes']:
        code = route['code']
        target = observed[code] = dict(verified=False, retrieval_verified=False)
        for role, records in records_by_role.items():
            dataset = route['paper_dataset_id' if role == 'body' else 'image_dataset_id']
            row = target[role] = dict(dataset_id=dataset, state='not_submitted',
                                     content_verified=False, expected_records=len(records), observed_at=time.time())
            obj = shadow.get(code, {}).get(role)
            if obj is None:
                continue  # _document durably records an intent before any external effect.
            row['state'] = 'outcome_unknown'
            if not isinstance(obj, dict) or not obj.get('document_id'):
                continue  # A lost upload response does not authorize another upload.
            row['document_id'] = obj['document_id']
            try:
                identity = dict(paperflow_source_sha256=task['source_sha256'],
                    paperflow_manifest_sha256=packet['manifest_sha256'],
                    paperflow_revision=str(task['revision']), paperflow_specialty=code,
                    paperflow_role=role, paperflow_task=task['task_id'])
                if obj.get('dataset_id') != dataset or obj.get('identity') != identity:
                    raise RemoteConflict('saved target identity changed')
                docs = reader.documents(dataset, id=obj['document_id'])
                if len(docs) != 1 or docs[0].get('id') != obj['document_id']:
                    raise RemoteConflict('original document identity changed')
                doc = docs[0]
                metadata = doc.get('meta_fields') or {}
                if not metadata:
                    names = reader.document_names(task, packet, code, role)
                    payload = NativeDeliveryPublisher(None, None, config).document_bytes(task, packet, role, identity)
                    if doc.get('name') not in names or reader._download(dataset, obj['document_id']) != payload:
                        raise RemoteConflict('original empty metadata bytes or name changed')
                    row['metadata_binding_required'] = True
                elif any(metadata.get(k) != v for k, v in identity.items()):
                    raise RemoteConflict('original document identity changed')
                row.update(remote_run=doc.get('run'), enabled=str(doc.get('status')) == '1')
                native = obj.get('native_batch')
                if role == 'body' and native:
                    config = SimpleNamespace(native_pipeline_id=native.get('pipeline_id'))
                    publisher = NativeDeliveryPublisher(None, None, config)
                    payload = publisher.document_bytes(task, packet, role, identity)
                    binding = dict(pipeline_id=config.native_pipeline_id, record_count=len(records),
                                   transport_sha256=hashlib.sha256(payload).hexdigest())
                    if (any(native.get(k) != v for k, v in binding.items())
                            or doc.get('pipeline_id') != config.native_pipeline_id):
                        raise RemoteConflict('native transport binding changed')
                    row['execution'] = native_document_execution(doc, runtime,
                        dataset_id=dataset, document_id=obj['document_id'])
                    row['identity'] = dict(identity, dataset_id=dataset, document_id=obj['document_id'],
                                           pipeline_id=config.native_pipeline_id)
                    run = str(doc.get('run'))
                    if run in {'4', 'FAIL', '-1', '2', 'CANCEL'} or float(doc.get('progress', 0)) < 0:
                        row['state'] = 'confirmed_failed'
                        continue
                    if run in {'1', 'RUNNING'}:
                        row['state'] = 'processing'
                        continue
                    if run not in {'3', 'DONE'} or float(doc.get('progress', 0)) < 1:
                        continue  # Includes UNSTART with a previously persisted native intent.
                elif role == 'body':
                    if str(doc.get('run')) in {'0', 'UNSTART'}:
                        row['state'] = 'accepted'
                    continue
                chunks = reader.chunks(dataset, obj['document_id'])
                expected = {reader.render(task, packet, record): record for record in records}
                if role == 'images':
                    present = {c.get('content') for c in chunks}
                    missing = set(expected) - present
                    if (missing and len(present) == len(chunks) and present <= set(expected)):
                        pending = [expected[text][0] for text in missing]
                        row.update(state=('outcome_unknown' if any(rid in obj.get('chunks', {}) for rid in pending)
                                          else 'accepted_partial'),
                                   returned_records=len(chunks), pending_record_ids=sorted(pending))
                        continue
                if (len(expected) != len(records) or len(chunks) != len(records)
                        or {c.get('content') for c in chunks} != set(expected)):
                    row['state'] = 'content_conflict'
                    continue
                by_content = {c['content']: c for c in chunks}
                obj['chunks'] = {record[0]: dict(status='written',
                    chunk_id=by_content[text]['id'], image_id=by_content[text].get('image_id'),
                    content_sha256=hashlib.sha256(text.encode('utf8')).hexdigest())
                    for text, record in expected.items()}
                row.update(state='completed_pending_verification', content_verified=True,
                           returned_records=len(chunks))
            except Exception as exc:
                row.update(error_type=type(exc).__name__)
        if (all(target[role]['content_verified'] for role in records_by_role)
                and not any(target[role].get('metadata_binding_required') for role in records_by_role)):
            try:
                reader.verify(task, dict(packet, routes=[route]), shadow)
                target['verified'] = True
            except Exception as exc:
                target['verification_error_type'] = type(exc).__name__
        # Retrieval is deliberately not run in stopped/GET-only observation mode.
    return dict(mode='read_only', status='reconciliation_required', observed_at=time.time(),
        runtime=runtime, next_action='observe_original_identities', retry_upload_allowed=False,
        identity=dict(task_id=task['task_id'], source_sha256=task['source_sha256'],
                      revision=task['revision'], manifest_sha256=packet['manifest_sha256']),
        required_specialties=[r['code'] for r in packet['routes']], targets=observed,
        whole_source_complete=False, receipt_changed=False,
        completion_scope='body_and_required_images; original attachments remain in handoff projection')


def reconcile_native_publication(prepared, *, receipt_path, client,
                                 continue_delivery=False, control=None):
    """GET-only by default; continuation requires the caller's lock and live control.

    The desktop handoff owns operation.lock, service binding, queue projections and
    original-attachment completion. This function never starts a worker or model.
    Pass the observation client directly, without clearing a stopped client wrapper.
    """
    from preextract_publish import _paperflow, _write_json
    package = _paperflow(prepared['paperflow_root'])
    from paperflow.rag import RagPublisher
    from paperflow.native_delivery import NativeDeliveryPublisher
    receipt_path = Path(receipt_path)
    original = receipt_path.read_bytes()
    existing = json.loads(original)
    task = prepared['task']
    packet = package.validate_package(prepared['package_path'], task, prepared['catalog'], strict_seal=True)
    codes = [r['code'] for r in packet['routes']]
    if (not codes or len(set(codes)) != len(codes)
            or len({r['paper_dataset_id'] for r in packet['routes']}) != len(codes)
            or existing.get('task') != task
            or existing.get('manifest_sha256') != packet['manifest_sha256']
            or packet['manifest_sha256'] != prepared['validation']['manifest_sha256']
            or existing.get('transport') != 'native_body_batch/1'
            or existing.get('status') not in {'pending', 'uncertain', 'complete'}
            or not isinstance(existing.get('targets'), dict)
            or not set(existing['targets']) <= set(codes)):
        raise ValueError('original native receipt, package or specialty identity changed')
    if type(continue_delivery) is not bool:
        raise ValueError('explicit continuation must be a boolean')
    guarded = None
    if continue_delivery:
        config = getattr(client, 'native_batch_config', None)
        if config is None or not callable(control):
            raise ValueError('continuation requires selected native configuration and live control')
        submitted_chunks = {(obj['dataset_id'], obj['document_id'], rid)
                            for target in existing['targets'].values() for role, obj in target.items()
                            if role == 'images' and isinstance(obj, dict) and obj.get('document_id')
                            for rid in obj.get('chunks', {})}
        guarded = _ContinuationClient(client, config, control, submitted_chunks=submitted_chunks)
        guarded.permit()
        if any(obj.get('native_batch', {}).get('pipeline_id') != config.native_pipeline_id
               for target in existing['targets'].values() for role, obj in target.items()
               if role == 'body' and isinstance(obj, dict) and obj.get('native_batch')):
            raise ValueError('selected native pipeline differs from the original binding')
    report = _observe_native_targets(task, packet, existing, client)
    if not continue_delivery:
        return report
    blocked = {'outcome_unknown', 'confirmed_failed', 'content_conflict', 'processing'}
    admitted = [route for route in packet['routes']
                if not report['targets'][route['code']].get('verification_error_type')
                and all(row['state'] not in blocked
                        for role, row in report['targets'][route['code']].items()
                        if role in {'body', 'images'})]
    if not admitted:
        # Unknown original requests remain held; a different route can be
        # admitted independently without replaying any of those requests.
        return report
    if not _qualified(prepared, packet, RagPublisher(None)):
        raise ValueError('original native package admission is missing or changed')
    guarded.permit()
    if receipt_path.read_bytes() != original:
        raise ValueError('receipt changed during observation; retain the current owner')
    submitted = {(obj['dataset_id'], obj['document_id'])
                 for target in existing['targets'].values() for role, obj in target.items()
                 if role == 'body' and isinstance(obj, dict) and obj.get('native_batch') and obj.get('document_id')}
    state = copy.deepcopy(existing)
    state['completion_scope'] = 'body_and_required_images_all_specialties'
    publisher = NativeDeliveryPublisher(_NativeClient(guarded, config, task, packet,
                                                      submitted_documents=submitted), None, config)
    binding_required = any(row.get('metadata_binding_required')
                           for target in report['targets'].values() for role, row in target.items()
                           if role in {'body', 'images'})
    if existing['status'] == 'complete' and len(admitted) == len(packet['routes']) and not binding_required:
        publisher.verify(task, packet, state['targets'])
        publisher.verify_retrieval(task, packet, state['targets'])
        publisher.states(state['targets'], enabled=True)
        return state
    archive = receipt_path.with_name(receipt_path.stem + '.before-native-reconcile.' + str(time.time_ns()) + '.json')
    archive.write_bytes(original)
    state.update(status='pending', stage='publishing',
                 reconciliation=dict(previous_receipt=str(archive), no_native_resubmission=True,
                     admitted_routes=[r['code'] for r in admitted],
                     blocked_routes=[r['code'] for r in packet['routes'] if r not in admitted],
                     pending_routes=[]))
    state.pop('error_type', None)

    def persist(receipts):
        state['targets'] = receipts
        _write_json(receipt_path, state)

    publisher.on_change = persist
    try:
        persist(state['targets'])
        for route in admitted:
            code = route['code']
            subset = dict(packet, routes=[route])
            state['stage'] = 'publishing'
            persist(state['targets'])
            for role, row in report['targets'][code].items():
                if role in {'body', 'images'} and row.get('metadata_binding_required'):
                    obj = state['targets'][code][role]
                    binder = NativeDeliveryPublisher(_OriginalBindingClient(guarded,
                        obj['dataset_id'], obj['document_id']), persist, config)
                    binder._document(task, packet, route, role, state['targets'][code], state['targets'])
            try:
                publisher.publish(task, subset, state['targets'])
                state['stage'] = 'readback'
                persist(state['targets'])
                publisher.verify(task, subset, state['targets'])
                guarded.permit()
                state['stage'] = 'activating'
                persist(state['targets'])
                publisher.set_enabled({code: state['targets'][code]}, True)
                state['stage'] = 'retrieval'
                persist(state['targets'])
                publisher.verify_retrieval(task, subset, state['targets'])
                publisher.states({code: state['targets'][code]}, enabled=True)
            except NativeReadbackPending:
                state['reconciliation']['pending_routes'].append(code)
                persist(state['targets'])
        if (state['reconciliation']['blocked_routes'] or state['reconciliation']['pending_routes']
                or not all(state['targets'].get(code, {}).get('verified') is True
                           and state['targets'][code].get('retrieval_verified') is True for code in codes)):
            state.update(status='pending', whole_source_complete=False, partial=True)
            persist(state['targets'])
            return state
        guarded.permit()
        state.pop('partial', None)
        state.pop('whole_source_complete', None)
        state.update(status='complete', stage='verified', source_retained=True,
                     cleanup='not_requested', last_remote_verified_at=time.time())
        persist(state['targets'])
        return state
    except BaseException as exc:
        state.update(status='uncertain', error_type=type(exc).__name__)
        persist(state['targets'])
        raise
