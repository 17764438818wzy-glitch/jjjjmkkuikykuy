"""Opt-in reuse of the installed native body pipeline, selected by a new worker."""
from __future__ import annotations

import hashlib
import json
import math
import re
import time
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import quote

from upload_tracking import service_key


def _digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(',', ':')).encode('utf8')).hexdigest()


def load_native_batch_config(root, client):
    """No policy means the unchanged direct transport; probe before claiming work."""
    path = Path(root) / 'private' / 'preextract_delivery' / 'native-batch.json'
    if not path.exists():
        return None
    policy = json.loads(path.read_text(encoding='utf8'))
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
    memory_path = Path(policy.get('memory_settings_path', ''))
    profile = policy.get('memory_profile', 'standard')
    profiles = {'standard': (8, 8), 'two_slot_recovery': (2, 3),
                'four_slot_balanced': (4, 4)}
    if profile not in profiles:
        raise ValueError('native memory safety profile is invalid')
    maximum, floor = profiles[profile]
    slots = policy.get('native_workers', 4)
    if type(slots) is not int or not 1 <= slots <= maximum:
        raise ValueError(f'native concurrency must be within 1..{maximum}')
    minimum = policy.get('min_available_gib', 8)
    if (not memory_path.is_file() or type(minimum) not in (int, float)
            or not math.isfinite(minimum) or minimum < floor):
        raise ValueError('native memory safety policy is missing')
    memory_settings = json.loads(memory_path.read_text(encoding='utf8'))
    if memory_settings.get('host') != '192.168.1.120':
        raise ValueError('native memory endpoint does not match this deployment')
    reserve = policy.get('per_slot_reserve_gib', 1.0)
    if type(reserve) not in (int, float) or not math.isfinite(reserve) or not .5 <= reserve <= 2:
        raise ValueError('native per-slot memory reserve is invalid')
    return SimpleNamespace(native_pipeline_id=policy['pipeline_id'], native_ingest_timeout=timeout,
        min_available_gib=minimum, memory_settings=memory_settings,
        per_slot_reserve_gib=reserve, memory_profile=profile, max_native_workers=maximum,
        paperflow_root=policy['paperflow_root'], status_path=path.with_name('native-memory.json'),
        policy_path=path, policy_identity={k:policy.get(k) for k in (
            'schema','enabled','service','pipeline_id','components_sha256','chunk_api_sha256',
            'memory_profile','min_available_gib','per_slot_reserve_gib')})


def native_worker_target(config):
    """Only the local slot limit is live-adjustable; transport identity stays pinned."""
    if getattr(config, 'policy_path', None) is None:
        return 4
    policy = json.loads(config.policy_path.read_text(encoding='utf8'))
    if any(policy.get(k) != v for k,v in config.policy_identity.items()):
        raise ValueError('native transport identity changed during admission')
    slots = policy.get('native_workers', 4)
    maximum = getattr(config, 'max_native_workers', 8)
    if type(slots) is not int or not 1 <= slots <= maximum:
        raise ValueError(f'native concurrency must be within 1..{maximum}')
    return slots


class NativeMemoryGate:
    """Read existing pinned host health; unknown/low memory admits no new work."""
    def __init__(self, config, *, monitor=None):
        if monitor is None:
            from preextract_publish import _paperflow
            _paperflow(config.paperflow_root)
            from paperflow.workbench_resources import ServerMemory
            monitor = ServerMemory(config.memory_settings)
        self.config, self.monitor = config, monitor
        self.last_sample = None

    def can_admit(self, pending_slots=0):
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
    def __init__(self, client, config, task, packet):
        self.client, self.config = client, config
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

        doc = observe()
        if str(doc.get('run')) not in ('0', 'UNSTART'):
            raise ValueError('native submission was already started; reconciliation required')
        started = time.monotonic()
        # Use the original stop-aware request path; a lost response is never retried.
        self.client.request('POST', '/documents/ingest',
                            dict(doc_ids=[document_id], run='1', delete=False))
        polls = 0
        while time.monotonic() - started < timeout:
            doc = observe()
            polls += 1
            if str(doc.get('run')) in ('3', 'DONE') and float(doc.get('progress', 0)) >= 1:
                return dict(document_id=document_id, pipeline_id=pipeline_id, run=doc['run'],
                            progress=doc['progress'], polls=polls,
                            elapsed_seconds=time.monotonic() - started)
            if str(doc.get('run')) in ('FAIL', '4', '-1', '2', 'CANCEL') or float(doc.get('progress', 0)) < 0:
                raise RuntimeError('native job failed; reconciliation required')
            time.sleep(.25)
        raise TimeoutError('native terminal readback deadline; reconciliation required')


def delivery_publisher(client, on_change, prepared, packet):
    from paperflow.rag import RagPublisher
    direct = RagPublisher(client, on_change=on_change)
    config = getattr(client, 'native_batch_config', None)
    if config is None or not _qualified(prepared, packet, direct):
        return direct
    from paperflow.native_delivery import NativeDeliveryPublisher
    return NativeDeliveryPublisher(_NativeClient(client, config, prepared['task'], packet),
                                   on_change, config)
