"""Manual, isolated replay into two new private RAGFlow test datasets.

This is not a Prefect worker, production admission gate, or release adapter.
The password is read once from the terminal and is never stored.  Remote write
intents are recorded before sending and an unknown outcome stops this run.
"""
import copy
import getpass
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import sys
import tempfile
import threading
import time
from contextlib import contextmanager


HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parents[1]
sys.path.insert(0, str(WORKSPACE / 'prefect-paperflow'))
sys.path.insert(0, str(HERE))
from paperflow_prefect.settings import Settings
from paperflow_prefect.legacy import bind
settings = Settings.load()
bind(settings)
from auto_config import load_key
from ragflow_client import RagflowClient
from preextract_batch_delivery import load_native_batch_config, NativeMemoryGate, _local_native_target, _qualified
from preextract_native_runtime import native_runtime_readiness
from paperflow.native_delivery import NativeDeliveryPublisher
from paperflow.package import validate_package
from paperflow.rag import compact
from effect_journal import EffectJournal
from guarded_rag_client import GuardedRagClient, _json, _sha
from native_ingest import FreshSubmissionProof, NativeIngestClient
from filelock import FileLock


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save(path, value):
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('w', encoding='utf8', newline='\n') as stream:
        json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)


class TestPermit:
    """Manual one-process permit; deliberately not a new Prefect control."""
    def check_current(self): pass
    def start_transport(self, record): record()
    def start_observation(self): pass


@contextmanager
def admit(*_): yield TestPermit()


@contextmanager
def observe(): yield TestPermit()


def main():
    if len(sys.argv) not in (2, 3) or (len(sys.argv) == 3 and sys.argv[2] != '--resume-known-unsent'):
        raise ValueError('one existing isolated test case is required')
    resume = len(sys.argv) == 3
    case = (HERE / 'live-tests' / sys.argv[1]).resolve(strict=True)
    if case.parent != (HERE / 'live-tests').resolve(strict=True):
        raise ValueError('case escapes test root')
    report = json.loads((case / 'report.json').read_text(encoding='utf8'))
    prior = ('rag-selftest.json', 'rag-effects.sqlite3', 'rag-receipts.json', 'rag-proofs', 'rag-retrieval.jsonl')
    if report['state'] != 'archive_verified':
        raise ValueError('case is not archive-verified')
    if not resume and any((case / name).exists() for name in prior):
        raise ValueError('case is not archive-verified or has a previous RAG attempt')
    if resume and ((case / 'rag-resume-preflight.json').exists()
                   or (case / 'rag-resume-script.py.txt').exists()
                   or (case / 'rag-original-stop.json').exists()
                   or (case / 'rag-original-receipts.json').exists()
                   or not all((case / name).exists() for name in prior[:-1])):
        raise ValueError('original evidence missing or this case already had a resume attempt')
    password = getpass.getpass('') if sys.stdin.isatty() else sys.stdin.readline().rstrip('\r\n')
    if not password:
        raise ValueError('server credential unavailable')
    prepared = json.loads((case / 'prepared-publication.json').read_text(encoding='utf8'))
    task = prepared['task']
    packet = validate_package(case / 'publication', task, prepared['catalog'], strict_seal=True)
    if (packet['manifest_sha256'] != report['manifest_sha256']
            or digest(case / 'input/body.doc') != task['source_sha256']):
        raise ValueError('test packet or test input changed')
    raw = RagflowClient(settings['rag_url'], load_key(settings.path('settings_root'), 'ragflow'))
    config = load_native_batch_config(Path(r'E:\uv\ragflow_upstream\data'), raw)
    check_publisher = NativeDeliveryPublisher(None, lambda _: None, config)
    if not _qualified(prepared, packet, check_publisher):
        raise ValueError('sealed packet is not qualified for native ingestion')
    if len(packet['routes']) != 2 or len(check_publisher.records(packet)['body']) != 59:
        raise ValueError('historical replay route or record count differs')
    existing_datasets = raw.datasets()
    by_id = {d.get('id'): d for d in existing_datasets}
    formal = {r['paper_dataset_id'] for r in packet['routes']}
    if len(formal) != 2 or not formal <= set(by_id):
        raise ValueError('original specialty targets are not uniquely readable')
    embeddings = {by_id[ds].get('embedding_model') for ds in formal}
    if len(embeddings) != 1 or not next(iter(embeddings)):
        raise ValueError('specialty embedding identities differ or are unavailable')
    model = next(iter(embeddings))
    tenants = {by_id[ds].get('tenant_id') for ds in formal}
    creators = {by_id[ds].get('created_by') for ds in formal}
    if (len(tenants) != 1 or not next(iter(tenants)) or len(creators) != 1
            or not next(iter(creators))):
        raise ValueError('formal dataset tenant or creator cannot be confirmed')
    tenant, creator = next(iter(tenants)), next(iter(creators))

    monitor_config = json.loads((case / 'observer-config.json').read_text(encoding='utf8'))
    if monitor_config.get('schema') != 'rag-runtime-monitor/1' or monitor_config.get('enabled') is not True:
        raise ValueError('the isolated runtime observer is not enabled')
    observer_cache = Path(monitor_config['output_path']).resolve()
    if not observer_cache.is_relative_to(case):
        raise ValueError('runtime observer output escapes test case')
    monitor_path = Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py')
    spec = importlib.util.spec_from_file_location('paperflow_test_observer', monitor_path)
    monitor = importlib.util.module_from_spec(spec); spec.loader.exec_module(monitor)
    import paramiko
    ssh = paramiko.SSHClient(); ssh.load_host_keys(config.memory_settings['known_hosts'])
    ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
    ssh.connect(config.memory_settings['host'], port=config.memory_settings['port'],
                username=config.memory_settings['username'], password=password,
                look_for_keys=False, allow_agent=False, timeout=5, auth_timeout=5, banner_timeout=5)
    expected_observer = {'rag_runtime_observe.py': '327c1120a956ce64e627ff262fed3a8a40b28d19dd6da9557ccccde6005da43b',
                         'rag_runtime_snapshot.py': 'e7b3920d35a16e22d156a7f5a07149ef9a4b560e3122805820fcf620d45024b8'}
    runtime_snapshot = {'observation': None}
    memory_config = copy.copy(config)
    memory_config.native_runtime_observation = lambda: runtime_snapshot['observation']
    memory = NativeMemoryGate(memory_config)
    state_path = case / 'rag-selftest.json'
    receipt_path = case / 'rag-receipts.json'
    journal = EffectJournal(case / 'rag-effects.sqlite3')
    proof_dir = case / 'rag-proofs'
    if resume:
        state_bytes, receipt_bytes = state_path.read_bytes(), receipt_path.read_bytes()
        state = json.loads(state_bytes.decode('utf8'))
        receipts = json.loads(receipt_bytes.decode('utf8'))
        if (state.get('case_id') != case.name or state.get('state') != 'stopped_for_review'
                or state.get('error') != 'native broker is active or its current occupancy is unknown'
                or state.get('manifest_sha256') != packet['manifest_sha256']
                or state.get('source_sha256') != task['source_sha256']):
            raise ValueError('previous stop is not the known pre-send health-gate case')
        for filename, content in (('rag-original-stop.json', state_bytes),
                                  ('rag-original-receipts.json', receipt_bytes)):
            with (case / filename).open('xb') as out:
                out.write(content); out.flush(); os.fsync(out.fileno())
    else:
        state = {'schema': 'paperflow-isolated-rag-selftest/1', 'case_id': case.name,
                 'state': 'preparing', 'manifest_sha256': packet['manifest_sha256'],
                 'source_sha256': task['source_sha256'], 'formal_targets_write_allowed': False,
                 'new_prefect_connected': False, 'datasets': {}, 'manual_test_only': True}
        save(state_path, state)
        journal.initialize()
        receipts = {}
        save(receipt_path, receipts)
        proof_dir.mkdir(exist_ok=False)
    remote = None
    targets = {}
    upload_expected = {}

    def update(): save(state_path, state)
    def changed(value): save(receipt_path, value)

    health_wait_seconds, health_poll_seconds = 90.0, 2.0
    selftest_ingest_guard = threading.Lock()

    def refresh_runtime(*, wait=True, deadline=None):
        """Observe before sending; never turn an unknown sample into admission.

        Long waits are requested only before the document fence. The final
        in-fence check uses wait=False and records the exact blocking reason.
        """
        class _RecordedPause(InterruptedError):
            pass

        limit = time.monotonic() + (health_wait_seconds if wait else 8.0)
        if deadline is not None:
            limit = min(limit, deadline)
        last_notified, last_reason = None, None

        def record(event):
            event['at_ns'] = time.time_ns()
            with (case / 'rag-runtime-events.jsonl').open('a', encoding='utf8', newline='\n') as out:
                out.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + '\n')
                out.flush(); os.fsync(out.fileno())
            state['last_runtime'] = event
            update()

        while True:
            if time.monotonic() >= limit:
                if last_reason is None:
                    record({'reason_code': 'admission_deadline_expired'})
                raise TimeoutError('native admission unavailable: ' + (last_reason or 'admission_deadline_expired'))
            try:
                # Check local operator pause even if runtime evidence is
                # unavailable; the memory gate otherwise skips its policy.
                if _local_native_target(memory_config) == 0:
                    record({'reason_code': 'upload_admission_paused', 'policy_check': 'before_observation'})
                    raise _RecordedPause('native upload admission is paused')
                def remaining():
                    budget = limit - time.monotonic()
                    if budget <= 0:
                        raise TimeoutError('runtime observation budget exhausted')
                    return budget

                remaining()
                transport = ssh.get_transport()
                old_channel_timeout = transport.channel_timeout
                transport.channel_timeout = min(5.0, remaining())
                sftp = None
                try:
                    sftp = ssh.open_sftp()
                    for name, want in expected_observer.items():
                        sftp.get_channel().settimeout(min(5.0, remaining()))
                        path = monitor_config['observer_script'].rsplit('/', 1)[0] + '/' + name
                        with sftp.open(path, 'rb') as stream:
                            if hashlib.sha256(stream.read(1024 * 1024)).hexdigest() != want:
                                raise ValueError('server observer source identity changed')
                finally:
                    if sftp is not None:
                        sftp.close()
                    transport.channel_timeout = old_channel_timeout
                # Each sample uses a private local cache. The collector may
                # otherwise silently return and leave an earlier cache intact.
                with tempfile.TemporaryDirectory(prefix='rag-observe-', dir=case) as temporary:
                    sample_root = Path(temporary)
                    sample_cache = sample_root / 'private/preextract_delivery/native-runtime-observation.json'
                    sample_cache.parent.mkdir(parents=True)
                    prior = runtime_snapshot['observation']
                    if prior is not None:
                        sample_cache.write_text(json.dumps(prior, ensure_ascii=False), encoding='utf8')
                    before = sample_cache.stat() if sample_cache.exists() else None
                    sample_config = {**monitor_config, 'data_root': str(sample_root), 'output_path': str(sample_cache)}
                    config_file = sample_root / 'observer.json'
                    config_file.write_text(json.dumps(sample_config, ensure_ascii=False), encoding='utf8')
                    monitor.publish_native_runtime(ssh, config_file, password=password, timeout=min(25.0, remaining()))
                    if not sample_cache.is_file():
                        raise ValueError('runtime collector did not create this sample')
                    after = sample_cache.stat()
                    if before is not None and (before.st_dev, before.st_ino) == (after.st_dev, after.st_ino):
                        raise ValueError('runtime collector did not replace this sample')
                    observation = json.loads(sample_cache.read_text(encoding='utf8'))
                runtime_snapshot['observation'] = observation
                runtime = native_runtime_readiness(observation,
                    service=getattr(config, 'native_service', None) or raw.base_url,
                    pipeline_id=config.native_pipeline_id)
                memory_ready = memory.can_admit() if runtime.get('ready') is True else None
                policy_target = _local_native_target(memory_config)
            except Exception as exc:
                if isinstance(exc, _RecordedPause):
                    raise
                if isinstance(exc, TimeoutError) and time.monotonic() >= limit and last_reason is not None:
                    raise TimeoutError('native admission unavailable: ' + last_reason) from exc
                record({'reason_code': 'observer_error', 'error_type': type(exc).__name__})
                raise
            observed = observation if isinstance(observation, dict) else {}
            broker = observed.get('broker')
            broker = broker if isinstance(broker, dict) else {}
            group = broker.get('group')
            group = group if isinstance(group, dict) else {}
            active, pending, lag = observed.get('active_documents'), group.get('pending'), group.get('lag')
            group_known = (isinstance(active, list) and type(pending) is int and pending >= 0
                           and type(lag) is int and lag >= 0)
            if policy_target == 0:
                reason = 'upload_admission_paused'
            elif runtime.get('state') == 'paused':
                reason = 'executor_paused'
            elif runtime.get('ready') is not True or runtime.get('state') != 'ready':
                reason = 'executor_observation_unknown'
            elif memory_ready is False and (memory.last_sample or {}).get('reason') == 'admission_paused':
                reason = 'upload_admission_paused'
            elif memory_ready is not True:
                reason = 'memory_not_admitted'
            elif not group_known:
                reason = 'broker_observation_unknown'
            elif active or pending or lag:
                reason = 'broker_busy'
            else:
                reason = 'admitted'
            mono_now, wall_now = time.monotonic(), time.time()
            memory_sample = memory.last_sample or {}
            memory_at = memory_sample.get('observed_at')
            runtime_until = runtime.get('ready_valid_until')
            valid_times = (type(memory_at) in (int, float) and math.isfinite(memory_at)
                           and 0 <= wall_now - memory_at <= 10
                           and type(runtime_until) in (int, float) and math.isfinite(runtime_until)
                           and runtime_until > wall_now
                           and memory_sample.get('memory_admission_bypassed') is not True)
            expires = mono_now + min(runtime_until - wall_now, memory_at + 10 - wall_now) if valid_times else None
            if reason == 'admitted' and (expires is None or expires <= time.monotonic()):
                reason = 'admission_evidence_expired_or_unavailable'
            if time.monotonic() >= limit:
                reason = 'admission_deadline_expired' if reason == 'admitted' else reason
            sample = {'reason_code': reason, 'executor_state': runtime.get('state'),
                      'executor_reason_code': runtime.get('reason_code'),
                      'observed_at': runtime.get('observed_at'), 'memory_ready': memory_ready,
                      'runtime_valid_until': runtime_until, 'memory_observed_at': memory_at,
                      'valid_until_monotonic': expires,
                      'memory_reason': (memory.last_sample or {}).get('reason'),
                      'available_gib': (memory.last_sample or {}).get('available_gib'),
                      'active_count': len(active) if isinstance(active, list) else None,
                      'broker_pending': pending if type(pending) is int else None,
                      'broker_lag': lag if type(lag) is int else None,
                      'active_type': type(active).__name__, 'pending_type': type(pending).__name__,
                      'lag_type': type(lag).__name__}
            record(sample)
            if reason == 'admitted' and time.monotonic() >= min(expires, limit):
                reason = ('admission_deadline_expired' if time.monotonic() >= limit
                          else 'admission_evidence_expired_or_unavailable')
                record({**sample, 'reason_code': reason})
            last_reason = reason
            if reason == 'admitted':
                return sample
            if reason in ('executor_paused', 'upload_admission_paused'):
                raise InterruptedError('native execution or upload admission is paused; no submission admitted')
            if not wait or time.monotonic() >= limit:
                raise TimeoutError('native admission unavailable: ' + reason)
            now = time.monotonic()
            if last_notified is None or now - last_notified >= 20:
                print('RAG self-test admission waiting: ' + reason, flush=True)
                last_notified = now
            time.sleep(min(health_poll_seconds, max(0, limit - now)))

    def authorize(op, request):
        if op.get('kind') == 'dataset-document':
            return (op.get('parent') in targets and op.get('parent') not in formal
                    and (op.get('name'), request.get('content_sha256')) == upload_expected.get(op.get('parent')))
        endpoint = op.get('endpoint', '')
        if endpoint == '/documents/ingest':
            binding = request.get('binding', {})
            ds = binding.get('dataset_id'); code = targets.get(ds)
            obj = receipts.get(code, {}).get('body', {})
            return (ds not in formal and code is not None and binding.get('document_id') == obj.get('document_id')
                    and binding.get('pipeline_id') == config.native_pipeline_id
                    and all(binding.get(k) == v for k, v in obj.get('identity', {}).items())
                    and bool(obj.get('identity')))
        match = re.fullmatch(r'/datasets/([^/]+)/documents/([^/]+)(?:/chunks/([^/]+))?', endpoint)
        if op.get('method') != 'PATCH' or not match:
            return False
        ds, did, chunk = match.groups(); code = targets.get(ds)
        obj = receipts.get(code, {}).get('body', {})
        if ds in formal or code is None or obj.get('document_id') != did:
            return False
        if chunk:
            return chunk in {x.get('chunk_id') for x in obj.get('chunks', {}).values()}
        return True

    def verify_patch(op, request, _result):
        match = re.fullmatch(r'/datasets/([^/]+)/documents/([^/]+)(?:/chunks/([^/]+))?', op['endpoint'])
        if not match or not authorize(op, request): return False
        ds, did, cid = match.groups(); body = request['body']
        docs = pub.documents(ds, id=did)
        if len(docs) != 1 or docs[0].get('id') != did: return False
        doc = docs[0]
        for key, value in body.items():
            if key == 'meta_fields' and any((doc.get('meta_fields') or {}).get(k) != v for k, v in value.items()): return False
            if key == 'enabled' and str(doc.get('status')) != str(value): return False
            if key in ('pipeline_id', 'name') and doc.get(key) != value: return False
        if cid:
            matches = [x for x in pub.chunks(ds, did) if x.get('id') == cid]
            if len(matches) != 1 or matches[0].get('available') is not body.get('available'): return False
        key = _sha(_json({'operation': op, 'request': request}))
        save(proof_dir / ('sync-' + key + '.json'), {'effect_sha256': key, 'dataset_id': ds,
             'document_id': did, 'readback_verified': True, 'observed_at_ns': time.time_ns()})
        return True

    def fresh(binding):
        ds = binding['dataset_id']; code = targets.get(ds)
        obj = receipts.get(code, {}).get('body', {})
        if (not code or ds in formal or obj.get('document_id') != binding['document_id']
                or state['datasets'][code]['state'] != 'created_and_verified'):
            return False
        if digest(config.policy_path) != config.temporary_policy_sha256:
            raise ValueError('native transport policy changed during the test')
        current = load_native_batch_config(Path(r'E:\uv\ragflow_upstream\data'), raw)
        if (current is None or not current.native_transport_verified
                or current.policy_identity != config.policy_identity
                or current.memory_settings != config.memory_settings
                or current.native_pipeline_id != config.native_pipeline_id):
            raise ValueError('native pipeline or deployment identity changed')
        def check_unstarted():
            docs = pub.documents(ds, id=binding['document_id'])
            if (len(docs) != 1 or docs[0].get('id') != binding['document_id']
                    or docs[0].get('pipeline_id') != config.native_pipeline_id
                    or str(docs[0].get('status')) != '0'
                    or any((docs[0].get('meta_fields') or {}).get(k) != v for k,v in obj['identity'].items())
                    or str(docs[0].get('run')) not in ('0', 'UNSTART')
                    or float(docs[0].get('progress', 0)) != 0
                    or pub.chunks(ds, binding['document_id'])):
                raise ValueError('native test document changed while awaiting admission')
        check_unstarted()
        expected_bytes = pub.document_bytes(task, packet, 'body', obj['identity'])
        if pub._download(ds, binding['document_id']) != expected_bytes:
            raise ValueError('remote test document bytes differ from sealed package')
        check_unstarted()
        # The slow identity and content reads finish before the final health
        # sample. Its expiry is checked again when intent is committed/sent.
        sample = refresh_runtime(wait=False)
        return FreshSubmissionProof.for_binding(binding, sample['valid_until_monotonic'])

    class RecordingNativeClient(NativeIngestClient):
        def ingest(self, *args, **kwargs):
            # This manual coordinator shares an observer and receipt state.
            # It is not a concurrent worker or production scheduler.
            if not selftest_ingest_guard.acquire(blocking=False):
                raise RuntimeError('the isolated self-test is already submitting another document')
            try:
                return super().ingest(*args, **kwargs)
            finally:
                selftest_ingest_guard.release()

        def request(self, method, endpoint, body=None):
            retrieval = method.upper() == 'POST' and endpoint == '/retrieval'
            if retrieval:
                datasets = body.get('dataset_ids') if isinstance(body, dict) else None
                documents = body.get('document_ids') if isinstance(body, dict) else None
                if not isinstance(datasets, list) or len(datasets) != 1 or not isinstance(documents, list) or len(documents) != 1:
                    raise ValueError('retrieval is not restricted to one test document')
                ds, did = datasets[0], documents[0]
                code = targets.get(ds)
                if not code or ds in formal or receipts.get(code, {}).get('body', {}).get('document_id') != did:
                    raise ValueError('retrieval escapes isolated test document')
            result = super().request(method, endpoint, body)
            if retrieval:
                hits = result.get('chunks', []) if isinstance(result, dict) else None
                if not isinstance(hits, list): raise ValueError('retrieval response cannot be recorded')
                evidence = {'at_ns': time.time_ns(), 'dataset_id': ds, 'document_id': did,
                            'question_sha256': hashlib.sha256(body['question'].encode('utf8')).hexdigest(),
                            'hit_ids': [h.get('id', h.get('chunk_id')) for h in hits if isinstance(h, dict)]}
                with (case / 'rag-retrieval.jsonl').open('a', encoding='utf8', newline='\n') as out:
                    out.write(json.dumps(evidence, ensure_ascii=False, sort_keys=True) + '\n')
                    out.flush(); os.fsync(out.fileno())
            return result

    class TrialPublisher(NativeDeliveryPublisher):
        def publish_records(self, task_arg, packet_arg, role, records, obj, all_receipts):
            super().publish_records(task_arg, packet_arg, role, records, obj, all_receipts)
            if role != 'body': raise ValueError('unexpected non-body records in this sample')
            ds, did = obj['dataset_id'], obj['document_id']
            if ds not in targets or ds in formal or obj['native_batch']['state'] != 'verified':
                raise ValueError('native result is not bound to this test')
            body = {'doc_ids': [did], 'run': '1', 'delete': False}
            binding = {**obj['identity'], 'dataset_id': ds, 'document_id': did,
                       'pipeline_id': config.native_pipeline_id}
            operation, request = guarded._native_effect(body, binding)
            effect_id, request_sha = guarded._identity(operation, request, shared=True)
            document_key, fence = guarded._document_fence(did)
            with fence:
                docs = self.documents(ds, id=did); chunks = self.chunks(ds, did)
                expected = {self.render(task_arg, packet_arg, r) for r in records}
                if (len(docs) != 1 or docs[0].get('id') != did
                        or docs[0].get('pipeline_id') != config.native_pipeline_id
                        or str(docs[0].get('status')) != '0'
                        or str(docs[0].get('run')) not in ('3', 'DONE')
                        or float(docs[0].get('progress', 0)) < 1
                        or any((docs[0].get('meta_fields') or {}).get(k) != v for k,v in obj['identity'].items())
                        or len(chunks) != len(expected) or {c.get('content') for c in chunks} != expected
                        or json.loads(receipt_path.read_text(encoding='utf8')) != all_receipts):
                    raise ValueError('native readback or durable receipt does not match')
                by_content = {c['content']: c for c in chunks}
                readback_records = []
                for record in records:
                    text = self.render(task_arg, packet_arg, record)
                    saved = obj['chunks'].get(record[0], {})
                    if (saved.get('chunk_id') != by_content[text].get('id')
                            or saved.get('content_sha256') != hashlib.sha256(text.encode()).hexdigest()):
                        raise ValueError('native record identity differs')
                    readback_records.append({'record_id': record[0],
                        'chunk_id': by_content[text]['id'],
                        'content_sha256': hashlib.sha256(by_content[text]['content'].encode()).hexdigest(),
                        'image_id': by_content[text].get('image_id')})
                snapshot = proof_dir / ('native-receipt-' + ds + '.json')
                if snapshot.exists(): raise ValueError('native receipt snapshot already exists')
                with snapshot.open('x', encoding='utf8', newline='\n') as out:
                    json.dump(all_receipts, out, ensure_ascii=False, sort_keys=True, indent=2)
                    out.flush(); os.fsync(out.fileno())
                if json.loads(snapshot.read_text(encoding='utf8')) != all_receipts:
                    raise ValueError('native receipt snapshot cannot be verified')
                snapshot_sha = digest(snapshot)
                proof = {'case_id': case.name, 'dataset_id': ds, 'document_id': did,
                         'manifest_sha256': packet_arg['manifest_sha256'], 'effect_id': effect_id,
                         'request_sha256': request_sha, 'receipt_snapshot': str(snapshot),
                         'receipt_sha256': snapshot_sha,
                         'records': len(records), 'exact_readback': True, 'at_ns': time.time_ns(),
                         'remote_document': {k: docs[0].get(k) for k in ('id', 'pipeline_id', 'status', 'run', 'progress')},
                         'remote_identity': {k: (docs[0].get('meta_fields') or {}).get(k) for k in obj['identity']},
                         'readback_records': readback_records,
                         'scope': 'manual isolated test only'}
                proof_path = proof_dir / ('native-' + ds + '.json'); save(proof_path, proof)
                with journal._connect() as db:
                    db.execute('BEGIN IMMEDIATE')
                    reservation = db.execute('SELECT * FROM remote_document_reservations WHERE document_key=?', (document_key,)).fetchone()
                    effect = db.execute('SELECT * FROM remote_effects WHERE effect_id=?', (effect_id,)).fetchone()
                    if (not reservation or reservation['state'] != 'active' or reservation['kind'] != 'native'
                            or reservation['effect_id'] != effect_id or reservation['request_sha256'] != request_sha
                            or not effect or effect['state'] != 'returned' or effect['request_sha256'] != request_sha):
                        raise ValueError('native journal reservation is not the verified effect')
                    db.execute('CREATE TABLE IF NOT EXISTS test_native_completion (effect_id TEXT PRIMARY KEY, proof_sha256 TEXT NOT NULL, receipt_sha256 TEXT NOT NULL)')
                    db.execute('INSERT INTO test_native_completion VALUES(?,?,?)', (effect_id, digest(proof_path), snapshot_sha))
                    cur = db.execute("UPDATE remote_document_reservations SET state='released', updated_at_ns=? WHERE document_key=? AND effect_id=? AND request_sha256=? AND state='active' AND kind='native'", (time.time_ns(),document_key,effect_id,request_sha))
                    if cur.rowcount != 1: raise ValueError('native test reservation did not release')
                    db.commit()

    try:
        refresh_runtime()
        existing_names = {d.get('name') for d in existing_datasets}
        for route in packet['routes']:
            code = route['code']
            name = 'PaperFlow SELFTEST ' + case.name + ' ' + code
            if resume:
                item = state['datasets'].get(code, {})
                ds = item.get('id'); rows = [d for d in existing_datasets if d.get('id') == ds]
                if item.get('state') != 'created_and_verified' or item.get('name') != name:
                    raise ValueError('original test dataset receipt differs')
            else:
                if name in existing_names: raise ValueError('test dataset name already exists')
                body = {'name': name, 'permission': 'me', 'chunk_method': 'naive', 'embedding_model': model}
                state['datasets'][code] = {'name': name, 'state': 'create_intent'}; update()
                effect_id = _sha(_json({'case': case.name, 'operation': 'create-test-dataset', 'name': name}))
                req_sha = _sha(_json(body))
                created = journal.perform(effect_id, req_sha, lambda: raw.request('POST', '/datasets', body),
                                          admit(effect_id, req_sha), reuse_returned=False)
                ds = created.get('id') if isinstance(created, dict) else None
                if not isinstance(ds, str) or not ds or ds in by_id:
                    raise ValueError('created dataset identity is invalid')
                state['datasets'][code].update(id=ds, state='created'); update()
                rows = [d for d in raw.datasets() if d.get('id') == ds]
            if (not isinstance(ds, str) or not ds or ds in formal or ds in targets
                    or len(rows) != 1 or rows[0].get('name') != name
                    or rows[0].get('embedding_model') != model
                    or rows[0].get('permission') != 'me'
                    or rows[0].get('tenant_id') != tenant
                    or rows[0].get('created_by') != creator
                    or (not resume and (rows[0].get('document_count') != 0
                                        or rows[0].get('chunk_count') != 0))):
                raise ValueError('test dataset readback differs')
            targets[ds] = code
            state['datasets'][code]['state'] = 'created_and_verified'
            if not resume: update()
            route['paper_dataset_id'] = ds
            identity = {'paperflow_source_sha256': task['source_sha256'],
                        'paperflow_manifest_sha256': packet['manifest_sha256'],
                        'paperflow_revision': str(task['revision']), 'paperflow_specialty': code,
                        'paperflow_role': 'body', 'paperflow_task': task['task_id']}
            name = check_publisher.document_names(task, packet, code, 'body')[0]
            payload = check_publisher.document_bytes(task, packet, 'body', identity)
            upload_expected[ds] = (name, hashlib.sha256(payload).hexdigest())
        guarded = GuardedRagClient(raw, journal, _sha(_json({'case': case.name,
            'manifest': packet['manifest_sha256']})), admit, observe, authorize, verify_patch)
        def wait_for_runtime(_binding, deadline):
            refresh_runtime(wait=True, deadline=deadline)
            return True
        remote = RecordingNativeClient(guarded, task, packet, config.native_pipeline_id, fresh,
                                        wait_before_submission=wait_for_runtime,
                                        require_bound_proof=True)
        pub = TrialPublisher(remote, changed, config)
        if resume:
            if set(receipts) != {route['code'] for route in packet['routes']}:
                raise ValueError('original test receipt route set differs')
            if set(state['datasets']) != set(receipts):
                raise ValueError('original test dataset route set differs')
            records = pub.records(packet)['body']
            pending = []; complete = []
            with journal._connect() as db:
                if db.execute("SELECT count(*) FROM remote_effects WHERE state='intent'").fetchone()[0] != 0:
                    raise ValueError('unresolved journal effects prevent any continuation')
            for route in packet['routes']:
                code, ds = route['code'], route['paper_dataset_id']
                obj = receipts[code].get('body', {}); did = obj.get('document_id')
                identity = {'paperflow_source_sha256': task['source_sha256'],
                            'paperflow_manifest_sha256': packet['manifest_sha256'],
                            'paperflow_revision': str(task['revision']), 'paperflow_specialty': code,
                            'paperflow_role': 'body', 'paperflow_task': task['task_id']}
                if (obj.get('dataset_id') != ds or not did or obj.get('identity') != identity
                        or obj.get('native_batch', {}).get('pipeline_id') != config.native_pipeline_id
                        or obj.get('native_batch', {}).get('record_count') != len(records)
                        or obj.get('native_batch', {}).get('transport_sha256') != upload_expected[ds][1]):
                    raise ValueError('original test document binding differs')
                all_docs = pub.documents(ds)
                docs = [doc for doc in all_docs if doc.get('id') == did]
                if len(all_docs) != 1 or len(docs) != 1:
                    raise ValueError('test dataset document inventory differs')
                doc = docs[0]
                if (str(doc.get('status')) != '0' or doc.get('pipeline_id') != config.native_pipeline_id
                        or any((doc.get('meta_fields') or {}).get(k) != v for k,v in identity.items())):
                    raise ValueError('test document remote identity differs')
                body = {'doc_ids': [did], 'run': '1', 'delete': False}
                binding = {**identity, 'dataset_id': ds, 'document_id': did,
                           'pipeline_id': config.native_pipeline_id}
                operation, request = guarded._native_effect(body, binding)
                effect_id, request_sha = guarded._identity(operation, request, shared=True)
                effect_state = journal.state(effect_id, request_sha)
                chunks = pub.chunks(ds, did)
                native_state = obj['native_batch'].get('state')
                if native_state == 'verified':
                    expected = {pub.render(task, packet, r) for r in records}
                    metadata = doc.get('meta_fields') or {}
                    expected_metadata = {'two_pass_profile_json': compact(packet['profile']),
                                         'two_pass_tree_json': compact(packet['tree']),
                                         'rag_mid_assets_json': compact(packet['assets']),
                                         'paperflow_image_associations_json': compact(receipts[code].get('images', {})),
                                         'paperflow_contract': 'paperflow/1.0'}
                    if (effect_state != 'returned' or str(doc.get('run')) not in ('3','DONE')
                            or float(doc.get('progress', 0)) < 1 or len(chunks) != len(expected)
                            or {x.get('content') for x in chunks} != expected
                            or any(metadata.get(k) != v for k,v in expected_metadata.items())):
                        raise ValueError('already completed test target no longer verifies')
                    proof_path = proof_dir / ('native-' + ds + '.json')
                    proof = json.loads(proof_path.read_text(encoding='utf8'))
                    snapshot = Path(proof['receipt_snapshot']).resolve(strict=True)
                    if (snapshot.parent != proof_dir.resolve(strict=True) or digest(snapshot) != proof['receipt_sha256']
                            or proof.get('effect_id') != effect_id or proof.get('request_sha256') != request_sha
                            or proof.get('document_id') != did or proof.get('dataset_id') != ds):
                        raise ValueError('completed native proof identity differs')
                    snapshot_obj = json.loads(snapshot.read_text(encoding='utf8')).get(code, {}).get('body', {})
                    proof_rows = {row['record_id']: row for row in proof.get('readback_records', [])}
                    remote_rows = {row['content']: row for row in chunks}
                    if (len(proof_rows) != len(records) or set(proof_rows) != {r[0] for r in records}
                            or set(obj.get('chunks', {})) != set(proof_rows)
                            or set(snapshot_obj.get('chunks', {})) != set(proof_rows)):
                        raise ValueError('completed native record inventories differ')
                    for record in records:
                        content = pub.render(task, packet, record)
                        remote_row, proof_row = remote_rows[content], proof_rows[record[0]]
                        expected_sha = hashlib.sha256(content.encode('utf8')).hexdigest()
                        saved = obj['chunks'][record[0]]
                        snapshot_saved = snapshot_obj['chunks'][record[0]]
                        if (remote_row.get('id') != proof_row.get('chunk_id')
                                or remote_row.get('id') != saved.get('chunk_id')
                                or saved != snapshot_saved or proof_row.get('content_sha256') != expected_sha
                                or saved.get('content_sha256') != expected_sha
                                or (remote_row.get('image_id') or None) != (proof_row.get('image_id') or None)
                                or (remote_row.get('image_id') or None) != (saved.get('image_id') or None)):
                            raise ValueError('completed native chunk receipt differs')
                    with journal._connect() as db:
                        saved = db.execute('SELECT * FROM test_native_completion WHERE effect_id=?', (effect_id,)).fetchone()
                    if not saved or saved['proof_sha256'] != digest(proof_path) or saved['receipt_sha256'] != digest(snapshot):
                        raise ValueError('completed native proof is not bound to the journal')
                    complete.append(code)
                elif native_state == 'intent':
                    if (effect_state is not None or str(doc.get('run')) not in ('0','UNSTART')
                            or float(doc.get('progress', 0)) != 0 or chunks or obj.get('chunks')):
                        raise ValueError('prior native intent has an unknown or started outcome')
                    expected_bytes = pub.document_bytes(task, packet, 'body', identity)
                    if pub._download(ds, did) != expected_bytes:
                        raise ValueError('unstarted remote test document bytes differ from package')
                    key, _ = guarded._document_fence(did)
                    journal.assert_document_available(key)
                    pending.append((route, obj, effect_id, request_sha, doc.get('run'), doc.get('progress')))
                else:
                    raise ValueError('native receipt is not a supported continuation state')
            if len(complete) != 1 or len(pending) != 1:
                raise ValueError('known-unsent continuation requires exactly one completed and one unsent target')
            route, obj, effect_id, request_sha, observed_run, observed_progress = pending[0]
            frozen_script = Path(__file__).read_bytes()
            with (case / 'rag-resume-script.py.txt').open('xb') as out:
                out.write(frozen_script); out.flush(); os.fsync(out.fileno())
            evidence = {'case_id': case.name, 'completed_route': complete[0],
                        'unsent_route': route['code'], 'dataset_id': obj['dataset_id'],
                        'document_id': obj['document_id'], 'native_effect_id': effect_id,
                        'request_sha256': request_sha, 'prior_native_effect': None,
                        'remote_run': observed_run, 'remote_progress': observed_progress, 'remote_chunk_count': 0,
                        'original_receipt_sha256': digest(case / 'rag-original-receipts.json'),
                        'original_state_sha256': digest(case / 'rag-original-stop.json'),
                        'receipt_sha256': digest(receipt_path), 'state_sha256': digest(state_path),
                        'resume_script_sha256': hashlib.sha256(frozen_script).hexdigest(),
                        'supporting_module_sha256': {name: digest(HERE / name) for name in
                            ('native_ingest.py', 'guarded_rag_client.py', 'effect_journal.py')},
                        'at_ns': time.time_ns(), 'scope': 'manual continuation of original test identity'}
            with (case / 'rag-resume-preflight.json').open('x', encoding='utf8', newline='\n') as out:
                json.dump(evidence, out, sort_keys=True, indent=2); out.flush(); os.fsync(out.fileno())
            state['previous_stop'] = {'error_type': state.get('error_type'), 'error': state.get('error')}
            state['state'] = 'resuming_known_unsent'; update()
            pub.publish_records(task, packet, 'body', records, obj, receipts)
            ds, did = obj['dataset_id'], obj['document_id']
            docs = pub.documents(ds, id=did)
            if len(docs) != 1: raise ValueError('continued test document missing')
            fields = {**docs[0].get('meta_fields', {}), 'two_pass_profile_json': compact(packet['profile']),
                      'two_pass_tree_json': compact(packet['tree']), 'rag_mid_assets_json': compact(packet['assets']),
                      'paperflow_image_associations_json': compact(receipts[route['code']].get('images', {})),
                      'paperflow_contract': 'paperflow/1.0'}
            endpoint = f'/datasets/{ds}/documents/{did}'
            if guarded.effect_state('PATCH', endpoint, {'meta_fields': fields}) is not None:
                raise ValueError('structured metadata update already has an effect history')
            remote.request('PATCH', endpoint, {'meta_fields': fields})
            changed(receipts)
        else:
            state['state'] = 'publishing'; update()
            pub.publish(task, packet, receipts)
        pub.verify(task, packet, receipts)
        pub.set_enabled(receipts, True)
        pub.verify_retrieval(task, packet, receipts)
        pub.verify(task, packet, receipts)
        state['state'] = 'verified'
        state.pop('error_type', None); state.pop('error', None)
        state['route_count'] = len(receipts)
        state['record_count_per_route'] = {code: len(value['body']['chunks']) for code,value in receipts.items()}
        state['retrieval_verified'] = all(value.get('retrieval_verified') is True for value in receipts.values())
        state['test_reservation_finalizer'] = 'manual isolated proof only, not integrated into new Prefect'
        update()
    except BaseException as exc:
        state['state'] = 'stopped_for_review'
        state['error_type'] = type(exc).__name__
        state['error'] = str(exc)[:500]
        update()
        print(json.dumps(state, ensure_ascii=True), flush=True)
        raise
    finally:
        ssh.close()
    print(json.dumps(state, ensure_ascii=True), flush=True)


if __name__ == '__main__':
    if len(sys.argv) not in (2, 3): raise ValueError('one existing isolated test case is required')
    locked_case = (HERE / 'live-tests' / sys.argv[1]).resolve(strict=True)
    if locked_case.parent != (HERE / 'live-tests').resolve(strict=True):
        raise ValueError('case escapes test root')
    with FileLock(str(locked_case / '.rag-selftest.lock'), timeout=0):
        main()
