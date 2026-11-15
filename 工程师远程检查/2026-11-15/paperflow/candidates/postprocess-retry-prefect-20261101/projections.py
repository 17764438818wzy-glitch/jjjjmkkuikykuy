"""Read-only projections for the actual folder and postprocessing ledgers.

No scan, claim, schema creation, Prefect scheduling, LLM call or RAG upload is
performed here. Source and output evidence must stay stable during observation;
when the evidence is incomplete, return unavailable rather than an invented 0.
"""
from contextlib import closing
import copy
from datetime import datetime, timezone
import hashlib
import importlib
import importlib.util
import json
import math
from pathlib import Path
import re
import sqlite3
import sys
from uuid import UUID, uuid5

import psutil


FIELDS = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')
STAGE_COUNTS = ('waiting_postprocessing', 'postprocessing_in_progress', 'waiting_rag_upload',
                'postprocessing_held', 'postprocessing_unresolved')
SOURCE_FIELDS = ('receiver_status', 'receiver_session_id', 'scan_source_instance', 'waiting_available',
                 'upstream_scan_verified', 'waiting_llm_total')
KNOWN_JOBS = {'reserved', 'submitting', 'submitted', 'executing', 'finished', 'failed',
              'submission_unknown', 'execution_unknown', 'identity_unknown', 'delivery_unknown',
              'stopped_before_submit', 'cancel_fenced_reserved', 'cancel_fenced_submitting',
              'cancel_fenced_submitted'}
UNRESOLVED_JOBS = {'reserved', 'submitting', 'submitted', 'submission_unknown', 'execution_unknown',
                   'identity_unknown', 'delivery_unknown', 'stopped_before_submit',
                   'cancel_fenced_reserved', 'cancel_fenced_submitting', 'cancel_fenced_submitted'}


class ProjectionUnavailable(ValueError):
    pass


def _readonly(path):
    db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=3)
    db.row_factory = sqlite3.Row
    return db


class _Guard:
    """Guard control commits and both database identities during observation.

    Queue heartbeat and scan-time writes are frequent; queue business rows are
    compared separately in one read transaction at each end of observation.
    """
    def __init__(self, paths):
        self.sources = []
        try:
            resolved = [Path(path).resolve(strict=True) for path in paths]
            if len(set(resolved)) != len(resolved) or any(not path.is_file() for path in resolved):
                raise ProjectionUnavailable('source database identity unavailable')
            for path in resolved:
                db = _readonly(path)
                self.sources.append((path, db, None, None))
                stat = path.stat()
                version = db.execute('PRAGMA data_version').fetchone()[0]
                self.sources[-1] = (path, db, version, (stat.st_dev, stat.st_ino))
        except Exception:
            self.close()
            raise

    def unchanged(self):
        try:
            for index, (path, db, version, identity) in enumerate(self.sources):
                stat = path.stat()
                if ((stat.st_dev, stat.st_ino) != identity
                        or (index == 0 and db.execute('PRAGMA data_version').fetchone()[0] != version)):
                    return False
            return True
        except (OSError, sqlite3.Error):
            return False

    def changed_sources(self):
        changed = []
        try:
            for index, (path, db, version, identity) in enumerate(self.sources):
                stat = path.stat()
                if ((stat.st_dev, stat.st_ino) != identity
                        or (index == 0 and db.execute('PRAGMA data_version').fetchone()[0] != version)):
                    changed.append('control' if index == 0 else 'queue')
        except (OSError, sqlite3.Error):
            return 'unavailable'
        return '+'.join(changed) or 'unavailable'

    def close(self):
        for _, db, _, _ in self.sources:
            db.close()
        self.sources = []


def _key(row):
    return (row['task_id'], row['group_id'])


def _unique(rows, key):
    result = {}
    for row in rows:
        item = dict(row)
        identity = key(item)
        if identity in result:
            raise ProjectionUnavailable('duplicate folder identity')
        result[identity] = item
    return result


def _source_key(snapshot):
    return tuple(snapshot.get(key) for key in SOURCE_FIELDS)


def _source_ready(snapshot):
    return (isinstance(snapshot, dict) and snapshot.get('receiver_status') == 'running'
            and isinstance(snapshot.get('receiver_session_id'), str) and bool(snapshot['receiver_session_id'])
            and isinstance(snapshot.get('scan_source_instance'), str) and bool(snapshot['scan_source_instance'])
            and snapshot.get('waiting_available') is True and snapshot.get('upstream_scan_verified') is True
            and type(snapshot.get('waiting_llm_total')) is int and snapshot['waiting_llm_total'] >= 0)


def _state_signature(state):
    if state is None:
        return None
    return (str(state.id), state.is_running(), state.is_paused(), state.is_completed(), state.is_final(),
            state.is_cancelling(), getattr(getattr(state, 'state_details', None), 'pause_reschedule', None))


def _prefect_signature(runs, task_runs, root_deployment, folder_deployment):
    return (str(root_deployment.id), str(folder_deployment.id),
            {key: (str(run.id), str(run.deployment_id), str(run.parent_task_run_id), run.idempotency_key,
                   getattr(run, 'work_queue_name', None), getattr(run, 'name', None),
                   copy.deepcopy(run.parameters), _state_signature(run.state)) for key, run in runs.items()},
            {key: (str(run.id), str(run.flow_run_id), run.task_key, run.dynamic_key,
                   tuple(sorted(run.tags or [])), _state_signature(run.state)) for key, run in task_runs.items()})


def process_alive(pid, started):
    """Confirm both PID and process creation time; a reused PID is not evidence."""
    if (type(pid) is not int or pid <= 0 or isinstance(started, bool)
            or not isinstance(started, (int, float)) or not math.isfinite(started) or started <= 0):
        return None
    try:
        process = psutil.Process(pid)
        return (process.is_running() and process.status() != psutil.STATUS_ZOMBIE
                and abs(process.create_time() - started) < 0.01)
    except (psutil.Error, OSError, ValueError):
        return None


def _file_basis(paths):
    basis = []
    for value in sorted(set(paths)):
        path = Path(value)
        resolved = path.resolve(strict=True)
        stat = resolved.stat()
        if not resolved.is_file():
            raise ProjectionUnavailable('tracked file unavailable')
        basis.append((str(path), str(resolved), stat.st_dev, stat.st_ino, stat.st_size,
                      stat.st_mtime_ns, stat.st_ctime_ns))
    return basis


def _retry_response_evidence(row, track):
    """Read a completed retry's existing journals without changing them."""
    work = Path(row['work_dir']).resolve(strict=True)
    model = Path(row['model_dir']).resolve(strict=True)
    root = work / 'model'
    valid_part = (row['kind'] == 'body' and model.parent == root
                  and re.fullmatch(r'part-\d{3,}', model.name) is not None)
    if model != root and not valid_part:
        raise ProjectionUnavailable('retry response directory mismatch')
    request, state_file, response, events = [model / name for name in
        ('request.json', 'request-state.json', 'response.txt', 'response-events.jsonl')]
    paths = [str(request), str(state_file), str(response), str(events)]
    before = _file_basis(paths)
    track(paths)
    state = json.loads(state_file.read_text(encoding='utf8'))
    request_value = json.loads(request.read_text(encoding='utf8'))
    request_hash = hashlib.sha256(json.dumps(request_value, ensure_ascii=False,
                                  separators=(',', ':')).encode('utf8')).hexdigest()
    with response.open('rb') as stream:
        response_hash = hashlib.file_digest(stream, 'sha256').hexdigest()
    if (row['state'] != 'raw_complete' or state.get('status') != 'response_complete'
            or state.get('finish_reason') != 'stop' or state.get('request_sha256') != row['request_sha256']
            or request_hash != row['request_sha256'] or state.get('response_sha256') != row['response_sha256']
            or response_hash != row['response_sha256'] or _file_basis(paths) != before):
        raise ProjectionUnavailable('retry response evidence unavailable')
    track(paths)


def _llm_stage_id(folder_id, generation, retry_key):
    suffix = '' if not retry_key else ':manual:' + str(UUID(retry_key))
    if retry_key and suffix != ':manual:' + retry_key:
        raise ProjectionUnavailable('retry stage key mismatch')
    return str(uuid5(UUID('6aaf059a-c89a-52a8-acd5-4a928260a3b7'),
                     str(UUID(folder_id)) + ':' + str(generation) + suffix))


class _Projected(dict):
    """A JSON-compatible value with a private, non-serialized cache witness."""
    def __init__(self, value, witness):
        super().__init__(value)
        self._cache_witness = witness

    def __deepcopy__(self, memo):
        return _Projected(copy.deepcopy(dict(self), memo), self._cache_witness)


class _CacheWitness:
    def __init__(self, projector, snapshot, initial, folders, jobs, llm_stages, active_jobs,
                 prefect_signature, deliveries, files):
        self.projector = projector
        self.source = _source_key(snapshot)
        self.initial = copy.deepcopy(initial)
        self.folders, self.jobs = copy.deepcopy(folders), copy.deepcopy(jobs)
        self.llm_stages = copy.deepcopy(llm_stages)
        self.active_jobs = copy.deepcopy(active_jobs)
        self.prefect_signature = prefect_signature
        self.deliveries = copy.deepcopy(deliveries)
        self.files = files

    def local(self, snapshot):
        """Check business data, file attributes and live process without full rehash."""
        try:
            if not _source_ready(snapshot) or _source_key(snapshot) != self.source:
                return False
            p = self.projector
            guard = _Guard((p.control, p.local))
            try:
                if p._snapshot() != self.initial:
                    return False
                if _file_basis(item[0] for item in self.files) != self.files:
                    return False
                if any(p.delivery_fingerprint(item, p.observer.receipt_root) != expected
                       for item, expected in self.deliveries):
                    return False
                if any(p.process_alive(job.get('owner_pid'), job.get('owner_started')) is not True
                       for job in self.active_jobs):
                    return False
                return guard.unchanged()
            finally:
                guard.close()
        except Exception:
            return False

    def remote(self):
        """Recheck Prefect in a background thread, then the local evidence."""
        try:
            p = self.projector
            snapshot = p.queue.snapshot(max_age_seconds=60)
            if (not _source_ready(snapshot) or _source_key(snapshot) != self.source
                    or p.observer._source_matches(snapshot) is not True):
                return False
            runs, tasks, root, folder, _ = p._prefect(self.folders, self.jobs, self.llm_stages)
            if _prefect_signature(runs, tasks, root, folder) != self.prefect_signature:
                return False
            return self.local(p.queue.snapshot(max_age_seconds=60))
        except Exception:
            return False


class _PrefetchedClient:
    def __init__(self, api_url, runs, task_runs, deployments):
        self.api_url, self.runs, self.task_runs, self.deployments = api_url, runs, task_runs, deployments

    def read_flow_run(self, run_id):
        value = self.runs.get(str(run_id))
        if value is None:
            raise ProjectionUnavailable('Prefect folder is unavailable')
        return value

    def read_deployment(self, deployment_id):
        value = self.deployments.get(str(deployment_id))
        if value is None:
            raise ProjectionUnavailable('Prefect deployment is unavailable')
        return value

    def read_task_run(self, run_id):
        value = self.task_runs.get(str(run_id))
        if value is None:
            raise ProjectionUnavailable('Prefect retry stage is unavailable')
        return value


class Projections:
    def __init__(self, queue, observer, runtime, stage_store, result_store, *, binding_factory, process_alive,
                 delivery_fingerprint=None):
        self.queue, self.observer, self.runtime = queue, observer, runtime
        self.stage_store, self.result_store = stage_store, result_store
        self.binding_factory, self.process_alive = binding_factory, process_alive
        self.delivery_fingerprint = delivery_fingerprint
        self.control = Path(observer.index).resolve(strict=True)
        self.local = Path(queue.path).resolve(strict=True)
        if (Path(observer.queue.path).resolve(strict=True) != self.local
                or Path(stage_store.database).resolve(strict=True) != self.control
                or Path(result_store.database).resolve(strict=True) != self.control
                or not callable(binding_factory) or not callable(process_alive)):
            raise ProjectionUnavailable('projection sources do not match')

    def _snapshot(self):
        with closing(_readonly(self.control)) as db:
            db.execute('BEGIN')
            rows = {name: [dict(row) for row in db.execute('SELECT * FROM ' + name)] for name in (
                'tasks', 'task_deletions', 'llm_folder_queue', 'paperflow_postprocess_stage_jobs',
                'postprocessing_claims', 'postprocessing_results', 'postprocessing_receipts')}
            # Older installations do not have manual retry tables yet. Their
            # existing folder counts keep using the original paused path.
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            columns = ({row[1] for row in db.execute('PRAGMA table_info(paperflow_llm_stage_jobs)')}
                       if 'paperflow_llm_stage_jobs' in tables else set())
            rows['llm_jobs'] = ([dict(row) for row in db.execute('SELECT * FROM paperflow_llm_stage_jobs')]
                                if 'retry_key' in columns else [])
            rows['retry_items'] = ([dict(row) for row in db.execute('SELECT * FROM paperflow_llm_retry_items')]
                                   if 'paperflow_llm_retry_items' in tables else [])
            rows['admissions'] = ([dict(row) for row in db.execute('SELECT * FROM llm_request_admissions')]
                                  if 'llm_request_admissions' in tables else [])
        with closing(_readonly(self.local)) as db:
            db.execute('BEGIN')
            local_rows = {name: [dict(row) for row in db.execute('SELECT * FROM ' + name)] for name in (
                'groups', 'retention_requests', 'claims', 'preprocessing_handoffs',
                'preprocessing_folder_orders', 'cancelled_tasks')}
            local_rows['metadata'] = [dict(row) for row in db.execute(
                "SELECT key,value FROM metadata WHERE key NOT IN "
                "('receiver_heartbeat_at','scan_observed_at','scan_completed_at')")]
            # Successful retention rechecks refresh only updated_at even when
            # the retained identity, state and checked session are unchanged.
            # Every business field stays in the comparison.
            for row in local_rows['retention_requests']:
                row.pop('updated_at', None)
            # No reliance on SQLite row order. These tables contain text/numeric
            # business records; heartbeat freshness is checked via snapshot().
            local_basis = {name: sorted(json.dumps(row, sort_keys=True, ensure_ascii=False)
                                        for row in values)
                           for name, values in local_rows.items()}
            orders = local_rows['preprocessing_folder_orders']
        return dict(tasks=_unique(rows['tasks'], lambda item: item['task_id']),
            deleted={item['task_id'] for item in rows['task_deletions']},
            folders=_unique(rows['llm_folder_queue'], _key),
            jobs=_unique(rows['paperflow_postprocess_stage_jobs'], _key),
            claims=_unique(rows['postprocessing_claims'], _key),
            results=_unique(rows['postprocessing_results'], _key),
            receipts=_unique(rows['postprocessing_receipts'], lambda item: item['claim_id']),
            llm_jobs=_unique(rows['llm_jobs'], lambda item: item['job_id']),
            retry_items=_unique(rows['retry_items'], lambda item: (item['request_id'], item['task_id'], item['group_id'])),
            admissions=_unique(rows['admissions'], lambda item: (item['task_id'], item['component_id'])),
            orders=_unique(orders, _key), groups=_unique(local_rows['groups'], _key), local_basis=local_basis)

    def _waiting(self):
        value = self.queue.list_waiting_llm()
        if (not isinstance(value, dict) or type(value.get('unavailable_count')) is not int
                or value['unavailable_count'] != 0
                or not isinstance(value.get('items'), list)
                or type(value.get('waiting_count')) is not int
                or value['waiting_count'] != len(value['items'])):
            raise ProjectionUnavailable('retained handoffs are unavailable')
        return _unique(value['items'], _key)

    def _prefect(self, folders, jobs, llm_stages):
        from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId, TaskRunFilter, TaskRunFilterId
        with self.observer._client() as client:
            if str(getattr(client, 'api_url', '')).rstrip('/') != self.observer.api_url.rstrip('/'):
                raise ProjectionUnavailable('Prefect service identity changed')
            folder_deployment = self.observer._safe_deployment(client)
            root_deployment = client.read_deployment_by_name('论文第一步-任务工单/手动运行')
            if 'paperflow-order-v2-live-20261031' not in (root_deployment.tags or []):
                raise ProjectionUnavailable('Prefect root deployment unavailable')
            flow_ids = {UUID(row[field]) for row in folders.values() for field in ('parent_run_id', 'task_run_id')}
            runs = {}
            ids = list(flow_ids)
            for offset in range(0, len(ids), 100):
                chunk = ids[offset:offset + 100]
                for run in client.read_flow_runs(flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=chunk)),
                                                 limit=len(chunk)):
                    if str(run.id) in runs:
                        raise ProjectionUnavailable('duplicate Prefect folder')
                    runs[str(run.id)] = run
            task_ids = list({UUID(job['job_id']) for job in list(jobs.values()) + list(llm_stages.values())})
            stages = {}
            for offset in range(0, len(task_ids), 100):
                chunk = task_ids[offset:offset + 100]
                for run in client.read_task_runs(task_run_filter=TaskRunFilter(id=TaskRunFilterId(any_=chunk)),
                                                 limit=len(chunk)):
                    if str(run.id) in stages:
                        raise ProjectionUnavailable('duplicate Prefect stage')
                    stages[str(run.id)] = run
            facade = _PrefetchedClient(str(client.api_url), runs, stages,
                {str(root_deployment.id): root_deployment, str(folder_deployment.id): folder_deployment})
            return runs, stages, root_deployment, folder_deployment, facade

    def _receipt(self, identity, folder, claim, result, receipts):
        if (not claim or not result or result.get('state') != 'passed'
                or any(claim.get(k) != identity[k] or result.get(k) != identity[k] for k in FIELDS)
                or claim.get('manifest_sha256') != folder['manifest_sha256']
                or result.get('manifest_sha256') != folder['manifest_sha256']
                or result.get('claim_id') != claim.get('claim_id')):
            raise ProjectionUnavailable('stage/result identity mismatch')
        saved = receipts.get(claim['claim_id'])
        if not saved or not isinstance(saved.get('receipt_json'), str):
            raise ProjectionUnavailable('accepted receipt unavailable')
        digest = hashlib.sha256(saved['receipt_json'].encode('utf8')).hexdigest()
        if saved.get('receipt_sha256') != digest or result.get('stage_receipt_sha256') != digest:
            raise ProjectionUnavailable('accepted receipt hash mismatch')
        receipt = json.loads(saved['receipt_json'])
        if receipt.get('manifest_sha256') != folder['manifest_sha256']:
            raise ProjectionUnavailable('accepted receipt source mismatch')
        self.stage_store._inspect_receipt(receipt, identity, claim['claim_id'])

    def _observe(self, snapshot, query=None):
        if not _source_ready(snapshot):
            raise ProjectionUnavailable('receiver or registered source unavailable')
        guard = _Guard((self.control, self.local))
        try:
            if self.observer._source_matches(snapshot) is not True:
                raise ProjectionUnavailable('registered source unavailable')
            initial = self._snapshot()
            waiting = self._waiting()
            folders = {}
            for key, row in initial['folders'].items():
                if row['task_id'] in initial['deleted']:
                    continue
                task = initial['tasks'].get(row['task_id'])
                if not task or task.get('prefect_order_model') != 'v2' or task.get('prefect_run_id') != row['parent_run_id']:
                    raise ProjectionUnavailable('root order identity mismatch')
                if row['state'] == 'waiting_postprocessing':
                    folders[key] = row
            jobs = {key: row for key, row in initial['jobs'].items() if row['task_id'] not in initial['deleted']}
            claims = {key: row for key, row in initial['claims'].items() if row['task_id'] not in initial['deleted']}
            results = {key: row for key, row in initial['results'].items() if row['task_id'] not in initial['deleted']}
            if (set(jobs) - set(folders) or set(claims) - set(jobs) or set(results) - set(jobs)):
                raise ProjectionUnavailable('orphan stage or result')
            retry_jobs, retry_conflicts, llm_stages = {}, set(), {}
            for item in initial['llm_jobs'].values():
                key = _key(item)
                if key in folders:
                    llm_stages[item['job_id']] = item
                if key in folders and item.get('retry_key') and item.get('state') == 'finished':
                    if key in retry_jobs:
                        retry_conflicts.add(key)
                    retry_jobs[key] = item
            runs, task_runs, root_deployment, folder_deployment, facade = self._prefect(folders, jobs, llm_stages)
            prefect_before = _prefect_signature(runs, task_runs, root_deployment, folder_deployment)
            binding = self.binding_factory(facade)
            counts = dict.fromkeys(STAGE_COUNTS, 0)
            counts['waiting_postprocessing_partial'] = False
            active_jobs = []
            deliveries, tracked = [], {}
            cacheable = callable(self.delivery_fingerprint)
            def track(paths):
                if not cacheable:
                    return
                for entry in _file_basis(paths):
                    if entry[0] in tracked and tracked[entry[0]] != entry:
                        raise ProjectionUnavailable('tracked file changed during verification')
                    tracked[entry[0]] = entry
            for key, row in folders.items():
                identity = self.runtime._identity({name: row[name] for name in FIELDS})
                order, handoff = initial['orders'].get(key), waiting.get(key)
                if (not order or not handoff or any(handoff.get(k) != identity[k]
                        for k in ('task_id', 'group_id', 'paper_id', 'receipt_id'))
                        or any(order.get(k) != identity[k] for k in ('receipt_id', 'attempt_id'))
                        or order.get('prefect_run_id') != identity['task_run_id']):
                    raise ProjectionUnavailable('retained handoff/order identity mismatch')
                record = {**identity, 'handoff': json.dumps({'manifest_path': handoff['manifest_path'],
                           'manifest_sha256': handoff['manifest_sha256']})}
                manifest = self.observer._llm_handoff(record, row['manifest_sha256'], {}, {})
                paths = [handoff['manifest_path']]
                for name in ('receipt', 'material', 'reading_material', 'body_text', 'index'):
                    reference = manifest.get(name)
                    if isinstance(reference, dict) and isinstance(reference.get('path'), str):
                        paths.append(reference['path'])
                track(paths)
                if cacheable:
                    group = initial['groups'].get(key)
                    if not group or not isinstance(group.get('record'), str):
                        raise ProjectionUnavailable('delivery fingerprint source unavailable')
                    delivery = json.loads(group['record'])
                    before = self.delivery_fingerprint(delivery, self.observer.receipt_root)
                parent, folder = runs.get(identity['parent_run_id']), runs.get(identity['task_run_id'])
                terminal_retry = bool(folder and folder.state and
                                      (folder.state.is_failed() or folder.state.is_crashed()))
                if terminal_retry:
                    try:
                        retry = retry_jobs.get(key)
                        record = (initial['retry_items'].get((retry['retry_key'], *key)) if retry else None)
                        if (key in retry_conflicts or not retry or not record or record.get('job_id') != retry['job_id']
                                or record.get('state') != 'submitted' or retry['generation'] != row['generation']
                                or any(retry.get(name) != identity[name] for name in FIELDS if name != 'task_run_id')
                                or retry.get('folder_run_id') != identity['task_run_id']
                                or key in jobs or key in claims or key in results
                                or any(other['job_id'] != retry['job_id'] and _key(other) == key
                                       and other['state'] in {'recovering_folder','recovery_unknown','reserved',
                                           'submitting','submitted','executing','submission_unknown','delivery_unknown',
                                           'execution_unknown','identity_unknown'}
                                       for other in initial['llm_jobs'].values())):
                            raise ProjectionUnavailable('completed manual retry is not uniquely bound')
                        retry_binding = self.binding_factory(facade, retry)
                        verified = retry_binding.load(identity, require_running=False)
                        completion = importlib.import_module(type(retry_binding).__module__.rsplit('.', 1)[0]
                                                             + '.folder_completion')
                        admissions = [item for item in initial['admissions'].values()
                                      if item['task_id'] == identity['task_id']
                                      and item['task_run_id'] == identity['task_run_id']]
                        raw = completion.inspect_folder(manifest, identity['task_run_id'], admissions,
                                                         lambda item: _retry_response_evidence(item, track))
                        if raw['status'] != 'raw_complete':
                            raise ProjectionUnavailable('retry folder responses are incomplete')
                    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, AttributeError, ImportError):
                        # Keep already-verified folders visible as a lower
                        # bound; this folder is not counted until its own
                        # stage and raw responses can be verified.
                        counts['waiting_postprocessing_partial'] = True
                        counts['postprocessing_unresolved'] += 1
                        continue
                else:
                    related = [item for item in llm_stages.values() if _key(item) == key]
                    if folder and folder.state and folder.state.is_paused():
                        try:
                            if (folder.state.state_details.pause_reschedule is not True
                                    or not (folder.name or '').endswith('等待后处理')):
                                raise ProjectionUnavailable('folder handoff is still settling')
                            if related:
                                stage = max(related, key=lambda item: (item['generation'], item['created_at_ns']))
                                observed = task_runs.get(stage['job_id'])
                                if (stage['state'] != 'finished' or stage['generation'] != row['generation']
                                        or any(stage.get(name) != identity[name] for name in FIELDS if name != 'task_run_id')
                                        or stage['folder_run_id'] != identity['task_run_id']
                                        or stage['task_key'] != 'paperflow-folder-llm-stage-v1'
                                        or stage['job_id'] != _llm_stage_id(identity['task_run_id'], stage['generation'], stage['retry_key'])
                                        or not observed or str(observed.id) != stage['job_id']
                                        or observed.flow_run_id not in (None, UUID(identity['task_run_id']))
                                        or observed.task_key != stage['task_key']
                                        or observed.dynamic_key != 'paperflow-llm-' + stage['job_id']
                                        or 'paperflow-llm-stage-v1' not in (observed.tags or [])
                                        or not observed.state or not observed.state.is_completed()):
                                    raise ProjectionUnavailable('LLM stage is still settling')
                                if any(item['job_id'] != stage['job_id'] and item['state'] in {
                                        'recovering_folder','recovery_unknown','reserved','submitting','submitted',
                                        'executing','submission_unknown','delivery_unknown','execution_unknown',
                                        'identity_unknown'} for item in related):
                                    raise ProjectionUnavailable('another LLM stage is unresolved')
                                paused_binding = self.binding_factory(facade, stage) if stage['retry_key'] else binding
                                verified = paused_binding.load(identity, require_running=False)
                            else:
                                verified = binding.load(identity, require_running=False)
                        except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, AttributeError, ImportError):
                            counts['waiting_postprocessing_partial'] = True
                            counts['postprocessing_unresolved'] += 1
                            continue
                    else:
                        verified = binding.load(identity, require_running=False)
                if not isinstance(verified, dict) or verified.get('manifest') != manifest:
                    raise ProjectionUnavailable('frozen material is unavailable')
                if cacheable:
                    if self.delivery_fingerprint(delivery, self.observer.receipt_root) != before:
                        raise ProjectionUnavailable('delivery changed during verification')
                    track(paths)
                    deliveries.append((delivery, before))
                if (not parent or parent.deployment_id != root_deployment.id or parent.parent_task_run_id is not None
                        or not parent.state or parent.state.is_final() or parent.state.is_cancelling()
                        or parent.idempotency_key != 'first-step-order-v2-' + identity['task_id']
                        or (parent.parameters or {}).get('task_id') != identity['task_id']
                        or not folder or folder.deployment_id != folder_deployment.id or folder.parent_task_run_id is not None
                        or not folder.state or (not terminal_retry and (not folder.state.is_paused()
                            or folder.state.state_details.pause_reschedule is not True
                            or not (folder.name or '').endswith('等待后处理')))
                        or folder.idempotency_key != 'folder-waiting-v2-' + identity['task_id'] + '-' + identity['group_id']
                        or folder.work_queue_name != 'task-' + identity['task_id']
                        or any((folder.parameters or {}).get(k) != identity[k]
                               for k in ('task_id', 'group_id', 'receipt_id', 'attempt_id'))):
                    raise ProjectionUnavailable('Prefect folder identity unavailable')
                job, claim, outcome = jobs.get(key), claims.get(key), results.get(key)
                if not job:
                    if claim or outcome:
                        raise ProjectionUnavailable('orphan processing result')
                    counts['waiting_postprocessing'] += 1
                    continue
                if (job['state'] not in KNOWN_JOBS or any(job.get(k) != identity[k] for k in FIELDS)
                        or job.get('folder_run_id') != identity['task_run_id']
                        or type(job.get('source_llm_generation')) is not int
                        or type(job.get('generation')) is not int
                        or type(row.get('generation')) is not int
                        or job['source_llm_generation'] != row['generation']
                        or not 1 <= job['source_llm_generation'] <= job['generation']
                        or job.get('manifest_sha256') != row['manifest_sha256']
                        or job['task_key'] != self.runtime.task_key_for_job(job['job_id'])):
                    raise ProjectionUnavailable('stage reservation identity mismatch')
                for record in (claim, outcome):
                    if record and (any(record.get(k) != identity[k] for k in FIELDS)
                                   or record.get('manifest_sha256') != row['manifest_sha256']):
                        raise ProjectionUnavailable('stage/result identity mismatch')
                if job['state'] in {'reserved', 'submitting', 'submitted', 'submission_unknown',
                                    'stopped_before_submit', 'cancel_fenced_reserved',
                                    'cancel_fenced_submitting', 'cancel_fenced_submitted'} and (claim or outcome):
                    raise ProjectionUnavailable('unstarted stage already has processing evidence')
                if outcome and outcome.get('state') == 'passed' and job['state'] != 'finished':
                    raise ProjectionUnavailable('passed result stage is not finalized')
                observed = task_runs.get(job['job_id'])
                if observed is not None and not self.runtime._task_identity(observed, job):
                    raise ProjectionUnavailable('Prefect stage identity mismatch')
                if job['state'] in {'submitted', 'executing', 'finished', 'failed', 'execution_unknown'} and observed is None:
                    raise ProjectionUnavailable('Prefect stage unavailable')
                if job['state'] in UNRESOLVED_JOBS:
                    # A reservation or uncertain/cancel-fenced stage is not
                    # evidence that the processing worker is idle.
                    counts['postprocessing_in_progress'] = None
                    counts['postprocessing_unresolved'] += 1
                    continue
                if job['state'] == 'executing':
                    active = (observed is not None and observed.state is not None and observed.state.is_running()
                              and self.process_alive(job.get('owner_pid'), job.get('owner_started')) is True)
                    if active:
                        if counts['postprocessing_in_progress'] is not None:
                            counts['postprocessing_in_progress'] += 1
                        active_jobs.append(job)
                    else:
                        counts['postprocessing_in_progress'] = None
                        counts['postprocessing_unresolved'] += 1
                    continue
                if job['state'] == 'failed':
                    if not outcome or outcome.get('state') != 'failed':
                        counts['postprocessing_unresolved'] += 1
                    continue
                if (not observed or not observed.state or
                        not (observed.state.is_completed() or observed.state.is_running())):
                    raise ProjectionUnavailable('completed Prefect stage unavailable')
                if cacheable and claim:
                    saved = initial['receipts'].get(claim['claim_id'])
                    if saved and isinstance(saved.get('receipt_json'), str):
                        saved_receipt = json.loads(saved['receipt_json'])
                        track([item['path'] for item in saved_receipt.get('artifacts', [])])
                self._receipt(identity, row, claim, outcome, initial['receipts'])
                if claim['state'] == 'waiting_rag':
                    counts['waiting_rag_upload'] += 1
                elif claim['state'] == 'receipt_held':
                    counts['postprocessing_held'] += 1
                else:
                    raise ProjectionUnavailable('passed result is not an accepted receipt')
            track([result['proof_path'] for result in results.values() if result.get('proof_path')])
            args = query or (None, 1, 20)
            outcome = self.result_store.summary(*args)
            if not isinstance(outcome, dict) or outcome.get('available') is not True:
                raise ProjectionUnavailable('result evidence unavailable')
            original_outcome = copy.deepcopy(outcome)
            if query is not None and query[0] is not None:
                listing = outcome.get('list')
                if (not isinstance(listing, dict) or listing.get('filter') != query[0]
                        or listing.get('page') != query[1] or listing.get('page_size') != query[2]
                        or not isinstance(listing.get('items'), list)):
                    raise ProjectionUnavailable('result page identity mismatch')
                self.observer._add_result_titles(listing['items'])
            latest_runs, latest_tasks, latest_root, latest_deployment, _ = self._prefect(folders, jobs, llm_stages)
            if _prefect_signature(latest_runs, latest_tasks, latest_root, latest_deployment) != prefect_before:
                raise ProjectionUnavailable('Prefect states changed during observation')
            latest_outcome = self.result_store.summary(*args)
            if (not isinstance(latest_outcome, dict) or latest_outcome.get('available') is not True
                    or {k: v for k, v in latest_outcome.items() if k != 'as_of'} !=
                       {k: v for k, v in original_outcome.items() if k != 'as_of'}):
                raise ProjectionUnavailable('result evidence changed after Prefect observation')
            for job in active_jobs:
                if self.process_alive(job.get('owner_pid'), job.get('owner_started')) is not True:
                    counts['postprocessing_in_progress'] = None
                    counts['postprocessing_unresolved'] += 1
            latest = self.queue.snapshot(max_age_seconds=60)
            if self._snapshot() != initial:
                raise ProjectionUnavailable('ledger snapshot changed')
            if self._waiting() != waiting:
                raise ProjectionUnavailable('handoff snapshot changed')
            if not _source_ready(latest) or _source_key(latest) != _source_key(snapshot):
                raise ProjectionUnavailable('receiver snapshot changed')
            if self.observer._source_matches(snapshot) is not True:
                raise ProjectionUnavailable('registered source changed')
            if cacheable:
                track(list(tracked))
                if any(self.delivery_fingerprint(item, self.observer.receipt_root) != expected
                       for item, expected in deliveries):
                    raise ProjectionUnavailable('delivery changed during observation')
            if not guard.unchanged():
                raise ProjectionUnavailable('database version changed ' + guard.changed_sources())
            witness = (_CacheWitness(self, snapshot, initial, folders, jobs, llm_stages, active_jobs, prefect_before,
                                     deliveries, sorted(tracked.values())) if cacheable else None)
            return counts, outcome, witness
        finally:
            guard.close()

    def stage_projection(self, snapshot):
        try:
            counts, _, witness = self._observe(snapshot)
            value = dict(available=True, basis='folder-stage-receipts/1', counts=counts,
                         as_of=datetime.now(timezone.utc).isoformat())
            return _Projected(value, witness) if witness else value
        except Exception:
            return dict(available=False, counts=dict.fromkeys(STAGE_COUNTS))

    def results_projection(self, result_filter=None, page=1, page_size=20):
        try:
            _, result, witness = self._observe(self.queue.snapshot(max_age_seconds=60), (result_filter, page, page_size))
            return _Projected(result, witness) if witness else result
        except Exception:
            return self.result_store._unavailable(result_filter, page, page_size)


def _load_module(name, path, *, package_dir=None):
    existing = sys.modules.get(name)
    if existing is not None:
        if Path(existing.__file__).resolve() != Path(path).resolve():
            raise ProjectionUnavailable('read-only module source changed')
        return existing
    options = {'submodule_search_locations': [str(package_dir)]} if package_dir is not None else {}
    spec = importlib.util.spec_from_file_location(name, path, **options)
    if spec is None or spec.loader is None:
        raise ProjectionUnavailable('read-only module unavailable')
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return module


def build_projections(queue, observer):
    """Bind existing read-only business modules without starting work or writing schemas."""
    root = Path(__file__).resolve().parents[1]
    workspace = root.parents[1]
    package = (workspace / 'prefect-paperflow' / 'paperflow_prefect').resolve(strict=True)
    _load_module('_paperflow_postprocess_readonly', package / '__init__.py', package_dir=package)
    binding_module = importlib.import_module('_paperflow_postprocess_readonly.llm_queue.upstream_flow_binding')
    registry_module = importlib.import_module('_paperflow_postprocess_readonly.attachment_queue')
    runtime = _load_module('_paperflow_postprocess_runtime_readonly', root / 'runtime_next' / 'stage_runtime.py')
    stages = _load_module('_paperflow_postprocess_stage_readonly', root / 'folder_state.py')
    results = _load_module('_paperflow_postprocess_results_readonly', root / 'results_next' / 'postprocessing_results.py')
    receiver = importlib.import_module(type(queue).__module__)
    receiver_session = getattr(receiver, '_current_receiver_session', None)
    delivery_fingerprint = getattr(receiver, '_delivery_fingerprint', None)
    if not callable(receiver_session) or not callable(delivery_fingerprint):
        raise ProjectionUnavailable('receiver session verifier unavailable')
    registry = registry_module.AttachmentQueue(queue.path, observer.receipt_root,
                                                retention_consumer=queue.consumer_id)

    def verify_receipt(path, receipt_id):
        record = registry.verified(receipt_id, require_attachments=False)
        if Path(record.get('receipt_path', '')).resolve() != Path(path).resolve():
            raise ProjectionUnavailable('registered receipt path changed')
        return record

    def binding(client, retry=None):
        return binding_module.UpstreamFlowBinding(queue.path, observer.index, client, observer.api_url,
            queue.consumer_id, verify_receipt, receiver_session=receiver_session,
            execution_task_run_id=retry['job_id'] if retry else None,
            execution_task_key=retry['task_key'] if retry else None,
            allow_terminal_retry=bool(retry), allow_completed_retry=bool(retry))

    stage_store = stages.FolderStageStore(observer.index)
    result_store = results.ResultsStore(observer.index, admission_gate=None,
                                        inspect_receipt=stages.FolderStageStore._inspect_receipt)
    projection = Projections(queue, observer, runtime, stage_store, result_store,
                             binding_factory=binding, process_alive=process_alive,
                             delivery_fingerprint=delivery_fingerprint)
    return projection.stage_projection, projection.results_projection
