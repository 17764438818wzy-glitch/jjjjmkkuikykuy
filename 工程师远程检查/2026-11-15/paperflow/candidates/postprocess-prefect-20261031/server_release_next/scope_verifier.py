"""Read-only scope verification using the existing folder projection helpers.

This never reserves work, starts Prefect, invokes a model or opens a queue.
It validates the source snapshot and original folder identity, not current
worker capacity, complete LLM outputs, or RAG acceptance.
"""
import copy
import importlib.util
import json
from pathlib import Path
from uuid import UUID


def _projection_helpers():
    path = Path(__file__).resolve().parents[1] / 'projection_next' / 'projections.py'
    spec = importlib.util.spec_from_file_location('_paperflow_scope_projection_helpers', path)
    if spec is None or spec.loader is None:
        raise ValueError('original read-only projection unavailable')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _deployment_signature(root, folder):
    """Freeze deployment identity fields; keep no mutable tag-list references."""
    def signature(deployment):
        tags = getattr(deployment, 'tags', None)
        if not isinstance(tags, (list, tuple)) or any(not isinstance(tag, str) for tag in tags):
            raise ValueError('Prefect deployment tags unavailable')
        return (str(deployment.id), str(getattr(deployment, 'flow_id', None)),
                getattr(deployment, 'name', None), tuple(sorted(tags)),
                getattr(deployment, 'work_pool_name', None))
    return signature(root), signature(folder)


class ScopeVerifier:
    """Check a bounded original source/folder scope; incomplete means False."""
    def __init__(self, projector, *, server_identity, check_original_files, projection_helpers=None):
        if not isinstance(server_identity, dict) or not callable(check_original_files):
            raise ValueError('fixed server identity and original file check are required')
        self.projector = projector
        self.server_identity = copy.deepcopy(server_identity)
        self.check_original_files = check_original_files
        self.helpers = projection_helpers if projection_helpers is not None else _projection_helpers()

    def __call__(self, request):
        try:
            return self.verify(request)
        except Exception:
            return False

    def verify_selected(self, request, selected_keys):
        if (not isinstance(selected_keys, set) or len(selected_keys) != 1
                or any(not isinstance(key, tuple) or len(key) != 2
                       or any(not isinstance(value, str) or not value for value in key) for key in selected_keys)):
            raise ValueError('one exact selected folder is required')
        return self.verify(request, selected_keys=selected_keys)

    def verify(self, request, *, selected_keys=None):
        h, p = self.helpers, self.projector
        if (not isinstance(request, dict) or request.get('schema') != 'paperflow-server-projection-scope/1'
                or request.get('server_identity') != self.server_identity
                or Path(request.get('database', '')).resolve(strict=True) != p.control
                or not isinstance(request.get('folders'), list)):
            raise ValueError('source scope does not match the original bindings')
        self.check_original_files()
        source = p.queue.snapshot(max_age_seconds=60)
        def source_ready(value):
            if selected_keys is None:
                return h._source_ready(value)
            return (isinstance(value, dict) and value.get('receiver_status') == 'running'
                    and isinstance(value.get('receiver_session_id'), str) and bool(value['receiver_session_id'])
                    and isinstance(value.get('scan_source_instance'), str) and bool(value['scan_source_instance']))
        def waiting_selected():
            if selected_keys is None:
                return p._waiting()
            value = p.queue.list_waiting_llm()
            if (not isinstance(value, dict) or not isinstance(value.get('items'), list)
                    or type(value.get('waiting_count')) is not int
                    or value['waiting_count'] != len(value['items'])
                    or type(value.get('unavailable_count')) is not int or value['unavailable_count'] < 0):
                raise ValueError('selected handoff is unavailable')
            items = [item for item in value['items'] if h._key(item) in selected_keys]
            return h._unique(items, h._key)

        if not source_ready(source) or p.observer._source_matches(source) is not True:
            raise ValueError('receiver source unavailable')
        guard = h._Guard((p.control, p.local))
        try:
            initial, waiting = p._snapshot(), waiting_selected()
            folders = {}
            for key, row in initial['folders'].items():
                if row['task_id'] in initial['deleted']:
                    continue
                task = initial['tasks'].get(row['task_id'])
                if (not task or task.get('prefect_order_model') != 'v2'
                        or task.get('prefect_run_id') != row['parent_run_id']):
                    raise ValueError('original large ticket differs')
                if row['state'] == 'waiting_postprocessing':
                    folders[key] = row
            expected = [dict(identity={name: row[name] for name in h.FIELDS},
                             manifest_sha256=row['manifest_sha256'], generation=row['generation'])
                        for row in folders.values()]
            expected.sort(key=lambda item: (item['identity']['task_id'], item['identity']['group_id']))
            if expected != request['folders']:
                raise ValueError('original folder scope or generation changed')
            if selected_keys is not None:
                if not selected_keys.issubset(folders):
                    raise ValueError('selected folder is outside the original scope')
                folders = {key: row for key, row in folders.items() if key in selected_keys}
            jobs = {key: row for key, row in initial['jobs'].items() if row['task_id'] not in initial['deleted']
                    and (selected_keys is None or key in selected_keys)}
            claims = {key: row for key, row in initial['claims'].items() if row['task_id'] not in initial['deleted']
                      and (selected_keys is None or key in selected_keys)}
            results = {key: row for key, row in initial['results'].items() if row['task_id'] not in initial['deleted']
                       and (selected_keys is None or key in selected_keys)}
            if set(jobs) - set(folders) or set(claims) - set(jobs) or set(results) - set(jobs):
                raise ValueError('orphan stage or result')
            llm_stages = {row['job_id']: row for row in initial['llm_jobs'].values() if h._key(row) in folders}
            runs, tasks, root, deployment, facade = p._prefect(folders, jobs, llm_stages)
            before = h._prefect_signature(runs, tasks, root, deployment)
            deployments_before = _deployment_signature(root, deployment)
            binding = p.binding_factory(facade)
            tracked = {}
            def track(paths):
                for entry in h._file_basis(paths):
                    if entry[0] in tracked and tracked[entry[0]] != entry:
                        raise ValueError('retry answer changed during source observation')
                    tracked[entry[0]] = entry
            for key, row in folders.items():
                identity = p.runtime._identity({name: row[name] for name in h.FIELDS})
                order, handoff = initial['orders'].get(key), waiting.get(key)
                if (not order or not handoff or any(handoff.get(k) != identity[k]
                        for k in ('task_id', 'group_id', 'paper_id', 'receipt_id'))
                        or any(order.get(k) != identity[k] for k in ('receipt_id', 'attempt_id'))
                        or order.get('prefect_run_id') != identity['task_run_id']):
                    raise ValueError('original retained handoff differs')
                parent, folder = runs.get(identity['parent_run_id']), runs.get(identity['task_run_id'])
                if (not parent or parent.deployment_id != root.id or parent.parent_task_run_id is not None
                        or not parent.state or parent.state.is_final() or parent.state.is_cancelling()
                        or parent.idempotency_key != 'first-step-order-v2-' + identity['task_id']
                        or (parent.parameters or {}).get('task_id') != identity['task_id']
                        or not folder or folder.deployment_id != deployment.id or folder.parent_task_run_id is not None
                        or not getattr(folder, 'state', None)):
                    raise ValueError('original folder identity is unavailable')
                terminal = ((callable(getattr(folder.state, 'is_failed', None)) and folder.state.is_failed())
                            or (callable(getattr(folder.state, 'is_crashed', None)) and folder.state.is_crashed()))
                if (not terminal and (not folder.state.is_paused()
                        or getattr(folder.state.state_details, 'pause_reschedule', None) is not True
                        or not (getattr(folder, 'name', '') or '').endswith('等待后处理'))):
                    raise ValueError('folder stage is not waiting for postprocessing')
                job = jobs.get(key)
                if job:
                    observed_job = tasks.get(job['job_id'])
                    if (observed_job is not None and not p.runtime._task_identity(observed_job, job)):
                        raise ValueError('postprocessing Prefect stage identity changed')
                    if (job['state'] in {'submitted', 'executing', 'finished', 'failed', 'execution_unknown'}
                            and observed_job is None):
                        raise ValueError('postprocessing Prefect stage is unavailable')
                    if (job['state'] == 'finished' and (not observed_job.state or
                            not (observed_job.state.is_completed() or observed_job.state.is_running()))):
                        raise ValueError('finished postprocessing stage is not confirmed')
                related = [stage for stage in llm_stages.values() if h._key(stage) == key]
                manual_retry = any(stage.get('retry_key') for stage in related)
                if related and not manual_retry:
                    stage = max(related, key=lambda item: (item['generation'], item['created_at_ns']))
                    observed = tasks.get(stage['job_id'])
                    if (stage['state'] != 'finished' or stage['generation'] != row['generation']
                            or any(stage.get(name) != identity[name] for name in h.FIELDS if name != 'task_run_id')
                            or stage['folder_run_id'] != identity['task_run_id']
                            or stage['task_key'] != 'paperflow-folder-llm-stage-v1'
                            or stage['job_id'] != h._llm_stage_id(identity['task_run_id'], stage['generation'], None)
                            or not observed or str(observed.id) != stage['job_id']
                            or observed.flow_run_id not in (None, UUID(identity['task_run_id']))
                            or observed.task_key != stage['task_key']
                            or observed.dynamic_key != 'paperflow-llm-' + stage['job_id']
                            or 'paperflow-llm-stage-v1' not in (observed.tags or [])
                            or not observed.state or not observed.state.is_completed()
                            or any(other['job_id'] != stage['job_id'] and other['state'] in {
                                'recovering_folder', 'recovery_unknown', 'reserved', 'submitting', 'submitted',
                                'executing', 'submission_unknown', 'delivery_unknown', 'execution_unknown',
                                'identity_unknown'} for other in related)):
                        raise ValueError('LLM stage has not settled')
                record = {**identity, 'handoff': json.dumps({'manifest_path': handoff['manifest_path'],
                          'manifest_sha256': handoff['manifest_sha256']})}
                manifest = p.observer._llm_handoff(record, row['manifest_sha256'], {}, {})
                verified = binding.load(identity, require_running=False)
                if not isinstance(verified, dict) or verified.get('manifest') != manifest:
                    raise ValueError('frozen source or registered receipt changed')
                retry_sha = verified.get('source_retry_sha256')
                if terminal or manual_retry:
                    if (not manual_retry or not isinstance(retry_sha, str) or len(retry_sha) != 64
                            or not isinstance(verified.get('source_retry_prefect_sha256'), str)
                            or len(verified['source_retry_prefect_sha256']) != 64
                            or (job is not None and (job.get('source_retry_sha256') != retry_sha
                                or job.get('source_retry_prefect_sha256') != verified['source_retry_prefect_sha256']))):
                        raise ValueError('manual retry source contract is unresolved')
                    from paperflow_prefect.llm_queue.folder_completion import inspect_folder
                    admissions = [item for item in initial['admissions'].values()
                                  if item.get('task_id') == identity['task_id']
                                  and item.get('task_run_id') == identity['task_run_id']]
                    raw = inspect_folder(manifest, identity['task_run_id'], admissions,
                                         lambda item: h._retry_response_evidence(item, track))
                    if raw.get('status') != 'raw_complete':
                        raise ValueError('manual retry responses are incomplete')
                elif retry_sha is not None:
                    raise ValueError('unexpected retry source')
            latest_runs, latest_tasks, latest_root, latest_deployment, _ = p._prefect(folders, jobs, llm_stages)
            if (h._prefect_signature(latest_runs, latest_tasks, latest_root, latest_deployment) != before
                    or _deployment_signature(latest_root, latest_deployment) != deployments_before):
                raise ValueError('Prefect source changed during observation')
            latest_source = p.queue.snapshot(max_age_seconds=60)
            if (p._snapshot() != initial or waiting_selected() != waiting or not source_ready(latest_source)
                    or h._source_key(latest_source) != h._source_key(source)
                    or p.observer._source_matches(latest_source) is not True or not guard.unchanged()
                    or (tracked and {entry[0]: entry for entry in h._file_basis(list(tracked))} != tracked)):
                raise ValueError('original control or source changed during observation')
            self.check_original_files()
            return True
        finally:
            guard.close()
