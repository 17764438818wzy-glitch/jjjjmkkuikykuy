"""Read-only scope verification using the existing folder projection helpers.

This never reserves work, starts Prefect, invokes a model or opens a queue.
It validates the source snapshot and original folder identity, not current
worker capacity, complete LLM outputs, or RAG acceptance.
"""
import copy
import importlib.util
import json
from pathlib import Path


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
    def __init__(self, projector, *, server_identity, check_original_files):
        if not isinstance(server_identity, dict) or not callable(check_original_files):
            raise ValueError('fixed server identity and original file check are required')
        self.projector = projector
        self.server_identity = copy.deepcopy(server_identity)
        self.check_original_files = check_original_files
        self.helpers = _projection_helpers()

    def __call__(self, request):
        try:
            return self.verify(request)
        except Exception:
            return False

    def verify(self, request):
        h, p = self.helpers, self.projector
        if (not isinstance(request, dict) or request.get('schema') != 'paperflow-server-projection-scope/1'
                or request.get('server_identity') != self.server_identity
                or Path(request.get('database', '')).resolve(strict=True) != p.control
                or not isinstance(request.get('folders'), list)):
            raise ValueError('source scope does not match the original bindings')
        self.check_original_files()
        source = p.queue.snapshot(max_age_seconds=60)
        if not h._source_ready(source) or p.observer._source_matches(source) is not True:
            raise ValueError('receiver source unavailable')
        guard = h._Guard((p.control, p.local))
        try:
            initial, waiting = p._snapshot(), p._waiting()
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
            jobs = {key: row for key, row in initial['jobs'].items() if row['task_id'] not in initial['deleted']}
            claims = {key: row for key, row in initial['claims'].items() if row['task_id'] not in initial['deleted']}
            results = {key: row for key, row in initial['results'].items() if row['task_id'] not in initial['deleted']}
            if set(jobs) - set(folders) or set(claims) - set(jobs) or set(results) - set(jobs):
                raise ValueError('orphan stage or result')
            llm_stages = {row['job_id']: row for row in initial['llm_jobs'].values() if h._key(row) in folders}
            runs, tasks, root, deployment, facade = p._prefect(folders, jobs, llm_stages)
            before = h._prefect_signature(runs, tasks, root, deployment)
            deployments_before = _deployment_signature(root, deployment)
            binding = p.binding_factory(facade)
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
                        or not getattr(folder, 'state', None) or not folder.state.is_paused()
                        or getattr(folder.state.state_details, 'pause_reschedule', None) is not True
                        or not (getattr(folder, 'name', '') or '').endswith('等待后处理')):
                    # In particular, a completed LLM retry on FAILED/CRASHED is
                    # not currently supported by the actual postprocess stage.
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
                if any(stage.get('retry_key') for stage in related):
                    raise ValueError('manual retry source contract is unresolved')
                if related:
                    stage = max(related, key=lambda item: (item['generation'], item['created_at_ns']))
                    observed = tasks.get(stage['job_id'])
                    if (stage['state'] != 'finished' or stage['generation'] != row['generation']
                            or any(stage.get(name) != identity[name] for name in h.FIELDS if name != 'task_run_id')
                            or stage['folder_run_id'] != identity['task_run_id']
                            or stage['task_key'] != 'paperflow-folder-llm-stage-v1'
                            or stage['job_id'] != h._llm_stage_id(identity['task_run_id'], stage['generation'], None)
                            or not observed or str(observed.id) != stage['job_id']
                            or observed.flow_run_id is not None or observed.task_key != stage['task_key']
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
            latest_runs, latest_tasks, latest_root, latest_deployment, _ = p._prefect(folders, jobs, llm_stages)
            if (h._prefect_signature(latest_runs, latest_tasks, latest_root, latest_deployment) != before
                    or _deployment_signature(latest_root, latest_deployment) != deployments_before):
                raise ValueError('Prefect source changed during observation')
            latest_source = p.queue.snapshot(max_age_seconds=60)
            if (p._snapshot() != initial or p._waiting() != waiting or not h._source_ready(latest_source)
                    or h._source_key(latest_source) != h._source_key(source)
                    or p.observer._source_matches(latest_source) is not True or not guard.unchanged()):
                raise ValueError('original control or source changed during observation')
            self.check_original_files()
            return True
        finally:
            guard.close()
