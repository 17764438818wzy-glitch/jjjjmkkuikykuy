"""Bind one durable Prefect flow run to one page task."""
from datetime import datetime, timezone
import json
import time
from uuid import UUID, uuid5
from urllib.parse import quote
from urllib.request import ProxyHandler, Request, build_opener
from .llm_admission_state import postprocess_task_key


class SchedulerNotSubmitted(Exception):
    """No new execution was requested."""


class PrefectScheduler:
    deployment_name = '论文第一步-任务工单/手动运行'

    def __init__(self, api_url):
        self.api_url = api_url.rstrip('/')
        self.opener = build_opener(ProxyHandler({}))

    def _request(self, path, payload=None, timeout=3):
        data = json.dumps(payload, ensure_ascii=False).encode('utf-8') if payload is not None else None
        headers = {'Content-Type': 'application/json'} if data is not None else {}
        request = Request(self.api_url + path, data=data, headers=headers,
                          method='POST' if data is not None else 'GET')
        with self.opener.open(request, timeout=timeout) as response:
            return json.load(response)

    def _deployment(self):
        flow_name, deployment_name = self.deployment_name.split('/', 1)
        path = '/deployments/name/' + quote(flow_name, safe='') + '/' + quote(deployment_name, safe='')
        deployment = self._request(path)
        if 'paperflow-order-v2-live-20261031' not in deployment.get('tags', []):
            raise SchedulerNotSubmitted('同名第一步部署不属于候选')
        return deployment

    @staticmethod
    def _check_json(run, deployment, task_id):
        if (run.get('deployment_id') != deployment.get('id')
                or (run.get('parameters') or {}).get('task_id') != task_id
                or run.get('idempotency_key') != 'first-step-order-v2-' + task_id
                or run.get('parent_task_run_id') is not None):
            raise SchedulerNotSubmitted('Prefect 大工单身份不匹配，不能控制')

    def _client(self, timeout=3):
        from prefect.client.orchestration import SyncPrefectClient
        return SyncPrefectClient(self.api_url, httpx_settings={'timeout': timeout, 'trust_env': False})

    @staticmethod
    def _task_queue(client, task_id):
        from prefect.exceptions import ObjectNotFound
        try:
            pool = client.read_work_pool('paperflow-unified-preprocessing')
        except ObjectNotFound as exc:
            raise SchedulerNotSubmitted('前处理工单工作池不可用') from exc
        if (pool.description != 'paperflow-order-v2-live-20261031' or pool.type != 'process'
                or pool.is_paused):
            raise SchedulerNotSubmitted('前处理工单工作池状态不可核验')
        try:
            return client.read_work_queue_by_name('task-' + task_id,
                                                  work_pool_name='paperflow-unified-preprocessing')
        except ObjectNotFound:
            return None

    def task_queue_status(self, task_id, *, paused):
        """Read whether the task queue currently matches its control intent."""
        task_id = str(UUID(task_id))
        with self._client(timeout=1) as client:
            queue = self._task_queue(client, task_id)
            if queue is None:
                return 'missing'
            return 'matched' if queue.is_paused == paused else 'different'

    def sync_task_queue(self, task_id, *, paused):
        """Update and read back a task queue when a folder order exists."""
        task_id = str(UUID(task_id))
        with self._client() as client:
            queue = self._task_queue(client, task_id)
            if queue is None:
                return 'missing'
            if queue.is_paused != paused:
                client.update_work_queue(queue.id, is_paused=paused)
                queue = self._task_queue(client, task_id)
                if queue is None or queue.is_paused != paused:
                    raise SchedulerNotSubmitted('前处理工单队列尚未同步')
            return 'matched'

    def is_suspended(self, task_id, run_id):
        """Bounded, read-only check after a suspension was submitted."""
        task_id, run_id = str(UUID(task_id)), str(UUID(run_id))
        run = self._request('/flow_runs/' + run_id)
        if (run.get('id') != run_id or run.get('idempotency_key') != 'first-step-order-v2-' + task_id
                or (run.get('parameters') or {}).get('task_id') != task_id
                or run.get('parent_task_run_id') is not None):
            raise SchedulerNotSubmitted('任务大工单身份不匹配')
        state = run.get('state') or {}
        return state.get('type') == 'PAUSED' and (state.get('state_details') or {}).get('pause_reschedule') is True

    @staticmethod
    def _check(run, deployment, task_id):
        expected_key = 'first-step-order-v2-' + task_id
        if ('paperflow-order-v2-live-20261031' not in deployment.tags
                or run.deployment_id != deployment.id or (run.parameters or {}).get('task_id') != task_id
                or run.idempotency_key != expected_key or run.parent_task_run_id is not None):
            raise SchedulerNotSubmitted('Prefect 大工单身份不匹配，不能控制')

    def ensure(self, task_id, name=None):
        """Create the order in a far-future state, without starting its worker."""
        task_id = str(UUID(task_id))
        try:
            deployment = self._deployment()
        except Exception as exc:
            raise SchedulerNotSubmitted('未连接到统一的第一步部署') from exc
        run = self._request('/deployments/' + deployment['id'] + '/create_flow_run', {
            'parameters': {'task_id': task_id},
            'state': {'type': 'SCHEDULED', 'name': '等待开始',
                      'state_details': {'scheduled_time': '2099-01-01T00:00:00Z'}},
            'name': '任务工单 · ' + (str(name or task_id).strip()[:80] or task_id),
            'tags': ['paperflow-task-order-v2', 'task-' + task_id],
            'idempotency_key': 'first-step-order-v2-' + task_id,
        })
        self._check_json(run, deployment, task_id)
        return {'flow_run_id': str(UUID(run['id']))}

    def verify_waiting(self, task_id, existing_run_id):
        """A completed first step waits in a suspended, still-bound order."""
        if not existing_run_id:
            raise SchedulerNotSubmitted('任务尚未绑定大工单')
        task_id = str(UUID(task_id))
        try:
            deployment = self._deployment()
            run = self._request('/flow_runs/' + str(UUID(existing_run_id)))
        except Exception as exc:
            raise SchedulerNotSubmitted('任务大工单不可核验') from exc
        self._check_json(run, deployment, task_id)
        state = run.get('state') or {}
        if state.get('type') not in {'SCHEDULED', 'PENDING', 'RUNNING', 'PAUSED'}:
            raise SchedulerNotSubmitted('任务大工单已结束或状态不可核验')
        return {'flow_run_id': run['id']}

    def start(self, task_id, generation, *, existing_run_id=None, name=None):
        """Schedule or resume only the bound run, without creating a new generation."""
        task_id = str(UUID(task_id))
        if existing_run_id:
            run_id = UUID(existing_run_id)
            result = {'flow_run_id': str(run_id)}
        else:
            result = self.ensure(task_id, name)
            run_id = UUID(result['flow_run_id'])
        try:
            deployment = self._deployment()
            run = self._request('/flow_runs/' + str(run_id))
        except Exception as exc:
            raise SchedulerNotSubmitted('已绑定工单不可核验') from exc
        self._check_json(run, deployment, task_id)
        state = run.get('state') or {}
        kind = state.get('type')
        if kind == 'PAUSED':
            transition = self._request('/flow_runs/' + str(run_id) + '/resume', {'run_input': None})
            if (transition.get('status') != 'ACCEPT' or
                    (transition.get('state') or {}).get('type') not in {'SCHEDULED', 'RUNNING'}):
                raise SchedulerNotSubmitted('工单未能恢复')
        elif kind == 'SCHEDULED' and state.get('name') == '等待开始':
            transition = self._request('/flow_runs/' + str(run_id) + '/set_state', {
                'state': {'type': 'SCHEDULED', 'name': 'Scheduled',
                          'state_details': {'scheduled_time': datetime.now(timezone.utc).isoformat()}},
                'force': False,
            })
            if (transition.get('status') != 'ACCEPT' or
                    (transition.get('state') or {}).get('type') != 'SCHEDULED'):
                raise SchedulerNotSubmitted('工单未能开始')
        elif kind not in {'SCHEDULED', 'PENDING', 'RUNNING'}:
            raise SchedulerNotSubmitted('工单已结束，不能自动新建或重跑')
        return result

    def wait_until_stopped(self, task_id, existing_run_id, timeout=30):
        """Allow an already-stopping classification flow to reach suspension."""
        if not existing_run_id:
            return
        task_id = str(UUID(task_id))
        deadline = time.monotonic() + timeout
        with self._client() as client:
            deployment = client.read_deployment_by_name(self.deployment_name)
            while True:
                run = client.read_flow_run(UUID(existing_run_id))
                self._check(run, deployment, task_id)
                if run.state and (run.state.is_final() or run.state.is_scheduled() or run.state.is_paused()):
                    return
                if time.monotonic() >= deadline:
                    raise SchedulerNotSubmitted('大工单仍在停止中')
                time.sleep(.25)

    def cancel_task(self, task_id, existing_run_id, orders, *, stage_jobs=(), postprocess_jobs=()):
        """Cancel only identity-checked waiting orders belonging to this task."""
        from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId, FlowRunFilterIdempotencyKey
        from prefect.exceptions import ObjectAlreadyExists, ObjectNotFound
        from prefect.states import Cancelled

        task_id = str(UUID(task_id))

        def lookup(client, key):
            matches = client.read_flow_runs(flow_run_filter=FlowRunFilter(
                idempotency_key=FlowRunFilterIdempotencyKey(any_=[key])), limit=2)
            if len(matches) > 1:
                raise SchedulerNotSubmitted('工单身份不唯一，不能删除')
            return matches[0] if matches else None

        def chunks(values, size):
            for pos in range(0, len(values), size):
                yield values[pos:pos + size]

        def read_runs(client, ids):
            result = {}
            for chunk in chunks(ids, 100):
                for run in client.read_flow_runs(flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=chunk)),
                                                 limit=len(chunk)):
                    result[run.id] = run
            return result

        def cancel_all(client, runs):
            if any(run.state is None for run in runs):
                raise SchedulerNotSubmitted('工单状态不可核验')
            waiting = [run.id for run in runs if not run.state.is_final()]
            for chunk in chunks(waiting, 50):
                response = self._request('/flow_runs/bulk_set_state', {
                    'flow_runs': {'id': {'any_': [str(value) for value in chunk]}},
                    'state': {'type': 'CANCELLING'}, 'force': False, 'limit': len(chunk),
                }, timeout=30)
                returned = {str(item.get('flow_run_id')) for item in response.get('results', [])}
                if returned != {str(value) for value in chunk}:
                    raise SchedulerNotSubmitted('部分工单取消请求未确认，请稍后重试')
            deadline = time.monotonic() + 30
            while waiting:
                current = read_runs(client, waiting)
                if len(current) != len(waiting) or any(run.state is None for run in current.values()):
                    raise SchedulerNotSubmitted('部分工单状态不可核验')
                waiting = [run_id for run_id in waiting if not current[run_id].state.is_final()]
                if not waiting:
                    return
                if time.monotonic() >= deadline:
                    raise SchedulerNotSubmitted('部分工单仍在停止，请稍后重试')
                time.sleep(.25)

        # A lost creation response can leave no local run id. Reusing the same
        # server idempotency key resolves that ambiguity before cancellation.
        if not existing_run_id:
            existing_run_id = self.ensure(task_id)['flow_run_id']
        with self._client() as client:
            deployment = client.read_deployment_by_name(self.deployment_name)
            root = client.read_flow_run(UUID(existing_run_id))
            self._check(root, deployment, task_id)
            pool_name = 'paperflow-unified-preprocessing'
            folder_deployment = client.read_deployment_by_name(
                '资料文件夹工单/前处理') if orders else None
            if folder_deployment is not None:
                pool = client.read_work_pool(pool_name)
                marker = 'paperflow-order-v2-live-20261031'
                if (folder_deployment.work_pool_name != pool_name or marker not in folder_deployment.tags
                        or pool.type != 'process' or pool.description != marker):
                    raise SchedulerNotSubmitted('前处理工单工作池状态不可核验')
                queue_name = 'task-' + task_id
                try:
                    queue = client.read_work_queue_by_name(queue_name, work_pool_name=pool_name)
                except ObjectNotFound:
                    try:
                        queue = client.create_work_queue(queue_name, is_paused=True, work_pool_name=pool_name)
                    except ObjectAlreadyExists:
                        queue = client.read_work_queue_by_name(queue_name, work_pool_name=pool_name)
                if not queue.is_paused:
                    client.update_work_queue(queue.id, is_paused=True)
            folders = []
            known_ids = [UUID(item['prefect_run_id']) for item in orders if item['prefect_run_id']]
            known = read_runs(client, known_ids)
            if len(known) != len(set(known_ids)):
                raise SchedulerNotSubmitted('已登记的文件夹工单无法核验')
            for item in orders:
                group_id = item['group_id']
                key = 'folder-waiting-v2-' + task_id + '-' + group_id
                run = (known.get(UUID(item['prefect_run_id'])) if item['prefect_run_id']
                       else lookup(client, key))
                if run is None:
                    # The task queue is already paused and the local tombstone
                    # prevents admission. A late response is reconciled by key.
                    continue
                if (run.idempotency_key != key or run.parent_task_run_id is not None
                        or run.deployment_id != folder_deployment.id
                        or run.work_queue_name != 'task-' + task_id
                        or any((run.parameters or {}).get(k) != item[k]
                               for k in ('task_id', 'group_id', 'receipt_id', 'attempt_id'))):
                    raise SchedulerNotSubmitted('文件夹工单身份不匹配，不能删除')
                folders.append(run)
            stage_runs = []
            order_by_group = {item['group_id']: item for item in orders}
            for job in stage_jobs:
                order = order_by_group.get(job['group_id'])
                if (job['task_id'] != task_id or not order or order['prefect_run_id'] != job['folder_run_id']
                        or order['receipt_id'] != job['receipt_id'] or order['attempt_id'] != job['attempt_id']
                        or job['parent_run_id'] != existing_run_id):
                    raise SchedulerNotSubmitted('LLM 阶段与文件夹工单身份不匹配')
                try:
                    stage = client.read_task_run(UUID(job['job_id']))
                except ObjectNotFound:
                    if job['state'] in {'reserved', 'cancel_fenced_reserved', 'stopped_before_submit'}:
                        continue
                    raise SchedulerNotSubmitted('LLM 阶段创建结果尚未确认，不能删除') from None
                if (stage.flow_run_id is not None or job['task_key'] != 'paperflow-folder-llm-stage-v1'
                        or stage.task_key != job['task_key']
                        or stage.dynamic_key != 'paperflow-llm-' + job['job_id']
                        or 'paperflow-llm-stage-v1' not in (stage.tags or []) or not stage.state):
                    raise SchedulerNotSubmitted('LLM 阶段任务身份不可核验')
                stage_runs.append(stage)
            for job in postprocess_jobs:
                order = order_by_group.get(job['group_id'])
                try:
                    folder_id = UUID(job['folder_run_id'])
                    if str(folder_id) != job['folder_run_id']:
                        raise ValueError('noncanonical folder identity')
                    expected_job = str(uuid5(UUID('5e9b7732-458e-50cb-8916-6b53007c204f'), str(folder_id)))
                except (KeyError, ValueError, TypeError, AttributeError):
                    raise SchedulerNotSubmitted('后处理阶段身份不可核验') from None
                if (job['task_id'] != task_id or not order or order['prefect_run_id'] != job['folder_run_id']
                        or job['task_run_id'] != job['folder_run_id'] or job['job_id'] != expected_job
                        or order['receipt_id'] != job['receipt_id'] or order['attempt_id'] != job['attempt_id']
                        or job['parent_run_id'] != existing_run_id):
                    raise SchedulerNotSubmitted('后处理阶段与文件夹工单身份不匹配')
                try:
                    stage = client.read_task_run(UUID(job['job_id']))
                except ObjectNotFound:
                    if job['state'] in {'reserved', 'cancel_fenced_reserved', 'stopped_before_submit'}:
                        continue
                    raise SchedulerNotSubmitted('后处理阶段创建结果尚未确认，不能删除') from None
                if (str(stage.id) != job['job_id'] or stage.flow_run_id is not None
                        or job['task_key'] != postprocess_task_key(job['job_id'])
                        or stage.task_key != job['task_key']
                        or stage.dynamic_key != 'paperflow-postprocess-' + job['job_id']
                        or 'paperflow-postprocess-stage-v1' not in (stage.tags or []) or not stage.state):
                    raise SchedulerNotSubmitted('后处理阶段任务身份不可核验')
                stage_runs.append(stage)
            # A stage already executing is allowed to drain under ControlStop.
            # Queued stages have already been fenced in the local registry.
            for stage in stage_runs:
                if stage.state.is_scheduled() or stage.state.is_pending():
                    client.set_task_run_state(stage.id, Cancelled(message='所属任务已删除'), force=False)
            deadline = time.monotonic() + 30
            waiting_stage = [stage.id for stage in stage_runs if not stage.state.is_final()]
            while waiting_stage:
                remaining = []
                for stage_id in waiting_stage:
                    observed = client.read_task_run(stage_id)
                    if not observed.state:
                        raise SchedulerNotSubmitted('处理阶段任务状态不可核验')
                    if not observed.state.is_final():
                        remaining.append(stage_id)
                waiting_stage = remaining
                if waiting_stage and time.monotonic() >= deadline:
                    raise SchedulerNotSubmitted('处理阶段任务仍在停止，请稍后重试')
                if waiting_stage:
                    time.sleep(.25)
            # Validate every identity before changing any state. Retries only
            # revisit this task's already-cancelled or still-waiting orders.
            if root.state is None or (not root.state.is_final()
                    and not (root.state.is_scheduled() or root.state.is_paused())):
                raise SchedulerNotSubmitted('大工单仍在停止中，请稍后重试删除')
            cancel_all(client, folders + [root])
