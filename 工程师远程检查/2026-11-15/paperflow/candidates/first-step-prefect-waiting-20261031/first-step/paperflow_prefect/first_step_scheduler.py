"""Bind one durable Prefect flow run to one page task."""
from datetime import datetime, timezone
import json
import time
from uuid import NAMESPACE_URL, UUID, uuid5
from urllib.parse import quote
from urllib.request import ProxyHandler, Request, build_opener


class SchedulerNotSubmitted(Exception):
    """No new execution was requested."""


class PrefectScheduler:
    deployment_name = '论文第一步-任务工单候选/手动运行'

    def __init__(self, api_url):
        self.api_url = api_url.rstrip('/')
        self.opener = build_opener(ProxyHandler({}))

    def _request(self, path, payload=None):
        data = json.dumps(payload, ensure_ascii=False).encode('utf-8') if payload is not None else None
        headers = {'Content-Type': 'application/json'} if data is not None else {}
        request = Request(self.api_url + path, data=data, headers=headers,
                          method='POST' if data is not None else 'GET')
        with self.opener.open(request, timeout=3) as response:
            return json.load(response)

    def _deployment(self):
        flow_name, deployment_name = self.deployment_name.split('/', 1)
        path = '/deployments/name/' + quote(flow_name, safe='') + '/' + quote(deployment_name, safe='')
        deployment = self._request(path)
        if 'paperflow-order-v2-candidate-20261031' not in deployment.get('tags', []):
            raise SchedulerNotSubmitted('同名第一步部署不属于候选')
        return deployment

    @staticmethod
    def _check_json(run, deployment, task_id):
        if (run.get('deployment_id') != deployment.get('id')
                or (run.get('parameters') or {}).get('task_id') != task_id
                or run.get('idempotency_key') != 'first-step-order-v2-' + task_id
                or run.get('parent_task_run_id') is not None):
            raise SchedulerNotSubmitted('Prefect 大工单身份不匹配，不能控制')

    def _client(self):
        from prefect.client.orchestration import SyncPrefectClient
        return SyncPrefectClient(self.api_url, httpx_settings={'timeout': 3, 'trust_env': False})

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
        if ('paperflow-order-v2-candidate-20261031' not in deployment.tags
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

    def cancel_task(self, task_id, existing_run_id, orders):
        """Cancel only identity-checked waiting orders belonging to this task."""
        from prefect import task
        from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterIdempotencyKey
        from prefect.exceptions import ObjectAlreadyExists, ObjectNotFound
        from prefect.states import Cancelling, Pending

        task_id = str(UUID(task_id))

        def lookup(client, key):
            matches = client.read_flow_runs(flow_run_filter=FlowRunFilter(
                idempotency_key=FlowRunFilterIdempotencyKey(any_=[key])), limit=2)
            if len(matches) > 1:
                raise SchedulerNotSubmitted('工单身份不唯一，不能删除')
            return matches[0] if matches else None

        def cancel_waiting(client, run):
            state = run.state
            if state is None:
                raise SchedulerNotSubmitted('工单状态不可核验')
            if state.is_final():
                return
            if not state.is_cancelling():
                client.set_flow_run_state(run.id, Cancelling())
            current = client.read_flow_run(run.id)
            if not current.state or not current.state.is_cancelled():
                raise SchedulerNotSubmitted('工单取消尚未确认，请稍后重试')

        # A lost creation response can leave no local run id. Reusing the same
        # server idempotency key resolves that ambiguity before cancellation.
        if not existing_run_id:
            existing_run_id = self.ensure(task_id)['flow_run_id']
        with self._client() as client:
            deployment = client.read_deployment_by_name(self.deployment_name)
            root = client.read_flow_run(UUID(existing_run_id))
            self._check(root, deployment, task_id)
            pool_name = 'paperflow-waiting-folder-v2-candidate-20261031'
            folder_deployment = client.read_deployment_by_name(
                '资料文件夹工单-等待前处理候选20261031/等待前处理候选') if orders else None
            if folder_deployment is not None:
                pool = client.read_work_pool(pool_name)
                marker = 'paperflow-order-v2-candidate-20261031'
                if (folder_deployment.work_pool_name != pool_name or marker not in folder_deployment.tags
                        or not pool.is_paused or pool.description != marker):
                    raise SchedulerNotSubmitted('等待工单的专用工作池状态不可核验')
            folders = []
            for item in orders:
                group_id = item['group_id']
                key = 'folder-waiting-v2-' + task_id + '-' + group_id
                run = (client.read_flow_run(UUID(item['prefect_run_id'])) if item['prefect_run_id']
                       else lookup(client, key))
                expected_parent = uuid5(NAMESPACE_URL, 'paperflow-folder-parent/2/' + str(root.id)
                                        + '/' + task_id + '/' + group_id)
                if run is None:
                    # Resolve an ambiguous submission by the same idempotency
                    # key before cancelling; never assume a timeout sent nothing.
                    try:
                        parent = client.read_task_run(expected_parent)
                    except ObjectNotFound:
                        placeholder = task(name='资料文件夹工单', retries=0)(lambda: None)
                        placeholder.task_key = 'paperflow-folder-waiting-v2'
                        try:
                            parent = client.create_task_run(placeholder, flow_run_id=root.id,
                                dynamic_key=group_id, id=expected_parent,
                                name='资料文件夹 ' + group_id, state=Pending())
                        except Exception:
                            parent = client.read_task_run(expected_parent)
                    if parent.flow_run_id != root.id:
                        raise SchedulerNotSubmitted('文件夹父任务身份不匹配')
                    queue_name = 'task-' + task_id
                    try:
                        queue = client.read_work_queue_by_name(queue_name, work_pool_name=pool_name)
                    except ObjectNotFound:
                        try:
                            queue = client.create_work_queue(queue_name, is_paused=True,
                                                             work_pool_name=pool_name)
                        except ObjectAlreadyExists:
                            queue = client.read_work_queue_by_name(queue_name, work_pool_name=pool_name)
                    if not queue.is_paused:
                        raise SchedulerNotSubmitted('任务等待队列状态不可核验')
                    params = {key: item[key] for key in ('task_id', 'group_id', 'receipt_id', 'attempt_id', 'folder_label')}
                    run = client.create_flow_run_from_deployment(folder_deployment.id, parameters=params,
                              parent_task_run_id=expected_parent, idempotency_key=key, work_queue_name=queue_name,
                              name='资料文件夹 ' + item['folder_label'] + ' · 等待前处理')
                parent = client.read_task_run(expected_parent)
                if (run.idempotency_key != key or run.parent_task_run_id != expected_parent
                        or run.deployment_id != folder_deployment.id or parent.flow_run_id != root.id
                        or run.work_queue_name != 'task-' + task_id
                        or any((run.parameters or {}).get(k) != item[k]
                               for k in ('task_id', 'group_id', 'receipt_id', 'attempt_id'))):
                    raise SchedulerNotSubmitted('文件夹工单身份不匹配，不能删除')
                folders.append(run)
            # Validate every identity before changing any state. Retries only
            # revisit this task's already-cancelled or still-waiting orders.
            if root.state is None or (not root.state.is_final()
                    and not (root.state.is_scheduled() or root.state.is_paused())):
                raise SchedulerNotSubmitted('大工单仍在停止中，请稍后重试删除')
            for folder in folders:
                cancel_waiting(client, folder)
            cancel_waiting(client, root)
