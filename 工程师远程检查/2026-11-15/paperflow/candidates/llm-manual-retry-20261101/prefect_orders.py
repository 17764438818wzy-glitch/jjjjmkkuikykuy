"""Create and observe folder orders beneath the matching first-step flow run."""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import sqlite3
from threading import Thread
import time
from uuid import UUID, uuid5

POOL = 'paperflow-unified-preprocessing'
DEPLOYMENT = '资料文件夹工单/前处理'
MARKER = 'paperflow-order-v2-live-20261031'
LLM_STAGE_NAMESPACE = UUID('6aaf059a-c89a-52a8-acd5-4a928260a3b7')
LLM_STAGE_UNKNOWN = {'recovery_unknown', 'submission_unknown', 'delivery_unknown', 'execution_unknown', 'identity_unknown'}
LLM_STAGE_TASK_KEY = 'paperflow-folder-llm-stage-v1'


class OrderServiceUnavailable(RuntimeError):
    """The shared Prefect service or its waiting deployment is unavailable."""


def prefect_unavailable(exc):
    import httpx
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, (OrderServiceUnavailable, httpx.RequestError, ConnectionError, TimeoutError)):
            return True
        if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code >= 500:
            return True
        exc = exc.__cause__ or exc.__context__
    return False


def _identity(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(',', ':')).encode()).hexdigest()


def _llm_stage_id(folder_id, generation, retry_key):
    suffix = '' if not retry_key else ':manual:' + str(UUID(retry_key))
    if retry_key and suffix != ':manual:' + retry_key:
        raise ValueError('人工重试编号无效')
    return str(uuid5(LLM_STAGE_NAMESPACE, str(UUID(folder_id)) + ':' + str(generation) + suffix))


def _result_folder_matches(run, root_id, item, deployment_id):
    """Observe the actual folder flow using its reserved identity."""
    task_id, group_id = item['task_id'], item['group_id']
    return (run.idempotency_key == 'folder-waiting-v2-' + task_id + '-' + group_id
            and run.parent_task_run_id is None and run.deployment_id == deployment_id
            and run.work_queue_name == 'task-' + task_id
            and all((run.parameters or {}).get(key) == item[key]
                    for key in ('task_id', 'group_id', 'receipt_id', 'attempt_id')))


def _chunks(items, size=100):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _llm_failure_reason(state, request_unconfirmed):
    """Show a small allowlist of useful causes, without exposing model output."""
    message = str(getattr(state, 'message', '') or '')
    status = re.search(r'HTTP Error (502|503|429):', message)
    if status:
        reason = '模型请求返回 HTTP ' + status.group(1)
    elif 'ModelExplicitError' in message:
        reason = '模型输出过程中返回错误'
    elif 'broken data stream when reading image file' in message:
        reason = '正文图片读取异常'
    else:
        reason = 'LLM 工单执行失败，原因请查看工单记录'
    if request_unconfirmed:
        reason += '；工单或请求状态待核对，未自动重发'
    return reason


def _effective_llm_job(jobs):
    # An unresolved earlier stage stays visible across later generations.
    uncertain = next((job for job in jobs if job['state'] in LLM_STAGE_UNKNOWN), None)
    if uncertain is not None:
        return uncertain
    # A later paused or unsubmitted attempt must not erase an earlier failed
    # execution from the user's result view.
    return next((job for job in jobs if job['state'] not in {'stopped_before_submit', 'stopped'}), None)


class FolderOrderService:
    def __init__(self, queue, *, prefect_api_url, first_step_index, first_step_data_root,
                 first_step_receipt_root):
        self.queue = queue
        self.api_url = prefect_api_url.rstrip('/')
        self.index = Path(first_step_index).resolve()
        self.data_root = Path(first_step_data_root).resolve()
        self.receipt_root = Path(first_step_receipt_root).resolve()
        self._summary_inflight = False
        self._summary_cache = None
        self._results_inflight = False
        self._results_cache = None
        self._result_manifest_cache = {}
        self._llm_manifest_cache = {}
        self._summary_cursor = 0
        self._ready_sync_inflight = False
        self._ready_sync_done = {}
        self._ready_sync_retry = {}
        self._deleted_task_cursor = ''
        self._deleted_order_cursors = {}

    def _client(self):
        from prefect.client.orchestration import SyncPrefectClient
        return SyncPrefectClient(self.api_url, httpx_settings={'timeout': 3, 'trust_env': False})

    def _control(self):
        if not self.index.is_file():
            raise ValueError('上游任务索引不可用')
        db = sqlite3.connect(self.index.as_uri() + '?mode=ro', uri=True, timeout=5)
        db.row_factory = sqlite3.Row
        return db

    def _eligible(self, task_id=None, group_id=None, *, include_claimed=False):
        with closing(self._control()) as control:
            v2_tasks = {row[0] for row in control.execute(
                "SELECT task_id FROM tasks WHERE prefect_order_model='v2'")}
        if task_id is not None and task_id not in v2_tasks:
            return []
        with closing(self.queue._connect()) as db:
            session = db.execute("SELECT value FROM metadata WHERE key='receiver_session_id'").fetchone()
            if not session or not session[0]:
                return []
            sql = """
                SELECT g.task_id,g.group_id,g.paper_id,g.receipt_id,g.record,
                       o.receipt_id AS order_receipt_id,o.attempt_id,o.prefect_run_id,o.folder_label
                  FROM groups g LEFT JOIN preprocessing_folder_orders o
                    ON o.task_id=g.task_id AND o.group_id=g.group_id
                 WHERE g.state='waiting' AND g.checked_session=?
                   AND NOT EXISTS(SELECT 1 FROM cancelled_tasks t WHERE t.task_id=g.task_id)
                   AND EXISTS(SELECT 1 FROM retention_requests r
                        WHERE r.task_id=g.task_id AND r.group_id=g.group_id
                          AND r.paper_id=g.paper_id AND r.receipt_id=g.receipt_id
                          AND r.consumer_id=? AND r.state='accepted' AND r.retention_id IS NOT NULL
                          AND r.last_error IS NULL AND r.checked_session=?)
            """
            params = [session[0], self.queue.consumer_id, session[0]]
            if not include_claimed:
                sql += (' AND NOT EXISTS(SELECT 1 FROM claims c WHERE c.task_id=g.task_id '
                        'AND c.group_id=g.group_id AND c.receipt_id=g.receipt_id)')
            if task_id is not None:
                sql += ' AND g.task_id=? AND g.group_id=?'
                params.extend((task_id, group_id))
            sql += ' ORDER BY g.rowid'
            return [dict(row) for row in db.execute(sql, params).fetchall() if row['task_id'] in v2_tasks]

    def _task(self, task_id):
        with closing(self._control()) as db:
            task = db.execute('SELECT task_id,name,prefect_run_id,desired_state,start_pending,prefect_order_model,control_revision '
                              'FROM tasks WHERE task_id=?',
                              (task_id,)).fetchone()
            deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone()
        if not task or deleted or task['prefect_order_model'] != 'v2':
            raise ValueError('上游任务已删除或不可核验')
        return dict(task)

    @staticmethod
    def _label(item):
        try:
            record = json.loads(item['record'])
            receipt = json.loads(Path(record['receipt_path']).read_text(encoding='utf-8-sig'))
            label = Path(receipt['group_root']).name
        except (OSError, ValueError, KeyError, TypeError):
            label = item['group_id']
        return re.sub(r'[\x00-\x1f\x7f]', ' ', str(label)).strip()[:80] or item['group_id']

    def _reserve(self, item):
        attempt = item['attempt_id'] or _identity(['local-folder-attempt/1', item['task_id'], item['group_id']])
        label = item['folder_label'] or self._label(item)
        with closing(self.queue._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT 1 FROM cancelled_tasks WHERE task_id=?', (item['task_id'],)).fetchone():
                raise ValueError('任务已删除')
            db.execute('INSERT OR IGNORE INTO preprocessing_folder_orders '
                       '(task_id,group_id,receipt_id,attempt_id,folder_label,created_at) VALUES(?,?,?,?,?,?)',
                       (item['task_id'], item['group_id'], item['receipt_id'], attempt, label,
                        datetime.now(timezone.utc).isoformat()))
            row = db.execute('SELECT receipt_id,attempt_id,prefect_run_id,folder_label '
                             'FROM preprocessing_folder_orders WHERE task_id=? AND group_id=?',
                             (item['task_id'], item['group_id'])).fetchone()
            if not row or row['receipt_id'] != item['receipt_id'] or row['attempt_id'] != attempt:
                raise ValueError('同一文件夹存在不同的工单身份')
            return dict(row)

    def _safe_deployment(self, client):
        from prefect.exceptions import ObjectNotFound
        try:
            deployment = client.read_deployment_by_name(DEPLOYMENT)
            pool = client.read_work_pool(POOL)
        except ObjectNotFound as exc:
            raise OrderServiceUnavailable('等待工单部署或工作池不存在') from exc
        if deployment.work_pool_name != POOL or MARKER not in deployment.tags:
            raise OrderServiceUnavailable('前处理工单部署未绑定统一工作池')
        if pool.is_paused or pool.description != MARKER or pool.type != 'process':
            raise OrderServiceUnavailable('前处理工单工作池不可用')
        return deployment

    @staticmethod
    def _queue(client, task_id):
        from prefect.exceptions import ObjectAlreadyExists, ObjectNotFound
        name = 'task-' + task_id
        try:
            result = client.read_work_queue_by_name(name, work_pool_name=POOL)
        except ObjectNotFound:
            try:
                result = client.create_work_queue(name, is_paused=True, work_pool_name=POOL)
            except ObjectAlreadyExists:
                result = client.read_work_queue_by_name(name, work_pool_name=POOL)
        return name

    def _sync_queue(self, client, task_id):
        """Reconcile an existing task queue from the latest task intent."""
        from prefect.exceptions import ObjectNotFound
        for _ in range(3):
            try:
                queue = client.read_work_queue_by_name('task-' + task_id, work_pool_name=POOL)
            except ObjectNotFound:
                return
            try:
                task = self._task(task_id)
            except ValueError:
                # Only a confirmed deletion justifies changing the queue here;
                # an unavailable index is not evidence that the task is gone.
                with closing(self._control()) as db:
                    deleting = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone()
                with closing(self.queue._connect()) as db:
                    cancelled = db.execute('SELECT 1 FROM cancelled_tasks WHERE task_id=?', (task_id,)).fetchone()
                if not deleting and not cancelled:
                    raise
                if not queue.is_paused:
                    client.update_work_queue(queue.id, is_paused=True)
                return
            paused = task['desired_state'] != 'running' or bool(task['start_pending'])
            if queue.is_paused != paused:
                client.update_work_queue(queue.id, is_paused=paused)
            try:
                latest = self._task(task_id)
            except ValueError:
                continue
            observed = client.read_work_queue_by_name('task-' + task_id, work_pool_name=POOL)
            if latest['control_revision'] == task['control_revision'] and observed.is_paused == paused:
                return
        raise ValueError('任务状态变化中，Prefect 队列尚未确认')

    def sync_one(self, task_id, group_id):
        task_id = str(UUID(task_id))
        if not re.fullmatch(r'[0-9a-f]{32}', group_id):
            raise ValueError('文件夹编号无效')
        task = self._task(task_id)
        # A completed registration may be recorded while the task is paused.
        # The task queue prevents a paused task from admitting new work.
        root_id = task['prefect_run_id']
        if not root_id:
            raise ValueError('任务大工单尚未绑定')
        items = self._eligible(task_id, group_id)
        if not items:
            return None
        item = items[0]
        if item['prefect_run_id']:
            if item['order_receipt_id'] != item['receipt_id'] or not item['attempt_id']:
                raise ValueError('已有文件夹工单身份不匹配')
            return item['prefect_run_id']
        # Record the stable identity before the remote call. Deletion marks
        # cancelled_tasks in the same database and resolves every reserved
        # identity using the same Prefect idempotency key.
        order = self._reserve(item)
        params = {'task_id': task_id, 'group_id': group_id, 'receipt_id': item['receipt_id'],
                  'attempt_id': order['attempt_id'], 'folder_label': order['folder_label']}
        from prefect.states import Scheduled
        with self._client() as client:
            deployment = self._safe_deployment(client)
            parent_run = client.read_flow_run(UUID(root_id))
            if (not parent_run.state or parent_run.state.is_final() or parent_run.state.is_cancelling()
                    or parent_run.idempotency_key != 'first-step-order-v2-' + task_id
                    or (parent_run.parameters or {}).get('task_id') != task_id
                    or parent_run.parent_task_run_id is not None):
                raise ValueError('上游大工单身份不匹配')
            queue_name = self._queue(client, task_id)
            run = client.create_flow_run_from_deployment(
                deployment.id, parameters=params,
                idempotency_key='folder-waiting-v2-' + task_id + '-' + group_id,
                work_queue_name=queue_name, name='资料文件夹 ' + order['folder_label'] + ' · 等待前处理',
                state=Scheduled(name='等待绑定', scheduled_time=datetime.now(timezone.utc)))
            if (run.deployment_id != deployment.id or run.parent_task_run_id is not None
                    or run.idempotency_key != 'folder-waiting-v2-' + task_id + '-' + group_id
                    or run.work_queue_name != queue_name
                    or any((run.parameters or {}).get(k) != v for k, v in params.items())):
                raise ValueError('文件夹工单与登记身份不匹配')
        with closing(self.queue._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('UPDATE preprocessing_folder_orders SET prefect_run_id=? '
                       'WHERE task_id=? AND group_id=? AND receipt_id=? AND attempt_id=? '
                       'AND (prefect_run_id IS NULL OR prefect_run_id=?)',
                       (str(run.id), task_id, group_id, item['receipt_id'], order['attempt_id'], str(run.id)))
            saved = db.execute('SELECT prefect_run_id FROM preprocessing_folder_orders '
                               'WHERE task_id=? AND group_id=?', (task_id, group_id)).fetchone()
            if not saved or saved['prefect_run_id'] != str(run.id):
                raise ValueError('文件夹工单记录发生冲突')
            cancelled = db.execute('SELECT 1 FROM cancelled_tasks WHERE task_id=?', (task_id,)).fetchone()
        if cancelled:
            return None
        with self._client() as client:
            self._safe_deployment(client)
            self._sync_queue(client, task_id)
        return str(run.id)

    def sync_existing(self, *, stop_event=None):
        failures = 0
        service_down = False
        for item in self._eligible():
            if stop_event is not None and stop_event.is_set():
                break
            if item['prefect_run_id']:
                continue
            try:
                result = self.sync_one(item['task_id'], item['group_id'])
                if result is None and self._eligible(item['task_id'], item['group_id']):
                    failures += 1
            except Exception as exc:
                failures += 1
                if prefect_unavailable(exc):
                    service_down = True
                    break
        if not service_down and not (stop_event is not None and stop_event.is_set()):
            failures += self.reconcile()
            try:
                failures += self.reconcile_cancelled()
            except Exception:
                failures += 1
        return failures

    def reconcile_cancelled(self):
        """Keep deleted tasks cancelled if an earlier remote resume arrives late."""
        with closing(self._control()) as control:
            # The delete path itself waits for active work before cancelling.
            # Compensation applies only after that path removed the task row.
            query = (
                'SELECT d.task_id FROM task_deletions d '
                'WHERE NOT EXISTS(SELECT 1 FROM tasks t WHERE t.task_id=d.task_id) '
                'AND d.task_id>? ORDER BY d.task_id LIMIT 20')
            deleted = [row[0] for row in control.execute(query, (self._deleted_task_cursor,))]
            if not deleted and self._deleted_task_cursor:
                deleted = [row[0] for row in control.execute(query, ('',))]
        if not deleted:
            return 0
        self._deleted_task_cursor = deleted[-1]
        with closing(self.queue._connect()) as db:
            query = ('SELECT o.rowid AS rowid,o.task_id,o.group_id,o.receipt_id,o.attempt_id,o.prefect_run_id '
                     'FROM preprocessing_folder_orders o JOIN cancelled_tasks c ON c.task_id=o.task_id '
                     'WHERE o.task_id=? AND o.rowid>? ORDER BY o.rowid LIMIT ?')
            rows = []
            per_task = max(1, 500 // len(deleted))
            for task_id in deleted:
                cursor = self._deleted_order_cursors.get(task_id, 0)
                page = [dict(row) for row in db.execute(query, (task_id, cursor, per_task))]
                if not page and cursor:
                    page = [dict(row) for row in db.execute(query, (task_id, 0, per_task))]
                if page:
                    self._deleted_order_cursors[task_id] = page[-1]['rowid']
                    rows.extend(page)
        from prefect.client.schemas.filters import (FlowRunFilter, FlowRunFilterIdempotencyKey,
                                                     FlowRunFilterState, FlowRunFilterStateType)
        from prefect.client.schemas.objects import StateType
        from prefect.client.schemas.responses import SetStateStatus
        from prefect.exceptions import ObjectNotFound
        from prefect.states import Cancelling
        failures = 0
        def cancellation_submitted(run):
            result = client.set_flow_run_state(run.id, Cancelling())
            return (result.status == SetStateStatus.ACCEPT or
                    (result.state is not None and result.state.is_final()))

        root_keys = {'first-step-order-v2-' + task_id: task_id for task_id in deleted}
        with self._client() as client:
            root_deployment = client.read_deployment_by_name('论文第一步-任务工单/手动运行')
            folder_deployment = client.read_deployment_by_name(DEPLOYMENT)
            if MARKER not in root_deployment.tags or MARKER not in folder_deployment.tags:
                raise OrderServiceUnavailable('统一工单部署身份不可核验')
            roots = {}
            for chunk in _chunks(list(root_keys)):
                for run in client.read_flow_runs(flow_run_filter=FlowRunFilter(
                        idempotency_key=FlowRunFilterIdempotencyKey(any_=chunk)), limit=len(chunk)):
                    task_id = root_keys.get(run.idempotency_key)
                    if (not task_id or run.deployment_id != root_deployment.id or run.parent_task_run_id is not None
                            or (run.parameters or {}).get('task_id') != task_id):
                        failures += 1
                        continue
                    roots[task_id] = run
            for task_id, root in roots.items():
                try:
                    queue = client.read_work_queue_by_name('task-' + task_id, work_pool_name=POOL)
                    if not queue.is_paused:
                        client.update_work_queue(queue.id, is_paused=True)
                except ObjectNotFound:
                    pass
                if root.state and not root.state.is_final() and not root.state.is_cancelling():
                    if not cancellation_submitted(root):
                        failures += 1
            relevant = {('folder-waiting-v2-' + row['task_id'] + '-' + row['group_id']): row
                        for row in rows if row['task_id'] in roots}
            nonfinal = [StateType.SCHEDULED, StateType.PENDING, StateType.RUNNING,
                        StateType.PAUSED, StateType.CANCELLING]
            for chunk in _chunks(list(relevant)):
                found = client.read_flow_runs(flow_run_filter=FlowRunFilter(
                    idempotency_key=FlowRunFilterIdempotencyKey(any_=chunk),
                    state=FlowRunFilterState(type=FlowRunFilterStateType(any_=nonfinal))), limit=len(chunk))
                for run in found:
                    row = relevant[run.idempotency_key]
                    if (run.deployment_id != folder_deployment.id or run.parent_task_run_id is not None
                            or run.work_queue_name != 'task-' + row['task_id']
                            or (row['prefect_run_id'] and row['prefect_run_id'] != str(run.id))
                            or any((run.parameters or {}).get(key) != row[key]
                                   for key in ('task_id', 'group_id', 'receipt_id', 'attempt_id'))):
                        failures += 1
                        continue
                    if run.state and not run.state.is_cancelling():
                        if not cancellation_submitted(run):
                            failures += 1
        return failures

    def _recoverable(self):
        """Read already-bound orders with currently verified local retention."""
        with closing(self._control()) as control:
            v2_tasks = {row[0] for row in control.execute(
                "SELECT task_id FROM tasks WHERE prefect_order_model='v2'")}
        with closing(self.queue._connect()) as db:
            session = db.execute("SELECT value FROM metadata WHERE key='receiver_session_id'").fetchone()
            if not session or not session[0]:
                return []
            rows = db.execute("""
                SELECT o.task_id,o.group_id,o.receipt_id,o.attempt_id,o.prefect_run_id,g.state
                  FROM preprocessing_folder_orders o JOIN groups g
                    ON g.task_id=o.task_id AND g.group_id=o.group_id AND g.receipt_id=o.receipt_id
                 WHERE o.prefect_run_id IS NOT NULL AND g.state IN ('waiting','processing','ready_for_llm')
                   AND g.checked_session=?
                   AND NOT EXISTS(SELECT 1 FROM cancelled_tasks t WHERE t.task_id=g.task_id)
                   AND NOT EXISTS(SELECT 1 FROM preprocessing_failures f
                        WHERE f.consumer_id='prefect-local-preprocessing' AND f.attempt_id=o.attempt_id)
                   AND EXISTS(SELECT 1 FROM retention_requests r
                        WHERE r.task_id=g.task_id AND r.group_id=g.group_id AND r.paper_id=g.paper_id
                          AND r.receipt_id=g.receipt_id AND r.consumer_id=? AND r.state='accepted'
                          AND r.retention_id IS NOT NULL AND r.last_error IS NULL AND r.checked_session=?)
                 ORDER BY g.rowid
            """, (session[0], self.queue.consumer_id, session[0])).fetchall()
        return [dict(row) for row in rows if row['task_id'] in v2_tasks]

    def reconcile(self):
        """Repair queue state and resume only the same verified waiting order."""
        items = self._recoverable()
        # Handed-off orders stay suspended. The independently bounded LLM
        # task worker consumes them while preserving the folder FlowRun ID.
        items = [item for item in items if item['state'] != 'ready_for_llm']
        with closing(self.queue._connect()) as db:
            queue_tasks = {row[0] for row in db.execute(
                'SELECT DISTINCT o.task_id FROM preprocessing_folder_orders o '
                'WHERE NOT EXISTS(SELECT 1 FROM cancelled_tasks t WHERE t.task_id=o.task_id)')}
        if not items and not queue_tasks:
            return 0
        from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId
        from prefect.client.schemas.responses import SetStateStatus
        tasks = {}
        failures = 0
        for task_id in queue_tasks | {item['task_id'] for item in items}:
            try:
                tasks[task_id] = self._task(task_id)
            except ValueError:
                continue
        with self._client() as client:
            deployment = self._safe_deployment(client)
            for task_id in list(tasks):
                try:
                    self._sync_queue(client, task_id)
                except Exception as exc:
                    if prefect_unavailable(exc):
                        raise
                    failures += 1
                    del tasks[task_id]
            ids = {UUID(item['prefect_run_id']) for item in items if item['task_id'] in tasks}
            item_task_ids = {item['task_id'] for item in items}
            ids.update(UUID(task['prefect_run_id']) for task_id, task in tasks.items()
                       if task_id in item_task_ids and task['prefect_run_id'])
            flows = {}
            for chunk in _chunks(list(ids)):
                for run in client.read_flow_runs(flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=chunk)),
                                                 limit=len(chunk)):
                    flows[run.id] = run
            now = datetime.now(timezone.utc)
            for item in items:
                try:
                    task = tasks.get(item['task_id'])
                    if not task or not task['prefect_run_id']:
                        continue
                    root_id = UUID(task['prefect_run_id'])
                    root, run = flows.get(root_id), flows.get(UUID(item['prefect_run_id']))
                    if (not root or not root.state or root.state.is_final() or root.state.is_cancelling()
                            or root.idempotency_key != 'first-step-order-v2-' + item['task_id']
                            or (root.parameters or {}).get('task_id') != item['task_id']
                            or not run or run.deployment_id != deployment.id or run.parent_task_run_id is not None
                            or run.idempotency_key != 'folder-waiting-v2-' + item['task_id'] + '-' + item['group_id']
                            or run.work_queue_name != 'task-' + item['task_id']
                            or any((run.parameters or {}).get(k) != item[k]
                                   for k in ('task_id', 'group_id', 'receipt_id', 'attempt_id'))):
                        raise ValueError('文件夹工单身份无法核对')
                    # Legacy native subflows cannot be independently suspended;
                    # retain them for diagnosis or cancellation, never replay them.
                    if (run.parent_task_run_id is not None or not run.state or not run.state.is_paused()
                            or run.state.state_details.pause_reschedule is not True
                            or task['desired_state'] != 'running'
                            or task['start_pending'] or not run.state.timestamp):
                        continue
                    name = run.name or ''
                    if not any(name.endswith(phase) for phase in
                               ('等待绑定', '等待任务开始', '等待资料复核', '资料暂忙')):
                        continue
                    if name.endswith('资料暂忙') and (now - run.state.timestamp).total_seconds() < 30:
                        continue
                    latest = self._task(item['task_id'])
                    with closing(self.queue._connect()) as db:
                        cancelled = db.execute('SELECT 1 FROM cancelled_tasks WHERE task_id=?',
                                               (item['task_id'],)).fetchone()
                    if latest['desired_state'] != 'running' or latest['start_pending'] or cancelled:
                        continue
                    result = client.resume_flow_run(run.id)
                    if result.status != SetStateStatus.ACCEPT:
                        raise ValueError('等待中的文件夹工单尚未恢复')
                except Exception as exc:
                    if prefect_unavailable(exc):
                        raise
                    failures += 1
        return failures

    def _source_matches(self, snapshot):
        instance = snapshot.get('scan_source_instance')
        if not instance:
            return False
        from upstream_discovery import FirstStepHTTPSource
        source = FirstStepHTTPSource(timeout=1)
        source.expected_index = self.index
        source.expected_queue = self.queue.path
        source.expected_prefect_api = self.api_url
        return source.verified_binding(self.receipt_root) == instance

    def _local_ticket_snapshot(self, snapshot):
        """Count locally registered folder orders, without delaying their execution."""
        with closing(self._control()) as control:
            tasks = {row[0]: row[1] for row in control.execute(
                "SELECT task_id,prefect_run_id FROM tasks WHERE prefect_order_model='v2' AND prefect_run_id IS NOT NULL")}
        as_of = datetime.now(timezone.utc).isoformat()
        started = time.monotonic()
        with closing(self.queue._connect()) as db:
            db.execute('BEGIN')
            meta = dict(db.execute(
                "SELECT key,value FROM metadata WHERE key IN "
                "('receiver_session_id','receiver_status','scan_source_instance')").fetchall())
            session = snapshot.get('receiver_session_id')
            if (not session or meta.get('receiver_session_id') != session
                    or meta.get('receiver_status') != 'running'
                    or meta.get('scan_source_instance') != snapshot.get('scan_source_instance')):
                raise ValueError('接收会话已经发生变化')
            rows = db.execute("""
                SELECT g.task_id,g.state,COUNT(*) AS number
                  FROM groups g
                  JOIN preprocessing_folder_orders o
                    ON o.task_id=g.task_id AND o.group_id=g.group_id AND o.receipt_id=g.receipt_id
                  JOIN retention_requests r
                    ON r.task_id=g.task_id AND r.group_id=g.group_id
                   AND r.paper_id=g.paper_id AND r.receipt_id=g.receipt_id AND r.consumer_id=?
                  LEFT JOIN claims c
                    ON c.task_id=g.task_id AND c.group_id=g.group_id AND c.receipt_id=g.receipt_id
                  LEFT JOIN preprocessing_handoffs h ON h.receipt_id=g.receipt_id
                 WHERE g.checked_session=? AND r.checked_session=?
                   AND r.state='accepted' AND r.retention_id IS NOT NULL AND r.last_error IS NULL
                   AND o.prefect_run_id IS NOT NULL AND o.attempt_id IS NOT NULL
                   AND NOT EXISTS(SELECT 1 FROM cancelled_tasks t WHERE t.task_id=g.task_id)
                   AND (
                     (g.state='waiting' AND
                        (c.attempt_id IS NULL OR
                           (c.consumer_id='prefect-local-preprocessing' AND c.attempt_id=o.attempt_id
                            AND c.state='checking')))
                     OR (g.state='processing' AND c.consumer_id='prefect-local-preprocessing'
                         AND c.attempt_id=o.attempt_id AND c.state='processing')
                     OR (g.state='ready_for_llm' AND c.consumer_id='prefect-local-preprocessing'
                         AND c.attempt_id=o.attempt_id AND c.state='completed' AND h.state='waiting'
                         AND h.task_id=g.task_id AND h.group_id=g.group_id AND h.paper_id=g.paper_id
                         AND h.consumer_id=c.consumer_id AND h.attempt_id=c.attempt_id)
                   )
                 GROUP BY g.task_id,g.state
            """, (self.queue.consumer_id, session, session)).fetchall()
            waiting_handoffs = db.execute("""
                SELECT g.task_id,g.group_id,g.receipt_id,o.attempt_id,o.prefect_run_id
                  FROM groups g
                  JOIN preprocessing_folder_orders o ON o.task_id=g.task_id AND o.group_id=g.group_id
                       AND o.receipt_id=g.receipt_id
                  JOIN retention_requests r ON r.task_id=g.task_id AND r.group_id=g.group_id
                       AND r.paper_id=g.paper_id AND r.receipt_id=g.receipt_id AND r.consumer_id=?
                  JOIN claims c ON c.task_id=g.task_id AND c.group_id=g.group_id
                       AND c.receipt_id=g.receipt_id AND c.consumer_id='prefect-local-preprocessing'
                       AND c.attempt_id=o.attempt_id AND c.state='completed'
                  JOIN preprocessing_handoffs h ON h.receipt_id=g.receipt_id AND h.task_id=g.task_id
                       AND h.group_id=g.group_id AND h.paper_id=g.paper_id AND h.consumer_id=c.consumer_id
                       AND h.attempt_id=c.attempt_id AND h.state='waiting'
                 WHERE g.state='ready_for_llm' AND g.checked_session=? AND r.checked_session=?
                   AND r.state='accepted' AND r.retention_id IS NOT NULL AND r.last_error IS NULL
                   AND o.prefect_run_id IS NOT NULL
                   AND NOT EXISTS(SELECT 1 FROM cancelled_tasks t WHERE t.task_id=g.task_id)
            """, (self.queue.consumer_id, session, session)).fetchall()
        numbers = {'waiting': 0, 'processing': 0, 'ready_for_llm': 0}
        for row in rows:
            if row['task_id'] in tasks:
                numbers[row['state']] += row['number']
        # A local handoff alone does not mean the Prefect order actually paused.
        # Observe a rotating portion if the status query takes too long. This
        # only bounds UI observation, never admission or execution of work.
        from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId
        handoffs = [dict(row) for row in waiting_handoffs if row['task_id'] in tasks]
        start_at = self._summary_cursor % len(handoffs) if handoffs else 0
        handoffs = handoffs[start_at:] + handoffs[:start_at]
        verified_waiting = 0
        checked = 0
        with self._client() as client:
            deployment = self._safe_deployment(client)
            for chunk in _chunks(handoffs):
                if checked and time.monotonic() - started >= 2.5:
                    break
                root_ids = {UUID(tasks[item['task_id']]) for item in chunk}
                flow_ids = {UUID(item['prefect_run_id']) for item in chunk} | root_ids
                flows = {run.id: run for run in client.read_flow_runs(
                    flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=list(flow_ids))), limit=len(flow_ids))}
                for item in chunk:
                    root_id = UUID(tasks[item['task_id']])
                    root, run = flows.get(root_id), flows.get(UUID(item['prefect_run_id']))
                    if (not root or not root.state or root.state.is_final() or root.state.is_cancelling()
                            or root.idempotency_key != 'first-step-order-v2-' + item['task_id']
                            or (root.parameters or {}).get('task_id') != item['task_id']
                            or not run or run.deployment_id != deployment.id or run.parent_task_run_id is not None
                            or run.idempotency_key != 'folder-waiting-v2-' + item['task_id'] + '-' + item['group_id']
                            or run.work_queue_name != 'task-' + item['task_id'] or not run.state
                            or not run.state.is_paused() or run.state.state_details.pause_reschedule is not True
                            or not (run.name or '').endswith('等待 LLM')
                            or any((run.parameters or {}).get(k) != item[k]
                                   for k in ('task_id', 'group_id', 'receipt_id', 'attempt_id'))):
                        continue
                    verified_waiting += 1
                checked += len(chunk)
        self._summary_cursor = ((start_at + checked) % len(handoffs)) if handoffs else 0
        counts = {
            'waiting_preprocessing': numbers['waiting'],
            'preprocessing_in_progress': numbers['processing'],
            'waiting_llm': verified_waiting,
        }
        complete = (snapshot.get('upstream_scan_verified') is True
                    and snapshot.get('waiting_count') == numbers['waiting']
                    and snapshot.get('in_progress_count') == numbers['processing']
                    and checked == len(handoffs)
                    and snapshot.get('waiting_llm_total') == numbers['ready_for_llm'] == verified_waiting)
        # An interrupted worker's database claim is not necessarily still executing.
        if snapshot.get('in_progress_count') != numbers['processing']:
            counts.pop('preprocessing_in_progress')
        # An incomplete scan can establish positive, locally registered orders;
        # it cannot establish that an empty queue is truly empty.
        if not complete:
            counts = {key: value for key, value in counts.items() if value > 0}
        return {'source': 'prefect', 'scope': 'all_tasks', 'available': True,
                'basis': 'local_registered_orders', 'as_of': as_of,
                'source_counts': {'waiting_preprocessing': snapshot.get('waiting_count'),
                                  'preprocessing_in_progress': snapshot.get('in_progress_count'),
                                  'waiting_llm': snapshot.get('waiting_llm_total')},
                'waiting_llm_prefect_verified': True,
                'partial': not complete, 'counts': counts}

    def summary(self, snapshot):
        """Return a dated local-order snapshot; refresh Prefect availability in the background."""
        unavailable = {'source': 'prefect', 'scope': 'all_tasks', 'available': False, 'counts': {}}
        if snapshot.get('receiver_status') != 'running':
            return unavailable
        try:
            if not self._source_matches(snapshot):
                return unavailable
        except Exception:
            return unavailable
        key = (snapshot.get('receiver_session_id'), snapshot.get('scan_source_instance'))

        def refresh():
            value = unavailable
            try:
                value = self._local_ticket_snapshot(snapshot)
                current = self.queue.snapshot()
                if (current.get('receiver_status') != 'running'
                        or (current.get('receiver_session_id'), current.get('scan_source_instance')) != key
                        or not self._source_matches(current)):
                    value = unavailable
            except Exception:
                value = unavailable
            self._summary_cache = (key, time.monotonic(), value)
            self._summary_inflight = False

        cached = self._summary_cache
        age = time.monotonic() - cached[1] if cached and cached[0] == key else None
        if age is not None and age <= 8:
            if age >= 4 and not self._summary_inflight:
                self._summary_inflight = True
                Thread(target=refresh, name='prefect-waiting-summary', daemon=True).start()
            result = dict(cached[2])
            if result.get('available'):
                current = {'waiting_preprocessing': snapshot.get('waiting_count'),
                           'preprocessing_in_progress': snapshot.get('in_progress_count'),
                           'waiting_llm': snapshot.get('waiting_llm_total')}
                counts = dict(result.get('counts', {}))
                for stage, value in list(counts.items()):
                    if (current.get(stage) != result.get('source_counts', {}).get(stage)
                            or (snapshot.get('upstream_scan_verified') is not True and value == 0)):
                        counts.pop(stage)
                        result['partial'] = True
                if snapshot.get('upstream_scan_verified') is not True:
                    result['partial'] = True
                result['counts'] = counts
            return result
        if not self._summary_inflight:
            self._summary_inflight = True
            Thread(target=refresh, name='prefect-waiting-summary', daemon=True).start()
        return unavailable

    def _result_tasks(self):
        with closing(self._control()) as db:
            return {row['task_id']: dict(row) for row in db.execute(
                "SELECT t.task_id,t.name,t.prefect_run_id FROM tasks t "
                "WHERE t.prefect_order_model='v2' "
                "AND NOT EXISTS(SELECT 1 FROM task_deletions d WHERE d.task_id=t.task_id)")}

    @staticmethod
    def _results_key(snapshot, tasks):
        return (snapshot.get('receiver_session_id'), snapshot.get('scan_source_instance'),
                tuple(sorted((task_id, task['prefect_run_id'], task['name'])
                             for task_id, task in tasks.items())))

    def _llm_snapshot(self):
        folders, admissions, jobs = {}, {}, {}
        with closing(self._control()) as control:
            control.execute('BEGIN')
            for value in control.execute(
                    'SELECT task_id,group_id,paper_id,receipt_id,attempt_id,parent_run_id,'
                    'task_run_id,manifest_sha256,state,updated_at_ns FROM llm_folder_queue'):
                entry = dict(value)
                key = (entry['task_id'], entry['group_id'])
                if key in folders:
                    raise ValueError('LLM 文件夹工单身份重复')
                folders[key] = entry
            for value in control.execute(
                    'SELECT task_id,component_id,task_run_id,receipt_id,kind,state,request_sha256,'
                    'response_sha256,source_sha256,updated_at_ns FROM llm_request_admissions'):
                entry = dict(value)
                admissions.setdefault((entry['task_id'], entry['task_run_id']), []).append(entry)
            exists = control.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                     "AND name='paperflow_llm_stage_jobs'").fetchone()
            if exists:
                columns = {row[1] for row in control.execute('PRAGMA table_info(paperflow_llm_stage_jobs)')}
                retry_key = 'retry_key' if 'retry_key' in columns else "'' AS retry_key"
                has_retry_items = control.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='paperflow_llm_retry_items'").fetchone()
                retry_fields = (
                    '(SELECT i.old_flow_state_id FROM paperflow_llm_retry_items i '
                    'WHERE i.request_id=paperflow_llm_stage_jobs.retry_key '
                    'AND i.job_id=paperflow_llm_stage_jobs.job_id) AS retry_old_flow_state_id,'
                    '(SELECT i.state FROM paperflow_llm_retry_items i '
                    'WHERE i.request_id=paperflow_llm_stage_jobs.retry_key '
                    'AND i.job_id=paperflow_llm_stage_jobs.job_id) AS retry_item_state '
                    if has_retry_items and 'retry_key' in columns
                    else 'NULL AS retry_old_flow_state_id,NULL AS retry_item_state ')
                for value in control.execute(
                        'SELECT job_id,task_id,group_id,paper_id,receipt_id,attempt_id,parent_run_id,'
                        'folder_run_id,generation,task_key,state,error_type,created_at_ns,updated_at_ns,'
                        + retry_key + ',' + retry_fields
                        + 'FROM paperflow_llm_stage_jobs ORDER BY generation DESC,created_at_ns DESC'):
                    entry = dict(value)
                    jobs.setdefault((entry['task_id'], entry['group_id']), []).append(entry)
        return folders, admissions, jobs

    @staticmethod
    def _llm_completion_reader():
        # Reuse the installed, pure folder completion contract without
        # importing or starting the worker or any model request code.
        path = (Path(__file__).resolve().parents[2] / 'prefect-paperflow' /
                'paperflow_prefect' / 'llm_queue' / 'folder_completion.py')
        spec = importlib.util.spec_from_file_location('_paperflow_llm_result_contract', path)
        if spec is None or spec.loader is None:
            raise ValueError('LLM 完成登记契约不可用')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if not callable(getattr(module, 'inspect_folder', None)):
            raise ValueError('LLM 完成登记契约不可用')
        return module

    @staticmethod
    def _llm_handoff(item, expected_digest, previous, current):
        reference = json.loads(item['handoff'])
        path = Path(reference['manifest_path']).resolve(strict=True)
        stat = path.stat()
        key = (str(path), reference['manifest_sha256'], stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
        manifest = previous.get(key)
        if manifest is None:
            raw = path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != reference['manifest_sha256']:
                raise ValueError('LLM 交接文件发生变化')
            manifest = json.loads(raw)
            if not isinstance(manifest, dict):
                raise ValueError('LLM 交接文件格式无效')
        digest = hashlib.sha256(json.dumps(manifest, ensure_ascii=False, sort_keys=True,
                                separators=(',', ':'), allow_nan=False).encode('utf8')).hexdigest()
        if digest != expected_digest:
            raise ValueError('LLM 完成登记与交接身份不一致')
        if any(manifest.get(name) != item[name] for name in
               ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')):
            raise ValueError('LLM 完成登记文件夹身份不一致')
        current[key] = manifest
        return manifest

    def _build_results(self, snapshot, tasks):
        """Collect one dated snapshot of local outcomes and Prefect run states."""
        from prefect.client.schemas.filters import (FlowRunFilter, FlowRunFilterId,
                                                    TaskRunFilter, TaskRunFilterId)

        as_of = datetime.now(timezone.utc).isoformat()
        session = snapshot.get('receiver_session_id')
        # Keep one short, read-only local snapshot. Never hold a SQLite read
        # transaction while talking to Prefect or processing a result page.
        llm_available, llm_rows, llm_admissions, llm_jobs = True, {}, {}, {}
        try:
            llm_rows, llm_admissions, llm_jobs = self._llm_snapshot()
        except (OSError, sqlite3.Error, ValueError):
            # Existing preprocessing results remain available independently.
            llm_available = False
        with closing(self.queue._connect()) as db:
            db.execute('BEGIN')
            meta = dict(db.execute("SELECT key,value FROM metadata WHERE key IN "
                                   "('receiver_session_id','receiver_status','scan_source_instance')"))
            if (not session or meta.get('receiver_session_id') != session
                    or meta.get('receiver_status') != 'running'
                    or meta.get('scan_source_instance') != snapshot.get('scan_source_instance')):
                raise ValueError('接收会话已经发生变化')
            rows = [dict(row) for row in db.execute("""
                SELECT g.task_id,g.group_id,g.paper_id,g.receipt_id,g.state AS group_state,
                       g.review_reason,o.attempt_id,o.prefect_run_id,o.folder_label,
                       c.state AS claim_state,f.error_type,h.handoff,h.state AS handoff_state
                  FROM groups g
                  JOIN preprocessing_folder_orders o ON o.task_id=g.task_id AND o.group_id=g.group_id
                       AND o.receipt_id=g.receipt_id
                  JOIN retention_requests r ON r.task_id=g.task_id AND r.group_id=g.group_id
                       AND r.paper_id=g.paper_id AND r.receipt_id=g.receipt_id AND r.consumer_id=?
                  LEFT JOIN claims c ON c.task_id=g.task_id AND c.group_id=g.group_id
                       AND c.receipt_id=g.receipt_id AND c.consumer_id='prefect-local-preprocessing'
                       AND c.attempt_id=o.attempt_id
                  LEFT JOIN preprocessing_failures f ON f.consumer_id='prefect-local-preprocessing'
                       AND f.attempt_id=o.attempt_id
                  LEFT JOIN preprocessing_handoffs h ON h.receipt_id=g.receipt_id AND h.task_id=g.task_id
                       AND h.group_id=g.group_id AND h.paper_id=g.paper_id
                       AND h.consumer_id='prefect-local-preprocessing' AND h.attempt_id=o.attempt_id
                 WHERE g.checked_session=? AND r.checked_session=? AND r.state='accepted'
                   AND r.retention_id IS NOT NULL AND r.last_error IS NULL
                   AND NOT EXISTS(SELECT 1 FROM cancelled_tasks t WHERE t.task_id=g.task_id)
                 ORDER BY g.task_id,g.group_id
            """, (self.queue.consumer_id, session, session)).fetchall() if row['task_id'] in tasks]
        ids = {UUID(row['prefect_run_id']) for row in rows if row['prefect_run_id']}
        ids.update(UUID(task['prefect_run_id']) for task in tasks.values() if task['prefect_run_id'])
        flows, stage_runs = {}, {}
        with self._client() as client:
            deployment = self._safe_deployment(client)
            root_deployment = client.read_deployment_by_name('论文第一步-任务工单/手动运行')
            if MARKER not in root_deployment.tags:
                raise OrderServiceUnavailable('大工单部署身份不可核验')
            for chunk in _chunks(list(ids)):
                for run in client.read_flow_runs(flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=chunk)),
                                                 limit=len(chunk)):
                    flows[run.id] = run
            if llm_available and llm_jobs:
                try:
                    selected = [_effective_llm_job(values) for key, values in llm_jobs.items()
                                if values and key[0] in tasks]
                    stage_ids = [UUID(job['job_id']) for job in selected if job]
                    for chunk in _chunks(stage_ids):
                        for stage in client.read_task_runs(
                                task_run_filter=TaskRunFilter(id=TaskRunFilterId(any_=chunk)), limit=len(chunk)):
                            stage_runs[str(stage.id)] = stage
                except Exception:
                    # The existing preprocessing snapshot remains independent.
                    llm_available = False
        buckets = {'passed': [], 'failed': []}
        llm_buckets = {'passed': [], 'failed': []}
        llm_partial = False
        llm_contract = None
        current_llm_manifests = {}
        if llm_available:
            try:
                llm_contract = self._llm_completion_reader()
            except Exception:
                llm_available = False
        changed_runs, changed_jobs = set(), set()
        uncertain_runs = {key for key, values in llm_admissions.items()
                          if any(r['state'] == 'unknown' for r in values)}
        if llm_available:
            try:
                recent_rows, recent_admissions, recent_jobs = self._llm_snapshot()
                if recent_rows != llm_rows:
                    llm_partial = True
                for key in set(llm_admissions) | set(recent_admissions):
                    before = {r['component_id']: r for r in llm_admissions.get(key, [])}
                    after = {r['component_id']: r for r in recent_admissions.get(key, [])}
                    if before != after:
                        changed_runs.add(key)
                if changed_runs:
                    llm_partial = True
                changed_jobs = {key for key in set(llm_jobs) | set(recent_jobs)
                                if llm_jobs.get(key) != recent_jobs.get(key)}
                if changed_jobs:
                    llm_partial = True
                # Preserve earlier unknown evidence as well as the latest state.
                uncertain_runs.update(key for key, values in recent_admissions.items()
                                      if any(r['state'] == 'unknown' for r in values))
                llm_admissions = recent_admissions
            except (OSError, sqlite3.Error, ValueError, KeyError, TypeError):
                llm_available = False
        seen_llm_keys, seen_llm_runs = set(), set()
        previous_manifests = self._result_manifest_cache
        verified_manifests = {}
        partial = (snapshot.get('upstream_scan_verified') is not True
                   or any(not task['prefect_run_id'] for task in tasks.values()))
        for task_id, task in tasks.items():
            root = flows.get(UUID(task['prefect_run_id'])) if task['prefect_run_id'] else None
            if (not root or not root.state or root.state.is_final() or root.state.is_cancelling()
                    or root.deployment_id != root_deployment.id or root.parent_task_run_id is not None
                    or root.idempotency_key != 'first-step-order-v2-' + task_id
                    or (root.parameters or {}).get('task_id') != task_id):
                partial = True
        seen_handoffs = 0
        order_failures = 0
        for row in rows:
            task = tasks[row['task_id']]
            root = flows.get(UUID(task['prefect_run_id'])) if task['prefect_run_id'] else None
            if (not root or not root.state or root.state.is_final() or root.state.is_cancelling()
                    or root.deployment_id != root_deployment.id or root.parent_task_run_id is not None
                    or root.idempotency_key != 'first-step-order-v2-' + row['task_id']
                    or (root.parameters or {}).get('task_id') != row['task_id']):
                partial = True
                continue
            if row['handoff'] and row['handoff_state'] == 'waiting' and row['claim_state'] == 'completed':
                seen_handoffs += 1
            if not row['prefect_run_id']:
                partial = True
                continue
            run = flows.get(UUID(row['prefect_run_id']))
            if (not run or not _result_folder_matches(run, str(root.id), row, deployment.id) or not run.state):
                partial = True
                continue
            item = {'task_id': row['task_id'], 'task_name': task['name'], 'group_id': row['group_id'],
                    'paper_id': row['paper_id'], 'receipt_id': row['receipt_id'],
                    'attempt_id': row['attempt_id'], 'folder_label': row['folder_label'],
                    'handoff': row['handoff']}
            if (row['group_state'] == 'ready_for_llm' and row['claim_state'] == 'completed'
                    and row['handoff_state'] == 'waiting' and row['handoff']):
                # Preprocessing is a completed stage even while this same
                # folder flow is running, paused, or failed in the LLM stage.
                try:
                    self._verified_gaps(item, previous_manifests, verified_manifests)
                    if run.state.is_failed() or run.state.is_crashed() or run.state.is_cancelled():
                        item['order_note'] = '前处理已交接；文件夹工单后续状态需核对'
                        order_failures += 1
                    buckets['passed'].append(item)
                except (OSError, ValueError, KeyError, TypeError):
                    partial = True
                    continue
                if llm_available:
                    try:
                        key = (row['task_id'], row['group_id'])
                        run_key = (row['task_id'], str(run.id))
                        seen_llm_runs.add(run_key)
                        entry = llm_rows.get(key)
                        if entry is not None:
                            seen_llm_keys.add(key)
                            if (entry['paper_id'] != row['paper_id'] or entry['receipt_id'] != row['receipt_id']
                                    or entry['attempt_id'] != row['attempt_id']
                                    or entry['parent_run_id'] != task['prefect_run_id']
                                    or entry['task_run_id'] != str(run.id)):
                                llm_partial = True
                                continue
                            phase = entry['state']
                        else:
                            phase = None
                        jobs = llm_jobs.get(key, [])
                        job = _effective_llm_job(jobs)
                        observed_state = run.state
                        if job and phase == 'waiting_postprocessing' and job['state'] in LLM_STAGE_UNKNOWN:
                            # Complete response evidence may coexist with an
                            # unresolved stage status; preserve both facts.
                            uncertain_runs.add(run_key)
                            llm_partial = True
                            item = dict(item, order_note='原始回复已完成登记；LLM 阶段工单状态待核对')
                        if job and phase != 'waiting_postprocessing':
                            if key in changed_jobs:
                                llm_partial = True
                                continue
                            if (job['paper_id'] != row['paper_id'] or job['receipt_id'] != row['receipt_id']
                                    or job['attempt_id'] != row['attempt_id']
                                    or job['parent_run_id'] != task['prefect_run_id']
                                    or job['folder_run_id'] != str(run.id)
                                    or job['task_key'] != LLM_STAGE_TASK_KEY
                                    or type(job['generation']) is not int or job['generation'] < 0
                                    or job['job_id'] != _llm_stage_id(str(run.id), job['generation'], job['retry_key'])):
                                llm_partial = True
                                continue
                            if job['state'] == 'recovering_folder':
                                # A real user request is restoring this same folder;
                                # no LLM TaskRun has been sent yet.
                                if not job['retry_key']:
                                    llm_partial = True
                                continue
                            if job['state'] in LLM_STAGE_UNKNOWN:
                                uncertain_runs.add(run_key)
                                llm_partial = True
                                continue
                            if job['state'] not in {'reserved', 'submitting', 'submitted', 'executing',
                                                    'finished', 'stopped', 'failed'}:
                                llm_partial = True
                                continue
                            terminal_retry = bool(job['retry_key'] and job['retry_item_state'] == 'submitted'
                                and (run.state.is_failed() or run.state.is_crashed()) and run.state.id
                                and job['retry_old_flow_state_id'] == str(run.state.id))
                            stage = stage_runs.get(job['job_id'])
                            if job['state'] == 'reserved':
                                # A local reservation is queued but has not yet
                                # created a Prefect TaskRun. Do not report it as
                                # a failed or unconfirmed model request.
                                if ((not terminal_retry and (not run.state.is_paused()
                                        or run.state.state_details.pause_reschedule is not True
                                        or not any((run.name or '').endswith(phase) for phase in
                                                   ('等待 LLM', '等待任务开始', 'LLM 失败待核对'))))
                                        or stage is not None):
                                    llm_partial = True
                                continue
                            if ((not terminal_retry and not run.state.is_paused()) or not stage or not stage.state
                                    or str(stage.id) != job['job_id'] or stage.flow_run_id is not None
                                    or stage.task_key != job['task_key']
                                    or stage.dynamic_key != 'paperflow-llm-' + job['job_id']
                                    or 'paperflow-llm-stage-v1' not in (stage.tags or [])):
                                llm_partial = True
                                continue
                            observed_state = stage.state
                            if job['state'] == 'failed' and not (
                                    observed_state.is_failed() or observed_state.is_crashed()):
                                llm_partial = True
                                continue
                        failed = observed_state.is_failed() or observed_state.is_crashed()
                        if phase == 'waiting_postprocessing':
                            manifest = self._llm_handoff(item, entry['manifest_sha256'],
                                self._llm_manifest_cache, current_llm_manifests)
                            # Check the same component identities and raw-complete
                            # records used when the folder was handed off. This
                            # does not re-read or parse response files.
                            proof = llm_contract.inspect_folder(
                                manifest, str(run.id), llm_admissions.get(run_key, []), lambda unused: None)
                            if proof['status'] == 'raw_complete':
                                llm_buckets['passed'].append(dict(item))
                            else:
                                llm_partial = True
                        elif phase in {'waiting_llm', 'llm_processing'} or (
                                phase is None and (job is not None or (run.name or '').endswith('LLM 处理中'))):
                            if failed:
                                admissions = llm_admissions.get(run_key, [])
                                unconfirmed = (run_key in changed_runs or run_key in uncertain_runs or
                                    any(a['state'] in {'admitted', 'returned_unverified', 'unknown'}
                                        for a in admissions))
                                if unconfirmed:
                                    uncertain_runs.add(run_key)
                                outcome = dict(item, request_unconfirmed=unconfirmed,
                                               reason=_llm_failure_reason(observed_state, unconfirmed))
                                llm_buckets['failed'].append(outcome)
                            elif observed_state.is_cancelled():
                                # Cancellation can be an operator action, not an LLM failure.
                                llm_partial = True
                            elif observed_state.is_completed():
                                # A finished run without a completed LLM ledger is ambiguous.
                                llm_partial = True
                            elif not job and phase == 'llm_processing' and run.state.is_paused() and (
                                    run.name or '').endswith('等待后处理'):
                                # The local snapshot and the Prefect state changed mid-read.
                                llm_partial = True
                        elif phase is not None:
                            llm_partial = True
                        elif failed or observed_state.is_cancelled():
                            # The handoff exists, but there is no proof which later stage failed.
                            llm_partial = True
                    except Exception:
                        # A damaged LLM snapshot must not hide the separately
                        # verified preprocessing result for the same folder.
                        llm_partial = True
            elif run.state.is_failed() or run.state.is_crashed() or run.state.is_cancelled():
                if row['claim_state'] == 'failed':
                    item['reason'] = '资料领取或核验失败'
                elif row['error_type']:
                    item['reason'] = '前处理异常：' + row['error_type']
                else:
                    item['reason'] = 'Prefect 工单执行失败或已取消'
                    order_failures += 1
                buckets['failed'].append(item)
            elif row['claim_state'] == 'failed' or row['error_type']:
                item['reason'] = ('资料领取或核验失败' if row['claim_state'] == 'failed'
                                  else '前处理异常：' + row['error_type'])
                buckets['failed'].append(item)
            elif row['group_state'] == 'ready_for_llm':
                # An incomplete or conflicting local handoff is not a pass.
                partial = True
        if snapshot.get('waiting_llm_total') != seen_handoffs:
            partial = True
        if any(key[0] in tasks and key not in seen_llm_keys for key in llm_rows):
            llm_partial = True
        self._result_manifest_cache = verified_manifests
        self._llm_manifest_cache = current_llm_manifests
        return {'as_of': as_of, 'partial': partial, 'order_failures': order_failures,
                'seen_handoffs': seen_handoffs, 'buckets': buckets,
                'llm_available': llm_available, 'llm_partial': llm_partial,
                'llm_buckets': llm_buckets,
                'llm_unconfirmed_folders': len(uncertain_runs & seen_llm_runs)}

    @staticmethod
    def _handoff_input_files(manifest):
        """Return only file labels from the already verified folder handoff."""
        body, attachments = manifest.get('body'), manifest.get('attachments')
        if not isinstance(body, dict) or not isinstance(attachments, list):
            raise ValueError('交接文件缺少正文或附件信息')

        def label(row, kind):
            if not isinstance(row, dict) or row.get('kind') != kind:
                raise ValueError('交接文件类型不一致')
            file_id = row.get('file_id')
            name = next((value.strip() for value in (row.get('relative'), row.get('display_name'), row.get('name'))
                         if isinstance(value, str) and value.strip()), None)
            if not isinstance(file_id, str) or not file_id or not name:
                raise ValueError('交接文件身份或名称不可用')
            return {'file_id': file_id, 'name': name}

        body_file = label(body, 'body')
        attachment_files = [label(row, 'attachment') for row in attachments]
        ids = [body_file['file_id']] + [row['file_id'] for row in attachment_files]
        if len(ids) != len(set(ids)):
            raise ValueError('交接文件身份重复')
        return {'body': [body_file], 'attachment': attachment_files}

    @staticmethod
    def _verified_gaps(item, previous=None, current=None, include_files=False):
        reference = json.loads(item['handoff'])
        path = Path(reference['manifest_path']).resolve(strict=True)
        stat = path.stat()
        identity = tuple(item[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'))
        cache_key = (str(path), reference['manifest_sha256'], stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns, identity)
        input_files = None
        if not include_files and previous is not None and cache_key in previous:
            gaps = previous[cache_key]
        else:
            raw = path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != reference['manifest_sha256']:
                raise ValueError('交接文件校验不一致')
            manifest = json.loads(raw)
            if not isinstance(manifest, dict):
                raise ValueError('交接文件格式无效')
            expected = dict(zip(('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'), identity))
            expected.update(consumer_id='prefect-local-preprocessing',
                            schema='paperflow-local-preprocessing-handoff/1')
            if any(manifest.get(key) != value for key, value in expected.items()):
                raise ValueError('交接文件身份不一致')
            gaps = manifest.get('gaps')
            if not isinstance(gaps, list):
                raise ValueError('交接文件缺少识别缺口记录')
            gaps = [gap for gap in gaps if isinstance(gap, dict)]
            if include_files:
                try:
                    input_files = FolderOrderService._handoff_input_files(manifest)
                except (ValueError, TypeError, KeyError):
                    # A missing label must not turn a known outcome into an
                    # empty folder, or hide the independently verified result.
                    input_files = None
        if current is not None:
            current[cache_key] = gaps
        return (gaps, input_files) if include_files else gaps

    def _result_item(self, item, *, include_files=False):
        """Recheck the selected list page's local gap details."""
        result = {key: value for key, value in item.items() if key != 'handoff'}
        if item.get('handoff'):
            try:
                if include_files:
                    result['gaps'], result['input_files'] = self._verified_gaps(item, include_files=True)
                else:
                    result['gaps'] = self._verified_gaps(item)
            except (OSError, ValueError, KeyError, TypeError):
                result['order_note'] = ((result.get('order_note') + '；') if result.get('order_note') else '') + '交接文件暂不可核验，识别缺口无法显示'
                result['handoff_unverified'] = True
        return result

    def _add_result_titles(self, items):
        """Reuse first-step titles for the selected page when identities match."""
        for item in items:
            item['title_status'] = '暂不可用'
        if not items:
            return
        clauses = ' OR '.join('(cg.task_id=? AND cg.group_id=?)' for _ in items)
        params = [value for item in items for value in (item['task_id'], item['group_id'])]
        try:
            with closing(self._control()) as db:
                # A busy title index must not hold the whole result page open.
                db.execute('PRAGMA busy_timeout=300')
                rows = db.execute(
                    'SELECT cg.task_id,cg.group_id,gr.payload FROM control_groups cg '
                    'JOIN group_results gr ON gr.task_id=cg.task_id AND gr.seq=cg.seq '
                    'WHERE ' + clauses, params).fetchall()
        except (OSError, sqlite3.Error, ValueError):
            return
        records = {}
        for row in rows:
            key = (row['task_id'], row['group_id'])
            records[key] = None if key in records else row['payload']
        for item in items:
            payload = records.get((item['task_id'], item['group_id']))
            try:
                record = json.loads(payload)
            except (TypeError, ValueError):
                continue
            if (not isinstance(record, dict) or
                    any(not item.get(key) or record.get(key) != item[key]
                        for key in ('group_id', 'paper_id', 'receipt_id')) or
                    record.get('status') != 'passed' or
                    record.get('registration_status') != 'registered' or
                    record.get('delivery_complete') is not True):
                continue
            status = record.get('title_status')
            title = record.get('paper_title')
            candidate = record.get('paper_title_candidate')
            if status == '已提取' and isinstance(title, str) and title.strip():
                item.update(title_status=status, paper_title=title.strip())
            elif status == '待核对' and isinstance(candidate, str) and candidate.strip():
                item.update(title_status=status, paper_title_candidate=candidate.strip())
            elif status == '未提取':
                item['title_status'] = status

    def preprocessing_results(self, snapshot, result_filter=None, page=1, page_size=20):
        unavailable = {'available': False, 'counts': {}, 'message': '前处理结果正在核对'}
        if snapshot.get('receiver_status') != 'running':
            return unavailable
        try:
            if not self._source_matches(snapshot):
                return unavailable
            tasks = self._result_tasks()
            key = self._results_key(snapshot, tasks)
        except Exception:
            return unavailable

        def refresh():
            value = None
            try:
                value = self._build_results(snapshot, tasks)
                current = self.queue.snapshot()
                if (current.get('receiver_status') != 'running' or not self._source_matches(current)
                        or self._results_key(current, self._result_tasks()) != key):
                    value = None
            except Exception:
                value = None
            self._results_cache = (key, time.monotonic(), value)
            self._results_inflight = False

        cached = self._results_cache
        age = time.monotonic() - cached[1] if cached and cached[0] == key else None
        if (age is None or age >= 10) and not self._results_inflight:
            self._results_inflight = True
            Thread(target=refresh, name='prefect-preprocessing-results', daemon=True).start()
        if age is None or age > 60 or cached[2] is None:
            return unavailable
        value = cached[2]
        partial = (value['partial'] or snapshot.get('upstream_scan_verified') is not True
                   or snapshot.get('waiting_llm_total') != value['seen_handoffs'])
        counts = {name: len(value['buckets'][name]) for name in ('passed', 'failed')}
        if partial:
            counts = {name: count if count else None for name, count in counts.items()}
        counts['unrecognized'] = None  # No whole-paper unrecognized outcome is recorded yet.
        result = {'available': True, 'counts': counts, 'partial': partial,
                  'order_failures': value['order_failures'], 'as_of': value['as_of']}
        if result_filter:
            if result_filter == 'unrecognized':
                result['list'] = {'filter': result_filter, 'page': page, 'total': None, 'items': [],
                                  'message': '前处理尚未记录整份资料的未识别结果，暂无法统计。局部缺口会列在已通过资料中。'}
            else:
                items = value['buckets'][result_filter]
                start = (page - 1) * page_size
                page_items = [self._result_item(item) for item in items[start:start + page_size]]
                if result_filter == 'passed' and any(item.get('handoff_unverified') for item in page_items):
                    self._results_cache = None
                    return {'available': False, 'counts': {}, 'message': '交接文件发生变化，结果正在重新核对'}
                self._add_result_titles(page_items)
                result['list'] = {'filter': result_filter, 'page': page, 'total': counts[result_filter],
                                  'items': page_items,
                                  'message': '结果尚在核对' if partial and not items else '暂无资料'}
        return result

    def llm_results(self, snapshot, result_filter=None, page=1, page_size=20):
        """Reuse the verified folder-order snapshot for LLM outcome display."""
        unavailable = {'available': False, 'counts': {}, 'message': 'LLM 结果正在核对'}
        if snapshot.get('receiver_status') != 'running':
            return unavailable
        try:
            if not self._source_matches(snapshot):
                return unavailable
            key = self._results_key(snapshot, self._result_tasks())
            cached = self._results_cache
            if not cached or cached[0] != key or time.monotonic() - cached[1] > 60 or cached[2] is None:
                return unavailable
            value = cached[2]
            if not value.get('llm_available'):
                return unavailable
            partial = (value['partial'] or value['llm_partial']
                       or snapshot.get('upstream_scan_verified') is not True
                       or snapshot.get('waiting_llm_total') != value['seen_handoffs'])
            buckets = value['llm_buckets']
            counts = {name: len(buckets[name]) for name in ('passed', 'failed')}
            if partial:
                counts = {name: count if count else None for name, count in counts.items()}
            # The LLM stage deliberately does not parse its replies.
            counts['unrecognized'] = None
            unconfirmed = value['llm_unconfirmed_folders']
            result = {'available': True, 'counts': counts, 'partial': partial,
                      'unconfirmed_folders': unconfirmed if not partial or unconfirmed else None,
                      'as_of': value['as_of']}
            if result_filter:
                if result_filter == 'unrecognized':
                    result['list'] = {'filter': result_filter, 'page': page, 'total': None, 'items': [],
                        'message': '当前阶段只保存模型原始回复，未解析内容，暂无整份资料的未识别结果。'}
                else:
                    items = buckets[result_filter]
                    start = (page - 1) * page_size
                    page_items = [self._result_item(item, include_files=True) for item in items[start:start + page_size]]
                    if any(item.get('handoff_unverified') for item in page_items):
                        self._results_cache = None
                        return unavailable
                    self._add_result_titles(page_items)
                    result['list'] = {'filter': result_filter, 'page': page, 'total': counts[result_filter],
                                      'items': page_items,
                                      'message': '结果尚在核对' if partial and not items else '暂无资料'}
            return result
        except (OSError, sqlite3.Error, ValueError, KeyError, TypeError):
            return unavailable
