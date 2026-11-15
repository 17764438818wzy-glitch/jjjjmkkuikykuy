"""Create and observe folder orders beneath the matching first-step flow run."""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from threading import Thread
import time
from uuid import NAMESPACE_URL, UUID, uuid5

POOL = 'paperflow-unified-preprocessing'
DEPLOYMENT = '资料文件夹工单/前处理'
MARKER = 'paperflow-order-v2-live-20261031'


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


def _parent_id(root_run_id, task_id, group_id):
    return uuid5(NAMESPACE_URL, 'paperflow-folder-parent/2/' + str(root_run_id) + '/' + task_id + '/' + group_id)


def _chunks(items, size=100):
    for start in range(0, len(items), size):
        yield items[start:start + size]


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
            task = db.execute('SELECT task_id,name,prefect_run_id,desired_state,start_pending,prefect_order_model '
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
        task = self._task(task_id)
        try:
            queue = client.read_work_queue_by_name('task-' + task_id, work_pool_name=POOL)
        except ObjectNotFound:
            return
        paused = task['desired_state'] != 'running' or bool(task['start_pending'])
        if queue.is_paused != paused:
            client.update_work_queue(queue.id, is_paused=paused)

    @staticmethod
    def _placeholder(client, root_id, task_id, group_id):
        from prefect import task
        from prefect.exceptions import ObjectNotFound
        from prefect.states import Pending

        parent_id = _parent_id(root_id, task_id, group_id)
        try:
            parent = client.read_task_run(parent_id)
        except ObjectNotFound:
            placeholder = task(name='资料文件夹工单', retries=0)(lambda: None)
            placeholder.task_key = 'paperflow-folder-waiting-v2'
            try:
                parent = client.create_task_run(placeholder, flow_run_id=UUID(root_id),
                    dynamic_key=group_id, id=parent_id, name='资料文件夹 ' + group_id, state=Pending())
            except Exception:
                # An interrupted creation may already have succeeded.
                parent = client.read_task_run(parent_id)
        if parent.id != parent_id or str(parent.flow_run_id) != root_id:
            raise ValueError('文件夹工单的父任务身份不匹配')
        return parent_id

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
            parent_id = self._placeholder(client, root_id, task_id, group_id)
            run = client.create_flow_run_from_deployment(
                deployment.id, parameters=params, parent_task_run_id=parent_id,
                idempotency_key='folder-waiting-v2-' + task_id + '-' + group_id,
                work_queue_name=queue_name, name='资料文件夹 ' + order['folder_label'] + ' · 等待前处理',
                state=Scheduled(name='等待绑定', scheduled_time=datetime.now(timezone.utc)))
            if (run.deployment_id != deployment.id or run.parent_task_run_id != parent_id
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
                    root = roots[row['task_id']]
                    parent_id = _parent_id(str(root.id), row['task_id'], row['group_id'])
                    if (run.deployment_id != folder_deployment.id or run.parent_task_run_id != parent_id
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
                 WHERE o.prefect_run_id IS NOT NULL AND g.state IN ('waiting','processing')
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
                    parent_id = _parent_id(str(root_id), item['task_id'], item['group_id'])
                    if (not root or not root.state or root.state.is_final() or root.state.is_cancelling()
                            or root.idempotency_key != 'first-step-order-v2-' + item['task_id']
                            or (root.parameters or {}).get('task_id') != item['task_id']
                            or not run or run.deployment_id != deployment.id or run.parent_task_run_id != parent_id
                            or run.idempotency_key != 'folder-waiting-v2-' + item['task_id'] + '-' + item['group_id']
                            or run.work_queue_name != 'task-' + item['task_id']
                            or any((run.parameters or {}).get(k) != item[k]
                                   for k in ('task_id', 'group_id', 'receipt_id', 'attempt_id'))):
                        raise ValueError('文件夹工单身份无法核对')
                    if (not run.state or not run.state.is_paused() or task['desired_state'] != 'running'
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
            tasks = {row[0] for row in control.execute(
                "SELECT task_id FROM tasks WHERE prefect_order_model='v2' AND prefect_run_id IS NOT NULL")}
        as_of = datetime.now(timezone.utc).isoformat()
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
        numbers = {'waiting': 0, 'processing': 0, 'ready_for_llm': 0}
        for row in rows:
            if row['task_id'] in tasks:
                numbers[row['state']] += row['number']
        counts = {
            'waiting_preprocessing': numbers['waiting'],
            'preprocessing_in_progress': numbers['processing'],
            'waiting_llm': numbers['ready_for_llm'],
        }
        complete = (snapshot.get('upstream_scan_verified') is True
                    and snapshot.get('waiting_count') == numbers['waiting']
                    and snapshot.get('in_progress_count') == numbers['processing']
                    and snapshot.get('waiting_llm_total') == numbers['ready_for_llm'])
        # An interrupted worker's database claim is not necessarily still executing.
        if snapshot.get('in_progress_count') != numbers['processing']:
            counts.pop('preprocessing_in_progress')
        # An incomplete scan can establish positive, locally registered orders;
        # it cannot establish that an empty queue is truly empty.
        if not complete:
            counts = {key: value for key, value in counts.items() if value > 0}
        with self._client() as client:
            self._safe_deployment(client)
        return {'source': 'prefect', 'scope': 'all_tasks', 'available': True,
                'basis': 'local_registered_orders', 'as_of': as_of,
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
            if result.get('available') and snapshot.get('upstream_scan_verified') is not True:
                result['partial'] = True
                result['counts'] = {key: value for key, value in result.get('counts', {}).items() if value > 0}
            elif result.get('available'):
                current = {'waiting_preprocessing': snapshot.get('waiting_count'),
                           'preprocessing_in_progress': snapshot.get('in_progress_count'),
                           'waiting_llm': snapshot.get('waiting_llm_total')}
                counts = dict(result.get('counts', {}))
                for stage, value in list(counts.items()):
                    if value == 0 and current.get(stage) != 0:
                        counts.pop(stage)
                        result['partial'] = True
                result['counts'] = counts
            return result
        if not self._summary_inflight:
            self._summary_inflight = True
            Thread(target=refresh, name='prefect-waiting-summary', daemon=True).start()
        return unavailable
