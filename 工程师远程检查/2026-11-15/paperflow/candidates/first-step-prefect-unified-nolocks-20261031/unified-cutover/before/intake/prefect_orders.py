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

POOL = 'paperflow-waiting-folder-v2-live-20261031'
DEPLOYMENT = '资料文件夹工单-等待前处理/等待前处理'


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
    def __init__(self, queue, *, prefect_api_url, first_step_index, first_step_data_root):
        self.queue = queue
        self.api_url = prefect_api_url.rstrip('/')
        self.index = Path(first_step_index).resolve()
        self.data_root = Path(first_step_data_root).resolve()
        self._summary_inflight = False
        self._summary_cache = None

    def _client(self):
        from prefect.client.orchestration import SyncPrefectClient
        return SyncPrefectClient(self.api_url, httpx_settings={'timeout': 3, 'trust_env': False})

    def _control(self):
        if not self.index.is_file():
            raise ValueError('上游任务索引不可用')
        db = sqlite3.connect(self.index.as_uri() + '?mode=ro', uri=True, timeout=5)
        db.row_factory = sqlite3.Row
        return db

    def _eligible(self, task_id=None, group_id=None):
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
                   AND NOT EXISTS(SELECT 1 FROM claims c WHERE c.task_id=g.task_id
                                  AND c.group_id=g.group_id AND c.receipt_id=g.receipt_id)
            """
            params = [session[0], self.queue.consumer_id, session[0]]
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
        marker = 'paperflow-order-v2-live-20261031'
        if deployment.work_pool_name != POOL or marker not in deployment.tags:
            raise OrderServiceUnavailable('等待工单部署未绑定专用工作池')
        if not pool.is_paused or pool.description != marker:
            raise OrderServiceUnavailable('等待工单工作池必须保持暂停')
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
        if not result.is_paused:
            raise ValueError('任务等待队列未暂停')
        return name

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
        # The child stays in the paused waiting pool and cannot start work.
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
                work_queue_name=queue_name, name='资料文件夹 ' + order['folder_label'] + ' · 等待前处理')
            if (run.deployment_id != deployment.id or run.parent_task_run_id != parent_id
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
        return None if cancelled else str(run.id)

    def sync_existing(self, *, stop_event=None):
        failures = 0
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
                    break
        return failures

    def _signature(self, snapshot):
        items = self._eligible()
        if snapshot.get('waiting_count') != len(items) or any(not item['prefect_run_id'] for item in items):
            raise ValueError('文件夹工单尚未全部核对')
        tasks = {task_id: self._task(task_id) for task_id in {item['task_id'] for item in items}}
        return (tuple((item['task_id'], item['group_id'], item['receipt_id'], item['attempt_id'],
                       item['prefect_run_id']) for item in items),
                tuple(sorted((key, value['prefect_run_id']) for key, value in tasks.items())))

    def summary(self, snapshot):
        """Return unavailable while a background Prefect read is pending."""
        unavailable = {'source': 'prefect', 'scope': 'all_tasks', 'available': False, 'counts': {}}
        if not snapshot.get('waiting_available') or snapshot.get('receiver_status') != 'running':
            return unavailable
        try:
            signature = self._signature(snapshot)
        except Exception:
            return unavailable

        def refresh():
            value = self._compute_summary(snapshot)
            try:
                current = self.queue.snapshot(include_in_progress=False)
                if (not current.get('waiting_available') or current.get('receiver_status') != 'running'
                        or self._signature(current) != signature):
                    value = unavailable
            except Exception:
                value = unavailable
            self._summary_cache = (signature, time.monotonic(), value)
            self._summary_inflight = False

        cached = self._summary_cache
        if cached and cached[0] == signature and time.monotonic() - cached[1] <= 8:
            return cached[2]
        if not self._summary_inflight:
            self._summary_inflight = True
            Thread(target=refresh, name='prefect-waiting-summary', daemon=True).start()
        return unavailable

    def _compute_summary(self, snapshot):
        """Count only runs whose local, parent, and Prefect identities agree."""
        unavailable = {'source': 'prefect', 'scope': 'all_tasks', 'available': False, 'counts': {}}
        try:
            items = self._eligible()
            if snapshot['waiting_count'] != len(items) or any(not item['prefect_run_id'] for item in items):
                return unavailable
            tasks = {task_id: self._task(task_id) for task_id in {item['task_id'] for item in items}}
            if any(not task['prefect_run_id'] for task in tasks.values()):
                return unavailable
            from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId, TaskRunFilter, TaskRunFilterId
            flow_ids = [UUID(item['prefect_run_id']) for item in items]
            root_ids = [UUID(task['prefect_run_id']) for task in tasks.values()]
            parent_ids = [_parent_id(tasks[item['task_id']]['prefect_run_id'], item['task_id'], item['group_id'])
                          for item in items]
            if len(set(flow_ids)) != len(flow_ids) or len(set(parent_ids)) != len(parent_ids):
                return unavailable
            with self._client() as client:
                deployment = self._safe_deployment(client)
                flows, parents = {}, {}
                for chunk in _chunks(flow_ids + root_ids):
                    for run in client.read_flow_runs(flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=chunk)),
                                                     limit=len(chunk)):
                        flows[run.id] = run
                for chunk in _chunks(parent_ids):
                    for run in client.read_task_runs(task_run_filter=TaskRunFilter(id=TaskRunFilterId(any_=chunk)),
                                                     limit=len(chunk)):
                        parents[run.id] = run
                for item, run_id, parent_id in zip(items, flow_ids, parent_ids):
                    run = flows.get(run_id)
                    parent = parents.get(parent_id)
                    root_id = UUID(tasks[item['task_id']]['prefect_run_id'])
                    root = flows.get(root_id)
                    if (not run or not parent or not root or not root.state or root.state.is_final()
                            or root.state.is_cancelling() or parent.flow_run_id != root_id
                            or root.idempotency_key != 'first-step-order-v2-' + item['task_id']
                            or (root.parameters or {}).get('task_id') != item['task_id']
                            or run.deployment_id != deployment.id or run.parent_task_run_id != parent_id
                            or run.work_queue_name != 'task-' + item['task_id']
                            or not run.state or not run.state.is_scheduled()
                            or any((run.parameters or {}).get(k) != item[k]
                                   for k in ('task_id', 'group_id', 'receipt_id', 'attempt_id'))):
                        return unavailable
            return {'source': 'prefect', 'scope': 'all_tasks', 'available': True,
                    'counts': {'waiting_preprocessing': len(items)}}
        except Exception:
            return unavailable
