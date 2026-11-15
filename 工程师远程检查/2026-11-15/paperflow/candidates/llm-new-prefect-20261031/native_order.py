"""Candidate real folder TaskRuns under the existing new Prefect parent.

No flow is created for a folder and no task-id tag is used for parentage.
The caller must first verify the folder with the trusted upstream queue and
must separately enforce task pause and deletion at every execution boundary.
"""
from contextlib import closing
from pathlib import Path
import re
import sqlite3
from uuid import NAMESPACE_URL, UUID, uuid5

from prefect.cache_policies import DEFAULT, NO_CACHE
from prefect.exceptions import ObjectNotFound
from prefect.states import Pending


MARKER = 'paperflow-order-v2-live-20261031'


class TicketStore:
    def __init__(self, path):
        self.path = Path(path).resolve(strict=True)

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    def initialize(self):
        with closing(self._connect()) as db, db:
            db.execute('''CREATE TABLE IF NOT EXISTS native_folder_tickets (
                task_id TEXT NOT NULL, group_id TEXT NOT NULL, paper_id TEXT NOT NULL,
                receipt_id TEXT NOT NULL, attempt_id TEXT NOT NULL, parent_run_id TEXT NOT NULL,
                task_run_id TEXT NOT NULL, bound INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(task_id, group_id), UNIQUE(task_run_id)
            )''')

    def reserve(self, values):
        columns = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT OR IGNORE INTO native_folder_tickets '
                       '(task_id,group_id,paper_id,receipt_id,attempt_id,parent_run_id,task_run_id) '
                       'VALUES(?,?,?,?,?,?,?)', tuple(values[k] for k in columns))
            row = db.execute('SELECT * FROM native_folder_tickets WHERE task_id=? AND group_id=?',
                             (values['task_id'], values['group_id'])).fetchone()
            if not row or any(row[k] != values[k] for k in columns):
                raise ValueError('已有文件夹工单身份不一致')
        return dict(row)

    def mark_bound(self, values):
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            result = db.execute('UPDATE native_folder_tickets SET bound=1 '
                                'WHERE task_id=? AND group_id=? AND receipt_id=? AND attempt_id=? '
                                'AND parent_run_id=? AND task_run_id=?',
                                tuple(values[k] for k in ('task_id', 'group_id', 'receipt_id', 'attempt_id',
                                                          'parent_run_id', 'task_run_id')))
            if result.rowcount != 1:
                raise ValueError('文件夹工单绑定身份发生变化')


def ensure_ticket(client, store, parent_run_id, order, executable_task):
    """Create one actual executable TaskRun per verified upstream folder."""
    if not isinstance(order, dict):
        raise ValueError('文件夹工单身份无效')
    task_id = str(UUID(order.get('task_id', '')))
    group_id, receipt_id, attempt_id = (order.get(k) for k in ('group_id', 'receipt_id', 'attempt_id'))
    if (not re.fullmatch(r'[0-9a-f]{32}', str(group_id or ''))
            or not re.fullmatch(r'[0-9a-f]{64}', str(receipt_id or ''))
            or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', str(attempt_id or ''))
            or not isinstance(order.get('paper_id'), str) or not order['paper_id']):
        raise ValueError('文件夹工单身份无效')
    if (not callable(getattr(executable_task, 'fn', None))
            or executable_task.task_key == 'paperflow-folder-waiting-v2'
            or executable_task.retries != 0
            or executable_task.with_options(persist_result=True, cache_policy=DEFAULT).cache_policy != NO_CACHE):
        raise ValueError('文件夹工单缺少禁止缓存和自动重试的真实处理任务')
    parent_id = UUID(str(parent_run_id))
    parent = client.read_flow_run(parent_id)
    if (not parent.deployment_id or parent.parent_task_run_id is not None or not parent.state
            or parent.state.is_final() or parent.state.is_cancelling()
            or parent.idempotency_key != 'first-step-order-v2-' + task_id
            or (parent.parameters or {}).get('task_id') != task_id):
        raise ValueError('新 Prefect 大工单身份不可核验')
    deployment = client.read_deployment(parent.deployment_id)
    if MARKER not in (deployment.tags or []):
        raise ValueError('新 Prefect 大工单部署身份不可核验')
    ticket_id = uuid5(NAMESPACE_URL, 'paperflow-native-folder-ticket/1/' + str(parent_id) + '/' + group_id)
    values = {**{k: order[k] for k in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
              'parent_run_id': str(parent_id), 'task_run_id': str(ticket_id)}
    store.reserve(values)
    try:
        run = client.read_task_run(ticket_id)
    except ObjectNotFound:
        try:
            run = client.create_task_run(executable_task, flow_run_id=parent_id, id=ticket_id,
                                         dynamic_key=group_id, name='资料文件夹 ' + group_id,
                                         state=Pending(name='等待前处理'))
        except Exception:
            # The create result may be unknown. Only read back this reserved id;
            # do not create a new id or assume the first request failed.
            run = client.read_task_run(ticket_id)
    if (run.id != ticket_id or run.flow_run_id != parent_id or run.dynamic_key != group_id
            or run.task_key != executable_task.task_key or not run.state):
        raise ValueError('Prefect 文件夹工单身份不一致')
    store.mark_bound(values)
    return run
