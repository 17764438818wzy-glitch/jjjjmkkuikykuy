"""Read-only identity check for the new folder TaskRun and upstream handoff.

Call while holding the same short task gate used by pause and deletion. This
does not create or schedule a Prefect run, send a model request, or alter the
upstream queue. Admission and source history must still be checked at send.
"""
from contextlib import closing
import json
from pathlib import Path
import sqlite3
from uuid import NAMESPACE_URL, UUID, uuid5

from handoff_contract import IDENTITY_KEYS, load_frozen_handoff
from local_queue import _current_receiver_session
from native_order import MARKER


class UpstreamBinding:
    def __init__(self, queue_database, control_database, client, expected_api_url,
                 task_key, retention_consumer, verify_registered_receipt):
        self.queue_database = Path(queue_database).resolve(strict=True)
        self.control_database = Path(control_database).resolve(strict=True)
        self.client = client
        self.api_url = expected_api_url
        self.task_key = task_key
        self.retention_consumer = retention_consumer
        self.verify_receipt = verify_registered_receipt
        if (not isinstance(expected_api_url, str) or not expected_api_url
                or not isinstance(task_key, str) or not task_key or task_key == 'paperflow-folder-waiting-v2'
                or not isinstance(retention_consumer, str) or not retention_consumer
                or not callable(verify_registered_receipt)):
            raise ValueError('新工单核验配置无效')

    @staticmethod
    def _read_database(path):
        db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)
        db.row_factory = sqlite3.Row
        return db

    def __call__(self, identity):
        return self._verify(identity, require_running=False)

    def for_execution(self, identity):
        """Starting or advancing work requires both parent and folder running."""
        return self._verify(identity, require_running=True)

    def _verify(self, identity, *, require_running):
        from folder_ledger import _identity
        identity = _identity(identity)
        if str(getattr(self.client, 'api_url', '')).rstrip('/') != self.api_url.rstrip('/'):
            raise ValueError('指定的新 Prefect 服务不一致')
        with closing(self._read_database(self.control_database)) as db:
            db.execute('BEGIN')
            task = db.execute('SELECT prefect_run_id,prefect_order_model FROM tasks WHERE task_id=?',
                              (identity['task_id'],)).fetchone()
            deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (identity['task_id'],)).fetchone()
            if (not task or deleted or task['prefect_order_model'] != 'v2'
                    or task['prefect_run_id'] != identity['parent_run_id']):
                raise ValueError('任务与新 Prefect 大工单不一致')
        with closing(self._read_database(self.queue_database)) as db:
            db.execute('BEGIN')
            schema = db.execute("SELECT value FROM metadata WHERE key='queue_schema'").fetchone()
            if not schema or schema[0] != 'paperflow-local-waiting/1':
                raise ValueError('上游队列身份不可核验')
            session = _current_receiver_session(db)
            if db.execute('SELECT 1 FROM cancelled_tasks WHERE task_id=?', (identity['task_id'],)).fetchone():
                raise ValueError('上游任务已删除')
            ticket = db.execute('SELECT * FROM native_folder_tickets WHERE task_id=? AND group_id=?',
                                (identity['task_id'], identity['group_id'])).fetchone()
            keys = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')
            if not ticket or ticket['bound'] != 1 or any(ticket[key] != identity[key] for key in keys):
                raise ValueError('文件夹工单绑定身份不一致')
            row = db.execute("SELECT h.handoff FROM preprocessing_handoffs h JOIN groups g "
                 "ON g.task_id=h.task_id AND g.group_id=h.group_id AND g.paper_id=h.paper_id "
                 "AND g.receipt_id=h.receipt_id JOIN claims c ON c.consumer_id=h.consumer_id "
                 "AND c.attempt_id=h.attempt_id AND c.task_id=h.task_id AND c.group_id=h.group_id "
                 "AND c.receipt_id=h.receipt_id WHERE h.task_id=? AND h.group_id=? AND h.paper_id=? "
                 "AND h.receipt_id=? AND h.attempt_id=? AND h.consumer_id='prefect-local-preprocessing' "
                 "AND h.state='waiting' AND g.state='ready_for_llm' AND c.state='completed' "
                 "AND g.checked_session=? AND EXISTS(SELECT 1 FROM retention_requests r "
                 "WHERE r.task_id=g.task_id AND r.group_id=g.group_id AND r.paper_id=g.paper_id "
                 "AND r.receipt_id=g.receipt_id AND r.consumer_id=? AND r.state='accepted' "
                 "AND r.retention_id IS NOT NULL AND r.last_error IS NULL AND r.checked_session=?)",
                 (identity['task_id'], identity['group_id'], identity['paper_id'], identity['receipt_id'],
                  identity['attempt_id'], session, self.retention_consumer, session)).fetchone()
            if not row:
                raise ValueError('上游交接、领取或保留状态不可核验')
            try:
                handoff = json.loads(row['handoff'])
            except (TypeError, ValueError) as exc:
                raise ValueError('上游交接引用不可核验') from exc
        try:
            parent_id = UUID(identity['parent_run_id'])
            expected_task_id = uuid5(NAMESPACE_URL, 'paperflow-native-folder-ticket/1/' + str(parent_id) +
                                     '/' + identity['group_id'])
            if str(expected_task_id) != identity['task_run_id']:
                raise ValueError('文件夹工单编号不一致')
            parent = self.client.read_flow_run(parent_id)
            if (parent.id != parent_id or not parent.deployment_id or parent.parent_task_run_id is not None
                    or not parent.state or parent.state.is_final() or parent.state.is_cancelling()
                    or parent.idempotency_key != 'first-step-order-v2-' + identity['task_id']
                    or (parent.parameters or {}).get('task_id') != identity['task_id']):
                raise ValueError('新 Prefect 大工单身份不可核验')
            deployment = self.client.read_deployment(parent.deployment_id)
            if MARKER not in (deployment.tags or []):
                raise ValueError('新 Prefect 大工单部署不可核验')
            task_run = self.client.read_task_run(expected_task_id)
            if (task_run.id != expected_task_id or task_run.flow_run_id != parent_id
                    or task_run.dynamic_key != identity['group_id'] or task_run.task_key != self.task_key
                    or not task_run.state or task_run.state.is_final() or task_run.state.is_cancelling()):
                raise ValueError('文件夹工单身份不可核验')
            if require_running and (not parent.state.is_running() or not task_run.state.is_running()):
                raise InterruptedError('大工单或文件夹工单当前未运行，停止处理')
        except (AttributeError, TypeError) as exc:
            raise ValueError('新 Prefect 工单不可核验') from exc
        if not isinstance(handoff, dict):
            raise ValueError('上游交接引用不可核验')
        record = {**{key: identity[key] for key in IDENTITY_KEYS}, **handoff}
        verified = load_frozen_handoff(record, {key: identity[key] for key in IDENTITY_KEYS}, self.verify_receipt)
        return verified['manifest']
