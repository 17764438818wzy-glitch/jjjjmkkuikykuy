"""Read-only identity check for the actual folder FlowRun and upstream handoff.

Call while holding the same short task gate used by pause and deletion. This
does not create or schedule a Prefect run, send a model request, or alter the
upstream queue. Admission and source history must still be checked at send.
"""
from contextlib import closing
import json
from pathlib import Path
import sqlite3
from uuid import UUID, uuid5

from .handoff_contract import IDENTITY_KEYS, load_frozen_handoff
MARKER = 'paperflow-order-v2-live-20261031'
_STAGE_NAMESPACE = UUID('6aaf059a-c89a-52a8-acd5-4a928260a3b7')
_STAGE_KEY = 'paperflow-folder-llm-stage-v1'


class UpstreamFlowBinding:
    def __init__(self, queue_database, control_database, client, expected_api_url,
                 retention_consumer, verify_registered_receipt, receiver_session=None,
                 execution_task_run_id=None, execution_task_key=None,
                 allow_terminal_retry=False, allow_completed_retry=False):
        self.queue_database = Path(queue_database).resolve(strict=True)
        self.control_database = Path(control_database).resolve(strict=True)
        self.client = client
        self.api_url = expected_api_url
        self.retention_consumer = retention_consumer
        self.verify_receipt = verify_registered_receipt
        if receiver_session is None:
            from local_queue import _current_receiver_session
            receiver_session = _current_receiver_session
        self.receiver_session = receiver_session
        self.execution_task_run_id = execution_task_run_id
        self.execution_task_key = execution_task_key
        self.allow_terminal_retry = allow_terminal_retry is True
        self.allow_completed_retry = allow_completed_retry is True
        if bool(execution_task_run_id) != bool(execution_task_key):
            raise ValueError('LLM 阶段执行身份不完整')
        if (self.allow_terminal_retry and not execution_task_run_id) or (
                self.allow_completed_retry and not self.allow_terminal_retry):
            raise ValueError('人工重试缺少真实 LLM 阶段身份')
        if (not isinstance(expected_api_url, str) or not expected_api_url
                or not isinstance(retention_consumer, str) or not retention_consumer
                or not callable(verify_registered_receipt) or not callable(receiver_session)):
            raise ValueError('新工单核验配置无效')

    @staticmethod
    def _read_database(path):
        db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)
        db.row_factory = sqlite3.Row
        return db

    def __call__(self, identity):
        return self.load(identity)['manifest']

    def for_execution(self, identity):
        return self.load(identity, require_running=True)['manifest']

    def _manual_retry(self, identity, folder, require_running):
        """Check an explicit retry without rewriting the old FlowRun state.

        This is a candidate-only path: a finished or cancelled historical run
        is never made to look suspended. A retry has to retain the same folder
        and the exact failed state observed at reservation time.
        """
        if not self.allow_terminal_retry:
            return False
        if not (folder.state.is_failed() or folder.state.is_crashed() or folder.state.is_paused()):
            raise ValueError('人工重试的原文件夹状态已变化')
        with closing(self._read_database(self.control_database)) as db:
            row = db.execute(
                'SELECT j.*,i.old_flow_state_id,i.state AS item_state '
                'FROM paperflow_llm_stage_jobs j JOIN paperflow_llm_retry_items i '
                'ON i.request_id=j.retry_key AND i.job_id=j.job_id '
                'AND i.task_id=j.task_id AND i.group_id=j.group_id '
                'JOIN paperflow_llm_retry_commands c ON c.request_id=i.request_id '
                'WHERE j.job_id=?', (self.execution_task_run_id,)).fetchone()
        if not row:
            raise ValueError('人工重试预约不可核验')
        item = dict(row)
        try:
            retry_key = str(UUID(item['retry_key']))
            folder_id = str(UUID(identity['task_run_id']))
            expected = str(uuid5(_STAGE_NAMESPACE,
                folder_id + ':' + str(item['generation']) + ':manual:' + retry_key))
            old_state_id = str(UUID(item['old_flow_state_id']))
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError('人工重试预约身份无效') from exc
        if (retry_key != item['retry_key'] or expected != self.execution_task_run_id
                or item['job_id'] != expected or item['task_key'] != _STAGE_KEY
                or self.execution_task_key != _STAGE_KEY
                or item['folder_run_id'] != identity['task_run_id']
                or any(item[key] != identity[key] for key in
                       ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id'))
                or not folder.state.id or str(folder.state.id) != old_state_id):
            raise ValueError('人工重试与原文件夹失败工单不一致')
        allowed = {'recovering_folder': {'recovering', 'recovered'},
                   'reserved': {'submitted'}, 'submitting': {'submitted'},
                   'submitted': {'submitted'}, 'executing': {'submitted'}}
        if self.allow_completed_retry and not require_running:
            allowed['finished'] = {'submitted'}
            if item['state'] != 'finished':
                raise ValueError('人工重试阶段尚未完成')
        if item['item_state'] not in allowed.get(item['state'], set()):
            raise ValueError('人工重试预约状态不可核验')
        if require_running and item['state'] != 'executing':
            raise InterruptedError('人工重试阶段尚未执行')
        if item['state'] == 'finished':
            stage = self.client.read_task_run(UUID(item['job_id']))
            if (str(stage.id) != item['job_id'] or stage.flow_run_id is not None
                    or stage.task_key != _STAGE_KEY
                    or stage.dynamic_key != 'paperflow-llm-' + item['job_id']
                    or 'paperflow-llm-stage-v1' not in (stage.tags or [])
                    or not stage.state or not stage.state.is_completed()):
                raise ValueError('已完成的人工重试阶段不可核验')
        return True

    def load(self, identity, *, require_running=False):
        from .folder_ledger import _identity
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
            session = self.receiver_session(db)
            if db.execute('SELECT 1 FROM cancelled_tasks WHERE task_id=?', (identity['task_id'],)).fetchone():
                raise ValueError('上游任务已删除')
            ticket = db.execute('SELECT receipt_id,attempt_id,prefect_run_id FROM preprocessing_folder_orders '
                                'WHERE task_id=? AND group_id=?',
                                (identity['task_id'], identity['group_id'])).fetchone()
            # The legacy ledger column is called task_run_id, but for the
            # already-running design it stores the actual folder FlowRun id.
            if (not ticket or ticket['receipt_id'] != identity['receipt_id']
                    or ticket['attempt_id'] != identity['attempt_id']
                    or ticket['prefect_run_id'] != identity['task_run_id']):
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
            folder_id = UUID(identity['task_run_id'])
            parent = self.client.read_flow_run(parent_id)
            if (parent.id != parent_id or not parent.deployment_id or parent.parent_task_run_id is not None
                    or not parent.state or parent.state.is_final() or parent.state.is_cancelling()
                    or parent.idempotency_key != 'first-step-order-v2-' + identity['task_id']
                    or (parent.parameters or {}).get('task_id') != identity['task_id']):
                raise ValueError('新 Prefect 大工单身份不可核验')
            deployment = self.client.read_deployment(parent.deployment_id)
            if MARKER not in (deployment.tags or []):
                raise ValueError('新 Prefect 大工单部署不可核验')
            folder = self.client.read_flow_run(folder_id)
            manual_retry = bool(folder.state and self._manual_retry(identity, folder, require_running))
            terminal_retry = bool(manual_retry and (folder.state.is_failed() or folder.state.is_crashed()))
            if (folder.id != folder_id or not folder.deployment_id or folder.parent_task_run_id is not None
                    or folder.idempotency_key != 'folder-waiting-v2-' + identity['task_id'] + '-' + identity['group_id']
                    or folder.work_queue_name != 'task-' + identity['task_id']
                    or any((folder.parameters or {}).get(key) != identity[key]
                           for key in ('task_id', 'group_id', 'receipt_id', 'attempt_id'))
                    or not folder.state or (folder.state.is_final() and not terminal_retry)
                    or folder.state.is_cancelling()):
                raise ValueError('文件夹工单身份不可核验')
            folder_deployment = self.client.read_deployment(folder.deployment_id)
            if (MARKER not in (folder_deployment.tags or [])
                    or folder_deployment.work_pool_name != 'paperflow-unified-preprocessing'):
                raise ValueError('新 Prefect 文件夹部署不可核验')
            # The parent is intentionally suspended; this independently
            # running real folder flow is controlled by the task's own gate.
            if require_running and self.execution_task_run_id is None:
                if not folder.state.is_running():
                    raise InterruptedError('文件夹工单当前未运行，停止处理')
            elif require_running:
                stage_id = UUID(self.execution_task_run_id)
                stage = self.client.read_task_run(stage_id)
                if ((not terminal_retry and (not folder.state.is_paused()
                        or folder.state.state_details.pause_reschedule is not True
                        or not any((folder.name or '').endswith(name)
                                   for name in ('等待 LLM', '等待任务开始', 'LLM 处理中', 'LLM 失败待核对'))))
                        or stage.id != stage_id or stage.flow_run_id is not None
                        or stage.task_key != self.execution_task_key or not stage.state
                        or not stage.state.is_running()
                        or (terminal_retry and stage.dynamic_key != 'paperflow-llm-' + str(stage_id))
                        or 'paperflow-llm-stage-v1' not in (stage.tags or [])):
                    raise InterruptedError('文件夹工单或 LLM 阶段执行状态不可核验')
        except (AttributeError, TypeError) as exc:
            raise ValueError('新 Prefect 工单不可核验') from exc
        if not isinstance(handoff, dict):
            raise ValueError('上游交接引用不可核验')
        record = {**{key: identity[key] for key in IDENTITY_KEYS}, **handoff}
        return load_frozen_handoff(record, {key: identity[key] for key in IDENTITY_KEYS}, self.verify_receipt)
