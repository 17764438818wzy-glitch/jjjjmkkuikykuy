"""Restore one original folder FlowRun to a verified suspension, without LLM work."""
from contextlib import closing
import asyncio
import hashlib
import json
import re
import sqlite3
import time
from uuid import UUID, uuid4

from filelock import FileLock, Timeout
from prefect.client.orchestration import get_client
from prefect.context import FlowRunContext
from prefect.flow_runs import asuspend_flow_run

from .handoff_contract import IDENTITY_KEYS, load_frozen_handoff


async def suspend_guarded_folder(*, retry_guard, task_id, group_id, receipt_id, attempt_id, folder_label):
    """The first action for a guarded retry; never dispatches any model request."""
    from paperflow_prefect.local_preprocessing import _bound_folder_order, _control_paths, _queue
    from paperflow_prefect.registered_input import verify_registered_receipt
    from paperflow_prefect.settings import Settings
    from paperflow_prefect.unified_flows import runtime_identity

    guard = str(UUID(retry_guard))
    if guard != retry_guard:
        raise ValueError('人工恢复编号无效')
    context = FlowRunContext.get()
    if context is None or not context.flow_run or not context.flow_run.deployment_id:
        raise ValueError('缺少真实的文件夹工单')
    if not isinstance(attempt_id, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', attempt_id):
        raise ValueError('文件夹交接编号无效')
    folder_id = str(context.flow_run.id)
    database, _ = _control_paths()
    settings = Settings({**Settings.load().values, 'execution_enabled': True})
    settings.require_execution()
    lock = FileLock(settings.path('data_root') / 'locks' / ('folder-order-' + attempt_id + '.lock'),
                    timeout=0, thread_local=False)
    while True:
        try:
            lock.acquire()
            break
        except Timeout:
            await asyncio.sleep(1)
    try:
        # Read no folder material until holding the same lock as deletion.
        # Check the durable tombstone before opening any handoff or receipt.
        with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
            db.row_factory = sqlite3.Row
            row = db.execute("""
                SELECT j.*,i.request_id AS accepted_request,t.task_status,
                       q.state AS ledger_state,q.manifest_sha256
                  FROM paperflow_llm_stage_jobs j
                  JOIN paperflow_llm_retry_items i ON i.request_id=j.retry_key AND i.job_id=j.job_id
                  JOIN tasks t ON t.task_id=j.task_id
                  JOIN llm_folder_queue q ON q.task_id=j.task_id AND q.group_id=j.group_id
                 WHERE j.retry_key=? AND j.task_id=? AND j.group_id=? AND j.receipt_id=?
                   AND j.attempt_id=? AND j.folder_run_id=? AND q.task_run_id=j.folder_run_id
                   AND t.prefect_order_model='v2' AND t.prefect_run_id=j.parent_run_id
                   AND q.paper_id=j.paper_id AND q.receipt_id=j.receipt_id
                   AND q.attempt_id=j.attempt_id AND q.parent_run_id=j.parent_run_id
                   AND i.task_id=j.task_id AND i.group_id=j.group_id AND i.paper_id=j.paper_id
                   AND i.receipt_id=j.receipt_id AND i.attempt_id=j.attempt_id
                   AND i.folder_run_id=j.folder_run_id AND i.generation=j.generation
                   AND NOT EXISTS(SELECT 1 FROM task_deletions d WHERE d.task_id=j.task_id)
            """, (guard, task_id, group_id, receipt_id, attempt_id, folder_id)).fetchone()
        if not row or row['task_status'] in {'deleting', 'delete_failed'}:
            raise ValueError('人工恢复工单身份不可核验')
        identity = {k: row[k] for k in IDENTITY_KEYS}
        queue = _queue()
        if not _bound_folder_order(queue, context.flow_run.id, task_id, group_id, receipt_id, attempt_id):
            raise ValueError('原文件夹工单绑定不可核验')
        handoff = queue.completed_preprocessing('prefect-local-preprocessing', attempt_id)
        if not handoff or any(handoff.get(k) != v for k, v in identity.items()):
            raise ValueError('原前处理交接不可核验')
        verified = load_frozen_handoff(handoff, identity, verify_registered_receipt)
        digest = hashlib.sha256(json.dumps(verified['manifest'], ensure_ascii=False, sort_keys=True,
                                separators=(',', ':'), allow_nan=False).encode('utf8')).hexdigest()
        if digest != row['manifest_sha256']:
            raise ValueError('原 LLM 文件夹交接发生变化')
        runtime_identity(settings)
        with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
            db.row_factory = sqlite3.Row
            task = db.execute('SELECT desired_state,task_status,start_generation,prefect_run_id FROM tasks '
                              'WHERE task_id=?', (task_id,)).fetchone()
            deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone()
            ledger = db.execute('SELECT state FROM llm_folder_queue WHERE task_id=? AND group_id=?',
                                (task_id, group_id)).fetchone()
            stage = db.execute('SELECT state FROM paperflow_llm_stage_jobs WHERE retry_key=? AND folder_run_id=?',
                               (guard, folder_id)).fetchone()
        if (not task or deleted or task['task_status'] in {'deleting', 'delete_failed'}
                or task['prefect_run_id'] != row['parent_run_id'] or not ledger or not stage):
            raise ValueError('任务或恢复记录已变化')
        if ledger['state'] == 'waiting_postprocessing':
            phase = '等待后处理'
        elif stage['state'] == 'recovery_unknown':
            phase = 'LLM 失败待核对'
        elif stage['state'] in {'submission_unknown', 'delivery_unknown', 'execution_unknown', 'identity_unknown'}:
            phase = 'LLM 阶段状态待核对'
        elif stage['state'] in {'failed', 'stopped', 'stopped_before_submit'}:
            phase = 'LLM 失败待核对'
        elif task['start_generation'] != row['generation']:
            phase = 'LLM 阶段状态待核对'
        else:
            phase = '等待 LLM' if task['desired_state'] == 'running' else '等待任务开始'
        async with get_client() as client:
            await client.update_flow_run(context.flow_run.id,
                name='资料文件夹 ' + (folder_label or group_id)[:80] + ' · ' + phase)
        await asuspend_flow_run(timeout=None, key='llm-retry-' + guard + '-' + uuid4().hex)
    finally:
        lock.release()
