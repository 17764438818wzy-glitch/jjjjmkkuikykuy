"""Read folder-grain LLM stages from retained handoffs and admission records."""
from contextlib import closing
from pathlib import Path
import sqlite3
from uuid import UUID


def _key(value):
    return tuple(value[k] for k in ('task_id', 'group_id', 'paper_id', 'receipt_id'))


def summarize_llm(queue, control_database, observer, snapshot):
    """Return only verified counts; an unknown result is None, never zero."""
    if (not isinstance(snapshot, dict) or snapshot.get('receiver_status') != 'running'
            or snapshot.get('waiting_available') is not True):
        raise ValueError('接收服务当前不可核验')
    waiting = queue.list_waiting_llm()
    if (not isinstance(waiting, dict) or type(waiting.get('unavailable_count')) is not int
            or waiting['unavailable_count'] < 0 or not isinstance(waiting.get('items'), list)):
        raise ValueError('上游 LLM 交接数量不可核验')
    items = waiting['items']
    by_key = {_key(item): item for item in items}
    if len(by_key) != len(items):
        raise ValueError('上游文件夹交接存在重复身份')
    path = Path(control_database).resolve(strict=True)
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=3)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        tasks = {row['task_id']: row['prefect_run_id'] for row in db.execute(
            "SELECT t.task_id,t.prefect_run_id FROM tasks t WHERE t.prefect_order_model='v2' "
            "AND t.prefect_run_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM task_deletions d "
            "WHERE d.task_id=t.task_id)")}
        rows = [dict(row) for row in db.execute(
            "SELECT q.* FROM llm_folder_queue q JOIN tasks t ON t.task_id=q.task_id "
            "WHERE t.prefect_order_model='v2' AND t.prefect_run_id=q.parent_run_id "
            "AND NOT EXISTS(SELECT 1 FROM task_deletions d WHERE d.task_id=q.task_id)")]
        admissions = [dict(row) for row in db.execute(
            "SELECT a.* FROM llm_request_admissions a JOIN llm_folder_queue q "
            "ON q.task_id=a.task_id AND q.task_run_id=a.task_run_id "
            "JOIN tasks t ON t.task_id=q.task_id AND t.prefect_run_id=q.parent_run_id "
            "WHERE t.prefect_order_model='v2' AND NOT EXISTS(SELECT 1 FROM task_deletions d "
            "WHERE d.task_id=a.task_id) AND a.state IN ('admitted','returned_unverified','unknown')")]
    phases = {}
    if any(item.get('task_id') not in tasks for item in items):
        raise ValueError('上游任务状态正在变化')
    for row in rows:
        key = _key(row)
        if key in phases or row['state'] not in {'waiting_llm','llm_processing','waiting_postprocessing'}:
            raise ValueError('文件夹 LLM 队列状态不一致')
        if key not in by_key:
            raise ValueError('文件夹 LLM 状态缺少有效的上游交接')
        phases[key] = row
    pending = [item for item in items if _key(item) not in phases or
               phases[_key(item)]['state'] == 'waiting_llm']
    post = sum(row['state'] == 'waiting_postprocessing' for row in rows)
    running, unknown = {'body':set(), 'attachment':set()}, {'body':False, 'attachment':False}
    row_by_run = {(row['task_id'],row['task_run_id']): row for row in rows}
    for admission in admissions:
        kind = admission.get('kind')
        row = row_by_run.get((admission.get('task_id'),admission.get('task_run_id')))
        if (kind not in running or not row or row['state'] != 'llm_processing'
                or admission.get('receipt_id') != row['receipt_id']):
            raise ValueError('文件夹模型请求状态不一致')
        if admission['state'] == 'unknown':
            unknown[kind] = True
        else:
            running[kind].add(row['task_run_id'])

    active_ids = set().union(*running.values())
    if active_ids:
        try:
            from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId
            with observer._client() as client:
                deployment = observer._safe_deployment(client)
                actual = {}
                ids = [UUID(value) for value in active_ids]
                for pos in range(0, len(ids), 100):
                    chunk = ids[pos:pos + 100]
                    actual.update({str(run.id): run for run in client.read_flow_runs(
                        flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=chunk)), limit=len(chunk))})
                for kind, values in running.items():
                    for value in values:
                        run = actual.get(value)
                        row = next(item for item in rows if item['task_run_id'] == value)
                        if (not run or run.deployment_id != deployment.id or run.parent_task_run_id is not None
                                or not run.state or not run.state.is_running()
                                or run.idempotency_key != 'folder-waiting-v2-' + row['task_id'] + '-' + row['group_id']
                                or run.work_queue_name != 'task-' + row['task_id']
                                or any((run.parameters or {}).get(k) != row[k]
                                       for k in ('task_id','group_id','receipt_id','attempt_id'))):
                            unknown[kind] = True
        except Exception:
            for kind, values in running.items():
                if values:
                    unknown[kind] = True
    partial = waiting['unavailable_count'] > 0
    return {'waiting_llm': len(pending) if not partial or pending else None,
            'llm_body_in_progress': None if partial or unknown['body'] else len(running['body']),
            'llm_attachment_in_progress': None if partial or unknown['attachment'] else len(running['attachment']),
            'waiting_postprocessing': None if partial else post,
            'waiting_llm_partial': partial, 'waiting_items': pending,
            'unavailable_count': waiting['unavailable_count']}
