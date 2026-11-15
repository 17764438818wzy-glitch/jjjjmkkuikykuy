"""One-time removal of cancelled placeholder task runs for the deleted task.

Defaults to read-only inspection. It never touches real folder flow runs,
model evidence, files of source materials, or any other task's runs.
"""
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import argparse
import json
import os
from pathlib import Path
import sqlite3
from uuid import NAMESPACE_URL, UUID, uuid5

from prefect.client.orchestration import SyncPrefectClient
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId


ROOT = Path(__file__).resolve().parents[2]
TASK_ID = 'ba3989b0-f9ec-42ce-a0a9-826470c86900'
ROOT_RUN_ID = UUID('c8e0d449-76ef-433a-92bb-80958c1c8365')
API_URL = 'http://192.168.1.120:4200/api'
PLACEHOLDER_KEY = 'paperflow-folder-waiting-v2'
MARKER = 'paperflow-order-v2-live-20261031'


def rows_for_task(client):
    rows = []
    for offset in range(0, 100000, 200):
        page = client.read_task_runs(
            flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=[ROOT_RUN_ID])),
            limit=200, offset=offset,
        )
        rows.extend(page)
        if len(page) < 200:
            return rows
    raise RuntimeError('task run pagination exceeded safety bound')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()

    queue_config = json.loads((ROOT / 'candidates/attachment-prefect-live-link-20261031/attachment-queue-config.json').read_text(encoding='utf8'))
    workspace_config = json.loads((ROOT / 'candidates/first-step-control-20261029/runtime/workspace-config.json').read_text(encoding='utf8'))
    queue_db = Path(queue_config['queue_db']).resolve(strict=True)
    control_db = (Path(workspace_config['classification_root']) / 'group-task-summaries' / 'summary-index.sqlite3').resolve(strict=True)
    with closing(sqlite3.connect(control_db.as_uri() + '?mode=ro', uri=True)) as control:
        if control.execute('SELECT 1 FROM tasks WHERE task_id=?', (TASK_ID,)).fetchone():
            raise RuntimeError('task has not been deleted from the upstream index')
        if not control.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (TASK_ID,)).fetchone():
            raise RuntimeError('upstream task deletion is not recorded')
    with closing(sqlite3.connect(queue_db.as_uri() + '?mode=ro', uri=True)) as queue:
        queue.row_factory = sqlite3.Row
        if not queue.execute('SELECT 1 FROM cancelled_tasks WHERE task_id=?', (TASK_ID,)).fetchone():
            raise RuntimeError('local queue has no cancellation tombstone')
        orders = [dict(row) for row in queue.execute(
            'SELECT group_id,receipt_id,attempt_id,prefect_run_id FROM preprocessing_folder_orders WHERE task_id=?',
            (TASK_ID,),
        )]
    if len(orders) != 309 or len({row['group_id'] for row in orders}) != 309:
        raise RuntimeError('unexpected folder order set')
    if any(not row['prefect_run_id'] for row in orders):
        raise RuntimeError('one or more folder flow identities are missing')
    expected = {
        uuid5(NAMESPACE_URL, 'paperflow-folder-parent/2/' + str(ROOT_RUN_ID) + '/' + TASK_ID + '/' + row['group_id']): row
        for row in orders
    }
    if len(expected) != len(orders):
        raise RuntimeError('placeholder identities collide')

    with SyncPrefectClient(API_URL, httpx_settings={'timeout': 8, 'trust_env': False}) as client:
        parent = client.read_flow_run(ROOT_RUN_ID)
        root_deployment = client.read_deployment_by_name('论文第一步-任务工单/手动运行')
        folder_deployment = client.read_deployment_by_name('资料文件夹工单/前处理')
        if (MARKER not in (root_deployment.tags or []) or MARKER not in (folder_deployment.tags or [])
                or parent.deployment_id != root_deployment.id
                or parent.idempotency_key != 'first-step-order-v2-' + TASK_ID
                or (parent.parameters or {}).get('task_id') != TASK_ID
                or not parent.state or not parent.state.is_cancelled()):
            raise RuntimeError('deleted task or Prefect deployment identity is not confirmed')

        order_by_id = {UUID(row['prefect_run_id']): row for row in orders}
        folder_runs = []
        ids = list(order_by_id)
        for start in range(0, len(ids), 100):
            folder_runs.extend(client.read_flow_runs(
                flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=ids[start:start+100])), limit=100,
            ))
        if len(folder_runs) != len(orders):
            raise RuntimeError('not all folder flow runs can be verified')
        for run in folder_runs:
            row = order_by_id[run.id]
            placeholder_id = uuid5(NAMESPACE_URL, 'paperflow-folder-parent/2/' + str(ROOT_RUN_ID) + '/' + TASK_ID + '/' + row['group_id'])
            if (run.deployment_id != folder_deployment.id or run.parent_task_run_id is not None
                    or 'paperflow-folder-ticket-' + str(placeholder_id) not in (run.tags or [])
                    or run.work_queue_name != 'task-' + TASK_ID
                    or run.idempotency_key != 'folder-waiting-v2-' + TASK_ID + '-' + row['group_id']
                    or any((run.parameters or {}).get(key) != value for key, value in {
                        'task_id': TASK_ID, 'group_id': row['group_id'],
                        'receipt_id': row['receipt_id'], 'attempt_id': row['attempt_id'],
                    }.items()) or not run.state or not run.state.is_cancelled()):
                raise RuntimeError('a real folder flow is active or has mismatched identity')

        all_tasks = rows_for_task(client)
        placeholders = [run for run in all_tasks if run.task_key == PLACEHOLDER_KEY]
        if len(placeholders) != len(expected) or {run.id for run in placeholders} != set(expected):
            raise RuntimeError('placeholder set has changed; no deletion performed')
        for run in placeholders:
            if (run.flow_run_id != ROOT_RUN_ID or run.dynamic_key != expected[run.id]['group_id']
                    or not run.state or not run.state.is_cancelled()):
                raise RuntimeError('placeholder identity or cancelled state is not confirmed')
        if any(not run.state or not run.state.is_final() for run in all_tasks):
            raise RuntimeError('parent task has a non-final child task')

        print('verified placeholders:', len(placeholders), 'other tasks:', len(all_tasks) - len(placeholders), flush=True)
        if not args.execute:
            print('read-only inspection; pass --execute to remove these placeholders', flush=True)
            return

        audit = Path(__file__).parent / ('removed-placeholders-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '.jsonl')
        with audit.open('x', encoding='utf8') as out:
            def record(value):
                out.write(json.dumps(value, ensure_ascii=False, sort_keys=True) + '\n')
                out.flush()
                os.fsync(out.fileno())
            record({'event': 'verified', 'task_id': TASK_ID, 'root_run_id': str(ROOT_RUN_ID),
                    'placeholder_ids': [str(run.id) for run in placeholders]})
            for number, run in enumerate(placeholders, 1):
                try:
                    client.delete_task_run(run.id)
                except Exception as exc:
                    record({'event': 'delete_outcome_unknown', 'id': str(run.id), 'error_type': type(exc).__name__})
                    raise
                record({'event': 'deleted', 'id': str(run.id)})
                if number % 50 == 0:
                    print('deleted', number, flush=True)
            remaining = rows_for_task(client)
            after = Counter(run.task_key for run in remaining)
            record({'event': 'readback', 'placeholder_count': after[PLACEHOLDER_KEY],
                    'other_task_count': len(remaining) - after[PLACEHOLDER_KEY]})
            if after[PLACEHOLDER_KEY] != 0 or len(remaining) != len(all_tasks) - len(placeholders):
                raise RuntimeError('deletion readback does not match the verified scope')
        print('readback: placeholders 0; other tasks preserved:', len(remaining), flush=True)
        print('audit:', audit, flush=True)


if __name__ == '__main__':
    main()
