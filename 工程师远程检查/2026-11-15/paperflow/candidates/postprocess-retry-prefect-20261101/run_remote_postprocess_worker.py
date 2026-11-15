"""Coordinate the existing Prefect folder tickets with the server executor.

New local admissions require the original task to be running. A remote start
already sent before pause may still arrive and must be reconciled separately.
This worker never calls an LLM or a RAG uploader and never falls back to local
postprocessing. The existing coordinator owns the stage, claim and Prefect slot.
"""
import argparse
import asyncio
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
from uuid import UUID


LIMIT = 8
POLL_SECONDS = 5
def log(event, **fields):
    print(json.dumps(dict(event=event, **fields), ensure_ascii=False), flush=True)


def running_tasks(database):
    """Read task authority and postprocessing state in one snapshot."""
    path = Path(database).resolve(strict=True)
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=3)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        gates = dict(db.execute('SELECT name,blocked FROM paperflow_queue_barriers'))
        if gates.get('postprocessing') != 0:
            return [], []
        candidates = db.execute('''SELECT t.task_id,t.start_generation FROM tasks t
            WHERE t.desired_state='running' AND t.start_pending=0
              AND t.prefect_order_model='v2' AND t.prefect_run_id IS NOT NULL
              AND t.task_status NOT IN ('deleting','delete_failed','unknown')
              AND NOT EXISTS(SELECT 1 FROM task_deletions d WHERE d.task_id=t.task_id)
            ORDER BY t.started_at_ns,t.task_id''').fetchall()
        tasks = []
        issues = []
        for row in candidates:
            if type(row['start_generation']) is not int or row['start_generation'] < 1:
                issues.append((row['task_id'], 'InvalidGeneration'))
                continue
            tasks.append((row['task_id'], row['start_generation']))
        return tasks, issues


def snapshot_stopped(database, task_id, max_folders):
    """Find original stages which have not started processing."""
    path = Path(database).resolve(strict=True)
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=3)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        rows = db.execute('''SELECT f.*, j.job_id, t.start_generation AS stage_generation
            FROM llm_folder_queue f
            JOIN tasks t ON t.task_id=f.task_id AND t.prefect_run_id=f.parent_run_id
              AND t.prefect_order_model='v2' AND t.desired_state='running' AND t.start_pending=0
              AND t.task_status NOT IN ('deleting','delete_failed','unknown')
            JOIN paperflow_postprocess_stage_jobs j ON j.task_id=f.task_id AND j.group_id=f.group_id
              AND j.paper_id=f.paper_id AND j.receipt_id=f.receipt_id AND j.attempt_id=f.attempt_id
              AND j.parent_run_id=f.parent_run_id AND j.task_run_id=f.task_run_id
              AND j.folder_run_id=f.task_run_id AND j.manifest_sha256=f.manifest_sha256
              AND j.source_llm_generation=f.generation AND f.generation>=1
              AND f.generation<=j.generation
            WHERE f.task_id=? AND f.state='waiting_postprocessing'
              AND ((j.state IN ('reserved','submitting','submitted') AND j.generation<=t.start_generation)
                   OR (j.state='stopped_before_submit' AND j.generation<t.start_generation))
              AND j.owner_pid IS NULL
              AND j.owner_started IS NULL AND j.error_type IS NULL
              AND NOT EXISTS(SELECT 1 FROM task_deletions d WHERE d.task_id=f.task_id)
              AND NOT EXISTS(SELECT 1 FROM postprocessing_claims c
                             WHERE c.task_id=f.task_id AND c.group_id=f.group_id)
              AND NOT EXISTS(SELECT 1 FROM postprocessing_results r
                             WHERE r.task_id=f.task_id AND r.group_id=f.group_id)
              AND EXISTS(SELECT 1 FROM llm_request_admissions a WHERE a.task_id=f.task_id
                         AND a.task_run_id=f.task_run_id AND a.kind='body' AND a.state='raw_complete')
              AND NOT EXISTS(SELECT 1 FROM llm_request_admissions a WHERE a.task_id=f.task_id
                             AND a.task_run_id=f.task_run_id AND (a.state IS NULL OR a.state!='raw_complete'))
            ORDER BY j.updated_at_ns,j.job_id LIMIT ?''', (task_id, max_folders)).fetchall()
    fields = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')
    return [({key: row[key] for key in fields}, row['stage_generation'], row['job_id']) for row in rows]


def snapshot_retryable(database, task_id, max_folders, job_ids):
    if not job_ids:
        return []
    with closing(sqlite3.connect(Path(database).resolve().as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        placeholders = ','.join('?' for _ in job_ids)
        rows = db.execute('''SELECT f.*,t.start_generation AS stage_generation,j.job_id
            FROM llm_folder_queue f JOIN tasks t ON t.task_id=f.task_id
            JOIN paperflow_postprocess_stage_jobs j ON j.task_id=f.task_id AND j.group_id=f.group_id
            LEFT JOIN postprocessing_results r ON r.task_id=f.task_id AND r.group_id=f.group_id
            LEFT JOIN postprocessing_claims c ON c.task_id=f.task_id AND c.group_id=f.group_id
            WHERE f.task_id=? AND f.state='waiting_postprocessing'
              AND t.desired_state='running' AND t.start_pending=0
              AND j.state IN ('execution_unknown','failed','submission_unknown')
              AND j.job_id IN (''' + placeholders + ''')
              AND j.generation<t.start_generation
              AND (r.state IS NULL OR r.state!='passed')
              AND (c.state IS NULL OR c.state NOT IN ('processing','receipt_held','waiting_rag'))
            ORDER BY j.updated_at_ns LIMIT ?''', (task_id, *job_ids, max_folders)).fetchall()
    fields = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')
    return [({key: row[key] for key in fields}, row['stage_generation'], row['job_id']) for row in rows]


async def run_folder(launcher, identity, generation, resume_job_id=None, *, retry_job_id=None):
    # Policy discovery is read-only; the launcher rechecks the same original
    # task and folder before it reserves or starts the remote Prefect stage.
    if retry_job_id is not None:
        prepared = await asyncio.to_thread(launcher.prepare, identity, generation, retry_job_id=retry_job_id)
    elif resume_job_id is None:
        prepared = await asyncio.to_thread(launcher.prepare, identity, generation)
    else:
        prepared = await asyncio.to_thread(launcher.prepare, identity, generation, resume_job_id=resume_job_id)
    return await launcher.launch(identity, generation, prepared)


def snapshot_held(database, task_id, generation, limit):
    with closing(sqlite3.connect(Path(database).resolve().as_uri() + '?mode=ro', uri=True)) as db:
        return db.execute('''SELECT c.group_id,j.job_id FROM postprocessing_claims c
            JOIN paperflow_postprocess_stage_jobs j ON j.task_id=c.task_id AND j.group_id=c.group_id
            WHERE c.task_id=? AND c.state='receipt_held' AND j.state='finished' AND j.generation<?
            ORDER BY c.updated_at_ns LIMIT ?''', (task_id, generation, limit)).fetchall()


def reconcile_held(runtime, access, job_id, generation):
    from remote_runtime_adapter import RemoteFolderStageStore
    from stage_runtime import StageConflict, _task_identity
    original = runtime.original_runtime
    coordinator = original['coordinator']
    item = coordinator.job(job_id)
    if (item['task_id'], generation) not in running_tasks(original['database'])[0]:
        raise StageConflict('the original task is paused or its generation changed')
    with original['client']() as client:
        stage = client.read_task_run(UUID(job_id))
    if not _task_identity(stage, item) or not stage.state.is_completed():
        raise ValueError('the original Prefect postprocessing task is not completed')
    row = access.row(item['task_id'], item['group_id'])
    saved_receipt = json.loads(row['receipt_json'])
    store = RemoteFolderStageStore(original['database'], observe_run=original['observe_run'],
        observe_activity=None, admission_gate=coordinator.task_gate, inspect_receipt=access.inspector)
    with access.inspector.prefetched([row]):
        result = coordinator.reconcile_held(job_id, generation, store=store,
            validator_factory=lambda *_: lambda receipt: receipt == saved_receipt)
    return 'finished' if result['state'] == 'waiting_rag' else result['state']


async def run_held(runtime, access, job_id, generation):
    from paperflow_prefect.resources import drain_thread
    async with runtime.original_runtime['coordinator'].slot():
        return await drain_thread(reconcile_held, runtime, access, job_id, generation)


async def serve(runtime, launcher, access, bridge=None, *, retry_targets=None):
    retry_targets = retry_targets or {}
    active = {}
    skipped = set()
    last_error = None
    last_bridge_error = None
    last_task_issues = set()
    log('remote_postprocessing_coordinator_started', limit=LIMIT)
    while True:
        for task in [item for item in active if item.done()]:
            task_id, group_id, generation = active.pop(task)
            try:
                state = task.result()
            except Exception as exc:
                skipped.add((task_id, group_id, generation))
                log('folder_unconfirmed', task_id=task_id, group_id=group_id,
                    error_type=type(exc).__name__)
            else:
                if state != 'finished':
                    skipped.add((task_id, group_id, generation))
                log('folder_stage_observed', task_id=task_id, group_id=group_id, state=state)

        try:
            task_rows, issues = await asyncio.to_thread(
                running_tasks, runtime.original_runtime['database'])
            running_ids = {task_id for task_id, _ in task_rows}
            active_running = sum(task_id in running_ids for task_id, _, _ in active.values())
            if active_running < LIMIT:
                last_error = None
                current_task_issues = set(issues)
                for task_id, error in sorted(current_task_issues - last_task_issues):
                    log('task_waiting_for_reconciliation', task_id=task_id, error_type=error)
                last_task_issues = current_task_issues
                for task_id, generation in task_rows:
                    if active_running >= LIMIT:
                        break
                    inflight = {(t, g) for t, g, _ in active.values()}
                    # A preparation failure can happen before a durable stage
                    # exists. Skip only that folder for this task generation;
                    # scan past it so other folders can proceed. Existing stage,
                    # claim and result rows remain excluded by snapshot_waiting.
                    omitted = {(t, g) for t, g, gen in skipped if t == task_id and gen == generation}
                    count = LIMIT + len(omitted | {pair for pair in inflight if pair[0] == task_id})
                    held = (await asyncio.to_thread(snapshot_held, runtime.original_runtime['database'],
                              task_id, generation, count)) if access else []
                    stopped = await asyncio.to_thread(snapshot_stopped,
                        runtime.original_runtime['database'], task_id, count)
                    retryable = await asyncio.to_thread(snapshot_retryable,
                        runtime.original_runtime['database'], task_id, count, retry_targets.get(task_id, []))
                    waiting = await asyncio.to_thread(runtime.batch.snapshot_waiting,
                        runtime.original_runtime['database'], task_id, count)
                    selections = [({'task_id': task_id, 'group_id': group_id}, generation, job_id, True, False)
                                  for group_id, job_id in held]
                    selections += [(identity, gen, job_id, False, False) for identity, gen, job_id in stopped]
                    selections += [(identity, gen, job_id, False, True) for identity, gen, job_id in retryable]
                    selections += [(identity, gen, None, False, False) for identity, gen in waiting]
                    for identity, current_generation, resume_job_id, is_held, is_retry in selections:
                        if active_running >= LIMIT:
                            break
                        if (identity['task_id'] != task_id or current_generation != generation
                                or (task_id, identity['group_id']) in inflight
                                or (task_id, identity['group_id']) in omitted):
                            continue
                        operation = (run_held(runtime, access, resume_job_id, generation) if is_held else
                                     run_folder(launcher, dict(identity), generation,
                                         None if is_retry else resume_job_id,
                                         retry_job_id=resume_job_id if is_retry else None))
                        item = asyncio.create_task(operation)
                        active[item] = (task_id, identity['group_id'], generation)
                        active_running += 1
                        inflight.add((task_id, identity['group_id']))
        except Exception as exc:
            error = type(exc).__name__
            if error != last_error:
                log('coordinator_waiting_for_authority', error_type=error)
                last_error = error

        if bridge:
            try:
                await bridge.tick()
                last_bridge_error = None
            except Exception as exc:
                error = type(exc).__name__
                if error != last_bridge_error:
                    log('rag_nas_coordinator_unavailable', error_type=error)
                    last_bridge_error = error
        pending = set(active)
        if bridge:
            pending.update(bridge.pending())
        if pending:
            await asyncio.wait(pending, timeout=POLL_SECONDS, return_when=asyncio.FIRST_COMPLETED)
        else:
            await asyncio.sleep(POLL_SECONDS)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--config-sha256', required=True)
    parser.add_argument('--rag-nas-batch-config')
    parser.add_argument('--retry-targets')
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    config_path = root / 'config.json'
    settings = json.loads(config_path.read_text(encoding='utf-8-sig'))
    api = str(settings.get('prefect_api_url', '')).rstrip('/')
    if not api:
        raise RuntimeError('the original Prefect address is unavailable')
    os.environ.update(PAPERFLOW_CONFIG=str(config_path), PREFECT_API_URL=api, PYTHONUTF8='1',
                      PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS='false')
    server_candidate = root.parent / 'candidates' / 'postprocess-server-20261101'
    sys.path.insert(0, str(server_candidate))
    from scoped_runtime_factory import build
    # A busy response is retried only when the pinned server confirms that the
    # same admission did not start; ambiguous replies remain unknown.
    runtime, launcher = build(args.config, args.config_sha256,
                              admission_wait={'wait': lambda: time.sleep(.5), 'max_attempts': None})
    sys.path.insert(0, str(root.parent / 'candidates' / 'postprocess-rag-unblock-20261101'))
    from receipt_access import build as build_receipt_access
    try:
        access = build_receipt_access()
    except Exception as exc:
        log('held_receipts_unavailable', error_type=type(exc).__name__)
        access = None
    bridge = None
    if args.rag_nas_batch_config and access:
        try:
            sys.path.insert(0, str(root.parent / 'candidates' / 'rag-nas-prefect-20261031'))
            from rag_nas_serve import build as build_bridge
            batch_config = json.loads(Path(args.rag_nas_batch_config).read_text(encoding='utf8'))
            bridge = build_bridge(runtime, access, log, batch_config=batch_config)
            log('rag_nas_coordinator_started', task_id=batch_config['task_id'])
        except Exception as exc:
            log('rag_nas_coordinator_unavailable', error_type=type(exc).__name__)
    retry_targets = json.loads(Path(args.retry_targets).read_text(encoding='utf8')) if args.retry_targets else {}
    asyncio.run(serve(runtime, launcher, access, bridge, retry_targets=retry_targets))


if __name__ == '__main__':
    root = Path(__file__).resolve().parent
    log_path = root / 'runtime' / 'remote-postprocessing' / 'mainline-worker.log'
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open('a', encoding='utf8', buffering=1) as stream:
        sys.stdout = sys.stderr = stream
        try:
            main()
        except BaseException as exc:
            log('remote_postprocessing_coordinator_stopped', error_type=type(exc).__name__)
            raise
