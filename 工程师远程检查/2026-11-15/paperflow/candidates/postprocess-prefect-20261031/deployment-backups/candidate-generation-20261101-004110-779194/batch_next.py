"""Bounded batch candidate; no import-time scan, worker, or dispatch.

Only an explicit caller chooses a task, maximum folder count and runs this
function. Each batch subscribes to its own at-most-eight deterministic task keys.
No model or RAG interface is used. Failed/unknown work stops further dispatch.
"""
import asyncio
from contextlib import AsyncExitStack, closing, contextmanager
from pathlib import Path
import sqlite3
from uuid import UUID

from stage_runtime import _identity, _job_id, build_task, task_key_for_job


def snapshot_waiting(database, task_id, max_folders):
    """Freeze a bounded set of eligible database candidates, without claiming."""
    if str(UUID(task_id)) != task_id or type(max_folders) is not int or max_folders < 1:
        raise ValueError('invalid task identity or folder limit')
    path = Path(database).resolve(strict=True)
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=3)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        rows = db.execute('''SELECT f.* FROM llm_folder_queue f
            JOIN tasks t ON t.task_id=f.task_id AND t.prefect_run_id=f.parent_run_id
             AND t.prefect_order_model='v2' AND t.desired_state='running' AND t.start_pending=0
             AND t.start_generation=f.generation AND t.task_status NOT IN ('deleting','delete_failed','unknown')
            WHERE f.task_id=? AND f.state='waiting_postprocessing'
             AND NOT EXISTS(SELECT 1 FROM task_deletions d WHERE d.task_id=f.task_id)
             AND NOT EXISTS(SELECT 1 FROM paperflow_postprocess_stage_jobs j
                            WHERE j.task_id=f.task_id AND j.group_id=f.group_id)
             AND NOT EXISTS(SELECT 1 FROM postprocessing_results r
                            WHERE r.task_id=f.task_id AND r.group_id=f.group_id)
             AND NOT EXISTS(SELECT 1 FROM postprocessing_claims c
                            WHERE c.task_id=f.task_id AND c.group_id=f.group_id)
             AND EXISTS(SELECT 1 FROM llm_request_admissions a WHERE a.task_id=f.task_id
                        AND a.task_run_id=f.task_run_id AND a.kind='body' AND a.state='raw_complete')
             AND NOT EXISTS(SELECT 1 FROM llm_request_admissions a WHERE a.task_id=f.task_id
                            AND a.task_run_id=f.task_run_id AND (a.state IS NULL OR a.state!='raw_complete'))
            ORDER BY f.rowid LIMIT ?''', (task_id, max_folders)).fetchall()
    fields = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')
    return [(_identity({key: row[key] for key in fields}), row['generation']) for row in rows]


def make_batch_worker(stages, job_ids):
    """Construct one worker subscribed to exactly the current bounded batch."""
    from prefect.task_worker import TaskWorker
    if not 1 <= len(stages) <= 8 or len(stages) != len(job_ids) or len(set(job_ids)) != len(job_ids):
        raise ValueError('batch must contain one to eight distinct jobs')
    keys = [task_key_for_job(job_id) for job_id in job_ids]
    if any(stage.task_key != key for stage, key in zip(stages, keys)):
        raise ValueError('batch task identity mismatch')
    worker = TaskWorker(*stages, limit=len(stages))
    if worker.task_keys != set(keys) or len(worker.tasks) != len(stages):
        raise ValueError('batch worker subscription mismatch')
    for task, key in zip(worker.tasks, keys):
        task.task_key = key
    return worker


@contextmanager
def guard_prior_failures(coordinator, active, worker):
    """Recheck earlier outcomes in the actual reserve/submit gate callbacks."""
    original = getattr(coordinator, 'postprocess_is_open', None)
    if not callable(original):
        raise ValueError('postprocessing gate is unavailable')
    def guarded():
        try:
            if original() is not True:
                return False
            for job_id, key in tuple(active):
                state = coordinator.job(job_id)['state']
                if (state in {'failed', 'submission_unknown', 'execution_unknown'}
                        or (worker.finished_task_runs.get(key, 0) and state != 'finished')):
                    return False
            return True
        except Exception:
            return False
    coordinator.postprocess_is_open = guarded
    try:
        yield
    finally:
        coordinator.postprocess_is_open = original


async def run_batch(coordinator, selections, *, client_factory, runtime_factory,
                    worker_factory=make_batch_worker, batch_size=8, poll_seconds=.5):
    """Run one fixed snapshot in at-most-eight-key batches, stopping on problems."""
    if type(batch_size) is not int or not 1 <= batch_size <= 8 or poll_seconds <= 0:
        raise ValueError('batch size or observation interval is invalid')
    selections = [(_identity(identity), generation) for identity, generation in selections]
    if any(type(generation) is not int or generation < 1 for _, generation in selections):
        raise ValueError('task generation is invalid')
    job_ids = [_job_id(item['task_run_id']) for item, _ in selections]
    if (len(set(job_ids)) != len(job_ids)
            or len({item['task_id'] for item, _ in selections}) > 1):
        raise ValueError('batch snapshot contains duplicate or mixed-task folders')
    completed, attempts, stopped, detail = [], 0, False, None
    for start in range(0, len(selections), batch_size):
        group = selections[start:start + batch_size]
        ids = job_ids[start:start + batch_size]
        stages = [build_task(runtime_factory, job_id) for job_id in ids]
        worker = worker_factory(stages, ids)
        active = []
        async with AsyncExitStack() as stack:
            stack.enter_context(guard_prior_failures(coordinator, active, worker))
            await stack.enter_async_context(worker)
            subscription = asyncio.create_task(worker.astart())

            async def current_problem():
                for job_id, key in active:
                    item = await asyncio.to_thread(coordinator.job, job_id)
                    state = item['state']
                    if state in {'failed', 'submission_unknown', 'execution_unknown'}:
                        return {'job_id': job_id, 'state': state}
                    if worker.finished_task_runs.get(key, 0) and state != 'finished':
                        return {'job_id': job_id, 'state': state, 'reason': 'stage_outcome_unconfirmed'}
                if subscription.done():
                    return {'reason': 'worker_stopped'}
                return None

            try:
                await asyncio.sleep(0)
                for (identity, generation), planned_id, stage in zip(group, ids, stages):
                    detail = await current_problem()
                    if detail:
                        stopped = True
                        break
                    try:
                        job = await asyncio.to_thread(coordinator.reserve, identity, generation)
                        attempts += 1
                        if job['job_id'] != planned_id:
                            raise ValueError('reservation identity mismatch')
                        detail = await current_problem()
                        if detail:
                            detail['reserved_unsubmitted'] = planned_id
                            stopped = True
                            break
                        # The worker can claim before the create API returns;
                        # track it even if the create response is then lost.
                        active.append((planned_id, stage.task_key))
                        with client_factory() as client:
                            await asyncio.to_thread(coordinator.submit, planned_id, client, stage)
                    except Exception as exc:
                        detail = {'job_id': planned_id, 'reason': type(exc).__name__}
                        if active and active[-1][0] == planned_id:
                            current = await asyncio.to_thread(coordinator.job, planned_id)
                            if current['state'] == 'reserved':
                                active.pop()
                                detail['reserved_unsubmitted'] = planned_id
                        stopped = True
                        break
                # Already submitted folders are allowed to finish. Never submit
                # another tranche after an observed failure or uncertainty.
                while active:
                    problem = await current_problem()
                    if problem:
                        stopped = True
                        detail = detail or problem
                    statuses = [(job_id, (await asyncio.to_thread(coordinator.job, job_id))['state'])
                                for job_id, _ in active]
                    terminal = {'finished', 'failed', 'submission_unknown', 'execution_unknown'}
                    if all(state in terminal for _, state in statuses):
                        completed.extend(job_id for job_id, state in statuses if state == 'finished')
                        break
                    if subscription.done():
                        stopped = True
                        detail = detail or {'reason': 'worker_stopped'}
                        break
                    # A final TaskRun with no confirmed stage claim is unknown.
                    if problem and problem.get('reason') == 'stage_outcome_unconfirmed':
                        stopped = True
                        break
                    await asyncio.sleep(poll_seconds)
            finally:
                subscription.cancel()
                try:
                    await subscription
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    stopped = True
                    detail = detail or {'reason': 'worker_stopped', 'error_type': type(exc).__name__}
        # Exiting a real TaskWorker waits for already-running tasks to drain.
        # Read again after that wait so the report does not omit late finishes.
        for job_id, _ in active:
            try:
                current = await asyncio.to_thread(coordinator.job, job_id)
                if current['state'] == 'finished' and job_id not in completed:
                    completed.append(job_id)
            except Exception as exc:
                stopped = True
                detail = detail or {'job_id': job_id, 'reason': type(exc).__name__}
        if stopped:
            break
    return {'completed': completed, 'stopped': stopped, 'detail': detail,
            'attempted': attempts, 'remaining': len(selections) - attempts}
