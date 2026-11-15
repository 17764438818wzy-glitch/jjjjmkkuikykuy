"""Submit and record the RAG/NAS stages for one existing folder order."""
from contextlib import ExitStack, closing, contextmanager
from pathlib import Path
import sqlite3
import time
from uuid import UUID

from group_stage import FIELDS, prefect_task_identity
from folder_native_client import NativeNotReady
from task_effect_admission import TaskNotRunning


class HandoffNeedsReview(RuntimeError):
    """The original folder has a known unread-range disclosure problem."""


def _db(path):
    db = sqlite3.connect(Path(path).resolve(strict=True).as_uri() + '?mode=rw', uri=True, timeout=10)
    db.row_factory = sqlite3.Row
    return db


def _current(db, task_id, group_id, *, running, resume_unknown=False):
    job = db.execute('SELECT * FROM paperflow_rag_nas_jobs WHERE task_id=? AND group_id=?',
                     (task_id, group_id)).fetchone()
    parent = db.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
    deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone()
    if (not job or not parent or deleted or parent['task_status'] in {'deleting', 'delete_failed', 'unknown'}):
        raise TaskNotRunning('parent task is deleted or unavailable')
    if running:
        if parent['desired_state'] != 'running' or parent['start_pending']:
            raise TaskNotRunning('parent task is not running')
        if parent['start_generation'] != job['run_generation']:
            if (parent['start_generation'] < job['run_generation']
                    or job['state'] not in ('waiting', 'paused') and not resume_unknown):
                raise TaskNotRunning('folder generation cannot be resumed')
            db.execute('UPDATE paperflow_rag_nas_jobs SET run_generation=? WHERE task_id=? AND group_id=?',
                       (parent['start_generation'], task_id, group_id))
            job = db.execute('SELECT * FROM paperflow_rag_nas_jobs WHERE task_id=? AND group_id=?',
                             (task_id, group_id)).fetchone()
    return job


def _check_run(actual, identity, folder_run_id, *, allow_detached=False):
    return (str(actual.id) == identity['id'] and actual.task_key == identity['task_key']
            and actual.dynamic_key == identity['dynamic_key'] and identity['tag'] in actual.tags
            and (str(actual.flow_run_id) == folder_run_id or allow_detached and actual.flow_run_id is None))


def submit_stage(database, task_id, group_id, stage, client, task):
    if stage not in ('rag', 'nas'):
        raise ValueError('unknown folder stage')
    with closing(_db(database)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        job = _current(db, task_id, group_id, running=True)
        field = stage + '_submission'
        identity = prefect_task_identity(stage, job[stage + '_task_run_id'])
        if job['stage'] != stage or job['state'] != 'waiting' or job[field] != 'reserved':
            raise ValueError('stage has already been submitted or is unavailable')
        if task.task_key != identity['task_key'] or identity['tag'] not in task.tags:
            raise ValueError('Prefect task identity does not match the folder')
        if stage == 'rag':
            barrier = db.execute("SELECT blocked FROM paperflow_queue_barriers WHERE name='rag_upload'").fetchone()
            if not barrier or barrier['blocked'] != 0:
                raise InterruptedError('RAG admission is paused')
        db.execute(f'UPDATE paperflow_rag_nas_jobs SET {field}=\'submitting\',updated_at_ns=? '
                   'WHERE task_id=? AND group_id=?', (time.time_ns(), task_id, group_id))
    try:
        from prefect.states import Scheduled
        state = Scheduled()
        state.state_details.deferred = True
        actual = client.create_task_run(task=task, flow_run_id=UUID(job['task_run_id']), id=UUID(identity['id']),
            dynamic_key=identity['dynamic_key'], name='论文组 RAG' if stage == 'rag' else '论文组 NAS 归档与清理',
            state=state)
        if not _check_run(actual, identity, job['task_run_id']):
            raise ValueError('Prefect returned a different folder stage')
    except Exception as exc:
        with closing(_db(database)) as db, db:
            db.execute(f"UPDATE paperflow_rag_nas_jobs SET {field}='unknown',"
                       f"state=CASE WHEN state='waiting' THEN 'unknown' ELSE state END,error_type=?,updated_at_ns=? "
                       f"WHERE task_id=? AND group_id=? AND {field}='submitting'",
                       (type(exc).__name__, time.time_ns(), task_id, group_id))
        raise
    with closing(_db(database)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        db.execute(f"UPDATE paperflow_rag_nas_jobs SET {field}='submitted',updated_at_ns=? "
                   f"WHERE task_id=? AND group_id=? AND {field}='submitting'",
                   (time.time_ns(), task_id, group_id))
        row = db.execute(f'SELECT {field} FROM paperflow_rag_nas_jobs WHERE task_id=? AND group_id=?',
                         (task_id, group_id)).fetchone()
        parent = db.execute('SELECT task_status FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone()
        stopped = (not row or row[field] not in ('submitted', 'finished') or not parent or deleted
                   or parent['task_status'] in {'deleting', 'delete_failed', 'unknown'})
    if stopped:
        raise InterruptedError('Prefect task was created after the parent stopped or was deleted')
    return identity['id']


def claim_stage(database, task_id, group_id, stage, actual):
    with closing(_db(database)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        field = stage + '_submission'
        row = db.execute('SELECT * FROM paperflow_rag_nas_jobs WHERE task_id=? AND group_id=?',
                         (task_id, group_id)).fetchone()
        identity = prefect_task_identity(stage, row[stage + '_task_run_id'])
        resume_unknown = (row['stage'] == stage and row['state'] == 'unknown'
                          and row[field] == 'unknown' and _check_run(actual, identity, row['task_run_id'], allow_detached=True))
        job = _current(db, task_id, group_id, running=True, resume_unknown=resume_unknown)
        claimable = (job['state'] in ('waiting', 'paused') and job[field] in ('submitting', 'submitted')
                     or job['state'] == 'unknown' and job[field] == 'unknown')
        if job['stage'] != stage or not claimable or not _check_run(actual, identity, row['task_run_id'], allow_detached=True):
            raise ValueError('Prefect folder stage is not claimable')
        if stage == 'rag':
            barrier = db.execute("SELECT blocked FROM paperflow_queue_barriers WHERE name='rag_upload'").fetchone()
            if not barrier or barrier['blocked'] != 0:
                raise TaskNotRunning('RAG admission is paused')
        now = time.time_ns()
        db.execute(f"UPDATE paperflow_rag_nas_jobs SET state='running',{field}='submitted',"
                   f"{stage}_started_at_ns=COALESCE({stage}_started_at_ns,?),updated_at_ns=? WHERE task_id=? AND group_id=?",
                   (now, now, task_id, group_id))
        db.execute('INSERT INTO paperflow_rag_nas_intervals(task_id,group_id,stage,started_at_ns) VALUES(?,?,?,?)',
                   (task_id, group_id, stage, now))


def pause_stage(database, task_id, group_id, stage):
    with closing(_db(database)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        job = db.execute('SELECT * FROM paperflow_rag_nas_jobs WHERE task_id=? AND group_id=?',
                         (task_id, group_id)).fetchone()
        if job['stage'] != stage or job['state'] != 'running':
            raise ValueError('folder stage is not running')
        now = time.time_ns()
        deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone()
        db.execute("UPDATE paperflow_rag_nas_jobs SET state=?,updated_at_ns=? WHERE task_id=? AND group_id=?",
                   ('cancelled' if deleted else 'paused', now, task_id, group_id))
        db.execute('UPDATE paperflow_rag_nas_intervals SET ended_at_ns=? WHERE task_id=? AND group_id=? '
                   'AND stage=? AND ended_at_ns IS NULL', (now, task_id, group_id, stage))


def unknown_stage(database, task_id, group_id, stage, error):
    with closing(_db(database)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        job = db.execute('SELECT * FROM paperflow_rag_nas_jobs WHERE task_id=? AND group_id=?',
                         (task_id, group_id)).fetchone()
        if job['stage'] != stage or job['state'] != 'running':
            raise ValueError('folder stage is not running')
        now = time.time_ns()
        db.execute("UPDATE paperflow_rag_nas_jobs SET state='unknown',error_type=?,updated_at_ns=? "
                   'WHERE task_id=? AND group_id=?', (type(error).__name__, now, task_id, group_id))
        db.execute('UPDATE paperflow_rag_nas_intervals SET ended_at_ns=? WHERE task_id=? AND group_id=? '
                   'AND stage=? AND ended_at_ns IS NULL', (now, task_id, group_id, stage))


def review_stage(database, task_id, group_id, stage):
    with closing(_db(database)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        job = db.execute('SELECT * FROM paperflow_rag_nas_jobs WHERE task_id=? AND group_id=?',
                         (task_id, group_id)).fetchone()
        if job['stage'] != stage or job['state'] != 'running':
            raise ValueError('folder stage is not running')
        now = time.time_ns()
        deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone()
        db.execute("UPDATE paperflow_rag_nas_jobs SET state=?,error_type='UnreadReviewRequired',updated_at_ns=? "
                   'WHERE task_id=? AND group_id=?', ('cancelled' if deleted else 'needs_review', now, task_id, group_id))
        db.execute('UPDATE paperflow_rag_nas_intervals SET ended_at_ns=? WHERE task_id=? AND group_id=? '
                   'AND stage=? AND ended_at_ns IS NULL', (now, task_id, group_id, stage))


def _parent_running(database, task_id, group_id):
    with closing(_db(database)) as db:
        _current(db, task_id, group_id, running=False)
        parent = db.execute('SELECT desired_state,start_pending FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        return parent['desired_state'] == 'running' and not parent['start_pending']


def controlled_prefect_slots(database, task_id, group_id, limits):
    """Use existing Prefect slots while allowing a waiting task to stop."""
    from paperflow_prefect.resources import cancellable_concurrency

    class ParentStop:
        checked_at, stopped = 0.0, False

        def is_set(self, force=False):
            if force or time.monotonic() - self.checked_at >= .25:
                try:
                    self.stopped = not _parent_running(database, task_id, group_id)
                except TaskNotRunning:
                    self.stopped = True
                self.checked_at = time.monotonic()
            return self.stopped

    @contextmanager
    def slot(stage):
        name, limit = limits[stage]
        stop = ParentStop()
        with ExitStack() as stack:
            try:
                stack.enter_context(cancellable_concurrency(name, stop, expected={name: limit}))
            except InterruptedError as exc:
                if stop.is_set(force=True):
                    raise TaskNotRunning('parent stopped while waiting for a Prefect slot') from exc
                raise
            yield
    return slot


def _run_attempt(database, task_id, group_id, stage, actual, operation, slot):
    with slot(stage):
        claimed = False
        try:
            claim_stage(database, task_id, group_id, stage, actual)
            claimed = True
            with closing(_db(database)) as db:
                job = dict(_current(db, task_id, group_id, running=False))
            result = operation(stage, job)
            if stage == 'rag':
                finish_rag(database, task_id, group_id, result['rag_receipt_path'],
                           copy_receipt_path=result['copy_receipt_path'])
            else:
                finish_nas(database, task_id, group_id, result['archive_receipt_path'], result['cleanup_receipt_path'])
            return result
        except (TaskNotRunning, NativeNotReady):
            if claimed:
                pause_stage(database, task_id, group_id, stage)
            return None
        except HandoffNeedsReview:
            if claimed:
                review_stage(database, task_id, group_id, stage)
            raise
        except Exception as exc:
            if claimed:
                unknown_stage(database, task_id, group_id, stage, exc)
            raise


def controlled_executor(database, task_id, group_id, operation, slot):
    """Keep one Prefect task while its drained business work waits for resume."""
    import asyncio
    from paperflow_prefect.resources import drain_thread

    async def execute(stage, actual):
        while True:
            if not await asyncio.to_thread(_parent_running, database, task_id, group_id):
                await asyncio.sleep(1)
                continue
            try:
                result = await drain_thread(_run_attempt, database, task_id, group_id, stage, actual, operation, slot)
            except TaskNotRunning:
                result = None
            if result is not None:
                return result
            await asyncio.sleep(1)
    return execute


def finish_rag(database, task_id, group_id, receipt_path, *, copy_receipt_path):
    from group_rag_evidence import read_verified_group
    receipt, _ = read_verified_group(receipt_path, copy_receipt_path)
    with closing(_db(database)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        job = _current(db, task_id, group_id, running=False)
        if (job['stage'] != 'rag' or job['state'] != 'running'
                or receipt.get('schema') != 'paperflow-isolated-group-rag/1' or receipt.get('status') != 'verified'
                or receipt.get('group_count') != 1
                or receipt['binding']['ticket'] != {key: job[key] for key in FIELDS}
                or receipt['binding']['postprocess'] != {key: job[key] for key in
                    ('claim_id', 'postprocess_job_id', 'postprocess_receipt_sha256',
                     'upstream_manifest_sha256', 'source_generation')}):
            raise ValueError('complete group RAG receipt is unavailable')
        now = time.time_ns()
        db.execute("UPDATE paperflow_rag_nas_jobs SET rag_submission='finished',rag_completed_at_ns=?,"
                   "rag_receipt_path=?,copy_receipt_path=?,updated_at_ns=? WHERE task_id=? AND group_id=?",
                   (now, str(receipt_path), str(copy_receipt_path),
                    now, task_id, group_id))
        db.execute("UPDATE paperflow_rag_nas_intervals SET ended_at_ns=? WHERE task_id=? AND group_id=? "
                   "AND stage='rag' AND ended_at_ns IS NULL", (now, task_id, group_id))


def finish_nas(database, task_id, group_id, archive_path, cleanup_path):
    from paperflow_prefect.records import read_json, sha_file
    archive, cleanup = read_json(archive_path), read_json(cleanup_path)
    with closing(_db(database)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        job = _current(db, task_id, group_id, running=False)
        if not job['copy_receipt_path'] or not job['rag_receipt_path']:
            raise ValueError('folder test copy or RAG receipt is unavailable')
        copied = read_json(job['copy_receipt_path'])
        expected = {(row['relative'], row['sha256'], row['bytes']) for row in copied['files']}
        archived = {(row['name'], row['sha256'], row['bytes']) for row in archive.get('files', [])}
        if (job['stage'] != 'nas' or job['state'] != 'running'
                or archive.get('schema') != 'paperflow-isolated-test-archive/1'
                or archive.get('readback_verified') is not True
                or any(archive[key] != job[key] or copied[key] != job[key]
                       for key in ('group_id', 'paper_id', 'receipt_id'))
                or cleanup.get('schema') != 'paperflow-isolated-cleanup/1'
                or cleanup.get('status') != 'completed' or cleanup.get('group_id') != group_id
                or cleanup['binding']['copy_receipt_sha256'] != sha_file(job['copy_receipt_path'])
                or cleanup['binding']['rag_receipt_sha256'] != sha_file(job['rag_receipt_path'])
                or cleanup['binding']['archive_receipt_sha256'] != sha_file(archive_path)
                or len(archive.get('files', [])) != len(copied['files']) or archived != expected
                or set(cleanup['files']) != {row['file_id'] for row in copied['files']}
                or any(cleanup['files'][row['file_id']].get('state') != 'deleted'
                       or cleanup['files'][row['file_id']].get('path') != row['path']
                       or cleanup['files'][row['file_id']].get('sha256') != row['sha256']
                       for row in copied['files'])):
            raise ValueError('archive or test copy deletion evidence is incomplete')
        now = time.time_ns()
        db.execute("UPDATE paperflow_rag_nas_jobs SET nas_submission='finished',"
                   "nas_completed_at_ns=?,deleted_at_ns=?,nas_receipt_path=?,"
                   "cleanup_receipt_path=?,updated_at_ns=? WHERE task_id=? AND group_id=?",
                   (Path(archive_path).stat().st_mtime_ns, cleanup.get('completed_at_ns', now),
                    str(archive_path), str(cleanup_path), now, task_id, group_id))
        db.execute("UPDATE paperflow_rag_nas_intervals SET ended_at_ns=? WHERE task_id=? AND group_id=? "
                   "AND stage='nas' AND ended_at_ns IS NULL", (now, task_id, group_id))


def advance_stage(database, task_id, group_id, stage, actual):
    """Advance only after the same Prefect TaskRun has actually completed."""
    with closing(_db(database)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        job = _current(db, task_id, group_id, running=False)
        identity = prefect_task_identity(stage, job[stage + '_task_run_id'])
        if (job['stage'] != stage or job['state'] != 'running' or job[stage + '_submission'] != 'finished'
                or not _check_run(actual, identity, job['task_run_id'], allow_detached=True) or not actual.state.is_completed()):
            raise ValueError('Prefect folder stage is not completed')
        now = time.time_ns()
        next_stage, next_state = ('nas', 'waiting') if stage == 'rag' else ('completed', 'completed')
        db.execute('UPDATE paperflow_rag_nas_jobs SET stage=?,state=?,completed_at_ns=?,updated_at_ns=? '
                   'WHERE task_id=? AND group_id=?',
                   (next_stage, next_state, now if stage == 'nas' else None, now, task_id, group_id))


def observed_concurrency(database, task_id, stage):
    with closing(_db(database)) as db:
        rows = db.execute('SELECT started_at_ns,ended_at_ns FROM paperflow_rag_nas_intervals '
                          'WHERE task_id=? AND stage=?', (task_id, stage)).fetchall()
    events = []
    for row in rows:
        events.append((row['started_at_ns'], 1))
        if row['ended_at_ns'] is not None:
            events.append((row['ended_at_ns'], -1))
    active = peak = 0
    for _, delta in sorted(events):
        active += delta
        peak = max(peak, active)
    return {'peak': peak, 'intervals': len(rows),
            'open_intervals': sum(row['ended_at_ns'] is None for row in rows)}


def build_task(stage, run_id, execute):
    """Construct a real deferred Prefect task without starting it."""
    from prefect import task
    from prefect.cache_policies import CacheKeyFnPolicy
    from prefect.context import TaskRunContext
    identity = prefect_task_identity(stage, run_id)

    @task(name='论文组 RAG' if stage == 'rag' else '论文组 NAS 归档与清理',
          tags=[identity['tag']], retries=0, persist_result=True,
          cache_policy=CacheKeyFnPolicy(cache_key_fn=lambda _context, _parameters: None))
    async def folder_stage():
        context = TaskRunContext.get()
        if context is None or context.task_run is None:
            raise ValueError('a real Prefect task run is required')
        return await execute(stage, context.task_run)

    folder_stage.task_key = identity['task_key']
    return folder_stage


def make_worker(stages):
    """Subscribe to this batch of folder-stage task keys."""
    from prefect.task_worker import TaskWorker
    if not stages or len(stages) > 20:
        raise ValueError('a folder batch must contain 1 to 20 tasks')
    keys = [stage.task_key for stage in stages]
    if len(set(keys)) != len(keys):
        raise ValueError('folder task keys must be unique')
    worker = TaskWorker(*stages, limit=len(stages))
    for task, key in zip(worker.tasks, keys):
        task.task_key = key
    return worker


async def execute_prefect_stage(database, job, stage, operation, slot):
    """Execute one folder stage using the caller's existing Prefect service."""
    import asyncio
    from prefect.client.orchestration import get_client
    task_id, group_id = job['task_id'], job['group_id']
    execute = controlled_executor(database, task_id, group_id, operation, slot)
    task = build_task(stage, job[stage + '_task_run_id'], execute)
    worker = make_worker([task])
    async with worker:
        subscription = asyncio.create_task(worker.astart())
        try:
            await asyncio.sleep(0)
            def submit():
                with get_client(sync_client=True) as client:
                    return submit_stage(database, task_id, group_id, stage, client, task)
            await asyncio.to_thread(submit)
            while not worker.finished_task_runs.get(task.task_key, 0):
                await asyncio.sleep(1)
            def finish():
                with get_client(sync_client=True) as client:
                    actual = client.read_task_run(UUID(job[stage + '_task_run_id']))
                if not actual.state.is_completed():
                    raise RuntimeError('Prefect folder stage did not complete')
                advance_stage(database, task_id, group_id, stage, actual)
            await asyncio.to_thread(finish)
        finally:
            subscription.cancel()
            try:
                await subscription
            except asyncio.CancelledError:
                pass
