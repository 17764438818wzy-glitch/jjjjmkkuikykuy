"""Run an LLM stage task for an existing, suspended folder FlowRun.

The stage record is actual work, not another folder order. A durable reservation
is written before its deterministic Prefect TaskRun is submitted. Ambiguous
submissions, unknown model requests and failed executions are never replayed.
"""
from contextlib import closing
import asyncio
import logging
import os
from pathlib import Path
import sqlite3
import time
from uuid import UUID, uuid5

from filelock import FileLock, Timeout
import psutil
from prefect import task
from prefect.cache_policies import CacheKeyFnPolicy
from prefect.context import TaskRunContext

from .pause_admission import ControlStop


LOG = logging.getLogger(__name__)
TAG = 'paperflow-llm-stage-v1'
STAGE_TASK_KEY = 'paperflow-folder-llm-stage-v1'
STAGE_NAMESPACE = UUID('6aaf059a-c89a-52a8-acd5-4a928260a3b7')
MAX_STAGE_TASKS = 70
BLOCKING_STATES = ('recovering_folder', 'recovery_unknown', 'reserved', 'submitting', 'submitted',
                   'executing', 'submission_unknown', 'delivery_unknown', 'execution_unknown', 'identity_unknown')


def _no_cache(_context, _parameters):
    return None


def _connect(path):
    db = sqlite3.connect(path, timeout=30)
    db.row_factory = sqlite3.Row
    return db


def initialize(database):
    with closing(_connect(database)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        db.execute('''CREATE TABLE IF NOT EXISTS paperflow_llm_stage_jobs (
            job_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, group_id TEXT NOT NULL,
            paper_id TEXT NOT NULL, receipt_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
            parent_run_id TEXT NOT NULL, folder_run_id TEXT NOT NULL,
            generation INTEGER NOT NULL, task_key TEXT NOT NULL, state TEXT NOT NULL,
            error_type TEXT, created_at_ns INTEGER NOT NULL, updated_at_ns INTEGER NOT NULL,
            owner_pid INTEGER, owner_started REAL, name_reconciled INTEGER NOT NULL DEFAULT 0,
            retry_key TEXT NOT NULL DEFAULT '', UNIQUE(task_id, group_id, generation, retry_key)
        )''')
        columns = {r[1] for r in db.execute('PRAGMA table_info(paperflow_llm_stage_jobs)')}
        if 'retry_key' not in columns:
            # Preserve every old stage row while allowing each explicit retry
            # to have its own Prefect TaskRun. The DDL is one transaction.
            db.execute('''CREATE TABLE paperflow_llm_stage_jobs_retry_migration (
                job_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, group_id TEXT NOT NULL,
                paper_id TEXT NOT NULL, receipt_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
                parent_run_id TEXT NOT NULL, folder_run_id TEXT NOT NULL,
                generation INTEGER NOT NULL, task_key TEXT NOT NULL, state TEXT NOT NULL,
                error_type TEXT, created_at_ns INTEGER NOT NULL, updated_at_ns INTEGER NOT NULL,
                owner_pid INTEGER, owner_started REAL, name_reconciled INTEGER NOT NULL DEFAULT 0,
                retry_key TEXT NOT NULL DEFAULT '', UNIQUE(task_id, group_id, generation, retry_key)
            )''')
            db.execute('INSERT INTO paperflow_llm_stage_jobs_retry_migration '
                '(job_id,task_id,group_id,paper_id,receipt_id,attempt_id,parent_run_id,folder_run_id,'
                'generation,task_key,state,error_type,created_at_ns,updated_at_ns,owner_pid,owner_started,name_reconciled) '
                'SELECT job_id,task_id,group_id,paper_id,receipt_id,attempt_id,parent_run_id,folder_run_id,'
                'generation,task_key,state,error_type,created_at_ns,updated_at_ns,owner_pid,owner_started,name_reconciled '
                'FROM paperflow_llm_stage_jobs')
            db.execute('DROP TABLE paperflow_llm_stage_jobs')
            db.execute('ALTER TABLE paperflow_llm_stage_jobs_retry_migration RENAME TO paperflow_llm_stage_jobs')
        db.execute('''CREATE TABLE IF NOT EXISTS paperflow_llm_stage_barriers (
            task_id TEXT PRIMARY KEY, blocked_through_generation INTEGER NOT NULL
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS paperflow_llm_stage_meta (
            name TEXT PRIMARY KEY, value TEXT NOT NULL
        )''')
        # Installing the wiring must not silently start the user's current
        # generation. The user's next explicit task start advances generation.
        if not db.execute("SELECT 1 FROM paperflow_llm_stage_meta WHERE name='activation_barrier_v1'").fetchone():
            db.execute('INSERT INTO paperflow_llm_stage_barriers(task_id,blocked_through_generation) '
                       'SELECT task_id,start_generation FROM tasks')
            db.execute("INSERT INTO paperflow_llm_stage_meta VALUES('activation_barrier_v1','installed')")


def _identity(item):
    return {key: item[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id',
                                       'parent_run_id')} | {'task_run_id': item['folder_run_id']}


def _job_id(folder_id, generation, retry_key=''):
    suffix = ''
    if retry_key:
        suffix = ':manual:' + str(UUID(retry_key))
        if suffix != ':manual:' + retry_key:
            raise ValueError('人工重试编号无效')
    return str(uuid5(STAGE_NAMESPACE, str(UUID(folder_id)) + ':' + str(generation) + suffix))


def _move(database, job_id, before, after, error_type=None):
    marks = ','.join('?' for _ in before)
    with closing(_connect(database)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        return db.execute('UPDATE paperflow_llm_stage_jobs SET state=?,error_type=?,updated_at_ns=? '
                          'WHERE job_id=? AND state IN (' + marks + ')',
                          (after, error_type, time.time_ns(), job_id, *before)).rowcount == 1


def _job(database, job_id):
    with closing(_connect(database)) as db:
        row = db.execute('SELECT * FROM paperflow_llm_stage_jobs WHERE job_id=?', (job_id,)).fetchone()
    if not row:
        raise ValueError('LLM 阶段任务没有本地工单绑定')
    return dict(row)


def _has_other_unresolved_stage(db, task_id, group_id, job_id):
    marks = ','.join('?' for _ in BLOCKING_STATES)
    return bool(db.execute('SELECT 1 FROM paperflow_llm_stage_jobs WHERE task_id=? AND group_id=? '
                           'AND job_id<>? AND state IN (' + marks + ') LIMIT 1',
                           (task_id, group_id, job_id, *BLOCKING_STATES)).fetchone())


def _claim_job(database, job_id):
    pid = os.getpid()
    started = psutil.Process(pid).create_time()
    with closing(_connect(database)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        active = ("EXISTS(SELECT 1 FROM tasks t WHERE t.task_id=paperflow_llm_stage_jobs.task_id "
                  "AND t.desired_state='running' AND t.start_pending=0 "
                  "AND t.start_generation=paperflow_llm_stage_jobs.generation "
                  "AND t.task_status NOT IN ('deleting','delete_failed')) "
                  "AND NOT EXISTS(SELECT 1 FROM task_deletions d "
                  "WHERE d.task_id=paperflow_llm_stage_jobs.task_id)")
        claimed = db.execute("UPDATE paperflow_llm_stage_jobs SET state='executing',owner_pid=?,owner_started=?,"
            "updated_at_ns=? WHERE job_id=? AND state IN ('submitting','submitted') AND " + active,
            (pid, started, time.time_ns(), job_id)).rowcount == 1
        if not claimed:
            # If pause/deletion won before claim, record that this stage did
            # not start. A deletion-specific fence remains untouched.
            db.execute("UPDATE paperflow_llm_stage_jobs SET state='stopped',updated_at_ns=? "
                "WHERE job_id=? AND state IN ('submitting','submitted') AND NOT (" + active + ')',
                (time.time_ns(), job_id))
        return claimed


def _owner_dead(item):
    pid, started = item['owner_pid'], item['owner_started']
    if type(pid) is not int or not isinstance(started, (float, int)):
        return None
    try:
        return abs(psutil.Process(pid).create_time() - started) > .01
    except psutil.NoSuchProcess:
        return True
    except (psutil.AccessDenied, psutil.Error, OSError):
        return None


def _binding(settings, queue, database, client, item):
    import sys
    from paperflow_prefect.local_preprocessing import NAMESPACE
    from paperflow_prefect.registered_input import verify_registered_receipt
    from .upstream_flow_binding import UpstreamFlowBinding

    session = sys.modules[NAMESPACE + '.local_queue']._current_receiver_session
    return UpstreamFlowBinding(queue.path, database, client, str(settings['prefect_api_url']).rstrip('/'),
                               queue.consumer_id, verify_registered_receipt, receiver_session=session,
                               execution_task_run_id=item.get('job_id'),
                               execution_task_key=item.get('task_key') if item.get('job_id') else None,
                               allow_terminal_retry=bool(item.get('retry_key')))


def _folder_paused(client, item, *, allow_processing=True, allow_post=False):
    run = client.read_flow_run(UUID(item['folder_run_id']))
    phases = (('等待 LLM', '等待任务开始', 'LLM 处理中', 'LLM 失败待核对', 'LLM 阶段状态待核对') if allow_processing
              else ('等待 LLM', '等待任务开始', 'LLM 失败待核对'))
    if allow_post:
        phases += ('等待后处理',)
    if (not run.state or not run.state.is_paused() or run.state.state_details.pause_reschedule is not True
            or not any((run.name or '').endswith(phase) for phase in phases)):
        raise InterruptedError('文件夹工单尚未在 LLM 入口挂起')
    return run


def _folder_for_job(client, item, *, allow_processing=True, allow_post=False):
    """Accept a terminal folder only for an explicitly journaled retry.

    Callers must follow this with the full upstream binding check; this helper
    is only a state-shape check and does not authorize model execution.
    """
    if item.get('retry_key'):
        run = client.read_flow_run(UUID(item['folder_run_id']))
        if run.state and (run.state.is_failed() or run.state.is_crashed()):
            return run
    return _folder_paused(client, item, allow_processing=allow_processing, allow_post=allow_post)


def _rename(client, folder, phase):
    current = folder.name or ''
    label = current.rsplit(' · ', 1)[0]
    client.update_flow_run(folder.id, name=label + ' · ' + phase)


def _await_recorded_running(client, item, stop):
    # Prefect 3 records task state asynchronously from its event stream.
    from prefect.exceptions import ObjectNotFound
    deadline = time.monotonic() + 30
    while True:
        try:
            stage = client.read_task_run(UUID(item['job_id']))
        except ObjectNotFound:
            # The background queue may deliver before the creation
            # transaction becomes visible to a read-only client.
            if stop.is_set() or time.monotonic() >= deadline:
                raise InterruptedError('LLM 阶段工单尚未落库') from None
            time.sleep(.25)
            continue
        if (stage.flow_run_id not in (None, UUID(item['folder_run_id'])) or stage.task_key != item['task_key']
                or stage.dynamic_key != 'paperflow-llm-' + item['job_id']
                or TAG not in (stage.tags or [])):
            raise ValueError('LLM 阶段任务身份不可核验')
        if stage.state and stage.state.is_running():
            return
        if stop.is_set() or (stage.state and stage.state.is_final()) or time.monotonic() >= deadline:
            raise InterruptedError('LLM 阶段运行状态尚未确认')
        time.sleep(.25)


@task(name='资料文件夹 LLM 阶段', tags=[TAG], retries=0, persist_result=True,
      cache_policy=CacheKeyFnPolicy(cache_key_fn=_no_cache))
def execute_llm_stage():
    """Parameters live in the durable reservation, indexed by this TaskRun ID."""
    from prefect.client.orchestration import SyncPrefectClient
    from paperflow_prefect.local_preprocessing import _control_paths, _queue
    from paperflow_prefect.settings import Settings
    from .flow_runtime import run_live_llm_folder

    context = TaskRunContext.get()
    if context is None or context.task_run is None:
        raise ValueError('缺少真实的 Prefect LLM 阶段任务')
    job_id = str(context.task_run.id)
    database, _ = _control_paths()
    item = _job(database, job_id)
    if (item['job_id'] != _job_id(item['folder_run_id'], item['generation'], item['retry_key'])
            or item['task_key'] != execute_llm_stage.task_key
            or context.task_run.flow_run_id not in (None, UUID(item['folder_run_id']))
            or context.task_run.task_key != item['task_key']):
        raise ValueError('LLM 阶段任务身份不一致')
    if not _claim_job(database, job_id):
        if _job(database, job_id)['state'] == 'stopped':
            return {'status': 'paused'}
        raise ValueError('LLM 阶段任务已被领取或状态不可核验')
    try:
        settings = Settings({**Settings.load().values, 'execution_enabled': True})
        settings.require_execution()
        stop = ControlStop(database, item['task_id'], item['generation'], execution_task_run_id=job_id)
        if stop.is_set():
            _move(database, job_id, ('executing',), 'stopped')
            return {'status': 'paused'}
        lock_root = settings.path('data_root') / 'locks'
        api_url = str(settings['prefect_api_url']).rstrip('/')
        with FileLock(lock_root / ('folder-order-' + item['attempt_id'] + '.lock'),
                      timeout=30, thread_local=False):
            if stop.is_set():
                _move(database, job_id, ('executing',), 'stopped')
                return {'status': 'paused'}
            with closing(_connect(database)) as db:
                unresolved = db.execute("SELECT 1 FROM llm_request_admissions WHERE task_id=? AND task_run_id=? "
                    "AND state IN ('unknown','admitted','returned_unverified') LIMIT 1",
                    (item['task_id'], item['folder_run_id'])).fetchone()
                unresolved_stage = _has_other_unresolved_stage(db, item['task_id'], item['group_id'], job_id)
            if unresolved or unresolved_stage:
                raise ValueError('文件夹已有未确认的模型请求，禁止自动重发')
            queue = _queue()
            with SyncPrefectClient(api_url, httpx_settings={'timeout': 3, 'trust_env': False}) as client:
                _await_recorded_running(client, item, stop)
                folder = _folder_for_job(client, item)
                _binding(settings, queue, database, client, item).load(_identity(item), require_running=True)
                if stop.is_set():
                    _move(database, job_id, ('executing',), 'stopped')
                    return {'status': 'paused'}
                if folder.state.is_paused() and not item['retry_key']:
                    _rename(client, folder, 'LLM 处理中')
            result = run_live_llm_folder(settings, item['task_id'], item['group_id'], item['receipt_id'],
                        item['attempt_id'], item['folder_run_id'], execution_task_run_id=job_id,
                        execution_task_key=item['task_key'], expected_generation=item['generation'],
                        allow_terminal_retry=bool(item['retry_key']))
            if result.get('status') not in {'paused', 'waiting_postprocessing'}:
                raise ValueError('LLM 阶段返回未知状态')
            with SyncPrefectClient(api_url, httpx_settings={'timeout': 3, 'trust_env': False}) as client:
                folder = _folder_for_job(client, item)
                _binding(settings, queue, database, client, item).load(_identity(item), require_running=True)
                if folder.state.is_paused() and (not item['retry_key'] or result['status'] == 'waiting_postprocessing'):
                    _rename(client, folder, '等待后处理' if result['status'] == 'waiting_postprocessing' else '等待任务开始')
            # The raw ledger can be complete before Prefect has persisted the
            # TaskRun's final state. Leave successful work as executing until
            # the reconciler verifies a real COMPLETED TaskRun.
            if result['status'] == 'paused':
                _move(database, job_id, ('executing',), 'stopped')
            return result
    except BaseException as exc:
        with closing(_connect(database)) as db:
            ledger = db.execute('SELECT state FROM llm_folder_queue WHERE task_id=? AND group_id=?',
                                (item['task_id'], item['group_id'])).fetchone()
        _move(database, job_id, ('executing',),
              'execution_unknown' if ledger and ledger['state'] == 'waiting_postprocessing' else 'failed',
              type(exc).__name__)
        raise


# Prefect normally hashes the function's implementation into task_key. This
# explicit versioned key is stable and independently checkable by observers.
execute_llm_stage.task_key = STAGE_TASK_KEY


def _candidate_rows(queue, database):
    """Read retained, already handed-off folders; do not claim or mutate them."""
    import sys
    from paperflow_prefect.local_preprocessing import NAMESPACE
    with closing(queue._connect()) as db:
        db.execute('BEGIN')
        session = sys.modules[NAMESPACE + '.local_queue']._current_receiver_session(db)
        rows = [dict(row) for row in db.execute("""
            SELECT h.task_id,h.group_id,h.paper_id,h.receipt_id,h.attempt_id,
                   o.prefect_run_id AS folder_run_id
              FROM preprocessing_handoffs h
              JOIN groups g ON g.task_id=h.task_id AND g.group_id=h.group_id
                   AND g.receipt_id=h.receipt_id AND g.paper_id=h.paper_id
              JOIN preprocessing_folder_orders o ON o.task_id=h.task_id AND o.group_id=h.group_id
                   AND o.receipt_id=h.receipt_id AND o.attempt_id=h.attempt_id
             WHERE h.consumer_id='prefect-local-preprocessing' AND h.state='waiting'
               AND g.state='ready_for_llm' AND g.checked_session=? AND o.prefect_run_id IS NOT NULL
               AND NOT EXISTS(SELECT 1 FROM cancelled_tasks c WHERE c.task_id=h.task_id)
               AND EXISTS(SELECT 1 FROM retention_requests r WHERE r.task_id=g.task_id
                   AND r.group_id=g.group_id AND r.paper_id=g.paper_id AND r.receipt_id=g.receipt_id
                   AND r.consumer_id=? AND r.state='accepted' AND r.retention_id IS NOT NULL
                   AND r.last_error IS NULL AND r.checked_session=?) ORDER BY h.rowid
        """, (session, queue.consumer_id, session))]
    with closing(_connect(database)) as db:
        tasks = {r['task_id']: dict(r) for r in db.execute("SELECT task_id,prefect_run_id,start_generation "
            "FROM tasks WHERE prefect_order_model='v2' AND desired_state='running' AND start_pending=0 "
            "AND task_status NOT IN ('deleting','delete_failed') AND prefect_run_id IS NOT NULL "
            "AND NOT EXISTS(SELECT 1 FROM task_deletions d WHERE d.task_id=tasks.task_id)")}
        completed = {(r['task_id'], r['group_id']) for r in db.execute(
            "SELECT task_id,group_id FROM llm_folder_queue WHERE state='waiting_postprocessing'")}
        uncertain = {(r['task_id'], r['task_run_id']) for r in db.execute(
            "SELECT task_id,task_run_id FROM llm_request_admissions "
            "WHERE state IN ('admitted','returned_unverified','unknown')")}
        jobs = {(r['task_id'], r['group_id'], r['generation']) for r in db.execute(
            'SELECT task_id,group_id,generation FROM paperflow_llm_stage_jobs')}
        marks = ','.join('?' for _ in BLOCKING_STATES)
        blocked = {(r['task_id'], r['group_id']) for r in db.execute(
            'SELECT task_id,group_id FROM paperflow_llm_stage_jobs WHERE state IN (' + marks + ')',
            BLOCKING_STATES)}
        # A failed folder needs its own explicit retry even after a whole-task
        # pause/start advances the generation.
        failed = {(r['task_id'], r['group_id']) for r in db.execute(
            "SELECT task_id,group_id FROM paperflow_llm_stage_jobs WHERE state='failed' "
            "OR (state='stopped_before_submit' AND retry_key<>'')")}
        barriers = {r['task_id']: r['blocked_through_generation'] for r in db.execute(
            'SELECT task_id,blocked_through_generation FROM paperflow_llm_stage_barriers')}
    eligible = []
    for item in rows:
        task = tasks.get(item['task_id'])
        if (not task or (item['task_id'], item['group_id']) in completed
                or (item['task_id'], item['group_id']) in blocked
                or (item['task_id'], item['group_id']) in failed):
            continue
        if (item['task_id'], item['folder_run_id']) in uncertain:
            continue
        generation = task['start_generation']
        if (type(generation) is not int or generation <= barriers.get(item['task_id'], 0)
                or (item['task_id'], item['group_id'], generation) in jobs):
            continue
        item.update(parent_run_id=task['prefect_run_id'], generation=generation)
        eligible.append(item)
    return eligible


def _reserve(database, item):
    item = dict(item, job_id=_job_id(item['folder_run_id'], item['generation']),
                task_key=execute_llm_stage.task_key)
    with closing(_connect(database)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        task = db.execute('SELECT prefect_run_id,desired_state,start_generation,start_pending,task_status '
                          'FROM tasks WHERE task_id=?', (item['task_id'],)).fetchone()
        if (not task or task['prefect_run_id'] != item['parent_run_id'] or task['desired_state'] != 'running'
                or task['start_generation'] != item['generation'] or task['start_pending']
                or task['task_status'] in {'deleting', 'delete_failed'}
                or db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (item['task_id'],)).fetchone()):
            return None
        if db.execute("SELECT 1 FROM llm_request_admissions WHERE task_id=? AND task_run_id=? "
                      "AND state IN ('unknown','admitted','returned_unverified') LIMIT 1",
                      (item['task_id'], item['folder_run_id'])).fetchone():
            return None
        if _has_other_unresolved_stage(db, item['task_id'], item['group_id'], item['job_id']):
            return None
        # Repeat under the reservation transaction: a failed job may have
        # appeared after the candidate snapshot was taken.
        if db.execute("SELECT 1 FROM paperflow_llm_stage_jobs WHERE task_id=? AND group_id=? "
                      "AND (state='failed' OR (state='stopped_before_submit' AND retry_key<>'')) LIMIT 1",
                      (item['task_id'], item['group_id'])).fetchone():
            return None
        prior = db.execute('SELECT state FROM llm_folder_queue WHERE task_id=? AND group_id=?',
                           (item['task_id'], item['group_id'])).fetchone()
        if prior and prior['state'] == 'waiting_postprocessing':
            return None
        now = time.time_ns()
        db.execute('INSERT OR IGNORE INTO paperflow_llm_stage_jobs '
            '(job_id,task_id,group_id,paper_id,receipt_id,attempt_id,parent_run_id,folder_run_id,'
            'generation,task_key,state,error_type,created_at_ns,updated_at_ns) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (item['job_id'], item['task_id'], item['group_id'], item['paper_id'], item['receipt_id'],
             item['attempt_id'], item['parent_run_id'], item['folder_run_id'], item['generation'],
             item['task_key'], 'reserved', None, now, now))
        row = db.execute('SELECT * FROM paperflow_llm_stage_jobs WHERE job_id=?', (item['job_id'],)).fetchone()
        if not row or any(row[key] != item[key] for key in ('job_id','task_id','group_id','paper_id',
                'receipt_id','attempt_id','parent_run_id','folder_run_id','generation','task_key')):
            raise ValueError('LLM 阶段预约与文件夹工单冲突')
        return dict(row)


def _submit(client, database, item):
    """At most one create attempt; a lost outcome is read back, never resent."""
    from prefect.states import Scheduled
    from paperflow_prefect.local_preprocessing import _control_paths
    _, lock_root = _control_paths()
    # Hold the task gate only while recording the durable submission fence.
    # A slow Prefect call must not prevent pause/delete from recording intent.
    with FileLock(lock_root / ('preprocessing-' + item['task_id'] + '.lock'), timeout=10):
        if (ControlStop(database, item['task_id'], item['generation']).is_set()
                or (item.get('retry_key') and not _manual_reservation(database, item))):
            _move(database, item['job_id'], ('reserved',), 'stopped_before_submit')
            return False
        if not _move(database, item['job_id'], ('reserved',), 'submitting'):
            return False
    if ControlStop(database, item['task_id'], item['generation']).is_set():
        # This branch precedes the only create call in this function. A
        # deletion may already have fenced this exact submission, but it is
        # still known not to have reached Prefect.
        _move(database, item['job_id'], ('submitting', 'cancel_fenced_submitting'), 'stopped_before_submit')
        return False
    state = Scheduled()
    state.state_details.deferred = True
    run = client.create_task_run(task=execute_llm_stage, flow_run_id=UUID(item['folder_run_id']),
              dynamic_key='paperflow-llm-' + item['job_id'], id=UUID(item['job_id']),
              name='资料文件夹 LLM 阶段', state=state)
    if (str(run.id) != item['job_id'] or run.flow_run_id != UUID(item['folder_run_id'])
            or run.task_key != item['task_key'] or run.dynamic_key != 'paperflow-llm-' + item['job_id']
            or TAG not in (run.tags or [])):
        raise ValueError('Prefect LLM 阶段任务身份冲突')
    # A pause/delete can win while Prefect receives the request. Its durable
    # fence and the worker's atomic claim prevent a late task from sending.
    _move(database, item['job_id'], ('submitting',), 'submitted')
    return True


def _validate_stage_run(run, item):
    if (str(run.id) != item['job_id'] or run.flow_run_id not in (None, UUID(item['folder_run_id']))
            or run.task_key != item['task_key'] or run.dynamic_key != 'paperflow-llm-' + item['job_id']
            or TAG not in (run.tags or []) or run.state is None):
        raise ValueError('Prefect LLM 阶段任务身份不可核验')


def _manual_reservation(database, item):
    """Read the persisted user action and recheck the task, without sending."""
    with closing(_connect(database)) as db:
        command = db.execute('SELECT c.state AS command_state,i.state AS item_state '
            'FROM paperflow_llm_retry_commands c JOIN paperflow_llm_retry_items i '
            'ON i.request_id=c.request_id WHERE c.request_id=? AND i.task_id=? AND i.group_id=? '
            'AND i.paper_id=? AND i.receipt_id=? AND i.attempt_id=? AND i.folder_run_id=? '
            'AND i.job_id=? AND i.generation=?',
            (item['retry_key'], item['task_id'], item['group_id'], item['paper_id'], item['receipt_id'],
             item['attempt_id'], item['folder_run_id'], item['job_id'], item['generation'])).fetchone()
        task = db.execute('SELECT prefect_run_id,desired_state,start_generation,start_pending,task_status '
                          'FROM tasks WHERE task_id=?', (item['task_id'],)).fetchone()
        deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (item['task_id'],)).fetchone()
        ledger = db.execute('SELECT state FROM llm_folder_queue WHERE task_id=? AND group_id=?',
                            (item['task_id'], item['group_id'])).fetchone()
        unknown = db.execute("SELECT 1 FROM llm_request_admissions WHERE task_id=? AND task_run_id=? "
            "AND state IN ('unknown','admitted','returned_unverified') LIMIT 1",
            (item['task_id'], item['folder_run_id'])).fetchone()
        other = _has_other_unresolved_stage(db, item['task_id'], item['group_id'], item['job_id'])
    # The accepted item is durable independently of its parent bulk command;
    # a service restart must not silently discard an already queued folder.
    return bool(command and command['item_state'] == 'submitted' and task and not deleted
                and task['prefect_run_id'] == item['parent_run_id'] and task['desired_state'] == 'running'
                and task['start_generation'] == item['generation'] and not task['start_pending']
                and task['task_status'] not in {'deleting', 'delete_failed'} and ledger
                and ledger['state'] in {'waiting_llm', 'llm_processing'} and not unknown and not other)


def _reconcile_jobs(client, database, settings, queue):
    """Recover safe reservations and fence ambiguous delivery; never create twice."""
    from prefect.exceptions import ObjectNotFound
    with closing(_connect(database)) as db:
        rows = [dict(row) for row in db.execute("SELECT * FROM paperflow_llm_stage_jobs WHERE state IN "
                 "('reserved','submitting','submitted','executing') ORDER BY created_at_ns")]
        barriers = {r['task_id']: r['blocked_through_generation'] for r in db.execute(
            'SELECT task_id,blocked_through_generation FROM paperflow_llm_stage_barriers')}
        active = db.execute("SELECT COUNT(*) FROM paperflow_llm_stage_jobs WHERE state IN "
                            "('submitting','submitted','executing')").fetchone()[0]
    for item in rows:
        try:
            if item['state'] == 'reserved':
                manual = bool(item['retry_key'])
                if ((not manual and item['generation'] <= barriers.get(item['task_id'], 0))
                        or ControlStop(database, item['task_id'], item['generation']).is_set()):
                    _move(database, item['job_id'], ('reserved',), 'stopped_before_submit')
                    continue
                if active >= MAX_STAGE_TASKS:
                    continue
                lock = settings.path('data_root') / 'locks' / ('folder-order-' + item['attempt_id'] + '.lock')
                with FileLock(lock, timeout=0, thread_local=False):
                    _folder_for_job(client, item, allow_processing=False)
                    _binding(settings, queue, database, client, item).load(_identity(item))
                    reservation = (item if _manual_reservation(database, item) else None) if manual else _reserve(database, item)
                # A late Prefect response must not hold the folder lock and
                # block deletion or the worker that received that TaskRun.
                if reservation and reservation['state'] == 'reserved':
                    # Even if remote creation raises, it may have succeeded.
                    active += 1
                    if not _submit(client, database, reservation):
                        active -= 1
                elif reservation is None:
                    _move(database, item['job_id'], ('reserved',), 'stopped_before_submit')
                continue
            try:
                run = client.read_task_run(UUID(item['job_id']))
            except ObjectNotFound:
                _move(database, item['job_id'], (item['state'],),
                      'execution_unknown' if item['state'] == 'executing' else 'submission_unknown',
                      'PrefectNotFound')
                continue
            try:
                _validate_stage_run(run, item)
            except ValueError:
                _move(database, item['job_id'], (item['state'],), 'identity_unknown', 'PrefectIdentityMismatch')
                continue
            if item['state'] == 'submitting':
                _move(database, item['job_id'], ('submitting',), 'submitted')
            elif item['state'] in {'submitted', 'executing'} and run.state.is_final():
                with closing(_connect(database)) as db:
                    row = db.execute('SELECT state FROM llm_folder_queue WHERE task_id=? AND group_id=?',
                                     (item['task_id'], item['group_id'])).fetchone()
                raw_complete = bool(row and row['state'] == 'waiting_postprocessing')
                if run.state.is_completed() and raw_complete:
                    status = 'finished'
                elif (run.state.is_failed() or run.state.is_crashed()) and not raw_complete:
                    status = 'failed'
                else:
                    status = 'execution_unknown'
                _move(database, item['job_id'], (item['state'],), status, 'TerminalReconciliation')
            elif item['state'] == 'executing' and _owner_dead(item) is True:
                _move(database, item['job_id'], ('executing',), 'execution_unknown', 'ExecutorExited')
            elif item['state'] == 'submitted' and run.state.is_running():
                if time.time_ns() - item['updated_at_ns'] > 60_000_000_000:
                    _move(database, item['job_id'], ('submitted',), 'execution_unknown', 'UnclaimedRunning')
            elif item['state'] == 'submitted' and (run.state.is_scheduled() or run.state.is_pending()):
                # A lost background-queue message cannot safely be sent again
                # while a worker may already have accepted it. Fence this job;
                # a late worker will fail the atomic claim before any request.
                if time.time_ns() - item['updated_at_ns'] > 120_000_000_000:
                    _move(database, item['job_id'], ('submitted',), 'delivery_unknown', 'ScheduledTooLong')
        except (Timeout, InterruptedError):
            continue
        except Exception:
            LOG.exception('LLM 阶段状态暂不可核验')


def _reconcile_failed_folder_names(client, database, settings, queue):
    """Repair only the verified folder label; never execute work here."""
    with closing(_connect(database)) as db:
        rows = [dict(row) for row in db.execute("""
            SELECT j.* FROM paperflow_llm_stage_jobs j JOIN tasks t ON t.task_id=j.task_id
             WHERE j.state IN ('failed','submission_unknown','delivery_unknown','execution_unknown','identity_unknown')
               AND j.name_reconciled=0
               AND NOT EXISTS(SELECT 1 FROM task_deletions d WHERE d.task_id=j.task_id)
               AND NOT EXISTS(SELECT 1 FROM paperflow_llm_stage_jobs n WHERE n.task_id=j.task_id
                              AND n.group_id=j.group_id AND n.state NOT IN ('stopped','stopped_before_submit')
                              AND (n.generation>j.generation OR
                                   (n.generation=j.generation AND n.created_at_ns>j.created_at_ns)))
        """)]
    for item in rows:
        try:
            # Manual retries preserve the original folder label while active;
            # a failed retry therefore has no renamed label to repair.
            if item.get('retry_key'):
                continue
            lock = settings.path('data_root') / 'locks' / ('folder-order-' + item['attempt_id'] + '.lock')
            with FileLock(lock, timeout=0, thread_local=False):
                with closing(_connect(database)) as db:
                    deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?',
                                         (item['task_id'],)).fetchone()
                    latest = db.execute('SELECT state FROM paperflow_llm_stage_jobs WHERE job_id=?',
                                        (item['job_id'],)).fetchone()
                    newer = db.execute('SELECT 1 FROM paperflow_llm_stage_jobs WHERE task_id=? '
                        "AND group_id=? AND state NOT IN ('stopped','stopped_before_submit') "
                        'AND (generation>? OR (generation=? AND created_at_ns>?))',
                        (item['task_id'], item['group_id'], item['generation'], item['generation'],
                         item['created_at_ns'])).fetchone()
                    ledger = db.execute('SELECT state FROM llm_folder_queue WHERE task_id=? AND group_id=?',
                                        (item['task_id'], item['group_id'])).fetchone()
                if deleted or newer or not latest or latest['state'] != item['state']:
                    continue
                folder = _folder_paused(client, item, allow_post=True)
                _binding(settings, queue, database, client, item).load(_identity(item))
                phase = ('等待后处理' if ledger and ledger['state'] == 'waiting_postprocessing' else
                         'LLM 失败待核对' if item['state'] == 'failed' else 'LLM 阶段状态待核对')
                if not (folder.name or '').endswith(phase):
                    _rename(client, folder, phase)
                with closing(_connect(database)) as db, db:
                    db.execute('UPDATE paperflow_llm_stage_jobs SET name_reconciled=1 '
                               'WHERE job_id=? AND state=?', (item['job_id'], item['state']))
        except (Timeout, InterruptedError):
            continue
        except Exception:
            LOG.exception('LLM 文件夹状态名称暂不可核验')


def dispatch_once():
    """One live dispatcher pass; requires the external dispatcher lock."""
    from prefect.client.orchestration import SyncPrefectClient
    from paperflow_prefect.local_preprocessing import _control_paths, _queue
    from paperflow_prefect.settings import Settings
    database, _ = _control_paths()
    settings = Settings({**Settings.load().values, 'execution_enabled': True})
    settings.require_execution()
    queue = _queue()
    api_url = str(settings['prefect_api_url']).rstrip('/')
    with SyncPrefectClient(api_url, httpx_settings={'timeout': 3, 'trust_env': False}) as client:
        _reconcile_jobs(client, database, settings, queue)
        _reconcile_failed_folder_names(client, database, settings, queue)
        candidates = _candidate_rows(queue, database)
        with closing(_connect(database)) as db:
            active = db.execute("SELECT COUNT(*) FROM paperflow_llm_stage_jobs WHERE state IN "
                                "('reserved','submitting','submitted','executing')").fetchone()[0]
        if active >= MAX_STAGE_TASKS:
            return
        available = MAX_STAGE_TASKS - active
        for item in candidates:
            if available <= 0:
                break
            try:
                lock = settings.path('data_root') / 'locks' / ('folder-order-' + item['attempt_id'] + '.lock')
                with FileLock(lock, timeout=0, thread_local=False):
                    _folder_paused(client, item, allow_processing=False)
                    _binding(settings, queue, database, client, item).load(_identity(item))
                    reserved = _reserve(database, item)
                if reserved and reserved['state'] == 'reserved':
                    available -= 1
                    _submit(client, database, reserved)
            except (Timeout, InterruptedError):
                continue
            except Exception:
                LOG.exception('LLM 文件夹预约未完成；不会自动重发')


async def serve_stage():
    """Run the real Prefect task worker and its sole durable dispatcher."""
    from prefect.task_worker import TaskWorker
    from prefect.settings import PREFECT_API_URL
    from paperflow_prefect.local_preprocessing import _control_paths, FIRST_STEP_CONFIG
    from paperflow_prefect.records import read_json
    from paperflow_prefect.settings import Settings
    settings = Settings.load()
    api_url = str(settings['prefect_api_url']).rstrip('/')
    expected = str(read_json(FIRST_STEP_CONFIG).get('prefect_api_url', '')).rstrip('/')
    if not api_url or api_url != expected or str(PREFECT_API_URL.value()).rstrip('/') != api_url:
        raise RuntimeError('LLM 阶段未连接到当前的新 Prefect')
    database, lock_root = _control_paths()
    # Hold the dispatcher lock before any schema initialization or migration.
    # Otherwise a second process could rebuild the shared table before it
    # notices that the current live dispatcher still owns the lock.
    dispatcher_lock = FileLock(lock_root / 'llm-stage-dispatcher.lock', timeout=0, thread_local=False)
    dispatcher_lock.acquire()
    try:
        initialize(database)
        worker = TaskWorker(execute_llm_stage, limit=MAX_STAGE_TASKS)
        # TaskWorker clones definitions through with_options(), which would
        # otherwise regenerate a code-hash key and miss this subscription.
        if len(worker.tasks) != 1:
            raise RuntimeError('LLM 阶段执行器注册数量不一致')
        worker.tasks[0].task_key = STAGE_TASK_KEY
        async def dispatch_loop():
            while True:
                try:
                    await asyncio.to_thread(dispatch_once)
                except Exception:
                    LOG.exception('LLM 阶段派发暂不可用')
                await asyncio.sleep(2)
        await asyncio.gather(worker.astart(), dispatch_loop())
    finally:
        dispatcher_lock.release()
