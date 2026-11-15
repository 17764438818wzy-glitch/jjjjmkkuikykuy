"""Experimental server-side pause fence on the existing execution ledger.

An INSERT trigger shares the *existing* ledger transaction with every version
of RemoteJobs, including old versions. It adds no snapshot/transfer lock and
does not stop or kill work already admitted before the close. This isolated
module is not installed, exposed through SSH, or wired to a task-control page.
"""
from contextlib import closing
import json
import time

from remote_jobs import RemoteJobs, _uuid, _validated


class FenceError(RuntimeError):
    pass


TABLE = """CREATE TABLE task_generation_fences (
    task_id TEXT PRIMARY KEY,
    closed_through INTEGER NOT NULL CHECK(typeof(closed_through)='integer' AND closed_through>=1),
    updated_at_ns INTEGER NOT NULL
)"""

TRIGGER = """CREATE TRIGGER paperflow_postprocess_task_fence
BEFORE INSERT ON executor_jobs
BEGIN
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM executor_jobs WHERE job_id=NEW.job_id OR claim_id=NEW.claim_id
          OR task_run_id=NEW.task_run_id OR (task_id=NEW.task_id AND group_id=NEW.group_id)
    ) THEN RAISE(ABORT, 'postprocessing execution identity already exists') END;
    SELECT CASE WHEN json_valid(NEW.frozen_json) != 1
        THEN RAISE(ABORT, 'postprocessing execution identity is invalid') END;
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM json_tree(NEW.frozen_json) WHERE typeof(key)='text'
        GROUP BY parent,key HAVING COUNT(*)>1
    ) THEN RAISE(ABORT, 'postprocessing execution identity has duplicate keys') END;
    SELECT CASE WHEN json_type(NEW.frozen_json) IS NOT 'object'
        OR json_type(NEW.frozen_json,'$.identity') IS NOT 'object'
        OR (SELECT COUNT(*) FROM json_each(NEW.frozen_json)) != 7
        OR (SELECT COUNT(*) FROM json_each(NEW.frozen_json,'$.identity')) != 7
        OR EXISTS (SELECT 1 FROM json_each(NEW.frozen_json)
            WHERE key NOT IN ('job_id','claim_id','identity','generation',
                              'manifest_sha256','snapshot_sha256','code_sha256'))
        OR EXISTS (SELECT 1 FROM json_each(NEW.frozen_json,'$.identity')
            WHERE key NOT IN ('task_id','group_id','paper_id','receipt_id','attempt_id',
                              'parent_run_id','task_run_id'))
        THEN RAISE(ABORT, 'postprocessing execution identity has unexpected keys') END;
    SELECT CASE WHEN json_type(NEW.frozen_json, '$.generation') IS NOT 'integer'
        OR json_extract(NEW.frozen_json, '$.generation') < 1
        OR json_type(NEW.frozen_json, '$.identity.task_id') IS NOT 'text'
        OR json_extract(NEW.frozen_json, '$.identity.task_id') IS NOT NEW.task_id
        THEN RAISE(ABORT, 'postprocessing task generation is invalid') END;
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM task_generation_fences
        WHERE task_id=NEW.task_id
          AND closed_through >= json_extract(NEW.frozen_json, '$.generation')
    ) THEN RAISE(ABORT, 'postprocessing task generation is closed') END;
END"""

IMMUTABLE_TRIGGER = """CREATE TRIGGER paperflow_postprocess_frozen_identity
BEFORE UPDATE OF job_id,claim_id,task_id,group_id,task_run_id,frozen_json ON executor_jobs
WHEN NEW.job_id IS NOT OLD.job_id OR NEW.claim_id IS NOT OLD.claim_id
    OR NEW.task_id IS NOT OLD.task_id OR NEW.group_id IS NOT OLD.group_id
    OR NEW.task_run_id IS NOT OLD.task_run_id OR NEW.frozen_json IS NOT OLD.frozen_json
BEGIN
    SELECT RAISE(ABORT, 'postprocessing execution identity is immutable');
END"""
DELETE_TRIGGER = """CREATE TRIGGER paperflow_postprocess_preserve_jobs
BEFORE DELETE ON executor_jobs
BEGIN
    SELECT RAISE(ABORT, 'postprocessing execution history is immutable');
END"""
FENCE_UPDATE_TRIGGER = """CREATE TRIGGER paperflow_postprocess_fence_monotonic
BEFORE UPDATE ON task_generation_fences
WHEN NEW.task_id IS NOT OLD.task_id OR NEW.closed_through < OLD.closed_through
BEGIN
    SELECT RAISE(ABORT, 'postprocessing fence cannot move backwards');
END"""
FENCE_DELETE_TRIGGER = """CREATE TRIGGER paperflow_postprocess_preserve_fences
BEFORE DELETE ON task_generation_fences
BEGIN
    SELECT RAISE(ABORT, 'postprocessing fence cannot be deleted');
END"""
FENCE_INSERT_TRIGGER = """CREATE TRIGGER paperflow_postprocess_no_replace_fences
BEFORE INSERT ON task_generation_fences
WHEN EXISTS(SELECT 1 FROM task_generation_fences WHERE task_id=NEW.task_id)
BEGIN
    SELECT RAISE(ABORT, 'postprocessing fence cannot be replaced');
END"""

# Index the frozen identity as well as malformed identities. A damaged row
# cannot hide behind a different relational task_id, and normal unrelated
# tasks need not be scanned while a pause reads its consistent snapshot.
FROZEN_TASK = ("CASE WHEN json_valid(frozen_json)=1 THEN CASE WHEN "
               "json_type(frozen_json,'$.identity.task_id')='text' THEN "
               "json_extract(frozen_json,'$.identity.task_id') END END")
INVALID_TASK = ("CASE WHEN json_valid(frozen_json)=0 THEN 1 WHEN "
                "json_type(frozen_json,'$.identity.task_id') IS NOT 'text' THEN 1 ELSE 0 END")
FROZEN_INDEX = 'CREATE INDEX fence_frozen_task_idx ON executor_jobs(' + FROZEN_TASK + ')'
INVALID_INDEX = 'CREATE INDEX fence_invalid_task_idx ON executor_jobs(' + INVALID_TASK + ')'
INVALID_STATE_INDEX = ("CREATE INDEX fence_invalid_state_idx ON executor_jobs(state) WHERE "
                       "state NOT IN ('reserved','running','unknown','finished')")
STATUS_JOBS_QUERY = ('SELECT * FROM executor_jobs WHERE job_id IN ('
    'SELECT job_id FROM executor_jobs WHERE task_id=? UNION '
    'SELECT job_id FROM executor_jobs WHERE ' + FROZEN_TASK + '=? UNION '
    'SELECT job_id FROM executor_jobs WHERE ' + INVALID_TASK + '=1)')


def _normalized(sql):
    return ' '.join(sql.strip().rstrip(';').split())


def _verify(db):
    expected = {'task_generation_fences': ('table', TABLE),
                'paperflow_postprocess_task_fence': ('trigger', TRIGGER),
                'paperflow_postprocess_frozen_identity': ('trigger', IMMUTABLE_TRIGGER),
                'paperflow_postprocess_preserve_jobs': ('trigger', DELETE_TRIGGER),
                'paperflow_postprocess_fence_monotonic': ('trigger', FENCE_UPDATE_TRIGGER),
                'paperflow_postprocess_preserve_fences': ('trigger', FENCE_DELETE_TRIGGER),
                'paperflow_postprocess_no_replace_fences': ('trigger', FENCE_INSERT_TRIGGER),
                'fence_frozen_task_idx': ('index', FROZEN_INDEX),
                'fence_invalid_task_idx': ('index', INVALID_INDEX),
                'fence_invalid_state_idx': ('index', INVALID_STATE_INDEX)}
    rows = db.execute("SELECT type,name,sql FROM sqlite_master WHERE name IN (?,?,?,?,?,?,?,?,?,?)",
                      tuple(expected)).fetchall()
    actual = {row['name']: (row['type'], _normalized(row['sql'] or '')) for row in rows}
    wanted = {name: (kind, _normalized(sql)) for name, (kind, sql) in expected.items()}
    if actual != wanted:
        raise FenceError('server task fence schema is missing or changed')


def _validate_row(job):
    try:
        decoded = json.loads(job['frozen_json'])
        valid = (_validated(decoded) == job['frozen_json']
            and decoded['identity']['task_id'] == job['task_id']
            and decoded['job_id'] == job['job_id'] and decoded['claim_id'] == job['claim_id']
            and decoded['identity']['group_id'] == job['group_id']
            and decoded['identity']['task_run_id'] == job['task_run_id']
            and job['state'] in ('reserved', 'running', 'unknown', 'finished'))
    except (ValueError, TypeError, KeyError):
        valid = False
    if not valid:
        raise FenceError('task has a malformed or mismatched execution record')
    return decoded


def check_business_gate(db, item):
    """Check the frozen job and pause fence inside the supervisor transaction.

    The caller must use the same ledger transaction for this check and the
    one-byte business gate release. This callback does not itself authenticate
    a pause request, install a server command, or stop an already released job.
    """
    if not db.in_transaction:
        raise FenceError('business gate requires the existing ledger transaction')
    frozen = _validated(item)
    _verify(db)
    row = db.execute('SELECT * FROM executor_jobs WHERE job_id=?', (item['job_id'],)).fetchone()
    if row is None or _validate_row(row) != item or row['frozen_json'] != frozen or row['state'] != 'running':
        raise FenceError('the original running job identity changed')
    task = item['identity']['task_id']
    fence = db.execute('SELECT closed_through FROM task_generation_fences WHERE task_id=?', (task,)).fetchone()
    if fence is not None and (type(fence['closed_through']) is not int or fence['closed_through'] < 1
            or item['generation'] <= fence['closed_through']):
        raise FenceError('the original task generation is closed')
    return True


def _count_snapshot(jobs, task_id, closed):
    counts = {state: 0 for state in ('reserved', 'running', 'unknown', 'finished')}
    for job in jobs:
        decoded = _validate_row(job)
        if job['task_id'] != task_id:
            raise FenceError('task has a malformed or mismatched execution record')
        if decoded['generation'] <= closed:
            counts[job['state']] += 1
    return counts


class TaskGenerationFence:
    def __init__(self, ledger):
        if not isinstance(ledger, RemoteJobs):
            raise FenceError('the original pinned server ledger is required')
        self.ledger = ledger

    def install(self):
        """Explicit additive install; never rebuild or overwrite existing objects."""
        with closing(self.ledger._connect()) as db, db:
            names = ('task_generation_fences', 'paperflow_postprocess_task_fence',
                     'paperflow_postprocess_frozen_identity', 'paperflow_postprocess_preserve_jobs',
                     'paperflow_postprocess_fence_monotonic', 'paperflow_postprocess_preserve_fences',
                     'paperflow_postprocess_no_replace_fences',
                     'fence_frozen_task_idx', 'fence_invalid_task_idx', 'fence_invalid_state_idx')
            version = db.execute('PRAGMA data_version').fetchone()[0]
            db.execute('BEGIN')
            rows = db.execute('SELECT name FROM sqlite_master WHERE name IN (?,?,?,?,?,?,?,?,?,?)', names).fetchall()
            if rows:
                _verify(db)
                return
            legacy = db.execute('SELECT * FROM executor_jobs').fetchall()
            db.rollback()
            # Verify existing frozen identities once without holding a read or
            # write transaction. Refuse installation if anything changed.
            for job in legacy:
                _validate_row(job)
            db.execute('BEGIN IMMEDIATE')
            if db.execute('PRAGMA data_version').fetchone()[0] != version:
                raise FenceError('server ledger changed while verifying existing jobs')
            db.execute(TABLE)
            db.execute(TRIGGER)
            db.execute(IMMUTABLE_TRIGGER)
            db.execute(DELETE_TRIGGER)
            db.execute(FENCE_UPDATE_TRIGGER)
            db.execute(FENCE_DELETE_TRIGGER)
            db.execute(FENCE_INSERT_TRIGGER)
            db.execute(FROZEN_INDEX)
            db.execute(INVALID_INDEX)
            db.execute(INVALID_STATE_INDEX)
            _verify(db)

    def close(self, task_id, generation):
        """Close this and earlier generations before observing existing work.

        A missing acknowledgement permits repeating this monotonic close, but
        says nothing about whether earlier admitted jobs have stopped. Network
        transmission and physical pause completion are outside this module.
        """
        if not _uuid(task_id) or type(generation) is not int or not 1 <= generation < 2**63:
            raise FenceError('canonical task and positive integer generation required')
        with closing(self.ledger._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            _verify(db)
            now = time.time_ns()
            changed = db.execute('UPDATE task_generation_fences SET '
                'closed_through=MAX(closed_through,?),updated_at_ns=? WHERE task_id=?',
                (generation, now, task_id)).rowcount
            if not changed:
                db.execute('INSERT INTO task_generation_fences VALUES(?,?,?)', (task_id, generation, now))
        return self.status(task_id)

    def status(self, task_id):
        if not _uuid(task_id):
            raise FenceError('canonical task identity required')
        with closing(self.ledger._connect()) as db:
            db.execute('BEGIN')
            _verify(db)
            schema_version = db.execute('PRAGMA schema_version').fetchone()[0]
            row = db.execute('SELECT closed_through FROM task_generation_fences WHERE task_id=?',
                             (task_id,)).fetchone()
            if row is None or type(row['closed_through']) is not int or row['closed_through'] < 1:
                raise FenceError('task has no verified closed generation')
            closed = row['closed_through']
            # Include rows whose frozen identity names this task even if the
            # relational task_id was damaged. Invalid JSON/identity is also
            # uncertain, never evidence of an empty task.
            jobs = db.execute(STATUS_JOBS_QUERY, (task_id, task_id)).fetchall()
            # Copy the consistent SQLite snapshot, then release its read
            # transaction before expensive Python/JSON validation. In DELETE
            # journal mode a slow validator must not hold up unrelated writes.
            db.rollback()
            counts = _count_snapshot(jobs, task_id, closed)
            # This only detects changes during the observation. Controlling
            # who may alter schema after return remains an installation duty.
            if db.execute('PRAGMA schema_version').fetchone()[0] != schema_version:
                raise FenceError('server task fence schema changed during observation')
            return dict(task_id=task_id, closed_through=closed, jobs=counts,
                        stopped_or_terminated_not_proven=True)
