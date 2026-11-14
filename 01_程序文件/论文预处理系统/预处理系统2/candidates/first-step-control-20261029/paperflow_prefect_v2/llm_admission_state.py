"""Candidate shared view of model admissions for pause and deletion."""
import hashlib
import json
import time
from uuid import UUID


POST_JOB_STATES = {'reserved', 'submitting', 'submitted', 'executing', 'submission_unknown',
                   'execution_unknown', 'identity_unknown', 'delivery_unknown', 'finished', 'failed',
                   'stopped_before_submit', 'cancel_fenced_reserved', 'cancel_fenced_submitting',
                   'cancel_fenced_submitted'}
POST_CLAIM_STATES = {'processing', 'unknown', 'failed', 'receipt_held', 'waiting_rag'}
POST_RESULT_STATES = {'unresolved', 'passed', 'failed', 'unrecognized'}
IDENTITY = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')


def postprocess_task_key(job_id):
    """Match runtime_next.stage_runtime.task_key_for_job exactly."""
    try:
        parsed = UUID(job_id)
        if str(parsed) != job_id:
            raise ValueError('noncanonical UUID')
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError('后处理阶段编号不可核验') from exc
    return 'paperflow-folder-postprocess-stage-v1-' + parsed.hex


def fence_postprocessing(db, task_id):
    """Fence queued work inside the caller's existing task-gate transaction.

    Never wait for the folder lock here: the worker takes folder then task.
    A possibly submitted TaskRun is fenced from the worker's CAS claim, but is
    still reported unknown until its external outcome is reconciled.
    """
    rows = db.execute('SELECT state FROM paperflow_postprocess_stage_jobs WHERE task_id=?', (task_id,)).fetchall()
    if any(row[0] not in POST_JOB_STATES for row in rows):
        raise ValueError('后处理阶段状态不可核验')
    now = time.time_ns()
    db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='stopped_before_submit',updated_at_ns=? "
               "WHERE task_id=? AND state='reserved'", (now, task_id))
    db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='cancel_fenced_' || state,updated_at_ns=? "
               "WHERE task_id=? AND state IN ('submitting','submitted')", (now, task_id))


def _postprocessing_counts(db, task_id):
    # Missing schema is not an empty or safe queue. All reads are performed on
    # the same connection/transaction as the caller's task-state check.
    columns = {row[1] for row in db.execute('PRAGMA table_info(paperflow_postprocess_stage_jobs)')}
    if 'source_llm_generation' not in columns:
        raise ValueError('后处理原 LLM 代次字段缺失')
    jobs = {r['group_id']: r for r in db.execute(
        'SELECT * FROM paperflow_postprocess_stage_jobs WHERE task_id=?', (task_id,))}
    folders = {r['group_id']: r for r in db.execute(
        'SELECT * FROM llm_folder_queue WHERE task_id=?', (task_id,))}
    claims = {r['group_id']: r for r in db.execute(
        'SELECT * FROM postprocessing_claims WHERE task_id=?', (task_id,))}
    results = {r['group_id']: r for r in db.execute(
        'SELECT * FROM postprocessing_results WHERE task_id=?', (task_id,))}
    db.execute('SELECT 1 FROM postprocessing_receipts LIMIT 1').fetchone()
    inflight = unknown = 0
    for group in set(jobs) | set(claims) | set(results):
        job, claim, result = jobs.get(group), claims.get(group), results.get(group)
        if ((job and job['state'] not in POST_JOB_STATES)
                or (claim and claim['state'] not in POST_CLAIM_STATES)
                or (result and result['state'] not in POST_RESULT_STATES)):
            raise ValueError('后处理阶段状态不可核验')
        if not job:
            unknown += 1
            continue
        folder = folders.get(group)
        source, generation = job['source_llm_generation'], job['generation']
        if (type(source) is not int or type(generation) is not int or not 1 <= source <= generation
                or not folder or folder['generation'] != source or folder['state'] != 'waiting_postprocessing'
                or any(folder[k] != job[k] for k in IDENTITY)
                or folder['manifest_sha256'] != job['manifest_sha256']):
            unknown += 1
            continue
        try:
            folder_id = UUID(job['folder_run_id'])
            if str(folder_id) != job['folder_run_id']:
                raise ValueError('noncanonical folder identity')
            if (job['task_run_id'] != job['folder_run_id']
                    or job['task_key'] != postprocess_task_key(job['job_id'])):
                raise ValueError('job identity mismatch')
        except (TypeError, ValueError, AttributeError):
            unknown += 1
            continue
        if ((claim and (any(claim[k] != job[k] for k in IDENTITY)
                        or claim['manifest_sha256'] != job['manifest_sha256']))
                or (result and (any(result[k] != job[k] for k in IDENTITY)
                                or result['manifest_sha256'] != job['manifest_sha256']))):
            unknown += 1
            continue
        state = job['state']
        if state in {'reserved', 'submitting', 'submitted', 'executing'}:
            if claim and (state != 'executing' or claim['state'] in {'unknown', 'failed'}):
                unknown += 1
            else:
                inflight += 1
        elif state in {'stopped_before_submit', 'cancel_fenced_reserved'}:
            unknown += bool(claim or result)
        elif state == 'finished':
            if not claim or claim['state'] not in {'receipt_held', 'waiting_rag'} or not result or result['state'] != 'passed':
                unknown += 1
                continue
            saved = db.execute('SELECT receipt_json,receipt_sha256 FROM postprocessing_receipts WHERE claim_id=?',
                               (claim['claim_id'],)).fetchone()
            try:
                if (not saved or result['claim_id'] != claim['claim_id']
                        or result['stage_receipt_sha256'] != saved['receipt_sha256']
                        or hashlib.sha256(saved['receipt_json'].encode('utf8')).hexdigest() != saved['receipt_sha256']):
                    raise ValueError('receipt mismatch')
                receipt = json.loads(saved['receipt_json'])
                if (receipt.get('schema') != 'paperflow-postprocessing-receipt/1'
                        or receipt.get('validation_status') != 'passed'
                        or receipt.get('claim_id') != claim['claim_id']
                        or receipt.get('identity') != {k: job[k] for k in IDENTITY}
                        or receipt.get('manifest_sha256') != job['manifest_sha256']):
                    raise ValueError('receipt identity mismatch')
            except (AttributeError, TypeError, ValueError):
                unknown += 1
        else:
            # Includes submission uncertainty, a lost executor, fenced remote
            # creation, and confirmed failure awaiting record reconciliation.
            unknown += 1
    return inflight, unknown


def initialize_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS llm_request_admissions (
        task_id TEXT NOT NULL, component_id TEXT NOT NULL, generation INTEGER NOT NULL,
        request_sha256 TEXT NOT NULL, state TEXT NOT NULL, response_sha256 TEXT,
        kind TEXT NOT NULL, work_dir TEXT NOT NULL, model_dir TEXT NOT NULL, receipt_id TEXT NOT NULL,
        task_run_id TEXT NOT NULL, source_sha256 TEXT NOT NULL,
        created_at_ns INTEGER NOT NULL, updated_at_ns INTEGER NOT NULL,
        PRIMARY KEY(task_id, component_id)
    )''')
    # Keep the folder queue available even when no task has yet been started.
    db.execute('''CREATE TABLE IF NOT EXISTS llm_folder_queue (
        task_id TEXT NOT NULL, group_id TEXT NOT NULL, paper_id TEXT NOT NULL,
        receipt_id TEXT NOT NULL, attempt_id TEXT NOT NULL, parent_run_id TEXT NOT NULL,
        task_run_id TEXT NOT NULL UNIQUE, manifest_sha256 TEXT NOT NULL,
        state TEXT NOT NULL, generation INTEGER, updated_at_ns INTEGER NOT NULL,
        PRIMARY KEY(task_id, group_id)
    )''')


def counts(db, task_id):
    rows = dict(db.execute('SELECT state,COUNT(*) FROM llm_request_admissions '
                           'WHERE task_id=? GROUP BY state', (task_id,)).fetchall())
    known = {'admitted', 'returned_unverified', 'unknown', 'raw_complete'}
    if set(rows) - known:
        raise ValueError('LLM 准入状态不可核验')
    post_inflight, post_unknown = _postprocessing_counts(db, task_id)
    stage_table = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                             "AND name='paperflow_llm_stage_jobs'").fetchone()
    if not stage_table:
        if db.execute('SELECT 1 FROM llm_folder_queue WHERE task_id=? LIMIT 1', (task_id,)).fetchone():
            raise ValueError('LLM 阶段登记表不可用')
        stages = {}
    else:
        stages = dict(db.execute('SELECT state,COUNT(*) FROM paperflow_llm_stage_jobs '
                                 'WHERE task_id=? GROUP BY state', (task_id,)).fetchall())
    known_stages = {'recovering_folder', 'recovery_unknown', 'reserved', 'submitting', 'submitted', 'executing',
                    'submission_unknown', 'delivery_unknown', 'execution_unknown', 'identity_unknown',
                    'finished', 'failed', 'stopped', 'stopped_before_submit',
                    'cancel_fenced_recovering_folder', 'cancel_fenced_recovery_unknown',
                    'cancel_fenced_reserved', 'cancel_fenced_submitting', 'cancel_fenced_submitted',
                    'cancel_fenced_executing'}
    if set(stages) - known_stages:
        raise ValueError('LLM 阶段状态不可核验')
    stage_inflight = sum(stages.get(s, 0) for s in ('recovering_folder', 'submitting', 'submitted', 'executing'))
    stage_unknown = sum(stages.get(s, 0) for s in
        ('submission_unknown', 'delivery_unknown', 'execution_unknown', 'identity_unknown'))
    recovery_unknown = stages.get('recovery_unknown', 0)
    if not db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone():
        stage_unknown += sum(stages.get(s, 0) for s in
                             ('cancel_fenced_submitting', 'cancel_fenced_submitted', 'cancel_fenced_executing'))
    return {'inflight': rows.get('admitted', 0) + rows.get('returned_unverified', 0) + post_inflight,
            'stage_inflight': stage_inflight, 'stage_unknown': stage_unknown,
            'recovery_unknown': recovery_unknown,
            'unknown': rows.get('unknown', 0) + post_unknown}
