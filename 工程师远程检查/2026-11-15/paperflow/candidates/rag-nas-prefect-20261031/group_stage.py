"""One local stage record per existing Prefect folder order.

This module only initializes and reserves the record; it creates no Prefect
run, does not send data and does not change upstream claims.
"""
from contextlib import closing
import sqlite3
import time
from pathlib import Path
from uuid import UUID, uuid5


NAMESPACE = UUID('ccf886ef-7347-4204-9a83-4fa33586d046')
FIELDS = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')


def initialize(database):
    with closing(sqlite3.connect(Path(database).resolve(strict=True).as_uri() + '?mode=rw', uri=True, timeout=10)) as db, db:
        db.execute('''CREATE TABLE IF NOT EXISTS paperflow_rag_nas_jobs (
            task_id TEXT NOT NULL, group_id TEXT NOT NULL, paper_id TEXT NOT NULL,
            receipt_id TEXT NOT NULL, attempt_id TEXT NOT NULL, parent_run_id TEXT NOT NULL,
            task_run_id TEXT NOT NULL, source_generation INTEGER NOT NULL, run_generation INTEGER NOT NULL,
            claim_id TEXT NOT NULL, postprocess_job_id TEXT NOT NULL, upstream_manifest_sha256 TEXT NOT NULL,
            postprocess_receipt_sha256 TEXT NOT NULL,
            rag_task_run_id TEXT NOT NULL, nas_task_run_id TEXT NOT NULL,
            rag_submission TEXT NOT NULL DEFAULT 'reserved', nas_submission TEXT NOT NULL DEFAULT 'reserved',
            stage TEXT NOT NULL, state TEXT NOT NULL, copy_receipt_path TEXT, rag_receipt_path TEXT,
            nas_receipt_path TEXT, cleanup_receipt_path TEXT, error_type TEXT,
            rag_started_at_ns INTEGER, rag_completed_at_ns INTEGER, nas_started_at_ns INTEGER,
            nas_completed_at_ns INTEGER, deleted_at_ns INTEGER, completed_at_ns INTEGER,
            created_at_ns INTEGER NOT NULL, updated_at_ns INTEGER NOT NULL,
            PRIMARY KEY(task_id, group_id)
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS paperflow_rag_nas_intervals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL, group_id TEXT NOT NULL,
            stage TEXT NOT NULL, started_at_ns INTEGER NOT NULL, ended_at_ns INTEGER
        )''')


def prefect_task_identity(stage, task_run_id):
    run = UUID(task_run_id)
    return {'id': str(run), 'tag': f'paperflow-{stage}-stage-v1',
            'task_key': f'paperflow-folder-{stage}-stage-v1-{run.hex}',
            'dynamic_key': f'paperflow-{stage}-{run}'}


def reserve_group(database, handoff, generation):
    """Reserve a local folder identity under the currently running parent."""
    ticket, job = handoff['identity'], handoff['job']
    with closing(sqlite3.connect(Path(database).resolve(strict=True).as_uri() + '?mode=rw', uri=True, timeout=10)) as db, db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN IMMEDIATE')
        parent = db.execute('SELECT * FROM tasks WHERE task_id=?', (ticket['task_id'],)).fetchone()
        deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (ticket['task_id'],)).fetchone()
        barrier = db.execute("SELECT blocked FROM paperflow_queue_barriers WHERE name='rag_upload'").fetchone()
        if (not parent or deleted or not barrier or barrier['blocked'] != 0
                or parent['desired_state'] != 'running' or parent['start_pending']
                or parent['start_generation'] != generation or parent['prefect_order_model'] != 'v2'
                or parent['prefect_run_id'] != ticket['parent_run_id']
                or parent['task_status'] in {'deleting', 'delete_failed', 'unknown'}):
            raise InterruptedError('parent task or RAG barrier is not open')
        claim = db.execute('SELECT * FROM postprocessing_claims WHERE task_id=? AND group_id=?',
                           (ticket['task_id'], ticket['group_id'])).fetchone()
        stage = db.execute('SELECT * FROM paperflow_postprocess_stage_jobs WHERE task_id=? AND group_id=?',
                           (ticket['task_id'], ticket['group_id'])).fetchone()
        result = db.execute('SELECT * FROM postprocessing_results WHERE task_id=? AND group_id=?',
                            (ticket['task_id'], ticket['group_id'])).fetchone()
        upstream = db.execute('SELECT * FROM llm_folder_queue WHERE task_id=? AND group_id=?',
                              (ticket['task_id'], ticket['group_id'])).fetchone()
        receipt = (db.execute('SELECT * FROM postprocessing_receipts WHERE claim_id=?', (claim['claim_id'],)).fetchone()
                   if claim else None)
        if (not claim or not stage or not result or not receipt or not upstream or claim['state'] != 'waiting_rag'
                or stage['state'] != 'finished' or result['state'] != 'passed' or result['code'] != 'passed'
                or upstream['state'] != 'waiting_postprocessing'
                or any(row[field] != ticket[field] for row in (claim, stage, result, upstream) for field in FIELDS)
                or claim['generation'] > generation or stage['generation'] > generation
                or result['generation'] != stage['generation'] or stage['generation'] != job['generation']
                or stage['job_id'] != job['job_id']
                or claim['claim_id'] != job['claim_id'] or result['claim_id'] != claim['claim_id']
                or any(row['manifest_sha256'] != job['manifest_sha256'] for row in (claim, stage, result, upstream))
                or result['stage_receipt_sha256'] != receipt['receipt_sha256']
                or receipt['receipt_sha256'] != handoff['receipt_canonical_sha256']):
            raise ValueError('postprocessing handoff is not waiting for this folder')
        folder = str(UUID(ticket['task_run_id']))
        rag_id = str(uuid5(NAMESPACE, folder + ':rag'))
        nas_id = str(uuid5(NAMESPACE, folder + ':nas'))
        now = time.time_ns()
        db.execute('''INSERT INTO paperflow_rag_nas_jobs
            (task_id,group_id,paper_id,receipt_id,attempt_id,parent_run_id,task_run_id,
             source_generation,run_generation,claim_id,postprocess_job_id,upstream_manifest_sha256,
             postprocess_receipt_sha256,rag_task_run_id,nas_task_run_id,stage,state,created_at_ns,updated_at_ns)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (*[ticket[field] for field in FIELDS], stage['generation'], generation, claim['claim_id'], stage['job_id'],
             job['manifest_sha256'], receipt['receipt_sha256'], rag_id, nas_id,
             'rag', 'waiting', now, now))
        return dict(db.execute('SELECT * FROM paperflow_rag_nas_jobs WHERE task_id=? AND group_id=?',
                               (ticket['task_id'], ticket['group_id'])).fetchone())


def reserve_local_group(database, task_id, group_id, generation):
    with closing(sqlite3.connect(Path(database).resolve(strict=True).as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        claim = db.execute('SELECT * FROM postprocessing_claims WHERE task_id=? AND group_id=?',
                           (task_id, group_id)).fetchone()
        stage = db.execute('SELECT * FROM paperflow_postprocess_stage_jobs WHERE task_id=? AND group_id=?',
                           (task_id, group_id)).fetchone()
        receipt = db.execute('SELECT * FROM postprocessing_receipts WHERE claim_id=?',
                             (claim['claim_id'],)).fetchone() if claim else None
    if not claim or not stage or not receipt:
        raise ValueError('local postprocessing handoff is unavailable')
    handoff = {'identity': {key: claim[key] for key in FIELDS},
               'job': {'job_id': stage['job_id'], 'claim_id': claim['claim_id'],
                       'generation': stage['generation'], 'manifest_sha256': stage['manifest_sha256']},
               'receipt_canonical_sha256': receipt['receipt_sha256']}
    return reserve_group(database, handoff, generation)


def verify_remote_handoff(job, artifact_map):
    remote = artifact_map['job']
    if (artifact_map['identity'] != {key: job[key] for key in FIELDS}
            or remote['job_id'] != job['postprocess_job_id'] or remote['claim_id'] != job['claim_id']
            or remote['generation'] != job['source_generation']
            or remote['manifest_sha256'] != job['upstream_manifest_sha256']
            or artifact_map['receipt_canonical_sha256'] != job['postprocess_receipt_sha256']):
        raise ValueError('remote handoff differs from the original folder stage')
