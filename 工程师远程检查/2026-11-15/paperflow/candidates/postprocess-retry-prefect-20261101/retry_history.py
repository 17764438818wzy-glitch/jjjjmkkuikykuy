"""Preserve earlier postprocessing attempts for the same Prefect folder."""
from contextlib import closing
import json
import sqlite3
import time


def initialize(database):
    with closing(sqlite3.connect(database)) as db, db:
        db.execute('''CREATE TABLE IF NOT EXISTS postprocessing_attempt_history (
            job_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, group_id TEXT NOT NULL,
            stage_json TEXT NOT NULL, claim_json TEXT, result_json TEXT, archived_at_ns INTEGER NOT NULL
        )''')


def archive_for_retry(db, task_id, group_id, generation, previous_job_id):
    """Archive in the caller's transaction that also reserves the next attempt."""
    task = db.execute('SELECT desired_state,start_generation,start_pending FROM tasks WHERE task_id=?',
                      (task_id,)).fetchone()
    stage = db.execute('SELECT * FROM paperflow_postprocess_stage_jobs WHERE task_id=? AND group_id=?',
                       (task_id, group_id)).fetchone()
    if (not task or task['desired_state'] != 'running' or task['start_pending']
            or task['start_generation'] != generation or not stage or stage['job_id'] != previous_job_id
            or stage['state'] not in {'execution_unknown', 'failed', 'submission_unknown'}
            or stage['generation'] >= generation):
        return False
    claim = db.execute('SELECT * FROM postprocessing_claims WHERE task_id=? AND group_id=?',
                       (task_id, group_id)).fetchone()
    result = db.execute('SELECT * FROM postprocessing_results WHERE task_id=? AND group_id=?',
                        (task_id, group_id)).fetchone()
    if (claim and claim['state'] in {'processing', 'receipt_held', 'waiting_rag'}) or (result and result['state'] == 'passed'):
        return False
    encode = lambda row: json.dumps(dict(row), ensure_ascii=False, sort_keys=True) if row else None
    db.execute('INSERT INTO postprocessing_attempt_history VALUES(?,?,?,?,?,?,?)',
               (stage['job_id'], task_id, group_id, encode(stage), encode(claim), encode(result), time.time_ns()))
    for table in ('postprocessing_claims', 'postprocessing_results', 'paperflow_postprocess_stage_jobs'):
        db.execute('DELETE FROM ' + table + ' WHERE task_id=? AND group_id=?', (task_id, group_id))
    return True
