"""Isolated shared control barriers for LLM and folder postprocessing.

Three-way merged onto llm-new-prefect-20261031/flow_control/llm_admission_state.py
SHA256 57b1e43230f2b274d60f77c14c730c4c845734e9c5d9652bb1b062759f4d48f4.
Only the explicit stage store creates postprocessing_claims: missing/broken
tables are NOT interpreted as an empty queue by this control candidate.
"""
import sqlite3


def initialize_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS llm_request_admissions (
        task_id TEXT NOT NULL, component_id TEXT NOT NULL, generation INTEGER NOT NULL,
        request_sha256 TEXT NOT NULL, state TEXT NOT NULL, response_sha256 TEXT,
        kind TEXT NOT NULL, work_dir TEXT NOT NULL, model_dir TEXT NOT NULL, receipt_id TEXT NOT NULL,
        task_run_id TEXT NOT NULL, source_sha256 TEXT NOT NULL,
        created_at_ns INTEGER NOT NULL, updated_at_ns INTEGER NOT NULL,
        PRIMARY KEY(task_id, component_id)
    )''')
    # Preserve upstream initialization before any task is admitted.
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
    try:
        stages = dict(db.execute('SELECT state,COUNT(*) FROM postprocessing_claims '
                                 'WHERE task_id=? GROUP BY state', (task_id,)).fetchall())
    except sqlite3.Error as exc:
        raise ValueError('后处理工单状态不可核验，禁止视为空闲') from exc
    if set(stages) - {'processing', 'unknown', 'failed', 'receipt_held', 'waiting_rag'}:
        raise ValueError('后处理工单存在不可核验的状态')
    return {'inflight': rows.get('admitted', 0) + rows.get('returned_unverified', 0) + stages.get('processing', 0),
            'unknown': rows.get('unknown', 0) + stages.get('unknown', 0) + stages.get('failed', 0)}
