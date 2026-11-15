"""Candidate shared view of model admissions for pause and deletion."""


def initialize_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS llm_request_admissions (
        task_id TEXT NOT NULL, component_id TEXT NOT NULL, generation INTEGER NOT NULL,
        request_sha256 TEXT NOT NULL, state TEXT NOT NULL, response_sha256 TEXT,
        kind TEXT NOT NULL, work_dir TEXT NOT NULL, model_dir TEXT NOT NULL, receipt_id TEXT NOT NULL,
        task_run_id TEXT NOT NULL, source_sha256 TEXT NOT NULL,
        created_at_ns INTEGER NOT NULL, updated_at_ns INTEGER NOT NULL,
        PRIMARY KEY(task_id, component_id)
    )''')


def counts(db, task_id):
    rows = dict(db.execute('SELECT state,COUNT(*) FROM llm_request_admissions '
                           'WHERE task_id=? GROUP BY state', (task_id,)).fetchall())
    known = {'admitted', 'returned_unverified', 'unknown', 'raw_complete'}
    if set(rows) - known:
        raise ValueError('LLM 准入状态不可核验')
    return {'inflight': rows.get('admitted', 0) + rows.get('returned_unverified', 0),
            'unknown': rows.get('unknown', 0)}
