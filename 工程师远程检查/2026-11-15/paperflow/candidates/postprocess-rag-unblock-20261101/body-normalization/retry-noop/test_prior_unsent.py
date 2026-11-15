import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
from uuid import uuid4

import pytest

workspace = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(workspace / 'candidates/postprocess-prefect-20261031/generation_next/tests'))
from test_retry_source import install_retry_records

spec = importlib.util.spec_from_file_location('retry_noop_candidate', Path(__file__).with_name('stage_runtime.py'))
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)


def test_unsent_review_does_not_hide_later_completed_answer(tmp_path):
    identity = dict(task_id=str(uuid4()), group_id='a' * 32, paper_id='paper-1', receipt_id='b' * 64,
                    attempt_id='attempt-1', parent_run_id=str(uuid4()), task_run_id=str(uuid4()))
    database, _, _, current_job, _ = install_retry_records(tmp_path / 'control.sqlite3', identity)
    earlier = str(uuid4())
    command = dict(request_id=earlier, scope='one', **{key: identity[key]
                   for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')})
    with sqlite3.connect(database) as db:
        db.execute('ALTER TABLE paperflow_llm_retry_items ADD COLUMN stage_mode TEXT')
        db.execute('ALTER TABLE paperflow_llm_retry_items ADD COLUMN message TEXT')
        db.execute('''INSERT INTO paperflow_llm_retry_items
            (request_id,task_id,group_id,paper_id,receipt_id,attempt_id,state,message)
            VALUES(?,?,?,?,?,?,?,?)''', (earlier, identity['task_id'], identity['group_id'], identity['paper_id'],
            identity['receipt_id'], identity['attempt_id'], 'needs_review',
            '原请求结果未确认，已保留记录，本次未重发'))
        db.execute('INSERT INTO paperflow_llm_retry_commands VALUES(?,?,?)',
                   (earlier, json.dumps(command), 'finished'))
    result = runtime.retry_source_snapshot(database, identity, 1)
    assert result['job_id'] == current_job
    with sqlite3.connect(database) as db:
        assert db.execute('SELECT state,message FROM paperflow_llm_retry_items WHERE request_id=?',
                          (earlier,)).fetchone() == ('needs_review', '原请求结果未确认，已保留记录，本次未重发')
        db.execute("UPDATE paperflow_llm_stage_jobs SET state='execution_unknown' WHERE job_id=?", (current_job,))
    with pytest.raises(runtime.StageConflict):
        runtime.retry_source_snapshot(database, identity, 1)
