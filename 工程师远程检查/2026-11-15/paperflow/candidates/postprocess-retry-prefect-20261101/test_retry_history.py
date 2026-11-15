import json
import sqlite3

from retry_history import archive_for_retry, initialize


def database(tmp_path):
    path = tmp_path / 'control.sqlite'
    with sqlite3.connect(path) as db:
        db.executescript('''
            CREATE TABLE tasks(task_id TEXT, desired_state TEXT, start_generation INTEGER, start_pending INTEGER);
            CREATE TABLE paperflow_postprocess_stage_jobs(job_id TEXT, task_id TEXT, group_id TEXT,
                generation INTEGER, state TEXT, error_type TEXT);
            CREATE TABLE postprocessing_claims(task_id TEXT, group_id TEXT, claim_id TEXT, state TEXT);
            CREATE TABLE postprocessing_results(task_id TEXT, group_id TEXT, result_id TEXT, state TEXT, code TEXT);
            CREATE TABLE postprocessing_receipts(claim_id TEXT, receipt_json TEXT);
            INSERT INTO tasks VALUES('task','paused',1,0);
            INSERT INTO paperflow_postprocess_stage_jobs VALUES('old-job','task','group',1,'execution_unknown','TransportError');
            INSERT INTO postprocessing_claims VALUES('task','group','old-claim','unknown');
            INSERT INTO postprocessing_results VALUES('task','group','old-result','unresolved','outcome_unknown');
            INSERT INTO postprocessing_receipts VALUES('old-claim','old receipt');
        ''')
    initialize(path)
    return path


def test_retry_archives_original_attempt_only_after_next_start(tmp_path):
    path = database(tmp_path)
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        assert archive_for_retry(db, 'task', 'group', 1, 'old-job') is False
    with sqlite3.connect(path) as db:
        db.execute("UPDATE tasks SET desired_state='running',start_generation=2")
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        assert archive_for_retry(db, 'task', 'group', 2, 'old-job') is True
    with sqlite3.connect(path) as db:
        stage, claim, result = db.execute('SELECT stage_json,claim_json,result_json FROM postprocessing_attempt_history').fetchone()
        assert json.loads(stage)['error_type'] == 'TransportError'
        assert json.loads(claim)['claim_id'] == 'old-claim'
        assert json.loads(result)['result_id'] == 'old-result'
        assert db.execute('SELECT receipt_json FROM postprocessing_receipts').fetchone()[0] == 'old receipt'
        assert db.execute('SELECT COUNT(*) FROM paperflow_postprocess_stage_jobs').fetchone()[0] == 0


def test_successful_or_executing_group_is_not_silently_retried(tmp_path):
    path = database(tmp_path)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE tasks SET desired_state='running',start_generation=2")
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='executing'")
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        assert archive_for_retry(db, 'task', 'group', 2, 'old-job') is False
    with sqlite3.connect(path) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='execution_unknown'")
        db.execute("UPDATE postprocessing_results SET state='passed'")
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        assert archive_for_retry(db, 'task', 'group', 2, 'old-job') is False
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT COUNT(*) FROM postprocessing_attempt_history').fetchone()[0] == 0
