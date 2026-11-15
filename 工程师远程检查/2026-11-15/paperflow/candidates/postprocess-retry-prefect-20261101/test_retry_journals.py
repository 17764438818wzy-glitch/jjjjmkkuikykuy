import json
from pathlib import Path
import sqlite3
import sys
from uuid import uuid4

root = Path(__file__).resolve().parents[2]
candidate = Path(__file__).resolve().parent
sys.path[:0] = [str(candidate), str(root / 'candidates/postprocess-prefect-20261031'),
                str(root / 'candidates/postprocess-prefect-20261031/runtime_next'),
                str(root / 'candidates/postprocess-server-20261101'), str(root / 'prefect-paperflow')]
from remote_jobs import RemoteJobs
from remote_coordinator import RemoteSubmissionJournal, JOURNAL_SCHEMA


def job(job_id, claim_id, folder_id):
    return dict(job_id=job_id, claim_id=claim_id, generation=1,
        identity=dict(task_id=str(uuid4()), group_id='a' * 32, paper_id='paper', receipt_id='b' * 64,
                      attempt_id='attempt', parent_run_id=str(uuid4()), task_run_id=folder_id),
        manifest_sha256='c' * 64, snapshot_sha256='d' * 64, code_sha256='e' * 64)


def test_server_attempt_migration_retains_old_job_and_accepts_new_one(tmp_path):
    path = tmp_path / 'executor.sqlite'
    ledger_id, folder_id = str(uuid4()), str(uuid4())
    old = job(str(uuid4()), str(uuid4()), folder_id)
    old_json = json.dumps(old, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    with sqlite3.connect(path) as db:
        db.executescript('''CREATE TABLE executor_settings(name TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE executor_jobs(job_id TEXT PRIMARY KEY,claim_id TEXT NOT NULL UNIQUE,task_id TEXT NOT NULL,
                group_id TEXT NOT NULL,task_run_id TEXT NOT NULL UNIQUE,frozen_json TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('reserved','running','unknown','finished')),
                receipt_sha256 TEXT,created_at_ns INTEGER NOT NULL,updated_at_ns INTEGER NOT NULL,
                UNIQUE(task_id, group_id));''')
        db.executemany('INSERT INTO executor_settings VALUES(?,?)', [('capacity','8'), ('ledger_id',ledger_id)])
        db.execute('INSERT INTO executor_jobs VALUES(?,?,?,?,?,?,?,?,?,?)',
            (old['job_id'], old['claim_id'], old['identity']['task_id'], old['identity']['group_id'],
             folder_id, old_json, 'finished', 'f' * 64, 1, 2))
    jobs = RemoteJobs(path, capacity=8, ledger_id=ledger_id)
    jobs.allow_multiple_attempts()
    jobs.allow_multiple_attempts()
    new = dict(old, job_id=str(uuid4()), claim_id=str(uuid4()))
    assert jobs.reserve(new) == {'may_start': True, 'state': 'reserved'}
    assert jobs.status(old['job_id'])['frozen_json'] == old_json
    assert jobs.status(new['job_id'])['task_run_id'] == folder_id


def test_journal_attempt_migration_keeps_old_transfer_and_start(tmp_path):
    control, path = tmp_path / 'control.sqlite', tmp_path / 'journal.sqlite'
    control.touch()
    journal_id = str(uuid4())
    old = job(str(uuid4()), str(uuid4()), str(uuid4()))
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE journal_settings(name TEXT PRIMARY KEY,value TEXT NOT NULL)')
        db.executemany('INSERT INTO journal_settings VALUES(?,?)',
            [('schema', JOURNAL_SCHEMA), ('control_database', str(control.resolve())), ('journal_id', journal_id)])
        for table in ('remote_transfer_intents', 'remote_start_intents'):
            db.execute('CREATE TABLE ' + table + '(job_id TEXT PRIMARY KEY,claim_id TEXT NOT NULL UNIQUE,'
                'task_run_id TEXT NOT NULL UNIQUE,task_id TEXT NOT NULL,group_id TEXT NOT NULL,intent_json TEXT NOT NULL,'
                'intent_sha256 TEXT NOT NULL,created_at_ns INTEGER NOT NULL,UNIQUE(task_id,group_id))')
            db.execute('INSERT INTO ' + table + ' VALUES(?,?,?,?,?,?,?,?)', (old['job_id'], old['claim_id'],
                old['identity']['task_run_id'], old['identity']['task_id'], old['identity']['group_id'], 'old', 'digest', 1))
    journal = RemoteSubmissionJournal(path, control_database=control, admission_gate=lambda _: None, journal_id=journal_id)
    journal.allow_multiple_attempts()
    journal.allow_multiple_attempts()
    new_job, new_claim = str(uuid4()), str(uuid4())
    journal._unreserved(old['identity'], job_id=new_job)
    with sqlite3.connect(path) as db:
        for table in ('remote_transfer_intents', 'remote_start_intents'):
            db.execute('INSERT INTO ' + table + ' VALUES(?,?,?,?,?,?,?,?)', (new_job, new_claim,
                old['identity']['task_run_id'], old['identity']['task_id'], old['identity']['group_id'], 'new', 'digest2', 2))
            assert db.execute('SELECT intent_json FROM ' + table + ' WHERE job_id=?', (old['job_id'],)).fetchone()[0] == 'old'
            assert db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0] == 2
