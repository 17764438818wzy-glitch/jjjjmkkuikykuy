"""Synthetic retry records; no Prefect service, model or queue is started."""
import json
import sqlite3
import sys
from pathlib import Path
from uuid import UUID, uuid4, uuid5

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import stage_runtime


def install_retry_records(path, identity, *, existing_admissions=False, preserve_admissions=False):
    request_id, state_id = str(uuid4()), str(uuid4())
    job_id = str(uuid5(UUID('6aaf059a-c89a-52a8-acd5-4a928260a3b7'),
                       identity['task_run_id'] + ':1:manual:' + request_id))
    with sqlite3.connect(path) as db:
        db.executescript('''
            CREATE TABLE paperflow_llm_stage_jobs(job_id TEXT,task_id TEXT,group_id TEXT,paper_id TEXT,
              receipt_id TEXT,attempt_id TEXT,parent_run_id TEXT,folder_run_id TEXT,generation INTEGER,
              task_key TEXT,state TEXT,retry_key TEXT);
            CREATE TABLE paperflow_llm_retry_items(request_id TEXT,task_id TEXT,group_id TEXT,paper_id TEXT,
              receipt_id TEXT,attempt_id TEXT,folder_run_id TEXT,job_id TEXT,generation INTEGER,
              old_flow_state_id TEXT,state TEXT,old_stage_state_id TEXT,old_job_json TEXT);
            CREATE TABLE paperflow_llm_retry_commands(request_id TEXT,request_json TEXT,state TEXT);
        ''')
        if existing_admissions:
            columns = {row[1] for row in db.execute('PRAGMA table_info(llm_request_admissions)')}
            for name in ('source_sha256', 'request_sha256', 'response_sha256', 'work_dir', 'model_dir', 'receipt_id'):
                if name not in columns:
                    db.execute('ALTER TABLE llm_request_admissions ADD COLUMN ' + name + ' TEXT')
            if 'generation' not in columns:
                db.execute('ALTER TABLE llm_request_admissions ADD COLUMN generation INTEGER')
                db.execute('UPDATE llm_request_admissions SET generation=1')
            if 'receipt_id' not in columns:
                db.execute('UPDATE llm_request_admissions SET receipt_id=?', (identity['receipt_id'],))
            if not preserve_admissions:
                db.execute('UPDATE llm_request_admissions SET source_sha256=?,request_sha256=?,response_sha256=?,'
                           'work_dir=?,model_dir=?', ('c' * 64, 'd' * 64, 'e' * 64, 'body-work', 'body-work/model'))
        else:
            db.execute('''CREATE TABLE llm_request_admissions(task_id TEXT,task_run_id TEXT,component_id TEXT,
              kind TEXT,source_sha256 TEXT,request_sha256 TEXT,response_sha256 TEXT,state TEXT,work_dir TEXT,model_dir TEXT,
              receipt_id TEXT,generation INTEGER)''')
        db.execute('INSERT INTO paperflow_llm_stage_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
            (job_id, identity['task_id'], identity['group_id'], identity['paper_id'], identity['receipt_id'],
             identity['attempt_id'], identity['parent_run_id'], identity['task_run_id'], 1,
             'paperflow-folder-llm-stage-v1', 'finished', request_id))
        db.execute('INSERT INTO paperflow_llm_retry_items VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (request_id, identity['task_id'], identity['group_id'], identity['paper_id'], identity['receipt_id'],
             identity['attempt_id'], identity['task_run_id'], job_id, 1, state_id, 'submitted', None, None))
        command = dict(request_id=request_id, scope='one', **{key: identity[key]
            for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')})
        db.execute('INSERT INTO paperflow_llm_retry_commands VALUES(?,?,?)',
                   (request_id, json.dumps(command), 'finished'))
        if not existing_admissions:
            db.execute('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                (identity['task_id'], identity['task_run_id'], 'body-component', 'body', 'c' * 64,
                 'd' * 64, 'e' * 64, 'raw_complete', 'body-work', 'body-work/model', identity['receipt_id'], 1))
    return path, identity, request_id, job_id, state_id


def install_prior_failed_retry(path, identity, current_request):
    prior_request, prior_state = str(uuid4()), str(uuid4())
    prior_id = str(uuid5(UUID('6aaf059a-c89a-52a8-acd5-4a928260a3b7'),
                         identity['task_run_id'] + ':1:manual:' + prior_request))
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        job = dict(db.execute('SELECT * FROM paperflow_llm_stage_jobs WHERE retry_key=?', (current_request,)).fetchone())
        job.update(job_id=prior_id, retry_key=prior_request, state='failed')
        db.execute('INSERT INTO paperflow_llm_stage_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?,?)', tuple(job.values()))
        db.execute('INSERT INTO paperflow_llm_retry_items VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (prior_request, identity['task_id'], identity['group_id'], identity['paper_id'], identity['receipt_id'],
             identity['attempt_id'], identity['task_run_id'], prior_id, 1, str(uuid4()), 'submitted', None, None))
        command = dict(request_id=prior_request, scope='one', **{key: identity[key]
            for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')})
        db.execute('INSERT INTO paperflow_llm_retry_commands VALUES(?,?,?)',
                   (prior_request, json.dumps(command), 'finished'))
        db.execute('UPDATE paperflow_llm_retry_items SET old_stage_state_id=?,old_job_json=? WHERE request_id=?',
                   (prior_state, json.dumps(job), current_request))
    return prior_id, prior_state


def install_stopped_retry(path, identity, current_request):
    with sqlite3.connect(path) as db:
        original = db.execute('SELECT old_stage_state_id,old_job_json FROM paperflow_llm_retry_items '
                              'WHERE request_id=?', (current_request,)).fetchone()
    stopped_id, _ = install_prior_failed_retry(path, identity, current_request)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE paperflow_llm_stage_jobs SET state='stopped' WHERE job_id=?", (stopped_id,))
        db.execute('UPDATE paperflow_llm_retry_items SET old_stage_state_id=?,old_job_json=? WHERE request_id=?',
                   (*original, current_request))
    return stopped_id


def install_normal_stopped(path, identity):
    job_id = str(uuid5(UUID('6aaf059a-c89a-52a8-acd5-4a928260a3b7'), identity['task_run_id'] + ':1'))
    with sqlite3.connect(path) as db:
        db.execute("INSERT INTO paperflow_llm_stage_jobs SELECT ?,task_id,group_id,paper_id,receipt_id,"
                   "attempt_id,parent_run_id,folder_run_id,generation,task_key,'stopped','' "
                   "FROM paperflow_llm_stage_jobs WHERE state='finished'", (job_id,))
    return job_id


@pytest.fixture
def retry_database(tmp_path):
    path = tmp_path / 'retry.sqlite3'
    identity = dict(task_id=str(uuid4()), group_id='a' * 32, paper_id='paper-1',
        receipt_id='b' * 64, attempt_id='attempt-1', parent_run_id=str(uuid4()), task_run_id=str(uuid4()))
    return install_retry_records(path, identity)


def test_unique_completed_retry_produces_a_stable_original_folder_proof(retry_database):
    path, identity, request_id, job_id, state_id = retry_database
    select = getattr(stage_runtime, 'retry_source_snapshot', None)
    assert callable(select), 'the postprocessing runtime needs a retry source verifier'
    value = select(path, identity, 1)
    assert value['job_id'] == job_id
    assert value['retry_key'] == request_id
    assert value['old_flow_state_id'] == state_id
    assert len(value['sha256']) == 64
    assert select(path, identity, 1) == value


@pytest.mark.parametrize('statement', [
    "UPDATE paperflow_llm_retry_items SET paper_id='another-paper'",
    "UPDATE paperflow_llm_retry_items SET receipt_id='another-receipt'",
    "UPDATE paperflow_llm_retry_items SET attempt_id='another-attempt'",
    "UPDATE paperflow_llm_retry_items SET folder_run_id='another-folder'",
    "UPDATE paperflow_llm_retry_items SET generation=2",
    "UPDATE paperflow_llm_retry_items SET state='needs_review'",
    "UPDATE paperflow_llm_retry_items SET state='delivery_unknown'",
    "UPDATE paperflow_llm_retry_items SET state=NULL",
    "UPDATE paperflow_llm_stage_jobs SET state='execution_unknown'",
    "UPDATE llm_request_admissions SET state='unknown'",
    "UPDATE llm_request_admissions SET response_sha256='invalid'",
    "UPDATE llm_request_admissions SET receipt_id='ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'",
    "UPDATE llm_request_admissions SET generation=99",
    "UPDATE llm_request_admissions SET generation=0",
    "DELETE FROM paperflow_llm_retry_commands",
])
def test_retry_identity_conflict_or_incomplete_evidence_is_rejected(retry_database, statement):
    path, identity, *_ = retry_database
    with sqlite3.connect(path) as db:
        db.execute(statement)
    with pytest.raises(stage_runtime.StageConflict):
        stage_runtime.retry_source_snapshot(path, identity, 1)


def test_a_second_unresolved_retry_item_blocks_the_old_success(retry_database):
    path, identity, *_ = retry_database
    with sqlite3.connect(path) as db:
        db.execute('INSERT INTO paperflow_llm_retry_items(request_id,task_id,group_id,state) VALUES(?,?,?,?)',
                   (str(uuid4()), identity['task_id'], identity['group_id'], 'checking'))
    with pytest.raises(stage_runtime.StageConflict):
        stage_runtime.retry_source_snapshot(path, identity, 1)


def test_a_second_submitted_retry_without_a_verifiable_stage_blocks_old_success(retry_database):
    path, identity, *_ = retry_database
    with sqlite3.connect(path) as db:
        db.execute('INSERT INTO paperflow_llm_retry_items(request_id,task_id,group_id,job_id,state) VALUES(?,?,?,?,?)',
                   (str(uuid4()), identity['task_id'], identity['group_id'], str(uuid4()), 'submitted'))
    with pytest.raises(stage_runtime.StageConflict):
        stage_runtime.retry_source_snapshot(path, identity, 1)


def test_retry_item_without_any_retry_stage_never_becomes_a_normal_source(retry_database):
    path, identity, *_ = retry_database
    with sqlite3.connect(path) as db:
        db.execute('DELETE FROM paperflow_llm_stage_jobs')
        db.execute("UPDATE paperflow_llm_retry_items SET state='not_sent'")
    with pytest.raises(stage_runtime.StageConflict):
        stage_runtime.retry_source_snapshot(path, identity, 1)


def test_second_manual_retry_can_follow_a_verified_failed_first_retry(retry_database):
    path, identity, request_id, job_id, _ = retry_database
    prior_id, prior_state = install_prior_failed_retry(path, identity, request_id)
    selected = stage_runtime.retry_source_snapshot(path, identity, 1)
    assert selected['job_id'] == job_id
    assert selected['prior_failed'] == [{'job_id': prior_id, 'state_id': prior_state}]


def test_unrelated_failed_stage_is_not_silently_treated_as_history(retry_database):
    path, identity, *_ = retry_database
    with sqlite3.connect(path) as db:
        db.execute("INSERT INTO paperflow_llm_stage_jobs SELECT ?,task_id,group_id,paper_id,receipt_id,"
                   "attempt_id,parent_run_id,folder_run_id,generation,task_key,'failed',? FROM paperflow_llm_stage_jobs",
                   (str(uuid4()), str(uuid4())))
    with pytest.raises(stage_runtime.StageConflict):
        stage_runtime.retry_source_snapshot(path, identity, 1)


def test_a_proven_not_submitted_attempt_does_not_block_later_success(retry_database):
    path, identity, request_id, job_id, _ = retry_database
    prior_id, _ = install_prior_failed_retry(path, identity, request_id)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE paperflow_llm_stage_jobs SET state='stopped_before_submit' WHERE job_id=?", (prior_id,))
        db.execute("UPDATE paperflow_llm_retry_items SET state='not_sent' WHERE job_id=?", (prior_id,))
        db.execute('UPDATE paperflow_llm_retry_items SET old_stage_state_id=NULL,old_job_json=NULL WHERE request_id=?',
                   (request_id,))
    selected = stage_runtime.retry_source_snapshot(path, identity, 1)
    assert selected['job_id'] == job_id
    with sqlite3.connect(path) as db:
        db.execute("UPDATE paperflow_llm_stage_jobs SET state='submission_unknown' WHERE job_id=?", (prior_id,))
    with pytest.raises(stage_runtime.StageConflict):
        stage_runtime.retry_source_snapshot(path, identity, 1)


def test_paused_retry_then_later_success_preserves_both_kinds_of_history(retry_database):
    path, identity, request_id, job_id, _ = retry_database
    failed_id, failed_state = install_prior_failed_retry(path, identity, request_id)
    stopped_id = install_stopped_retry(path, identity, request_id)
    selected = stage_runtime.retry_source_snapshot(path, identity, 1)
    assert selected['job_id'] == job_id
    assert selected['prior_failed'] == [dict(job_id=failed_id, state_id=failed_state)]
    assert selected['prior_stopped'] == [stopped_id]


def test_prior_paused_normal_llm_stage_is_tracked_and_verified(retry_database):
    path, identity, *_ = retry_database
    stopped_id = install_normal_stopped(path, identity)
    selected = stage_runtime.retry_source_snapshot(path, identity, 1)
    assert selected['prior_stopped'] == [stopped_id]
    with sqlite3.connect(path) as db:
        db.execute("UPDATE paperflow_llm_stage_jobs SET paper_id='another-paper' WHERE job_id=?", (stopped_id,))
    with pytest.raises(stage_runtime.StageConflict):
        stage_runtime.retry_source_snapshot(path, identity, 1)


def test_prior_normal_stage_known_not_submitted_is_preserved(retry_database):
    path, identity, *_ = retry_database
    stopped_id = install_normal_stopped(path, identity)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE paperflow_llm_stage_jobs SET state='stopped_before_submit' WHERE job_id=?", (stopped_id,))
    assert stage_runtime.retry_source_snapshot(path, identity, 1)['prior_stopped'] == []


def test_already_running_history_must_keep_the_same_folder_identity(retry_database):
    path, identity, *_ = retry_database
    with sqlite3.connect(path) as db:
        db.execute('INSERT INTO paperflow_llm_retry_items(request_id,task_id,group_id,paper_id,receipt_id,attempt_id,state) '
                   "VALUES(?,?,?,?,?,?,'already_running')", (str(uuid4()), identity['task_id'], identity['group_id'],
                   identity['paper_id'], identity['receipt_id'], identity['attempt_id']))
    assert stage_runtime.retry_source_snapshot(path, identity, 1)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE paperflow_llm_retry_items SET paper_id='wrong' WHERE state='already_running'")
    with pytest.raises(stage_runtime.StageConflict):
        stage_runtime.retry_source_snapshot(path, identity, 1)


def test_changed_answer_hash_changes_the_selected_retry_proof(retry_database):
    path, identity, *_ = retry_database
    before = stage_runtime.retry_source_snapshot(path, identity, 1)
    with sqlite3.connect(path) as db:
        db.execute('UPDATE llm_request_admissions SET response_sha256=?', ('f' * 64,))
    after = stage_runtime.retry_source_snapshot(path, identity, 1)
    assert after['sha256'] != before['sha256']


def test_all_scope_retry_command_cannot_contain_a_different_folder_identity(retry_database):
    path, identity, request_id, *_ = retry_database
    with sqlite3.connect(path) as db:
        db.execute('UPDATE paperflow_llm_retry_commands SET request_json=?',
                   (json.dumps(dict(request_id=request_id, scope='all', paper_id='unrelated')),))
    with pytest.raises(stage_runtime.StageConflict):
        stage_runtime.retry_source_snapshot(path, identity, 1)
