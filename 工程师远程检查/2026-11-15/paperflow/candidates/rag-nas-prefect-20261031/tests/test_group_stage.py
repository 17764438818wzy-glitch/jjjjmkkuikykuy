from contextlib import closing
import sqlite3
from uuid import uuid4

import pytest

from group_stage import FIELDS, initialize, prefect_task_identity, reserve_group


@pytest.fixture
def control(tmp_path):
    database = tmp_path / 'control.sqlite3'
    ticket = {'task_id': str(uuid4()), 'group_id': 'a' * 32, 'paper_id': 'paper-1',
              'receipt_id': 'b' * 64, 'attempt_id': 'attempt-1',
              'parent_run_id': str(uuid4()), 'task_run_id': str(uuid4())}
    with closing(sqlite3.connect(database)) as db, db:
        db.execute('CREATE TABLE tasks(task_id TEXT, prefect_run_id TEXT, prefect_order_model TEXT, '
                   'desired_state TEXT, start_pending INTEGER, start_generation INTEGER, task_status TEXT)')
        db.execute('CREATE TABLE task_deletions(task_id TEXT)')
        db.execute('CREATE TABLE paperflow_queue_barriers(name TEXT, blocked INTEGER)')
        db.execute("INSERT INTO paperflow_queue_barriers VALUES('rag_upload',0)")
        fields = ('task_id TEXT, group_id TEXT, paper_id TEXT, receipt_id TEXT, attempt_id TEXT, '
                  'parent_run_id TEXT, task_run_id TEXT')
        db.execute(f'CREATE TABLE postprocessing_claims({fields}, generation INTEGER, state TEXT, '
                   'claim_id TEXT, manifest_sha256 TEXT)')
        db.execute(f'CREATE TABLE paperflow_postprocess_stage_jobs({fields}, job_id TEXT, state TEXT, '
                   'manifest_sha256 TEXT, generation INTEGER)')
        db.execute(f'CREATE TABLE postprocessing_results({fields}, claim_id TEXT, state TEXT, code TEXT, '
                   'stage_receipt_sha256 TEXT, manifest_sha256 TEXT, generation INTEGER)')
        db.execute('CREATE TABLE postprocessing_receipts(claim_id TEXT, receipt_sha256 TEXT)')
        db.execute(f'CREATE TABLE llm_folder_queue({fields}, state TEXT, manifest_sha256 TEXT)')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?)',
                   (ticket['task_id'], ticket['parent_run_id'], 'v2', 'running', 0, 5, 'complete'))
        db.execute('INSERT INTO postprocessing_claims VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                   (*ticket.values(), 5, 'waiting_rag', 'claim-1', 'c' * 64))
        db.execute('INSERT INTO paperflow_postprocess_stage_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                   (*ticket.values(), 'job-1', 'finished', 'c' * 64, 4))
        db.execute('INSERT INTO postprocessing_results VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (*ticket.values(), 'claim-1', 'passed', 'passed', 'd' * 64, 'c' * 64, 4))
        db.execute('INSERT INTO postprocessing_receipts VALUES(?,?)', ('claim-1', 'd' * 64))
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?)',
                   (*ticket.values(), 'waiting_postprocessing', 'c' * 64))
    initialize(database)
    verified = {'identity': ticket, 'job': {'job_id': 'job-1', 'claim_id': 'claim-1',
                                            'manifest_sha256': 'c' * 64, 'generation': 4},
                'receipt_canonical_sha256': 'd' * 64}
    return database, ticket, verified


def test_old_output_generation_remains_bound_to_current_task_generation(control):
    database, ticket, verified = control
    row = reserve_group(database, verified, 5)
    assert (row['source_generation'], row['run_generation']) == (4, 5)
    assert (row['stage'], row['state']) == ('rag', 'waiting')
    assert row['task_run_id'] == ticket['task_run_id']
    assert row['rag_task_run_id'] != row['nas_task_run_id']
    assert prefect_task_identity('rag', row['rag_task_run_id'])['dynamic_key'].endswith(row['rag_task_run_id'])


def test_local_reservation_preserves_original_ticket_for_later_remote_inspection(control):
    from group_stage import reserve_local_group, verify_remote_handoff
    database, ticket, verified = control
    job = reserve_local_group(database, ticket['task_id'], ticket['group_id'], 5)
    assert {key: job[key] for key in FIELDS} == ticket
    verify_remote_handoff(job, verified)
    verified['receipt_canonical_sha256'] = 'e' * 64
    with pytest.raises(ValueError, match='handoff'):
        verify_remote_handoff(job, verified)


def test_paused_or_deleted_parent_cannot_reserve_group(control):
    database, ticket, verified = control
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE tasks SET desired_state='paused'")
    with pytest.raises(InterruptedError):
        reserve_group(database, verified, 5)
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE tasks SET desired_state='running'")
        db.execute('INSERT INTO task_deletions VALUES(?)', (ticket['task_id'],))
    with pytest.raises(InterruptedError):
        reserve_group(database, verified, 5)
    with closing(sqlite3.connect(database)) as db, db:
        assert db.execute('SELECT COUNT(*) FROM paperflow_rag_nas_jobs').fetchone()[0] == 0


def test_closed_rag_barrier_cannot_reserve_group(control):
    database, _, verified = control
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE paperflow_queue_barriers SET blocked=1 WHERE name='rag_upload'")
    with pytest.raises(InterruptedError):
        reserve_group(database, verified, 5)


def test_same_folder_cannot_be_reserved_twice(control):
    database, ticket, verified = control
    reserve_group(database, verified, 5)
    with pytest.raises(sqlite3.IntegrityError):
        reserve_group(database, verified, 5)
    with closing(sqlite3.connect(database)) as db, db:
        assert db.execute('SELECT COUNT(*) FROM paperflow_rag_nas_jobs').fetchone()[0] == 1


def test_waiting_claim_is_not_enough_until_result_and_stage_finish(control):
    database, _, verified = control
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE postprocessing_results SET state='processing'")
    with pytest.raises(ValueError, match='handoff'):
        reserve_group(database, verified, 5)
    with closing(sqlite3.connect(database)) as db, db:
        assert db.execute('SELECT COUNT(*) FROM paperflow_rag_nas_jobs').fetchone()[0] == 0


def test_changed_original_folder_handoff_cannot_be_reserved(control):
    database, _, verified = control
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE llm_folder_queue SET state='cancelled'")
    with pytest.raises(ValueError, match='handoff'):
        reserve_group(database, verified, 5)


def test_inspected_server_job_must_match_recorded_source_generation(control):
    database, _, verified = control
    verified['job']['generation'] = 3
    with pytest.raises(ValueError, match='handoff'):
        reserve_group(database, verified, 5)


def test_missing_control_database_is_not_created(tmp_path):
    database = tmp_path / 'missing.sqlite3'
    with pytest.raises(FileNotFoundError):
        initialize(database)
    assert not database.exists()


def test_database_connection_is_released_after_reservation(control):
    database, _, verified = control
    reserve_group(database, verified, 5)
    database.unlink()
    assert not database.exists()
