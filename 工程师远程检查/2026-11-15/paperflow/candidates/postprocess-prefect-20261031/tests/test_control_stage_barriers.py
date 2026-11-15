"""Exercise copied control entry points against temporary databases only."""
from contextlib import closing
import importlib
import importlib.util
from pathlib import Path
import sqlite3
import sys
import threading
from types import ModuleType, SimpleNamespace
from uuid import uuid4

import pytest


HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parents[1]
BASELINE = ROOT / 'candidates/llm-new-prefect-20261031/flow_control'
LEGACY = ROOT / 'candidates/first-step-control-20261029'
NAME = '_postprocess_control_barrier_lab'
package = ModuleType(NAME)
package.__path__ = [str(HERE / 'control_candidate'), str(BASELINE),
                    str(LEGACY / 'paperflow_prefect_v2'), str(LEGACY / 'paperflow_prefect')]
package.__package__ = NAME
sys.modules[NAME] = package
control_module = importlib.import_module(NAME + '.first_step_control')
delete_module = importlib.import_module(NAME + '.first_step_delete')
counts_module = importlib.import_module(NAME + '.llm_admission_state')
spec = importlib.util.spec_from_file_location('_stage_for_control_barrier_lab', HERE / 'folder_state.py')
stage_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stage_module)


@pytest.fixture
def control(tmp_path):
    ctl = control_module.FirstStepControl(tmp_path / 'output', tmp_path / 'data')
    task_id = ctl.create_task('synthetic stage control')['task_id']
    run_id = str(uuid4())
    with closing(ctl.summary._connect()) as db, db:
        db.execute("UPDATE tasks SET desired_state='running',task_status='complete',start_generation=7,"
                   "prefect_run_id=?,start_pending=0 WHERE task_id=?", (run_id, task_id))
    stage_module.FolderStageStore(ctl.summary.path).initialize()
    return ctl, task_id


def add_stage(ctl, task_id, state):
    with closing(ctl.summary._connect()) as db, db:
        now, ident = 1, str(uuid4())
        db.execute('INSERT INTO postprocessing_claims VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (task_id, ident.replace('-', '')[:32], 'paper', 'a' * 64, 'attempt', str(uuid4()),
                    str(uuid4()), 'b' * 64, ident, 7, state, None, now, now))
    return ident


def update_stage(ctl, state):
    with closing(ctl.summary._connect()) as db, db:
        db.execute('UPDATE postprocessing_claims SET state=?', (state,))


def generation(ctl, task_id):
    with closing(ctl.summary._connect()) as db:
        return db.execute('SELECT start_generation FROM tasks WHERE task_id=?', (task_id,)).fetchone()[0]


@pytest.mark.parametrize('state,inflight,unknown', [
    ('processing', 1, 0), ('unknown', 0, 1), ('failed', 0, 1),
    ('receipt_held', 0, 0), ('waiting_rag', 0, 0),
])
def test_combined_counts_classify_stages_without_claiming_safe_outputs_active(control, state, inflight, unknown):
    ctl, task_id = control
    add_stage(ctl, task_id, state)
    with closing(ctl.summary._connect()) as db:
        assert counts_module.counts(db, task_id) == {'inflight': inflight, 'unknown': unknown}


@pytest.mark.parametrize('state,expected', [('processing', 'pausing'), ('unknown', 'unknown'),
                                         ('failed', 'unknown'), ('receipt_held', 'paused'), ('waiting_rag', 'paused')])
def test_pause_and_summary_include_stage_inflight_and_uncertainty(control, state, expected):
    ctl, task_id = control
    add_stage(ctl, task_id, state)
    result = ctl._pause(task_id)
    assert result['desired_state'] == 'paused'
    assert result['pause_status'] == expected
    assert ctl.summary.read(task_id, None, 1, 20)['pause_status'] == expected
    assert generation(ctl, task_id) == 7


@pytest.mark.parametrize('state', ['processing', 'unknown', 'failed'])
def test_next_generation_cannot_start_over_postprocessing(control, state):
    ctl, task_id = control
    add_stage(ctl, task_id, state)
    with closing(ctl.summary._connect()) as db:
        rev = ctl._task(db, task_id)['control_revision']
    with pytest.raises(control_module.ControlError):
        ctl._start(task_id, rev)
    assert generation(ctl, task_id) == 7


@pytest.mark.parametrize('state', ['receipt_held', 'waiting_rag'])
def test_safe_stage_does_not_prevent_explicit_next_generation(control, state):
    ctl, task_id = control
    add_stage(ctl, task_id, state)
    with closing(ctl.summary._connect()) as db:
        row = ctl._task(db, task_id)
    ctl.scheduler = SimpleNamespace(verify_waiting=lambda task, run: {'flow_run_id': run})
    ctl._start(task_id, row['control_revision'])
    assert generation(ctl, task_id) == 8


def test_delete_waits_for_processing_to_become_safe_without_holding_task_gate(control):
    ctl, task_id = control
    add_stage(ctl, task_id, 'processing')
    done, errors = threading.Event(), []
    def wait():
        try:
            delete_module._wait_first_step(ctl, task_id)
        except BaseException as exc:
            errors.append(exc)
        finally:
            done.set()
    worker = threading.Thread(target=wait)
    worker.start()
    try:
        assert not done.wait(.1)
        with ctl._lock('preprocessing', task_id):
            update_stage(ctl, 'receipt_held')
        assert done.wait(3)
        assert not errors
    finally:
        update_stage(ctl, 'receipt_held')
        worker.join(timeout=3)


@pytest.mark.parametrize('state', ['unknown', 'failed'])
def test_delete_wait_and_initial_delete_barrier_reject_unresolved_stages(control, state):
    ctl, task_id = control
    add_stage(ctl, task_id, state)
    with pytest.raises(control_module.ControlError):
        delete_module._wait_first_step(ctl, task_id)
    with closing(ctl.summary._connect()) as db:
        with pytest.raises(control_module.ControlError):
            delete_module._delete_locked(ctl, db, task_id)
        assert ctl._task(db, task_id)


@pytest.mark.parametrize('damage', ['missing', 'invalid'])
def test_missing_or_invalid_stage_table_fails_closed_in_pause_start_and_delete(control, damage):
    ctl, task_id = control
    if damage == 'missing':
        with closing(ctl.summary._connect()) as db, db:
            db.execute('DROP TABLE postprocessing_claims')
    else:
        add_stage(ctl, task_id, 'unrecognized')
    result = ctl._pause(task_id)
    assert result['desired_state'] == 'paused'
    assert result['pause_status'] == 'unknown'
    assert result['inflight_requests'] is None and result['unknown_requests'] is None
    with closing(ctl.summary._connect()) as db:
        assert ctl._task(db, task_id)['desired_state'] == 'paused'
    with closing(ctl.summary._connect()) as db:
        revision = ctl._task(db, task_id)['control_revision']
    with pytest.raises((ValueError, sqlite3.Error, control_module.ControlError)):
        ctl._start(task_id, revision)
    with pytest.raises((ValueError, sqlite3.Error, control_module.ControlError)):
        delete_module._wait_first_step(ctl, task_id)
    assert generation(ctl, task_id) == 7


@pytest.mark.parametrize('transition', ['before_plan_commit', 'during_file_cleanup'])
@pytest.mark.parametrize('state', ['processing', 'unknown', 'failed'])
def test_delete_rechecks_stage_before_files_and_final_task_removal(control, monkeypatch, transition, state):
    ctl, task_id = control
    add_stage(ctl, task_id, 'receipt_held')
    delete_module._request_delete(ctl, task_id)
    plan = {'paths': [{'synthetic': True}], 'shared_groups': [], 'receipt_records': []}
    # Preserve the real control transactions; replace only external cleanup.
    monkeypatch.setattr(delete_module, '_check_conversion_processes', lambda *args: None)
    monkeypatch.setattr(delete_module, '_cancel_local_task', lambda *args: None)
    monkeypatch.setattr(delete_module, '_assert_originals_outside_plan', lambda *args: None)
    cleanup_started = []
    def build_plan(*args):
        if transition == 'before_plan_commit':
            update_stage(ctl, state)
        return plan
    monkeypatch.setattr(delete_module, '_build_plan', build_plan)
    def remove_path(*args):
        cleanup_started.append(True)
        if transition == 'during_file_cleanup':
            update_stage(ctl, state)
    monkeypatch.setattr(delete_module, '_remove_path', remove_path)
    retention = importlib.import_module(NAME + '.first_step_retention')
    registration = importlib.import_module(NAME + '.first_step_registration')
    monkeypatch.setattr(retention, 'assert_deletion_safe', lambda *args, **kwargs: None)
    monkeypatch.setattr(registration, 'remove_task_receipt_events', lambda *args: None)
    with closing(ctl.summary._connect()) as db:
        with pytest.raises(control_module.ControlError):
            delete_module._delete_locked(ctl, db, task_id)
        assert ctl._task(db, task_id), 'task identity was deleted despite an unresolved stage'
    if transition == 'before_plan_commit':
        assert not cleanup_started


def test_stage_counts_are_scoped_to_the_target_task(control):
    ctl, task_id = control
    add_stage(ctl, str(uuid4()), 'processing')
    with closing(ctl.summary._connect()) as db:
        assert counts_module.counts(db, task_id) == {'inflight': 0, 'unknown': 0}


def test_existing_llm_and_stage_barriers_are_added_not_replaced(control):
    ctl, task_id = control
    add_stage(ctl, task_id, 'processing')
    add_stage(ctl, task_id, 'unknown')
    with closing(ctl.summary._connect()) as db, db:
        for index, state in enumerate(('admitted', 'returned_unverified', 'unknown', 'raw_complete')):
            db.execute('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       (task_id, str(index), 7, 'a' * 64, state, None, 'body', '/synthetic/work',
                        '/synthetic/work/model', 'b' * 64, str(uuid4()), 'c' * 64, 1, 1))
        assert counts_module.counts(db, task_id) == {'inflight': 3, 'unknown': 2}


def test_control_initialization_preserves_upstream_folder_flow_identity_constraint(control):
    ctl, task_id = control
    flow_id = str(uuid4())
    row = (task_id, 'a' * 32, 'paper', 'b' * 64, 'attempt', str(uuid4()), flow_id, 'c' * 64,
           'waiting_llm', 7, 1)
    with closing(ctl.summary._connect()) as db, db:
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?,?)', row)
        with pytest.raises(sqlite3.IntegrityError):
            db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                       (task_id, 'd' * 32, *row[2:]))


def test_delete_waits_for_folder_execution_even_after_preprocessing_claim_completed(control, tmp_path):
    ctl, task_id = control
    local = tmp_path / 'synthetic-waiting.sqlite3'
    ctl.local_intake_queue_db = local
    with sqlite3.connect(local) as db:
        db.execute('CREATE TABLE claims(task_id TEXT,state TEXT,attempt_id TEXT)')
        db.execute('CREATE TABLE preprocessing_folder_orders(task_id TEXT,attempt_id TEXT)')
        db.execute('CREATE TABLE groups(task_id TEXT,receipt_id TEXT,record TEXT,state TEXT)')
        db.execute('INSERT INTO claims VALUES(?,?,?)', (task_id, 'completed', 'existing-folder'))
        db.execute('INSERT INTO preprocessing_folder_orders VALUES(?,?)', (task_id, 'existing-folder'))
    lock_root = tmp_path / 'locks'
    attempts, people, papers = delete_module._local_lock_names(ctl, task_id, lock_root)
    assert attempts == [lock_root / 'folder-order-existing-folder.lock']
    assert people == papers == []


def test_delete_does_not_ignore_invalid_folder_order_identity(control, tmp_path):
    ctl, task_id = control
    local = tmp_path / 'invalid-waiting.sqlite3'
    ctl.local_intake_queue_db = local
    with sqlite3.connect(local) as db:
        db.execute('CREATE TABLE claims(task_id TEXT,state TEXT,attempt_id TEXT)')
        db.execute('CREATE TABLE preprocessing_folder_orders(task_id TEXT,attempt_id TEXT)')
        db.execute('CREATE TABLE groups(task_id TEXT,receipt_id TEXT,record TEXT,state TEXT)')
        db.execute('INSERT INTO preprocessing_folder_orders VALUES(?,?)', (task_id, '../invalid'))
    with pytest.raises(control_module.ControlError):
        delete_module._local_lock_names(ctl, task_id, tmp_path / 'locks')
