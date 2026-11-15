"""Candidate pause/delete integration using the real control code on a temp DB."""
from contextlib import closing
import importlib
import importlib.util
import os
from pathlib import Path
import sqlite3
import sys
import threading
import time
from types import ModuleType
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parents[1]
BASE = ROOT / 'candidates/first-step-control-20261029'
NAME = '_flow_candidate_first_step_llm'
package = ModuleType(NAME)
stage_name = os.environ.get('PAPERFLOW_TEST_STAGE')
control_dir = ROOT / 'candidates' / stage_name / 'control' if stage_name else HERE / 'flow_control'
package.__path__ = [str(control_dir), str(BASE / 'paperflow_prefect_v2'),
                    str(BASE / 'paperflow_prefect')]
package.__package__ = NAME
sys.modules[NAME] = package
control_module = importlib.import_module(NAME + '.first_step_control')
delete_module = importlib.import_module(NAME + '.first_step_delete')
if stage_name:
    admission_module = importlib.import_module('llm_queue.pause_admission')
else:
    spec = importlib.util.spec_from_file_location('flow_llm_admission_control_test', HERE / 'flow_pause_admission.py')
    admission_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(admission_module)


@pytest.fixture
def control(tmp_path):
    ctl = control_module.FirstStepControl(tmp_path / 'out', tmp_path / 'data')
    task = ctl.create_task('合成任务')['task_id']
    with closing(ctl.summary._connect()) as db, db:
        db.execute("UPDATE tasks SET desired_state='running',task_status='complete',start_generation=7,"
                   "start_pending=0 WHERE task_id=?", (task,))
    lock_root = ctl.data / 'control-locks'
    lock_root.mkdir(parents=True, exist_ok=True)
    guard = admission_module.AdmissionGuard(ctl.summary.path, lock_root)
    guard.initialize()
    options = {'kind': 'body', 'model_dir': tmp_path / 'work/model', 'receipt_id': 'a' * 64,
               'task_run_id': str(uuid4()), 'source_sha256': 'b' * 64}
    return ctl, guard, task, options


def test_terminal_first_step_pause_reports_llm_inflight_without_changing_first_step_status(control):
    ctl, guard, task, options = control
    with guard.admit(task, 7, 'c' * 64, 'd' * 64, **options) as token:
        token.start_transport(lambda: None)
        result = ctl._pause(task)
        assert result['task_status'] == 'complete'
        assert result['pause_status'] == 'pausing' and result['inflight_requests'] == 1
        summary = ctl.summary.read(task, None, 1, 20)
        assert summary['desired_state'] == 'paused' and summary['inflight_requests'] == 1
        assert summary['pause_status'] == 'pausing'
    with closing(ctl.summary._connect()) as db, db:
        db.execute("UPDATE llm_request_admissions SET state='raw_complete' WHERE task_id=?", (task,))
    result = ctl._pause(task)
    assert result['task_status'] == 'complete' and result['pause_status'] == 'paused'
    assert ctl.summary.read(task, None, 1, 20)['pause_status'] == 'paused'


def test_deletion_waits_for_admitted_or_unverified_request(control):
    ctl, guard, task, options = control
    ended, errors = threading.Event(), []
    def deleting():
        try:
            delete_module._wait_first_step(ctl, task)
        except BaseException as exc:
            errors.append(exc)
        finally:
            ended.set()
    with guard.admit(task, 7, 'c' * 64, 'd' * 64, **options) as token:
        token.start_transport(lambda: None)
        thread = threading.Thread(target=deleting)
        thread.start()
        assert not ended.wait(0.5)
    assert not ended.wait(0.5)  # returned response remains unverified
    with closing(ctl.summary._connect()) as db, db:
        db.execute("UPDATE llm_request_admissions SET state='raw_complete' WHERE task_id=?", (task,))
    assert ended.wait(3)
    thread.join(1)
    assert not errors


def test_deletion_blocks_unresolved_llm_result_and_preserves_the_row(control):
    ctl, guard, task, options = control
    with pytest.raises(RuntimeError):
        with guard.admit(task, 7, 'c' * 64, 'd' * 64, **options):
            raise RuntimeError('unknown synthetic outcome')
    with pytest.raises(control_module.ControlError, match='结果未确认'):
        delete_module._wait_first_step(ctl, task)
    with closing(ctl.summary._connect()) as db:
        with pytest.raises(control_module.ControlError, match='结果未确认'):
            delete_module._delete_locked(ctl, db, task)
    with closing(ctl.summary._connect()) as db:
        assert db.execute("SELECT state FROM llm_request_admissions WHERE task_id=?", (task,)).fetchone()[0] == 'unknown'


def test_pause_does_not_claim_completion_if_a_request_outcome_is_unknown(control):
    ctl, guard, task, options = control
    with pytest.raises(RuntimeError):
        with guard.admit(task, 7, 'c' * 64, 'd' * 64, **options):
            raise RuntimeError('synthetic unknown outcome')
    result = ctl._pause(task)
    assert result['desired_state'] == 'paused'
    assert result['unknown_requests'] == 1
    assert result['pause_status'] == 'unknown'
    assert ctl.summary.read(task, None, 1, 20)['pause_status'] == 'unknown'


def test_unknown_model_result_blocks_starting_a_new_generation(control):
    ctl, guard, task, options = control
    with pytest.raises(RuntimeError):
        with guard.admit(task, 7, 'c' * 64, 'd' * 64, **options):
            raise RuntimeError('synthetic unknown outcome')
    result = ctl._pause(task)
    with closing(ctl.summary._connect()) as db, db:
        db.execute("UPDATE tasks SET prefect_order_model='v2' WHERE task_id=?", (task,))
    with pytest.raises(control_module.ControlError, match='结果未确认'):
        ctl._start(task, result['control_revision'])
