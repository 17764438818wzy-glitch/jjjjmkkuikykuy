import sqlite3
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from paperflow_prefect_v2.first_step_control import FirstStepControl, PauseRequested, RequestNotSent


def prepared(tmp_path):
    control = FirstStepControl(tmp_path / 'output', tmp_path / 'data')
    task_id = control.create_task('synthetic')['task_id']
    with control.summary._connect() as db, db:
        db.execute("UPDATE tasks SET desired_state='running',task_status='running',"
                   "start_generation=1,start_pending=0,total_groups=2,discovered_groups=2,total_known=1 "
                   'WHERE task_id=?', (task_id,))
        db.executemany('INSERT INTO control_groups(task_id,seq,group_root,state) VALUES(?,?,?,?)',
                       [(task_id, 0, 'synthetic-one', 'pending'), (task_id, 1, 'synthetic-two', 'pending')])
    assert control.claim_next(task_id, 1)[0] == 0
    return control, task_id


@pytest.mark.parametrize('restart', [False, True])
def test_pause_drains_claimed_group_without_starting_another(tmp_path, restart):
    control, task_id = prepared(tmp_path)
    paused = control._pause(task_id)
    assert paused['task_status'] == 'pausing'
    assert control.claim_next(task_id, 1) is None
    if restart:
        with control.summary._connect() as db, db:
            db.execute("UPDATE tasks SET desired_state='running',start_generation=2 WHERE task_id=?", (task_id,))
    assert control.group_allowed(task_id, 0, 1)
    sent = []
    for step in ('body', 'attachment'):
        control.invoke_model(task_id, 0, step, 'snapshot', step, lambda: sent.append(step),
                             lambda _: {'choice': step}, timeout=1, generation=1)
    assert sent == ['body', 'attachment']
    assert control.cached_model(task_id, 0, 'body', 'snapshot', 'body', 1) == {'choice': 'body'}
    control.finish_group(task_id, 0, 1, {'status': 'passed'})
    with control.summary._connect() as db:
        states = dict(db.execute('SELECT seq,state FROM control_groups WHERE task_id=?', (task_id,)))
    assert states == {0: 'done', 1: 'pending'}


def test_deleted_group_cannot_continue(tmp_path):
    control, task_id = prepared(tmp_path)
    with control.summary._connect() as db, db:
        db.execute("UPDATE tasks SET task_status='deleting' WHERE task_id=?", (task_id,))
    assert not control.group_allowed(task_id, 0, 1)
    with pytest.raises(PauseRequested):
        control.invoke_model(task_id, 0, 'body', 'snapshot', 'body', lambda: None,
                             lambda _: {}, timeout=1, generation=1)


def test_connection_failure_during_paused_drain_keeps_the_reason(tmp_path):
    control, task_id = prepared(tmp_path)
    control._pause(task_id)
    def unavailable():
        raise RequestNotSent('synthetic connection refused')
    with pytest.raises(RequestNotSent):
        control.invoke_model(task_id, 0, 'body', 'snapshot', 'body', unavailable,
                             lambda _: {}, timeout=1, generation=1)
    with control.summary._connect() as db:
        task = db.execute('SELECT desired_state,stop_reason FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        assert task['desired_state'] == 'paused' and 'Nimble' in task['stop_reason']
