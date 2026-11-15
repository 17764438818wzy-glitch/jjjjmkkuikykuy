"""Exercise task pause vs model admission on a synthetic local control DB."""
from contextlib import closing
import importlib.util
from pathlib import Path
import sqlite3
import threading
from uuid import uuid4

from filelock import FileLock
import pytest


spec = importlib.util.spec_from_file_location('pause_admission', Path(__file__).parent.parent / 'pause_admission.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


@pytest.fixture
def control(tmp_path):
    db_path = tmp_path / 'control.sqlite'
    task_id = str(uuid4())
    with closing(sqlite3.connect(db_path)) as db, db:
        db.executescript('''
            CREATE TABLE tasks(task_id TEXT PRIMARY KEY, desired_state TEXT,
                               start_generation INTEGER, start_pending INTEGER, task_status TEXT);
            CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY);
        ''')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?)', (task_id, 'running', 7, 0, 'complete'))
    root = tmp_path / 'locks'
    root.mkdir()
    guard = module.AdmissionGuard(db_path, root)
    guard.initialize()
    return guard, db_path, root, task_id


def admission_args(root):
    return {'kind': 'body', 'model_dir': str(root.parent / 'work' / 'model'),
            'receipt_id': 'f' * 64, 'task_run_id': str(uuid4()), 'source_sha256': 'e' * 64}


def test_pause_blocks_future_admission_and_counts_inflight_without_blocking(control):
    guard, db_path, root, task_id = control
    entered, release = threading.Event(), threading.Event()
    errors = []
    def send():
        try:
            with guard.admit(task_id, 7, 'a' * 64, 'b' * 64, **admission_args(root)):
                entered.set()
                assert release.wait(5)
        except BaseException as exc:
            errors.append(exc)
    thread = threading.Thread(target=send)
    thread.start()
    assert entered.wait(5)
    with FileLock(str(root / ('preprocessing-' + task_id + '.lock')), timeout=1):
        with closing(sqlite3.connect(db_path)) as db, db:
            db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (task_id,))
            assert guard.counts(db, task_id)['inflight'] == 1
    with pytest.raises(InterruptedError):
        with guard.admit(task_id, 7, 'c' * 64, 'd' * 64, **admission_args(root)):
            pass
    release.set()
    thread.join(5)
    assert not thread.is_alive() and not errors
    with closing(sqlite3.connect(db_path)) as db:
        assert guard.counts(db, task_id)['inflight'] == 1  # response not yet verified
    with pytest.raises(ValueError, match='证据'):
        guard.complete(task_id, 'a' * 64, 'e' * 64)
    with closing(sqlite3.connect(db_path)) as db:
        assert guard.counts(db, task_id)['inflight'] == 1


def test_old_generation_and_repeated_component_cannot_send(control):
    guard, db_path, root, task_id = control
    with pytest.raises(InterruptedError):
        with guard.admit(task_id, 6, 'a' * 64, 'b' * 64, **admission_args(root)):
            pass
    with guard.admit(task_id, 7, 'a' * 64, 'b' * 64, **admission_args(root)):
        pass
    with pytest.raises(ValueError, match='已有'):
        with guard.admit(task_id, 7, 'a' * 64, 'b' * 64, **admission_args(root)):
            pass
    with closing(sqlite3.connect(db_path)) as db:
        assert guard.counts(db, task_id)['inflight'] == 1


def test_uncertain_failure_is_retained_without_automatic_resend(control):
    guard, db_path, root, task_id = control
    with pytest.raises(RuntimeError, match='fake transport'):
        with guard.admit(task_id, 7, 'a' * 64, 'b' * 64, **admission_args(root)):
            raise RuntimeError('fake transport')
    with closing(sqlite3.connect(db_path)) as db:
        assert guard.counts(db, task_id)['unknown'] == 1
    with pytest.raises(ValueError, match='已有'):
        with guard.admit(task_id, 7, 'a' * 64, 'b' * 64, **admission_args(root)):
            pass


def test_control_stop_observes_pause_and_generation_change(control):
    _, db_path, _, task_id = control
    stop = module.ControlStop(db_path, task_id, 7)
    assert not stop.is_set()
    with closing(sqlite3.connect(db_path)) as db, db:
        db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (task_id,))
    assert stop.is_set() and stop.wait(0)
    with closing(sqlite3.connect(db_path)) as db, db:
        db.execute("UPDATE tasks SET desired_state='running',start_generation=8 WHERE task_id=?", (task_id,))
    assert stop.is_set()
