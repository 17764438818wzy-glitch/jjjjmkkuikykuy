"""Pause wins before send; already invoked requests drain completely."""
from contextlib import closing
from pathlib import Path
import sqlite3
import sys
import threading
from uuid import uuid4

from filelock import FileLock

from test_raw_request import arguments, FakeOpener, FakeResponse

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import flow_pause_admission as admission
import flow_raw_request as raw


def prepare(args, tmp_path):
    db_path, locks = tmp_path / 'control.db', tmp_path / 'control-locks'
    locks.mkdir()
    task_id = str(uuid4())
    with closing(sqlite3.connect(db_path)) as db, db:
        db.executescript('CREATE TABLE tasks(task_id TEXT,desired_state TEXT,start_generation INTEGER,'
                         'start_pending INTEGER,task_status TEXT); CREATE TABLE task_deletions(task_id TEXT);')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?)', (task_id,'running',1,0,'complete'))
    guard = admission.AdmissionGuard(db_path, locks)
    guard.initialize()
    args['stop'] = admission.ControlStop(db_path, task_id, 1)
    ticket = str(uuid4())
    args['admit'] = lambda transport: guard.admit(task_id, 1, 'a' * 64, transport['request_sha256'],
        kind='body', model_dir=transport['work_dir'], receipt_id='b' * 64,
        task_run_id=ticket, source_sha256='c' * 64)
    return db_path, locks, task_id, guard


def pause(db_path, locks, task_id, done):
    with FileLock(str(locks / ('preprocessing-' + task_id + '.lock')), timeout=5):
        with closing(sqlite3.connect(db_path)) as db, db:
            db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (task_id,))
    done.set()


def test_pause_during_slow_source_check_creates_no_admission(arguments, tmp_path):
    args, _ = arguments
    db_path, locks, task_id, guard = prepare(args, tmp_path)
    entered, release, done, errors = threading.Event(), threading.Event(), threading.Event(), []
    checks = []
    def source():
        checks.append(1)
        if len(checks) == 3:
            entered.set()
            assert release.wait(5)
    args['check_source'] = source
    opener = FakeOpener()
    def run():
        try: raw.run_raw_request(**args, opener=opener)
        except Exception as exc: errors.append(exc)
    worker = threading.Thread(target=run)
    worker.start()
    assert entered.wait(5)
    pause(db_path, locks, task_id, done)
    release.set()
    worker.join(5)
    assert errors and isinstance(errors[0], InterruptedError)
    assert opener.calls == 0
    with closing(sqlite3.connect(db_path)) as db:
        assert db.execute('SELECT COUNT(*) FROM llm_request_admissions').fetchone()[0] == 0


def test_started_request_finishes_after_pause_and_no_new_request_is_opened(arguments, tmp_path):
    args, _ = arguments
    db_path, locks, task_id, guard = prepare(args, tmp_path)
    before_start, allow_start, paused, opened, finish, results, errors = [threading.Event() for _ in range(5)] + [[], []]
    real_once = args['request_once']
    def request_once(*values, **kwargs):
        original = kwargs['on_request_started']
        def delayed():
            before_start.set()
            assert allow_start.wait(5)
            original()
        kwargs['on_request_started'] = delayed
        return real_once(*values, **kwargs)
    class BlockingOpener:
        calls = 0
        def open(self, *_args, **_kwargs):
            self.calls += 1
            opened.set()
            assert finish.wait(5)
            return FakeResponse('完整原始回复','stop')
    args['request_once'] = request_once
    opener = BlockingOpener()
    def run():
        try: results.append(raw.run_raw_request(**args, opener=opener))
        except Exception as exc: errors.append(exc)
    worker = threading.Thread(target=run)
    worker.start()
    assert before_start.wait(5)
    pauser = threading.Thread(target=pause, args=(db_path, locks, task_id, paused))
    pauser.start()
    assert not paused.wait(.2)  # pause waits for the short pre-transport gate
    allow_start.set()
    assert opened.wait(5) and paused.wait(5)
    finish.set()
    worker.join(5)
    pauser.join(5)
    assert not errors and len(results) == 1 and opener.calls == 1
    guard.complete(task_id, 'a' * 64, results[0]['response_sha256'])
    with closing(sqlite3.connect(db_path)) as db:
        assert db.execute('SELECT state FROM llm_request_admissions').fetchone()[0] == 'raw_complete'
