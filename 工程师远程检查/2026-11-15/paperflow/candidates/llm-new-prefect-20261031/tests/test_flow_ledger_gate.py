"""A slow receipt check must not block the task pause gate."""
from contextlib import closing
from pathlib import Path
import sqlite3
import sys
import threading

from filelock import FileLock
import pytest

from test_folder_ledger import context

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from flow_folder_ledger import FolderLedger


def test_pause_can_win_while_prefect_and_receipt_are_being_checked(context):
    original, database, identity, manifest, _, upstream = context
    ledger = FolderLedger(database, original.lock_root)
    ledger.initialize()
    ledger.register(identity, upstream)
    entered, release, errors = threading.Event(), threading.Event(), []
    class SlowVerifier:
        def __call__(self, given): return upstream(given)
        def for_execution(self, given):
            entered.set()
            assert release.wait(5)
            return upstream(given)
    def begin():
        try:
            ledger.begin(identity, 7, SlowVerifier())
        except Exception as exc:
            errors.append(exc)
    thread = threading.Thread(target=begin)
    thread.start()
    assert entered.wait(5)
    with FileLock(str(original.lock_root / ('preprocessing-' + identity['task_id'] + '.lock')), timeout=1):
        with closing(sqlite3.connect(database)) as db, db:
            db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (identity['task_id'],))
    release.set()
    thread.join(5)
    assert not thread.is_alive()
    assert len(errors) == 1 and isinstance(errors[0], InterruptedError)
    assert ledger.counts(identity['task_id'])['waiting_llm'] == 1
