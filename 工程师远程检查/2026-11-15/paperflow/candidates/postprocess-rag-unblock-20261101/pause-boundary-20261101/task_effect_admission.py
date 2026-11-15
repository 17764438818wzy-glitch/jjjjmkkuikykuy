"""Admit one external effect under the existing parent-task transaction."""
from contextlib import closing, contextmanager
from pathlib import Path
import sqlite3


class TaskNotRunning(InterruptedError):
    """The parent denied this effect before it was sent."""


@contextmanager
def task_effect_admission(database, task_id, generation, *, started=False):
    path = Path(database).resolve(strict=True).as_uri() + '?mode=rw'
    with closing(sqlite3.connect(path, uri=True, timeout=10)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN IMMEDIATE')

        class Permit:
            def check_current(self):
                task = db.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
                deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone()
                if (not task or deleted or task['task_status'] in {'deleting', 'delete_failed', 'unknown'}
                        or not started and (task['desired_state'] != 'running' or task['start_pending']
                                            or task['start_generation'] != generation)):
                    raise TaskNotRunning('parent task is not running in this generation')

            def start_transport(self, record_intent):
                record_intent()
                db.commit()

        yield Permit()
