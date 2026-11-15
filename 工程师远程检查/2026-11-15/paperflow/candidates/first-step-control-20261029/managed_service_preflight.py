"""Read-only guard for starting the independent first-step services."""
from contextlib import closing
from pathlib import Path
import sqlite3


class UnsafeToStart(RuntimeError):
    pass


def check_control_available(control_db):
    """A v2 API can reconnect while Prefect continues running work."""
    control_db = Path(control_db).resolve()
    if not control_db.is_file():
        raise UnsafeToStart('first-step state database is unavailable')
    try:
        with closing(sqlite3.connect(control_db.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
            db.execute('SELECT COUNT(*) FROM tasks').fetchone()
    except sqlite3.Error as exc:
        raise UnsafeToStart('first-step state cannot be checked') from exc


def check_idle(control_db, prefect_db):
    control_db, prefect_db = Path(control_db).resolve(), Path(prefect_db).resolve()
    if not control_db.is_file() or not prefect_db.is_file():
        raise UnsafeToStart('first-step state database is unavailable')
    try:
        with closing(sqlite3.connect(control_db.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
            for query in (
                "SELECT COUNT(*) FROM tasks WHERE task_status IN ('starting','running','pausing') OR start_pending=1",
                "SELECT COUNT(*) FROM control_groups WHERE state='running'",
                "SELECT COUNT(*) FROM uploads WHERE state IN ('uploading','sealed')",
                "SELECT COUNT(*) FROM model_decisions WHERE state='inflight'",
            ):
                if db.execute(query).fetchone()[0]:
                    raise UnsafeToStart('first-step work or upload is active')
        with closing(sqlite3.connect(prefect_db.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
            if db.execute("SELECT COUNT(*) FROM flow_run WHERE state_type IN "
                          "('SCHEDULED','PENDING','RUNNING','CANCELLING')").fetchone()[0]:
                raise UnsafeToStart('Prefect contains unfinished or queued work')
    except sqlite3.Error as exc:
        raise UnsafeToStart('first-step state cannot be checked') from exc


if __name__ == '__main__':
    root = Path(__file__).resolve().parent
    check_idle(root / 'runtime/workspace/output/group-task-summaries/summary-index.sqlite3',
               root / 'runtime/prefect-4291/prefect.db')
