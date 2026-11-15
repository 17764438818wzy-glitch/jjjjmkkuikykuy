"""Read-only stop guard used by the user-invoked candidate stop script."""
from contextlib import closing
from pathlib import Path
import sqlite3
import sys

from paperflow_prefect.settings import Settings


def main():
    settings = Settings.load()
    path = settings.path('classification_root') / 'group-task-summaries' / 'summary-index.sqlite3'
    if not path.is_file():
        return 0
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
        active = db.execute("SELECT 1 FROM tasks WHERE desired_state='running' OR start_pending=1 LIMIT 1").fetchone()
        groups = db.execute("SELECT 1 FROM control_groups WHERE state='running' LIMIT 1").fetchone()
        requests = db.execute("SELECT 1 FROM model_decisions WHERE state='inflight' LIMIT 1").fetchone()
    if active or groups or requests:
        print('候选任务或请求仍在运行；请先在候选页面暂停并等待其停止。', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
