"""Incremental, local first-step result index for the read-only summary API."""
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import time


class GroupSummaryStore:
    def __init__(self, output_dir):
        self.path = Path(output_dir) / 'group-task-summaries' / 'summary-index.sqlite3'

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        return db

    def _initialize(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db, db:
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY, started_at_ns INTEGER NOT NULL,
                    task_status TEXT NOT NULL, total_groups INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS group_results (
                    task_id TEXT NOT NULL, seq INTEGER NOT NULL,
                    status TEXT NOT NULL, payload TEXT NOT NULL,
                    PRIMARY KEY(task_id, seq)
                );
                CREATE INDEX IF NOT EXISTS group_results_filter
                    ON group_results(task_id, status, seq);
            ''')
    def create_task(self, task_id, total):
        self._initialize()
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            existing = db.execute('SELECT task_status FROM tasks WHERE task_id=?', (task_id,)).fetchone()
            if existing is None:
                db.execute('INSERT INTO tasks VALUES (?, ?, ?, ?)',
                           (task_id, time.time_ns(), 'running', total))
            elif (existing['task_status'] != 'running' or
                  db.execute('SELECT 1 FROM group_results WHERE task_id=? LIMIT 1', (task_id,)).fetchone()):
                raise ValueError('第一步任务编号已经使用')
            else:
                db.execute('UPDATE tasks SET total_groups=? WHERE task_id=?', (total, task_id))

    def mark_failed(self, task_id, total):
        self._initialize()
        with closing(self._connect()) as db, db:
            db.execute('INSERT OR IGNORE INTO tasks VALUES (?, ?, ?, ?)',
                       (task_id, time.time_ns(), 'failed', total))
            db.execute('UPDATE tasks SET task_status=? WHERE task_id=?', ('failed', task_id))

    def record(self, task_id, index, row):
        payload = json.dumps(row, ensure_ascii=False, separators=(',', ':'))
        with closing(self._connect()) as db, db:
            db.execute('INSERT INTO group_results VALUES (?, ?, ?, ?)',
                       (task_id, index, row['status'], payload))

    def finish(self, task_id, status):
        with closing(self._connect()) as db, db:
            db.execute('UPDATE tasks SET task_status=? WHERE task_id=?', (status, task_id))

    def read(self, task_id, status_filter, page, page_size):
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        with closing(self._connect()) as db:
            db.execute('BEGIN')  # Counts and page must come from one snapshot.
            if task_id == 'current':
                task = db.execute('SELECT * FROM tasks ORDER BY started_at_ns DESC, rowid DESC LIMIT 1').fetchone()
            else:
                task = db.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
            if task is None:
                raise LookupError('task not found')
            task_id = task['task_id']
            counts = {'passed': 0, 'failed': 0, 'unrecognized': 0}
            for row in db.execute('SELECT status, COUNT(*) AS n FROM group_results '
                                  'WHERE task_id=? GROUP BY status', (task_id,)):
                counts[row['status']] = row['n']
            processed = sum(counts.values())
            where = 'task_id=?'
            args = [task_id]
            if status_filter is not None:
                where += ' AND status=?'
                args.append(status_filter)
            args.extend([page_size, (page - 1) * page_size])
            rows = db.execute('SELECT payload FROM group_results WHERE ' + where +
                              ' ORDER BY seq LIMIT ? OFFSET ?', args).fetchall()
            return dict(task_id=task_id, task_status=task['task_status'],
                        total_groups=task['total_groups'], processed_groups=processed,
                        passed_groups=counts['passed'], failed_groups=counts['failed'],
                        unrecognized_groups=counts['unrecognized'], filter=status_filter,
                        page=page, page_size=page_size,
                        list_total=counts[status_filter] if status_filter else processed,
                        items=[json.loads(row['payload']) for row in rows])
