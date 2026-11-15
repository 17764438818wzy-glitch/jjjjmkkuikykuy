"""Incremental, local first-step result index for the read-only summary API."""
from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import time


def timestamp(value):
    return datetime.fromtimestamp(value / 1_000_000_000, timezone.utc).isoformat() if value is not None else None


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
                    task_status TEXT NOT NULL, total_groups INTEGER NOT NULL,
                    discovered_groups INTEGER, total_known INTEGER NOT NULL DEFAULT 1,
                    ungrouped_files TEXT NOT NULL DEFAULT '[]',
                    desired_state TEXT NOT NULL DEFAULT 'running',
                    control_revision INTEGER NOT NULL DEFAULT 0,
                    start_generation INTEGER NOT NULL DEFAULT 0,
                    prefect_run_id TEXT
                );
                CREATE TABLE IF NOT EXISTS group_results (
                    task_id TEXT NOT NULL, seq INTEGER NOT NULL,
                    status TEXT NOT NULL, payload TEXT NOT NULL,
                    PRIMARY KEY(task_id, seq)
                );
                CREATE INDEX IF NOT EXISTS group_results_filter
                    ON group_results(task_id, status, seq);
                CREATE INDEX IF NOT EXISTS group_results_registration
                    ON group_results(task_id, json_extract(payload, '$.registration_status'), seq);
            ''')
            columns = {row['name'] for row in db.execute('PRAGMA table_info(tasks)')}
            for name, definition in (
                ('discovered_groups', 'INTEGER'), ('total_known', 'INTEGER NOT NULL DEFAULT 1'),
                ('ungrouped_files', "TEXT NOT NULL DEFAULT '[]'"),
                ('desired_state', "TEXT NOT NULL DEFAULT 'running'"),
                ('control_revision', 'INTEGER NOT NULL DEFAULT 0'),
                ('start_generation', 'INTEGER NOT NULL DEFAULT 0'), ('prefect_run_id', 'TEXT'),
                ('prefect_order_model', "TEXT NOT NULL DEFAULT 'legacy'"),
                ('prefect_suspend_pending', 'INTEGER NOT NULL DEFAULT 0'),
                ('prefect_suspend_sequence', 'INTEGER NOT NULL DEFAULT 0'),
                ('name', "TEXT NOT NULL DEFAULT '未命名任务'"), ('updated_at_ns', 'INTEGER'),
                ('saved_at_ns', 'INTEGER'), ('auto_saved_at_ns', 'INTEGER'),
                ('upload_id', 'TEXT'), ('uploaded_files', 'INTEGER NOT NULL DEFAULT 0'),
                ('source_root', 'TEXT'), ('source_library', 'TEXT'),
                ('artifact_scoped', 'INTEGER NOT NULL DEFAULT 0'),
                ('delete_plan', 'TEXT'),
                ('start_pending', 'INTEGER NOT NULL DEFAULT 0'), ('worker_run_id', 'TEXT'),
                ('start_owner_pid', 'INTEGER'), ('start_owner_started', 'REAL'),
                ('worker_owner_pid', 'INTEGER'), ('worker_owner_started', 'REAL'), ('stop_reason', 'TEXT')):
                if name not in columns:
                    db.execute(f'ALTER TABLE tasks ADD COLUMN {name} {definition}')
    def create_task(self, task_id, total):
        self._initialize()
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            existing = db.execute('SELECT task_status FROM tasks WHERE task_id=?', (task_id,)).fetchone()
            if existing is None:
                db.execute('INSERT INTO tasks (task_id,started_at_ns,task_status,total_groups) VALUES (?, ?, ?, ?)',
                           (task_id, time.time_ns(), 'running', total))
            elif (existing['task_status'] != 'running' or
                  db.execute('SELECT 1 FROM group_results WHERE task_id=? LIMIT 1', (task_id,)).fetchone()):
                raise ValueError('第一步任务编号已经使用')
            else:
                db.execute('UPDATE tasks SET total_groups=? WHERE task_id=?', (total, task_id))

    def mark_failed(self, task_id, total):
        self._initialize()
        with closing(self._connect()) as db, db:
            db.execute('INSERT OR IGNORE INTO tasks (task_id,started_at_ns,task_status,total_groups) VALUES (?, ?, ?, ?)',
                       (task_id, time.time_ns(), 'failed', total))
            db.execute('UPDATE tasks SET task_status=? WHERE task_id=?', ('failed', task_id))

    def record(self, task_id, index, row):
        payload = json.dumps(row, ensure_ascii=False, separators=(',', ':'))
        with closing(self._connect()) as db, db:
            db.execute('INSERT INTO group_results VALUES (?, ?, ?, ?)',
                       (task_id, index, row['status'], payload))
            now = time.time_ns()
            db.execute('UPDATE tasks SET updated_at_ns=?,auto_saved_at_ns=? WHERE task_id=?', (now, now, task_id))

    def finish(self, task_id, status):
        with closing(self._connect()) as db, db:
            db.execute('UPDATE tasks SET task_status=? WHERE task_id=?', (status, task_id))

    def group_row(self, task_id, group_id):
        """Resolve one group in the requested task, independently of the page."""
        with closing(self._connect()) as db:
            has_control = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='control_groups'").fetchone()
            if has_control:
                group = db.execute('SELECT seq FROM control_groups WHERE task_id=? AND group_id=?',
                                   (task_id, group_id)).fetchone()
                if group is not None:
                    saved = db.execute('SELECT payload FROM group_results WHERE task_id=? AND seq=?',
                                       (task_id, group['seq'])).fetchone()
                    if saved is None:
                        return {'group_id': group_id, 'delivery_dir': None}
                    row = json.loads(saved['payload'])
                    if row.get('group_id') not in (None, group_id):
                        return {'group_id': group_id, 'delivery_dir': None}
                    row['group_id'] = group_id
                    return row
            saved = db.execute("SELECT payload FROM group_results WHERE task_id=? AND json_valid(payload) "
                               "AND json_extract(payload, '$.group_id')=? ORDER BY seq LIMIT 1",
                               (task_id, group_id)).fetchone()
            if saved is None:
                raise LookupError('group not found')
            return json.loads(saved['payload'])

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
            registration_counts = {'registered': 0, 'failed': 0}
            for row in db.execute("SELECT json_extract(payload, '$.registration_status') AS state, COUNT(*) AS n "
                                  'FROM group_results WHERE task_id=? GROUP BY state', (task_id,)):
                if row['state'] in registration_counts:
                    registration_counts[row['state']] = row['n']
            where = 'task_id=?'
            args = [task_id]
            if status_filter == 'registration_failed':
                where += " AND json_extract(payload, '$.registration_status')='failed'"
            elif status_filter is not None:
                where += ' AND status=?'
                args.append(status_filter)
            args.extend([page_size, (page - 1) * page_size])
            rows = db.execute('SELECT payload FROM group_results WHERE ' + where +
                              ' ORDER BY seq LIMIT ? OFFSET ?', args).fetchall()
            items = [json.loads(row['payload']) for row in rows]
            discovered = task['discovered_groups'] if task['discovered_groups'] is not None else task['total_groups']
            active = inflight = unknown = unknown_groups = 0
            downstream_available = True
            unknown_details = []
            has_control = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='control_groups'").fetchone()
            if has_control:
                active = db.execute("SELECT COUNT(*) FROM control_groups WHERE task_id=? AND state='running'",
                                    (task_id,)).fetchone()[0]
                inflight = db.execute("SELECT COUNT(*) FROM model_decisions WHERE task_id=? AND state='inflight'",
                                      (task_id,)).fetchone()[0]
                unknown = db.execute("SELECT COUNT(*) FROM model_decisions WHERE task_id=? AND state='unknown'",
                                     (task_id,)).fetchone()[0]
                from .llm_admission_state import counts as llm_counts
                try:
                    llm = llm_counts(db, task_id)
                    inflight += llm['inflight']
                    unknown += llm['unknown']
                except (ValueError, sqlite3.Error, KeyError, IndexError, TypeError):
                    downstream_available = False
                    inflight = unknown = None
                unknown_groups = db.execute("SELECT COUNT(*) FROM control_groups WHERE task_id=? AND state='unknown'",
                                            (task_id,)).fetchone()[0]
                unknown_details = [dict(group=row['group_root'], step=row['step'],
                                        reason=row['detail'] or '模型请求结果未确认')
                    for row in db.execute("SELECT g.group_root,d.step,d.detail FROM model_decisions d "
                        "JOIN control_groups g ON g.task_id=d.task_id AND g.seq=d.seq "
                        "WHERE d.task_id=? AND d.state='unknown' ORDER BY d.seq,d.step LIMIT 50", (task_id,))]
                if status_filter == 'unknown':
                    items = []
                    for row in db.execute("SELECT * FROM control_groups WHERE task_id=? AND state='unknown' "
                                          'ORDER BY seq LIMIT ? OFFSET ?',
                                          (task_id, page_size, (page - 1) * page_size)):
                        body_hash = row['body_sha256'] if 'body_sha256' in row.keys() else None
                        display_hash = body_hash or row['group_id']
                        items.append(dict(group=row['group_root'], group_id=row['group_id'], status='unknown',
                                          display_hash=display_hash, hash_type='file_sha256' if body_hash else
                                          'group_id' if display_hash else None, display_title=display_hash,
                                          display_subtitle='题目未识别', title_status='未提取', paper_title=None,
                                          paper_title_candidate=None, reason=row['blocked_reason'] or '模型请求结果未确认',
                                          delivery_dir=None, delivery_complete=False))
            has_deletions = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='task_deletions'").fetchone()
            deleting = has_deletions and db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone()
            pause_status = None
            if task['desired_state'] == 'paused':
                pause_status = ('unknown' if not downstream_available else
                                'pausing' if active or inflight or task['start_pending'] else
                                'unknown' if unknown or task['task_status'] == 'unknown' else 'paused')
            return dict(task_id=task_id, name=task['name'],
                        task_status='deleting' if deleting else task['task_status'],
                        desired_state=task['desired_state'],
                        pause_status=pause_status,
                        total_groups=task['total_groups'] if task['total_known'] else None,
                        discovered_groups=discovered, ungrouped_files=json.loads(task['ungrouped_files']),
                        control_revision=task['control_revision'], processed_groups=processed,
                        upload_id=task['upload_id'], uploaded_files=task['uploaded_files'],
                        active_groups=active, inflight_requests=inflight, unknown_requests=unknown,
                        unknown_groups=unknown_groups,
                        unknown_details=unknown_details, stop_reason=task['stop_reason'],
                        created_at=timestamp(task['started_at_ns']),
                        updated_at=timestamp(task['updated_at_ns'] or task['started_at_ns']),
                        saved_at=timestamp(task['saved_at_ns']), auto_saved_at=timestamp(task['auto_saved_at_ns']),
                        passed_groups=counts['passed'], failed_groups=counts['failed'],
                        unrecognized_groups=counts['unrecognized'],
                        registered_groups=registration_counts['registered'],
                        registration_failed_groups=registration_counts['failed'], filter=status_filter,
                        page=page, page_size=page_size,
                        list_total=unknown_groups if status_filter == 'unknown' else
                                   registration_counts['failed'] if status_filter == 'registration_failed' else
                                   counts[status_filter] if status_filter else processed,
                        items=items)
