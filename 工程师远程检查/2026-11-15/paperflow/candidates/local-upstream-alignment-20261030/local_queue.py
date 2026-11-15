"""Durable local acceptance records; never start document preparation here."""

from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

from local_handoff import read_registered_item


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _input_row(row):
    if not isinstance(row, dict):
        raise ValueError('正文或附件资料无效')
    required = ('file_id', 'upstream_file_id', 'path', 'sha256', 'bytes')
    if any(row.get(key) is None for key in required):
        raise ValueError('正文或附件身份不完整')
    keys = (*required, 'delivered_path', 'source_path', 'relative', 'attachment_id',
            'format', 'name', 'evidence_records')
    return {key: row[key] for key in keys if key in row}


def _accepted_record(task_id, observed):
    if not isinstance(task_id, str) or not task_id.strip():
        raise ValueError('缺少上游任务编号')
    if (observed.get('receipt_schema') != 'paperflow-receipt/1'
            or observed.get('receipt_state') != '已接收'
            or observed.get('observation')
            or observed.get('body_available') is not True):
        raise ValueError('本地核验未完整通过')
    paper = observed.get('paper')
    items = observed.get('items')
    if not isinstance(paper, dict) or not isinstance(items, list):
        raise ValueError('本地核验结果缺失')
    if any(row.get('kind') not in {'body', 'attachment'} or row.get('state') != '已接收'
           or row.get('unverified_evidence') for row in items):
        raise ValueError('正文或附件核验未完整通过')
    bodies = [row for row in items if row.get('kind') == 'body']
    attachments = [row for row in items if row.get('kind') == 'attachment']
    if len(bodies) != 1 or not isinstance(paper.get('attachments'), list):
        raise ValueError('正文或附件核验未完整通过')
    if (paper.get('body') != bodies[0]
            or paper['attachments'] != attachments):
        raise ValueError('回执条目与论文材料核验不一致')
    return {
        'task_id': task_id,
        'group_id': observed['group_id'],
        'origin_group_id': observed.get('origin_group_id'),
        'paper_id': observed['paper_id'],
        'receipt_id': observed['receipt_id'],
        'receipt_path': observed['receipt_path'],
        'receipt_sha256': observed['receipt_sha256'],
        'delivery_dir': observed['delivery_dir'],
        'result_version': observed.get('result_version'),
        'body': _input_row(bodies[0]),
        'attachments': [_input_row(row) for row in attachments],
    }


class LocalQueue:
    """SQLite-backed observations, isolated from the first-step database."""

    def __init__(self, path):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists() and self.path.stat().st_size:
            try:
                with closing(sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True)) as probe:
                    row = probe.execute("SELECT value FROM metadata WHERE key='queue_schema'").fetchone()
                if row is None or row[0] != 'paperflow-local-waiting/1':
                    raise ValueError('不能使用其他用途的数据库作为本地队列')
            except sqlite3.Error as exc:
                raise ValueError('不能使用其他用途的数据库作为本地队列') from exc
        with closing(self._connect()) as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS groups (
                    task_id TEXT NOT NULL, group_id TEXT NOT NULL,
                    paper_id TEXT NOT NULL, receipt_id TEXT NOT NULL,
                    state TEXT NOT NULL, record TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    PRIMARY KEY (task_id, group_id)
                );
                CREATE INDEX IF NOT EXISTS groups_paper ON groups(paper_id);
                CREATE TABLE IF NOT EXISTS observations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL, group_id TEXT NOT NULL,
                    receipt_id TEXT, kind TEXT NOT NULL, reason TEXT,
                    observed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS receive_failures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL, group_id TEXT NOT NULL,
                    paper_id TEXT, receipt_id TEXT, receipt_path TEXT, delivery_dir TEXT,
                    reason_code TEXT NOT NULL, reason TEXT NOT NULL, observed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY, value TEXT NOT NULL
                );
                INSERT OR IGNORE INTO metadata(key,value)
                    VALUES('queue_schema','paperflow-local-waiting/1');
            ''')

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    def _mark_unverified(self, task_id, item, error):
        if (not isinstance(task_id, str) or not task_id
                or not isinstance(item, dict) or not isinstance(item.get('group_id'), str)
                or not item['group_id']):
            return
        group_id = item['group_id']
        now = _utc_now()
        def safe_string(value):
            return value if isinstance(value, str) else None

        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT INTO receive_failures(task_id,group_id,paper_id,receipt_id,'
                       'receipt_path,delivery_dir,reason_code,reason,observed_at) '
                       'VALUES(?,?,?,?,?,?,?,?,?)',
                       (task_id, group_id, safe_string(item.get('paper_id')),
                        safe_string(item.get('receipt_id')), safe_string(item.get('receipt_path')),
                        safe_string(item.get('delivery_dir')), type(error).__name__,
                        str(error)[:1000] or type(error).__name__, now))
            old = db.execute('SELECT receipt_id FROM groups WHERE task_id=? AND group_id=?',
                             (task_id, group_id)).fetchone()
            if old is not None:
                db.execute("UPDATE groups SET state='needs_review', observed_at=? "
                           'WHERE task_id=? AND group_id=?', (now, task_id, group_id))
                db.execute('INSERT INTO observations(task_id,group_id,receipt_id,kind,reason,observed_at) '
                           'VALUES(?,?,?,?,?,?)',
                           (task_id, group_id, old['receipt_id'], 'recheck_failed',
                            type(error).__name__, now))

    def receive(self, task_id, item, *, receipt_root):
        """Verify one registered delivery and persist a local acceptance once."""
        try:
            observation = read_registered_item(item, receipt_root=receipt_root)
            record = _accepted_record(task_id, observation)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            self._mark_unverified(task_id, item, exc)
            raise

        now = _utc_now()
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            existing = db.execute('SELECT * FROM groups WHERE task_id=? AND group_id=?',
                                  (task_id, record['group_id'])).fetchone()
            if existing is not None:
                if existing['receipt_id'] != record['receipt_id'] or existing['paper_id'] != record['paper_id']:
                    db.execute("UPDATE groups SET state='needs_review', observed_at=? "
                               'WHERE task_id=? AND group_id=?', (now, task_id, record['group_id']))
                    db.execute('INSERT INTO observations(task_id,group_id,receipt_id,kind,reason,observed_at) '
                               'VALUES(?,?,?,?,?,?)',
                               (task_id, record['group_id'], record['receipt_id'], 'changed_receipt',
                                '同组回执或论文身份发生变化', now))
                    return {**json.loads(existing['record']), 'state': 'needs_review'}
                db.execute('UPDATE groups SET observed_at=? WHERE task_id=? AND group_id=?',
                           (now, task_id, record['group_id']))
                return {**json.loads(existing['record']), 'state': existing['state']}

            others = db.execute('SELECT task_id, group_id FROM groups WHERE paper_id=?',
                                (record['paper_id'],)).fetchall()
            state = 'needs_review' if others else 'waiting'
            if others:
                db.execute("UPDATE groups SET state='needs_review', observed_at=? WHERE paper_id=?",
                           (now, record['paper_id']))
            db.execute('INSERT INTO groups(task_id,group_id,paper_id,receipt_id,state,record,observed_at) '
                       'VALUES(?,?,?,?,?,?,?)',
                       (task_id, record['group_id'], record['paper_id'], record['receipt_id'], state,
                        json.dumps(record, ensure_ascii=False), now))
            db.execute('INSERT INTO observations(task_id,group_id,receipt_id,kind,reason,observed_at) '
                       'VALUES(?,?,?,?,?,?)',
                       (task_id, record['group_id'], record['receipt_id'], 'received',
                        '同一论文在其他任务中已有记录' if others else None, now))
            return {**record, 'state': state}

    def _list(self, state):
        with closing(self._connect()) as db:
            rows = db.execute('SELECT state,record FROM groups WHERE state=? ORDER BY task_id,group_id',
                              (state,)).fetchall()
        return [{**json.loads(row['record']), 'state': row['state']} for row in rows]

    def list_waiting(self):
        return self._list('waiting')

    def list_review(self):
        return self._list('needs_review')

    def reconcile_visible(self, visible, *, candidates=None):
        """Hold waiting entries absent from a complete stable upstream scan."""
        visible = set(visible)
        candidates = None if candidates is None else set(candidates)
        now = _utc_now()
        missing = []
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            rows = db.execute("SELECT task_id,group_id,receipt_id FROM groups WHERE state='waiting'").fetchall()
            for row in rows:
                key = (row['task_id'], row['group_id'], row['receipt_id'])
                if key in visible or (candidates is not None and key not in candidates):
                    continue
                missing.append(key)
                db.execute("UPDATE groups SET state='needs_review',observed_at=? "
                           'WHERE task_id=? AND group_id=?', (now, row['task_id'], row['group_id']))
                db.execute('INSERT INTO observations(task_id,group_id,receipt_id,kind,reason,observed_at) '
                           'VALUES(?,?,?,?,?,?)',
                           (row['task_id'], row['group_id'], row['receipt_id'], 'upstream_missing',
                            '完整扫描未找到原登记资料', now))
        return missing

    def record_scan(self, *, complete, reason=None):
        """Persist discovery status; failures never overwrite last success time."""
        if type(complete) is not bool:
            raise ValueError('扫描状态无效')
        now = _utc_now()
        values = {'scan_status': 'complete' if complete else 'incomplete',
                  'scan_observed_at': now, 'scan_reason': reason or ''}
        if complete:
            values['scan_completed_at'] = now
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            db.executemany('INSERT OR REPLACE INTO metadata(key,value) VALUES(?,?)', values.items())

    def snapshot(self, *, max_age_seconds=60):
        """Return a consistent local count; unknown or stale scan is not zero."""
        if not isinstance(max_age_seconds, (int, float)) or max_age_seconds <= 0:
            raise ValueError('扫描有效期无效')
        now = datetime.now(timezone.utc)
        with closing(self._connect()) as db:
            db.execute('BEGIN')
            meta = dict(db.execute('SELECT key,value FROM metadata').fetchall())
            waiting, oldest = db.execute("SELECT COUNT(*),MIN(observed_at) FROM groups WHERE state='waiting'").fetchone()
            review = db.execute("SELECT COUNT(*) FROM groups WHERE state='needs_review'").fetchone()[0]
        completed = meta.get('scan_completed_at')
        try:
            age = (now - datetime.fromisoformat(completed)).total_seconds() if completed else None
            oldest_age = (now - datetime.fromisoformat(oldest)).total_seconds() if oldest else None
        except (ValueError, TypeError):
            age = oldest_age = None
        available = bool(meta.get('scan_status') == 'complete' and age is not None
                         and 0 <= age <= max_age_seconds and review == 0
                         and (waiting == 0 or (oldest_age is not None and 0 <= oldest_age <= max_age_seconds)))
        return {
            'scope': 'all_tasks',
            'waiting_count': waiting if available else None,
            'waiting_available': available,
            'as_of': now.isoformat(),
            'upstream_scan_completed_at': completed,
            'oldest_verified_at': oldest,
            'needs_review_count': review,
            'preprocessing_handoff_connected': False,
        }
