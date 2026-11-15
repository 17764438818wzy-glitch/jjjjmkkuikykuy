"""Durable local acceptance records; never start document preparation here."""

from contextlib import closing, contextmanager, nullcontext
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path, PureWindowsPath
import re
import sqlite3
import uuid
from urllib.error import HTTPError

from filelock import FileLock, Timeout

if __package__:
    from .local_handoff import read_registered_item
else:
    from local_handoff import read_registered_item


class RetentionIdentityConflict(ValueError):
    pass


class ReceiverUnavailable(ValueError):
    """The receiver must finish checking its current session before a claim."""


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _current_receiver_session(db):
    meta = dict(db.execute("SELECT key,value FROM metadata WHERE key IN "
                           "('receiver_status','receiver_session_id','receiver_heartbeat_at')"))
    try:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(meta['receiver_heartbeat_at'])).total_seconds()
    except (KeyError, ValueError, TypeError):
        raise ReceiverUnavailable('接收进程心跳不可核验') from None
    session = meta.get('receiver_session_id')
    if meta.get('receiver_status') != 'running' or not session or not 0 <= age <= 60:
        raise ReceiverUnavailable('接收进程当前不可核验')
    return session


def _input_row(row):
    if not isinstance(row, dict):
        raise ValueError('正文或附件资料无效')
    required = ('file_id', 'upstream_file_id', 'path', 'sha256', 'bytes')
    if any(row.get(key) is None for key in required):
        raise ValueError('正文或附件身份不完整')
    keys = (*required, 'delivered_path', 'source_path', 'relative', 'attachment_id',
            'format', 'name', 'evidence_records')
    return {key: row[key] for key in keys if key in row}


def _delivery_fingerprint(item, receipt_root):
    """Detect changes to a previously hash-verified delivery without rereading large files."""
    receipt_id = item.get('receipt_id')
    if not isinstance(receipt_id, str) or not re.fullmatch(r'[0-9a-f]{64}', receipt_id):
        raise ValueError('登记回执编号无效')
    root = Path(receipt_root).resolve(strict=True)
    receipt = Path(item['receipt_path']).resolve(strict=True)
    if receipt != (root / 'receipts' / (receipt_id + '.json')).resolve(strict=True):
        raise ValueError('登记回执不属于指定目录')
    delivery = Path(item['delivery_dir']).resolve(strict=True)
    if not delivery.is_dir():
        raise ValueError('交付目录不可用')
    manifest_path = (delivery / 'delivery.json').resolve(strict=True)
    handoff_path = (delivery / 'handoff.json').resolve(strict=True)
    if not manifest_path.is_relative_to(delivery) or not handoff_path.is_relative_to(delivery):
        raise ValueError('交付清单路径无效')
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    if not isinstance(manifest.get('files'), list) or not isinstance(manifest.get('evidence_records'), list):
        raise ValueError('交付清单无效')
    handoff = json.loads(handoff_path.read_text(encoding='utf-8'))
    if not isinstance(handoff.get('attachments'), list):
        raise ValueError('正文或附件清单无效')
    selected = ([handoff['body']] if handoff.get('body') else []) + handoff['attachments']
    ids = {row['file_id'] for row in selected}
    declared = [row for row in manifest['files'] if row.get('file_id') in ids]
    if len(ids) != len(selected) or len(declared) != len(selected):
        raise ValueError('正文或附件身份不完整')

    def inside(base, relative):
        if (not isinstance(relative, str) or not relative or '\\' in relative
                or PureWindowsPath(relative).drive or Path(relative).is_absolute()
                or '..' in Path(relative).parts):
            raise ValueError('交付文件路径无效')
        base = base.resolve(strict=True)
        path = (base / relative).resolve(strict=True)
        if path == base or not path.is_relative_to(base) or not path.is_file():
            raise ValueError('交付文件路径无效')
        return path

    metadata = [receipt, manifest_path, handoff_path]
    metadata.extend(inside(delivery, ref['path']) for ref in manifest['evidence_records'])
    files = [inside(delivery / 'files', row['relative']) for row in declared]
    all_paths = sorted({str(path): path for path in metadata + files}.items())
    metadata_set = {str(path) for path in metadata}
    result = []
    for name, path in all_paths:
        stat = path.stat()
        digest = None
        if name in metadata_set:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
        result.append([name, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns,
                       stat.st_dev, stat.st_ino, digest])
    return result


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

    def __init__(self, path, *, require_retention=False, consumer_id='local-intake', require_live_watcher=False):
        self.path = Path(path).resolve()
        self.require_retention = require_retention
        self.consumer_id = consumer_id
        self.require_live_watcher = require_live_watcher
        self.receiver_session_id = None
        self.watch_owner_thread = None
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
                    observed_at TEXT NOT NULL, review_reason TEXT, checked_session TEXT,
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
                CREATE TABLE IF NOT EXISTS retention_requests (
                    task_id TEXT NOT NULL, group_id TEXT NOT NULL, paper_id TEXT NOT NULL,
                    receipt_id TEXT NOT NULL, consumer_id TEXT NOT NULL,
                    request_id TEXT NOT NULL UNIQUE, retention_id TEXT,
                    state TEXT NOT NULL, last_error TEXT, confirm_error TEXT,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, checked_session TEXT,
                    PRIMARY KEY(task_id,group_id,receipt_id,consumer_id)
                );
                CREATE TABLE IF NOT EXISTS claims (
                    consumer_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
                    task_id TEXT NOT NULL, group_id TEXT NOT NULL, receipt_id TEXT NOT NULL,
                    created_at TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'processing',
                    PRIMARY KEY(consumer_id,attempt_id),
                    UNIQUE(task_id,group_id,receipt_id)
                );
                CREATE TABLE IF NOT EXISTS failed_claims (
                    consumer_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
                    task_id TEXT NOT NULL, group_id TEXT NOT NULL, receipt_id TEXT NOT NULL,
                    created_at TEXT NOT NULL, archived_at TEXT NOT NULL,
                    PRIMARY KEY(consumer_id,attempt_id)
                );
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY, value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS cancelled_tasks (
                    task_id TEXT PRIMARY KEY, cancelled_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS preprocessing_handoffs (
                    receipt_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
                    group_id TEXT NOT NULL, paper_id TEXT NOT NULL,
                    consumer_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
                    state TEXT NOT NULL, handoff TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(consumer_id, attempt_id)
                );
                CREATE TABLE IF NOT EXISTS preprocessing_failures (
                    consumer_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
                    error_type TEXT NOT NULL, observed_at TEXT NOT NULL,
                    PRIMARY KEY(consumer_id, attempt_id)
                );
                CREATE TABLE IF NOT EXISTS preprocessing_folder_orders (
                    task_id TEXT NOT NULL, group_id TEXT NOT NULL, receipt_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL, prefect_run_id TEXT UNIQUE,
                    folder_label TEXT NOT NULL, created_at TEXT NOT NULL,
                    PRIMARY KEY(task_id, group_id)
                );
                INSERT OR IGNORE INTO metadata(key,value)
                    VALUES('queue_schema','paperflow-local-waiting/1');
            ''')
            with db:
                db.execute('BEGIN IMMEDIATE')
                for table, column, declaration in (('groups', 'review_reason', 'TEXT'),
                                                    ('groups', 'checked_session', 'TEXT'),
                                                    ('claims', 'state', "TEXT NOT NULL DEFAULT 'processing'"),
                                                    ('retention_requests', 'checked_session', 'TEXT'),
                                                    ('retention_requests', 'confirm_error', 'TEXT')):
                    existing = {row['name'] for row in db.execute('PRAGMA table_info(' + table + ')')}
                    if column not in existing:
                        db.execute('ALTER TABLE ' + table + ' ADD COLUMN ' + column + ' ' + declaration)

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    @contextmanager
    def _claim_transaction(self, admission):
        with closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                with (admission() if admission else nullcontext()):
                    yield db
                    db.commit()
            except BaseException:
                db.rollback()
                raise

    def prepare_retention(self, task_id, item):
        """Persist an idempotency key before any upstream reservation request."""
        if not isinstance(item, dict):
            raise ValueError('资料组身份缺失')
        values = (task_id, item.get('group_id'), item.get('paper_id'),
                  item.get('receipt_id'), self.consumer_id)
        if any(not isinstance(value, str) or not value for value in values):
            raise ValueError('接收占用身份缺失')
        now = _utc_now()
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT 1 FROM cancelled_tasks WHERE task_id=?', (task_id,)).fetchone():
                raise ValueError('任务已删除')
            db.execute('INSERT OR IGNORE INTO retention_requests(task_id,group_id,paper_id,receipt_id,'
                       'consumer_id,request_id,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)',
                       (*values, str(uuid.uuid4()), 'intent', now, now))
            row = db.execute('SELECT * FROM retention_requests WHERE task_id=? AND group_id=? '
                             'AND receipt_id=? AND consumer_id=?',
                             (task_id, item['group_id'], item['receipt_id'], self.consumer_id)).fetchone()
            if row is None or row['paper_id'] != item['paper_id'] or row['state'] == 'invalid':
                raise ValueError('接收占用身份或状态不一致')
            return dict(row)

    def record_retention(self, intent, response, *, confirmed=False):
        if not isinstance(response, dict):
            raise ValueError('上游接收占用响应无效')
        for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'consumer_id', 'request_id'):
            if key not in response:
                raise ValueError('上游接收占用响应不完整')
            if response.get(key) != intent[key]:
                raise RetentionIdentityConflict('上游接收占用身份不一致')
        retention_id = response.get('retention_id')
        try:
            if str(uuid.UUID(retention_id)) != retention_id:
                raise ValueError('invalid id')
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError('上游接收占用编号无效') from exc
        state = response.get('state')
        if state not in ({'accepted'} if confirmed else {'reserved', 'accepted'}):
            raise ValueError('上游接收占用状态无效')
        if intent['retention_id'] and intent['retention_id'] != retention_id:
            raise RetentionIdentityConflict('上游接收占用编号发生变化')
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            changed = db.execute("UPDATE retention_requests SET retention_id=?,state=?,last_error=NULL,updated_at=?,"
                                 "checked_session=?,confirm_error=CASE WHEN ?='accepted' THEN NULL ELSE confirm_error END "
                                 "WHERE request_id=? AND state NOT IN ('invalid','cancelled') "
                                 "AND NOT EXISTS(SELECT 1 FROM cancelled_tasks t WHERE t.task_id=retention_requests.task_id) "
                                 'AND (retention_id IS NULL OR retention_id=?)',
                                 (retention_id, state, _utc_now(), self.receiver_session_id, state,
                                  intent['request_id'], retention_id)).rowcount
            if changed != 1:
                raise ValueError('接收占用已变化')
            return dict(db.execute('SELECT * FROM retention_requests WHERE request_id=?',
                                   (intent['request_id'],)).fetchone())

    def retention_error(self, intent, error, *, invalid=False, phase='reserve'):
        reason = f'HTTP_{error.code}' if isinstance(error, HTTPError) and isinstance(error.code, int) else type(error).__name__
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT 1 FROM cancelled_tasks WHERE task_id=?', (intent['task_id'],)).fetchone():
                return
            if invalid:
                db.execute("UPDATE retention_requests SET state='invalid',last_error=?,updated_at=?,"
                           "confirm_error=CASE WHEN ?='confirm' THEN ? ELSE confirm_error END WHERE request_id=?",
                           (reason, _utc_now(), phase, reason, intent['request_id']))
                db.execute("UPDATE groups SET state='needs_review',review_reason='retention_invalid',observed_at=? "
                           'WHERE task_id=? AND group_id=? AND receipt_id=?',
                           (_utc_now(), intent['task_id'], intent['group_id'], intent['receipt_id']))
            else:
                db.execute("UPDATE retention_requests SET last_error=?,updated_at=?,"
                           "confirm_error=CASE WHEN ?='confirm' THEN ? ELSE confirm_error END WHERE request_id=?",
                           (reason, _utc_now(), phase, reason, intent['request_id']))

    def retained_tasks(self):
        with closing(self._connect()) as db:
            return [row[0] for row in db.execute(
                "SELECT DISTINCT task_id FROM retention_requests "
                "WHERE consumer_id=? AND state IN ('reserved','accepted')",
                (self.consumer_id,)).fetchall()]

    def retention_unavailable(self, task_id):
        """Pause affected groups when the live protection capability cannot be checked."""
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            db.execute("UPDATE retention_requests SET last_error='upstream_unavailable',updated_at=? "
                       "WHERE task_id=? AND consumer_id=? AND state IN ('reserved','accepted')",
                       (_utc_now(), task_id, self.consumer_id))
            db.execute("UPDATE groups SET review_reason=CASE WHEN state='waiting' THEN 'retention_unavailable' "
                       "WHEN state='processing' THEN 'retention_unavailable_processing' "
                       "ELSE COALESCE(review_reason,'unknown') END, state='needs_review', observed_at=? "
                       "WHERE task_id=? AND state!='ready_for_llm' AND EXISTS(SELECT 1 FROM retention_requests r WHERE r.task_id=groups.task_id "
                       "AND r.group_id=groups.group_id AND r.receipt_id=groups.receipt_id AND r.consumer_id=? "
                       "AND r.state IN ('reserved','accepted'))",
                       (_utc_now(), task_id, self.consumer_id))

    def reconcile_retention(self, task_id, response):
        """Compare persisted protection references with the currently running upstream."""
        if (not isinstance(response, dict) or response.get('task_id') != task_id
                or not isinstance(response.get('items'), list)):
            raise ValueError('上游接收保护列表无效')
        refs = {}
        for row in response['items']:
            if not isinstance(row, dict) or not isinstance(row.get('retention_id'), str):
                raise ValueError('上游接收保护列表无效')
            if row['retention_id'] in refs:
                raise ValueError('上游接收保护编号重复')
            refs[row['retention_id']] = row
        invalid = 0
        keys = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'consumer_id', 'request_id', 'retention_id')
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            rows = db.execute("SELECT r.* FROM retention_requests r "
                              "WHERE r.task_id=? AND r.consumer_id=? AND r.state IN ('reserved','accepted')",
                              (task_id, self.consumer_id)).fetchall()
            for local in rows:
                remote = refs.get(local['retention_id'])
                valid = (remote is not None and all(remote.get(key) == local[key] for key in keys)
                         and remote.get('state') in ('reserved', 'accepted')
                         and not (local['state'] == 'accepted' and remote['state'] != 'accepted'))
                if not valid:
                    invalid += 1
                    db.execute("UPDATE retention_requests SET state='invalid',last_error='remote_reference_changed',"
                               "updated_at=? WHERE request_id=?", (_utc_now(), local['request_id']))
                    db.execute("UPDATE groups SET state='needs_review',review_reason='retention_invalid',observed_at=? "
                               "WHERE task_id=? AND group_id=? AND receipt_id=?",
                               (_utc_now(), task_id, local['group_id'], local['receipt_id']))
                    continue
                db.execute("UPDATE retention_requests SET state=?,last_error=NULL,updated_at=?,checked_session=?,"
                           "confirm_error=CASE WHEN ?='accepted' THEN NULL ELSE confirm_error END WHERE request_id=?",
                           (remote['state'], _utc_now(), self.receiver_session_id, remote['state'], local['request_id']))
                db.execute("UPDATE groups SET review_reason='recheck_failed' WHERE task_id=? AND group_id=? "
                           "AND receipt_id=? AND state='needs_review' AND review_reason='retention_unavailable'",
                           (task_id, local['group_id'], local['receipt_id']))
        return invalid

    def _has_retention(self, task_id, item):
        with closing(self._connect()) as db:
            return bool(db.execute('SELECT 1 FROM retention_requests WHERE task_id=? AND group_id=? '
                                   'AND paper_id=? AND receipt_id=? AND consumer_id=? '
                                   "AND state IN ('reserved','accepted') AND retention_id IS NOT NULL AND last_error IS NULL",
                                   (task_id, item.get('group_id'), item.get('paper_id'),
                                    item.get('receipt_id'), self.consumer_id)).fetchone())

    def check_source_identity(self, task_id, item):
        """Freeze an existing group if the upstream changes its registered identity."""
        if not isinstance(item, dict) or not isinstance(item.get('group_id'), str):
            return False
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT 1 FROM cancelled_tasks WHERE task_id=?', (task_id,)).fetchone():
                return False
            old = db.execute('SELECT paper_id,receipt_id FROM groups WHERE task_id=? AND group_id=?',
                             (task_id, item['group_id'])).fetchone()
            if old is None or (old['paper_id'] == item.get('paper_id') and
                               old['receipt_id'] == item.get('receipt_id')):
                return True
            now = _utc_now()
            db.execute("UPDATE groups SET state='needs_review',review_reason='identity_conflict',observed_at=? "
                       'WHERE task_id=? AND group_id=?', (now, task_id, item['group_id']))
            db.execute('INSERT INTO observations(task_id,group_id,receipt_id,kind,reason,observed_at) '
                       'VALUES(?,?,?,?,?,?)',
                       (task_id, item['group_id'], item.get('receipt_id') if isinstance(item.get('receipt_id'), str) else None, 'identity_conflict',
                        '上游论文或回执身份发生变化', now))
            return False

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
            if db.execute('SELECT 1 FROM cancelled_tasks WHERE task_id=?', (task_id,)).fetchone():
                return
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
                db.execute("UPDATE groups SET review_reason=CASE WHEN state='needs_review' AND "
                           "COALESCE(review_reason,'unknown') NOT IN ('recheck_failed','upstream_missing','claim_validation_failed') "
                           "THEN COALESCE(review_reason,'unknown') ELSE 'recheck_failed' END, state='needs_review', observed_at=? "
                           'WHERE task_id=? AND group_id=?', (now, task_id, group_id))
                db.execute('INSERT INTO observations(task_id,group_id,receipt_id,kind,reason,observed_at) '
                           'VALUES(?,?,?,?,?,?)',
                           (task_id, group_id, old['receipt_id'], 'recheck_failed',
                            type(error).__name__, now))

    def receive(self, task_id, item, *, receipt_root):
        """Verify one registered delivery and persist a local acceptance once."""
        try:
            if self.require_retention and (not isinstance(item, dict) or not self._has_retention(task_id, item)):
                raise ValueError('上游接收占用尚未确认')
            if (not isinstance(item, dict) or item.get('status') != 'passed'
                    or item.get('registration_status') != 'registered'
                    or item.get('delivery_complete') is not True):
                raise ValueError('该资料组尚未完成分类与登记')
            with closing(self._connect()) as db:
                old = db.execute('SELECT state,record,checked_session FROM groups WHERE task_id=? AND group_id=?',
                                 (task_id, item.get('group_id'))).fetchone()
            fingerprint = _delivery_fingerprint(item, receipt_root)
            if old is not None and old['state'] in {'waiting', 'processing', 'ready_for_llm'}:
                previous = json.loads(old['record'])
                if (previous.get('paper_id') == item.get('paper_id')
                        and previous.get('receipt_id') == item.get('receipt_id')
                        and previous.get('receipt_path') == str(Path(item['receipt_path']).resolve())
                        and previous.get('delivery_dir') == str(Path(item['delivery_dir']).resolve())
                        and (self.receiver_session_id is None or old['checked_session'] == self.receiver_session_id)
                        and previous.get('_fingerprint') == fingerprint):
                    return {**previous, 'state': old['state']}
            observation = read_registered_item(item, receipt_root=receipt_root)
            record = _accepted_record(task_id, observation)
            if _delivery_fingerprint(item, receipt_root) != fingerprint:
                raise ValueError('交付材料在接收核验期间发生变化')
            record['_fingerprint'] = fingerprint
        except (OSError, ValueError, KeyError, TypeError) as exc:
            self._mark_unverified(task_id, item, exc)
            raise

        now = _utc_now()
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT 1 FROM cancelled_tasks WHERE task_id=?', (task_id,)).fetchone():
                raise ValueError('任务已删除')
            existing = db.execute('SELECT * FROM groups WHERE task_id=? AND group_id=?',
                                  (task_id, record['group_id'])).fetchone()
            if existing is not None:
                if existing['receipt_id'] != record['receipt_id'] or existing['paper_id'] != record['paper_id']:
                    db.execute("UPDATE groups SET state='needs_review',review_reason='identity_conflict', observed_at=? "
                               'WHERE task_id=? AND group_id=?', (now, task_id, record['group_id']))
                    db.execute('INSERT INTO observations(task_id,group_id,receipt_id,kind,reason,observed_at) '
                               'VALUES(?,?,?,?,?,?)',
                               (task_id, record['group_id'], record['receipt_id'], 'changed_receipt',
                                '同组回执或论文身份发生变化', now))
                    return {**json.loads(existing['record']), 'state': 'needs_review'}
                state = existing['state']
                if state == 'needs_review' and existing['review_reason'] in {
                        'recheck_failed', 'upstream_missing', 'claim_validation_failed',
                        'retention_unavailable_processing', 'processing_revalidation_failed'}:
                    completed = db.execute("SELECT h.handoff,c.consumer_id,c.attempt_id FROM preprocessing_handoffs h "
                                           "JOIN claims c ON c.consumer_id=h.consumer_id AND c.attempt_id=h.attempt_id "
                                           "WHERE h.task_id=? AND h.group_id=? AND h.paper_id=? AND h.receipt_id=? "
                                           "AND c.state='completed' AND h.state='waiting'",
                                           (task_id, record['group_id'], record['paper_id'], record['receipt_id'])).fetchone()
                    if completed:
                        try:
                            reference = json.loads(completed['handoff'])
                            path = Path(reference['manifest_path']).resolve(strict=True)
                            raw = path.read_bytes()
                            manifest = json.loads(raw)
                            expected = {'task_id': task_id, 'group_id': record['group_id'],
                                        'paper_id': record['paper_id'], 'receipt_id': record['receipt_id'],
                                        'consumer_id': completed['consumer_id'], 'attempt_id': completed['attempt_id'],
                                        'schema': 'paperflow-local-preprocessing-handoff/1'}
                            if (hashlib.sha256(raw).hexdigest() == reference['manifest_sha256']
                                    and all(manifest.get(key) == value for key, value in expected.items())):
                                state = 'ready_for_llm'
                        except (OSError, ValueError, KeyError, TypeError):
                            pass
                    processing = db.execute("SELECT 1 FROM claims WHERE task_id=? AND group_id=? "
                                            "AND receipt_id=? AND state='processing' LIMIT 1",
                                            (task_id, record['group_id'], record['receipt_id'])).fetchone()
                    if processing and self.require_retention:
                        clause = ' AND checked_session=?' if self.receiver_session_id else ''
                        protected = db.execute("SELECT 1 FROM retention_requests WHERE task_id=? AND group_id=? "
                                               "AND paper_id=? AND receipt_id=? AND consumer_id=? "
                                               "AND state='accepted' AND retention_id IS NOT NULL AND last_error IS NULL" + clause,
                                               (task_id, record['group_id'], record['paper_id'], record['receipt_id'],
                                                self.consumer_id) + ((self.receiver_session_id,) if self.receiver_session_id else ())).fetchone()
                    else:
                        protected = not self.require_retention
                    if processing and protected:
                        state = 'processing'
                    active_claim = db.execute("SELECT 1 FROM claims WHERE task_id=? AND group_id=? "
                                              "AND state NOT IN ('failed','checking') LIMIT 1",
                                              (task_id, record['group_id'])).fetchone()
                    conflict = db.execute("SELECT 1 FROM groups WHERE paper_id=? AND state!='cancelled' "
                                          'AND NOT EXISTS(SELECT 1 FROM cancelled_tasks t WHERE t.task_id=groups.task_id) '
                                          'AND NOT(task_id=? AND group_id=?) LIMIT 1',
                                          (record['paper_id'], task_id, record['group_id'])).fetchone()
                    if not active_claim and not conflict:
                        db.execute('INSERT INTO failed_claims(consumer_id,attempt_id,task_id,group_id,receipt_id,'
                                   'created_at,archived_at) SELECT consumer_id,attempt_id,task_id,group_id,receipt_id,'
                                   "created_at,? FROM claims WHERE task_id=? AND group_id=? AND state='failed'",
                                   (now, task_id, record['group_id']))
                        db.execute("DELETE FROM claims WHERE task_id=? AND group_id=? AND state='failed'",
                                   (task_id, record['group_id']))
                        state = 'waiting'
                db.execute('UPDATE groups SET observed_at=?,record=?,state=?,review_reason=?,checked_session=? '
                           'WHERE task_id=? AND group_id=?',
                           (now, json.dumps(record, ensure_ascii=False), state,
                            None if state in {'waiting', 'processing', 'ready_for_llm'} else existing['review_reason'], self.receiver_session_id,
                            task_id, record['group_id']))
                return {**record, 'state': state}

            others = db.execute("SELECT task_id, group_id FROM groups WHERE paper_id=? AND state!='cancelled' "
                                'AND NOT EXISTS(SELECT 1 FROM cancelled_tasks t WHERE t.task_id=groups.task_id)',
                                (record['paper_id'],)).fetchall()
            state = 'needs_review' if others else 'waiting'
            if others:
                db.execute("UPDATE groups SET state='needs_review',review_reason='duplicate_paper', observed_at=? "
                           "WHERE paper_id=? AND state!='cancelled' "
                           'AND NOT EXISTS(SELECT 1 FROM cancelled_tasks t WHERE t.task_id=groups.task_id)',
                           (now, record['paper_id']))
            db.execute('INSERT INTO groups(task_id,group_id,paper_id,receipt_id,state,record,observed_at,review_reason,checked_session) '
                       'VALUES(?,?,?,?,?,?,?,?,?)',
                       (task_id, record['group_id'], record['paper_id'], record['receipt_id'], state,
                        json.dumps(record, ensure_ascii=False), now, 'duplicate_paper' if others else None,
                        self.receiver_session_id))
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

    def claim_next(self, *, consumer_id, attempt_id, task_id=None, group_id=None,
                   receipt_id=None, admission=None):
        """Reserve a group, verify it outside the DB lock, then transfer it once."""
        if any(not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', value)
               for value in (consumer_id, attempt_id)):
            raise ValueError('领取方或尝试编号无效')
        if task_id is not None and (not isinstance(task_id, str) or not task_id):
            raise ValueError('任务编号无效')
        if (group_id is None) != (receipt_id is None):
            raise ValueError('资料文件夹和交付回执必须一起指定')
        if group_id is not None and (not task_id or
                any(not isinstance(value, str) or not value for value in (group_id, receipt_id))):
            raise ValueError('资料文件夹身份无效')
        with self._claim_transaction(admission) as db:
            session = _current_receiver_session(db) if self.require_live_watcher else None
            if db.execute('SELECT 1 FROM failed_claims WHERE consumer_id=? AND attempt_id=?',
                          (consumer_id, attempt_id)).fetchone():
                raise ValueError('该领取尝试已校验失败，需要核对并使用新编号')
            old = db.execute('SELECT * FROM claims WHERE consumer_id=? AND attempt_id=?',
                             (consumer_id, attempt_id)).fetchone()
            replay_processing = False
            if old is not None:
                if task_id is not None and old['task_id'] != task_id:
                    raise ValueError('领取记录不属于当前任务')
                if group_id is not None and (old['group_id'] != group_id or old['receipt_id'] != receipt_id):
                    raise ValueError('领取记录不属于当前资料文件夹或交付回执')
                group = db.execute('SELECT * FROM groups WHERE task_id=? AND group_id=? '
                                   'AND receipt_id=?',
                                   (old['task_id'], old['group_id'], old['receipt_id'])).fetchone()
                if group is None or old['state'] not in {'checking', 'processing'}:
                    raise ValueError('已领取记录不完整或状态改变，需要核对')
                if (group['state'] == 'needs_review' and
                        ((old['state'] == 'checking' and group['review_reason'] in
                          {'retention_unavailable', 'recheck_failed'}) or
                         (old['state'] == 'processing' and group['review_reason'] in
                          {'retention_unavailable_processing', 'recheck_failed'}))):
                    raise ReceiverUnavailable('接收服务正在重新核对')
                if old['state'] == 'processing':
                    if group['state'] != 'processing':
                        raise ValueError('已领取资料状态改变，需要核对')
                    replay_processing = True
                elif group['state'] != 'waiting':
                    raise ValueError('领取核验期间资料状态改变，需要核对')
            else:
                protected_clause = (" AND EXISTS(SELECT 1 FROM retention_requests r WHERE r.task_id=g.task_id "
                                    "AND r.group_id=g.group_id AND r.paper_id=g.paper_id AND r.receipt_id=g.receipt_id "
                                    "AND r.consumer_id=? AND r.state='accepted' "
                                    'AND r.retention_id IS NOT NULL AND r.last_error IS NULL' +
                                    (' AND r.checked_session=?' if session else '') + ')') if self.require_retention else ''
                params = (self.consumer_id,) if self.require_retention else ()
                if self.require_retention and session:
                    params += (session,)
                if session:
                    protected_clause += ' AND g.checked_session=?'
                    params += (session,)
                task_clause = ' AND g.task_id=?' if task_id is not None else ''
                if task_id is not None:
                    params += (task_id,)
                group_clause = ' AND g.group_id=? AND g.receipt_id=?' if group_id is not None else ''
                if group_id is not None:
                    params += (group_id, receipt_id)
                group = db.execute("SELECT g.* FROM groups g WHERE g.state='waiting'" + protected_clause + task_clause + group_clause +
                                   ' AND NOT EXISTS(SELECT 1 FROM claims c WHERE c.task_id=g.task_id '
                                   'AND c.group_id=g.group_id AND c.receipt_id=g.receipt_id) '
                                   'ORDER BY g.rowid LIMIT 1', params).fetchone()
                if group is None:
                    if group_id is not None and session:
                        current = db.execute('SELECT state,checked_session FROM groups WHERE task_id=? '
                                             'AND group_id=? AND receipt_id=?',
                                             (task_id, group_id, receipt_id)).fetchone()
                        if current and current['state'] == 'waiting' and current['checked_session'] != session:
                            raise ReceiverUnavailable('资料文件夹尚未由当前接收进程复核')
                    return None
                db.execute('INSERT INTO claims(consumer_id,attempt_id,task_id,group_id,receipt_id,created_at,state) '
                           "VALUES(?,?,?,?,?,?,'checking')",
                           (consumer_id, attempt_id, group['task_id'], group['group_id'],
                            group['receipt_id'], _utc_now()))
            if session and group['checked_session'] != session:
                raise ReceiverUnavailable('领取资料尚未由当前接收进程复核')
            previous = json.loads(group['record'])
            key = (group['task_id'], group['group_id'], group['receipt_id'])

        # The potentially expensive SHA-256 verification never holds SQLite's write lock.
        try:
            receipt_root = Path(previous['receipt_path']).parent.parent
            item = {**{key: previous[key] for key in ('group_id', 'paper_id', 'receipt_id',
                                                       'receipt_path', 'delivery_dir')},
                    'status': 'passed', 'registration_status': 'registered', 'delivery_complete': True}
            before = _delivery_fingerprint(item, receipt_root)
            observation = read_registered_item(item, receipt_root=receipt_root)
            record = _accepted_record(key[0], observation)
            if (record['receipt_sha256'] != previous['receipt_sha256']
                    or _delivery_fingerprint(item, receipt_root) != before):
                raise ValueError('领取核验期间交付材料发生变化')
            record['_fingerprint'] = before
        except (OSError, ValueError, KeyError, TypeError, RuntimeError):
            with closing(self._connect()) as db, db:
                db.execute('BEGIN IMMEDIATE')
                if self.require_live_watcher:
                    current_session = _current_receiver_session(db)
                    current_group = db.execute('SELECT state,review_reason,checked_session FROM groups '
                                               'WHERE task_id=? AND group_id=? AND receipt_id=?', key).fetchone()
                    if (current_group and (current_group['checked_session'] != current_session or
                            (current_group['state'] == 'needs_review' and current_group['review_reason'] in
                             {'retention_unavailable', 'retention_unavailable_processing', 'recheck_failed'}))):
                        raise ReceiverUnavailable('资料等待复核')
                if replay_processing:
                    db.execute("UPDATE groups SET state='needs_review',review_reason='processing_revalidation_failed',"
                               "observed_at=? WHERE task_id=? AND group_id=? AND receipt_id=? AND state='processing'",
                               (_utc_now(), *key))
                else:
                    changed = db.execute("UPDATE claims SET state='failed' WHERE consumer_id=? AND attempt_id=? "
                                         "AND state='checking'", (consumer_id, attempt_id)).rowcount
                    if changed:
                        db.execute("UPDATE groups SET state='needs_review',review_reason='claim_validation_failed',"
                                   "observed_at=? WHERE task_id=? AND group_id=? AND receipt_id=? AND state='waiting'",
                                   (_utc_now(), *key))
            if replay_processing:
                raise ValueError('已领取资料重新核验失败，需要核对')
            raise ValueError('领取前资料核验失败，需要核对')

        with self._claim_transaction(admission) as db:
            session = _current_receiver_session(db) if self.require_live_watcher else None
            claim = db.execute('SELECT state FROM claims WHERE consumer_id=? AND attempt_id=?',
                               (consumer_id, attempt_id)).fetchone()
            group = db.execute('SELECT state,record,checked_session,review_reason FROM groups WHERE task_id=? AND group_id=? AND receipt_id=?',
                               key).fetchone()
            if session and (not group or group['checked_session'] != session):
                raise ReceiverUnavailable('领取核验期间接收进程已更换')
            if (claim and group and group['state'] == 'needs_review' and
                    ((claim['state'] == 'checking' and group['review_reason'] in
                      {'retention_unavailable', 'recheck_failed'}) or
                     (claim['state'] == 'processing' and group['review_reason'] in
                      {'retention_unavailable_processing', 'recheck_failed'}))):
                raise ReceiverUnavailable('领取核验期间资料等待复核')
            replay_ready = bool(claim and claim['state'] == 'processing' and group and group['state'] == 'processing')
            if not replay_ready and (not claim or claim['state'] != 'checking' or not group or group['state'] != 'waiting'):
                raise ValueError('领取核验期间资料状态改变，需要核对')
            if self.require_retention:
                retained = db.execute('SELECT 1 FROM retention_requests WHERE task_id=? AND group_id=? '
                                      'AND paper_id=? AND receipt_id=? AND consumer_id=? '
                                      "AND state='accepted' AND retention_id IS NOT NULL AND last_error IS NULL" +
                                      (' AND checked_session=?' if session else ''),
                                      (key[0], key[1], record['paper_id'], key[2], self.consumer_id) +
                                      ((session,) if session else ())).fetchone()
                if not retained:
                    if session:
                        previous_retention = db.execute('SELECT state,retention_id,last_error,checked_session '
                            'FROM retention_requests WHERE task_id=? AND group_id=? AND paper_id=? '
                            'AND receipt_id=? AND consumer_id=?',
                            (key[0], key[1], record['paper_id'], key[2], self.consumer_id)).fetchone()
                        if (previous_retention and previous_retention['state'] == 'accepted'
                                and previous_retention['retention_id'] is not None
                                and previous_retention['last_error'] is None
                                and previous_retention['checked_session'] != session):
                            raise ReceiverUnavailable('接收占用正在由当前进程重新核对')
                    raise ValueError('领取核验期间上游占用状态不明')
            if replay_ready:
                return {**json.loads(group['record']), 'state': 'processing',
                        'consumer_id': consumer_id, 'attempt_id': attempt_id}
            now = _utc_now()
            db.execute("UPDATE groups SET state='processing',record=?,observed_at=? "
                       'WHERE task_id=? AND group_id=? AND receipt_id=?',
                       (json.dumps(record, ensure_ascii=False), now, *key))
            db.execute("UPDATE claims SET state='processing' WHERE consumer_id=? AND attempt_id=?",
                       (consumer_id, attempt_id))
            db.execute('INSERT INTO observations(task_id,group_id,receipt_id,kind,reason,observed_at) '
                       'VALUES(?,?,?,?,?,?)',
                       (*key, 'claimed', json.dumps({'consumer_id': consumer_id, 'attempt_id': attempt_id}), now))
            return {**record, 'state': 'processing', 'consumer_id': consumer_id, 'attempt_id': attempt_id}

    def incomplete_preprocessing(self, consumer_id, task_id=None):
        """Return interrupted claims; explicit failures await an explicit resume."""
        with closing(self._connect()) as db:
            rows = db.execute("SELECT c.attempt_id FROM claims c LEFT JOIN preprocessing_failures f "
                              "ON f.consumer_id=c.consumer_id AND f.attempt_id=c.attempt_id "
                              "WHERE c.consumer_id=? AND c.state IN ('checking','processing') "
                              "AND f.attempt_id IS NULL" + (' AND c.task_id=?' if task_id is not None else '') +
                              ' ORDER BY c.created_at,c.attempt_id',
                              (consumer_id, task_id) if task_id is not None else (consumer_id,)).fetchall()
        return [row['attempt_id'] for row in rows]

    def clear_preprocessing_failure(self, consumer_id, attempt_id):
        with closing(self._connect()) as db, db:
            db.execute('DELETE FROM preprocessing_failures WHERE consumer_id=? AND attempt_id=?',
                       (consumer_id, attempt_id))

    def record_preprocessing_failure(self, consumer_id, attempt_id, error):
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            claim = db.execute('SELECT task_id,group_id,receipt_id,state FROM claims '
                               'WHERE consumer_id=? AND attempt_id=?', (consumer_id, attempt_id)).fetchone()
            if not claim or claim['state'] not in {'checking', 'processing'}:
                return
            now = _utc_now()
            db.execute('INSERT OR REPLACE INTO preprocessing_failures VALUES(?,?,?,?)',
                       (consumer_id, attempt_id, type(error).__name__, now))
            db.execute('INSERT INTO observations(task_id,group_id,receipt_id,kind,reason,observed_at) '
                       'VALUES(?,?,?,?,?,?)',
                       (claim['task_id'], claim['group_id'], claim['receipt_id'],
                        'preprocessing_failed', type(error).__name__, now))

    def completed_preprocessing(self, consumer_id, attempt_id):
        with closing(self._connect()) as db:
            row = db.execute("SELECT c.state AS claim_state,h.handoff,h.task_id,h.group_id,h.paper_id,h.receipt_id "
                             "FROM claims c LEFT JOIN preprocessing_handoffs h "
                             "ON h.consumer_id=c.consumer_id AND h.attempt_id=c.attempt_id "
                             "WHERE c.consumer_id=? AND c.attempt_id=?", (consumer_id, attempt_id)).fetchone()
        if not row or row['claim_state'] != 'completed':
            return None
        if not row['handoff']:
            raise ValueError('已完成领取缺少前处理交接记录')
        handoff = json.loads(row['handoff'])
        path = Path(handoff['manifest_path']).resolve(strict=True)
        if hashlib.sha256(path.read_bytes()).hexdigest() != handoff['manifest_sha256']:
            raise ValueError('已完成前处理交接文件发生变化')
        manifest = json.loads(path.read_text(encoding='utf-8'))
        expected = {key: row[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id')}
        expected.update(consumer_id=consumer_id, attempt_id=attempt_id,
                        schema='paperflow-local-preprocessing-handoff/1')
        if any(manifest.get(key) != value for key, value in expected.items()):
            raise ValueError('已完成前处理交接身份不一致')
        return {'status': 'already_handed_off', 'task_id': row['task_id'], 'group_id': row['group_id'],
                'paper_id': row['paper_id'],
                'receipt_id': row['receipt_id'], 'attempt_id': attempt_id, **handoff}

    def complete_preprocessing(self, consumer_id, attempt_id, handoff):
        """Atomically move a completed claim into the durable LLM waiting pool."""
        if not isinstance(handoff, dict):
            raise ValueError('前处理交接记录无效')
        path = Path(handoff.get('manifest_path', '')).resolve(strict=True)
        digest = handoff.get('manifest_sha256')
        if not re.fullmatch(r'[0-9a-f]{64}', str(digest or '')) or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise ValueError('前处理交接文件不完整')
        manifest = json.loads(path.read_text(encoding='utf-8'))
        if manifest.get('schema') != 'paperflow-local-preprocessing-handoff/1':
            raise ValueError('前处理交接格式无效')
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            claim = db.execute('SELECT * FROM claims WHERE consumer_id=? AND attempt_id=?',
                               (consumer_id, attempt_id)).fetchone()
            if not claim:
                raise ValueError('前处理领取记录不存在')
            group = db.execute('SELECT * FROM groups WHERE task_id=? AND group_id=? AND receipt_id=?',
                               (claim['task_id'], claim['group_id'], claim['receipt_id'])).fetchone()
            if not group:
                raise ValueError('前处理领取资料不存在')
            expected = {'task_id': group['task_id'], 'group_id': group['group_id'],
                        'paper_id': group['paper_id'], 'receipt_id': group['receipt_id'],
                        'consumer_id': consumer_id, 'attempt_id': attempt_id}
            if any(manifest.get(key) != value for key, value in expected.items()):
                raise ValueError('前处理交接与领取身份不一致')
            saved = db.execute('SELECT handoff FROM preprocessing_handoffs WHERE receipt_id=?',
                               (group['receipt_id'],)).fetchone()
            encoded = json.dumps(handoff, ensure_ascii=False, sort_keys=True)
            if saved:
                if saved['handoff'] != encoded or claim['state'] != 'completed' or group['state'] != 'ready_for_llm':
                    raise ValueError('已有前处理交接身份不一致')
                return handoff
            if (claim['state'] == 'processing' and group['state'] == 'needs_review' and
                    group['review_reason'] in {'retention_unavailable_processing', 'recheck_failed'}):
                raise ReceiverUnavailable('交接期间资料等待复核')
            if claim['state'] != 'processing' or group['state'] != 'processing':
                raise ValueError('前处理领取状态已改变')
            now = _utc_now()
            db.execute('INSERT INTO preprocessing_handoffs VALUES(?,?,?,?,?,?,?,?,?)',
                       (group['receipt_id'], group['task_id'], group['group_id'], group['paper_id'],
                        consumer_id, attempt_id, 'waiting', encoded, now))
            db.execute("UPDATE claims SET state='completed' WHERE consumer_id=? AND attempt_id=?",
                       (consumer_id, attempt_id))
            db.execute("UPDATE groups SET state='ready_for_llm',observed_at=? "
                       'WHERE task_id=? AND group_id=? AND receipt_id=?',
                       (now, group['task_id'], group['group_id'], group['receipt_id']))
            db.execute('DELETE FROM preprocessing_failures WHERE consumer_id=? AND attempt_id=?',
                       (consumer_id, attempt_id))
            db.execute('INSERT INTO observations(task_id,group_id,receipt_id,kind,reason,observed_at) '
                       'VALUES(?,?,?,?,?,?)', (group['task_id'], group['group_id'], group['receipt_id'],
                       'ready_for_llm', None, now))
        return handoff

    def list_waiting_llm(self):
        with closing(self._connect()) as db:
            db.execute('BEGIN')
            total = db.execute("SELECT COUNT(*) FROM preprocessing_handoffs WHERE state='waiting'").fetchone()[0]
            if not total:
                return {'waiting_count': 0, 'unavailable_count': 0, 'items': []}
            session = _current_receiver_session(db) if self.require_live_watcher else None
            clause = ''
            params = []
            if session:
                clause += ' AND g.checked_session=?'
                params.append(session)
            if self.require_retention:
                clause += (" AND EXISTS(SELECT 1 FROM retention_requests r WHERE r.task_id=g.task_id "
                           "AND r.group_id=g.group_id AND r.paper_id=g.paper_id AND r.receipt_id=g.receipt_id "
                           "AND r.consumer_id=? AND r.state='accepted' AND r.retention_id IS NOT NULL "
                           "AND r.last_error IS NULL" + (' AND r.checked_session=?' if session else '') + ')')
                params.append(self.consumer_id)
                if session:
                    params.append(session)
            rows = db.execute("SELECT h.task_id,h.group_id,h.paper_id,h.receipt_id,h.handoff,h.created_at "
                              "FROM preprocessing_handoffs h JOIN groups g ON g.task_id=h.task_id "
                              "AND g.group_id=h.group_id AND g.paper_id=h.paper_id AND g.receipt_id=h.receipt_id "
                              "WHERE h.state='waiting' AND g.state='ready_for_llm'" + clause + ' ORDER BY h.rowid',
                              tuple(params)).fetchall()
        items = []
        for row in rows:
            try:
                reference = json.loads(row['handoff'])
                path = Path(reference['manifest_path']).resolve(strict=True)
                raw = path.read_bytes()
                manifest = json.loads(raw)
                if (hashlib.sha256(raw).hexdigest() != reference['manifest_sha256'] or
                        any(manifest.get(key) != row[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id'))):
                    continue
                items.append({**{key: row[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'created_at')},
                              **reference})
            except (OSError, ValueError, KeyError, TypeError):
                continue
        return {'waiting_count': len(items), 'unavailable_count': total - len(items), 'items': items}

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
                db.execute("UPDATE groups SET state='needs_review',review_reason='upstream_missing',observed_at=? "
                           'WHERE task_id=? AND group_id=?', (now, row['task_id'], row['group_id']))
                db.execute('INSERT INTO observations(task_id,group_id,receipt_id,kind,reason,observed_at) '
                           'VALUES(?,?,?,?,?,?)',
                           (row['task_id'], row['group_id'], row['receipt_id'], 'upstream_missing',
                            '完整扫描未找到原登记资料', now))
        return missing

    def record_scan(self, *, complete, reason=None, source_instance=None):
        """Persist discovery status; failures never overwrite last success time."""
        if type(complete) is not bool:
            raise ValueError('扫描状态无效')
        if source_instance is not None:
            try:
                if str(uuid.UUID(source_instance)) != source_instance:
                    raise ValueError('上游实例身份无效')
            except (ValueError, TypeError, AttributeError):
                raise ValueError('上游实例身份无效') from None
        now = _utc_now()
        values = {'scan_status': 'complete' if complete else 'incomplete',
                  'scan_observed_at': now, 'scan_reason': reason or ''}
        if complete:
            values['scan_completed_at'] = now
            values['scan_completed_session'] = self.receiver_session_id or ''
        if source_instance is not None:
            values['scan_source_instance'] = source_instance
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            if self.receiver_session_id:
                current = dict(db.execute("SELECT key,value FROM metadata WHERE key IN "
                                          "('receiver_session_id','receiver_status')").fetchall())
                if (current.get('receiver_session_id') != self.receiver_session_id
                        or current.get('receiver_status') != 'running'):
                    return
            db.executemany('INSERT OR REPLACE INTO metadata(key,value) VALUES(?,?)', values.items())

    def record_receiver(self, state, *, expected_session=None):
        if state not in {'running', 'stopped', 'interrupted'}:
            raise ValueError('接收进程状态无效')
        session_id = expected_session or self.receiver_session_id
        if not session_id:
            raise ValueError('接收进程会话未知')
        with closing(sqlite3.connect(self.path, timeout=1)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            meta = dict(db.execute("SELECT key,value FROM metadata WHERE key IN "
                                   "('receiver_session_id','receiver_status')").fetchall())
            if (meta.get('receiver_session_id') != session_id
                    or (state == 'running' and meta.get('receiver_status') != 'running')):
                return
            db.executemany('INSERT OR REPLACE INTO metadata(key,value) VALUES(?,?)',
                           [('receiver_status', state), ('receiver_heartbeat_at', _utc_now())])

    def start_receiver(self):
        """A new watcher must check the current upstream before exposing old counts."""
        session_id = str(uuid.uuid4())
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            db.executemany('INSERT OR REPLACE INTO metadata(key,value) VALUES(?,?)',
                           [('receiver_status', 'running'), ('receiver_heartbeat_at', _utc_now()),
                            ('receiver_session_id', session_id), ('scan_status', 'incomplete'),
                            ('scan_source_instance', ''), ('scan_completed_session', '')])
        self.receiver_session_id = session_id

    def snapshot(self, *, max_age_seconds=60, include_in_progress=True):
        """Count locally received waiting groups; report discovery separately."""
        if not isinstance(max_age_seconds, (int, float)) or max_age_seconds <= 0:
            raise ValueError('扫描有效期无效')
        now = datetime.now(timezone.utc)
        with closing(self._connect()) as db:
            db.execute('BEGIN')
            meta = dict(db.execute('SELECT key,value FROM metadata').fetchall())
            session_clause = ' AND r.checked_session=?' if self.require_live_watcher else ''
            protected_clause = (" AND EXISTS(SELECT 1 FROM retention_requests r WHERE r.task_id=g.task_id "
                                "AND r.group_id=g.group_id AND r.paper_id=g.paper_id AND r.receipt_id=g.receipt_id "
                                "AND r.consumer_id=? AND r.state='accepted' "
                                'AND r.retention_id IS NOT NULL AND r.last_error IS NULL' + session_clause + ')') if self.require_retention else ''
            params = ((self.consumer_id, meta.get('receiver_session_id')) if self.require_live_watcher
                      else (self.consumer_id,)) if self.require_retention else ()
            if self.require_live_watcher:
                protected_clause += ' AND g.checked_session=?'
                params += (meta.get('receiver_session_id'),)
            active_task = ' AND NOT EXISTS(SELECT 1 FROM cancelled_tasks t WHERE t.task_id=g.task_id)'
            waiting, oldest = db.execute("SELECT COUNT(*),MIN(g.observed_at) FROM groups g "
                                         "WHERE g.state='waiting'" + active_task + protected_clause, params).fetchone()
            total_waiting = db.execute("SELECT COUNT(*) FROM groups g WHERE g.state='waiting'" + active_task).fetchone()[0]
            review = db.execute("SELECT COUNT(*) FROM groups g WHERE g.state='needs_review'" + active_task).fetchone()[0]
            incomplete_receipts = db.execute(
                "SELECT COUNT(*) FROM retention_requests r WHERE r.state IN ('intent','reserved','invalid') "
                "AND NOT EXISTS(SELECT 1 FROM cancelled_tasks t WHERE t.task_id=r.task_id)").fetchone()[0]
            inconsistent_processing = db.execute(
                "SELECT COUNT(*) FROM groups g WHERE g.state='processing'" + active_task +
                " AND NOT EXISTS(SELECT 1 FROM claims c WHERE c.task_id=g.task_id AND c.group_id=g.group_id "
                "AND c.receipt_id=g.receipt_id AND c.consumer_id='prefect-local-preprocessing' "
                "AND c.state='processing')").fetchone()[0]
            inconsistent_claims = db.execute(
                "SELECT COUNT(*) FROM claims c WHERE c.consumer_id='prefect-local-preprocessing' "
                "AND c.state IN ('checking','processing') "
                "AND NOT EXISTS(SELECT 1 FROM cancelled_tasks t WHERE t.task_id=c.task_id) "
                "AND NOT EXISTS(SELECT 1 FROM groups g WHERE g.task_id=c.task_id AND g.group_id=c.group_id "
                "AND g.receipt_id=c.receipt_id AND ((c.state='checking' AND g.state='waiting') "
                "OR (c.state='processing' AND g.state='processing')))").fetchone()[0]
            inconsistent_ready = db.execute(
                "SELECT COUNT(*) FROM groups g WHERE g.state='ready_for_llm'" + active_task +
                " AND NOT EXISTS(SELECT 1 FROM preprocessing_handoffs h JOIN claims c "
                "ON c.consumer_id=h.consumer_id AND c.attempt_id=h.attempt_id "
                "WHERE h.task_id=g.task_id AND h.group_id=g.group_id AND h.paper_id=g.paper_id "
                "AND h.receipt_id=g.receipt_id AND h.state='waiting' AND c.task_id=g.task_id "
                "AND c.group_id=g.group_id AND c.receipt_id=g.receipt_id AND c.state='completed')").fetchone()[0]
            waiting_llm_total = db.execute(
                "SELECT COUNT(*) FROM preprocessing_handoffs h WHERE h.state='waiting' "
                "AND NOT EXISTS(SELECT 1 FROM cancelled_tasks t WHERE t.task_id=h.task_id)").fetchone()[0]
            accepted_before = bool(db.execute('SELECT 1 FROM groups g WHERE 1=1' + active_task + protected_clause + ' LIMIT 1',
                                              params).fetchone())
            attempts = db.execute(
                "SELECT c.attempt_id,EXISTS(SELECT 1 FROM cancelled_tasks t WHERE t.task_id=c.task_id) "
                "FROM claims c WHERE c.consumer_id='prefect-local-preprocessing' "
                "AND c.state='processing'").fetchall() if include_in_progress else []
        completed = meta.get('scan_completed_at')
        receiver_state = meta.get('receiver_status', 'not_started')
        heartbeat = meta.get('receiver_heartbeat_at')
        if receiver_state == 'running':
            try:
                heartbeat_age = (now - datetime.fromisoformat(heartbeat)).total_seconds()
                receiver_state = 'running' if 0 <= heartbeat_age <= max_age_seconds else 'interrupted'
            except (ValueError, TypeError):
                receiver_state = 'unknown'
        # A partial discovery cannot erase verified records already received.
        # A truly empty local ledger is known only after a confirmed reception or complete scan.
        try:
            scan_age = (now - datetime.fromisoformat(completed)).total_seconds()
            fresh_scan = 0 <= scan_age <= max_age_seconds
        except (ValueError, TypeError):
            fresh_scan = False
        verified_scan = (meta.get('scan_status') == 'complete' and fresh_scan
                          and bool(meta.get('scan_source_instance'))
                          and (not self.require_live_watcher or
                               meta.get('scan_completed_session') == meta.get('receiver_session_id')))
        clean = not (review or incomplete_receipts or inconsistent_processing or inconsistent_claims or inconsistent_ready)
        all_waiting_verified = waiting > 0 and waiting == total_waiting and clean
        empty_known = verified_scan if self.require_retention and self.require_live_watcher else (
            accepted_before or verified_scan or (not self.require_retention and meta.get('scan_status') == 'complete'))
        if self.require_retention and self.require_live_watcher:
            available = bool(verified_scan and clean and waiting == total_waiting)
        else:
            available = bool(all_waiting_verified or (total_waiting == 0 and clean and empty_known))
        if self.require_live_watcher:
            available = available and receiver_state == 'running' and bool(meta.get('receiver_session_id'))
        in_progress = None
        try:
            if not include_in_progress:
                raise ValueError('未请求前处理数量')
            config = json.loads((self.path.parent.parent / 'prefect-paperflow' / 'config.json').read_text(encoding='utf8'))
            lock_root = Path(config['data_root']).resolve() / 'locks'
            if any(row[1] for row in attempts):
                raise ValueError('任务正在删除，前处理数量暂不可确认')
            if attempts and not lock_root.is_dir():
                raise OSError('前处理锁目录不可用')
            in_progress = 0
            for attempt, _ in attempts:
                if not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', attempt):
                    raise ValueError('前处理记录编号无效')
                path = lock_root / ('local-preprocess-' + attempt + '.lock')
                if not path.is_file():
                    continue
                lock = FileLock(path, timeout=0, thread_local=False)
                try:
                    lock.acquire()
                except Timeout:
                    in_progress += 1
                else:
                    lock.release()
        except (OSError, ValueError, KeyError, TypeError):
            in_progress = None
        return {
            'scope': 'all_tasks',
            'waiting_count': waiting if available else None,
            'waiting_available': available,
            'in_progress_count': in_progress,
            'as_of': now.isoformat(),
            'upstream_scan_completed_at': completed,
            'upstream_scan_status': meta.get('scan_status', 'not_started'),
            'receiver_status': receiver_state,
            'receiver_heartbeat_at': heartbeat,
            'receiver_session_id': meta.get('receiver_session_id'),
            'scan_source_instance': meta.get('scan_source_instance'),
            'upstream_scan_verified': bool(verified_scan and clean and waiting == total_waiting),
            'waiting_llm_total': waiting_llm_total,
            'oldest_verified_at': oldest,
            'needs_review_count': review,
            'preprocessing_handoff_configured': meta.get('preprocessing_handoff_configured') == 'true',
            'preprocessing_handoff_connected': False,
        }
