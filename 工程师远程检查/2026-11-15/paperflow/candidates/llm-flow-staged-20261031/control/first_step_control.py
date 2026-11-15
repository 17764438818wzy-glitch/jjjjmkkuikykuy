"""Durable upload and task control for the independent local first step."""
from contextlib import closing
import base64
import json
import os
from pathlib import Path, PurePosixPath
import re
import sqlite3
import threading
import time
import uuid
from urllib.request import Request, urlopen

from filelock import FileLock, Timeout as FileLockTimeout
import psutil

from .first_step_scheduler import SchedulerNotSubmitted
from .first_step_limits import GROUP_CONCURRENCY
from .group_summary_store import GroupSummaryStore, timestamp
from .records import identity

# A failed final write can leave a start pending even though its request ended.
# Keep its precise identity until a later status read or deletion can reconcile it.
_abandoned_starts = set()

class ControlError(Exception):
    def __init__(self, message, status=409):
        super().__init__(message)
        self.status = status


class PauseRequested(Exception):
    pass


class UnknownRequest(Exception):
    pass


class RequestNotSent(PauseRequested, ConnectionError):
    pass


def _uuid(value):
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, TypeError, AttributeError):
        raise ControlError('任务或上传编号无效', 404) from None


def _relative(raw, root_name=None):
    if not isinstance(raw, str) or not raw or '\\' in raw or raw.startswith('/'):
        raise ControlError('上传相对路径无效', 400)
    parts = raw.split('/')
    if any(not part or part in {'.', '..'} or part.endswith((' ', '.'))
           or re.search(r'[<>:"|?*\x00-\x1f]', part)
           or re.fullmatch(r'(?i)(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\..*)?', part)
           for part in parts):
        raise ControlError('上传相对路径无效', 400)
    if root_name is not None and (len(parts) < 2 or parts[0] != root_name):
        raise ControlError('文件路径必须包含选择的根文件夹', 400)
    return PurePosixPath(*parts)


def _same_request_scope(first, second):
    if not first or not second:
        return False
    first, second = str(first).replace('\\', '/').casefold().rstrip('/'), str(second).replace('\\', '/').casefold().rstrip('/')
    if first == second:
        return True
    first_local, second_local = first.startswith('local:'), second.startswith('local:')
    if first_local == second_local:
        return False
    local_path, relative = (first[6:], second) if first_local else (second[6:], first)
    relative = relative.strip('/')
    return bool(relative) and (local_path == relative or local_path.endswith('/' + relative))


def discover_groups(relative_paths, root_name):
    """Discover non-overlapping candidate folders and retain ambiguous files."""
    categories = {'正文', '附件', '垃圾', '垃圾材料'}
    groups, ungrouped = {}, []
    for raw in relative_paths:
        p = _relative(raw, root_name)
        parts = p.parts
        if len(parts) < 3:
            ungrouped.append(raw)
            continue
        parent = PurePosixPath(*parts[:-1])
        if parent.name in categories:
            parent = parent.parent
        if len(parent.parts) < 2:
            ungrouped.append(raw)
            continue
        groups.setdefault(parent.as_posix(), []).append(raw)
    ambiguous = set()
    for group in groups:
        parent = PurePosixPath(group).parent
        while len(parent.parts) >= 2:
            if parent.as_posix() in groups:
                ambiguous.update((group, parent.as_posix()))
            parent = parent.parent
    for group in ambiguous:
        ungrouped.extend(groups.pop(group))
    if not groups and ungrouped:
        paths = [_relative(raw, root_name) for raw in ungrouped]
        if all(len(path.parts) == 2 or (len(path.parts) == 3 and path.parts[1] in categories)
               for path in paths):
            groups[root_name] = list(ungrouped)
            ungrouped.clear()
    return sorted(groups, key=str.casefold), sorted(ungrouped, key=str.casefold)


def _task_name(value):
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > 120:
        raise ControlError('任务名称须为 1 至 120 个字符', 400)
    return value.strip()


def _process_dead(pid, started):
    if pid is None or started is None:
        return False
    try:
        process = psutil.Process(pid)
        return abs(process.create_time() - started) > .01 or process.status() == psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return True
    except psutil.Error:
        return False


def _start_is_abandoned(task):
    if not task['start_pending']:
        return False
    owner = (task['task_id'], task['start_generation'], task['start_owner_pid'], task['start_owner_started'])
    return (_process_dead(task['start_owner_pid'], task['start_owner_started']) or
            (task['start_owner_pid'] == os.getpid() and owner in _abandoned_starts))


class FirstStepControl:
    def __init__(self, output_dir, data_dir, scheduler=None, tus_base_url='http://127.0.0.1:11080/files/',
                 tus_backend_url=None, receipt_root=None, local_intake_queue_db=None, nimble_lock_path=None):
        self.output = Path(output_dir).resolve()
        self.data = Path(data_dir).resolve()
        self.nimble_lock_path = (Path(nimble_lock_path).resolve() if nimble_lock_path
                                 else self.data / 'control-locks' / 'nimble-requests.lock')
        self.receipt_root = Path(receipt_root).resolve() if receipt_root is not None else self.data.parent / 'receipts'
        self.local_intake_queue_db = (Path(local_intake_queue_db).resolve()
                                      if local_intake_queue_db is not None else None)
        self.scheduler = scheduler
        self.tus_dir = self.data / 'tusd'
        self.legacy_conversion_root = (Path(__file__).resolve().parent.parent / 'runtime' / 'group-conversion' /
                                       'private' / 'material-conversion' / 'cache')
        self.tus_base_url = tus_base_url.rstrip('/') + '/'
        self.tus_backend_url = (tus_backend_url or tus_base_url).rstrip('/') + '/'
        self.tus_gateway_enabled = tus_backend_url is not None
        self._upload_transfer_lock = threading.Lock()
        self._upload_transfers = {}
        self.summary = GroupSummaryStore(self.output)
        self.summary._initialize()
        with closing(self.summary._connect()) as db, db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS uploads (
                    upload_id TEXT PRIMARY KEY, root_name TEXT NOT NULL, state TEXT NOT NULL,
                    created_at_ns INTEGER NOT NULL, task_id TEXT
                );
                CREATE TABLE IF NOT EXISTS control_groups (
                    task_id TEXT NOT NULL, seq INTEGER NOT NULL, group_root TEXT NOT NULL,
                    state TEXT NOT NULL, snapshot_id TEXT, worker_pid INTEGER,
                    PRIMARY KEY(task_id, seq)
                );
                CREATE TABLE IF NOT EXISTS model_decisions (
                    task_id TEXT NOT NULL, seq INTEGER NOT NULL, step TEXT NOT NULL,
                    snapshot_id TEXT NOT NULL, payload_hash TEXT NOT NULL,
                    state TEXT NOT NULL, response TEXT, detail TEXT,
                    PRIMARY KEY(task_id, seq, step)
                );
                CREATE TABLE IF NOT EXISTS tus_manifest (
                    upload_id TEXT NOT NULL, path_key TEXT NOT NULL, relative_path TEXT NOT NULL,
                    bytes INTEGER NOT NULL, tus_id TEXT, create_request_id TEXT,
                    state TEXT NOT NULL DEFAULT 'pending', PRIMARY KEY(upload_id, path_key),
                    UNIQUE(tus_id)
                );
                CREATE TABLE IF NOT EXISTS task_create_requests (
                    request_id TEXT PRIMARY KEY, task_id TEXT NOT NULL UNIQUE, name TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS task_deletions (
                    task_id TEXT PRIMARY KEY, requested_at_ns INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS retired_upload_creates (
                    invocation_id TEXT PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS deleted_unknown_requests (
                    task_id TEXT NOT NULL, seq INTEGER NOT NULL, step TEXT NOT NULL,
                    source_digest TEXT, group_scope TEXT, request_scope TEXT,
                    request_kind TEXT, request_members TEXT,
                    PRIMARY KEY(task_id, seq, step)
                );
                CREATE TABLE IF NOT EXISTS local_retention_references (
                    retention_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE,
                    task_id TEXT NOT NULL, group_id TEXT NOT NULL, paper_id TEXT NOT NULL,
                    receipt_id TEXT NOT NULL, consumer_id TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('reserved','accepted','released')),
                    created_at_ns INTEGER NOT NULL, updated_at_ns INTEGER NOT NULL,
                    accepted_at_ns INTEGER, released_at_ns INTEGER, release_reason TEXT
                );
                CREATE INDEX IF NOT EXISTS local_retention_by_task_receipt
                    ON local_retention_references(task_id, receipt_id, state);
                CREATE INDEX IF NOT EXISTS local_retention_by_task_order
                    ON local_retention_references(task_id, created_at_ns, retention_id);
                CREATE TABLE IF NOT EXISTS local_retention_revisions (
                    task_id TEXT PRIMARY KEY, revision INTEGER NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS local_retention_insert_revision
                AFTER INSERT ON local_retention_references BEGIN
                    INSERT INTO local_retention_revisions(task_id, revision) VALUES(NEW.task_id, 1)
                    ON CONFLICT(task_id) DO UPDATE SET revision=revision+1;
                END;
                CREATE TRIGGER IF NOT EXISTS local_retention_update_revision
                AFTER UPDATE ON local_retention_references BEGIN
                    INSERT INTO local_retention_revisions(task_id, revision) VALUES(NEW.task_id, 1)
                    ON CONFLICT(task_id) DO UPDATE SET revision=revision+1;
                END;
                CREATE TRIGGER IF NOT EXISTS local_retention_delete_revision
                AFTER DELETE ON local_retention_references BEGIN
                    INSERT INTO local_retention_revisions(task_id, revision) VALUES(OLD.task_id, 1)
                    ON CONFLICT(task_id) DO UPDATE SET revision=revision+1;
                END;
            ''')
            from .llm_admission_state import initialize_schema
            initialize_schema(db)
            columns = {row['name'] for row in db.execute('PRAGMA table_info(control_groups)')}
            for name, kind in (('run_generation', 'INTEGER'), ('worker_started', 'REAL'),
                               ('source_digest', 'TEXT'), ('evidence_capture', 'TEXT'),
                               ('capture_snapshot', 'TEXT'), ('capture_version', 'TEXT'),
                               ('group_id', 'TEXT'), ('blocked_reason', 'TEXT'), ('body_sha256', 'TEXT')):
                if name not in columns:
                    db.execute(f'ALTER TABLE control_groups ADD COLUMN {name} {kind}')
            decision_columns = {row['name'] for row in db.execute('PRAGMA table_info(model_decisions)')}
            for name in ('request_scope', 'request_kind', 'request_members'):
                if name not in decision_columns:
                    db.execute(f'ALTER TABLE model_decisions ADD COLUMN {name} TEXT')
            db.execute('CREATE INDEX IF NOT EXISTS control_groups_by_id ON control_groups(task_id, group_id)')
            upload_columns = {row['name'] for row in db.execute('PRAGMA table_info(uploads)')}
            if 'expected_files' not in upload_columns:
                db.execute('ALTER TABLE uploads ADD COLUMN expected_files INTEGER')
            manifest_columns = {row['name'] for row in db.execute('PRAGMA table_info(tus_manifest)')}
            for name in ('writer_id', 'create_invocation_id'):
                if name not in manifest_columns:
                    db.execute(f'ALTER TABLE tus_manifest ADD COLUMN {name} TEXT')
        from .first_step_tusd import TusFolderIntake
        self.tus = TusFolderIntake(self)

    def _tus_head(self, tus_id):
        if not re.fullmatch(r'[0-9a-f]{32}', tus_id):
            raise ControlError('上传编号无效')
        request = Request(self.tus_backend_url + tus_id, method='HEAD', headers={'Tus-Resumable': '1.0.0'})
        try:
            with urlopen(request, timeout=5) as response:
                offset = int(response.headers['Upload-Offset'])
                length = int(response.headers['Upload-Length'])
                metadata = {}
                for entry in (response.headers.get('Upload-Metadata') or '').split(','):
                    key, separator, value = entry.strip().partition(' ')
                    if key:
                        metadata[key] = base64.b64decode(value, validate=True).decode('utf-8') if separator else ''
                return {'offset': offset, 'length': length, 'metadata': metadata}
        except Exception:
            raise ControlError('文件上传结果未确认，请稍后重新核对', 409) from None

    def _lock(self, prefix, identifier):
        directory = self.data / 'control-locks'
        directory.mkdir(parents=True, exist_ok=True)
        return FileLock(str(directory / (prefix + '-' + _uuid(identifier) + '.lock')), timeout=30)

    def _upload(self, db, upload_id):
        row = db.execute('SELECT * FROM uploads WHERE upload_id=?', (_uuid(upload_id),)).fetchone()
        if row is None:
            raise ControlError('上传任务不存在', 404)
        return row

    def _task(self, db, task_id):
        row = db.execute('SELECT * FROM tasks WHERE task_id=?', (_uuid(task_id),)).fetchone()
        if row is None:
            raise ControlError('第一步任务不存在', 404)
        return row

    def create_task(self, name, request_id=None):
        name = _task_name(name)
        if request_id is not None:
            try:
                request_id = str(uuid.UUID(request_id))
            except (ValueError, TypeError, AttributeError):
                raise ControlError('新建请求编号无效', 400) from None
        task_id, now = str(uuid.uuid4()), time.time_ns()
        with closing(self.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            if request_id is not None:
                previous = db.execute('SELECT * FROM task_create_requests WHERE request_id=?', (request_id,)).fetchone()
                if previous:
                    if previous['name'] != name:
                        raise ControlError('新建请求编号已用于其他任务')
                    task_id = previous['task_id']
                    if self._task(db, task_id)['prefect_order_model'] != 'v2':
                        raise ControlError('该创建请求属于原工单服务，请使用原服务核对', 409)
                else:
                    db.execute('INSERT INTO tasks (task_id,started_at_ns,task_status,total_groups,discovered_groups,'
                               'total_known,desired_state,name,updated_at_ns,artifact_scoped,prefect_order_model) '
                               'VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                               (task_id, now, 'draft', 0, 0, 0, 'paused', name, now, 1, 'v2'))
                    db.execute('INSERT INTO task_create_requests VALUES (?,?,?)', (request_id, task_id, name))
            else:
                db.execute('INSERT INTO tasks (task_id,started_at_ns,task_status,total_groups,discovered_groups,'
                           'total_known,desired_state,name,updated_at_ns,artifact_scoped,prefect_order_model) '
                           'VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                           (task_id, now, 'draft', 0, 0, 0, 'paused', name, now, 1, 'v2'))
        order_status = 'pending'
        if self.scheduler is not None:
            try:
                self.ensure_task_order(task_id, name)
                order_status = 'bound'
            except Exception:
                # The task and its request id are durable; start retries the same
                # idempotency key and must not assume an ambiguous response failed.
                pass
        with closing(self.summary._connect()) as db:
            task = self._task(db, task_id)
            return {'task_id': task_id, 'name': task['name'], 'task_status': task['task_status'],
                    'total_groups': task['total_groups'] if task['total_known'] else None,
                    'discovered_groups': task['discovered_groups'] or 0,
                    'control_revision': task['control_revision'], 'prefect_order_status': order_status}

    def ensure_task_order(self, task_id, name=None):
        """Bind the stable order without holding a database write transaction over HTTP."""
        task_id = _uuid(task_id)
        if self.scheduler is None:
            raise ControlError('工单服务未配置', 503)
        with closing(self.summary._connect()) as db:
            task = self._task(db, task_id)
            if task['prefect_order_model'] != 'v2':
                raise ControlError('旧任务不属于新工单服务，需保留原工单处理', 409)
            if db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone():
                raise ControlError('任务正在删除，不能绑定工单', 409)
            previous = task['prefect_run_id']
            task_name = name or task['name']
        run = self.scheduler.ensure(task_id, task_name)
        run_id = run['flow_run_id']
        if previous and previous != run_id:
            raise ControlError('已有工单不属于当前统一服务，需先核对', 409)
        with closing(self.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            current = self._task(db, task_id)
            if current['prefect_run_id'] not in (None, run_id):
                raise ControlError('任务工单绑定已变化', 409)
            if db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone():
                raise ControlError('任务正在删除，不能绑定工单', 409)
            db.execute('UPDATE tasks SET prefect_run_id=? WHERE task_id=?', (run_id, task_id))
        return run_id

    def task_by_request(self, request_id):
        try:
            request_id = str(uuid.UUID(request_id))
        except (ValueError, TypeError, AttributeError):
            raise ControlError('新建请求编号无效', 400) from None
        with closing(self.summary._connect()) as db:
            row = db.execute('SELECT task_id FROM task_create_requests WHERE request_id=?', (request_id,)).fetchone()
            if row is None:
                raise ControlError('新建请求不存在', 404)
            task = self._task(db, row['task_id'])
            if task['prefect_order_model'] != 'v2':
                raise ControlError('该创建请求属于原工单服务，请使用原服务核对', 409)
            return {'task_id': task['task_id'], 'name': task['name'], 'task_status': task['task_status'],
                    'control_revision': task['control_revision'],
                    'prefect_order_status': 'bound' if task['prefect_run_id'] else 'pending'}

    def list_tasks(self, page, page_size):
        with closing(self.summary._connect()) as db:
            db.execute('BEGIN')
            total = db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0]
            rows = db.execute('SELECT * FROM tasks ORDER BY COALESCE(updated_at_ns,started_at_ns) DESC, '
                              'started_at_ns DESC LIMIT ? OFFSET ?', (page_size, (page - 1) * page_size)).fetchall()
            items = []
            for task in rows:
                deleting = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task['task_id'],)).fetchone()
                counts = {'passed': 0, 'failed': 0, 'unrecognized': 0}
                for count in db.execute('SELECT status,COUNT(*) AS n FROM group_results WHERE task_id=? GROUP BY status',
                                        (task['task_id'],)):
                    counts[count['status']] = count['n']
                items.append(dict(task_id=task['task_id'], name=task['name'],
                                  task_status='deleting' if deleting else task['task_status'],
                                  created_at=timestamp(task['started_at_ns']),
                                  updated_at=timestamp(task['updated_at_ns'] or task['started_at_ns']),
                                  saved_at=timestamp(task['saved_at_ns']),
                                  total_groups=task['total_groups'] if task['total_known'] else None,
                                  discovered_groups=task['discovered_groups'] or 0,
                                  processed_groups=sum(counts.values()),
                                  passed_groups=counts['passed'], failed_groups=counts['failed'],
                                  unrecognized_groups=counts['unrecognized'],
                                  unknown_groups=db.execute("SELECT COUNT(*) FROM control_groups WHERE task_id=? "
                                      "AND state='unknown'", (task['task_id'],)).fetchone()[0],
                                  control_revision=task['control_revision']))
            return {'items': items, 'total': total, 'page': page, 'page_size': page_size,
                    'has_next': page * page_size < total}

    def save_task(self, task_id, name=None):
        task_id = _uuid(task_id)
        if name is not None:
            name = _task_name(name)
        with closing(self.summary._connect()) as db, db:
            task = self._task(db, task_id)
            now = time.time_ns()
            db.execute('UPDATE tasks SET name=?,saved_at_ns=?,updated_at_ns=? WHERE task_id=?',
                       (name if name is not None else task['name'], now, now, task_id))
            row = self._task(db, task_id)
            return {'task_id': task_id, 'name': row['name'], 'task_status': row['task_status'],
                    'saved_at': timestamp(row['saved_at_ns']), 'control_revision': row['control_revision']}

    def create_upload(self, task_id, root_name):
        task_id = _uuid(task_id)
        root = _relative(root_name)
        if len(root.parts) != 1:
            raise ControlError('根文件夹名称无效', 400)
        upload_id = str(uuid.uuid4())
        with self._lock('task', task_id), closing(self.summary._connect()) as db, db:
            task = self._task(db, task_id)
            if task['upload_id']:
                existing = self._upload(db, task['upload_id'])
                if existing['root_name'] != root_name:
                    raise ControlError('当前任务已有其他上传文件夹')
                return {'task_id': task_id, 'upload_id': existing['upload_id'], 'status': existing['state']}
            if task['task_status'] != 'draft':
                raise ControlError('当前任务不能创建新的上传')
            db.execute('INSERT INTO uploads (upload_id,root_name,state,created_at_ns,task_id) VALUES (?, ?, ?, ?, ?)',
                       (upload_id, root_name, 'uploading', time.time_ns(), task_id))
            db.execute("UPDATE tasks SET task_status='uploading',upload_id=?,updated_at_ns=?,"
                       'control_revision=control_revision+1 WHERE task_id=?', (upload_id, time.time_ns(), task_id))
        return {'task_id': task_id, 'upload_id': upload_id, 'status': 'uploading'}

    def source_library(self, task):
        if task['source_library']:
            return Path(task['source_library'])
        return self.data / 'uploads' / task['upload_id'] / 'source'

    def begin_upload_transfer(self, task_id, upload_id):
        task_id, upload_id = _uuid(task_id), _uuid(upload_id)
        with self._lock('task', task_id), closing(self.summary._connect()) as db:
            task = self._task(db, task_id)
            if task['task_status'] != 'uploading' or task['upload_id'] != upload_id:
                raise ControlError('任务不在上传状态')
            with self._upload_transfer_lock:
                self._upload_transfers[task_id] = self._upload_transfers.get(task_id, 0) + 1
        return task_id

    def end_upload_transfer(self, task_id):
        with self._upload_transfer_lock:
            remaining = self._upload_transfers.get(task_id, 0) - 1
            if remaining > 0:
                self._upload_transfers[task_id] = remaining
            else:
                self._upload_transfers.pop(task_id, None)

    def has_upload_transfers(self, task_id):
        with self._upload_transfer_lock:
            return bool(self._upload_transfers.get(task_id))

    def group_scope(self, task, group_path):
        group = Path(group_path).resolve()
        if task['source_root']:
            return 'local:' + group.as_posix().casefold()
        return group.relative_to(self.source_library(task).resolve()).as_posix().casefold()

    def artifact_root(self, task):
        return self.output / 'tasks' / task['task_id'] if task['artifact_scoped'] else self.output

    def select_local_source(self, task_id, raw_path):
        task_id = _uuid(task_id)
        if not isinstance(raw_path, str) or not raw_path.strip():
            raise ControlError('请选择本机资料文件夹', 400)
        source = Path(raw_path.strip()).expanduser()
        if not source.is_absolute() or not source.is_dir():
            raise ControlError('本机资料文件夹不存在或不是绝对路径', 400)
        source = source.resolve()
        if not source.name:
            raise ControlError('请选择具体的资料文件夹', 400)
        for work_root in (self.output, self.data, self.receipt_root, self.legacy_conversion_root):
            if source == work_root or source.is_relative_to(work_root) or work_root.is_relative_to(source):
                raise ControlError('请选择工作区以外的原始资料文件夹', 400)
        def fail(error):
            raise ControlError('资料文件夹无法完整读取', 400) from error
        relative_paths = []
        for current, dirs, files in os.walk(source, followlinks=False, onerror=fail):
            current = Path(current)
            for name in [*dirs, *files]:
                path = current / name
                if path.is_symlink() or path.is_junction():
                    raise ControlError('资料文件夹包含链接，请选择真实文件夹', 400)
            for name in files:
                relative_paths.append((PurePosixPath(source.name) / (current / name).relative_to(source).as_posix()).as_posix())
        groups, ungrouped = discover_groups(relative_paths, source.name)
        library = source.parent if source.name in groups else source
        group_paths = [source if group == source.name else source.joinpath(*PurePosixPath(group).parts[1:])
                       for group in groups]
        known = bool(groups) and not ungrouped
        state = 'ready' if groups else 'needs_review'
        with self._lock('task', task_id), closing(self.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            task = self._task(db, task_id)
            if task['task_status'] != 'draft' or task['upload_id']:
                raise ControlError('当前任务已有资料，不能再次选择')
            db.execute("UPDATE tasks SET task_status=?,total_groups=?,discovered_groups=?,total_known=?,"
                       "ungrouped_files=?,source_root=?,source_library=?,desired_state='paused',updated_at_ns=?,"
                       'control_revision=control_revision+1 WHERE task_id=?',
                       (state, len(groups), len(groups), int(known), json.dumps(ungrouped, ensure_ascii=False),
                        str(source), str(library), time.time_ns(), task_id))
            db.executemany('INSERT INTO control_groups (task_id,seq,group_root,state,group_id) VALUES (?,?,?,?,?)',
                [(task_id, i, str(group), 'pending', identity(['source-group/1', str(library).casefold(),
                  group.relative_to(library).as_posix().casefold()])[:32]) for i, group in enumerate(group_paths)])
            task = self._task(db, task_id)
            return {'task_id': task_id, 'task_status': state, 'source_root': str(source),
                    'input_files': len(relative_paths), 'discovered_groups': len(groups),
                    'total_groups': len(groups) if known else None, 'ungrouped_files': ungrouped,
                    'control_revision': task['control_revision']}

    def _stopped_state(self, db, task_id, *, start_pending=False, old_status=None):
        from .llm_admission_state import counts
        active = db.execute("SELECT COUNT(*) FROM control_groups WHERE task_id=? AND state='running'", (task_id,)).fetchone()[0]
        unknown = db.execute("SELECT COUNT(*) FROM model_decisions WHERE task_id=? AND state='unknown'",
                             (task_id,)).fetchone()[0]
        inflight = db.execute("SELECT COUNT(*) FROM model_decisions WHERE task_id=? AND state='inflight'", (task_id,)).fetchone()[0]
        llm = counts(db, task_id)
        inflight += llm['inflight']
        unknown += llm['unknown']
        state = ('unknown' if old_status == 'unknown' else
                 'pausing' if active or inflight or start_pending else
                 'unknown' if unknown else 'paused')
        return state, active, inflight, unknown

    def _pause_for_nimble(self, db, task_id, generation, reason=None):
        task = db.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        if (task is None or not self._running(db, task_id, generation) or
                task['task_status'] in {'deleting', 'delete_failed'} or
                db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone()):
            return False
        state = self._stopped_state(db, task_id, start_pending=bool(task['start_pending']),
                                    old_status=task['task_status'])[0]
        db.execute("UPDATE tasks SET desired_state='paused',task_status=?,stop_reason=?,"
                   'updated_at_ns=?,control_revision=control_revision+1 WHERE task_id=?',
                   (state, reason or 'Nimble 服务不可用，任务已停止领取新资料', time.time_ns(), task_id))
        return True

    def pause_for_nimble(self, task_id, generation, reason=None):
        task_id = _uuid(task_id)
        with closing(self.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            return self._pause_for_nimble(db, task_id, generation, reason)

    def _recover_stale_groups(self, db, task_id):
        groups = db.execute("SELECT seq,worker_pid,worker_started FROM control_groups "
                            "WHERE task_id=? AND state='running'", (task_id,)).fetchall()
        for group in groups:
            pid, started = group['worker_pid'], group['worker_started']
            if not _process_dead(pid, started):
                continue
            blocking = db.execute("SELECT detail FROM model_decisions WHERE task_id=? AND seq=? "
                                  "AND state IN ('inflight','unknown') AND step!='filename-title' LIMIT 1",
                                  (task_id, group['seq'])).fetchone()
            db.execute("UPDATE model_decisions SET state='unknown',detail='执行进程结束，结果未确认' "
                                 "WHERE task_id=? AND seq=? AND state='inflight'", (task_id, group['seq'])).rowcount
            db.execute("UPDATE control_groups SET state=?,blocked_reason=?,worker_pid=NULL,worker_started=NULL "
                       'WHERE task_id=? AND seq=?',
                       ('unknown' if blocking else 'pending',
                        (blocking['detail'] or '执行进程结束，模型请求结果未确认') if blocking else None,
                        task_id, group['seq']))

    def _running(self, db, task_id, generation=None):
        task = self._task(db, task_id)
        return (task['desired_state'] == 'running' and
                (generation is None or task['start_generation'] == generation))

    def run_is_requested(self, task_id, generation):
        task_id = _uuid(task_id)
        with closing(self.summary._connect()) as db:
            task = db.execute('SELECT desired_state,task_status,start_generation FROM tasks WHERE task_id=?',
                              (task_id,)).fetchone()
            return bool(task and task['desired_state'] == 'running' and task['start_generation'] == generation
                        and task['task_status'] not in {'deleting', 'delete_failed'}
                        and not db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone())

    def begin_run(self, task_id, generation, run_id):
        task_id = _uuid(task_id)
        with closing(self.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            task = self._task(db, task_id)
            if task['task_status'] in {'complete', 'needs_review', 'failed', 'deleting', 'delete_failed'}:
                return False
            if not self._running(db, task_id, generation):
                return False
            if task['worker_run_id'] not in (None, run_id):
                return False
            db.execute('UPDATE tasks SET worker_run_id=?,worker_owner_pid=?,worker_owner_started=? WHERE task_id=?',
                       (run_id, os.getpid(), psutil.Process().create_time(), task_id))
            return True

    def finish_run(self, task_id, generation, run_id):
        """Release a cleanly stopped worker without masking outstanding requests."""
        task_id = _uuid(task_id)
        with closing(self.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            task = db.execute('SELECT worker_run_id,start_generation FROM tasks WHERE task_id=?',
                              (task_id,)).fetchone()
            if not task or task['worker_run_id'] != run_id or task['start_generation'] != generation:
                return False
            active = db.execute("SELECT 1 FROM control_groups WHERE task_id=? AND state='running' LIMIT 1",
                                (task_id,)).fetchone()
            inflight = db.execute("SELECT 1 FROM model_decisions WHERE task_id=? AND state='inflight' LIMIT 1",
                                  (task_id,)).fetchone()
            if active or inflight:
                return False
            db.execute('UPDATE tasks SET worker_run_id=NULL,worker_owner_pid=NULL,worker_owner_started=NULL '
                       'WHERE task_id=?', (task_id,))
            return True

    def refresh_task(self, task_id):
        """Reflect a vanished worker on status reads, without restarting it."""
        task_id = _uuid(task_id)
        # Ordinary dashboard reads do not need SQLite's global writer lock.
        # Recheck under the write transaction only if a dead owner is observed.
        with closing(self.summary._connect()) as probe:
            task = self._task(probe, task_id)
            if task['task_status'] in {'deleting', 'delete_failed'}:
                return
            dead_start = _start_is_abandoned(task)
            if task['task_status'] in {'complete', 'needs_review', 'failed'} and not dead_start:
                return
            groups = probe.execute("SELECT worker_pid,worker_started FROM control_groups "
                                   "WHERE task_id=? AND state='running'", (task_id,)).fetchall()
            stale_group = any(_process_dead(row['worker_pid'], row['worker_started']) for row in groups)
            dead_worker = bool(task['worker_run_id'] and
                               _process_dead(task['worker_owner_pid'], task['worker_owner_started']))
            dead_start = _start_is_abandoned(task)
            if not (stale_group or dead_worker or dead_start):
                return
        with closing(self.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            task = self._task(db, task_id)
            if task['task_status'] in {'deleting', 'delete_failed'}:
                return
            dead_start = _start_is_abandoned(task)
            if task['task_status'] in {'complete', 'needs_review', 'failed'} and not dead_start:
                return
            before = db.execute("SELECT COUNT(*) FROM control_groups WHERE task_id=? AND state='running'",
                                (task_id,)).fetchone()[0]
            self._recover_stale_groups(db, task_id)
            after = db.execute("SELECT COUNT(*) FROM control_groups WHERE task_id=? AND state='running'",
                               (task_id,)).fetchone()[0]
            dead_worker = bool(task['worker_run_id'] and
                               _process_dead(task['worker_owner_pid'], task['worker_owner_started']))
            dead_start = _start_is_abandoned(task)
            if before != after or dead_worker or dead_start:
                current = self._task(db, task_id)
                if dead_start or current['task_status'] == 'unknown':
                    state, reason = 'unknown', '模型请求或工单启动结果未确认'
                else:
                    state, reason = self._stopped_state(db, task_id)[0], '执行进程已退出，任务已暂停'
                db.execute("UPDATE tasks SET desired_state='paused',task_status=?,stop_reason=?,"
                           "start_pending=0,worker_run_id=NULL,worker_owner_pid=NULL,worker_owner_started=NULL,"
                           "updated_at_ns=?,control_revision=control_revision+1 WHERE task_id=?",
                           (state, reason, time.time_ns(), task_id))

    def group_allowed(self, task_id, seq, generation):
        with closing(self.summary._connect()) as db:
            group = db.execute('SELECT state,run_generation FROM control_groups WHERE task_id=? AND seq=?',
                               (_uuid(task_id), seq)).fetchone()
            return bool(group and group['state'] == 'running' and group['run_generation'] == generation
                        and self._running(db, task_id, generation))

    def bind_group_source(self, task_id, seq, source_digest, group_id=None):
        if source_digest is None:
            return
        with closing(self.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT state,source_digest FROM control_groups WHERE task_id=? AND seq=?',
                             (_uuid(task_id), seq)).fetchone()
            if not row or row['state'] != 'running' or row['source_digest'] not in (None, source_digest):
                raise UnknownRequest('资料组内容已变化，无法复用原判断')
            db.execute('UPDATE control_groups SET source_digest=?,group_id=COALESCE(?,group_id) WHERE task_id=? AND seq=?',
                       (source_digest, group_id, task_id, seq))

    def record_confirmed_body(self, task_id, seq, generation, digest):
        if not isinstance(digest, str) or not re.fullmatch(r'[0-9a-fA-F]{64}', digest):
            return
        with closing(self.summary._connect()) as db, db:
            changed = db.execute("UPDATE control_groups SET body_sha256=? WHERE task_id=? AND seq=? "
                                 "AND state='running' AND run_generation=?",
                                 (digest.lower(), _uuid(task_id), seq, generation)).rowcount
            if not changed:
                raise PauseRequested()

    def save_evidence_capture(self, task_id, seq, snapshot, version, capture):
        encoded = json.dumps(capture, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        with closing(self.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM control_groups WHERE task_id=? AND seq=?',
                             (_uuid(task_id), seq)).fetchone()
            if not row or row['state'] != 'running':
                raise UnknownRequest('资料组状态变化，无法保存附件证据')
            if row['evidence_capture'] is not None and (row['capture_snapshot'] != snapshot or
                    row['capture_version'] != version or row['evidence_capture'] != encoded):
                raise UnknownRequest('已有附件证据与当前结果不一致')
            db.execute('UPDATE control_groups SET capture_snapshot=?,capture_version=?,evidence_capture=? '
                       'WHERE task_id=? AND seq=?', (snapshot, version, encoded, task_id, seq))

    def load_evidence_capture(self, task_id, seq, snapshot, version):
        with closing(self.summary._connect()) as db:
            row = db.execute('SELECT * FROM control_groups WHERE task_id=? AND seq=?',
                             (_uuid(task_id), seq)).fetchone()
            if not row or row['capture_snapshot'] != snapshot or row['capture_version'] != version:
                return None
            return json.loads(row['evidence_capture']) if row['evidence_capture'] else None

    def claim_next(self, task_id, generation=None, *, seq=None):
        """Admit a pending group when its Prefect task actually starts."""
        task_id = _uuid(task_id)
        with closing(self.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT 1 FROM tasks WHERE task_id=?', (task_id,)).fetchone() is None:
                return None
            if not self._running(db, task_id, generation):
                return None
            task = self._task(db, task_id)
            if seq is None:
                row = db.execute("SELECT seq,group_root FROM control_groups WHERE task_id=? AND state='pending' "
                                 'ORDER BY seq LIMIT 1', (task_id,)).fetchone()
            else:
                row = db.execute("SELECT seq,group_root FROM control_groups WHERE task_id=? AND state='pending' "
                                 'AND seq=?', (task_id, seq)).fetchone()
            if row is None:
                return None
            db.execute("UPDATE control_groups SET state='running',worker_pid=?,worker_started=?,run_generation=? "
                       'WHERE task_id=? AND seq=?',
                       (os.getpid(), psutil.Process().create_time(), task['start_generation'], task_id, row['seq']))
            db.execute("UPDATE tasks SET task_status='running',updated_at_ns=?,control_revision=control_revision+1 WHERE task_id=?",
                       (time.time_ns(), task_id))
            return row['seq'], row['group_root']

    def defer_group(self, task_id, seq, generation=None):
        task_id = _uuid(task_id)
        with closing(self.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            task = self._task(db, task_id)
            changed = db.execute("UPDATE control_groups SET state='pending',worker_pid=NULL WHERE task_id=? AND seq=? "
                                 "AND state='running' AND (? IS NULL OR run_generation=?)",
                                 (task_id, seq, generation, generation)).rowcount
            if not changed:
                return
            state = task['task_status']
            if task['task_status'] in {'deleting', 'delete_failed'}:
                state = task['task_status']
            elif task['desired_state'] == 'paused':
                state = self._stopped_state(db, task_id, start_pending=bool(task['start_pending']),
                                            old_status=task['task_status'])[0]
            db.execute('UPDATE tasks SET task_status=?,updated_at_ns=?,control_revision=control_revision+1 WHERE task_id=?',
                       (state, time.time_ns(), task_id))

    def mark_unknown(self, task_id, reason='模型请求结果未确认', *, generation=None):
        with closing(self.summary._connect()) as db, db:
            db.execute("UPDATE tasks SET desired_state='paused',task_status='unknown',stop_reason=?,updated_at_ns=?,"
                       "control_revision=control_revision+1 WHERE task_id=? AND task_status NOT IN ('deleting','delete_failed') "
                       'AND (? IS NULL OR start_generation=?) '
                       'AND NOT EXISTS (SELECT 1 FROM task_deletions WHERE task_deletions.task_id=tasks.task_id)',
                       (str(reason)[:500], time.time_ns(), _uuid(task_id), generation, generation))

    def mark_group_unknown(self, task_id, seq, generation, reason):
        task_id = _uuid(task_id)
        with closing(self.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            task = self._task(db, task_id)
            changed = db.execute("UPDATE control_groups SET state='unknown',blocked_reason=?,worker_pid=NULL,"
                "worker_started=NULL WHERE task_id=? AND seq=? AND state='running' AND run_generation=?",
                (str(reason)[:500], task_id, seq, generation)).rowcount
            if not changed:
                return
            remaining = db.execute("SELECT COUNT(*) FROM control_groups WHERE task_id=? "
                                   "AND state IN ('pending','running')", (task_id,)).fetchone()[0]
            if task['task_status'] in {'deleting', 'delete_failed'}:
                state = task['task_status']
            elif task['task_status'] == 'unknown':
                state = 'unknown'
            elif task['desired_state'] == 'paused':
                state = self._stopped_state(db, task_id, start_pending=bool(task['start_pending']))[0]
            else:
                state = 'running' if remaining else 'needs_review'
            now = time.time_ns()
            db.execute('UPDATE tasks SET task_status=?,updated_at_ns=?,auto_saved_at_ns=?,'
                       'control_revision=control_revision+1 WHERE task_id=?', (state, now, now, task_id))

    def finish_group(self, task_id, seq, generation, row):
        task_id = _uuid(task_id)
        with closing(self.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            task = self._task(db, task_id)
            group = db.execute('SELECT state,run_generation FROM control_groups WHERE task_id=? AND seq=?',
                               (task_id, seq)).fetchone()
            if (group is None or group['state'] != 'running' or group['run_generation'] != generation
                    or task['start_generation'] != generation):
                raise ControlError('资料组处理代次已变化')
            db.execute('INSERT INTO group_results VALUES (?,?,?,?)',
                       (task_id, seq, row['status'], json.dumps(row, ensure_ascii=False, separators=(',', ':'))))
            db.execute("UPDATE control_groups SET state='done',worker_pid=NULL WHERE task_id=? AND seq=?", (task_id, seq))
            remaining = db.execute("SELECT COUNT(*) FROM control_groups WHERE task_id=? "
                                   "AND state IN ('pending','running')", (task_id,)).fetchone()[0]
            unknown = db.execute("SELECT COUNT(*) FROM control_groups WHERE task_id=? AND state='unknown'",
                                 (task_id,)).fetchone()[0]
            state, total_known = task['task_status'], task['total_known']
            if task['task_status'] in {'deleting', 'delete_failed'}:
                state = task['task_status']
            elif task['task_status'] == 'unknown':
                state = 'unknown'
            elif remaining == 0:
                ungrouped = json.loads(task['ungrouped_files'])
                state = 'needs_review' if unknown or ungrouped else 'complete'
                total_known = int(not ungrouped)
            elif task['desired_state'] == 'paused':
                state = self._stopped_state(db, task_id, start_pending=bool(task['start_pending']))[0]
            now = time.time_ns()
            db.execute('UPDATE tasks SET task_status=?,total_known=?,updated_at_ns=?,auto_saved_at_ns=?,'
                       'control_revision=control_revision+1 WHERE task_id=?',
                       (state, total_known, now, now, task_id))

    def _unknown_request(self, db, task_id, seq, step, detail):
        db.execute("UPDATE model_decisions SET state='unknown',detail=? WHERE task_id=? AND seq=? AND step=?",
                   (str(detail)[:500], task_id, seq, step))

    def _transport_unknown(self, db, task_id, seq, step, generation, exc):
        self._unknown_request(db, task_id, seq, step, 'Nimble 传输中断：' + type(exc).__name__)
        if generation is None:
            generation = self._task(db, task_id)['start_generation']
        errors = db.execute("SELECT COUNT(DISTINCT d.seq) FROM model_decisions d "
                            'JOIN control_groups g ON d.task_id=g.task_id AND d.seq=g.seq '
                            "WHERE d.task_id=? AND d.state='unknown' AND d.detail LIKE 'Nimble 传输中断：%' "
                            'AND g.run_generation=?', (task_id, generation)).fetchone()[0]
        if errors >= 2:
            self._pause_for_nimble(db, task_id, generation,
                                   'Nimble 多次出现传输中断，相关请求结果待核对')

    def reject_model_response(self, task_id, seq, step, generation, reason):
        """Keep an unusable received response fenced as an unconfirmed judgment."""
        task_id = _uuid(task_id)
        with closing(self.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            task = db.execute('SELECT start_generation FROM tasks WHERE task_id=?', (task_id,)).fetchone()
            group = db.execute('SELECT state,run_generation FROM control_groups WHERE task_id=? AND seq=?',
                               (task_id, seq)).fetchone()
            if not task or task['start_generation'] != generation or not group or group['state'] != 'running' or group['run_generation'] != generation:
                return False
            changed = db.execute("UPDATE model_decisions SET state='unknown',detail=? WHERE task_id=? AND seq=? "
                                 "AND step=? AND state='complete'", (str(reason)[:500], task_id, seq, step)).rowcount
            if changed:
                self._pause_for_nimble(db, task_id, generation, reason + '，已停止领取新资料')
            return bool(changed)

    def cached_model(self, task_id, seq, step, snapshot_id, payload_hash, generation):
        with closing(self.summary._connect()) as db:
            if not self._running(db, task_id, generation):
                raise PauseRequested()
            row = db.execute('SELECT * FROM model_decisions WHERE task_id=? AND seq=? AND step=?',
                             (_uuid(task_id), seq, step)).fetchone()
            if (row and row['state'] == 'complete' and row['snapshot_id'] == snapshot_id
                    and row['payload_hash'] == payload_hash):
                return json.loads(row['response'])
            return None

    def _model_queue_timed_out(self, task_id, generation):
        with closing(self.summary._connect()) as db:
            if not self._running(db, task_id, generation):
                raise PauseRequested()
        raise TimeoutError('Nimble 请求排队超时')

    def invoke_model(self, task_id, seq, step, snapshot_id, payload_hash, begin, receive, *, timeout, generation=None,
                     request_identity=None):
        """Persist a decision before sending; never repeat a request of unknown outcome."""
        task_id = _uuid(task_id)
        if float(timeout) <= 0:
            self._model_queue_timed_out(task_id, generation)
        process_gate = None
        process_acquired = False
        try:
            directory = self.nimble_lock_path.parent
            directory.mkdir(parents=True, exist_ok=True)
            process_gate = FileLock(str(self.nimble_lock_path))
            while not process_acquired:
                with closing(self.summary._connect()) as db:
                    if not self._running(db, task_id, generation):
                        raise PauseRequested()
                try:
                    process_gate.acquire(timeout=.05)
                    process_acquired = True
                except FileLockTimeout:
                    pass
            deadline = time.monotonic() + float(timeout)
            with closing(self.summary._connect()) as db:
                with db:
                    db.execute('BEGIN IMMEDIATE')
                    if not self._running(db, task_id, generation):
                        raise PauseRequested()
                    group = db.execute('SELECT * FROM control_groups WHERE task_id=? AND seq=?',
                                       (task_id, seq)).fetchone()
                    if group is None or group['state'] != 'running':
                        raise ControlError('资料组未在处理')
                    prior_unknown = None
                    if request_identity:
                        previous = db.execute("SELECT request_scope,request_members FROM model_decisions WHERE "
                            "request_kind=? AND task_id!=? AND state IN ('inflight','unknown')",
                            (request_identity['kind'], task_id)).fetchall()
                        previous += db.execute("SELECT request_scope,request_members FROM deleted_unknown_requests WHERE "
                            "request_kind=?", (request_identity['kind'],)).fetchall()
                        members = request_identity['members']
                        for previous_row in previous:
                            if not _same_request_scope(previous_row['request_scope'], request_identity['scope']):
                                continue
                            old_members = json.loads(previous_row['request_members'])
                            if (bool(set(members) & set(old_members)) if step == 'body' else members == old_members):
                                prior_unknown = previous_row
                                break
                    if not prior_unknown and group['source_digest']:
                        legacy = db.execute("SELECT g.group_root,t.upload_id,t.source_library FROM control_groups g JOIN model_decisions d "
                            "ON d.task_id=g.task_id AND d.seq=g.seq JOIN tasks t ON t.task_id=g.task_id "
                            "WHERE g.source_digest=? "
                            "AND g.task_id!=? AND d.state IN ('inflight','unknown') "
                            "AND d.request_scope IS NULL AND (d.step=? OR (? AND d.step LIKE 'other:%'))",
                            (group['source_digest'], task_id, step, int(step.startswith('other:')))).fetchall()
                        def upload_scope(root, upload_id, source_library=None):
                            try:
                                source = Path(source_library) if source_library else self.data / 'uploads' / upload_id / 'source'
                                return Path(root).resolve().relative_to(source.resolve()).as_posix().casefold()
                            except (TypeError, OSError, ValueError):
                                return None
                        scope = (request_identity['scope'] if request_identity else
                                  upload_scope(group['group_root'], self._task(db, task_id)['upload_id'],
                                               self._task(db, task_id)['source_library']))
                        for old in legacy:
                            old_scope = upload_scope(old['group_root'], old['upload_id'], old['source_library'])
                            if not scope or not old_scope or _same_request_scope(old_scope, scope):
                                prior_unknown = old
                                break
                        if not prior_unknown:
                            deleted = db.execute("SELECT group_scope FROM deleted_unknown_requests WHERE source_digest=? "
                                "AND request_scope IS NULL AND (step=? OR (? AND step LIKE 'other:%'))",
                                (group['source_digest'], step, step.startswith('other:'))).fetchall()
                            for old in deleted:
                                if not scope or not old['group_scope'] or _same_request_scope(old['group_scope'], scope):
                                    prior_unknown = old
                                    break
                    row = db.execute('SELECT * FROM model_decisions WHERE task_id=? AND seq=? AND step=?',
                                     (task_id, seq, step)).fetchone()
                    if prior_unknown:
                        conflict = 'unknown'
                    elif row is not None:
                        if row['snapshot_id'] != snapshot_id or row['payload_hash'] != payload_hash:
                            if row['state'] == 'complete':
                                conflict = 'changed'
                            else:
                                self._unknown_request(db, task_id, seq, step, '已保存判断与当前资料或请求不同')
                                conflict = 'unknown'
                        elif row['state'] == 'complete':
                            return json.loads(row['response'])
                        else:
                            self._unknown_request(db, task_id, seq, step, '前次模型请求结果未确认')
                            conflict = 'unknown'
                    else:
                        conflict = False
                        key = request_identity or {}
                        db.execute('INSERT INTO model_decisions (task_id,seq,step,snapshot_id,payload_hash,state,'
                                   'request_scope,request_kind,request_members) VALUES (?,?,?,?,?,\'inflight\',?,?,?)',
                                   (task_id, seq, step, snapshot_id, payload_hash, key.get('scope'), key.get('kind'),
                                    json.dumps(key['members'], ensure_ascii=False) if key else None))
                if conflict == 'changed':
                    raise ControlError('已保存判断与当前资料或请求不同，无法自动复用')
                if conflict:
                    raise UnknownRequest('前次模型判断无法安全复用')
                # Serialize the final pause check with the actual request write.
                # The committed intent above allows recovery if the process exits.
                with db:
                    db.execute('BEGIN IMMEDIATE')
                    if not self._running(db, task_id, generation):
                        db.execute('DELETE FROM model_decisions WHERE task_id=? AND seq=? AND step=?',
                                   (task_id, seq, step))
                        paused_before_send = True
                        timed_out_before_send = False
                        handle = None
                        send_error = None
                    elif time.monotonic() >= deadline:
                        db.execute('DELETE FROM model_decisions WHERE task_id=? AND seq=? AND step=?',
                                   (task_id, seq, step))
                        paused_before_send = False
                        timed_out_before_send = True
                        handle = None
                        send_error = None
                    else:
                        paused_before_send = False
                        timed_out_before_send = False
                        try:
                            handle = begin()
                            send_error = None
                        except Exception as exc:
                            send_error = exc
                            if isinstance(exc, RequestNotSent):
                                db.execute('DELETE FROM model_decisions WHERE task_id=? AND seq=? AND step=?',
                                           (task_id, seq, step))
                                self._pause_for_nimble(db, task_id, generation)
                            else:
                                self._transport_unknown(db, task_id, seq, step, generation, exc)
                if paused_before_send:
                    raise PauseRequested()
                if timed_out_before_send:
                    self._model_queue_timed_out(task_id, generation)
                if send_error is not None:
                    if isinstance(send_error, RequestNotSent):
                        raise send_error
                    raise UnknownRequest('模型请求发送结果未知') from send_error
            try:
                response = receive(handle)
                encoded = json.dumps(response, ensure_ascii=False, separators=(',', ':'))
            except Exception as exc:
                with closing(self.summary._connect()) as db, db:
                    db.execute('BEGIN IMMEDIATE')
                    self._transport_unknown(db, task_id, seq, step, generation, exc)
                raise UnknownRequest('模型请求结果未知') from exc
            status = response.get('status') if isinstance(response, dict) else None
            bad_status = isinstance(status, int) and not 200 <= status < 300
            invalid_decision = isinstance(response, dict) and response.get('invalid_decision') is True
            unusable = bad_status or invalid_decision
            reason = (f'Nimble 返回 HTTP {status}，未得到可用判断' if bad_status else
                      'Nimble 响应格式异常，未得到可用判断')
            with closing(self.summary._connect()) as db, db:
                db.execute('BEGIN IMMEDIATE')
                db.execute('UPDATE model_decisions SET state=?,response=?,detail=? '
                           'WHERE task_id=? AND seq=? AND step=?',
                           ('unknown' if unusable else 'complete', encoded,
                            reason if unusable else None,
                            task_id, seq, step))
                if unusable:
                    self._pause_for_nimble(db, task_id, generation, reason + '，已停止领取新资料')
            if unusable:
                raise UnknownRequest(reason + '，需核对')
            return response
        finally:
            if process_acquired:
                process_gate.release()

    def _task_has_folder_orders(self, task_id):
        """A missing task queue is normal only before any folder order exists."""
        if self.local_intake_queue_db is None or not self.local_intake_queue_db.is_file():
            raise ValueError('本地工单状态不可用')
        with closing(sqlite3.connect(self.local_intake_queue_db.as_uri() + '?mode=ro',
                                     uri=True, timeout=1)) as db:
            return bool(db.execute('SELECT 1 FROM preprocessing_folder_orders WHERE task_id=? LIMIT 1',
                                   (task_id,)).fetchone())

    def order_queue_status(self, task_id):
        """Read, without changing Prefect, the queue status for one task revision."""
        revision = None
        try:
            task_id = _uuid(task_id)
            with closing(self.summary._connect()) as db:
                task = self._task(db, task_id)
                deleting = bool(db.execute('SELECT 1 FROM task_deletions WHERE task_id=?',
                                           (task_id,)).fetchone())
                revision = task['control_revision']
                paused = deleting or task['desired_state'] != 'running' or bool(task['start_pending'])
            if self.scheduler is None:
                raise ValueError('Prefect 服务未配置')
            observed = self.scheduler.task_queue_status(task_id, paused=paused)
            synced = (observed == 'matched' or
                      (observed == 'missing' and not self._task_has_folder_orders(task_id)))
            with closing(self.summary._connect()) as db:
                if self._task(db, task_id)['control_revision'] != revision:
                    synced = False
            return {'prefect_queue_status': 'synced' if synced else 'pending',
                    'prefect_queue_revision': revision}
        except Exception:
            return {'prefect_queue_status': 'pending', 'prefect_queue_revision': revision}

    def _sync_order_queue(self, task_id, *, expected_revision=None):
        """Sync Prefect after committing intent; admission rechecks the same intent."""
        if self.scheduler is None:
            return 'pending'
        try:
            task_id = _uuid(task_id)
            for _ in range(2):
                with closing(self.summary._connect()) as db:
                    task = self._task(db, task_id)
                    deleting = bool(db.execute('SELECT 1 FROM task_deletions WHERE task_id=?',
                                               (task_id,)).fetchone())
                    paused = (deleting or task['desired_state'] != 'running' or bool(task['start_pending']))
                    revision = task['control_revision']
                if expected_revision is not None and revision != expected_revision:
                    return 'pending'
                status = self.scheduler.sync_task_queue(task_id, paused=paused)
                if status == 'missing' and self._task_has_folder_orders(task_id):
                    return 'pending'
                with closing(self.summary._connect()) as db:
                    if self._task(db, task_id)['control_revision'] == revision:
                        return 'synced'
        except Exception:
            pass  # The receiver will retry; local admission still observes the newer intent.
        return 'pending'

    def start(self, task_id, expected_revision):
        try:
            result = self._start(task_id, expected_revision)
        except Exception:
            self._sync_order_queue(task_id)
            raise
        result['prefect_queue_status'] = self._sync_order_queue(
            task_id, expected_revision=result['control_revision'])
        return result

    def _start(self, task_id, expected_revision):
        task_id = _uuid(task_id)
        suspension = None
        with closing(self.summary._connect()) as probe:
            observed = self._task(probe, task_id)
            if observed['prefect_order_model'] != 'v2':
                raise ControlError('旧任务仍属于原工单服务，请使用原服务处理', 409)
            if observed['prefect_suspend_pending']:
                suspension = (observed['prefect_run_id'], observed['prefect_suspend_sequence'])
        if suspension:
            try:
                if self.scheduler is None or not suspension[0] or not self.scheduler.is_suspended(task_id, suspension[0]):
                    raise ControlError('工单正在暂停，请稍后再开始', 409)
            except ControlError:
                raise
            except Exception:
                raise ControlError('工单暂停状态未确认，请稍后再开始', 503) from None
        with closing(self.summary._connect()) as db:
            with self._lock('preprocessing', task_id), db:
                db.execute('BEGIN IMMEDIATE')
                task = self._task(db, task_id)
                if task['control_revision'] != expected_revision:
                    raise ControlError('任务状态已变化，请刷新后再开始')
                if task['start_pending']:
                    raise ControlError('任务正在连接工单，请稍后再开始', 409)
                if (task['prefect_suspend_pending'] and
                        suspension != (task['prefect_run_id'], task['prefect_suspend_sequence'])):
                    raise ControlError('工单暂停状态已变化，请稍后再开始', 409)
                if db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone():
                    raise ControlError('任务正在删除，不能开始')
                pending = db.execute('SELECT 1 FROM control_groups WHERE task_id=? AND state=? LIMIT 1',
                                     (task_id, 'pending')).fetchone()
                terminal = (task['task_status'] in {'complete', 'needs_review', 'failed'}
                            or (task['task_status'] == 'paused' and not pending))
                if terminal:
                    status = task['task_status']
                    if status == 'paused':
                        unresolved = db.execute("SELECT 1 FROM control_groups WHERE task_id=? AND state='unknown' LIMIT 1",
                                                (task_id,)).fetchone()
                        status = 'needs_review' if unresolved or json.loads(task['ungrouped_files']) else 'complete'
                else:
                    if task['task_status'] not in {'ready', 'paused'}:
                        raise ControlError('任务当前不能开始或恢复')
                    if not pending:
                        raise ControlError('没有可处理的候选资料组')
                    status = 'starting'
                if db.execute("SELECT 1 FROM model_decisions WHERE task_id=? AND state='inflight' LIMIT 1",
                              (task_id,)).fetchone():
                    raise ControlError('存在结果未知的模型请求，不能自动重发')
                from .llm_admission_state import counts
                if counts(db, task_id)['inflight']:
                    raise ControlError('LLM 请求仍在收尾，不能开始新一代任务')
                generation = task['start_generation'] + 1
                owner_pid, owner_started = os.getpid(), psutil.Process().create_time()
                existing_run_id, task_name = task['prefect_run_id'], task['name']
                fallback_status = status if terminal else 'paused'
                db.execute("UPDATE tasks SET desired_state='running',task_status=?,start_generation=?,start_pending=1,"
                           "prefect_suspend_pending=0,"
                           "worker_run_id=NULL,worker_owner_pid=NULL,worker_owner_started=NULL,stop_reason=NULL,"
                           "start_owner_pid=?,start_owner_started=?,"
                           'updated_at_ns=?,control_revision=control_revision+1 WHERE task_id=?',
                           (status, generation, owner_pid, owner_started, time.time_ns(), task_id))

            def _finish_start(*, run_id=None, failure=None):
                # A pause or delete can be requested while Prefect is being
                # contacted. Record the run identity for deletion, but never
                # overwrite a newer deletion or another start generation.
                with db:
                    db.execute('BEGIN IMMEDIATE')
                    row = self._task(db, task_id)
                    if row['start_generation'] != generation or not row['start_pending']:
                        raise ControlError('任务启动状态已变化，请重新核对', 409)
                    deleting = (row['task_status'] in {'deleting', 'delete_failed'} or
                                db.execute('SELECT 1 FROM task_deletions WHERE task_id=?',
                                           (task_id,)).fetchone() is not None)
                    if run_id is not None and row['prefect_run_id'] not in (None, run_id):
                        raise ControlError('任务工单绑定已变化，请重新核对', 409)
                    if deleting:
                        state = row['task_status'] if row['task_status'] in {'deleting', 'delete_failed'} else 'deleting'
                        desired = row['desired_state']
                    elif failure:
                        state = 'unknown' if failure == 'unknown' else fallback_status
                        desired = 'paused'
                    else:
                        state, desired = row['task_status'], row['desired_state']
                        if desired == 'paused' and state not in {'complete', 'needs_review', 'failed'}:
                            state = self._stopped_state(db, task_id)[0]
                    db.execute('UPDATE tasks SET prefect_run_id=?,desired_state=?,task_status=?,start_pending=0,'
                               'updated_at_ns=?,control_revision=control_revision+1 WHERE task_id=?',
                               (run_id or row['prefect_run_id'], desired, state, time.time_ns(), task_id))
                return self._task(db, task_id)

            def finish_start(*, run_id=None, failure=None):
                owner = (task_id, generation, owner_pid, owner_started)
                try:
                    result = _finish_start(run_id=run_id, failure=failure)
                except BaseException:
                    _abandoned_starts.add(owner)
                    raise
                _abandoned_starts.discard(owner)
                return result

            if self.scheduler is None:
                finish_start(failure='not_submitted')
                raise ControlError('执行器未配置，任务未启动', 503)
            try:
                run = (self.scheduler.verify_waiting(task_id, existing_run_id) if terminal else
                       self.scheduler.start(task_id, generation, existing_run_id=existing_run_id, name=task_name))
            except SchedulerNotSubmitted:
                finish_start(failure='not_submitted')
                raise ControlError('执行器未连接，任务未启动', 503) from None
            except Exception:
                finish_start(failure='unknown')
                raise ControlError('启动结果未知，未自动重复提交', 503) from None
            row = finish_start(run_id=run.get('flow_run_id'))
            return {'task_id': task_id, 'task_status': row['task_status'],
                    'desired_state': row['desired_state'], 'control_revision': row['control_revision']}

    def pause(self, task_id):
        result = self._pause(task_id)
        result['prefect_queue_status'] = self._sync_order_queue(
            task_id, expected_revision=result['control_revision'])
        return result

    def _pause(self, task_id):
        task_id = _uuid(task_id)
        # Record the newer pause intent immediately, even while start is waiting
        # for Prefect or a model admission gate is temporarily occupied.
        with self._lock('preprocessing', task_id), closing(self.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            task = self._task(db, task_id)
            if task['task_status'] in {'complete', 'needs_review', 'failed'}:
                if _start_is_abandoned(task):
                    db.execute("UPDATE tasks SET desired_state='paused',task_status='unknown',start_pending=0,"
                               "stop_reason=?,updated_at_ns=?,control_revision=control_revision+1 WHERE task_id=?",
                               ('工单启动结果未确认，请核对', time.time_ns(), task_id))
                else:
                    db.execute("UPDATE tasks SET desired_state='paused',updated_at_ns=?,control_revision=control_revision+1 "
                               'WHERE task_id=?', (time.time_ns(), task_id))
                row = self._task(db, task_id)
                pause_status, active, inflight, unknown = self._stopped_state(
                    db, task_id, start_pending=bool(row['start_pending']),
                    old_status=row['task_status'] if row['task_status'] == 'unknown' else None)
                return {'task_id': task_id, 'task_status': row['task_status'],
                        'desired_state': row['desired_state'], 'control_revision': row['control_revision'],
                        'pause_status': pause_status, 'active_groups': active,
                        'inflight_requests': inflight, 'unknown_requests': unknown}
            if task['task_status'] not in {'ready', 'starting', 'running', 'pausing', 'paused', 'unknown'}:
                raise ControlError('任务当前不能暂停')
            self._recover_stale_groups(db, task_id)
            task = self._task(db, task_id)
            if _start_is_abandoned(task):
                db.execute("UPDATE tasks SET task_status='unknown',start_pending=0 WHERE task_id=?", (task_id,))
                task = self._task(db, task_id)
            state, active, inflight, unknown = self._stopped_state(
                db, task_id, start_pending=bool(task['start_pending']), old_status=task['task_status'])
            db.execute('UPDATE tasks SET desired_state=?,task_status=?,updated_at_ns=?,control_revision=control_revision+1 WHERE task_id=?',
                       ('paused', state, time.time_ns(), task_id))
            row = self._task(db, task_id)
            return {'task_id': task_id, 'task_status': state, 'control_revision': row['control_revision'],
                    'active_groups': active, 'inflight_requests': inflight, 'unknown_requests': unknown}
