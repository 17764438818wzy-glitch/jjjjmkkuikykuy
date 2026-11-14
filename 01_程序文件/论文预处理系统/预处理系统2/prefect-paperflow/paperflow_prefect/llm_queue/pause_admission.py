"""Candidate model admission coordinated with the upstream task pause lock.

No database changes are made until initialize() is called explicitly.
An admitted request left by a crash remains unresolved and is never reissued.
"""
from contextlib import closing, contextmanager
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time
from uuid import UUID

from filelock import FileLock

from .work_locks import work_locks


def _digest(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{64}', value):
        raise ValueError('模型请求身份无效')
    return value


def _stage_is_executing(db, job_id, task_id, generation, folder_run_id=None):
    if job_id is None:
        return False
    return bool(db.execute("SELECT 1 FROM paperflow_llm_stage_jobs WHERE job_id=? AND task_id=? "
                           "AND generation=? AND state='executing' AND (? IS NULL OR folder_run_id=?)",
                           (job_id, task_id, generation, folder_run_id, folder_run_id)).fetchone())


class ControlStop:
    """Stop new stages while letting an already claimed stage finish."""
    def __init__(self, database, task_id, generation, *, execution_task_run_id=None):
        self.database = Path(database).resolve(strict=True)
        self.task_id = str(UUID(task_id))
        self.generation = generation
        self.execution_task_run_id = execution_task_run_id

    def is_set(self):
        try:
            with closing(sqlite3.connect(self.database.as_uri() + '?mode=ro', uri=True, timeout=3)) as db:
                row = db.execute('SELECT desired_state,start_generation,start_pending,task_status FROM tasks '
                                 'WHERE task_id=?', (self.task_id,)).fetchone()
                deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (self.task_id,)).fetchone()
                if not row or deleted or row[3] in {'deleting', 'delete_failed'}:
                    return True
                return ((row[0] != 'running' or row[1] != self.generation or bool(row[2]))
                        and not _stage_is_executing(db, self.execution_task_run_id, self.task_id, self.generation))
        except (OSError, sqlite3.Error):
            return True

    def wait(self, seconds=None):
        deadline = None if seconds is None else time.monotonic() + max(0, seconds)
        while not self.is_set():
            if deadline is not None and time.monotonic() >= deadline:
                return False
            time.sleep(0.05 if deadline is None else min(0.05, max(0, deadline - time.monotonic())))
        return True


class _AdmissionToken:
    """Serialize the instant a request becomes in flight against pause."""
    def __init__(self, guard, task_id, generation, component_id, task_run_id, gate):
        self.guard, self.task_id = guard, task_id
        self.generation, self.component_id = generation, component_id
        self.task_run_id = task_run_id
        self.gate = gate

    def start_transport(self, mark_started):
        if not callable(mark_started):
            raise ValueError('模型发送标记不可用')
        if not self.gate.is_locked:
            raise ValueError('模型发送准入已释放')
        with closing(self.guard._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            task = db.execute('SELECT desired_state,start_generation,start_pending,task_status FROM tasks '
                              'WHERE task_id=?', (self.task_id,)).fetchone()
            deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (self.task_id,)).fetchone()
            request = db.execute('SELECT state,generation FROM llm_request_admissions '
                                 'WHERE task_id=? AND component_id=?',
                                 (self.task_id, self.component_id)).fetchone()
            running = task and task['desired_state'] == 'running' and task['start_generation'] == self.generation and not task['start_pending']
            if (not task or deleted or (not running and not _stage_is_executing(db, self.guard.execution_task_run_id,
                        self.task_id, self.generation, self.task_run_id))
                    or task['task_status'] in {'deleting', 'delete_failed'}
                    or not request or request['state'] != 'admitted' or request['generation'] != self.generation):
                raise InterruptedError('任务已暂停，未启动新的模型请求')
            mark_started()
            db.commit()
        # The HTTP open and entire response happen without the task gate.
        self.gate.release()


class AdmissionGuard:
    def __init__(self, database, lock_root, *, execution_task_run_id=None):
        self.database = Path(database).resolve(strict=True)
        self.lock_root = Path(lock_root).resolve(strict=True)
        self.execution_task_run_id = execution_task_run_id

    def _connect(self):
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def initialize(self):
        with closing(self._connect()) as db, db:
            db.execute('''CREATE TABLE IF NOT EXISTS llm_request_admissions (
                task_id TEXT NOT NULL, component_id TEXT NOT NULL, generation INTEGER NOT NULL,
                request_sha256 TEXT NOT NULL, state TEXT NOT NULL, response_sha256 TEXT,
                kind TEXT NOT NULL, work_dir TEXT NOT NULL, model_dir TEXT NOT NULL, receipt_id TEXT NOT NULL,
                task_run_id TEXT NOT NULL, source_sha256 TEXT NOT NULL,
                created_at_ns INTEGER NOT NULL, updated_at_ns INTEGER NOT NULL,
                PRIMARY KEY(task_id, component_id)
            )''')

    @staticmethod
    def counts(db, task_id):
        rows = dict(db.execute('SELECT state,COUNT(*) FROM llm_request_admissions '
                               'WHERE task_id=? GROUP BY state', (task_id,)).fetchall())
        return {'inflight': rows.get('admitted', 0) + rows.get('returned_unverified', 0),
                'unknown': rows.get('unknown', 0)}

    @contextmanager
    def admit(self, task_id, generation, component_id, request_sha256, *, kind, model_dir,
              receipt_id, task_run_id, source_sha256, work_dir=None):
        task_id = str(UUID(task_id))
        _digest(component_id)
        _digest(request_sha256)
        _digest(receipt_id)
        _digest(source_sha256)
        task_run_id = str(UUID(task_run_id))
        model_dir = Path(model_dir).resolve()
        work_dir = Path(work_dir).resolve() if work_dir is not None else model_dir.parent
        root_model = work_dir / 'model'
        is_root = model_dir == root_model
        is_body_part = (kind == 'body' and model_dir.parent == root_model
                        and re.fullmatch(r'part-\d{3,}', model_dir.name) is not None)
        if kind not in {'body', 'attachment'} or not (is_root or is_body_part):
            raise ValueError('模型工作目录或类型无效')
        if type(generation) is not int or generation < 0:
            raise ValueError('任务代次无效')
        lock = self.lock_root / ('preprocessing-' + task_id + '.lock')
        gate = FileLock(str(lock), timeout=30)
        with gate:
            with closing(self._connect()) as db, db:
                db.execute('BEGIN IMMEDIATE')
                row = db.execute('SELECT desired_state,start_generation,start_pending,task_status FROM tasks '
                                 'WHERE task_id=?', (task_id,)).fetchone()
                deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone()
                running = row and row['desired_state'] == 'running' and row['start_generation'] == generation and not row['start_pending']
                if (not row or deleted or (not running and not _stage_is_executing(db, self.execution_task_run_id,
                            task_id, generation, task_run_id))
                        or row['task_status'] in {'deleting', 'delete_failed'}):
                    raise InterruptedError('任务已暂停、删除或代次发生变化')
                if db.execute('SELECT 1 FROM llm_request_admissions WHERE task_id=? AND component_id=?',
                              (task_id, component_id)).fetchone():
                    raise ValueError('已有请求准入记录，禁止自动重复发送')
                now = time.time_ns()
                db.execute('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                           (task_id, component_id, generation, request_sha256, 'admitted', None,
                            kind, str(work_dir), str(model_dir), receipt_id, task_run_id, source_sha256, now, now))
            try:
                # Retain the gate across short local request-intent writes.
                # start_transport releases it immediately before the HTTP open.
                yield _AdmissionToken(self, task_id, generation, component_id, task_run_id, gate)
            except BaseException:
                self._mark(task_id, component_id, 'admitted', 'unknown')
                raise
            else:
                self._mark(task_id, component_id, 'admitted', 'returned_unverified')

    def _mark(self, task_id, component_id, before, after):
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            result = db.execute('UPDATE llm_request_admissions SET state=?,updated_at_ns=? '
                                'WHERE task_id=? AND component_id=? AND state=?',
                                (after, time.time_ns(), task_id, component_id, before))
            if result.rowcount != 1:
                raise ValueError('模型准入记录发生变化')

    def complete(self, task_id, component_id, response_sha256):
        task_id = str(UUID(task_id))
        _digest(component_id)
        _digest(response_sha256)
        with closing(self._connect()) as db:
            row = db.execute('SELECT * FROM llm_request_admissions WHERE task_id=? AND component_id=?',
                             (task_id, component_id)).fetchone()
        if not row:
            raise ValueError('模型准入证据不存在')
        model, kind = Path(row['model_dir']), row['kind']
        work = Path(row['work_dir'])
        root_model = work / 'model'
        if not (model == root_model or (kind == 'body' and model.parent == root_model
                                        and re.fullmatch(r'part-\d{3,}', model.name))):
            raise ValueError('模型响应证据目录不匹配')
        if not work.is_dir() or not model.is_dir():
            raise ValueError('模型响应证据不存在')
        with work_locks(work, kind):
            try:
                from preextract_scheduler_adapter import request_observation
                phase, step, proof = request_observation(model)
                state = json.loads((model / 'request-state.json').read_text(encoding='utf8'))
                events = model / 'response-events.jsonl'
                response = model / 'response.txt'
                with response.open('rb') as stream:
                    actual_response = hashlib.file_digest(stream, 'sha256').hexdigest()
            except (OSError, ValueError, KeyError, TypeError) as exc:
                raise ValueError('模型响应证据不可核验') from exc
            if (phase != 'response_complete' or step != 'postprocess' or not events.is_file()
                    or state.get('status') != 'response_complete' or state.get('finish_reason') != 'stop'
                    or proof.get('request_sha256') != row['request_sha256']
                    or proof.get('response_sha256') != response_sha256 or actual_response != response_sha256):
                raise ValueError('模型响应证据不完整或身份不一致')
            with closing(self._connect()) as db, db:
                db.execute('BEGIN IMMEDIATE')
                latest = db.execute('SELECT * FROM llm_request_admissions WHERE task_id=? AND component_id=?',
                                    (task_id, component_id)).fetchone()
                if (not latest or any(latest[key] != row[key] for key in
                        ('work_dir', 'model_dir', 'kind', 'request_sha256', 'receipt_id', 'task_run_id', 'source_sha256'))):
                    raise ValueError('模型准入证据发生变化')
                if latest['state'] == 'raw_complete' and latest['response_sha256'] == response_sha256:
                    return
                if latest['state'] not in {'admitted', 'returned_unverified', 'unknown'}:
                    raise ValueError('模型准入证据状态不匹配')
                db.execute("UPDATE llm_request_admissions SET state='raw_complete',response_sha256=?,updated_at_ns=? "
                           'WHERE task_id=? AND component_id=?',
                           (response_sha256, time.time_ns(), task_id, component_id))
