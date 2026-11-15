"""Isolated candidate for durable folder-level LLM queue state.

The upstream verifier must independently confirm the registered receipt,
delivery, frozen handoff, current retention, and the actual TaskRun and parent
in the selected Prefect. This module does not create tickets, dispatch model
requests, start Prefect, or run postprocessing. It is not a live queue API.
"""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time
from uuid import UUID

from filelock import FileLock

from folder_completion import expected_components, inspect_folder


IDENTITY = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id',
            'parent_run_id', 'task_run_id')


def _identity(value):
    if not isinstance(value, dict) or set(value) != set(IDENTITY):
        raise ValueError('文件夹工单身份无效')
    try:
        canonical = {**value,
                     'task_id': str(UUID(value['task_id'])),
                     'parent_run_id': str(UUID(value['parent_run_id'])),
                     'task_run_id': str(UUID(value['task_run_id']))}
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError('文件夹工单身份无效') from exc
    if (canonical != value or not re.fullmatch(r'[0-9a-f]{32}', str(value['group_id']))
            or not re.fullmatch(r'[0-9a-f]{64}', str(value['receipt_id']))
            or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', str(value['attempt_id']))
            or not isinstance(value['paper_id'], str) or not value['paper_id']):
        raise ValueError('文件夹工单身份无效')
    return canonical


def _manifest(identity, verify_upstream):
    if not callable(verify_upstream):
        raise ValueError('上游工单核验不可用')
    manifest = verify_upstream(dict(identity))
    if (not isinstance(manifest, dict)
            or manifest.get('schema') != 'paperflow-local-preprocessing-handoff/1'
            or any(manifest.get(key) != identity[key] for key in
                   ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'))):
        raise ValueError('文件夹交接身份不一致')
    expected_components(manifest, identity['task_run_id'])
    try:
        encoded = json.dumps(manifest, ensure_ascii=False, sort_keys=True,
                             separators=(',', ':'), allow_nan=False).encode('utf8')
    except (TypeError, ValueError) as exc:
        raise ValueError('文件夹交接内容无效') from exc
    return manifest, hashlib.sha256(encoded).hexdigest()


class FolderLedger:
    def __init__(self, database, lock_root):
        self.database = Path(database).resolve(strict=True)
        self.lock_root = Path(lock_root).resolve(strict=True)

    def _connect(self):
        db = sqlite3.connect(self.database, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    def _lock(self, identity):
        return FileLock(str(self.lock_root / ('preprocessing-' + identity['task_id'] + '.lock')), timeout=30)

    def initialize(self):
        with closing(self._connect()) as db, db:
            db.execute('''CREATE TABLE IF NOT EXISTS llm_folder_queue (
                task_id TEXT NOT NULL, group_id TEXT NOT NULL, paper_id TEXT NOT NULL,
                receipt_id TEXT NOT NULL, attempt_id TEXT NOT NULL, parent_run_id TEXT NOT NULL,
                task_run_id TEXT NOT NULL UNIQUE, manifest_sha256 TEXT NOT NULL,
                state TEXT NOT NULL, generation INTEGER, updated_at_ns INTEGER NOT NULL,
                PRIMARY KEY(task_id, group_id)
            )''')

    @staticmethod
    def _check_control(db, identity, *, generation=None):
        task = db.execute('SELECT desired_state,start_generation,start_pending,task_status FROM tasks '
                          'WHERE task_id=?', (identity['task_id'],)).fetchone()
        deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (identity['task_id'],)).fetchone()
        if not task or deleted or task['task_status'] in {'deleting', 'delete_failed'}:
            raise InterruptedError('任务已删除或状态不可核验')
        if generation is not None and (type(generation) is not int or generation < 0
                or task['desired_state'] != 'running' or task['start_generation'] != generation
                or task['start_pending']):
            raise InterruptedError('任务已暂停或代次发生变化')

    @staticmethod
    def _check_row(db, identity, digest):
        row = db.execute('SELECT * FROM llm_folder_queue WHERE task_id=? AND group_id=?',
                         (identity['task_id'], identity['group_id'])).fetchone()
        if not row or any(row[key] != identity[key] for key in IDENTITY) or row['manifest_sha256'] != digest:
            raise ValueError('文件夹工单或交接已发生变化')
        if row['state'] not in {'waiting_llm', 'llm_processing', 'waiting_postprocessing'}:
            raise ValueError('文件夹工单状态不可核验')
        return row

    def register(self, identity, verify_upstream):
        identity = _identity(identity)
        # Receipt hashing and Prefect reads can be slow; the pause gate must
        # cover only the final control check and the local transaction.
        _, digest = _manifest(identity, verify_upstream)
        with self._lock(identity):
            with closing(self._connect()) as db, db:
                db.execute('BEGIN IMMEDIATE')
                self._check_control(db, identity)
                values = tuple(identity[key] for key in IDENTITY)
                db.execute('INSERT OR IGNORE INTO llm_folder_queue '
                           '(task_id,group_id,paper_id,receipt_id,attempt_id,parent_run_id,task_run_id,'
                           'manifest_sha256,state,generation,updated_at_ns) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                           values + (digest, 'waiting_llm', None, time.time_ns()))
                return dict(self._check_row(db, identity, digest))

    def begin(self, identity, generation, verify_upstream):
        identity = _identity(identity)
        if type(generation) is not int or generation < 0:
            raise InterruptedError('任务代次不可核验')
        active = getattr(verify_upstream, 'for_execution', None)
        if not callable(active):
            raise ValueError('缺少运行中的大工单和小工单核验')
        _, digest = _manifest(identity, active)
        with self._lock(identity):
            with closing(self._connect()) as db, db:
                db.execute('BEGIN IMMEDIATE')
                self._check_control(db, identity, generation=generation)
                row = self._check_row(db, identity, digest)
                if row['state'] == 'waiting_postprocessing':
                    raise ValueError('文件夹已进入等待后处理')
                db.execute("UPDATE llm_folder_queue SET state='llm_processing',generation=?,updated_at_ns=? "
                           'WHERE task_id=? AND group_id=?',
                           (generation, time.time_ns(), identity['task_id'], identity['group_id']))
                return dict(self._check_row(db, identity, digest))

    def finish(self, identity, verify_upstream, response_verifier):
        identity = _identity(identity)
        if not callable(getattr(response_verifier, 'complete', None)):
            raise ValueError('原始响应核验不可用')
        active = getattr(verify_upstream, 'for_execution', None)
        if not callable(active):
            raise ValueError('缺少运行中的大工单和小工单核验')
        manifest, digest = _manifest(identity, active)
        with closing(self._connect()) as db:
            row = self._check_row(db, identity, digest)
            if row['state'] == 'waiting_llm':
                raise ValueError('文件夹尚未进入 LLM 处理')
            self._check_control(db, identity, generation=row['generation'])
            admissions = [dict(item) for item in db.execute(
                'SELECT * FROM llm_request_admissions WHERE task_id=? AND task_run_id=? ORDER BY component_id',
                (identity['task_id'], identity['task_run_id'])).fetchall()]
        result = inspect_folder(manifest, identity['task_run_id'], admissions,
                                lambda item: response_verifier.complete(
                                    item['task_id'], item['component_id'], item['response_sha256']))
        if result['status'] != 'raw_complete' and row['state'] == 'waiting_postprocessing':
            raise ValueError('待后处理的原始响应证据不完整')
        with self._lock(identity):
            with closing(self._connect()) as db, db:
                db.execute('BEGIN IMMEDIATE')
                latest_row = self._check_row(db, identity, digest)
                self._check_control(db, identity, generation=latest_row['generation'])
                latest = [dict(item) for item in db.execute(
                    'SELECT * FROM llm_request_admissions WHERE task_id=? AND task_run_id=? ORDER BY component_id',
                    (identity['task_id'], identity['task_run_id'])).fetchall()]
                if latest != admissions:
                    raise ValueError('原始响应准入证据发生变化')
                if result['status'] != 'raw_complete':
                    return dict(latest_row)
                db.execute("UPDATE llm_folder_queue SET state='waiting_postprocessing',updated_at_ns=? "
                           'WHERE task_id=? AND group_id=?',
                           (time.time_ns(), identity['task_id'], identity['group_id']))
                return dict(self._check_row(db, identity, digest))

    def counts(self, task_id):
        task_id = str(UUID(task_id))
        with closing(self._connect()) as db:
            db.execute('BEGIN')
            phases = dict(db.execute('SELECT state,COUNT(*) FROM llm_folder_queue '
                                     'WHERE task_id=? GROUP BY state', (task_id,)).fetchall())
            active = dict(db.execute("SELECT a.kind,COUNT(DISTINCT a.task_run_id) FROM llm_request_admissions a "
                    "JOIN llm_folder_queue q ON q.task_id=a.task_id AND q.task_run_id=a.task_run_id "
                    "WHERE a.task_id=? AND q.state='llm_processing' "
                    "AND a.state IN ('admitted','returned_unverified') GROUP BY a.kind", (task_id,)).fetchall())
            unknown_by_kind = dict(db.execute("SELECT a.kind,COUNT(*) FROM llm_request_admissions a JOIN llm_folder_queue q "
                    "ON q.task_id=a.task_id AND q.task_run_id=a.task_run_id "
                    "WHERE a.task_id=? AND a.state='unknown' GROUP BY a.kind", (task_id,)).fetchall())
        if (set(phases) - {'waiting_llm', 'llm_processing', 'waiting_postprocessing'}
                or (set(active) | set(unknown_by_kind)) - {'body', 'attachment'}):
            raise ValueError('文件夹工单状态不可核验')
        return {'waiting_llm': phases.get('waiting_llm', 0),
                'body_folders_in_progress': None if unknown_by_kind.get('body') else active.get('body', 0),
                'attachment_folders_in_progress': None if unknown_by_kind.get('attachment') else active.get('attachment', 0),
                'waiting_postprocessing': phases.get('waiting_postprocessing', 0),
                'unknown_requests': sum(unknown_by_kind.values())}
