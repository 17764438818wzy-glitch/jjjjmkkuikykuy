"""Explicit, folder-scoped retries for the existing new-Prefect LLM stage.

Loading this module and starting the HTTP service never dispatches a model
request. Only a separately journaled user command can do that. The original
folder FlowRun, task generation, request admissions and completed responses
are retained.
"""
from __future__ import annotations

from contextlib import closing, contextmanager, nullcontext
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import threading
import time
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import httpx
from pydantic import BaseModel, Field


ROOT = Path(__file__).resolve().parents[2]
PROJECT = ROOT / 'prefect-paperflow'
QUEUE_DIR = ROOT / 'candidates' / 'local-intake-live-link-20261031'
sys.path.insert(0, str(PROJECT))
sys.path.insert(0, str(QUEUE_DIR))

from filelock import FileLock, Timeout
from prefect.client.orchestration import SyncPrefectClient
from prefect.client.schemas import OrchestrationResult
from prefect.client.schemas.responses import SetStateStatus
from prefect.exceptions import ObjectNotFound
from prefect.states import Scheduled, to_state_create
from paperflow_prefect.attachment_queue import settings_for_verified_receipt
from paperflow_prefect.local_preprocessing import FIRST_STEP_CONFIG, NAMESPACE, _control_paths, _queue
from paperflow_prefect.llm_queue.folder_completion import expected_components, inspect_folder
from paperflow_prefect.llm_queue.handoff_contract import IDENTITY_KEYS, load_frozen_handoff
from paperflow_prefect.llm_queue.isolated_stage import (
    BLOCKING_STATES, TAG, STAGE_TASK_KEY, _binding, _folder_for_job, _folder_paused, _identity,
    _job_id, _validate_stage_run, execute_llm_stage,
)
from paperflow_prefect.llm_queue.pause_admission import AdmissionGuard, ControlStop
from paperflow_prefect.llm_queue.upstream_flow_binding import MARKER
from paperflow_prefect.records import read_json
from paperflow_prefect.registered_input import verify_registered_receipt
from paperflow_prefect.request_admission import require_model_history
from paperflow_prefect.settings import Settings
from paperflow_prefect.unified import UnifiedBackend
from prefect_orders import FolderOrderService


ACTIVE_ITEMS = ('queued', 'checking', 'recovering', 'guard_writing', 'waiting_folder', 'recovered', 'stage_checking', 'scheduling')
JOURNAL_FILES = ('request.json', 'request-state.json', 'response.txt',
                 'response-events.jsonl', 'http-error.txt')
# Reviewed new-Prefect folder flow: completed handoffs return to suspension;
# the older implementation at the same entrypoint called the LLM inline.
APPROVED_FOLDER_FLOW_SHA256 = '1dd1de4571e6ff86cf7c39e1c04735cc65abae69abb21d4f9ed81bb20297ccd5'
APPROVED_POOL_TEMPLATE_SHA256 = '37048982ac71d06e981c692b0b95142eefe543d41e5b78d969a36da9122b27a2'


class RetryRequest(BaseModel):
    request_id: UUID
    scope: str
    task_id: UUID | None = None
    group_id: str | None = Field(default=None, pattern=r'^[0-9a-f]{32}$')
    paper_id: str | None = Field(default=None, min_length=1, max_length=128)
    receipt_id: str | None = Field(default=None, pattern=r'^[0-9a-f]{64}$')
    attempt_id: str | None = Field(default=None, pattern=r'^[A-Za-z0-9_.:-]{1,128}$')


def _connect(path):
    db = sqlite3.connect(path, timeout=30)
    db.row_factory = sqlite3.Row
    return db


def _error(exc):
    # Do not leak source paths or response bodies to the browser.
    return str(exc) if isinstance(exc, RetryStopped) else '身份或历史证据暂不可核验'


class RetryStopped(Exception):
    pass


@contextmanager
def _pinned_folder_flow():
    """Keep the reviewed flow readable but prevent replacement during recovery."""
    if os.name != 'nt':
        raise RetryStopped('无法固定当前文件夹工单代码版本，暂不恢复')
    import ctypes
    from ctypes import wintypes
    create = ctypes.windll.kernel32.CreateFileW
    create.argtypes = (wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
                       wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE)
    create.restype = wintypes.HANDLE
    close = ctypes.windll.kernel32.CloseHandle
    close.argtypes = (wintypes.HANDLE,)
    close.restype = wintypes.BOOL
    source = PROJECT / 'paperflow_prefect' / 'local_preprocessing.py'
    # Only share reads: another process cannot modify, delete, or replace the
    # reviewed module while the worker loads it.
    handle = create(str(source), 0x80000000, 0x00000001, None, 3, 0x80, None)
    if not handle or handle == ctypes.c_void_p(-1).value:
        raise RetryStopped('文件夹工单代码正在修改，暂不恢复')
    try:
        if hashlib.sha256(source.read_bytes()).hexdigest() != APPROVED_FOLDER_FLOW_SHA256:
            raise RetryStopped('文件夹工单代码版本已变化，暂不恢复，请先核对')
        yield
    finally:
        close(handle)


class RetryService:
    def __init__(self):
        self.database, self.control_locks = _control_paths()
        self.service_lock = FileLock(self.control_locks / 'llm-manual-retry-service.lock', timeout=0,
                                     thread_local=False)
        self.service_lock.acquire()
        self.settings = Settings({**Settings.load().values, 'execution_enabled': True})
        config = read_json(FIRST_STEP_CONFIG)
        self.api = str(self.settings['prefect_api_url']).rstrip('/')
        if not self.api or self.api != str(config['prefect_api_url']).rstrip('/'):
            raise RuntimeError('新 Prefect 身份不可核验')
        self.queue = _queue()
        self.observer = FolderOrderService(
            self.queue, prefect_api_url=self.api, first_step_index=self.database,
            first_step_data_root=config['data_root'], first_step_receipt_root=config['receipt_root'])
        self.guard = AdmissionGuard(self.database, self.control_locks)
        self.gate = threading.Lock()
        self._initialize()

    def _initialize(self):
        with closing(_connect(self.database)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('''CREATE TABLE IF NOT EXISTS paperflow_llm_retry_commands (
                request_id TEXT PRIMARY KEY, request_json TEXT NOT NULL,
                state TEXT NOT NULL, message TEXT, created_at_ns INTEGER NOT NULL,
                updated_at_ns INTEGER NOT NULL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS paperflow_llm_retry_items (
                request_id TEXT NOT NULL, task_id TEXT NOT NULL, group_id TEXT NOT NULL,
                paper_id TEXT NOT NULL, receipt_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
                folder_label TEXT, paper_title TEXT,
                folder_run_id TEXT, job_id TEXT, generation INTEGER, stage_mode TEXT,
                old_flow_state_id TEXT, old_stage_state_id TEXT, old_job_json TEXT,
                state TEXT NOT NULL, message TEXT, updated_at_ns INTEGER NOT NULL,
                PRIMARY KEY(request_id,task_id,group_id))''')
            db.execute("CREATE UNIQUE INDEX IF NOT EXISTS paperflow_llm_retry_active "
                       "ON paperflow_llm_retry_items(task_id,group_id) "
                       "WHERE state IN ('queued','checking','recovering','guard_writing','waiting_folder','recovered','stage_checking','scheduling')")
            # A process restart cannot establish whether an external state
            # transition was accepted. Preserve every item for readback.
            db.execute("UPDATE paperflow_llm_retry_commands SET state='interrupted',"
                       "message='服务重启，原操作状态待核对',updated_at_ns=? "
                       "WHERE state IN ('queued','running')", (time.time_ns(),))
            db.execute("UPDATE paperflow_llm_stage_jobs SET state='stopped_before_submit',updated_at_ns=? "
                "WHERE state='recovering_folder' AND job_id IN "
                "(SELECT job_id FROM paperflow_llm_retry_items WHERE state IN ('recovering','recovered'))", (time.time_ns(),))
            db.execute("UPDATE paperflow_llm_stage_jobs SET state='recovery_unknown',updated_at_ns=? "
                "WHERE state='recovering_folder' AND job_id IN "
                "(SELECT job_id FROM paperflow_llm_retry_items WHERE state='waiting_folder')", (time.time_ns(),))
            db.execute("UPDATE paperflow_llm_stage_jobs SET state='identity_unknown',updated_at_ns=? "
                "WHERE state='recovering_folder' AND job_id IN "
                "(SELECT job_id FROM paperflow_llm_retry_items WHERE state IN ('guard_writing','stage_checking'))", (time.time_ns(),))
            db.execute("UPDATE paperflow_llm_retry_items SET state='not_sent',"
                       "message='原文件夹身份已保留，本次 LLM 阶段未提交，可重新请求',updated_at_ns=? "
                       "WHERE state='recovered'", (time.time_ns(),))
            db.execute("UPDATE paperflow_llm_retry_items SET state='needs_review',"
                       "message='服务重启，原操作状态待核对，请勿重复提交',updated_at_ns=? "
                       "WHERE state IN ('queued','checking','recovering','guard_writing','waiting_folder','stage_checking','scheduling')",
                       (time.time_ns(),))

    def _client(self):
        return SyncPrefectClient(self.api, httpx_settings={'timeout': 3, 'trust_env': False})

    def submit(self, body):
        request = body.model_dump(mode='json')
        if body.scope not in {'one', 'all'}:
            raise HTTPException(400, '重试范围无效')
        keys = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')
        if (body.scope == 'one' and any(not request[k] for k in keys)) or (
                body.scope == 'all' and any(request[k] is not None for k in keys)):
            raise HTTPException(400, '文件夹身份不完整或范围冲突')
        encoded = json.dumps(request, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        now = time.time_ns()
        with closing(_connect(self.database)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            previous = db.execute('SELECT request_json FROM paperflow_llm_retry_commands WHERE request_id=?',
                                  (str(body.request_id),)).fetchone()
            if previous:
                if previous['request_json'] != encoded:
                    raise HTTPException(409, '请求编号已用于其他操作')
            else:
                db.execute('INSERT INTO paperflow_llm_retry_commands VALUES(?,?,?,?,?,?)',
                           (str(body.request_id), encoded, 'queued', '等待核对失败记录', now, now))
        if not previous:
            threading.Thread(target=self._run, args=(str(body.request_id), request),
                             name='llm-manual-retry', daemon=True).start()
        return self.status(str(body.request_id))

    def status(self, request_id):
        try:
            request_id = str(UUID(request_id))
        except (ValueError, TypeError):
            raise HTTPException(400, '请求编号无效') from None
        with closing(_connect(self.database)) as db:
            command = db.execute('SELECT request_id,state,message FROM paperflow_llm_retry_commands '
                                 'WHERE request_id=?', (request_id,)).fetchone()
            if not command:
                raise HTTPException(404, '重试记录不存在')
            items = [dict(row) for row in db.execute(
                'SELECT i.task_id,i.group_id,i.paper_id,i.receipt_id,i.attempt_id,'
                'i.folder_label,i.paper_title,i.state,i.message,j.state AS stage_state '
                'FROM paperflow_llm_retry_items i LEFT JOIN paperflow_llm_stage_jobs j ON j.job_id=i.job_id '
                'WHERE i.request_id=? ORDER BY i.task_id,i.group_id', (request_id,))]
        for item in items:
            stage = item.pop('stage_state')
            if item['state'] == 'needs_review' and stage == 'recovery_unknown':
                item['message'] = '原文件夹历史状态待核对；本次 LLM 阶段尚未提交，请勿重复操作'
            if item['state'] == 'submitted':
                states = {
                    'reserved': ('queued', '已进入 LLM 等待队列，尚未提交 Prefect'),
                    'submitting': ('pending', '正在提交 LLM 阶段'),
                    'submitted': ('submitted', '已提交 Prefect，等待执行'),
                    'executing': ('submitted', 'LLM 阶段正在执行'),
                    'finished': ('completed', '原始回复已登记，等待后处理'),
                    'failed': ('failed', '本次 LLM 阶段失败，请查看失败详情'),
                    'stopped': ('paused', '任务已暂停；此前已完成的部分保留'),
                    'stopped_before_submit': ('not_sent', '本次 LLM 阶段未提交'),
                }
                item['state'], item['message'] = states.get(stage, ('needs_review', '阶段状态待核对，请勿重复提交'))
        return {**dict(command), 'items': items}

    def history(self, limit, offset):
        """Expose persisted command history without creating or replaying work."""
        with closing(_connect(self.database)) as db:
            db.execute('BEGIN')
            total = db.execute('SELECT COUNT(*) FROM paperflow_llm_retry_commands').fetchone()[0]
            rows = [dict(row) for row in db.execute(
                'SELECT request_id,created_at_ns FROM paperflow_llm_retry_commands '
                'ORDER BY created_at_ns DESC,request_id DESC LIMIT ? OFFSET ?', (limit, offset))]
        commands = []
        for row in rows:
            result = self.status(row['request_id'])
            result['started_ms'] = row['created_at_ns'] // 1_000_000
            commands.append(result)
        return {'total': total, 'limit': limit, 'offset': offset, 'commands': commands}

    def _recovery_loop(self):
        while True:
            try:
                self._reconcile_recoveries()
            except Exception:
                # No automatic retry or state clearing follows an error.
                pass
            time.sleep(10)

    def _reconcile_recoveries(self):
        """Observe one earlier explicit command; never submit any LLM work."""
        if not self.gate.acquire(blocking=False):
            return
        try:
            with closing(_connect(self.database)) as db:
                rows = [dict(row) for row in db.execute("""
                    SELECT j.*,i.state AS retry_state,i.old_flow_state_id,i.folder_label,i.paper_title
                      FROM paperflow_llm_stage_jobs j JOIN paperflow_llm_retry_items i
                        ON i.job_id=j.job_id AND i.request_id=j.retry_key
                     WHERE j.state='recovery_unknown' AND i.state='needs_review'
                     ORDER BY j.created_at_ns
                """)]
            with self._client() as client:
                for job in rows:
                    try:
                        self._reconcile_recovery(client, job)
                    except Exception:
                        # A missing or changing fact leaves this folder fenced.
                        continue
        finally:
            self.gate.release()

    def _reconcile_recovery(self, client, job):
        item = {k: job[k] for k in ('task_id','group_id','paper_id','receipt_id','attempt_id')}
        lock = self.settings.path('data_root') / 'locks' / ('folder-order-' + item['attempt_id'] + '.lock')
        with FileLock(lock, timeout=0, thread_local=False):
            with closing(_connect(self.database)) as db:
                task = db.execute('SELECT * FROM tasks WHERE task_id=?', (item['task_id'],)).fetchone()
                deleted = bool(db.execute('SELECT 1 FROM task_deletions WHERE task_id=?',
                                          (item['task_id'],)).fetchone())
            if not task:
                return
            folder = self._prefect_identity(client, item, dict(task), job['folder_run_id'],
                                            allow_recovering=deleted or task['desired_state'] == 'paused',
                                            allow_deleting=deleted)
            if ((folder.parameters or {}).get('retry_guard') != job['retry_key']
                    or not folder.state or str(folder.state.id) == job['old_flow_state_id']):
                return
            # A direct single POST uses this persisted transition ID. A
            # positive history match proves that attempt was consumed; a
            # missing history entry never proves non-submission.
            states = client.read_flow_run_states(UUID(job['folder_run_id']))
            matches = [state for state in states if state.is_scheduled() and
                       str(state.state_details.transition_id or '') == job['retry_key']]
            if len(matches) != 1:
                return
            suspended = (folder.state.is_paused() and folder.state.state_details.pause_reschedule is True
                and str(folder.state.state_details.pause_key or '').startswith('llm-retry-' + job['retry_key'] + '-'))
            waiting_paused = (not deleted and task['desired_state'] == 'paused' and folder.state.is_scheduled())
            if not suspended and not deleted and not waiting_paused:
                return
            if deleted and not (suspended or folder.state.is_scheduled() or folder.state.is_pending()
                                or folder.state.is_running() or folder.state.is_failed()
                                or folder.state.is_crashed() or folder.state.is_cancelled()):
                return
            try:
                client.read_task_run(UUID(job['job_id']))
            except ObjectNotFound:
                pass
            else:
                return
            if not deleted and suspended and not any((folder.name or '').endswith(phase) for phase in
                    ('等待 LLM', '等待任务开始', 'LLM 失败待核对')):
                return
            with FileLock(self.control_locks / ('preprocessing-' + item['task_id'] + '.lock'), timeout=10):
                with closing(_connect(self.database)) as db, db:
                    db.execute('BEGIN IMMEDIATE')
                    marker = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (item['task_id'],)).fetchone()
                    if bool(marker) != deleted:
                        return
                    changed = db.execute("UPDATE paperflow_llm_stage_jobs SET state='stopped_before_submit',"
                        "error_type='RecoveryConfirmedNoStage',updated_at_ns=? WHERE job_id=? "
                        "AND retry_key=? AND state='recovery_unknown'", (time.time_ns(), job['job_id'],
                                                                            job['retry_key'])).rowcount
                    if changed != 1:
                        return
                    updated = db.execute("UPDATE paperflow_llm_retry_items SET state='not_sent',message=?,updated_at_ns=? "
                        "WHERE request_id=? AND job_id=? AND state='needs_review'",
                        ('本次 LLM 阶段未提交，原工单等待任务恢复后挂起' if waiting_paused else
                         '原工单恢复已核对，本次 LLM 阶段未提交' if deleted else
                         '原工单恢复已核对，本次 LLM 阶段未提交，可重新请求',
                         time.time_ns(), job['retry_key'], job['job_id'])).rowcount
                    if updated != 1:
                        raise ValueError('恢复记录已变化')

    def _command(self, request_id, state, message):
        with closing(_connect(self.database)) as db, db:
            db.execute('UPDATE paperflow_llm_retry_commands SET state=?,message=?,updated_at_ns=? '
                       'WHERE request_id=?', (state, message, time.time_ns(), request_id))

    def _item(self, request_id, item, state, message):
        with closing(_connect(self.database)) as db, db:
            db.execute('UPDATE paperflow_llm_retry_items SET state=?,message=?,updated_at_ns=? '
                       'WHERE request_id=? AND task_id=? AND group_id=?',
                       (state, message, time.time_ns(), request_id, item['task_id'], item['group_id']))

    def _failed_snapshot(self, scope):
        # Normal upstream scans briefly mark the snapshot incomplete.  Let the
        # same user command continue after that scan instead of failing every
        # folder merely because the button was clicked during the scan.
        deadline = time.monotonic() + 10
        while True:
            snapshot = self.queue.snapshot(max_age_seconds=60)
            if (snapshot.get('receiver_status') != 'running'
                    or snapshot.get('upstream_scan_status') != 'incomplete'):
                break
            if time.monotonic() >= deadline:
                raise RetryStopped('上游扫描尚未完成，本次未提交')
            time.sleep(0.5)
        if (snapshot.get('receiver_status') != 'running'
                or snapshot.get('upstream_scan_verified') is not True
                or not self.observer._source_matches(snapshot)):
            raise RetryStopped('上游接收状态待核对，本次未提交')
        tasks = self.observer._result_tasks()
        values = self.observer._build_results(snapshot, tasks)
        if (not values.get('llm_available') or (scope == 'all' and
                (values.get('partial') or values.get('llm_partial')
                 or snapshot.get('waiting_llm_total') != values.get('seen_handoffs')))):
            raise RetryStopped('失败列表尚未完整核对，本次未提交')
        return values['llm_buckets']['failed']

    def _stopped_retry(self, request):
        # A confirmed pre-submission stop can outlive the original failure
        # listing after the folder itself has been safely suspended. Its
        # retained retry item is sufficient to locate the same folder; all
        # handoff, admission and Prefect checks still run again below.
        with closing(_connect(self.database)) as db:
            row = db.execute("""
                SELECT i.task_id,i.group_id,i.paper_id,i.receipt_id,i.attempt_id,
                       i.folder_label,i.paper_title
                  FROM paperflow_llm_retry_items i JOIN paperflow_llm_stage_jobs j
                    ON j.job_id=i.job_id AND j.retry_key=i.request_id
                 WHERE i.task_id=? AND i.group_id=? AND i.paper_id=? AND i.receipt_id=?
                   AND i.attempt_id=? AND j.state='stopped_before_submit'
                   AND j.job_id=(SELECT n.job_id FROM paperflow_llm_stage_jobs n
                         WHERE n.task_id=j.task_id AND n.group_id=j.group_id
                         ORDER BY n.generation DESC,n.created_at_ns DESC,n.job_id DESC LIMIT 1)
                 ORDER BY i.updated_at_ns DESC LIMIT 1
            """, tuple(request[k] for k in ('task_id','group_id','paper_id','receipt_id','attempt_id'))).fetchone()
        return dict(row) if row else None

    def _run(self, request_id, request):
        # Only one snapshot/dispatcher per service instance. The unique index
        # is the cross-command fence for the same logical folder.
        with self.gate:
            try:
                self._command(request_id, 'running', '正在核对原请求及文件夹工单')
                rows = self._failed_snapshot(request['scope'])
                if request['scope'] == 'one':
                    keys = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')
                    rows = [row for row in rows if all(row.get(k) == request[k] for k in keys)]
                    if not rows:
                        stopped = self._stopped_retry(request)
                        rows = [stopped] if stopped else []
                    if len(rows) != 1:
                        raise RetryStopped('文件夹已不在可核验的失败列表，请刷新后查看')
                for row in rows:
                    now = time.time_ns()
                    try:
                        with closing(_connect(self.database)) as db, db:
                            db.execute('INSERT INTO paperflow_llm_retry_items '
                                '(request_id,task_id,group_id,paper_id,receipt_id,attempt_id,folder_label,paper_title,state,message,updated_at_ns) '
                                'VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                                (request_id, row['task_id'], row['group_id'], row['paper_id'], row['receipt_id'],
                                 row['attempt_id'], row.get('folder_label'), row.get('paper_title'), 'queued', '等待核对', now))
                    except sqlite3.IntegrityError:
                        # Another command already owns this folder; do not
                        # switch its state or issue a second request.
                        with closing(_connect(self.database)) as db, db:
                            db.execute('INSERT OR IGNORE INTO paperflow_llm_retry_items '
                                '(request_id,task_id,group_id,paper_id,receipt_id,attempt_id,folder_label,paper_title,state,message,updated_at_ns) '
                                'VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                                (request_id, row['task_id'], row['group_id'], row['paper_id'], row['receipt_id'],
                                 row['attempt_id'], row.get('folder_label'), row.get('paper_title'),
                                 'already_running', '已有重试在进行，本次未重复提交', now))
                for row in rows:
                    with closing(_connect(self.database)) as db:
                        owned = db.execute("SELECT 1 FROM paperflow_llm_retry_items "
                            "WHERE state='queued' AND "
                            'request_id=? AND task_id=? AND group_id=?',
                            (request_id, row['task_id'], row['group_id'])).fetchone()
                    if not owned:
                        continue
                    try:
                        self._process_folder(request_id, row)
                    except Exception as exc:
                        # A folder recovery has not sent a stage TaskRun. Keep
                        # uncertain folder recovery fenced, but do not call it
                        # an uncertain model submission.
                        with closing(_connect(self.database)) as db, db:
                            db.execute('BEGIN IMMEDIATE')
                            attempt = db.execute('SELECT state,job_id FROM paperflow_llm_retry_items '
                                'WHERE request_id=? AND task_id=? AND group_id=?',
                                (request_id, row['task_id'], row['group_id'])).fetchone()
                            if attempt and attempt['state'] == 'submitted':
                                # The durable reservation already won; never
                                # replace its truthful status on a later error.
                                continue
                            if attempt and attempt['job_id']:
                                next_state = ('recovery_unknown' if attempt['state'] == 'waiting_folder' else
                                              'identity_unknown' if attempt['state'] in {'guard_writing','stage_checking'} else
                                              'stopped_before_submit')
                                changed = db.execute("UPDATE paperflow_llm_stage_jobs SET state=?,updated_at_ns=? "
                                           "WHERE job_id=? AND state='recovering_folder'",
                                           (next_state, time.time_ns(), attempt['job_id'])).rowcount
                                if changed != 1:
                                    next_state = None
                            else:
                                next_state = None
                            if attempt and not attempt['job_id']:
                                result_state = ('paused' if isinstance(exc, RetryStopped) and '暂停' in str(exc)
                                                else 'not_sent')
                            else:
                                result_state = 'not_sent' if next_state == 'stopped_before_submit' else 'needs_review'
                            db.execute("UPDATE paperflow_llm_retry_items SET state=?,message=?,updated_at_ns=? "
                                'WHERE request_id=? AND task_id=? AND group_id=?',
                                (result_state, _error(exc), time.time_ns(), request_id, row['task_id'], row['group_id']))
                self._command(request_id, 'finished', '核对完成，逐项结果见下方')
            except Exception as exc:
                with closing(_connect(self.database)) as db, db:
                    db.execute("UPDATE paperflow_llm_retry_items SET state='needs_review',"
                               "message='批量核对未完成，本项未执行',updated_at_ns=? "
                               "WHERE request_id=? AND state='queued'", (time.time_ns(), request_id))
                self._command(request_id, 'failed', _error(exc))

    def _task(self, db, item):
        task = db.execute('SELECT * FROM tasks WHERE task_id=?', (item['task_id'],)).fetchone()
        deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (item['task_id'],)).fetchone()
        if (not task or deleted or task['prefect_order_model'] != 'v2'
                or task['task_status'] in {'deleting', 'delete_failed'}):
            raise RetryStopped('大任务已删除或状态待核对，本次未提交')
        if task['desired_state'] != 'running' or task['start_pending']:
            raise RetryStopped('大任务当前已暂停，本次未提交')
        if type(task['start_generation']) is not int or not task['prefect_run_id']:
            raise RetryStopped('大任务运行身份待核对，本次未提交')
        return dict(task)

    def _handoff(self, item):
        with closing(self.queue._connect()) as db:
            db.execute('BEGIN')
            session = sys.modules[NAMESPACE + '.local_queue']._current_receiver_session(db)
            if db.execute('SELECT 1 FROM cancelled_tasks WHERE task_id=?', (item['task_id'],)).fetchone():
                raise RetryStopped('文件夹所属任务已删除，本次未提交')
            row = db.execute("""
                SELECT h.handoff,o.prefect_run_id FROM preprocessing_handoffs h
                JOIN groups g ON g.task_id=h.task_id AND g.group_id=h.group_id
                    AND g.paper_id=h.paper_id AND g.receipt_id=h.receipt_id
                JOIN claims c ON c.task_id=h.task_id AND c.group_id=h.group_id
                    AND c.receipt_id=h.receipt_id AND c.consumer_id=h.consumer_id
                    AND c.attempt_id=h.attempt_id
                JOIN preprocessing_folder_orders o ON o.task_id=h.task_id AND o.group_id=h.group_id
                    AND o.receipt_id=h.receipt_id AND o.attempt_id=h.attempt_id
                WHERE h.task_id=? AND h.group_id=? AND h.paper_id=? AND h.receipt_id=?
                    AND h.attempt_id=? AND h.consumer_id='prefect-local-preprocessing'
                    AND h.state='waiting' AND g.state='ready_for_llm' AND c.state='completed'
                    AND g.checked_session=? AND o.prefect_run_id IS NOT NULL
                    AND EXISTS(SELECT 1 FROM retention_requests r WHERE r.task_id=g.task_id
                        AND r.group_id=g.group_id AND r.paper_id=g.paper_id AND r.receipt_id=g.receipt_id
                        AND r.consumer_id=? AND r.state='accepted' AND r.retention_id IS NOT NULL
                        AND r.last_error IS NULL AND r.checked_session=?)
            """, (item['task_id'], item['group_id'], item['paper_id'], item['receipt_id'],
                  item['attempt_id'], session, self.queue.consumer_id, session)).fetchone()
        if not row:
            raise RetryStopped('前处理交接身份待核对，本次未提交')
        handoff = json.loads(row['handoff'])
        identity = {key: item[key] for key in IDENTITY_KEYS}
        verified = load_frozen_handoff({**identity, **handoff}, identity, verify_registered_receipt)
        return str(UUID(row['prefect_run_id'])), verified

    def _prefect_identity(self, client, item, task, folder_id, *, allow_recovering=False, allow_deleting=False):
        parent = client.read_flow_run(UUID(task['prefect_run_id']))
        if (not parent.state or (not allow_deleting and (parent.state.is_final() or parent.state.is_cancelling()))
                or not parent.deployment_id or parent.parent_task_run_id is not None
                or parent.idempotency_key != 'first-step-order-v2-' + item['task_id']
                or (parent.parameters or {}).get('task_id') != item['task_id']
                or MARKER not in (client.read_deployment(parent.deployment_id).tags or [])):
            raise RetryStopped('大工单身份待核对，本次未提交')
        folder = client.read_flow_run(UUID(folder_id))
        if (folder.parent_task_run_id is not None or not folder.deployment_id or not folder.state
                or folder.idempotency_key != 'folder-waiting-v2-' + item['task_id'] + '-' + item['group_id']
                or folder.work_queue_name != 'task-' + item['task_id']
                or any((folder.parameters or {}).get(k) != item[k]
                       for k in ('task_id', 'group_id', 'receipt_id', 'attempt_id'))):
            raise RetryStopped('文件夹工单身份待核对，本次未提交')
        deployment = client.read_deployment(folder.deployment_id)
        pool = client.read_work_pool('paperflow-unified-preprocessing')
        queue = client.read_work_queue_by_name('task-' + item['task_id'], work_pool_name=pool.name)
        if (MARKER not in (deployment.tags or []) or deployment.work_pool_name != 'paperflow-unified-preprocessing'
                or pool.type != 'process' or pool.description != MARKER
                or folder.work_pool_id != pool.id or folder.work_pool_name != pool.name
                or folder.work_queue_id != queue.id):
            raise RetryStopped('当前文件夹工单部署身份待核对，本次未提交')
        if folder.state.is_paused():
            if (folder.state.state_details.pause_reschedule is not True
                    or not any((folder.name or '').endswith(s) for s in
                               ('等待 LLM', '等待任务开始', 'LLM 失败待核对', 'LLM 阶段状态待核对'))):
                raise RetryStopped('文件夹工单没有停在 LLM 入口，本次未提交')
        elif not (folder.state.is_failed() or folder.state.is_crashed() or
                  (allow_deleting and folder.state.is_cancelled()) or
                  (allow_recovering and (folder.state.is_scheduled() or folder.state.is_pending()
                                         or folder.state.is_running()))):
            raise RetryStopped('文件夹工单未处于失败或等待状态，本次未提交')
        return folder

    def _stage_before(self, client, item, task, folder_id):
        with closing(_connect(self.database)) as db:
            jobs = [dict(row) for row in db.execute(
                'SELECT * FROM paperflow_llm_stage_jobs WHERE task_id=? AND group_id=?',
                (item['task_id'], item['group_id']))]
        if any(job['state'] in BLOCKING_STATES for job in jobs):
            raise RetryStopped('已有重试或未确认的 LLM 阶段，本次未重复提交')
        choices = [job for job in jobs if job['state'] not in {'stopped', 'stopped_before_submit'}]
        current = max(choices, key=lambda job: (job['generation'], job['created_at_ns'])) if choices else None
        if not current:
            return None, None
        expected_id = _job_id(folder_id, current['generation'], current['retry_key'])
        expected = {**{k: item[k] for k in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
                    'parent_run_id': task['prefect_run_id'], 'folder_run_id': folder_id,
                    'job_id': expected_id, 'task_key': STAGE_TASK_KEY}
        if any(current[k] != v for k, v in expected.items()) or current['state'] != 'failed':
            raise RetryStopped('已有阶段记录尚不能安全重试，本次未提交')
        run = client.read_task_run(UUID(expected_id))
        _validate_stage_run(run, current)
        if not (run.state.is_failed() or run.state.is_crashed()):
            raise RetryStopped('原 LLM 阶段未结束，本次未提交')
        return current, str(run.state.id)

    def _evidence(self, item, task, folder_id, verified):
        # Bind the same versioned native adapters used by the LLM worker; this
        # does not load the model credential or send a request.
        from paperflow_prefect import legacy
        legacy.bind(self.settings)
        manifest = verified['manifest']
        expected = {entry['component_id']: entry for entry in expected_components(manifest, folder_id)}
        with closing(_connect(self.database)) as db:
            ledger = db.execute('SELECT * FROM llm_folder_queue WHERE task_id=? AND group_id=?',
                                (item['task_id'], item['group_id'])).fetchone()
            rows = [dict(row) for row in db.execute(
                'SELECT * FROM llm_request_admissions WHERE task_id=? AND task_run_id=?',
                (item['task_id'], folder_id))]
        digest = hashlib.sha256(json.dumps(manifest, ensure_ascii=False, sort_keys=True,
                    separators=(',', ':'), allow_nan=False).encode('utf8')).hexdigest()
        if (not ledger or ledger['state'] not in {'waiting_llm', 'llm_processing'}
                or ledger['task_run_id'] != folder_id or ledger['parent_run_id'] != task['prefect_run_id']
                or ledger['manifest_sha256'] != digest
                or any(ledger[k] != item[k] for k in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'))):
            raise RetryStopped('原 LLM 文件夹登记待核对，本次未提交')
        # Validate every known component against the frozen handoff before
        # considering any state change or any remaining unsent component.
        inspect_folder(manifest, folder_id, rows, lambda unused: None)
        paper = verified['paper']
        scoped = settings_for_verified_receipt(self.settings, verified['registered'])
        backend = UnifiedBackend(scoped, invoke=None)
        def component_paths(component):
            if component['kind'] == 'body':
                source = paper['body']
                work = Path(backend.body_work(paper)).resolve()
                if work != Path(manifest['body_work']).resolve():
                    raise RetryStopped('正文历史目录待核对，本次未提交')
                model = work / 'model'
                if manifest['capacity']['mode'] == 'partitioned':
                    model = model / component['part_id']
                from paperflow_prefect.profile_recovery import INTENT, _other_revisions
                if (work / INTENT).exists() or _other_revisions(work):
                    raise RetryStopped('正文恢复记录待核对，本次未提交')
            else:
                source = next((row for row in paper['attachments'] if row['file_id'] == component['file_id']), None)
                if source is None:
                    raise RetryStopped('附件身份待核对，本次未提交')
                work = Path(backend.attachment_work(paper, source)).resolve()
                model = work / 'model'
            return source, work, model
        paths = {key: component_paths(value) for key, value in expected.items()}
        for row in rows:
            _, work, model = paths[row['component_id']]
            if Path(row['work_dir']).resolve() != work or Path(row['model_dir']).resolve() != model:
                raise RetryStopped('原请求目录与文件夹交接不一致，本次未提交')
            self._review_existing_response(item, row, model)
        present = {row['component_id'] for row in rows}
        missing = [value for key, value in expected.items() if key not in present]
        for component in missing:
            source, work, model = paths[component['component_id']]
            if any((model / name).exists() for name in JOURNAL_FILES):
                raise RetryStopped('资料原请求记录待核对，本次未重发')
        return dict(ledger)

    def _review_existing_response(self, item, row, model):
        response_sha = row['response_sha256']
        if row['state'] != 'raw_complete':
            try:
                state = read_json(model / 'request-state.json')
            except (OSError, ValueError, KeyError, TypeError):
                if row['state'] == 'unknown':
                    return
                raise RetryStopped('原请求仍在处理中，本次未重复提交') from None
            if (row['state'] == 'unknown' and state.get('status') != 'response_complete'
                    and state.get('finish_reason') != 'stop'):
                return
            response_sha = state.get('response_sha256')
        try:
            self.guard.complete(item['task_id'], row['component_id'], response_sha)
        except (OSError, ValueError, KeyError, TypeError):
            raise RetryStopped('已有回复暂无法读取，本次未提交') from None

    def _reset_failed(self, item, task, folder_id):
        """Prepare the explicitly retried failed components for another request."""
        from paperflow_prefect.llm_queue.work_locks import work_locks

        with closing(_connect(self.database)) as db:
            rows = [dict(row) for row in db.execute(
                "SELECT * FROM llm_request_admissions WHERE task_id=? AND task_run_id=? AND state='unknown'",
                (item['task_id'], folder_id))]
        for row in rows:
            work, model = Path(row['work_dir']).resolve(), Path(row['model_dir']).resolve()
            root = work / 'model'
            part = (row['kind'] == 'body' and model.parent == root
                    and re.fullmatch(r'part-\d{3,}', model.name))
            if row['kind'] not in {'body', 'attachment'} or not (model == root or part):
                raise RetryStopped('请求目录不匹配，本次未提交')
            with work_locks(work, row['kind']):
                with closing(_connect(self.database)) as db, db:
                    db.execute('BEGIN IMMEDIATE')
                    current = self._task(db, item)
                    if (current['start_generation'] != task['start_generation']
                            or current['prefect_run_id'] != task['prefect_run_id']):
                        raise RetryStopped('大任务状态已变化，本次未提交')
                    saved = db.execute('SELECT * FROM llm_request_admissions WHERE task_id=? AND component_id=?',
                                       (item['task_id'], row['component_id'])).fetchone()
                    if not saved or dict(saved) != row:
                        raise RetryStopped('请求状态已变化，本次未提交')
                    for name in JOURNAL_FILES:
                        (model / name).unlink(missing_ok=True)
                    db.execute("DELETE FROM llm_request_admissions WHERE task_id=? AND component_id=? AND state='unknown'",
                               (item['task_id'], row['component_id']))

    def _hold_stage(self, request_id, item, task, folder, previous, previous_state):
        generation = task['start_generation']
        job_id = _job_id(str(folder.id), generation, request_id)
        with FileLock(self.control_locks / ('preprocessing-' + item['task_id'] + '.lock'), timeout=10):
            with closing(_connect(self.database)) as db, db:
                db.execute('BEGIN IMMEDIATE')
                latest = self._task(db, item)
                if (latest['start_generation'] != generation
                        or latest['prefect_run_id'] != task['prefect_run_id']):
                    raise RetryStopped('大任务状态已变化，本次未提交')
                if db.execute("SELECT 1 FROM llm_request_admissions WHERE task_id=? AND task_run_id=? "
                              "AND state IN ('admitted','returned_unverified') LIMIT 1",
                              (item['task_id'], str(folder.id))).fetchone():
                    raise RetryStopped('当前请求仍在处理中，本次未重复提交')
                marks = ','.join('?' for _ in BLOCKING_STATES)
                if db.execute('SELECT 1 FROM paperflow_llm_stage_jobs WHERE task_id=? AND group_id=? '
                              "AND state IN (" + marks + ") LIMIT 1",
                              (item['task_id'], item['group_id'], *BLOCKING_STATES)).fetchone():
                    raise RetryStopped('已有未确认的 LLM 阶段，本次未重复提交')
                if previous:
                    prior = db.execute('SELECT * FROM paperflow_llm_stage_jobs WHERE job_id=?',
                                       (previous['job_id'],)).fetchone()
                    if not prior or dict(prior) != previous or prior['state'] != 'failed':
                        raise RetryStopped('原 LLM 阶段状态已变化，本次未提交')
                old_job = json.dumps(previous, ensure_ascii=False, sort_keys=True) if previous else None
                db.execute('INSERT INTO paperflow_llm_stage_jobs '
                    '(job_id,task_id,group_id,paper_id,receipt_id,attempt_id,parent_run_id,folder_run_id,'
                    'generation,task_key,state,error_type,created_at_ns,updated_at_ns,retry_key) '
                    'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (job_id, item['task_id'], item['group_id'], item['paper_id'], item['receipt_id'],
                     item['attempt_id'], task['prefect_run_id'], str(folder.id), generation,
                     STAGE_TASK_KEY, 'recovering_folder', None, time.time_ns(), time.time_ns(), request_id))
                mode = 'create'
                db.execute('UPDATE paperflow_llm_retry_items SET folder_run_id=?,job_id=?,generation=?,stage_mode=?,'
                    'old_flow_state_id=?,old_stage_state_id=?,old_job_json=?,state=?,message=?,updated_at_ns=? '
                    'WHERE request_id=? AND task_id=? AND group_id=?',
                    (str(folder.id), job_id, generation, mode, str(folder.state.id), previous_state, old_job,
                     'recovering', '原请求已核对，正在准备原工单', time.time_ns(),
                     request_id, item['task_id'], item['group_id']))
        return job_id, mode

    def _resume_folder(self, client, request_id, item, task, folder):
        if folder.state.is_paused():
            return False
        # This is the existing FlowRun, not a placeholder. Do not force a
        # terminal run to PAUSED; let Prefect run the current deployed flow.
        with FileLock(self.control_locks / ('preprocessing-' + item['task_id'] + '.lock'), timeout=10):
            with closing(_connect(self.database)) as db:
                current = self._task(db, item)
            if (current['start_generation'] != task['start_generation']
                    or current['prefect_run_id'] != task['prefect_run_id']):
                raise RetryStopped('大任务状态已变化，原工单未恢复')
            # Check the pool, worker, entrypoint and per-run overrides again
            # immediately before setting the durable guard and scheduling.
            latest = self._prefect_identity(client, item, current, str(folder.id))
            if (not latest.state or str(latest.state.id) != str(folder.state.id)
                    or not (latest.state.is_failed() or latest.state.is_crashed())):
                raise RetryStopped('原工单状态已变化，请核对后再操作')
            # Parameters are writable on a FAILED flow. The new flow checks
            # this marker before any work; an older flow does not accept the
            # argument and is rejected by Prefect before its body executes.
            parameters = dict(latest.parameters or {})
            parameters['retry_guard'] = request_id
            self._item(request_id, item, 'guard_writing', '正在写入原工单恢复标记')
            try:
                transport = httpx.HTTPTransport(retries=0)
                with httpx.Client(transport=transport, timeout=3, trust_env=False, follow_redirects=False) as http:
                    response = http.patch(self.api + '/flow_runs/' + str(folder.id), json={'parameters': parameters})
                    response.raise_for_status()
            except Exception as exc:
                raise RetryStopped('原工单恢复标记结果待核对，未发出恢复请求') from exc
            # The single PATCH has returned; there is no SDK retry still in
            # flight. A failure of the following read never sends Scheduled.
            self._item(request_id, item, 'recovering', '恢复标记已写入，正在回读')
            guarded = client.read_flow_run(folder.id)
            if (not guarded.state or str(guarded.state.id) != str(folder.state.id)
                    or (guarded.parameters or {}) != parameters):
                raise RetryStopped('原工单恢复标记未确认，未发出恢复请求')
            self._item(request_id, item, 'waiting_folder', '正在恢复原文件夹工单')
            try:
                # Use one direct HTTP attempt. Prefect's SDK retries POST on
                # timeout, which could leave a second late state transition.
                # The persisted request UUID lets recovery find this exact
                # transition in the original FlowRun's state history.
                proposed = to_state_create(Scheduled())
                proposed.state_details.flow_run_id = folder.id
                proposed.state_details.transition_id = UUID(request_id)
                transport = httpx.HTTPTransport(retries=0)
                with httpx.Client(transport=transport, timeout=3, trust_env=False, follow_redirects=False) as http:
                    response = http.post(self.api + '/flow_runs/' + str(folder.id) + '/set_state',
                        json={'state': proposed.model_dump(mode='json', serialize_as_any=True), 'force': False})
                    response.raise_for_status()
                    result = OrchestrationResult.model_validate(response.json())
            except Exception as exc:
                raise RetryStopped('原工单恢复结果待核对，请勿重复点击') from exc
            if result.status != SetStateStatus.ACCEPT or not result.state or not result.state.is_scheduled():
                # A rejected transition may return a different current state;
                # retain the fence until that state is checked.
                raise RetryStopped('Prefect 恢复状态待核对，本次未提交 LLM')
            return True

    def _wait_folder(self, client, request_id, item, task, folder_id, *, guarded):
        deadline = time.monotonic() + 120
        stop = ControlStop(self.database, item['task_id'], task['start_generation'])
        while time.monotonic() < deadline:
            if stop.is_set():
                raise RetryStopped('大任务已暂停或删除，未继续提交 LLM')
            folder = client.read_flow_run(UUID(folder_id))
            if folder.state and folder.state.is_paused():
                folder = _folder_paused(client, {'folder_run_id': folder_id}, allow_processing=False)
                if guarded and ((folder.parameters or {}).get('retry_guard') != request_id
                        or not str(folder.state.state_details.pause_key or '').startswith('llm-retry-' + request_id + '-')):
                    raise RetryStopped('原工单挂起身份待核对，未提交 LLM')
                self._item(request_id, item, 'recovered', '原文件夹工单已挂起，正在复核')
                return {'state_id': str(folder.state.id),
                        'guard': request_id if guarded else None,
                        'pause_key': folder.state.state_details.pause_key}
            if folder.state and folder.state.is_final():
                raise RetryStopped('原文件夹工单恢复失败，LLM 未提交')
            time.sleep(1)
        raise RetryStopped('原文件夹工单恢复结果待核对，请勿重复点击')

    def _send_stage(self, client, request_id, item, task, folder_id, job_id, suspension):
        generation = task['start_generation']
        job = {**{k: item[k] for k in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
               'parent_run_id': task['prefect_run_id'], 'folder_run_id': folder_id,
               'generation': generation, 'job_id': job_id, 'task_key': STAGE_TASK_KEY,
               'retry_key': request_id}
        def same_suspension():
            folder = _folder_for_job(client, job, allow_processing=False)
            if str(folder.state.id) != suspension['state_id']:
                raise RetryStopped('原文件夹工单状态已变化，本次未提交')
            if suspension.get('terminal'):
                if not (folder.state.is_failed() or folder.state.is_crashed()):
                    raise RetryStopped('原文件夹失败状态已变化，本次未提交')
            elif (not folder.state.is_paused()
                    or folder.state.state_details.pause_key != suspension['pause_key']
                    or (suspension['guard'] is not None and
                        (folder.parameters or {}).get('retry_guard') != suspension['guard'])):
                raise RetryStopped('文件夹挂起状态已变化，本次未提交')
            # In the terminal case this also verifies the persisted retry,
            # the original state ID, and the frozen folder identity.
            return _binding(self.settings, self.queue, self.database, client, job).load(_identity(job))
        verified = same_suspension()
        self._evidence(item, task, folder_id, verified)
        self._item(request_id, item, 'stage_checking', '正在核对 LLM 阶段编号')
        try:
            client.read_task_run(UUID(job_id))
        except ObjectNotFound:
            self._item(request_id, item, 'recovered', '原文件夹身份及阶段编号已核对')
        else:
            raise RetryStopped('LLM 阶段编号已有运行记录，本次未重复提交')
        # Prefect and frozen-file checks can be slow; never hold the task gate
        # or SQLite's write transaction while performing them. The worker
        # repeats this same identity validation immediately before execution.
        same_suspension()
        with FileLock(self.control_locks / ('preprocessing-' + item['task_id'] + '.lock'), timeout=10):
            with closing(_connect(self.database)) as db, db:
                db.execute('BEGIN IMMEDIATE')
                current = self._task(db, item)
                if (current['start_generation'] != generation
                        or current['prefect_run_id'] != task['prefect_run_id']):
                    raise RetryStopped('大任务状态已变化，LLM 未提交')
                current_item = db.execute('SELECT state,old_flow_state_id,folder_run_id,job_id,generation '
                    'FROM paperflow_llm_retry_items WHERE request_id=? AND task_id=? AND group_id=?',
                    (request_id, item['task_id'], item['group_id'])).fetchone()
                if (not current_item or current_item['state'] != 'recovered'
                        or current_item['old_flow_state_id'] != suspension['state_id']
                        or current_item['folder_run_id'] != folder_id or current_item['job_id'] != job_id
                        or current_item['generation'] != generation):
                    raise RetryStopped('重试预约身份已变化，LLM 未提交')
                if db.execute("SELECT 1 FROM llm_request_admissions WHERE task_id=? AND task_run_id=? "
                              "AND state IN ('unknown','admitted','returned_unverified') LIMIT 1",
                              (item['task_id'], folder_id)).fetchone():
                    raise RetryStopped('原请求结果未确认，本次未重发')
                marks = ','.join('?' for _ in BLOCKING_STATES)
                if db.execute('SELECT 1 FROM paperflow_llm_stage_jobs WHERE task_id=? AND group_id=? '
                              'AND job_id<>? AND state IN (' + marks + ') LIMIT 1',
                              (item['task_id'], item['group_id'], job_id, *BLOCKING_STATES)).fetchone():
                    raise RetryStopped('已有未确认的 LLM 阶段，本次未重复提交')
                if db.execute("UPDATE paperflow_llm_stage_jobs SET state='reserved',updated_at_ns=? "
                              "WHERE job_id=? AND state='recovering_folder' AND retry_key=?",
                              (time.time_ns(), job_id, request_id)).rowcount != 1:
                    raise RetryStopped('LLM 阶段预约状态已变化，本次未提交')
                if db.execute("UPDATE paperflow_llm_retry_items SET state='submitted',message=?,updated_at_ns=? "
                              "WHERE request_id=? AND task_id=? AND group_id=? AND state='recovered'",
                              ('已进入 LLM 等待队列，等待现有派单器提交', time.time_ns(),
                               request_id, item['task_id'], item['group_id'])).rowcount != 1:
                    raise RetryStopped('重试预约状态已变化，LLM 未提交')

    def _process_folder(self, request_id, item):
        self._item(request_id, item, 'checking', '正在核对原请求和文件夹工单')
        folder_id, verified = self._handoff(item)
        lock_root = self.settings.path('data_root') / 'locks'
        lock = lock_root / ('folder-order-' + item['attempt_id'] + '.lock')
        with self._client() as client:
            with FileLock(lock, timeout=1, thread_local=False):
                with closing(_connect(self.database)) as db:
                    task = self._task(db, item)
                folder = self._prefect_identity(client, item, task, folder_id)
                previous, previous_state = self._stage_before(client, item, task, folder_id)
                self._evidence(item, task, folder_id, verified)
                candidate_job = _job_id(folder_id, task['start_generation'], request_id)
                try:
                    client.read_task_run(UUID(candidate_job))
                except ObjectNotFound:
                    pass
                else:
                    raise RetryStopped('LLM 阶段编号已有运行记录，本次未提交')
                job_id, mode = self._hold_stage(request_id, item, task, folder, previous, previous_state)
                self._reset_failed(item, task, folder_id)
            # Preserve the old failed FlowRun and its history. The actual
            # retry is a separate LLM stage bound to this same folder ID.
            if folder.state.is_paused():
                suspension = {'state_id': str(folder.state.id),
                              'pause_key': folder.state.state_details.pause_key,
                              'guard': None, 'terminal': False}
            elif folder.state.is_failed() or folder.state.is_crashed():
                suspension = {'state_id': str(folder.state.id), 'terminal': True}
            else:
                raise RetryStopped('原文件夹工单状态已变化，本次未提交')
            self._item(request_id, item, 'recovered', '原文件夹身份已保留，正在复核')
            with FileLock(lock, timeout=10, thread_local=False):
                self._send_stage(client, request_id, item, task, folder_id, job_id, suspension)


def create_app():
    app = FastAPI(title='LLM 文件夹人工重试')
    app.add_middleware(CORSMiddleware, allow_origins=['http://127.0.0.1:8795', 'http://localhost:8795'],
                       allow_methods=['GET', 'POST'], allow_headers=['content-type'])
    service = RetryService()

    @app.post('/api/v1/llm-retries')
    def create_retry(body: RetryRequest):
        return service.submit(body)

    @app.get('/api/v1/llm-retries')
    def list_retries(limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0)):
        return service.history(limit, offset)

    @app.get('/api/v1/llm-retries/{request_id}')
    def read_retry(request_id: str):
        return service.status(request_id)

    return app


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(create_app(), host='127.0.0.1', port=14207, log_level='warning')
