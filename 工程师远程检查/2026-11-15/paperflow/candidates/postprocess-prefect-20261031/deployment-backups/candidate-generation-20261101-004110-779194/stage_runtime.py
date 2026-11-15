"""Candidate: one Prefect stage for one existing folder FlowRun.

Importing this module starts nothing. The application must explicitly initialize
the schema, provide gate/identity/receipt adapters, and reserve a selected folder.
No scanner, worker, LLM caller or RAG uploader is started here.
"""
from contextlib import closing
import asyncio
import copy
import os
from pathlib import Path
import re
import sqlite3
import time
from uuid import UUID, uuid5

import psutil

TASK_KEY = 'paperflow-folder-postprocess-stage-v1'
TAG = 'paperflow-postprocess-stage-v1'
NAMESPACE = UUID('5e9b7732-458e-50cb-8916-6b53007c204f')
FIELDS = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')


class StageConflict(RuntimeError):
    """A gate, durable record, or identity could not be established."""


class ConfirmedFailure(RuntimeError):
    """The caller has positively recorded a final, non-ambiguous failure."""
    def __init__(self, message, *, code=None, confirm=None, evidence_path=None):
        super().__init__(message)
        self.code = code
        self.confirm = confirm
        self.evidence_path = evidence_path


def _identity(identity):
    if not isinstance(identity, dict) or set(identity) != set(FIELDS):
        raise StageConflict('完整的文件夹身份不可核验')
    try:
        if any(str(UUID(identity[k])) != identity[k] for k in ('task_id', 'parent_run_id', 'task_run_id')):
            raise ValueError('UUID')
    except (TypeError, ValueError, AttributeError) as exc:
        raise StageConflict('文件夹 UUID 不可核验') from exc
    if (not isinstance(identity['paper_id'], str) or not identity['paper_id']
            or not re.fullmatch('[0-9a-f]{32}', str(identity['group_id']))
            or not re.fullmatch('[0-9a-f]{64}', str(identity['receipt_id']))
            or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', str(identity['attempt_id']))):
        raise StageConflict('文件夹业务身份不可核验')
    return {key: identity[key] for key in FIELDS}


def _job_id(folder_run_id):
    # No generation suffix: even a new generation cannot silently redo a folder.
    return str(uuid5(NAMESPACE, str(UUID(folder_run_id))))


def task_key_for_job(job_id):
    try:
        parsed = UUID(job_id)
        if str(parsed) != job_id:
            raise ValueError('noncanonical UUID')
    except (TypeError, ValueError, AttributeError) as exc:
        raise StageConflict('后处理阶段编号不可核验') from exc
    return TASK_KEY + '-' + parsed.hex


def _truth(fn):
    try:
        return callable(fn) and fn() is True
    except Exception:
        return False


def _task_identity(stage, item):
    return (stage is not None and str(getattr(stage, 'id', '')) == item['job_id']
            and getattr(stage, 'flow_run_id', object()) is None
            and getattr(stage, 'task_key', None) == item['task_key'] == task_key_for_job(item['job_id'])
            and getattr(stage, 'dynamic_key', None) == 'paperflow-postprocess-' + item['job_id']
            and TAG in (getattr(stage, 'tags', None) or []))


def validate_prefect_stage(item, context_task, client, *, expected_api_url):
    """Read the actual TaskRun and paused folder; never infer from a local PID."""
    if (not isinstance(expected_api_url, str) or not expected_api_url.strip()
            or str(getattr(client, 'api_url', '')).rstrip('/') != expected_api_url.rstrip('/')):
        raise StageConflict('后处理阶段 Prefect 服务身份不一致')
    if not _task_identity(context_task, item):
        raise StageConflict('Prefect 执行上下文身份不一致')
    current = client.read_task_run(UUID(item['job_id']))
    if (not _task_identity(current, item) or not getattr(current, 'state', None)
            or not current.state.is_running()):
        raise StageConflict('Prefect 后处理阶段未确认运行')
    folder = client.read_flow_run(UUID(item['folder_run_id']))
    if (str(getattr(folder, 'id', '')) != item['folder_run_id'] or not getattr(folder, 'state', None)
            or not folder.state.is_paused()
            or getattr(folder.state.state_details, 'pause_reschedule', None) is not True
            or not (getattr(folder, 'name', '') or '').endswith('等待后处理')):
        raise StageConflict('原文件夹工单尚未处于等待后处理')
    return True


class StageBoundHandoff:
    """Preserve upstream binding checks while verifying our separate TaskRun."""
    def __init__(self, upstream, client, item, context_task):
        self.upstream, self.client, self.item, self.context_task = upstream, client, item, context_task

    def load(self, identity, *, require_running=False):
        expected = {key: self.item[key] for key in FIELDS}
        if _identity(identity) != expected:
            raise StageConflict('后处理与上游文件夹身份不同')
        if require_running:
            validate_prefect_stage(self.item, self.context_task, self.client,
                                   expected_api_url=getattr(self.upstream, 'api_url', None))
        verified = self.upstream.load(identity, require_running=False)
        if require_running:
            validate_prefect_stage(self.item, self.context_task, self.client,
                                   expected_api_url=getattr(self.upstream, 'api_url', None))
        return verified


class Coordinator:
    """One-shot reservation, submission and execution; no automatic scanner."""
    def __init__(self, database, *, task_gate, folder_gate, postprocess_is_open, rag_is_held,
                 verify_upstream, verify_execution, slot, process_folder, results):
        self.database = Path(database).resolve(strict=True)
        self.task_gate, self.folder_gate = task_gate, folder_gate
        self.postprocess_is_open, self.rag_is_held = postprocess_is_open, rag_is_held
        self.verify_upstream, self.verify_execution = verify_upstream, verify_execution
        self.slot, self.process_folder, self.results = slot, process_folder, results
        if (not all(callable(x) for x in (task_gate, folder_gate, postprocess_is_open, rag_is_held,
                                           verify_upstream, verify_execution, slot, process_folder))
                or not all(callable(getattr(results, x, None)) for x in ('reserve', 'success', 'failure', 'unknown'))):
            raise ValueError('后处理阶段依赖不完整')

    def _db(self, *, readonly=False):
        db = sqlite3.connect(self.database.as_uri() + ('?mode=ro' if readonly else '?mode=rw'),
                             uri=True, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    def initialize(self):
        """Explicit schema setup only; never opens a stage gate or starts work."""
        with closing(self._db()) as db, db:
            db.execute('''CREATE TABLE IF NOT EXISTS paperflow_postprocess_stage_jobs (
                job_id TEXT PRIMARY KEY,task_id TEXT NOT NULL,group_id TEXT NOT NULL,paper_id TEXT NOT NULL,
                receipt_id TEXT NOT NULL,attempt_id TEXT NOT NULL,parent_run_id TEXT NOT NULL,
                task_run_id TEXT NOT NULL UNIQUE,folder_run_id TEXT NOT NULL,generation INTEGER NOT NULL,
                manifest_sha256 TEXT NOT NULL,task_key TEXT NOT NULL,state TEXT NOT NULL,error_type TEXT,
                owner_pid INTEGER,owner_started REAL,created_at_ns INTEGER NOT NULL,updated_at_ns INTEGER NOT NULL,
                UNIQUE(task_id,group_id))''')

    def _gates(self):
        if not _truth(self.rag_is_held) or not _truth(self.postprocess_is_open):
            raise StageConflict('后处理未开放或 RAG 等待门禁不可核验')

    def _verified(self, identity, generation):
        evidence = self.verify_upstream(dict(identity), generation)
        if (not isinstance(evidence, dict) or evidence.get('identity') != identity
                or type(evidence.get('generation')) is not int or evidence['generation'] != generation
                or evidence.get('raw_complete') is not True or evidence.get('session_current') is not True
                or evidence.get('not_deleted') is not True
                or not re.fullmatch('[0-9a-f]{64}', str(evidence.get('manifest_sha256', '')))):
            raise StageConflict('上游完整回答或当前会话不可核验')
        return evidence['manifest_sha256']

    @staticmethod
    def _check_db(db, identity, generation, manifest):
        task = db.execute('SELECT * FROM tasks WHERE task_id=?', (identity['task_id'],)).fetchone()
        if (not task or task['desired_state'] != 'running' or task['start_generation'] != generation
                or task['start_pending'] or task['task_status'] in {'deleting', 'delete_failed', 'unknown'}
                or task['prefect_order_model'] != 'v2' or task['prefect_run_id'] != identity['parent_run_id']
                or db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (identity['task_id'],)).fetchone()):
            raise StageConflict('大工单已暂停、删除或代次不符')
        row = db.execute('SELECT * FROM llm_folder_queue WHERE task_id=? AND group_id=?',
                         (identity['task_id'], identity['group_id'])).fetchone()
        if (not row or any(row[key] != identity[key] for key in FIELDS)
                or row['state'] != 'waiting_postprocessing' or row['generation'] != generation
                or row['manifest_sha256'] != manifest):
            raise StageConflict('上游文件夹完成状态或身份有变化')
        rows = db.execute('SELECT state,kind FROM llm_request_admissions WHERE task_id=? AND task_run_id=?',
                          (identity['task_id'], identity['task_run_id'])).fetchall()
        if (not rows or not any(row['kind'] == 'body' for row in rows)
                or any(row['state'] != 'raw_complete' for row in rows)):
            raise StageConflict('模型回答未全部确认完成')
        # A prior material-processing claim is never silently reused or rerun.
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='postprocessing_claims'").fetchone():
            if db.execute('SELECT 1 FROM postprocessing_claims WHERE task_id=? AND group_id=?',
                          (identity['task_id'], identity['group_id'])).fetchone():
                raise StageConflict('已有后处理领取记录，需保留并核对')
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='postprocessing_results'").fetchone():
            if db.execute('SELECT 1 FROM postprocessing_results WHERE task_id=? AND group_id=?',
                          (identity['task_id'], identity['group_id'])).fetchone():
                raise StageConflict('已有处理结果预约，需保留并核对')

    def reserve(self, identity, generation):
        """Reserve exactly one existing folder; no task is submitted here."""
        identity = _identity(identity)
        if type(generation) is not int or generation < 1:
            raise StageConflict('任务代次不可核验')
        self._gates()
        manifest = self._verified(identity, generation)
        job_id, now = _job_id(identity['task_run_id']), time.time_ns()
        with self.task_gate(identity['task_id']), closing(self._db()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._gates()
            self._check_db(db, identity, generation, manifest)
            try:
                db.execute('INSERT INTO paperflow_postprocess_stage_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (job_id, *(identity[k] for k in FIELDS), identity['task_run_id'], generation,
                     manifest, task_key_for_job(job_id), 'reserved', None, None, None, now, now))
            except sqlite3.IntegrityError as exc:
                raise StageConflict('该文件夹已有后处理阶段记录，禁止重领') from exc
            self._gates()
        return self.job(job_id)

    def job(self, job_id):
        with closing(self._db(readonly=True)) as db:
            row = db.execute('SELECT * FROM paperflow_postprocess_stage_jobs WHERE job_id=?', (job_id,)).fetchone()
        if not row:
            raise StageConflict('后处理阶段预约不存在')
        return dict(row)

    def jobs_for_task(self, task_id):
        """One read-only snapshot for UI reconciliation; it does not claim liveness."""
        try:
            if str(UUID(task_id)) != task_id:
                raise ValueError('noncanonical UUID')
        except (TypeError, ValueError, AttributeError) as exc:
            raise StageConflict('大工单身份不可核验') from exc
        with closing(self._db(readonly=True)) as db:
            db.execute('BEGIN')
            return [dict(row) for row in db.execute('SELECT * FROM paperflow_postprocess_stage_jobs '
                'WHERE task_id=? ORDER BY created_at_ns,job_id', (task_id,))]

    def _move(self, job_id, before, after, error=None):
        with closing(self._db()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            return db.execute('UPDATE paperflow_postprocess_stage_jobs SET state=?,error_type=?,updated_at_ns=? '
                              'WHERE job_id=? AND state=?', (after, error, time.time_ns(), job_id, before)).rowcount == 1

    def submit(self, job_id, client, task):
        """Submit once; an ambiguous create response is never resent."""
        item = self.job(job_id)
        self._gates()
        identity = {key: item[key] for key in FIELDS}
        with self.task_gate(identity['task_id']), closing(self._db()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._gates()
            self._check_db(db, identity, item['generation'], item['manifest_sha256'])
            changed = db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitting',updated_at_ns=? "
                                 "WHERE job_id=? AND state='reserved'", (time.time_ns(), job_id)).rowcount
            if changed != 1:
                raise StageConflict('预约已提交或结果未知，禁止重发')
            self._gates()
        try:
            from prefect.states import Scheduled
            state = Scheduled()
            state.state_details.deferred = True
            actual = client.create_task_run(task=task, flow_run_id=None, id=UUID(job_id),
                dynamic_key='paperflow-postprocess-' + job_id, name='资料文件夹后处理阶段', state=state)
            if not _task_identity(actual, item):
                raise StageConflict('Prefect 返回的后处理阶段身份不一致')
            # The worker may have already claimed it before create returns.
            self._move(job_id, 'submitting', 'submitted')
            return self.job(job_id)
        except BaseException as exc:
            self._move(job_id, 'submitting', 'submission_unknown', type(exc).__name__)
            raise

    async def execute(self, job_id, context_task):
        """Use the injected native Prefect capacity lease over one whole folder."""
        from paperflow_prefect.resources import drain_thread
        self._gates()
        async with self.slot():
            return await drain_thread(self._execute_sync, job_id, context_task)

    def _execute_sync(self, job_id, context_task):
        item = self.job(job_id)
        identity = _identity({key: item[key] for key in FIELDS})
        if (job_id != _job_id(identity['task_run_id']) or item['folder_run_id'] != identity['task_run_id']
                or item['task_key'] != task_key_for_job(job_id)):
            raise StageConflict('后处理阶段预约身份不一致')
        owned, result_id = False, None
        try:
            with self.folder_gate(item):
                self._gates()
                if self.verify_execution(item, context_task) is not True:
                    raise StageConflict('真实后处理 TaskRun 未确认')
                manifest = self._verified(identity, item['generation'])
                if manifest != item['manifest_sha256']:
                    raise StageConflict('冻结材料身份已改变')
                with self.task_gate(identity['task_id']), closing(self._db()) as db, db:
                    db.execute('BEGIN IMMEDIATE')
                    self._gates()
                    self._check_db(db, identity, item['generation'], manifest)
                    changed = db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='executing',owner_pid=?,"
                        "owner_started=?,updated_at_ns=? WHERE job_id=? AND state IN ('submitting','submitted')",
                        (os.getpid(), psutil.Process(os.getpid()).create_time(), time.time_ns(), job_id)).rowcount
                    if changed != 1:
                        raise StageConflict('阶段已领取或执行结果未知，禁止重做')
                owned = True
                self._gates()
                if self.verify_execution(item, context_task) is not True:
                    raise StageConflict('真实后处理 TaskRun 不再可核验')
                # This durable result identity exists before parsing any material.
                result_id = self.results.reserve(identity, item['generation'], manifest)
                if not isinstance(result_id, str) or not result_id:
                    raise StageConflict('处理结果预约身份不可核验')
                self._gates()
                if self._verified(identity, item['generation']) != manifest:
                    raise StageConflict('后处理前上游材料发生变化')
                accepted = []
                def accept(stage_result, validate_receipt):
                    if (accepted or not isinstance(stage_result, dict)
                            or stage_result.get('state') not in {'waiting_rag', 'receipt_held'}
                            or stage_result.get('identity') != identity
                            or not isinstance(stage_result.get('claim_id'), str)
                            or not stage_result['claim_id'] or not callable(validate_receipt)):
                        raise StageConflict('后处理阶段交接回执不可核验')
                    frozen_result = copy.deepcopy(stage_result)
                    self.results.success(identity, result_id, stage_result, validate_receipt)
                    if stage_result != frozen_result:
                        raise StageConflict('交接校验期间结果发生变化')
                    accepted.append(frozen_result)
                result = self.process_folder(identity, item['generation'], item, accept, context_task)
                if len(accepted) != 1 or result != accepted[0]:
                    raise StageConflict('后处理返回结果未经过同一回执校验')
                if not self._move(job_id, 'executing', 'finished'):
                    raise StageConflict('后处理阶段完成状态冲突')
                return result
        except BaseException as exc:
            if owned:
                known = isinstance(exc, ConfirmedFailure)
                if result_id:
                    try:
                        (self.results.failure if known else self.results.unknown)(identity, result_id, exc)
                    except BaseException as record_error:
                        known = False
                        exc.add_note('result outcome could not be recorded: ' + type(record_error).__name__)
                self._move(job_id, 'executing', 'failed' if known else 'execution_unknown', type(exc).__name__)
            raise


def build_task(runtime_factory, job_id):
    """Create a real Prefect Task definition. Constructing it dispatches nothing."""
    if not callable(runtime_factory):
        raise ValueError('缺少后处理运行时工厂')
    from prefect import task
    from prefect.cache_policies import CacheKeyFnPolicy
    from prefect.context import TaskRunContext

    @task(name='资料文件夹后处理阶段', tags=[TAG], retries=0, persist_result=True,
          cache_policy=CacheKeyFnPolicy(cache_key_fn=lambda _context, _parameters: None))
    async def execute_postprocessing_stage():
        context = TaskRunContext.get()
        if context is None or context.task_run is None:
            raise StageConflict('缺少真实的 Prefect TaskRun')
        coordinator = runtime_factory()
        if not isinstance(coordinator, Coordinator):
            raise StageConflict('后处理运行时不可核验')
        return await coordinator.execute(str(context.task_run.id), context.task_run)

    execute_postprocessing_stage.task_key = task_key_for_job(job_id)
    return execute_postprocessing_stage


def make_one_worker(stage, job_id):
    """Construct, but do not start, a worker subscribed to one stage only."""
    from prefect.task_worker import TaskWorker
    expected = task_key_for_job(job_id)
    if getattr(stage, 'task_key', None) != expected:
        raise StageConflict('单工单 worker 的任务键不一致')
    worker = TaskWorker(stage, limit=1)
    if worker.task_keys != {expected} or len(worker.tasks) != 1:
        raise StageConflict('单工单 worker 订阅范围不一致')
    # TaskWorker clones a Task and may recompute its code-derived key.
    worker.tasks[0].task_key = expected
    return worker
