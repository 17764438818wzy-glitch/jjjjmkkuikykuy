"""Explicit one-folder runtime glue. Importing it performs no I/O or dispatch.

The command defined below does not scan other folders, send model requests, or
upload to RAG. Initialization, gate opening, and running are separate actions.
"""
from contextlib import closing
import argparse
import asyncio
import hashlib
import importlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import time
from types import ModuleType
from uuid import UUID

from stage_runtime import (ConfirmedFailure, Coordinator, FIELDS, RetryAwareUpstream, StageBoundHandoff, StageConflict,
                           _identity, _task_identity, build_task, make_one_worker, reserved_source_matches, validate_prefect_stage)

BODY_RESULT_FAILURE_CODES = frozenset({'field_type', 'candidate_incomplete', 'learning_unusable'})


def select_folder(database, task_id, group_id):
    """Read the exact upstream ticket; this never reserves or alters anything."""
    path = Path(database).resolve(strict=True)
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=3)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        row = db.execute('SELECT * FROM llm_folder_queue WHERE task_id=? AND group_id=?',
                         (task_id, group_id)).fetchone()
        if not row or row['state'] != 'waiting_postprocessing':
            raise ValueError('指定文件夹不在等待后处理')
        identity = _identity({key: row[key] for key in FIELDS})
        task = db.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        if (not task or task['desired_state'] != 'running' or task['start_pending']
                or task['prefect_order_model'] != 'v2' or task['prefect_run_id'] != identity['parent_run_id']
                or task['task_status'] in {'deleting', 'delete_failed', 'unknown'}
                or db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone()):
            raise ValueError('指定大工单尚未恢复运行')
        generation, source = task['start_generation'], row['generation']
        if type(generation) is not int or type(source) is not int or not 1 <= source <= generation:
            raise ValueError('指定文件夹代次不可核验')
        return identity, generation


class ResultsHooks:
    """Use the shared result ledger; acceptance must check the real receipt."""
    def __init__(self, ledger):
        self.ledger = ledger

    def reserve(self, identity, generation, manifest_sha256):
        return self.ledger.reserve(identity, generation, manifest_sha256)

    def success(self, identity, result_id, result, validate_receipt):
        claim_id = result.get('claim_id') if isinstance(result, dict) else None
        if not isinstance(claim_id, str) or not claim_id or not callable(validate_receipt):
            raise ValueError('阶段回执或其验证器不可核验')
        return self.ledger.record_pass(identity, result_id, claim_id, validate_receipt=validate_receipt)

    def failure(self, identity, result_id, error):
        code, confirm = getattr(error, 'code', None), getattr(error, 'confirm', None)
        evidence = getattr(error, 'evidence_path', None)
        if not isinstance(code, str) or not callable(confirm) or not isinstance(evidence, str):
            raise ValueError('缺少确定失败的独立证据')
        return self.ledger.record_failed(identity, result_id, code=code, confirm=confirm, evidence_path=evidence)

    def unknown(self, identity, result_id, error):
        return self.ledger.record_unresolved(identity, result_id, code='outcome_unknown')


def body_validation_failure(identity, generation, manifest, error, output_root, *, verify_upstream,
                            source_llm_generation, source_retry_sha256=None, source_retry_prefect_sha256=None):
    """Confirm a saved invalid-profile report and its original input binding."""
    from body_adapter import BodyValidationError
    from folder_processor import _hash
    if (not isinstance(error, BodyValidationError)
            or str(error) not in {'candidate_validation_failed', 'assembled_candidate_validation_failed'}
            or not isinstance(error.diagnostic, dict) or not callable(verify_upstream)
            or type(source_llm_generation) is not int or type(generation) is not int
            or not 1 <= source_llm_generation <= generation):
        raise ValueError('正文失败诊断不可核验')
    reference = dict(error.diagnostic)
    if (set(reference) != {'path', 'sha256'} or not isinstance(reference['path'], str)
            or not re.fullmatch('[0-9a-f]{64}', str(reference['sha256']))):
        raise ValueError('正文失败诊断引用不可核验')
    body = Path(output_root).absolute() / _hash(identity) / 'body'

    def confirm(actual, code):
        try:
            if actual != identity or code != 'body_validation':
                return False
            proof = Path(reference['path']).resolve(strict=True)
            private = (body / 'private').resolve(strict=True)
            if (not proof.is_file() or proof.parent != private
                    or not re.fullmatch(r'(?:part-\d{3}-validation|validation)\.json', proof.name)):
                return False
            raw = proof.read_bytes()
            if hashlib.sha256(raw).hexdigest() != reference['sha256']:
                return False
            report = json.loads(raw)
            diagnostic = json.loads((body / 'diagnostic.json').read_text(encoding='utf8'))
            binding = json.loads((body / 'input-identity.json').read_text(encoding='utf8'))
            if (not isinstance(report, dict) or report.get('valid') is not False
                    or not isinstance(report.get('errors'), list) or not report['errors']
                    or any(not isinstance(issue, dict) or issue.get('code') not in BODY_RESULT_FAILURE_CODES
                           for issue in report['errors'])
                    or binding.get('identity') != identity
                    or diagnostic.get('identity') != identity or diagnostic.get('status') != 'blocked'
                    or diagnostic.get('input_fingerprint') != binding.get('input_fingerprint')
                    or diagnostic.get('error_type') != 'BodyValidationError'
                    or diagnostic.get('reason') != str(error)
                    or diagnostic.get('original_responses_retained') is not True
                    or diagnostic.get('model_requests_added') != 0):
                return False
            upstream = verify_upstream(dict(identity), generation)
            return (isinstance(upstream, dict) and upstream.get('identity') == identity
                    and upstream.get('generation') == generation and upstream.get('manifest_sha256') == manifest
                    and upstream.get('source_llm_generation') == source_llm_generation
                    and upstream.get('source_retry_sha256') == source_retry_sha256
                    and upstream.get('source_retry_prefect_sha256') == source_retry_prefect_sha256
                    and upstream.get('raw_complete') is True and upstream.get('session_current') is True
                    and upstream.get('not_deleted') is True
                    and hashlib.sha256(proof.read_bytes()).hexdigest() == reference['sha256'])
        except Exception:
            return False

    return ConfirmedFailure('正文画像校验未通过', code='body_validation', confirm=confirm,
                            evidence_path=reference['path'])


def build_runtime():
    """Build actual adapters. This does not initialize tables or start a worker."""
    _prepare_imports()
    from filelock import FileLock
    from prefect.client.orchestration import SyncPrefectClient
    from paperflow_prefect import legacy
    from paperflow_prefect.attachment_queue import settings_for_verified_receipt
    from paperflow_prefect.llm_queue.folder_completion import expected_components, inspect_folder
    from paperflow_prefect.llm_queue.pause_admission import ControlStop
    from paperflow_prefect.llm_queue.upstream_flow_binding import UpstreamFlowBinding
    from paperflow_prefect.local_preprocessing import (_control_paths, FIRST_STEP_CONFIG,
        NAMESPACE, QUEUE_CONFIG, QUEUE_DIR)
    from paperflow_prefect.records import read_json
    from paperflow_prefect.registered_input import verify_registered_receipt
    from paperflow_prefect.settings import Settings
    from paperflow_prefect.unified import UnifiedBackend

    from body_adapter import frozen_plan_evidence
    from folder_state import FolderStageStore
    from handoff_inputs import build_inputs
    from generation_mainline_bridge import _snapshot, process_mainline_folder
    from postprocessing_results import ResultsStore
    from prefect_slot import postprocessing_slot
    from stage_gates import QueueBarriers

    settings = Settings({**Settings.load().values, 'execution_enabled': True})
    settings.require_execution()
    api = str(settings['prefect_api_url']).rstrip('/')
    first_step_api = str(read_json(FIRST_STEP_CONFIG).get('prefect_api_url', '')).rstrip('/')
    if not api or api != first_step_api:
        raise ValueError('前处理与后处理的 Prefect 服务身份不一致')
    database, control_locks = _control_paths()
    queue_config = read_json(QUEUE_CONFIG)
    if not isinstance(queue_config, dict) or set(queue_config) != {'queue_db', 'receipt_root', 'journal_root'}:
        raise ValueError('资料池配置不可核验')
    queue_database = Path(queue_config['queue_db']).resolve(strict=True)

    # Import the existing receiver-session verifier without constructing a
    # LocalQueue (its constructor executes schema setup on the live database).
    if NAMESPACE not in sys.modules:
        package = ModuleType(NAMESPACE)
        package.__path__ = [str(QUEUE_DIR.resolve(strict=True))]
        package.__package__ = NAMESPACE
        sys.modules[NAMESPACE] = package
    receiver = importlib.import_module(NAMESPACE + '.local_queue')
    session = receiver._current_receiver_session

    def client():
        return SyncPrefectClient(api, httpx_settings={'timeout': 3, 'trust_env': False})

    def upstream(prefect_client):
        base = UpstreamFlowBinding(queue_database, database, prefect_client, api, 'local-intake',
                                   verify_registered_receipt, receiver_session=session)
        return RetryAwareUpstream(base)

    def task_gate(task_id):
        return FileLock(control_locks / ('preprocessing-' + str(UUID(task_id)) + '.lock'),
                        timeout=30, thread_local=False)

    def folder_gate(item):
        lock_dir = settings.path('data_root') / 'locks'
        if not lock_dir.is_dir():
            raise ValueError('文件夹工单锁目录不存在')
        return FileLock(lock_dir / ('folder-order-' + item['attempt_id'] + '.lock'),
                        timeout=30, thread_local=False)

    def backend_for(verified):
        scoped = settings_for_verified_receipt(settings, verified['registered'])
        # This binds reviewed local business modules; invoke=None prevents LLM.
        return UnifiedBackend(scoped, invoke=None)

    def observe_run(identity):
        """Observe the existing folder binding using a fresh, closed-after-use client.

        This does not prove the task is running or the server worker is alive;
        the existing task gates and stage checks remain authoritative.
        """
        try:
            with client() as prefect_client:
                verified = upstream(prefect_client).load(identity, require_running=False)
            if not isinstance(verified, dict):
                return False
            retry = verified.get('source_retry_sha256')
            prefect_proof = verified.get('source_retry_prefect_sha256')
            with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
                db.row_factory = sqlite3.Row
                db.execute('BEGIN')
                exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                    "AND name='paperflow_postprocess_stage_jobs'").fetchone()
                row = (db.execute('SELECT * FROM paperflow_postprocess_stage_jobs WHERE task_id=? AND group_id=?',
                                  (identity['task_id'], identity['group_id'])).fetchone() if exists else None)
                if row is not None and (any(row[key] != identity[key] for key in FIELDS)
                        or 'source_retry_sha256' not in row.keys() or row['source_retry_sha256'] != retry
                        or 'source_retry_prefect_sha256' not in row.keys()
                        or row['source_retry_prefect_sha256'] != prefect_proof):
                    return False
                if retry is None:
                    return True
                if row is None:
                    return False
                task = db.execute('SELECT start_generation FROM tasks WHERE task_id=?',
                                  (identity['task_id'],)).fetchone()
                if not task or type(task['start_generation']) is not int:
                    return False
                generation = task['start_generation']
            full = verify_upstream(identity, generation)
            return (full.get('source_retry_sha256') == retry
                    and full.get('source_retry_prefect_sha256') == prefect_proof
                    and reserved_source_matches(database, identity, full))
        except Exception:
            return False

    def verify_upstream(identity, generation):
        legacy.bind(settings)
        from preextract_scheduler_adapter import request_observation
        with client() as prefect_client:
            binding = upstream(prefect_client)
            verified = binding.load(identity, require_running=False)
            backend = backend_for(verified)
            row, admissions = _snapshot(database, identity)
            build_inputs(identity=identity, verified=verified, admissions=admissions,
                manifest_sha256=row['manifest_sha256'], expected_components=expected_components,
                inspect_folder=inspect_folder, observe_request=request_observation,
                frozen_plan=frozen_plan_evidence, attachment_work=backend.attachment_work)
            checked = binding.load(identity, require_running=False)
            if (checked.get('source_retry_sha256') != verified.get('source_retry_sha256')
                    or checked.get('source_retry_prefect_sha256') != verified.get('source_retry_prefect_sha256')
                    or checked.get('manifest') != verified.get('manifest')):
                raise StageConflict('核验原始回答期间人工重试来源发生变化')
        return dict(identity=dict(identity), generation=generation, source_llm_generation=row['generation'],
                    manifest_sha256=row['manifest_sha256'],
                    source_retry_sha256=verified.get('source_retry_sha256'),
                    source_retry_prefect_sha256=verified.get('source_retry_prefect_sha256'),
                    raw_complete=True, session_current=True, not_deleted=True)

    def verify_execution(item, context_task):
        stop = ControlStop(database, item['task_id'], item['generation'])
        deadline = time.monotonic() + 30
        with client() as prefect_client:
            while True:
                try:
                    return validate_prefect_stage(item, context_task, prefect_client, expected_api_url=api,
                                                  retry_upstream=upstream(prefect_client))
                except StageConflict:
                    if time.monotonic() >= deadline:
                        raise
                    if stop.is_set():
                        with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True)) as db:
                            db.row_factory = sqlite3.Row
                            if not FolderStageStore._can_finish(db, {key: item[key] for key in FIELDS}, item['generation']):
                                raise
                    time.sleep(.25)

    barriers = QueueBarriers(database)
    results_store = ResultsStore(database, admission_gate=task_gate,
                                 inspect_receipt=FolderStageStore._inspect_receipt)

    def process_folder(identity, generation, item, accepted_hook, context_task):
        legacy.bind(settings)
        from body_adapter import BodyValidationError
        from preextract_pipeline import offline_counter
        with client() as prefect_client:
            binding = upstream(prefect_client)
            stage_binding = StageBoundHandoff(binding, prefect_client, item, context_task)
            verified = stage_binding.load(identity, require_running=True)
            backend = backend_for(verified)

            stage_store = FolderStageStore(database, observe_run=observe_run, admission_gate=task_gate)
            paperflow_root = settings.path('release_root') / 'core'
            counter = offline_counter(backend.runtime, paperflow_root)
            try:
                output_root = settings.path('data_root') / 'postprocessing-v2'
                try:
                    return process_mainline_folder(identity=identity, generation=generation,
                        control_database=database, binding=stage_binding, backend=backend, store=stage_store,
                        catalog=backend.catalog, paperflow_root=paperflow_root, counter=counter,
                        output_root=output_root, accepted_hook=accepted_hook)
                except BodyValidationError as exc:
                    raise body_validation_failure(identity, generation, item['manifest_sha256'], exc,
                        output_root, verify_upstream=verify_upstream,
                        source_llm_generation=item['source_llm_generation'],
                        source_retry_sha256=item.get('source_retry_sha256'),
                        source_retry_prefect_sha256=item.get('source_retry_prefect_sha256')) from exc
            finally:
                counter.close()

    coordinator = Coordinator(database, task_gate=task_gate, folder_gate=folder_gate,
        postprocess_is_open=barriers.postprocess_is_open, rag_is_held=barriers.rag_is_held,
        verify_upstream=verify_upstream, verify_execution=verify_execution,
        slot=lambda: postprocessing_slot(api), process_folder=process_folder,
        results=ResultsHooks(results_store))

    def _reconcile_held_sync(job_id, generation):
        legacy.bind(settings)
        with client() as prefect_client:
            binding = upstream(prefect_client)
            item = coordinator.job(job_id)
            identity = _identity({key: item[key] for key in FIELDS})
            verified = binding.load(identity, require_running=False)
            backend = backend_for(verified)

            store = FolderStageStore(database, observe_run=observe_run, admission_gate=task_gate)
            def validator_factory(_item, claim_id):
                return process_mainline_folder(identity=identity, generation=generation,
                    control_database=database, binding=binding, backend=backend, store=store,
                    catalog=backend.catalog, paperflow_root=settings.path('release_root') / 'core',
                    counter=None, output_root=settings.path('data_root') / 'postprocessing-v2',
                    reconcile_claim_id=claim_id, validator_only=True)
            return coordinator.reconcile_held(job_id, generation, store=store, validator_factory=validator_factory)

    async def reconcile_held(job_id, generation):
        # Administrative verification reuses the same global Prefect capacity;
        # it never creates another business job or repeats model/packaging work.
        from paperflow_prefect.resources import drain_thread
        async with postprocessing_slot(api):
            return await drain_thread(_reconcile_held_sync, job_id, generation)

    return dict(coordinator=coordinator, database=database, barriers=barriers, results=results_store,
                stage_store=FolderStageStore(database, admission_gate=task_gate), client=client, api=api,
                upstream_factory=upstream, backend_factory=backend_for, observe_run=observe_run,
                reconcile_held=reconcile_held)


def _prepare_imports():
    candidate = Path(__file__).resolve().parent.parent
    workspace = candidate.parents[1]
    for path in (workspace / 'prefect-paperflow', candidate, candidate / 'results_next', Path(__file__).resolve().parent):
        value = str(path)
        if value not in sys.path:
            sys.path.insert(0, value)


async def run_selected(coordinator, identity, generation, *, client_factory, runtime_factory,
                       worker_factory=make_one_worker, poll_seconds=.5, retry_job_id=None):
    """Submit and observe a single already-chosen folder; never scan a queue."""
    item = (await asyncio.to_thread(coordinator.reserve, identity, generation) if retry_job_id is None else
            await asyncio.to_thread(coordinator.reserve, identity, generation, retry_job_id=retry_job_id))
    return await run_reserved(coordinator, item, client_factory=client_factory, runtime_factory=runtime_factory,
                              worker_factory=worker_factory, poll_seconds=poll_seconds)


async def resume_stopped(coordinator, job_id, generation, *, client_factory, expected_api_url,
                         runtime_factory, worker_factory=make_one_worker, poll_seconds=.5):
    """Resume the existing stage without submitting another TaskRun."""
    item = await asyncio.to_thread(coordinator.job, job_id)
    if item['state'] in {'submitting', 'submitted'}:
        with client_factory() as client:
            actual = await asyncio.to_thread(client.read_task_run, UUID(job_id))
        if (not _task_identity(actual, item) or not actual.state
                or not (actual.state.is_running() or actual.state.is_pending() or actual.state.is_scheduled())):
            raise StageConflict('已提交的后处理工单无法接续')
        stage = build_task(runtime_factory, job_id)
        async with worker_factory(stage, job_id) as worker:
            await worker.execute_task_run(actual)
        with client_factory() as client:
            finished = await asyncio.to_thread(client.read_task_run, UUID(job_id))
        if not _task_identity(finished, item) or not finished.state or not finished.state.is_completed():
            raise StageConflict('接续后的 Prefect 后处理工单未完成')
        return await asyncio.to_thread(coordinator.job, job_id)
    with client_factory() as client:
        item = await asyncio.to_thread(coordinator.rearm_stopped, job_id, generation, client=client,
                                       expected_api_url=expected_api_url)
    return await run_reserved(coordinator, item, client_factory=client_factory, runtime_factory=runtime_factory,
                              worker_factory=worker_factory, poll_seconds=poll_seconds)


async def run_reserved(coordinator, item, *, client_factory, runtime_factory,
                       worker_factory=make_one_worker, poll_seconds=.5):
    """Start the same durable stage identity after fresh reserve or safe rearm."""
    job_id = item['job_id']
    stage = build_task(runtime_factory, job_id)
    worker = worker_factory(stage, job_id)
    async with worker:
        subscription = asyncio.create_task(worker.astart())
        try:
            # Give immediate worker configuration errors a chance to surface
            # before submission; subscription uses only this job's unique key.
            await asyncio.sleep(0)
            if subscription.done():
                subscription.result()
                raise StageConflict('后处理 worker 未保持运行')
            with client_factory() as client:
                await asyncio.to_thread(coordinator.submit, job_id, client, stage)
            while True:
                current = await asyncio.to_thread(coordinator.job, job_id)
                if current['state'] in {'finished', 'failed', 'submission_unknown', 'execution_unknown'}:
                    return current
                if subscription.done():
                    subscription.result()
                    return dict(current, observation='worker_stopped_before_result')
                if worker.finished_task_runs.get(stage.task_key, 0):
                    # No retry or invented failure: the durable stage outcome
                    # must be reconciled with the actual Prefect TaskRun.
                    return dict(current, observation='task_finished_before_stage_confirmation')
                await asyncio.sleep(poll_seconds)
        finally:
            subscription.cancel()
            try:
                await subscription
            except asyncio.CancelledError:
                pass


def main(argv=None):
    parser = argparse.ArgumentParser(description='只处理明确指定的一个文件夹，绝不扫描队列')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('init-tables', help='显式创建候选阶段表和默认关闭门禁；不启动任务')
    run = sub.add_parser('run-one', help='启动一个指定文件夹后处理；门禁必须事先开放')
    run.add_argument('--task-id', required=True)
    run.add_argument('--group-id', required=True)
    args = parser.parse_args(argv)
    _prepare_imports()
    workspace = Path(__file__).resolve().parents[3]
    config = json.loads((workspace / 'prefect-paperflow' / 'config.json').read_text(encoding='utf-8-sig'))
    api = str(config.get('prefect_api_url', '')).rstrip('/')
    if not api:
        raise ValueError('Prefect 服务地址不可核验')
    os.environ['PREFECT_API_URL'] = api
    os.environ['PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS'] = 'false'
    runtime = build_runtime()
    if runtime['api'] != api:
        raise ValueError('Prefect 服务地址不一致')
    if args.command == 'init-tables':
        runtime['barriers'].initialize()
        runtime['stage_store'].initialize()
        runtime['results'].initialize()
        runtime['coordinator'].initialize()
        print('阶段表已检查；未开放门禁、未启动任务。')
        return
    identity, generation = select_folder(runtime['database'], args.task_id, args.group_id)
    result = asyncio.run(run_selected(runtime['coordinator'], identity, generation,
        client_factory=runtime['client'], runtime_factory=lambda: build_runtime()['coordinator']))
    print(json.dumps({'job_id': result['job_id'], 'state': result['state'],
                      'observation': result.get('observation')}, ensure_ascii=False))


if __name__ == '__main__':
    main()
