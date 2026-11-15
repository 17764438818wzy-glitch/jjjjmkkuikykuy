"""Connect the retained first-step waiting pool to local preparation only."""
from contextlib import closing, contextmanager
import asyncio
import copy
from datetime import datetime, timezone
import importlib
import json
from pathlib import Path
import re
import sqlite3
import sys
import time
from types import ModuleType
import uuid

from filelock import FileLock, Timeout
from prefect import flow

from . import flows
from .attachment_queue import settings_for_verified_receipt
from .backend import Backend
from .records import Records, identity, read_json, sha_file, write_json
from .registered_input import verify_registered_receipt
from .resources import drain_thread
from .settings import PROJECT, Settings
from .unified import UnifiedBackend


CONSUMER = 'prefect-local-preprocessing'
PREPROCESSING_LIMIT = flows.LIMIT_PREFIX + 'preprocessing'
PREPROCESSING_CONCURRENCY = 8
PREPROCESSING_POOL = 'paperflow-unified-preprocessing'
QUEUE_DIR = PROJECT.parent / 'candidates' / 'local-intake-live-link-20261031'
QUEUE_CONFIG = PROJECT.parent / 'candidates' / 'attachment-prefect-live-link-20261031' / 'attachment-queue-config.json'
NAMESPACE = '_paperflow_live_local_intake'
FIRST_STEP_CONFIG = PROJECT.parent / 'candidates' / 'first-step-control-20261029' / 'runtime' / 'workspace-config.json'
DISPATCH_FLOW_NAME = '本地前处理自动派单'
DISPATCH_DEPLOYMENT_NAME = '自动派单'
WORKER_FLOW_NAME = '资料文件夹工单'
WORKER_DEPLOYMENT_NAME = '前处理'
DISPATCH_INTENT = PROJECT / 'runtime' / 'preprocessing-dispatch-intent.json'


class TaskStopped(Exception):
    """The task was paused, deleted, or restarted before admission."""


def _control_paths():
    config = read_json(FIRST_STEP_CONFIG)
    database = Path(config['classification_root']) / 'group-task-summaries' / 'summary-index.sqlite3'
    lock_root = Path(config['data_root']) / 'control-locks'
    return database.resolve(strict=True), lock_root.resolve(strict=True)


def _running_tasks():
    database, _ = _control_paths()
    with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=30)) as db:
        rows = db.execute("SELECT task_id,start_generation FROM tasks WHERE desired_state='running' AND start_pending=0 "
                          "AND task_status NOT IN ('deleting','delete_failed')").fetchall()
    return {task: generation for task, generation in rows}


def _v2_task_ids():
    """The legacy submitter must not create a second order for v2 tasks."""
    database, _ = _control_paths()
    with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=30)) as db:
        columns = {row[1] for row in db.execute('PRAGMA table_info(tasks)')}
        if 'prefect_order_model' not in columns:
            return set()
        return {row[0] for row in db.execute("SELECT task_id FROM tasks WHERE prefect_order_model='v2'")}


@contextmanager
def _admit_task(task_id, generation):
    database, lock_root = _control_paths()
    with FileLock(lock_root / ('preprocessing-' + str(uuid.UUID(task_id)) + '.lock'), timeout=30):
        with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=30)) as db:
            row = db.execute('SELECT desired_state,start_generation,task_status,start_pending FROM tasks WHERE task_id=?',
                             (task_id,)).fetchone()
        if (not row or row[0] != 'running' or row[1] != generation or row[3]
                or row[2] in {'deleting', 'delete_failed'}):
            raise TaskStopped()
        yield


def _queue():
    config = read_json(QUEUE_CONFIG)
    if not isinstance(config, dict) or set(config) != {'queue_db', 'receipt_root', 'journal_root'}:
        raise ValueError('已登记资料池配置无效')
    if NAMESPACE not in sys.modules:
        package = ModuleType(NAMESPACE)
        package.__path__ = [str(QUEUE_DIR.resolve(strict=True))]
        package.__package__ = NAMESPACE
        sys.modules[NAMESPACE] = package
    module = importlib.import_module(NAMESPACE + '.local_queue')
    return module.LocalQueue(config['queue_db'], require_retention=True, require_live_watcher=True)


def _file_ref(path):
    path = Path(path).resolve(strict=True)
    return {'path': str(path), 'sha256': sha_file(path)}


def _verified_aux(path, material, gaps, label):
    path = Path(path).resolve()
    expected = (material.get('file_hashes') or {}).get(str(path))
    try:
        actual = sha_file(path)
    except (OSError, ValueError):
        actual = None
    if expected and actual == expected:
        return {'path': str(path), 'sha256': actual}
    if not any(gap.get('path') == str(path) for gap in gaps):
        gaps.append({'code': 'material_reference_unavailable', 'path': str(path),
                     'message': label + '不可读取或发生变化，其他材料继续。'})
    return None


def _available_material(material, paper):
    if (not isinstance(material, dict) or
            (material.get('source') or {}).get('source_sha256') != paper['body']['sha256']):
        raise ValueError('前处理材料与领取正文身份不一致')
    available = copy.deepcopy(material)
    hashes = material.get('file_hashes') or {}
    if not isinstance(hashes, dict):
        raise ValueError('前处理材料的文件记录无效')
    invalid = set()
    for name, digest in hashes.items():
        try:
            if sha_file(name) != digest:
                invalid.add(name)
        except (OSError, TypeError, ValueError):
            invalid.add(name)
    gaps = []
    for asset in available.get('assets', []):
        name = asset.get('image_path')
        if not name:
            continue
        try:
            usable = name not in invalid and sha_file(name) == asset.get('sha256')
        except (OSError, TypeError, ValueError):
            usable = False
        if not usable:
            gaps.append({'code': 'image_unavailable', 'asset_id': asset.get('id'),
                         'position': asset.get('position') or {},
                         'message': '图片文件不可读取或发生变化，其他材料继续。'})
            asset['image_path'] = None
            asset['local_path'] = None
            invalid.discard(name)
    gaps.extend({'code': 'material_reference_unavailable', 'path': name,
                 'message': '材料引用文件不可读取或发生变化，其他材料继续。'}
                for name in sorted(invalid))
    return available, gaps


def _capacity_and_projection(material, work, backend):
    """Use the existing local image/capacity planner without loading a key."""
    if material.get('parser_version') != 'candidate-direct-clean/25':
        return material, [], {'status': 'existing_material'}
    from .body_clean_handoff import project_for_reading, plan_with_available_images
    from .body_segment_tokenizer import with_body_token_counter
    from preextract_contract import load_template
    try:
        projection, gaps = project_for_reading(material)
    except Exception as exc:
        return material, [{'code': 'reading_projection_unavailable',
                           'message': '部分阅读位置无法整理，保留现有材料。'}], {
            'status': 'unavailable', 'error_type': type(exc).__name__}
    model_dir = work / 'model'
    if model_dir.is_dir() and any(model_dir.iterdir()):
        return projection, gaps, {'status': 'existing_request_history'}
    try:
        model = read_json(backend.settings.path('model_config'))
        limits = dict(model.get('preextract_limits') or {})
        if (not limits or limits.get('model') != model.get('text_model')
                or str(limits.get('base_url', '')).rstrip('/') != str(model.get('base_url', '')).rstrip('/')
                or limits.get('supports_images') is not True):
            raise ValueError('模型容量配置尚未确认')
        connection = {**limits, 'model': model['text_model'], 'max_tokens': model.get('max_tokens', 8192)}
        planning = with_body_token_counter(connection)
        planning['context_window'] = min(int(planning['context_window']), 1000000)
        template, _ = load_template(backend.settings.path('template_path'))
        plan, _, image_gaps = plan_with_available_images(
            projection, work, template, backend.catalog, connection, planning)
        gaps += image_gaps + plan.get('partition_gaps', [])
        return projection, gaps, {'status': 'planned', 'mode': plan['mode'], 'parts': len(plan['parts'])}
    except Exception as exc:
        # Capacity is checked again by the existing model entry before sending.
        # A local planning problem must not discard otherwise readable material.
        return projection, gaps, {'status': 'unavailable', 'error_type': type(exc).__name__}


def _receiver_is_current(queue, task_id, group_id, receipt_id, require_retention=False):
    module = sys.modules[NAMESPACE + '.local_queue']
    with closing(queue._connect()) as db:
        try:
            session = module._current_receiver_session(db)
        except module.ReceiverUnavailable:
            return False
        group = db.execute('SELECT paper_id,checked_session,state,review_reason FROM groups WHERE task_id=? AND group_id=? '
                           'AND receipt_id=?', (task_id, group_id, receipt_id)).fetchone()
        if not group:
            return not require_retention  # Let a missing-record error remain visible during verification.
        if group['checked_session'] != session:
            return False
        if (group['state'] == 'needs_review' and group['review_reason'] in
                {'retention_unavailable', 'retention_unavailable_processing', 'recheck_failed'}):
            return False
        retention = db.execute('SELECT checked_session,state,last_error,retention_id FROM retention_requests '
                               'WHERE task_id=? AND group_id=? AND receipt_id=? AND consumer_id=?',
                               (task_id, group_id, receipt_id, queue.consumer_id)).fetchone()
        if not retention:
            return not require_retention
        if retention['state'] in {'reserved', 'accepted'} and retention['last_error'] == 'upstream_unavailable':
            return False
        accepted = (retention['state'] == 'accepted' and retention['retention_id'] is not None
                    and retention['last_error'] is None)
        if require_retention:
            return (group['state'] in {'waiting', 'processing'} and accepted
                    and retention['checked_session'] == session)
        return not accepted or retention['checked_session'] == session


def _verify_folder_receipt(queue, claim):
    try:
        return verify_registered_receipt(claim['receipt_path'], claim['receipt_id'])
    except ValueError as exc:
        if not _receiver_is_current(queue, claim['task_id'], claim['group_id'], claim['receipt_id']):
            raise sys.modules[NAMESPACE + '.local_queue'].ReceiverUnavailable('接收服务正在重新核对') from exc
        raise


def _prepare_claim(queue, attempt, settings, task_id, generation, group_id, receipt_id):
    completed = queue.completed_preprocessing(CONSUMER, attempt)
    if completed is not None:
        return completed
    claim = queue.claim_next(consumer_id=CONSUMER, attempt_id=attempt, task_id=task_id,
                             group_id=group_id, receipt_id=receipt_id,
                             admission=lambda: _admit_task(task_id, generation))
    if claim is None:
        with closing(queue._connect()) as db:
            current = db.execute('SELECT state FROM groups WHERE task_id=? AND group_id=? AND receipt_id=?',
                                 (task_id, group_id, receipt_id)).fetchone()
        if current and not _receiver_is_current(
                queue, task_id, group_id, receipt_id):
            raise sys.modules[NAMESPACE + '.local_queue'].ReceiverUnavailable('接收服务正在重新核对')
        raise ValueError('该资料文件夹已不在等待队列中，不能领取其他文件夹')
    verified = _verify_folder_receipt(queue, claim)
    if any(verified.get(key) != claim.get(key) for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'receipt_sha256')):
        raise ValueError('领取记录与登记资料不一致')
    paper = verified['paper']
    if not paper or not paper.get('body'):
        raise ValueError('领取记录没有正文')
    scoped = settings_for_verified_receipt(settings, verified)
    backend = UnifiedBackend(scoped, invoke=None)
    lock_root = backend.root / 'locks'
    lock_root.mkdir(parents=True, exist_ok=True)
    person_lock = lock_root / (identity(str(Path(paper['person_dir']).resolve()).casefold()) + '.lock')
    paper_lock = lock_root / ('paper-' + identity(paper['paper_id']) + '.lock')
    with FileLock(person_lock, timeout=0, thread_local=False), FileLock(paper_lock, timeout=0, thread_local=False):
        current = _verify_folder_receipt(queue, claim)
        if current['paper'] != paper or current['receipt_sha256'] != verified['receipt_sha256']:
            raise ValueError('前处理锁定期间登记资料发生变化')
        paper = backend.receive(claim['receipt_path'])
        if paper != current['paper']:
            raise ValueError('前处理接收与登记资料不一致')
        # Call the existing local adapter directly: UnifiedBackend.prepare also
        # checks model-request history, which belongs to the later LLM step.
        result = Backend.prepare(backend, paper)
        material_path = Path(result['material']).resolve(strict=True)
        work = material_path.parent
        material = read_json(material_path)
        prior_profile = Records(work).cached('profile')
        model_dir = work / 'model'
        model_history = ('complete' if prior_profile is not None else
                         'existing' if model_dir.is_dir() and any(model_dir.iterdir()) else 'not_started')
        available, integrity_gaps = _available_material(material, paper)
        projection, gaps, capacity = _capacity_and_projection(available, work, backend)
        gaps = integrity_gaps + gaps
        handoff_dir = backend.paper_root(paper) / 'local-preprocessing'
        projection_path = handoff_dir / (claim['receipt_id'] + '-reading.json')
        if projection_path.exists():
            if read_json(projection_path) != projection:
                raise ValueError('已有前处理阅读材料与当前材料不一致')
        else:
            write_json(projection_path, projection)
        manifest = {
            'schema': 'paperflow-local-preprocessing-handoff/1',
            **{key: claim[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id')},
            'consumer_id': CONSUMER, 'attempt_id': attempt,
            'receipt': {'path': claim['receipt_path'], 'sha256': claim['receipt_sha256']},
            'revision': paper['revision'], 'body': paper['body'], 'attachments': paper['attachments'],
            'body_work': str(work), 'profile_history': model_history, 'completed_profile': prior_profile,
            'material': _file_ref(material_path), 'reading_material': _file_ref(projection_path),
            'body_text': _verified_aux(work / 'private' / 'body.txt', material, gaps, '正文文本'),
            'index': _verified_aux(work / 'private' / 'index.json', material, gaps, '正文索引'),
            'gaps': gaps, 'capacity': capacity,
        }
        manifest_path = handoff_dir / (claim['receipt_id'] + '-handoff.json')
        if manifest_path.exists():
            if read_json(manifest_path) != manifest:
                raise ValueError('已有前处理交接与领取身份不一致')
        else:
            write_json(manifest_path, manifest)
        handoff = {'manifest_path': str(manifest_path.resolve()), 'manifest_sha256': sha_file(manifest_path)}
        queue.complete_preprocessing(CONSUMER, attempt, handoff)
    return {'status': 'waiting_for_llm', 'task_id': task_id, 'group_id': group_id,
            'paper_id': claim['paper_id'], 'receipt_id': claim['receipt_id'],
            'attempt_id': attempt, **handoff}


def _folder_key(task_id, group_id):
    return 'folder-' + identity(['local-preprocessing-folder/1', task_id, group_id])


def prepare_folder(settings, task_id, group_id, receipt_id, attempt_id):
    """Prepare only the folder named by this order, including any recovery."""
    task_id = str(uuid.UUID(task_id))
    if any(not isinstance(value, str) or not value for value in (group_id, receipt_id)):
        raise ValueError('缺少资料文件夹或交付回执身份')
    if not isinstance(attempt_id, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', attempt_id):
        raise ValueError('前处理领取编号无效')
    queue = _queue()
    lock_root = settings.path('data_root') / 'locks'
    lock_root.mkdir(parents=True, exist_ok=True)
    try:
        with FileLock(lock_root / ('local-preprocess-' + attempt_id + '.lock'), timeout=0, thread_local=False):
            with closing(queue._connect()) as db:
                old = db.execute('SELECT task_id,group_id,receipt_id FROM claims '
                                 'WHERE consumer_id=? AND attempt_id=?', (CONSUMER, attempt_id)).fetchone()
            if old and tuple(old) != (task_id, group_id, receipt_id):
                raise ValueError('前处理领取记录不属于该资料文件夹')
            completed = queue.completed_preprocessing(CONSUMER, attempt_id)
            if completed is not None:
                if any(completed[key] != value for key, value in (
                        ('task_id', task_id), ('group_id', group_id), ('receipt_id', receipt_id))):
                    raise ValueError('已完成的交接不属于该资料文件夹')
                return completed
            generation = _running_tasks().get(task_id)
            if type(generation) is not int:
                return {'status': 'paused'}
            try:
                return _prepare_claim(queue, attempt_id, settings, task_id, generation, group_id, receipt_id)
            except TaskStopped:
                return {'status': 'paused'}
            except Timeout:
                return {'status': 'busy'}
            except Exception as exc:
                if isinstance(exc, sys.modules[NAMESPACE + '.local_queue'].ReceiverUnavailable):
                    return {'status': 'receiver_wait'}
                queue.record_preprocessing_failure(CONSUMER, attempt_id, exc)
                raise
    except Timeout:
        return {'status': 'busy'}


def _bound_folder_order(queue, run_id, task_id, group_id, receipt_id, attempt_id):
    """Check the durable binding before an early-scheduled worker may claim files."""
    with closing(queue._connect()) as db:
        if db.execute('SELECT 1 FROM cancelled_tasks WHERE task_id=?', (task_id,)).fetchone():
            raise ValueError('任务已删除，文件夹工单不能领取')
        row = db.execute('SELECT receipt_id,attempt_id,prefect_run_id FROM preprocessing_folder_orders '
                         'WHERE task_id=? AND group_id=?', (task_id, group_id)).fetchone()
    if row is None:
        return False
    if row['receipt_id'] != receipt_id or row['attempt_id'] != attempt_id:
        raise ValueError('文件夹工单与登记身份不一致')
    if row['prefect_run_id'] is None:
        return False
    if row['prefect_run_id'] != str(run_id):
        raise ValueError('文件夹工单与绑定的 Prefect 运行不一致')
    return True


@flow(name=WORKER_FLOW_NAME, description='一张工单处理一个资料文件夹，完成后停在等待 LLM；不调用模型。',
      persist_result=True, retries=0, log_prints=False)
async def local_preprocessing_handoff(resume_attempt: str | None = None, task_id: str | None = None,
                                      generation: int | None = None, group_id: str | None = None,
                                      receipt_id: str | None = None, attempt_id: str | None = None,
                                      folder_label: str | None = None):
    from prefect.client.orchestration import get_client
    from prefect.context import FlowRunContext
    from prefect.flow_runs import asuspend_flow_run
    from .unified_flows import runtime_identity

    if not all((task_id, group_id, receipt_id, attempt_id)):
        raise ValueError('请从等待前处理队列创建指定资料文件夹的工单')
    if resume_attempt is not None and resume_attempt != attempt_id:
        raise ValueError('恢复编号与资料文件夹工单不一致')
    if not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', attempt_id):
        raise ValueError('前处理领取编号无效')
    context = FlowRunContext.get()
    queue = _queue()
    while True:
        for _ in range(20):
            if await asyncio.to_thread(_bound_folder_order, queue, context.flow_run.id,
                                       task_id, group_id, receipt_id, attempt_id):
                break
            await asyncio.sleep(.5)
        else:
            async with get_client() as api:
                await api.update_flow_run(context.flow_run.id,
                    name='资料文件夹 ' + (folder_label or group_id)[:80] + ' · 等待绑定')
            await asuspend_flow_run(timeout=None, key=uuid.uuid4().hex)
            continue
        break
    settings = Settings({**Settings.load().values, 'execution_enabled': True})
    settings.require_execution()
    runtime_identity(settings)
    lock_root = settings.path('data_root') / 'locks'
    lock_root.mkdir(parents=True, exist_ok=True)
    execution_lock = FileLock(lock_root / ('folder-order-' + attempt_id + '.lock'), timeout=0, thread_local=False)
    while True:
        try:
            execution_lock.acquire()
            break
        except Timeout:
            await asyncio.sleep(2)
    try:
        async with flows.async_concurrency(PREPROCESSING_LIMIT, occupy=1, strict=True):
            result = await drain_thread(prepare_folder, settings, task_id, group_id, receipt_id, attempt_id)

        status = result.get('status')
        if status not in {'waiting_for_llm', 'already_handed_off', 'paused', 'busy', 'receiver_wait'}:
            raise ValueError('资料文件夹前处理返回了未知状态')
        label = folder_label or group_id
        phase = ('等待 LLM' if status in {'waiting_for_llm', 'already_handed_off'} else
                 '资料暂忙' if status == 'busy' else '等待资料复核' if status == 'receiver_wait' else '等待任务开始')
        context = FlowRunContext.get()
        async with get_client() as api:
            await api.update_flow_run(context.flow_run.id, name='资料文件夹 ' + label[:80] + ' · ' + phase)
        # The same Prefect run remains open; no LLM or attachment continuation is enabled.
        # A fresh pause key makes an accidental resume stop here again.
        await asuspend_flow_run(timeout=None, key=uuid.uuid4().hex)
    finally:
        execution_lock.release()


def _folder_candidates(queue):
    if queue.snapshot(include_in_progress=False)['receiver_status'] != 'running':
        return []
    with closing(queue._connect()) as db:
        session = db.execute("SELECT value FROM metadata WHERE key='receiver_session_id'").fetchone()
        if not session or not session[0]:
            return []
        rows = db.execute("""
            SELECT g.task_id,g.group_id,g.receipt_id,g.record,c.attempt_id,
                   o.receipt_id AS order_receipt_id,o.attempt_id AS order_attempt_id,o.prefect_run_id
              FROM groups g
              LEFT JOIN claims c ON c.task_id=g.task_id AND c.group_id=g.group_id
                   AND c.receipt_id=g.receipt_id AND c.consumer_id=?
              LEFT JOIN preprocessing_failures f ON f.consumer_id=c.consumer_id AND f.attempt_id=c.attempt_id
              LEFT JOIN preprocessing_folder_orders o ON o.task_id=g.task_id AND o.group_id=g.group_id
             WHERE ((g.state='waiting' AND c.attempt_id IS NULL AND NOT EXISTS (
                       SELECT 1 FROM claims any_claim WHERE any_claim.task_id=g.task_id
                       AND any_claim.group_id=g.group_id AND any_claim.receipt_id=g.receipt_id))
                    OR (g.state IN ('waiting','processing') AND c.state IN ('checking','processing')
                        AND f.attempt_id IS NULL))
               AND NOT EXISTS(SELECT 1 FROM cancelled_tasks t WHERE t.task_id=g.task_id)
               AND g.checked_session=? AND EXISTS (
                   SELECT 1 FROM retention_requests r WHERE r.task_id=g.task_id AND r.group_id=g.group_id
                   AND r.paper_id=g.paper_id AND r.receipt_id=g.receipt_id AND r.consumer_id=?
                   AND r.state='accepted' AND r.retention_id IS NOT NULL AND r.last_error IS NULL
                   AND r.checked_session=?)
             ORDER BY g.rowid
        """, (CONSUMER, session[0], queue.consumer_id, session[0])).fetchall()
    v2_tasks = _v2_task_ids()
    return [dict(row) for row in rows if row['task_id'] not in v2_tasks]


def _folder_parameters(item):
    task_id, group_id = item['task_id'], item['group_id']
    attempt = item.get('order_attempt_id') or item.get('attempt_id') or identity(['local-folder-attempt/1', task_id, group_id])
    label = item.get('folder_label')
    if not label:
        try:
            record = json.loads(item['record'])
            label = Path(read_json(record['receipt_path'])['group_root']).name
        except (OSError, ValueError, KeyError, TypeError):
            label = group_id
    label = re.sub(r'[\x00-\x1f\x7f]', ' ', str(label)).strip()[:80] or group_id
    return {'task_id': task_id, 'group_id': group_id, 'receipt_id': item['receipt_id'],
            'attempt_id': attempt, 'folder_label': label}


async def _submit_folder(api, worker, item):
    from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterIdempotencyKey

    key = _folder_key(item['task_id'], item['group_id'])
    params = _folder_parameters(item)
    queue_name = 'task-' + str(uuid.UUID(item['task_id']))
    matches = await api.read_flow_runs(flow_run_filter=FlowRunFilter(
        idempotency_key=FlowRunFilterIdempotencyKey(any_=[key])), limit=1)
    run = matches[0] if matches else await api.create_flow_run_from_deployment(
        worker.id, parameters=params, idempotency_key=key,
        work_queue_name=queue_name,
        name='资料文件夹 ' + params['folder_label'] + ' · 等待前处理')
    if run.deployment_id != worker.id or run.work_queue_name != queue_name or any((run.parameters or {}).get(k) != params[k]
            for k in ('task_id', 'group_id', 'receipt_id', 'attempt_id')):
        raise ValueError('已有工单与资料文件夹身份不一致')
    return run


def _reserve_folder_order(queue, item):
    params = _folder_parameters(item)
    expected = (item['receipt_id'], params['attempt_id'])
    with closing(queue._connect()) as db, db:
        db.execute('BEGIN IMMEDIATE')
        db.execute('INSERT OR IGNORE INTO preprocessing_folder_orders '
                   '(task_id,group_id,receipt_id,attempt_id,folder_label,created_at) '
                   'VALUES(?,?,?,?,?,?)', (item['task_id'], item['group_id'], *expected,
                                          params['folder_label'], datetime.now(timezone.utc).isoformat()))
        saved = db.execute('SELECT receipt_id,attempt_id FROM preprocessing_folder_orders '
                           'WHERE task_id=? AND group_id=?', (item['task_id'], item['group_id'])).fetchone()
        if not saved or tuple(saved) != expected:
            raise ValueError('资料文件夹已有不同的 Prefect 工单记录')


def _pending_folder_orders(queue):
    with closing(queue._connect()) as db:
        rows = db.execute('SELECT o.task_id,o.group_id,o.receipt_id,o.attempt_id,o.folder_label,o.prefect_run_id '
                          'FROM preprocessing_folder_orders o JOIN groups g ON g.task_id=o.task_id '
                          'AND g.group_id=o.group_id AND g.receipt_id=o.receipt_id '
                          'WHERE o.prefect_run_id IS NULL AND NOT EXISTS(SELECT 1 FROM cancelled_tasks t '
                          'WHERE t.task_id=o.task_id) ORDER BY o.created_at,o.task_id,o.group_id').fetchall()
    v2_tasks = _v2_task_ids()
    return [dict(row) for row in rows if row['task_id'] not in v2_tasks]


def _record_folder_order(queue, item, run):
    params = _folder_parameters(item)
    expected = (item['receipt_id'], params['attempt_id'], str(run.id))
    with closing(queue._connect()) as db, db:
        db.execute('BEGIN IMMEDIATE')
        db.execute('UPDATE preprocessing_folder_orders SET prefect_run_id=? WHERE task_id=? AND group_id=? '
                   'AND receipt_id=? AND attempt_id=? AND prefect_run_id IS NULL',
                   (str(run.id), item['task_id'], item['group_id'], item['receipt_id'], params['attempt_id']))
        saved = db.execute('SELECT receipt_id,attempt_id,prefect_run_id FROM preprocessing_folder_orders '
                           'WHERE task_id=? AND group_id=?', (item['task_id'], item['group_id'])).fetchone()
        if not saved or tuple(saved) != expected:
            raise ValueError('资料文件夹已有不同的 Prefect 工单记录')


async def _ensure_task_queue(api, task_id, running):
    from prefect.exceptions import ObjectAlreadyExists, ObjectNotFound
    name = 'task-' + str(uuid.UUID(task_id))
    try:
        queue = await api.read_work_queue_by_name(name, work_pool_name=PREPROCESSING_POOL)
    except ObjectNotFound:
        try:
            queue = await api.create_work_queue(name, is_paused=True, work_pool_name=PREPROCESSING_POOL)
        except ObjectAlreadyExists:
            queue = await api.read_work_queue_by_name(name, work_pool_name=PREPROCESSING_POOL)
    paused = task_id not in running
    if queue.is_paused != paused:
        await api.update_work_queue(queue.id, is_paused=paused)
        queue = await api.read_work_queue_by_name(name, work_pool_name=PREPROCESSING_POOL)
        if queue.is_paused != paused:
            raise RuntimeError('资料任务的 Prefect 队列状态未同步')
    return queue


def _order_waits_for_local(queue, run):
    params = run.parameters or {}
    with closing(queue._connect()) as db:
        row = db.execute("""
            SELECT 1 FROM preprocessing_folder_orders o JOIN groups g
              ON g.task_id=o.task_id AND g.group_id=o.group_id AND g.receipt_id=o.receipt_id
            WHERE o.prefect_run_id=? AND o.task_id=? AND o.group_id=? AND o.receipt_id=? AND o.attempt_id=?
              AND g.state IN ('waiting','processing') AND NOT EXISTS (
                  SELECT 1 FROM preprocessing_failures f WHERE f.consumer_id=? AND f.attempt_id=o.attempt_id)
        """, (str(run.id), params.get('task_id'), params.get('group_id'), params.get('receipt_id'),
              params.get('attempt_id'), CONSUMER)).fetchone()
    return bool(row and _receiver_is_current(queue, params['task_id'], params['group_id'], params['receipt_id'], True))


def _completed_order(queue, lock_root, run):
    params = run.parameters or {}
    attempt = params.get('attempt_id')
    if not isinstance(attempt, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', attempt):
        return False
    with closing(queue._connect()) as db:
        row = db.execute('SELECT task_id,group_id,receipt_id,attempt_id FROM preprocessing_folder_orders '
                         'WHERE prefect_run_id=?', (str(run.id),)).fetchone()
    if not row or tuple(row) != tuple(params.get(key) for key in ('task_id', 'group_id', 'receipt_id', 'attempt_id')):
        return False
    try:
        with FileLock(lock_root / ('local-preprocess-' + attempt + '.lock'), timeout=0, thread_local=False):
            completed = queue.completed_preprocessing(CONSUMER, attempt)
            return bool(completed and all(completed[key] == params[key]
                        for key in ('task_id', 'group_id', 'receipt_id')))
    except Timeout:
        return False


async def _reconcile_folder_orders(api, worker, queue, running, lock_root, check_failed):
    from prefect.client.schemas.filters import (FlowRunFilter, FlowRunFilterDeploymentId,
        FlowRunFilterName, FlowRunFilterState, FlowRunFilterStateType)
    from prefect.client.schemas.objects import StateType
    from prefect.client.schemas.responses import SetStateStatus
    from prefect.states import Scheduled
    from prefect import get_run_logger

    base = FlowRunFilterDeploymentId(any_=[worker.id])
    now = datetime.now(timezone.utc)
    for phase, minimum_wait in (('等待任务开始', 0), ('等待资料复核', 0), ('资料暂忙', 30)):
        paused_filter = FlowRunFilter(deployment_id=base, name=FlowRunFilterName(like_=phase),
            state=FlowRunFilterState(type=FlowRunFilterStateType(any_=[StateType.PAUSED])))
        offset = 0
        paused_runs = []
        while True:
            paused = await api.read_flow_runs(flow_run_filter=paused_filter, limit=200, offset=offset)
            paused_runs.extend(paused)
            if len(paused) < 200:
                break
            offset += len(paused)
        for run in paused_runs:
            params = run.parameters or {}
            if params.get('task_id') not in running or not run.state or not run.state.timestamp:
                continue
            if (now - run.state.timestamp).total_seconds() < minimum_wait:
                continue
            if await asyncio.to_thread(_order_waits_for_local, queue, run):
                result = await api.resume_flow_run(run.id)
                if result.status != SetStateStatus.ACCEPT:
                    get_run_logger().warning('等待前处理工单暂未恢复：%s', run.id)

    if not check_failed:
        return
    failed_filter = FlowRunFilter(deployment_id=base,
        state=FlowRunFilterState(type=FlowRunFilterStateType(any_=[StateType.FAILED, StateType.CRASHED])))
    offset = 0
    failed_runs = []
    while True:
        failed = await api.read_flow_runs(flow_run_filter=failed_filter, limit=200, offset=offset)
        failed_runs.extend(failed)
        if len(failed) < 200:
            break
        offset += len(failed)
    for run in failed_runs:
        attempt = (run.parameters or {}).get('attempt_id')
        if not isinstance(attempt, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', attempt):
            continue
        execution_lock = FileLock(lock_root / ('folder-order-' + attempt + '.lock'), timeout=0, thread_local=False)
        try:
            execution_lock.acquire()
            completed = await asyncio.to_thread(_completed_order, queue, lock_root, run)
            if completed:
                result = await api.set_flow_run_state(run.id, Scheduled(name='恢复已交接工单'))
                if result.status != SetStateStatus.ACCEPT:
                    get_run_logger().warning('已交接工单状态暂未恢复：%s', run.id)
        except Timeout:
            continue
        except (OSError, ValueError, KeyError):
            get_run_logger().exception('已交接工单核对失败：%s', run.id)
        finally:
            if execution_lock.is_locked:
                execution_lock.release()


@flow(name=DISPATCH_FLOW_NAME, description='根据任务的开始、暂停和删除状态，为资料文件夹补位。',
      persist_result=False, retries=0, log_prints=False)
async def auto_preprocessing_dispatch():
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import WorkPoolUpdate
    from prefect import get_run_logger
    from .unified_flows import runtime_identity

    settings = Settings({**Settings.load().values, 'execution_enabled': True})
    settings.require_execution()
    runtime_identity(settings)
    queue = _queue()
    blocked = set()
    last_reconcile = 0.0
    async with get_client() as api:
        worker = await api.read_deployment_by_name(WORKER_FLOW_NAME + '/' + WORKER_DEPLOYMENT_NAME)
        while True:
            running = await asyncio.to_thread(_running_tasks)
            receiver = await asyncio.to_thread(queue.snapshot, include_in_progress=False)
            effective_running = running if receiver['receiver_status'] == 'running' else {}
            candidates = await asyncio.to_thread(_folder_candidates, queue)
            known_queues = []
            offset = 0
            while True:
                page = await api.read_work_queues(work_pool_name=PREPROCESSING_POOL, limit=200, offset=offset)
                known_queues.extend(page)
                if len(page) < 200:
                    break
                offset += len(page)
            for known in known_queues:
                if known.name.startswith('task-'):
                    await _ensure_task_queue(api, known.name[5:], effective_running)

            unreserved = [item for item in candidates if not item['prefect_run_id']
                          and _folder_key(item['task_id'], item['group_id']) not in blocked][:32]
            for item in unreserved:
                try:
                    await asyncio.to_thread(_reserve_folder_order, queue, item)
                except ValueError:
                    blocked.add(_folder_key(item['task_id'], item['group_id']))
                    get_run_logger().exception('资料文件夹已有不同的工单，保留记录等待核对')
            pending = [item for item in await asyncio.to_thread(_pending_folder_orders, queue)
                       if _folder_key(item['task_id'], item['group_id']) not in blocked][:32]
            for task in {item['task_id'] for item in pending}:
                await _ensure_task_queue(api, task, effective_running)
            pool = await api.read_work_pool(PREPROCESSING_POOL)
            if pool.is_paused:
                await api.update_work_pool(PREPROCESSING_POOL, WorkPoolUpdate(is_paused=False))

            for item in pending:
                key = _folder_key(item['task_id'], item['group_id'])
                try:
                    run = await _submit_folder(api, worker, item)
                    await asyncio.to_thread(_record_folder_order, queue, item, run)
                except ValueError:
                    blocked.add(key)
                    get_run_logger().exception('资料文件夹已有工单身份不一致，保留记录等待核对')
            check_failed = time.monotonic() - last_reconcile >= 30
            await _reconcile_folder_orders(api, worker, queue, effective_running, settings.path('data_root') / 'locks',
                                           check_failed)
            if check_failed:
                last_reconcile = time.monotonic()
            await asyncio.sleep(2)


async def submit_one_from_queue():
    """Submit the same folder order as the dispatcher and return its actual state."""
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.objects import StateType

    queue = _queue()
    running = await asyncio.to_thread(_running_tasks)
    receiver = await asyncio.to_thread(queue.snapshot, include_in_progress=False)
    if receiver['receiver_status'] != 'running':
        return {'status': 'waiting_for_receiver'}
    candidates = await asyncio.to_thread(_folder_candidates, queue)
    async with get_client() as api:
        worker = await api.read_deployment_by_name(WORKER_FLOW_NAME + '/' + WORKER_DEPLOYMENT_NAME)
        found_problem = False
        for item in candidates:
            if item['task_id'] not in running:
                continue
            await _ensure_task_queue(api, item['task_id'], running)
            await asyncio.to_thread(_reserve_folder_order, queue, item)
            run = await _submit_folder(api, worker, item)
            await asyncio.to_thread(_record_folder_order, queue, item, run)
            if run.state and run.state.type in {StateType.FAILED, StateType.CRASHED,
                                               StateType.CANCELLED, StateType.COMPLETED}:
                found_problem = True
                continue
            params = _folder_parameters(item)
            completed = await asyncio.to_thread(queue.completed_preprocessing, CONSUMER, params['attempt_id'])
            if completed is not None:
                if any(completed[key] != params[key] for key in ('task_id', 'group_id', 'receipt_id')):
                    raise ValueError('前处理交接与资料文件夹不一致')
                return completed
            return {'status': 'submitted', 'flow_run_id': str(run.id),
                    'task_id': item['task_id'], 'group_id': item['group_id']}
    return {'status': 'needs_attention' if found_problem else 'empty'}
