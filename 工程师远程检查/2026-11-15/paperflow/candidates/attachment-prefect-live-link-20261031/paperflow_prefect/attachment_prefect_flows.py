"""Independent Prefect flows for retained attachment descriptions."""
import asyncio
import os
from pathlib import Path
import time
from uuid import UUID, uuid4

from . import flows
from prefect import flow
from prefect.client.orchestration import get_client
from prefect.runtime import flow_run
from .attachment_queue import AttachmentQueue, paper_is_available, schedule_ready, settings_for_verified_receipt
from .records import identity, read_json, sha_file, write_json
from .settings import PROJECT, Settings

JOB_FLOW_NAME = '附件直通-单篇说明'
JOB_DEPLOYMENT_NAME = '已登记附件'
SCAN_FLOW_NAME = '附件直通-等待池接续'
SCAN_DEPLOYMENT_NAME = '自动接续'


def queue_settings():
    value = read_json(PROJECT/'attachment-queue-config.json')
    if not isinstance(value, dict) or set(value) != {'queue_db', 'receipt_root', 'journal_root'}:
        raise ValueError('附件接续配置无效')
    return value


def source_files():
    files = {str(path.relative_to(PROJECT)): sha_file(path)
             for path in sorted((PROJECT/'paperflow_prefect').rglob('*.py'))}
    for name in ('attachment-queue-config.json', 'manage_attachment.py', 'run_attachment_service.py'):
        files[name] = sha_file(PROJECT/name)
    config_path = Path(os.environ.get('PAPERFLOW_CONFIG', PROJECT/'config.json')).resolve()
    files['configured-settings:'+str(config_path)] = sha_file(config_path)
    return files


LOADED_FILES = source_files()
LOADED_AT = time.time()


def runtime_identity():
    files = source_files()
    if files != LOADED_FILES:
        raise RuntimeError('附件执行器代码发生变化，须重新加载')
    return {'pid': os.getpid(), 'source_root': str(PROJECT), 'loaded_at': LOADED_AT,
            'source_sha256': identity(files), 'disk_matches_loaded': True}


def checked_queue():
    config = queue_settings()
    return AttachmentQueue(config['queue_db'], config['receipt_root'])


def record_retryable_before_request(receipt_id):
    """Only mark failures proven to precede the attachment model request."""
    run_id = flow_run.id
    if run_id:
        value = {'schema': 'attachment-run-observation/1', 'run_id': str(run_id),
                 'receipt_id': receipt_id, 'retryable': True}
        write_json(Path(queue_settings()['journal_root'])/'outcomes'/(str(UUID(str(run_id)))+'.json'), value)


async def check_runtime():
    runtime = runtime_identity()
    await asyncio.to_thread(flows.verify_limits, Settings.load())
    queue = checked_queue()
    ready = await asyncio.to_thread(queue.eligible)
    result = {'status': 'attachment_check_passed', 'runtime': runtime,
              'eligible_groups_with_attachments': sum(not row.get('_invalid') for row in ready),
              'invalid_queue_records': sum(bool(row.get('_invalid')) for row in ready), 'real_model_requests': 0,
              'prefect_limits_checked': True,
              'flow_run_id': str(flow_run.id) if flow_run.id else None}
    write_json(PROJECT/'runtime'/'attachment-check.json', result)
    return result


def save_scan_result(result):
    run_id = str(UUID(str(flow_run.id))) if flow_run.id else str(uuid4())
    result = dict(result, flow_run_id=run_id)
    write_json(PROJECT/'runtime'/'attachment-scans'/(run_id+'.json'), result)
    write_json(PROJECT/'runtime'/'attachment-scan-last.json', result)
    return result


@flow(name=JOB_FLOW_NAME, persist_result=False, retries=0, log_prints=False)
async def attachment_job(receipt_id: str = '', run_mode: str = 'check'):
    if run_mode == 'check':
        return await check_runtime()
    if run_mode != 'process':
        raise ValueError('附件处理模式无效')
    runtime_identity()
    settings = Settings.load()
    queue = checked_queue()
    # Recheck after the Prefect queue delay, immediately before this paper starts.
    try:
        record = await asyncio.to_thread(queue.verified, receipt_id)
    except (OSError, ValueError, KeyError, TypeError):
        record_retryable_before_request(receipt_id)
        raise
    scoped = settings_for_verified_receipt(settings, record)
    try:
        result = await flows.received_handoff_with_settings(scoped, record['receipt_path'], attachments_only=True)
    except flows.ReceivedPaperBusy:
        record_retryable_before_request(receipt_id)
        raise
    if result.get('status') != 'stage3_complete':
        raise flows.PaperIncomplete('附件业务结果尚未完成')
    return dict(result, runtime=runtime_identity())


@flow(name=SCAN_FLOW_NAME, persist_result=False, retries=0, log_prints=False)
async def attachment_scan(run_mode: str = 'check'):
    if run_mode == 'check':
        return await check_runtime()
    if run_mode != 'process':
        raise ValueError('附件接续模式无效')
    runtime = None
    try:
        runtime = runtime_identity()
        config = queue_settings()
        queue = checked_queue()
        async with get_client() as api:
            deployment = await api.read_deployment_by_name(JOB_FLOW_NAME+'/'+JOB_DEPLOYMENT_NAME)
            async def submit(deployment_id, *, parameters, idempotency_key):
                run = await api.create_flow_run_from_deployment(UUID(str(deployment_id)), parameters=parameters,
                                                                 idempotency_key=idempotency_key)
                return str(run.id)
            async def observe(run_id):
                run = await api.read_flow_run(UUID(str(run_id)))
                state = run.state_type.value if run.state_type is not None else 'UNKNOWN'
                result = {'state': state, 'run_id': str(run_id), 'retryable': False}
                marker = Path(config['journal_root'])/'outcomes'/(str(UUID(str(run_id)))+'.json')
                if state in {'FAILED', 'CRASHED', 'CANCELLED'} and marker.is_file():
                    value = read_json(marker)
                    if (value.get('schema') == 'attachment-run-observation/1'
                            and value.get('run_id') == str(run_id) and value.get('retryable') is True):
                        result.update(retryable=True, receipt_id=value.get('receipt_id'))
                return result
            data_root = Settings.load().path('data_root')
            result = await schedule_ready(queue, config['journal_root'], str(deployment.id), submit, observe,
                                          available=lambda record: paper_is_available(record, data_root))
        result = dict(result, status='incomplete' if result['failed'] else 'complete', runtime=runtime_identity())
    except Exception as exc:
        save_scan_result({'status': 'incomplete', 'failed': 1,
                          'errors': [{'error_type': type(exc).__name__}], 'runtime': runtime})
        raise
    result = save_scan_result(result)
    if result['failed']:
        raise flows.BatchIncomplete('部分附件未完成调度，详情已记录')
    return result
