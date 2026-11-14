"""Pause one parent after 20 distinct folders fail fresh model requests."""
import asyncio
import hashlib
from http.client import IncompleteRead, RemoteDisconnected
import logging
from urllib.error import URLError
from uuid import uuid4

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import VariableCreate, VariableUpdate
from prefect.concurrency.sync import concurrency as sync_concurrency

from .admission import _SESSION, read_variables_sync
from .receipts import save_receipt_sync

logger = logging.getLogger(__name__)
LIMIT = 20


def counter_name(task_id):
    return 'paperflow_v3_model_failures_' + str(task_id).replace('-', '_')


def _epoch(client, task_id, values=None):
    suffix = str(task_id).replace('-', '_')
    task_key, intent_key = 'paperflow_v3_task_' + suffix, 'paperflow_v3_intent_' + suffix
    if values is None:
        values = read_variables_sync(client, [_SESSION, task_key, intent_key])
    session, admission, intent = values.get(_SESSION), values.get(task_key), values.get(intent_key)
    if (session is None or admission is None or intent is None
            or admission.value != {'state': 'running', 'session': session.value}):
        return None
    value = intent.value
    if (value.get('action') != 'start' or value.get('target', {}).get('task_id') != str(task_id)
            or not value.get('request_id') or not isinstance(value.get('intent_seq'), int)):
        return None
    return {key: value[key] for key in ('request_id', 'intent_seq')} | {'session': session.value}


def capture_model_epoch_sync(task_id):
    try:
        with get_client(sync_client=True) as client:
            suffix = str(task_id).replace('-', '_')
            values = read_variables_sync(client, [_SESSION, 'paperflow_v3_task_' + suffix,
                'paperflow_v3_intent_' + suffix, counter_name(task_id)])
            epoch = _epoch(client, task_id, values)
            saved = values.get(counter_name(task_id)) if epoch is not None else None
            if (saved is not None and saved.value.get('epoch') == epoch
                    and len(saved.value['failed_folders']) >= LIMIT):
                # Reconcile only the idempotent pause, never replay a model POST.
                record_model_result_sync(task_id, saved.value['failed_folders'][-1], 'pause-confirmation',
                                         success=False, epoch=epoch)
                raise InterruptedError('连续20组模型请求失败，保护暂停尚未放行新请求')
            return epoch
    except InterruptedError:
        raise
    except Exception as exc:
        logger.warning('Model protection identity unavailable: %s', type(exc).__name__)
        raise InterruptedError('模型请求准入尚未确认，未发送新请求') from None


def is_model_request_error(exc):
    # HTTPError is an URLError too. Do not count content/metadata exceptions.
    if isinstance(exc, (URLError, TimeoutError, ConnectionError, IncompleteRead, RemoteDisconnected)):
        return True
    from preextract_request import ModelExplicitError
    return isinstance(exc, ModelExplicitError)


def record_model_result_sync(task_id, folder_id, request_id, *, success, epoch):
    if epoch is None:
        return False
    paused = False
    try:
        with get_client(sync_client=True) as client:
            gate = 'paperflow-v3-control-' + str(task_id).replace('-', '')
            limit = client.read_global_concurrency_limit_by_name(gate)
            if limit.limit != 1 or not limit.active or limit.slot_decay_per_second != 0:
                raise RuntimeError('Invalid parent control gate')
            with sync_concurrency(gate, occupy=1, strict=True):
                suffix = str(task_id).replace('-', '_')
                name = counter_name(task_id)
                values = read_variables_sync(client, [_SESSION, 'paperflow_v3_task_' + suffix,
                    'paperflow_v3_intent_' + suffix, name])
                if _epoch(client, task_id, values) != epoch:
                    return False
                saved = values.get(name)
                value = saved.value if saved is not None and saved.value.get('epoch') == epoch else {
                    'epoch': epoch, 'failed_folders': [], 'recent_events': []}
                event = hashlib.sha256((str(request_id) + ':' + str(success)).encode()).hexdigest()[:32]
                if ((event in value['recent_events'] and len(value['failed_folders']) < LIMIT)
                        or success and not value['failed_folders']):
                    return False
                folders = (value['failed_folders'] if len(value['failed_folders']) >= LIMIT else
                           [] if success else list(dict.fromkeys([*value['failed_folders'], str(folder_id)])))
                data = {'epoch': epoch, 'failed_folders': folders[-LIMIT:],
                        'recent_events': [*value['recent_events'], event][-40:]}
                if saved is None:
                    client.create_variable(VariableCreate(name=name, value=data))
                else:
                    client.update_variable(VariableUpdate(name=name, value=data))
                if len(folders) >= LIMIT:
                    admission_name = 'paperflow_v3_task_' + str(task_id).replace('-', '_')
                    try:
                        client.update_variable(VariableUpdate(name=admission_name,
                            value={'state': 'paused', 'session': None}))
                    except Exception:
                        observed = client.read_variable_by_name(admission_name)
                        if observed is None or observed.value != {'state': 'paused', 'session': None}:
                            raise
                    paused = True
    except Exception as exc:
        # A counter/API fault must not reclassify or retry the business request.
        logger.warning('Model protection observation not saved: %s', type(exc).__name__)
        return False
    if paused:
        # Keep the durable reason tied to the triggering epoch, without
        # retaining the short parent control gate during artifact I/O.
        try:
            save_receipt_sync(task_id=task_id, folder_id='task', child_flow_run_id=task_id,
                event_id=uuid4(), kind='dependency_failure', data={
                    'status': 'active', 'dependency': 'model', 'folder_id': str(folder_id),
                    'reason': '连续20组模型请求失败，已保护暂停',
                    'consecutive_failed_folders': LIMIT, 'control_epoch': epoch})
        except Exception as exc:
            logger.warning('Model pause reason not saved: %s', type(exc).__name__)
    return paused


async def record_model_result(task_id, folder_id, request_id, *, success, epoch):
    return await asyncio.to_thread(record_model_result_sync, task_id, folder_id, request_id,
                                   success=success, epoch=epoch)
