"""Native executor evidence only: no probe, transport, task control or disk writes."""
from __future__ import annotations

import copy
import json
import math
import re
import time
from pathlib import Path
from urllib.parse import urlsplit

from upload_tracking import service_key

HEARTBEAT_SOURCE_SHA256 = '519958be3fc33d9d5f3e9fc38eae44e23aa24f5629e86fc5d7a0f047c239561f'


def _number(value):
    return type(value) in (int, float) and math.isfinite(value)


def _fresh(value, now, maximum):
    return _number(value) and 0 <= now - value <= maximum


def _worker(row):
    return (isinstance(row, dict) and type(row.get('pid')) is int and row['pid'] > 0
            and isinstance(row.get('worker_id'), str) and bool(row['worker_id'].strip())
            and re.fullmatch(r'[1-9][0-9]*', str(row.get('starttime', ''))) is not None
            and isinstance(row.get('state'), str)
            and row['state'] in {'R', 'S', 'D', 'I', 'T', 't', 'Z', 'X', 'x'})


def _complete_worker(row):
    if not _worker(row) or row.get('thread_scan_complete') is not True or row.get('broker_consumer_registered') is not True:
        return False
    threads = row.get('thread_states')
    if (not isinstance(threads, dict) or not threads or str(row['pid']) not in threads
            or any(not isinstance(tid, str) or not tid.isdecimal() or not isinstance(state, str)
                   or state not in {'R', 'S', 'D', 'I', 'T', 't'} for tid, state in threads.items())):
        return False
    frozen = row['state'] in {'T', 't'}
    return all((state in {'T', 't'}) == frozen for state in threads.values())


def _heartbeat(row, now):
    stamp = row.get('heartbeat_at')
    maximum = row.get('heartbeat_max_age_seconds')
    interval = row.get('heartbeat_interval_seconds')
    valid = (row.get('heartbeat_process_binding') == 'pid_name_boot_at_after_kernel_start'
             and _number(maximum) and maximum > 0 and _number(interval) and interval > 0
             and _fresh(stamp, now, maximum))
    return dict(worker_id=row['worker_id'], pid=row['pid'], starttime=row['starttime'],
        heartbeat_at=stamp if _number(stamp) else None,
        heartbeat_age_seconds=now-stamp if _number(stamp) else None,
        heartbeat_interval_seconds=interval, heartbeat_max_age_seconds=maximum,
        heartbeat_valid_until=stamp+maximum if valid else None, valid=bool(valid),
        heartbeat_process_binding=row.get('heartbeat_process_binding'))


def native_runtime_readiness(observation=None, *, service, pipeline_id=None, now=None, max_age_seconds=10):
    """Normalize a service-bound snapshot. HTTP reachability is never readiness."""
    now = time.time() if now is None else now
    if not _number(now) or not _number(max_age_seconds) or max_age_seconds <= 0:
        raise ValueError('invalid native runtime observation clock')
    result = dict(schema='rag-native-runtime-readiness/1', state='unknown', ready=False,
        scope='native_execution_admission', service=service, pipeline_id=pipeline_id,
        identity=None, observed_at=None, evaluated_at=now, age_seconds=None,
        max_age_seconds=max_age_seconds, fresh=False, observed_state='unknown',
        reason_code='runtime_observation_missing', reason='缺少绑定执行器的运行观察',
        active_documents=[], heartbeat_evidence=[], ready_valid_until=None)

    def finish(state, code, reason):
        result.update(state=state, ready=state == 'ready', reason_code=code, reason=reason)
        return result

    if not isinstance(observation, dict):
        return result
    if observation.get('schema') != 'rag-native-runtime-observation/1':
        return finish('unknown', 'runtime_observation_schema', '运行观察格式尚未接入，不能以API可达替代')
    observed = observation.get('observed_at')
    result.update(observed_at=observed if _number(observed) else None,
        age_seconds=now-observed if _number(observed) else None,
        fresh=_fresh(observed, now, max_age_seconds),
        startup_enabled=observation.get('startup_enabled'))
    try:
        expected = service_key(service)
        bound = observation.get('service')
        service_matches = isinstance(bound, str) and bool(bound) and service_key(bound) == expected
    except (ValueError, TypeError, AttributeError):
        service_matches = False
    if not service_matches or (pipeline_id is not None and observation.get('pipeline_id') != pipeline_id):
        return finish('unknown', 'runtime_service_identity', '运行观察的服务或管线与当前原生绑定不一致')
    identity = observation.get('identity')
    if (not isinstance(identity, dict)
            or any(not isinstance(identity.get(k), str) or not identity[k].strip()
                    for k in ('server', 'container_id', 'container_started_at', 'boot_id', 'pid_namespace', 'stream', 'consumer_group'))
            or identity['server'] != urlsplit(service).hostname):
        return finish('unknown', 'runtime_executor_identity', '缺少匹配的服务器、容器或消费队列身份')
    result.update(service=expected, identity=copy.deepcopy(identity))
    if observation.get('collection_complete') is not True or observation.get('collection_errors'):
        return finish('unknown', 'runtime_collection_incomplete', '原生观察不完整，不能从部分worker推断就绪或暂停')
    if observation.get('heartbeat_source_sha256') != HEARTBEAT_SOURCE_SHA256:
        return finish('unknown', 'runtime_heartbeat_source_unconfirmed', '原生心跳源码契约缺失或漂移，未推测有效期')
    workers = observation.get('workers')
    if (not isinstance(workers, list) or any(not _complete_worker(row) for row in workers)
            or len({row['pid'] for row in workers}) != len(workers)
            or len({row['worker_id'] for row in workers}) != len(workers)):
        return finish('unknown', 'runtime_worker_inventory_incomplete', 'worker或完整线程/消费组身份缺失、重复或不一致')
    valid = workers
    live = [row for row in valid if row['state'] in {'R', 'S', 'D', 'I'}]
    frozen = bool(valid) and all(row['state'] in {'T', 't'} for row in valid)
    disabled = not valid and observation.get('startup_enabled') is False
    result['worker_identities'] = [{k: row[k] for k in ('pid', 'starttime', 'state', 'worker_id')} for row in valid]
    result['observed_state'] = 'paused' if frozen or disabled else 'unknown'
    if not result['fresh']:
        return finish('unknown', 'runtime_observation_stale', '运行观察缺失、过期或来自未来时间，保留历史状态')
    if frozen:
        return finish('paused', 'executor_frozen', '原生任务执行器已停止在T状态，API接收不代表执行')
    if disabled:
        return finish('paused', 'executor_disabled', '未观察到运行worker且执行器启动入口明确关闭')
    if not live:
        return finish('unknown', 'executor_not_observed', '尚未观察到身份完整的可运行执行器')
    if len(live) != len(valid):
        return finish('unknown', 'runtime_worker_states_mixed', 'worker运行与冻结状态混合，不能推测整体已就绪')
    heartbeat_evidence = [_heartbeat(row, now) for row in live]
    result['heartbeat_evidence'] = heartbeat_evidence
    heartbeat = any(row['valid'] for row in heartbeat_evidence)
    cursor = observation.get('consumer_progress')
    advancing = False
    if (isinstance(cursor, dict) and _fresh(cursor.get('observed_at'), now, max_age_seconds)
            and _fresh(cursor.get('before_observed_at'), now, max_age_seconds)
            and 0 < cursor['observed_at'] - cursor['before_observed_at'] <= max_age_seconds
            and cursor.get('stream') == identity['stream'] and cursor.get('consumer_group') == identity['consumer_group']):
        before, after = cursor.get('before_id'), cursor.get('after_id')
        if all(isinstance(v, str) and re.fullmatch(r'[0-9]+-[0-9]+', v) for v in (before, after)):
            advancing = tuple(map(int, after.split('-'))) > tuple(map(int, before.split('-')))
    if not heartbeat and not advancing:
        return finish('unknown', 'executor_activity_unconfirmed', 'worker存在，但缺少绑定心跳或消费推进的当前证据')
    deadlines = [row['heartbeat_valid_until'] for row in heartbeat_evidence if row['valid']]
    if advancing:deadlines.append(cursor['before_observed_at']+max_age_seconds)
    result['ready_valid_until'] = min(observed+max_age_seconds, max(deadlines))
    rows = observation.get('active_documents')
    if isinstance(rows, list):
        result['active_documents'] = [copy.deepcopy(row) for row in rows
            if isinstance(row, dict) and _fresh(row.get('observed_at'), now, max_age_seconds)
            and all(isinstance(row.get(k), str) and row[k] for k in ('dataset_id', 'document_id', 'task_id'))
            and row.get('pipeline_id') == observation.get('pipeline_id')
            and row.get('evidence') == 'native_heartbeat_reported_current_task'
            and any(evidence['valid'] and row.get('worker_id') == evidence['worker_id']
                    and row['observed_at'] == evidence['heartbeat_at'] for evidence in heartbeat_evidence)]
    result['observed_state'] = 'ready'
    return finish('ready', 'executor_ready', '绑定执行器存在且有新鲜心跳或消费推进；不代表目标已交付')


def readiness_for_client(client=None, config=None):
    """Read the existing observer's cache/provider; never invent or refresh evidence."""
    service, pipeline = '', None
    try:
        service = getattr(config, 'native_service', None) or getattr(client, 'base_url', '')
        pipeline = getattr(config, 'native_pipeline_id', None)
        provider = getattr(client, 'native_runtime_observation', None)
        if provider is None:
            provider = getattr(config, 'native_runtime_observation', None)
        observation = provider() if callable(provider) else provider
        return native_runtime_readiness(observation, service=service, pipeline_id=pipeline)
    except Exception as exc:
        result = native_runtime_readiness(service=service, pipeline_id=pipeline)
        result.update(reason_code='runtime_observer_error', reason='运行观察器不可用，未开放新原生写入', error_type=type(exc).__name__)
        return result


def runtime_observation_for_client(client, fallback_path=None):
    """Read the shared observer cache for ordinary standalone upload workers too."""
    provider = getattr(client, 'native_runtime_observation', None)
    if provider is not None:
        return provider() if callable(provider) else provider
    if fallback_path is not None:
        return json.loads(Path(fallback_path).read_bytes())
    return None


class NativeExecutionUnavailable(InterruptedError):
    """Admission is deferred, not a paper-quality rejection or a failed upload."""
    def __init__(self, readiness):
        self.readiness = copy.deepcopy(readiness)
        self.reason_code = readiness['reason_code']
        self.reconciliation_required = True
        super().__init__('native executor ' + readiness['state'] + ': ' + readiness['reason'])


def require_runtime_ready(client=None, config=None):
    readiness = readiness_for_client(client, config)
    if readiness['state'] != 'ready':
        raise NativeExecutionUnavailable(readiness)
    return readiness


def native_document_execution(document, runtime, *, dataset_id, document_id, now=None):
    """Separate a workflow receipt from the executor and last-reported document state."""
    now = time.time() if now is None else now
    run = str(document.get('run'))
    progress = document.get('progress')
    error = run in {'4', 'FAIL', '-1', '2', 'CANCEL'} or (_number(progress) and progress < 0)
    done = run in {'3', 'DONE'} and _number(progress) and progress >= 1
    lines = [line.strip() for line in str(document.get('progress_msg') or '').splitlines() if line.strip()]
    queued = bool(lines and re.search(r'\b(?:task\s+(?:is\s+)?)?queued\b', lines[-1], re.I))
    reported = 'execution_error' if error else 'completed_pending_readback' if done else 'queued' if queued else 'unknown'
    service_state = runtime['state'] if _fresh(runtime.get('observed_at'), now, runtime.get('max_age_seconds', 10)) else 'unknown'
    if service_state == 'ready' and (_number(runtime.get('ready_valid_until')) and now > runtime['ready_valid_until']):
        service_state = 'unknown'
    state = reported if error or done else 'service_paused' if service_state == 'paused' else reported
    if not error and not done and service_state != 'paused':
        active = runtime.get('active_documents', []) if service_state == 'ready' else []
        if run in {'1', 'RUNNING'} and any(row['dataset_id'] == dataset_id and row['document_id'] == document_id
                and _fresh(row.get('observed_at'), now, runtime.get('max_age_seconds', 10)) for row in active):
            state = 'executing'
    return dict(state=state, reported_state=reported, observed_at=now,
        service_state=service_state, runtime_observed_at=runtime.get('observed_at'),
        reason_code=('document_execution_error' if error else 'document_complete_pending_readback' if done
            else runtime['reason_code'] if state == 'service_paused' else 'document_active_identity' if state == 'executing'
            else 'document_queue_reported' if queued else 'document_execution_unconfirmed'),
        evidence_scope='original_document_and_bound_executor', queue_is_last_report_only=queued)


class NativeReadbackPending(TimeoutError):
    """The local wait ended; retain the exact target and never start a fresh upload."""
    def __init__(self, identity, execution):
        self.identity = copy.deepcopy(identity)
        self.execution = copy.deepcopy(execution)
        self.reconciliation_required = True
        self.retry_upload_allowed = False
        super().__init__('native terminal readback deadline; reconciliation required')
