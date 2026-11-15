"""One model request using the existing raw journal and new task admission."""
import asyncio
from contextlib import contextmanager, nullcontext
from pathlib import Path
import time
from prefect.concurrency.sync import concurrency as sync_concurrency

from paperflow_prefect.llm_queue.raw_request import _body_hash, run_raw_request
from paperflow_prefect.records import identity as fingerprint, read_json, sha_file
from paperflow_prefect.resources import drain_thread
from paperflow_v3.control.admission import admitted_slot
from paperflow_v3.control.stage_queue import StageQueueWait
from paperflow_v3.control.model_protection import (capture_model_epoch_sync, is_model_request_error,
                                                 record_model_result)
from paperflow_v3.control.receipts import read_receipts, save_receipt
from paperflow_v3.control.retry import uncertain_model_receipt
from . import gateway_admission as gateway


class _StopOnPause:
    def __init__(self, admission):
        self.admission = admission

    def is_set(self):
        return not self.admission.running_sync()

    def wait(self, seconds=None):
        deadline = None if seconds is None else time.monotonic() + max(seconds, 0)
        while not self.is_set():
            if deadline is not None and time.monotonic() >= deadline:
                return False
            time.sleep(.05 if deadline is None else min(.05, max(0, deadline - time.monotonic())))
        return True


class _TransportAdmission:
    def __init__(self, admission, on_started, *, task_id):
        self.admission = admission
        self.on_started = on_started
        self.task_id = str(task_id)

    def start_transport(self, mark_started):
        # Slow receipt persistence is outside the parent's control gate. A
        # pause accepted while it saves must still prevent physical dispatch.
        self.on_started()
        root = self.admission
        while hasattr(root, 'parent'):
            root = root.parent
        gate = 'paperflow-v3-control-' + self.task_id.replace('-', '')
        with sync_concurrency(gate, occupy=1, strict=True):
            if not root.running_sync() or not self.admission.running_sync():
                raise InterruptedError('大工单已暂停，未发送模型请求')
            mark_started()


def normalize_model_history(records, identity=None):
    """Project a verified model-not-dispatched outcome for existing resume code.

    Stored receipts and native http_invoked=True stay unchanged. Only this
    in-memory view can treat the model request as eligible for a new attempt.
    """
    from paperflow_v3.control.attempts import active_model_records
    records = active_model_records(records, identity)
    result = []
    for row in records:
        if row.get('kind') == gateway.KIND or (
                row.get('kind') == 'llm_request_not_sent' and row.get('data', {}).get('gateway_binding')):
            gateway.verify_receipt(row['data'], identity)
            row = {**row, 'kind': 'llm_request_not_sent'}
        result.append(row)
    return result


def request_outcome(transport):
    """Distinguish a completely evidenced HTTP failure from an unknown result."""
    if transport.get('http_invoked') is False:
        return 'llm_request_not_sent', {}
    from preextract_scheduler_adapter import request_observation

    model = Path(transport['work_dir'])
    try:
        if transport.get('gateway_binding') is not None and (model / gateway.PROOF_FILE).is_file():
            proof = gateway.verify(model, transport['gateway_binding'])
            return gateway.KIND, proof
        phase, _, proof = request_observation(model)
        state = read_json(model / 'request-state.json')
        events, error = model / 'response-events.jsonl', model / 'http-error.txt'
        status = state.get('http_status')
        if (status == 429 or isinstance(status, int) and 500 <= status <= 599):
            return 'llm_request_unknown', {'http_status': status,
                'request_state_sha256': proof['request_state_sha256'],
                **({'error_response_sha256': state['error_response_sha256']}
                   if error.is_file() and state.get('error_response_sha256') == sha_file(error) else {})}
        rejected = {400, 401, 402, 403, 404, 405, 409, 413, 415, 422}
        if (status in rejected and state.get('error_type') == 'HTTPError'
                and proof.get('reason') in {'explicit_transient_response', 'non_retryable_http'}
                and not (model / 'response.txt').exists()
                and (not events.exists() or events.stat().st_size == 0)
                and error.is_file() and state.get('error_response_sha256') == sha_file(error)):
            reason = ('http_payload_too_large' if status == 413 else
                      'http_rejection' if status < 500 else 'explicit_transient_response')
            return 'llm_request_failed', {'failure_reason': reason, 'retryable': status != 413,
                'request_state_sha256': proof['request_state_sha256'],
                'error_response_sha256': state['error_response_sha256'], 'http_status': status}
        response = model / 'response.txt'
        if (phase == 'material_review' and proof.get('reason') == 'output_length_requires_changed_plan'
                and response.is_file() and events.is_file()
                and state.get('response_sha256') == sha_file(response)):
            return 'llm_request_failed', {'failure_reason': proof['reason'], 'retryable': False,
                'request_state_sha256': proof['request_state_sha256'],
                'partial_response_sha256': state['response_sha256']}
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return 'llm_request_unknown', {}


def verify_request_history(records, component_id, request_id, model_dir, *, retry_of=None, resume_not_sent=False):
    """Use server identities to decide whether this exact request may resume."""
    model = Path(model_dir)
    from .body_plan_revision import check_revised_request
    check_revised_request(records, component_id, request_id, model)
    records = normalize_model_history(records)
    prior = [row for row in records if row.get('data', {}).get('component_id') == component_id
             and row.get('kind') in {'llm_request_intent', 'llm_request_started', 'llm_request_not_sent', 'llm_request_failed',
                                     'llm_request_unknown', 'llm_response_saved'}]
    latest = {}
    for row in prior:
        latest.setdefault(row['data']['request_id'], row['kind'])
    from paperflow_v3.control.attempts import replacement_for, MARKER
    grant = replacement_for(records, component_id)
    historical_replacement = any(row.get('kind') == MARKER and
        row['data']['component_id'] == component_id for row in records)
    if historical_replacement and grant is None and latest.get(request_id) != 'llm_response_saved':
        if retry_of is None or latest.get(retry_of) != 'llm_request_failed':
            raise RuntimeError('旧重跑授权不能用于另一次首次发送')
    if grant is not None and not resume_not_sent and latest.get(request_id) != 'llm_response_saved':
        expected_id = fingerprint(['manual-llm-retry/1', component_id, grant['retry_id']])
        expected_model = Path(grant['retry_work_root']) / expected_id / 'model'
        if (retry_of is None or replacement_for(records, component_id, request_id=retry_of) is None
                or request_id != expected_id or model.resolve() != expected_model.resolve()):
            raise RuntimeError('明确重跑只能使用已授权的新请求及专用目录')
    if (any(uncertain_model_receipt(row['kind'], row['data']) for row in prior)
            or any(state in {'llm_request_intent', 'llm_request_started', 'llm_request_unknown'}
                   for state in latest.values())):
        raise RuntimeError('该处理项的原请求仍待核对，不能重新发送')
    journal_exists = any((model / name).exists() for name in
                         ('request.json', 'request-state.json', 'response.txt', 'response-events.jsonl', 'http-error.txt'))
    if not prior and journal_exists:
        raise RuntimeError('本机请求没有此工单的服务器绑定，待核对')
    if retry_of is not None:
        from paperflow_v3.control.attempts import replacement_for
        replacement = None if resume_not_sent else replacement_for(records, component_id, request_id=retry_of)
        if replacement is not None:
            if request_id != fingerprint(['manual-llm-retry/1', component_id, replacement['retry_id']]):
                raise RuntimeError('明确重跑的新请求身份不一致')
        parent_state = 'llm_request_not_sent' if resume_not_sent else 'llm_request_failed'
        if ((resume_not_sent and any(row['kind'] == 'llm_request_unknown' for row in prior))
                or (replacement is None and latest.get(retry_of) != parent_state)
                or any(state == 'llm_response_saved' and key != request_id for key, state in latest.items())
                or latest.get(request_id) not in {None, 'llm_request_not_sent', 'llm_response_saved'}):
            raise RuntimeError('原请求结果待核对或不允许人工重试')
        if latest.get(request_id) == 'llm_response_saved':
            saved = next(row['data'] for row in prior if row['kind'] == 'llm_response_saved'
                         and row['data']['request_id'] == request_id)
            if (Path(saved['response_path']).resolve().parent != model.resolve()
                    or not (model / 'request-state.json').is_file()):
                raise RuntimeError('人工重试的原响应记录待核对')
            return False
        if journal_exists:
            raise RuntimeError('人工重试已有本机请求记录，待核对')
        return True
    completed = {key for key, state in latest.items() if state == 'llm_response_saved'}
    not_sent = {key for key, state in latest.items() if state == 'llm_request_not_sent'}
    unknown = {row['data']['request_id'] for row in prior if row['kind'] == 'llm_request_unknown'}
    resumable = ({request_id} if request_id in not_sent - unknown and not journal_exists else set())
    if any(row['data']['request_id'] not in completed | resumable for row in prior):
        raise RuntimeError('该处理项的原请求仍待核对，不能重新发送')
    if completed:
        if completed != {request_id}:
            raise RuntimeError('该处理项存在多个响应身份，待核对')
        saved = next(row['data'] for row in prior if row['kind'] == 'llm_response_saved'
                     and row['data']['request_id'] == request_id) if request_id in completed else None
        if (not saved or Path(saved['response_path']).resolve().parent != model.resolve()
                or not (model / 'request-state.json').is_file()):
            raise RuntimeError('该处理项已有响应，原请求记录待核对')
    return not completed


async def run_raw_component(*, identity, component_id, request_id, kind, request_body_factory,
                            connection, work, admission, limit_names, check_history, check_source,
                            opener=None, retry_of=None, retry_id=None, resume_not_sent=False):
    """Save one original request/reply; do not parse or start postprocessing.

    The caller provides the same child FlowRun and component/request identities
    on resume, its verified handoff/history checks, and the configured shared
    Prefect limit names. The existing raw adapter owns request journaling and
    cached/unknown-request handling; this function does not start a queue.
    """
    from preextract_request import request_once

    loop = asyncio.get_running_loop()
    stop = _StopOnPause(admission)
    current = {}
    transport_state = {}
    gateway_binding = gateway.binding(identity, component_id, request_id, kind)
    receipt_identity = dict(task_id=identity['task_id'], folder_id=identity['group_id'],
                            child_flow_run_id=identity['task_run_id'])

    @contextmanager
    def admit(transport):
        if stop.is_set():
            raise InterruptedError('大工单已暂停，未发送模型请求')
        current.update(component_id=component_id, request_id=request_id, kind=kind,
                       request_sha256=transport['request_sha256'], model_dir=transport['work_dir'])
        if retry_of is not None:
            current.update(retry_of=retry_of, retry_id=retry_id)
            if resume_not_sent:
                previous = next((row['data'] for row in records
                    if row.get('data', {}).get('request_id') == retry_of), {})
                current['resume_reason'] = 'gateway_not_dispatched' if previous.get('gateway_binding') else 'not_sent'
        transport_state['current'] = transport
        transport['gateway_binding'] = gateway_binding
        transport_state['epoch'] = capture_model_epoch_sync(identity['task_id'])
        transport_state['started_saved'] = False
        future = asyncio.run_coroutine_threadsafe(save_receipt(**receipt_identity,
            event_id=str(request_id) + '-intent', kind='llm_request_intent', data=dict(current)), loop)
        future.result()
        def started():
            future = asyncio.run_coroutine_threadsafe(save_receipt(**receipt_identity,
                event_id=str(request_id) + '-started', kind='llm_request_started', data=dict(current)), loop)
            future.result()
            transport_state['started_saved'] = True
        yield _TransportAdmission(admission, started, task_id=identity['task_id'])

    async def reuse(records):
        from .handoff_inputs import _saved_response
        from paperflow_prefect.llm_queue.work_locks import work_locks
        from preextract_scheduler_adapter import request_observation

        saved = next(row['data'] for row in records if row.get('kind') == 'llm_response_saved'
                     and row.get('data', {}).get('component_id') == component_id
                     and row['data'].get('request_id') == request_id)

        def validate():
            with work_locks(work, kind):
                if stop.is_set():
                    raise InterruptedError('大工单已暂停，未继续处理')
                check_history()
                check_source()
                refs = _saved_response({'model_dir': str(Path(work) / 'model'),
                    'request_sha256': saved['request_sha256'], 'response_sha256': saved['response_sha256']},
                    request_observation)
                request = request_body_factory()
                check_source()
                response_path = Path(refs['response']['path'])
                if (not isinstance(request, dict) or _body_hash(request) != saved['request_sha256']
                        or saved.get('kind') != kind or saved.get('status') != 'raw_response_saved'
                        or Path(saved['response_path']).resolve() != response_path
                        or not isinstance(saved.get('response_text'), str)
                        or saved['response_text'].encode('utf8') != response_path.read_bytes()):
                    raise ValueError('已保存的模型请求或响应身份不一致')
                if stop.is_set():
                    raise InterruptedError('大工单已暂停，未继续处理')
                return {key: saved[key] for key in
                        ('status', 'kind', 'request_sha256', 'response_path', 'response_sha256')}

        return await drain_thread(validate)

    await admission.wait()
    records = normalize_model_history(await read_receipts(identity['task_run_id']), identity)
    await admission.wait()
    if not verify_request_history(records, component_id, request_id, Path(work) / 'model',
                                  retry_of=retry_of, resume_not_sent=resume_not_sent):
        return await reuse(records)

    reuse_saved = False
    try:
        async with admitted_slot(admission, limit_names):
            # Capacity waiting does not reserve history. Recheck after waiting.
            records = normalize_model_history(await read_receipts(identity['task_run_id']), identity)
            reuse_saved = not verify_request_history(records, component_id, request_id, Path(work) / 'model',
                retry_of=retry_of, resume_not_sent=resume_not_sent)
            if not reuse_saved:
                def request_with_gateway(*args, **kwargs):
                    # Native _perform_one owns the before/after-send boundary.
                    from paperflow_v3.control.stage_queue import current_phase
                    if phase := current_phase():
                        kwargs['stop'] = phase.fenced_stop(kwargs['stop'])
                    kwargs['opener'] = gateway.GatewayOpener(connection=connection, model_dir=Path(work) / 'model',
                        request_binding=gateway_binding, stop=kwargs['stop'], delegate=opener)
                    return request_once(*args, **kwargs)
                result = await drain_thread(lambda: run_raw_request(kind=kind,
                    request_body_factory=request_body_factory, connection=connection, work=work, stop=stop,
                    request_once=request_with_gateway, lease=lambda _kind, _stop: nullcontext(), admit=admit,
                    check_history=check_history, check_source=check_source))
    except StageQueueWait:
        raise
    except Exception as exc:
        if current:
            outcome, proof = request_outcome(transport_state['current'])
            if (transport_state.get('started_saved')
                    and transport_state['current'].get('http_invoked') is True
                    and outcome != gateway.KIND and is_model_request_error(exc)):
                await record_model_result(identity['task_id'], identity['group_id'], request_id,
                    success=False, epoch=transport_state['epoch'])
            await save_receipt(**receipt_identity, event_id=str(request_id) + '-outcome',
                kind=outcome, data={**current, **proof, 'error_type': type(exc).__name__})
            if outcome == gateway.KIND:
                prior_waits = sum(bool(row.get('data', {}).get('gateway_binding')) and
                    row.get('data', {}).get('component_id') == component_id for row in records)
                raise gateway.GatewayCapacityWait(gateway.retry_delay(proof, prior_waits)) from None
        raise
    if reuse_saved:
        return await reuse(records)
    await record_model_result(identity['task_id'], identity['group_id'], request_id,
        success=True, epoch=transport_state.get('epoch'))
    response = {'component_id': component_id, 'request_id': request_id, **result,
                'response_text': Path(result['response_path']).read_bytes().decode('utf8')}
    if retry_of is not None:
        response.update(retry_of=retry_of, retry_id=retry_id)
        if resume_not_sent:
            response['resume_reason'] = current['resume_reason']
    await save_receipt(**receipt_identity, event_id=str(request_id) + '-response',
                       kind='llm_response_saved', data=response)
    return result


async def retry_raw_component(*, identity, component_id, retry_id, kind, original_work, original_model,
                              expected_request_sha256, connection, admission, limit_names, check_source,
                              opener=None, check_history=None):
    """Explicitly retry an evidenced failed request without changing its journal."""
    records = normalize_model_history(await read_receipts(identity['task_run_id']), identity)
    manual = next((row['data'] for row in records if row.get('kind') == 'manual_retry'), {})
    if manual.get('retry_id') != retry_id or manual.get('state') not in {'scheduled', 'running'}:
        raise RuntimeError('缺少本篇已确认的人工重试')
    from paperflow_v3.control.attempts import replacement_for
    replacement = replacement_for(records, component_id, retry_id)
    if replacement is not None:
        work = Path(original_work).resolve()
        old_model = Path(replacement['model_dir']).resolve()
        retry_root = work / 'retries' / fingerprint(component_id)
        if (replacement['identity'] != identity or replacement['kind'] != kind
                or replacement['request_sha256'] != expected_request_sha256
                or retry_root != Path(replacement['retry_work_root']).resolve()
                or (old_model != Path(original_model).resolve() and not old_model.is_relative_to(retry_root))):
            raise ValueError('明确重跑的原请求不属于当前处理项')

        def checked_original():
            from paperflow_prefect.profile_recovery import _checked_request
            frozen = replacement['journal_files']
            known = {'request.json', 'request-state.json', 'response.txt', 'response-events.jsonl',
                     'http-error.txt', 'gateway-not-dispatched.json'}
            if {name for name in known if (old_model / name).is_file()} != set(frozen):
                raise ValueError('明确重跑前原请求文件清单发生变化')
            if any(sha_file(old_model / name) != value for name, value in frozen.items()):
                raise ValueError('明确重跑前原请求记录发生变化')
            body = _checked_request(old_model)
            if _body_hash(body) != expected_request_sha256 or body.get('model') != connection.get('model'):
                raise ValueError('明确重跑前原请求或模型发生变化')
            return body

        request_id = fingerprint(['manual-llm-retry/1', component_id, retry_id])
        result = await run_raw_component(identity=identity, component_id=component_id, request_id=request_id,
            kind=kind, request_body_factory=checked_original, connection=connection,
            work=retry_root / request_id, admission=admission, limit_names=limit_names,
            check_history=check_history or (lambda: None), check_source=check_source, opener=opener,
            retry_of=replacement['request_id'], retry_id=retry_id)
        return {**result, 'request_id': request_id}
    prior = [row for row in records if row.get('data', {}).get('component_id') == component_id
             and row.get('kind') in {'llm_request_intent', 'llm_request_started', 'llm_request_not_sent', 'llm_request_failed',
                                     'llm_request_unknown', 'llm_response_saved'}]
    latest = {}
    for row in prior:
        latest.setdefault(row['data']['request_id'], row['kind'])
    if (any(uncertain_model_receipt(row['kind'], row['data']) for row in prior)
            or any(state in {'llm_request_intent', 'llm_request_started', 'llm_request_unknown'} for state in latest.values())):
        raise RuntimeError('原请求结果待核对，不能人工重发')
    failure = next((row['data'] for row in prior if row['kind'] == 'llm_request_failed'
                    and latest[row['data']['request_id']] == 'llm_request_failed'), None)
    if not failure:
        raise RuntimeError('没有可确认的失败请求供人工重试')
    if failure.get('retryable') is not True:
        raise RuntimeError('该失败需要调整材料或处理计划，不能原样重发')
    work = Path(original_work).resolve()
    failed_model = Path(failure['model_dir']).resolve()
    retry_root = work / 'retries' / fingerprint(component_id)
    if failed_model != Path(original_model).resolve() and not failed_model.is_relative_to(retry_root):
        raise ValueError('失败请求不属于当前处理项')

    def check_prior():
        from paperflow_prefect.profile_recovery import _checked_request
        outcome, proof = request_outcome({'http_invoked': True, 'work_dir': str(failed_model)})
        request = _checked_request(failed_model)
        if (outcome != 'llm_request_failed' or failure['request_sha256'] != expected_request_sha256
                or _body_hash(request) != expected_request_sha256 or request.get('model') != connection.get('model')
                or any(proof.get(key) != failure.get(key) for key in
                       ('request_state_sha256', 'error_response_sha256', 'failure_reason', 'retryable'))):
            raise ValueError('已确认失败的原请求证据发生变化')
        return request

    request_id = fingerprint(['manual-llm-retry/1', component_id, retry_id])
    result = await run_raw_component(identity=identity, component_id=component_id, request_id=request_id,
        kind=kind, request_body_factory=check_prior, connection=connection, work=retry_root / request_id,
        admission=admission, limit_names=limit_names, check_history=lambda: None, check_source=check_source,
        opener=opener, retry_of=failure['request_id'], retry_id=retry_id)
    return {**result, 'request_id': request_id}


async def resume_not_sent_component(*, identity, component_id, kind, original_work, original_model,
                                    expected_request_sha256, connection, admission, limit_names,
                                    check_source, opener=None):
    """Resume a proved no-model-send without overwriting its original journal."""
    records = normalize_model_history(await read_receipts(identity['task_run_id']), identity)
    prior = [row for row in records if row.get('data', {}).get('component_id') == component_id
             and row.get('kind') in {'llm_request_intent', 'llm_request_started', 'llm_request_not_sent', 'llm_request_failed',
                                     'llm_request_unknown', 'llm_response_saved'}]
    latest = {}
    for row in prior:
        latest.setdefault(row['data']['request_id'], row['kind'])
    if any(state in {'llm_request_intent', 'llm_request_started', 'llm_request_unknown'} for state in latest.values()):
        raise RuntimeError('原请求结果待核对，不能继续发送')
    if any(uncertain_model_receipt(row['kind'], row['data']) for row in prior):
        raise RuntimeError('原请求结果待核对，不能继续发送')
    previous = pending_not_sent_request(records, component_id)
    if not previous:
        raise RuntimeError('未找到带完整记录的未发送请求')
    work = Path(original_work).resolve()
    previous_model = Path(previous['model_dir']).resolve()
    retry_root = work / 'retries' / fingerprint(component_id)
    if previous_model != Path(original_model).resolve() and not previous_model.is_relative_to(retry_root):
        raise ValueError('未发送请求不属于当前处理项')
    state_sha256 = sha_file(previous_model / 'request-state.json')

    def check_prior():
        from paperflow_prefect.profile_recovery import _checked_request
        state = read_json(previous_model / 'request-state.json')
        request = _checked_request(previous_model)
        events = previous_model / 'response-events.jsonl'
        if previous.get('gateway_binding'):
            gateway.verify_receipt(previous, identity)
            if (gateway.endpoint(connection) != gateway.ENDPOINT
                    or previous['request_sha256'] != expected_request_sha256
                    or _body_hash(request) != expected_request_sha256
                    or request.get('model') != connection.get('model')):
                raise ValueError('网关等待请求或模型入口发生变化')
            return request
        if (sha_file(previous_model / 'request-state.json') != state_sha256
                or state.get('status') != 'request_failed_or_uncertain' or state.get('http_invoked') is not False
                or state.get('client_http_requests') != 0 or not state.get('finished_at')
                or previous['request_sha256'] != expected_request_sha256
                or state.get('request_sha256') != expected_request_sha256
                or _body_hash(request) != expected_request_sha256 or request.get('model') != connection.get('model')
                or (previous_model / 'response.txt').exists() or (previous_model / 'http-error.txt').exists()
                or (events.exists() and events.stat().st_size != 0)):
            raise ValueError('未发送请求的原始证据发生变化')
        return request

    request_id = fingerprint(['not-sent-resume/1', component_id, previous['request_id'], state_sha256])
    result = await run_raw_component(identity=identity, component_id=component_id, request_id=request_id,
        kind=kind, request_body_factory=check_prior, connection=connection, work=retry_root / request_id,
        admission=admission, limit_names=limit_names, check_history=lambda: None, check_source=check_source,
        opener=opener, retry_of=previous['request_id'], resume_not_sent=True)
    return {**result, 'request_id': request_id}


def pending_not_sent_request(records, component_id):
    """Select a no-send journal whose successor has never been recorded.

    An explicit replacement marker still identifies its frozen old request.
    Its no-send ancestor must not select that already-used retry directory.
    Current-attempt no-send journals remain eligible for ordinary resumption.
    """
    latest = {}
    known_requests = set()
    consumed = set()
    kinds = {'llm_request_intent', 'llm_request_started', 'llm_request_not_sent',
             'llm_request_failed', 'llm_request_unknown', 'llm_response_saved'}
    for row in records:
        data = row.get('data') or {}
        if data.get('component_id') != component_id:
            continue
        if data.get('request_id'):
            known_requests.add(data['request_id'])
        if row.get('kind') in kinds:
            latest.setdefault(data['request_id'], row)
            if data.get('retry_of'):
                consumed.add(data['retry_of'])
    for row in latest.values():
        data = row['data']
        journal = Path(data.get('model_dir', '')) / 'request-state.json'
        if row['kind'] != 'llm_request_not_sent' or not journal.is_file():
            continue
        successor = fingerprint(['not-sent-resume/1', component_id,
                                 data['request_id'], sha_file(journal)])
        if data['request_id'] not in consumed and successor not in known_requests:
            return data
    return None
