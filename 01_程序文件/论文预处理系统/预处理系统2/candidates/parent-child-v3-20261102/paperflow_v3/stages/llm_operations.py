"""Concrete body and attachment request construction for one folder."""
from pathlib import Path
from contextlib import contextmanager

from paperflow_prefect.body_clean_handoff import plan_with_available_images, save_projection, saved_projection
from paperflow_prefect.body_segment_tokenizer import with_body_token_counter
from paperflow_prefect.llm_queue.attachment_raw_input import check_attachment_source, prepare_attachment_body
from paperflow_prefect.llm_queue.folder_completion import expected_components
from paperflow_prefect.llm_queue.frozen_body_plan import partition_plan, single_plan
from paperflow_prefect.llm_queue.image_evidence import verify_body_images
from paperflow_prefect.llm_queue.raw_request import _body_hash
from paperflow_prefect.llm_queue.raw_partition import run_partition_raw
from paperflow_prefect.llm_queue.segment_plan_contract import verify_plan
from paperflow_prefect.profile_recovery import INTENT, _other_revisions
from paperflow_prefect.records import identity as fingerprint, read_json, sha_file, write_json
from paperflow_prefect.request_admission import bind_work, require_model_history
from paperflow_prefect.resources import drain_thread
from paperflow_v3.control.admission import admitted_sync_slot
from paperflow_v3.control.stage_queue import StageQueueWait
from paperflow_v3.control.model_protection import (capture_model_epoch_sync, is_model_request_error,
                                                 record_model_result, record_model_result_sync)
from paperflow_v3.control.receipts import read_receipts, read_receipts_sync, save_receipt, save_receipt_sync
from paperflow_v3.control.retry import uncertain_model_receipt
from .handoff_inputs import _saved_response
from .llm_request import (_StopOnPause, _TransportAdmission, request_outcome,
                          normalize_model_history, resume_not_sent_component, retry_raw_component,
                          run_raw_component, verify_request_history, pending_not_sent_request)
from . import gateway_admission as gateway
from paperflow_v3.control.attempts import replacement_for


def _confirm_history(backend, paper, row, work):
    """Reuse only a confirmation audited in this same synchronous operation."""
    confirmed = backend.confirm_history(paper, row)
    if confirmed is None:
        require_model_history(paper, row, work)


def _saved_retry(records, component_id):
    from preextract_scheduler_adapter import request_observation

    rows = [row for row in records if row.get('data', {}).get('component_id') == component_id
            and row.get('kind') in {'llm_request_intent', 'llm_request_started', 'llm_request_not_sent', 'llm_request_failed',
                                    'llm_request_unknown', 'llm_response_saved'}]
    latest = {}
    for row in rows:
        latest.setdefault(row['data']['request_id'], row['kind'])
    if (any(uncertain_model_receipt(row['kind'], row['data']) for row in rows)
            or any(state in {'llm_request_intent', 'llm_request_started', 'llm_request_unknown'} for state in latest.values())):
        raise RuntimeError('该处理项原请求结果待核对')
    saved = next((row['data'] for row in rows if row['kind'] == 'llm_response_saved'
                  and (row['data']['request_id'] != component_id or replacement_for(records, component_id))), None)
    if saved:
        _saved_response({'model_dir': str(Path(saved['response_path']).parent),
                         'request_sha256': saved['request_sha256'],
                         'response_sha256': saved['response_sha256']}, request_observation)
    return saved


def _not_sent_with_journal(records, component_id):
    return pending_not_sent_request(records, component_id)


def _new_body_plan(backend, manifest, connection, work, original, reading):
    from preextract_contract import load_template
    model = read_json(backend.settings.path('model_config'))
    limits = dict(model.get('preextract_limits') or {})
    if (limits.get('model') != model.get('text_model')
            or str(limits.get('base_url', '')).rstrip('/') != str(model.get('base_url', '')).rstrip('/')
            or limits.get('supports_images') is not True or connection.get('model') != model.get('text_model')
            or str(connection.get('base_url', '')).rstrip('/') != str(model.get('base_url', '')).rstrip('/')):
        raise ValueError('正文模型或容量配置与前处理不一致')
    planning_connection = {**limits, 'model': model['text_model'], 'max_tokens': model.get('max_tokens', 8192)}
    planning = with_body_token_counter(planning_connection)
    planning['context_window'] = min(int(planning['context_window']), 1000000)
    from paperflow_prefect.body_template import manifest_template_path
    template, _ = load_template(manifest_template_path(backend.settings, manifest))
    options = {'request_context': manifest.get('planning_context', {'legacy_output_budget': True})}
    plan, adapted, _ = plan_with_available_images(reading, work, template, backend.catalog,
                                                  planning_connection, planning, **options)
    verify_plan(plan, manifest['capacity']['plan_identity'], source_sha256=manifest['body']['sha256'],
                reading_sha256=manifest['reading_material']['sha256'])
    return plan, original, adapted


async def run_single_body(*, identity, manifest, paper, backend, connection, admission,
                          limit_names, request_id, opener=None, retry_id=None):
    """Construct or reuse the existing whole-body request, without parsing it."""
    plan_identity = manifest['capacity']['plan_identity']
    if plan_identity['mode'] != 'single':
        raise ValueError('多段正文请求尚未接入新工单')
    component = next(row for row in expected_components(manifest, identity['task_run_id'])
                     if row['kind'] == 'body')
    work = Path(manifest['body_work']).resolve()
    if Path(backend.body_work(paper)).resolve() != work or manifest['body'] != paper['body']:
        raise ValueError('正文工作目录或交付身份不一致')
    original = read_json(manifest['material']['path'])
    reading = read_json(manifest['reading_material']['path'])
    used = {}

    def history():
        if manifest.get('body_plan_revision'):
            from .body_plan_revision import validate_revision
            validate_revision(read_receipts_sync(identity['task_run_id']), identity, manifest, sending=True)
            return
        if (work / INTENT).exists() or _other_revisions(work):
            raise ValueError('正文恢复记录待核对')
        _confirm_history(backend, paper, paper['body'], work)
        bind_work(backend.root, paper, paper['body'], 'body', work)

    def source():
        if sha_file(paper['body']['path']) != paper['body']['sha256']:
            raise ValueError('正文来源发生变化')
        if manifest.get('body_plan_revision'):
            from .body_plan_revision import validate_revision
            validate_revision(read_receipts_sync(identity['task_run_id']), identity, manifest, sending=True)
        if 'part' in used:
            verify_body_images(reading, used['part'], work / 'model')

    def new_plan():
        return _new_body_plan(backend, manifest, connection, work, original, reading)[0]

    def request_body():
        if (work / 'model' / 'request-state.json').exists():
            if saved_projection(work, original) != reading:
                raise ValueError('已有正文交接材料与原记录不一致')
        else:
            save_projection(work, original, reading)
        plan = single_plan(work, plan_identity, paper['body']['sha256'], manifest['reading_material']['sha256'],
                           new_plan_factory=new_plan)
        used['part'] = plan['parts'][0]
        if _body_hash(used['part']['body']) != component['request_sha256']:
            raise ValueError('正文请求与前处理计划不一致')
        return used['part']['body']

    records = normalize_model_history(await read_receipts(identity['task_run_id']), identity)
    saved = _saved_retry(records, component['component_id'])
    if saved:
        source()
        return saved
    not_sent = _not_sent_with_journal(records, component['component_id'])
    failed = retry_id and (replacement_for(records, component['component_id'], retry_id) is not None or
        any(row.get('kind') == 'llm_request_failed'
            and row.get('data', {}).get('component_id') == component['component_id'] for row in records))
    if not_sent or failed:
        if saved_projection(work, original) != reading:
            raise ValueError('已有正文交接材料与原记录不一致')
        used['part'] = single_plan(work, plan_identity, paper['body']['sha256'],
            manifest['reading_material']['sha256'], new_plan_factory=new_plan)['parts'][0]
        if not_sent:
            return await resume_not_sent_component(identity=identity, component_id=component['component_id'],
                kind='body', original_work=work, original_model=work / 'model',
                expected_request_sha256=component['request_sha256'], connection=connection, admission=admission,
                limit_names=limit_names, check_source=source, opener=opener)
        return await retry_raw_component(identity=identity, component_id=component['component_id'],
            retry_id=retry_id, kind='body', original_work=work, original_model=work / 'model',
            expected_request_sha256=component['request_sha256'], connection=connection, admission=admission,
            limit_names=limit_names, check_source=source, opener=opener, check_history=history)
    return await run_raw_component(identity=identity, component_id=component['component_id'],
        request_id=request_id, kind='body', request_body_factory=request_body, connection=connection,
        work=work, admission=admission, limit_names=limit_names, check_history=history,
        check_source=source, opener=opener)


async def run_partitioned_body(*, identity, manifest, paper, backend, connection, admission,
                               limit_names, opener=None, retry_id=None):
    """Save every frozen body part, reusing completed requests on resume."""
    from preextract_contract import load_template
    from preextract_partition_run import create_plan
    from preextract_request import request_once

    plan_identity = manifest['capacity']['plan_identity']
    if plan_identity['mode'] != 'partitioned':
        raise ValueError('正文计划不是分段模式')
    work = Path(manifest['body_work']).resolve()
    if Path(backend.body_work(paper)).resolve() != work or manifest['body'] != paper['body']:
        raise ValueError('正文工作目录或交付身份不一致')
    original = read_json(manifest['material']['path'])
    reading = read_json(manifest['reading_material']['path'])
    from paperflow_prefect.body_template import manifest_template_path
    template, _ = load_template(manifest_template_path(backend.settings, manifest))
    parts = {row['part_id']: row for row in expected_components(manifest, identity['task_run_id'])
             if row['kind'] == 'body'}
    records = normalize_model_history(await read_receipts(identity['task_run_id']), identity)
    component_ids = {component['component_id'] for component in parts.values()}
    resumed = any(replacement_for(records, component_id, retry_id) for component_id in component_ids) or any(row.get('data', {}).get('component_id') in component_ids and
                  ((row.get('kind') == 'llm_response_saved'
                    and row['data']['request_id'] != row['data']['component_id'])
                   or (retry_id and row.get('kind') == 'llm_request_failed')
                   or (row.get('kind') == 'llm_request_not_sent'
                       and (Path(row['data']['model_dir']) / 'request-state.json').is_file())) for row in records)
    resumable = {} if resumed else {
        part_id: verify_request_history(records, component['component_id'], component['component_id'],
                                        work / 'model' / part_id) for part_id, component in parts.items()}
    stop = _StopOnPause(admission)
    current = {}
    observation = {}
    receipt_identity = dict(task_id=identity['task_id'], folder_id=identity['group_id'],
                            child_flow_run_id=identity['task_run_id'])

    def history():
        if manifest.get('body_plan_revision'):
            from .body_plan_revision import validate_revision
            validate_revision(read_receipts_sync(identity['task_run_id']), identity, manifest, sending=True)
            return
        if (work / INTENT).exists() or _other_revisions(work):
            raise ValueError('正文恢复记录待核对')
        if not any(row.get('data', {}).get('component_id') in component_ids for row in records):
            _confirm_history(backend, paper, paper['body'], work)
        else:
            backend.confirm_history(paper, paper['body'])
        bind_work(backend.root, paper, paper['body'], 'body', work)

    def source():
        if sha_file(paper['body']['path']) != paper['body']['sha256']:
            raise ValueError('正文来源发生变化')
        if manifest.get('body_plan_revision'):
            from .body_plan_revision import validate_revision
            validate_revision(read_receipts_sync(identity['task_run_id']), identity, manifest, sending=True)

    def plan_factory():
        if (work / 'model' / 'partition-state.json').exists():
            if saved_projection(work, original) != reading:
                raise ValueError('已有正文交接材料与原记录不一致')
        else:
            save_projection(work, original, reading)
        return partition_plan(work, plan_identity, paper['body']['sha256'], manifest['reading_material']['sha256'],
            original_factory=lambda: original,
            new_plan_factory=lambda: _new_body_plan(backend, manifest, connection, work, original, reading))

    @contextmanager
    def lease(_kind, _stop):
        with admitted_sync_slot(admission, limit_names):
            yield

    @contextmanager
    def admit(part_id, transport):
        component = parts[part_id]
        if stop.is_set() or transport['request_sha256'] != component['request_sha256']:
            raise InterruptedError('大工单已暂停或正文分段请求身份不一致')
        fresh = normalize_model_history(read_receipts_sync(identity['task_run_id']), identity)
        if not verify_request_history(fresh, component['component_id'], component['component_id'],
                                      Path(transport['work_dir'])):
            raise RuntimeError('正文分段已有响应，需复用原结果，未重复发送')
        transport['gateway_binding'] = gateway.binding(identity, component['component_id'], component['component_id'], 'body')
        current.update(component_id=component['component_id'], request_id=component['component_id'],
                       kind='body', request_sha256=transport['request_sha256'], model_dir=transport['work_dir'],
                       transport=transport)
        observation.update(epoch=capture_model_epoch_sync(identity['task_id']), started_saved=False, returned=False)
        saved = {key: value for key, value in current.items() if key != 'transport'}
        save_receipt_sync(**receipt_identity, event_id=component['component_id'] + '-intent',
                          kind='llm_request_intent', data=saved)
        def started():
            save_receipt_sync(**receipt_identity, event_id=component['component_id'] + '-started',
                              kind='llm_request_started', data=saved)
            observation['started_saved'] = True
        yield _TransportAdmission(admission, started, task_id=identity['task_id'])

    class PartOpener:
        def __init__(self, transport_stop):
            self.stop = transport_stop

        def open(self, request, timeout=None):
            transport = current['transport']
            return gateway.GatewayOpener(connection=connection, model_dir=transport['work_dir'],
                request_binding=transport['gateway_binding'], stop=self.stop, delegate=opener).open(request, timeout)

    def request_with_gateway(*args, **kwargs):
        from paperflow_v3.control.stage_queue import current_phase
        if phase := current_phase():
            kwargs['stop'] = phase.fenced_stop(kwargs['stop'])
        kwargs['opener'] = PartOpener(kwargs['stop'])
        return request_once(*args, **kwargs)

    def accept(part_id, result):
        component_id = parts[part_id]['component_id']
        if not resumed and not resumable[part_id]:
            saved = next(row['data'] for row in records if row.get('kind') == 'llm_response_saved'
                         and row.get('data', {}).get('component_id') == component_id
                         and row['data'].get('request_id') == component_id)
            if (any(saved.get(key) != result.get(key) for key in
                    ('status', 'kind', 'request_sha256', 'response_path', 'response_sha256'))
                    or not isinstance(saved.get('response_text'), str)
                    or saved['response_text'].encode('utf8') != Path(result['response_path']).read_bytes()):
                raise ValueError('已保存的正文分段响应身份不一致')
            return
        observation['returned'] = True
        record_model_result_sync(identity['task_id'], identity['group_id'], component_id,
            success=True, epoch=observation.get('epoch'))
        save_receipt_sync(**receipt_identity, event_id=component_id + '-response',
            kind='llm_response_saved', data={'component_id': component_id, 'request_id': component_id, **result,
                'response_text': Path(result['response_path']).read_bytes().decode('utf8')})
        current.clear()

    try:
        if resumed:
            from paperflow_prefect.llm_queue.raw_partition import _frozen_state, _update_usage
            from paperflow_prefect.llm_queue.raw_request import _perform_one
            from paperflow_prefect.llm_queue.work_locks import work_locks
            from preextract_scheduler_adapter import request_observation

            def load_plan():
                with work_locks(work, 'body'):
                    history()
                    source()
                    plan, original, adapted = plan_factory()
                    verify_plan(plan, plan_identity, source_sha256=paper['body']['sha256'],
                                reading_sha256=manifest['reading_material']['sha256'])
                    _frozen_state(work / 'model', plan, original, adapted, template, connection)
                    return plan, original, adapted

            plan, original, adapted = await drain_thread(load_plan)
            results = []
            for part in plan['parts']:
                component = parts[part['id']]
                component_id = component['component_id']
                records = normalize_model_history(await read_receipts(identity['task_run_id']), identity)
                _saved_retry(records, component_id)
                saved = next((row['data'] for row in records if row.get('kind') == 'llm_response_saved'
                              and row.get('data', {}).get('component_id') == component_id), None)
                model = work / 'model' / part['id']

                def check_part():
                    source()
                    verify_body_images(reading, part, model)

                if saved:
                    retry_model = work / 'retries' / fingerprint(component_id) / saved['request_id'] / 'model'
                    actual = Path(saved['response_path']).resolve().parent
                    if (saved['request_sha256'] != component['request_sha256']
                            or actual not in {model.resolve(), retry_model.resolve()}):
                        raise ValueError('已完成的正文分段请求身份不一致')
                    check_part()
                    _saved_response({'model_dir': str(actual), 'request_sha256': saved['request_sha256'],
                                     'response_sha256': saved['response_sha256']}, request_observation)
                    results.append({'part_id': part['id'], **saved})
                    continue
                not_sent = _not_sent_with_journal(records, component_id)
                if not_sent:
                    result = await resume_not_sent_component(identity=identity, component_id=component_id,
                        kind='body', original_work=work, original_model=model,
                        expected_request_sha256=component['request_sha256'], connection=connection,
                        admission=admission, limit_names=limit_names, check_source=check_part, opener=opener)
                    results.append({'part_id': part['id'], **result})
                    continue
                failed = next((row['data'] for row in records if row.get('kind') == 'llm_request_failed'
                               and row.get('data', {}).get('component_id') == component_id), None)
                if failed or replacement_for(records, component_id, retry_id):
                    if not retry_id:
                        raise RuntimeError('正文分段失败，等待用户人工重试')
                    result = await retry_raw_component(identity=identity, component_id=component_id,
                        retry_id=retry_id, kind='body', original_work=work, original_model=model,
                        expected_request_sha256=component['request_sha256'], connection=connection,
                        admission=admission, limit_names=limit_names, check_source=check_part, opener=opener,
                        check_history=history)
                    results.append({'part_id': part['id'], **result})
                    continue

                verify_request_history(records, component_id, component_id, model)

                def submit_part():
                    with work_locks(work, 'body'):
                        check_part()
                        state, frozen = _frozen_state(work / 'model', plan, original, adapted, template, connection)
                        row = next((item for item in state['requests'] if item['id'] == part['id']), None)
                        if row is None:
                            limit = frozen.get('max_model_requests')
                            if limit is not None and len(state['requests']) >= limit:
                                raise ValueError('分段达到原计划请求上限')
                            if any((model / name).exists() for name in ('request-state.json', 'request.json',
                                    'response.txt', 'response-events.jsonl', 'http-error.txt')):
                                raise ValueError('分段已有未绑定的请求记录')
                            state['requests'].append({'id': part['id'], 'request_sha256': component['request_sha256']})
                            _update_usage(work / 'model', state)
                        try:
                            return _perform_one(kind='body', request_body=part['body'], connection=connection,
                                model=model, stop=stop, request_once=request_with_gateway, lease=lease,
                                admit=lambda transport: admit(part['id'], transport),
                                check_source=check_part)
                        finally:
                            _update_usage(work / 'model', state)

                result = await drain_thread(submit_part, stop=getattr(admission, 'stop', None))
                await drain_thread(lambda: accept(part['id'], result))
                results.append({'part_id': part['id'], **result})
            state_path = work / 'model/partition-state.json'
            state = read_json(state_path)
            state.update(aggregate_client_http_requests=None, aggregate_usage=None,
                         usage_note='人工重试的各次请求用量以各自请求记录为准')
            write_json(state_path, state)
            return {'status': 'body_parts_raw_saved', 'parts': results}
        return await drain_thread(lambda: run_partition_raw(plan_factory=plan_factory, plan_receipt=plan_identity,
            source_sha256=paper['body']['sha256'], reading_sha256=manifest['reading_material']['sha256'],
            template=template, connection=connection, work=work, stop=stop, request_once=request_with_gateway,
            lease=lease, admit=admit, accept=accept, check_history=history, check_source=source,
            check_part=lambda part: verify_body_images(reading, part, work / 'model' / part['id']),
            create_plan=create_plan, retry_missing=lambda part_id: resumable[part_id]),
            stop=getattr(admission, 'stop', None))
    except StageQueueWait:
        raise
    except Exception as exc:
        if current:
            outcome, proof = request_outcome(current['transport'])
            if (observation.get('started_saved') and not observation.get('returned')
                    and current['transport'].get('http_invoked') is True
                    and outcome != gateway.KIND and is_model_request_error(exc)):
                await record_model_result(identity['task_id'], identity['group_id'], current['request_id'],
                    success=False, epoch=observation['epoch'])
            data = {key: value for key, value in current.items() if key != 'transport'}
            await save_receipt(**receipt_identity, event_id=current['request_id'] + '-outcome',
                               kind=outcome, data={**data, **proof, 'error_type': type(exc).__name__})
            if outcome == gateway.KIND:
                prior_waits = sum(bool(row.get('data', {}).get('gateway_binding')) and
                    row.get('data', {}).get('component_id') == current['component_id'] for row in records)
                raise gateway.GatewayCapacityWait(gateway.retry_delay(proof, prior_waits)) from None
        raise


async def run_attachment(*, identity, manifest, paper, row, backend, connection, admission,
                         limit_names, request_id, opener=None, retry_id=None):
    """Construct or reuse one attachment request under the same folder ticket."""
    if row not in paper['attachments'] or row not in manifest['attachments']:
        raise ValueError('附件不属于当前论文')
    component = next(item for item in expected_components(manifest, identity['task_run_id'])
                     if item['kind'] == 'attachment' and item['file_id'] == row['file_id'])
    work = Path(backend.attachment_work(paper, row)).resolve()

    def history():
        _confirm_history(backend, paper, row, work)
        bind_work(backend.root, paper, row, 'attachment', work)

    records = normalize_model_history(await read_receipts(identity['task_run_id']), identity)
    saved = _saved_retry(records, component['component_id'])
    if saved:
        check_attachment_source(row)
        return saved
    not_sent = _not_sent_with_journal(records, component['component_id'])
    failure = next((event['data'] for event in records if event.get('kind') == 'llm_request_failed'
                    and event.get('data', {}).get('component_id') == component['component_id']), None)
    if not_sent or (retry_id and (failure or replacement_for(records, component['component_id'], retry_id))):
        expected = (not_sent or failure or replacement_for(records, component['component_id'], retry_id))['request_sha256']
        def source():
            if _body_hash(prepare_attachment_body(row, work, connection)) != expected:
                raise ValueError('附件原请求或来源发生变化')
        if not_sent:
            return await resume_not_sent_component(identity=identity, component_id=component['component_id'],
                kind='attachment', original_work=work, original_model=work / 'model',
                expected_request_sha256=expected, connection=connection, admission=admission,
                limit_names=limit_names, check_source=source, opener=opener)
        return await retry_raw_component(identity=identity, component_id=component['component_id'],
            retry_id=retry_id, kind='attachment', original_work=work, original_model=work / 'model',
            expected_request_sha256=expected, connection=connection, admission=admission,
            limit_names=limit_names, check_source=source, opener=opener, check_history=history)
    return await run_raw_component(identity=identity, component_id=component['component_id'],
        request_id=request_id, kind='attachment',
        request_body_factory=lambda: prepare_attachment_body(row, work, connection),
        connection=connection, work=work, admission=admission, limit_names=limit_names,
        check_history=history, check_source=lambda: check_attachment_source(row), opener=opener)
