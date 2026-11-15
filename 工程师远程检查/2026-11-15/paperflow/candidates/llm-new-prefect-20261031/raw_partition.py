"""Candidate: save frozen body-part responses, without parsing or synthesis.

No queue, worker, Prefect connection or model is started by this module.
The caller owns trusted handoff, source/history checks and admissions.
"""
from contextlib import ExitStack, contextmanager
import json
from pathlib import Path

from filelock import FileLock
from raw_request import _body_hash, _perform_one, _sha_file
from segment_plan_contract import verify_plan


def _read(path):
    try:
        return json.loads(Path(path).read_text(encoding='utf8'))
    except (OSError, ValueError) as exc:
        raise ValueError('正文分段记录不可核验') from exc


def _write(path, value):
    # Reuse the same JSON writer and format as the existing partition runner.
    from preextract_material import write_json
    write_json(path, value)


def _frozen_state(model, plan, original, adapted, template, connection):
    from preextract_partition_init import json_sha
    from preextract_partition_run import SCHEMA, _binding
    from preextract_partition import digest
    state = _read(model / 'partition-state.json')
    frozen = _read(model / 'partition-plan.json')
    if (state.get('schema') != SCHEMA or frozen.get('schema') != SCHEMA
            or state.get('status') != 'parts_pending'
            or state.get('plan_sha256') != _sha_file(model / 'partition-plan.json')
            or frozen.get('source_material_sha256') != digest(original)
            or frozen.get('template_sha256') != digest(template)
            or frozen.get('material_sha256') != _sha_file(model / 'partition-material.json')
            or frozen.get('material_sha256') != json_sha(adapted)
            or frozen.get('binding') != _binding(connection)
            or frozen.get('coverage') != plan['coverage']):
        raise ValueError('原有正文分段计划或来源发生变化')
    rows = frozen.get('parts')
    if not isinstance(rows, list) or len(rows) != len(plan['parts']):
        raise ValueError('原有正文分段计划数量不一致')
    for row, part in zip(rows, plan['parts']):
        directory = model / part['id']
        if (row.get('id') != part['id'] or row.get('ranges') != part['ranges']
                or row.get('asset_ids') != part['asset_ids']
                or row.get('request_sha256') != _body_hash(part['body'])
                or row.get('material_sha256') != json_sha(part['bundle'])
                or _sha_file(directory / 'material.json') != row['material_sha256']
                or _body_hash(_read(directory / 'planned-request.json')) != row['request_sha256']):
            raise ValueError('原有正文分段计划内容发生变化')
    requests = state.get('requests')
    if (not isinstance(requests, list) or state.get('recoveries')
            or [row.get('id') for row in requests] != [p['id'] for p in plan['parts'][:len(requests)]]
            or any(row.get('request_sha256') != _body_hash(plan['parts'][i]['body'])
                   for i, row in enumerate(requests))):
        raise ValueError('原有正文分段请求记录不一致')
    return state, frozen


def _require_completed(model, row):
    """A registered earlier request is never treated as unsent without proof."""
    from preextract_scheduler_adapter import request_observation
    directory = model / row['id']
    try:
        phase, next_step, evidence = request_observation(directory)
        state = _read(directory / 'request-state.json')
        request = _read(directory / 'request.json')
        good = (phase == 'response_complete' and next_step == 'postprocess'
                and state.get('status') == 'response_complete' and state.get('finish_reason') == 'stop'
                and state.get('request_sha256') == row['request_sha256']
                and _body_hash(request) == row['request_sha256']
                and evidence.get('request_sha256') == row['request_sha256']
                and evidence.get('response_sha256') == state.get('response_sha256')
                and _sha_file(directory / 'response.txt') == state.get('response_sha256')
                and (directory / 'response-events.jsonl').is_file())
    except (OSError, ValueError, KeyError, TypeError):
        good = False
    if not good:
        raise ValueError('分段请求结果未确认，原记录保留，禁止自动重发')


def _update_usage(model, state):
    calls, usage, missing = 0, {}, []
    for row in state['requests']:
        path = model / row['id'] / 'request-state.json'
        if not path.is_file():
            missing.append(row['id'])
            continue
        facts = _read(path)
        calls += facts.get('client_http_requests', 0)
        current = facts.get('usage') or {}
        if not current:
            missing.append(row['id'])
        for key in ('prompt_tokens', 'completion_tokens', 'total_tokens'):
            if isinstance(current.get(key), int):
                usage[key] = usage.get(key, 0) + current[key]
    state.update(aggregate_client_http_requests=calls, aggregate_usage=usage,
                 missing_usage_requests=missing)
    _write(model / 'partition-state.json', state)


@contextmanager
def _work_locks(work):
    with ExitStack() as stack:
        stack.enter_context(FileLock(str(work / 'profile-recovery.lock'), timeout=0))
        stack.enter_context(FileLock(str(work / 'operation.lock'), timeout=0))
        yield


def run_partition_raw(*, plan_factory, plan_receipt, source_sha256, reading_sha256,
                      template, connection, work, stop, request_once, lease, admit, accept,
                      check_history, check_source, check_part, create_plan, opener=None):
    """Save each frozen part's response; never parse or send global requests.

    plan_factory runs under both existing body locks. `admit(part_id, transport)`
    must enforce the real task's pause/deletion boundary; `accept` must bind the
    verified response to its admission. `check_part` must recheck that part's
    original and request-bound images. This candidate supplies none of those.
    """
    required = (plan_factory, request_once, lease, admit, accept, check_history, check_source, check_part, create_plan)
    if stop is None or not all(callable(fn) for fn in required):
        raise ValueError('正文分段缺少来源、暂停或准入核验')
    if stop.is_set():
        raise InterruptedError('任务已暂停，未发送分段请求')
    work = Path(work).resolve()
    work.mkdir(parents=True, exist_ok=True)
    with _work_locks(work):
        check_history()
        check_source()
        plan, original, adapted = plan_factory()
        verify_plan(plan, plan_receipt, source_sha256=source_sha256, reading_sha256=reading_sha256)
        if plan['mode'] != 'partitioned':
            raise ValueError('正文计划不是多段请求')
        if stop.is_set():
            raise InterruptedError('任务已暂停，未初始化分段请求')
        model = work / 'model'
        if not (model / 'partition-state.json').is_file():
            # The native initializer can reconcile a partially written plan,
            # but refuses any request or unknown evidence under this model.
            create_plan(plan, original, adapted, work, template, connection)
        state, frozen = _frozen_state(model, plan, original, adapted, template, connection)
        # Validate all existing replies before accepting or starting any part.
        for row in state['requests']:
            _require_completed(model, row)
    results = []
    for part in plan['parts']:
        with _work_locks(work):
            if stop.is_set():
                raise InterruptedError('任务已暂停，已完成分段保留')
            check_source()
            state, frozen = _frozen_state(model, plan, original, adapted, template, connection)
            for prior in state['requests']:
                _require_completed(model, prior)
            check_part(part)
            expected = _body_hash(part['body'])
            row = next((item for item in state['requests'] if item['id'] == part['id']), None)
            if row is None:
                limit = frozen.get('max_model_requests')
                if limit is not None and len(state['requests']) >= limit:
                    raise ValueError('分段达到原计划请求上限')
                directory = model / part['id']
                if any((directory / name).exists() for name in
                       ('request-state.json', 'request.json', 'response.txt', 'response-events.jsonl', 'http-error.txt')):
                    raise ValueError('分段有未登记请求证据，禁止自动重发')
                state['requests'].append({'id': part['id'], 'request_sha256': expected})
                _update_usage(model, state)
            try:
                def check_at_send():
                    check_source()
                    check_part(part)
                result = _perform_one(kind='body', request_body=part['body'], connection=connection,
                                      model=model / part['id'], stop=stop, request_once=request_once,
                                      lease=lease, admit=lambda transport: admit(part['id'], transport),
                                      check_source=check_at_send, opener=opener)
            finally:
                _update_usage(model, state)
        # Admission reconciliation takes the same body locks, so it must run
        # after the request holder has released them.
        accept(part['id'], result)
        results.append({'part_id': part['id'], **result})
    return {'status': 'body_parts_raw_saved', 'parts': results}
