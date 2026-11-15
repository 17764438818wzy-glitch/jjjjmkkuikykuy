"""Candidate body branch: frozen input to verified raw replies only.

The caller supplies a verified folder handoff, real upstream verifier, history
guard, existing planner, image checker, and Prefect lease. This module creates
no task or queue and never parses a model answer or executes postprocessing.
"""
import hashlib
import json
from pathlib import Path

from folder_completion import expected_components
from folder_ledger import _identity
from frozen_body_plan import partition_plan, single_plan
from raw_partition import run_partition_raw
from raw_request import run_raw_request


def _reference(reference, label):
    try:
        path = Path(reference['path']).resolve(strict=True)
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != reference['sha256']:
            raise ValueError(label + '发生变化')
        value = json.loads(raw)
    except (KeyError, OSError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(label + '不可核验') from exc
    if not isinstance(value, dict):
        raise ValueError(label + '不可核验')
    return value


def run_body_raw(*, identity, generation, manifest, work, connection, template, stop,
                 guard, upstream, lease, check_history, new_plan_factory, verify_images,
                 request_once, create_plan, opener=None):
    identity = _identity(identity)
    if (type(generation) is not int or generation < 0 or stop is None
            or not callable(getattr(guard, 'admit', None)) or not callable(getattr(guard, 'complete', None))
            or not callable(getattr(upstream, 'for_execution', None))
            or not all(callable(fn) for fn in (lease, check_history, new_plan_factory, verify_images,
                                                request_once, create_plan))):
        raise ValueError('正文处理缺少工单、历史或发送准入')
    if stop.is_set():
        raise InterruptedError('任务已暂停，未开始正文处理')
    if (not isinstance(manifest, dict) or any(manifest.get(k) != identity[k] for k in
            ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'))):
        raise ValueError('正文文件夹交接身份不一致')
    expected = expected_components(manifest, identity['task_run_id'])
    parts = {item['part_id']: item for item in expected if item['kind'] == 'body'}
    body = manifest['body']
    try:
        work = Path(work).resolve(strict=True)
        if work != Path(manifest['body_work']).resolve(strict=True) or not work.is_dir():
            raise ValueError('正文工作目录身份不一致')
    except (KeyError, OSError, TypeError) as exc:
        raise ValueError('正文工作目录不可核验') from exc
    receipt = manifest['capacity']['plan_identity']
    model = work / 'model'

    def check_source():
        dependency_check = getattr(new_plan_factory, 'check_dependencies', None)
        if callable(dependency_check):
            dependency_check()
        if upstream.for_execution(identity) != manifest:
            raise ValueError('正文上游交接身份发生变化')
        try:
            with Path(body['path']).open('rb') as stream:
                digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        except (OSError, KeyError, TypeError) as exc:
            raise ValueError('正文交付文件不可核验') from exc
        if digest != body['sha256']:
            raise ValueError('正文交付文件发生变化')
        material = _reference(manifest['material'], '正文材料')
        reading = _reference(manifest['reading_material'], '正文阅读材料')
        if (material.get('source', {}).get('source_sha256') != body['sha256']
                or reading.get('source', {}).get('source_sha256') != body['sha256']):
            raise ValueError('正文阅读材料来源不一致')
        return reading

    def admit(part_id, transport):
        item = parts.get(part_id)
        if not item or transport.get('request_sha256') != item['request_sha256']:
            raise ValueError('正文请求与前处理计划不一致')
        return guard.admit(identity['task_id'], generation, item['component_id'], item['request_sha256'],
                           kind='body', work_dir=work, model_dir=transport['work_dir'],
                           receipt_id=identity['receipt_id'], task_run_id=identity['task_run_id'],
                           source_sha256=body['sha256'])

    def accept(part_id, result):
        item = parts[part_id]
        guard.complete(identity['task_id'], item['component_id'], result['response_sha256'])

    mode = manifest['capacity']['mode']
    if mode == 'single':
        current = {}
        def request_body():
            plan = single_plan(work, receipt, body['sha256'], manifest['reading_material']['sha256'],
                               new_plan_factory=lambda: new_plan_factory()[0])
            current['part'] = plan['parts'][0]
            return current['part']['body']
        def check_single():
            reading = check_source()
            if 'part' in current:
                verify_images(reading, current['part'], model)
        result = run_raw_request(kind='body', request_body_factory=request_body, connection=connection,
            work=work, stop=stop, request_once=request_once, lease=lease,
            admit=lambda transport: admit('part-001', transport), check_history=check_history,
            check_source=check_single, opener=opener)
        accept('part-001', result)
        return {'status': 'body_parts_raw_saved', 'parts': [{'part_id': 'part-001', **result}]}
    if mode == 'partitioned':
        def plan_factory():
            return partition_plan(work, receipt, body['sha256'], manifest['reading_material']['sha256'],
                                  original_factory=lambda: _reference(manifest['material'], '正文材料'),
                                  new_plan_factory=new_plan_factory)
        def check_part(part):
            verify_images(check_source(), part, model / part['id'])
        return run_partition_raw(plan_factory=plan_factory, plan_receipt=receipt,
            source_sha256=body['sha256'], reading_sha256=manifest['reading_material']['sha256'],
            template=template, connection=connection, work=work, stop=stop, request_once=request_once,
            lease=lease, admit=admit, accept=accept, check_history=check_history, check_source=check_source,
            check_part=check_part, create_plan=create_plan, opener=opener)
    raise ValueError('正文计划模式不可核验')
