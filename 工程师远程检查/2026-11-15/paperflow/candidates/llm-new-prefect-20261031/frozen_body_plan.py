"""Freeze enough planner metadata to resume without adapting images again.

The existing partition initializer owns the per-part request and material
files. This candidate stores only additional planner fields outside `model`,
and reconstructs the plan from those files when a partition state exists.
Run under the same work locks as `raw_partition.run_partition_raw`.
"""
import json
import os
from pathlib import Path
import re

from segment_plan_contract import verify_plan


PART_FIELDS = {'id', 'ranges', 'asset_ids', 'capacity', 'body', 'bundle'}


def _read(path):
    try:
        with Path(path).open(encoding='utf8') as stream:
            return json.load(stream)
    except (OSError, ValueError, TypeError) as exc:
        raise ValueError('正文分段计划证据不可核验') from exc


def _save_once(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if _read(path) != value:
            raise ValueError('已有正文分段计划身份不一致')
        return
    with path.open('x', encoding='utf8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())


def _metadata(plan, receipt):
    if plan.get('mode') != 'partitioned':
        raise ValueError('正文分段计划不是多段模式')
    return {'schema': 'paperflow-raw-frozen-plan-metadata/1', 'plan_sha256': receipt['plan_sha256'],
            'top': {key: value for key, value in plan.items() if key != 'parts'},
            'part_extras': [{'id': part['id'], 'extra': {k: v for k, v in part.items() if k not in PART_FIELDS}}
                            for part in plan['parts']]}


def _from_saved(model, metadata, receipt):
    if (not isinstance(metadata, dict) or metadata.get('schema') != 'paperflow-raw-frozen-plan-metadata/1'
            or metadata.get('plan_sha256') != receipt.get('plan_sha256')
            or not isinstance(metadata.get('top'), dict) or 'parts' in metadata['top']
            or not isinstance(metadata.get('part_extras'), list)):
        raise ValueError('原有正文分段计划元数据不一致')
    saved = _read(model / 'partition-plan.json')
    rows = saved.get('parts') if isinstance(saved, dict) else None
    extras = metadata['part_extras']
    if not isinstance(rows, list) or len(rows) != len(extras):
        raise ValueError('原有正文分段计划内容不一致')
    parts = []
    for index, (row, extra) in enumerate(zip(rows, extras), 1):
        ident = f'part-{index:03d}'
        if (not isinstance(row, dict) or row.get('id') != ident or not isinstance(extra, dict)
                or extra.get('id') != ident or not isinstance(extra.get('extra'), dict)
                or set(extra['extra']) & PART_FIELDS or not re.fullmatch(r'part-\d{3,}', ident)):
            raise ValueError('原有正文分段计划内容不一致')
        parts.append({**extra['extra'], 'id': ident, 'ranges': row.get('ranges'),
                      'asset_ids': row.get('asset_ids'), 'capacity': row.get('capacity'),
                      'body': _read(model / ident / 'planned-request.json'),
                      'bundle': _read(model / ident / 'material.json')})
    return {**metadata['top'], 'parts': parts}, _read(model / 'partition-material.json')


def partition_plan(work, receipt, source_sha256, reading_sha256, *, original_factory, new_plan_factory):
    """Return the same plan, original and adapted material without reply parsing."""
    if not callable(original_factory) or not callable(new_plan_factory) or not isinstance(receipt, dict):
        raise ValueError('正文分段计划来源不可核验')
    work = Path(work)
    model = work / 'model'
    metadata_path = work / 'private' / 'llm-frozen-plan-metadata.json'
    if (model / 'partition-state.json').is_file():
        plan, adapted = _from_saved(model, _read(metadata_path), receipt)
        original = original_factory()
        verify_plan(plan, receipt, source_sha256=source_sha256, reading_sha256=reading_sha256)
        return plan, original, adapted
    # With no committed plan, actual request evidence is never treated as a
    # new plan; the initializer separately reconciles its own partial files.
    if model.is_dir() and any(path.name in {'request.json', 'request-state.json', 'response.txt',
                                             'response-events.jsonl', 'http-error.txt'}
                              for path in model.rglob('*') if path.is_file()):
        raise ValueError('正文分段计划缺失但存在请求证据，不得重新规划')
    plan, original, adapted = new_plan_factory()
    verify_plan(plan, receipt, source_sha256=source_sha256, reading_sha256=reading_sha256)
    _save_once(metadata_path, _metadata(plan, receipt))
    return plan, original, adapted


def single_plan(work, receipt, source_sha256, reading_sha256, *, new_plan_factory):
    """Reuse the exact full-body input after an interruption, without replanning."""
    if not callable(new_plan_factory) or not isinstance(receipt, dict) or receipt.get('mode') != 'single':
        raise ValueError('正文分段计划来源不可核验')
    work = Path(work)
    model = work / 'model'
    path = work / 'private' / 'llm-frozen-single-plan.json'
    if path.exists():
        value = _read(path)
        if (not isinstance(value, dict) or value.get('schema') != 'paperflow-raw-frozen-single-plan/1'
                or value.get('plan_sha256') != receipt.get('plan_sha256')):
            raise ValueError('原有正文分段计划元数据不一致')
        plan = value.get('plan')
    else:
        if model.is_dir() and any(path.name.startswith(('partition-', 'part-', 'global-', 'initialization-evidence'))
                                  or path.name in {'request.json', 'request-state.json', 'response.txt',
                                                   'response-events.jsonl', 'http-error.txt'}
                                  for path in model.rglob('*')):
            raise ValueError('正文计划缺失但存在请求证据，不得重新规划')
        plan = new_plan_factory()
        verify_plan(plan, receipt, source_sha256=source_sha256, reading_sha256=reading_sha256)
        if plan.get('mode') != 'single':
            raise ValueError('正文分段计划不是整篇模式')
        _save_once(path, {'schema': 'paperflow-raw-frozen-single-plan/1',
                          'plan_sha256': receipt['plan_sha256'], 'plan': plan})
    verify_plan(plan, receipt, source_sha256=source_sha256, reading_sha256=reading_sha256)
    if plan.get('mode') != 'single':
        raise ValueError('正文分段计划不是整篇模式')
    return plan
