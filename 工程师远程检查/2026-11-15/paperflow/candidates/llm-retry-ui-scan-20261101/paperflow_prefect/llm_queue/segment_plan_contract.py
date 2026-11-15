"""Candidate identity for an upstream body input plan; no model or file writes.

The current live upstream does not yet publish this receipt. It is intended
to accompany, not replace, the original planner and its coverage checks.
"""
from collections import Counter, defaultdict
import hashlib
import json
import re


SCHEMA = 'paperflow-body-input-plan/1'


def _hash(value, *, sort_keys=True):
    encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=sort_keys,
                               separators=(',', ':'), allow_nan=False)
    digest = hashlib.sha256()
    for piece in encoder.iterencode(value):
        digest.update(piece.encode('utf8'))
    return digest.hexdigest()


def _valid_digest(value):
    return isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value) is not None


def _check_coverage(plan):
    """Check the recorded primary intervals, never count context as coverage."""
    coverage = plan.get('coverage')
    if not isinstance(coverage, dict) or coverage.get('complete') is not True:
        raise ValueError('分段覆盖记录不完整')
    try:
        ranges = [span for part in plan['parts'] for span in part['ranges']]
        assets = [asset for part in plan['parts'] for asset in part['asset_ids']]
        if ranges != coverage['ranges'] or Counter(assets) != Counter(coverage['asset_ids']):
            raise ValueError('分段覆盖记录与各段范围不一致')
        if len(assets) != len(set(assets)):
            raise ValueError('分段覆盖中的资产主归属重复')
        by_block = defaultdict(list)
        for span in ranges:
            block, start, end = span['block_id'], span['start'], span['end']
            if not isinstance(block, str) or not block or type(start) is not int or type(end) is not int:
                raise ValueError('分段覆盖范围无效')
            by_block[block].append((start, end))
        characters = 0
        for spans in by_block.values():
            cursor = 0
            for start, end in sorted(spans):
                if start != cursor or end < start:
                    raise ValueError('分段覆盖不连续或重复')
                characters += end - start
                cursor = end
        if len(by_block) != coverage['block_count'] or characters != coverage['character_count']:
            raise ValueError('分段覆盖数量不一致')
    except (KeyError, TypeError) as exc:
        raise ValueError('分段覆盖记录无效') from exc


def describe_plan(plan, *, source_sha256, reading_sha256):
    """Bind the exact planner output, with no source text stored in receipt."""
    if not _valid_digest(source_sha256) or not _valid_digest(reading_sha256):
        raise ValueError('正文分段计划来源身份无效')
    if not isinstance(plan, dict) or plan.get('schema') != 'preextract-input-plan/1':
        raise ValueError('正文分段计划格式无效')
    parts = plan.get('parts')
    mode = plan.get('mode')
    if (not isinstance(parts, list) or not parts or mode not in {'single', 'partitioned'}
            or (mode == 'single' and len(parts) != 1)
            or (mode == 'partitioned' and len(parts) < 2)):
        raise ValueError('正文分段计划模式无效')
    for number, part in enumerate(parts, 1):
        if (not isinstance(part, dict) or not isinstance(part.get('body'), dict)
                or not isinstance(part.get('bundle'), dict) or not isinstance(part.get('ranges'), list)
                or not isinstance(part.get('asset_ids'), list) or not isinstance(part.get('capacity'), dict)
                or (mode == 'partitioned' and part.get('id') != f'part-{number:03d}')):
            raise ValueError('正文分段计划内容无效')
    _check_coverage(plan)
    try:
        identities = [{'id': f'part-{number:03d}',
                       # Existing transport computes the request hash without
                       # sorting object keys; keep that exact byte convention.
                       'request_sha256': _hash(part['body'], sort_keys=False),
                       'material_sha256': _hash(part['bundle']),
                       'scope_sha256': _hash({'ranges': part['ranges'], 'asset_ids': part['asset_ids']})}
                      for number, part in enumerate(parts, 1)]
        return {'schema': SCHEMA, 'source_sha256': source_sha256, 'reading_sha256': reading_sha256,
                'mode': mode, 'part_count': len(parts), 'parts': identities,
                'coverage_sha256': _hash(plan['coverage']), 'plan_sha256': _hash(plan)}
    except (TypeError, ValueError) as exc:
        raise ValueError('正文分段计划无法计算身份') from exc


def verify_plan(plan, receipt, *, source_sha256, reading_sha256):
    """Reconstruction must match exact plan/request content, not only count."""
    expected = describe_plan(plan, source_sha256=source_sha256, reading_sha256=reading_sha256)
    if receipt != expected:
        raise ValueError('正文分段计划与前处理交接不一致')
    return expected
