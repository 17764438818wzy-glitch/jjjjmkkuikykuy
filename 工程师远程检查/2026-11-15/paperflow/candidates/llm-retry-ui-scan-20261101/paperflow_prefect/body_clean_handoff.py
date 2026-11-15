"""Prepare the existing cleaned material for the existing body request path."""
import copy
from pathlib import Path

from .records import identity, read_json, write_json


SCHEMA = 'prefect-clean-reading-projection/1'
GAP_CODES = {
    'ocr_page_incomplete', 'pdf_page_incomplete', 'post_ocr_scope_unverified',
    'scope_incomplete', 'ocr_scope_suspect', 'toc_transition_text', 'toc_transition_visual',
    'pdf_column_removed_review', 'pdf_column_kept_review', 'missing_note',
    'external_images_missing', 'inline_image_review', 'embedded_object_review',
    'native_chart_visual', 'body_container', 'scanned_page', 'image_missing',
    'formula_review', 'image_format_unreadable', 'image_position_unresolved',
    'formula_position_unresolved', 'appendix_ocr_ambiguity', 'pdf_vector_review', 'pdf_table_review',
    'pdf_visual_incomplete', 'pdf_image_clip_review', 'pdf_scan_crop_scope_review',
}


def _context(bundle, message):
    context = bundle.setdefault('reading_context', {})
    context['source_completeness'] = 'partial'
    previous = context.get('instruction', '')
    if message not in previous:
        context['instruction'] = (previous + ' ' + message).strip()
    scope = bundle.get('source', {}).get('reading_scope')
    if isinstance(scope, dict):
        scope['notes'] = context['instruction']


def project_for_reading(original):
    """Exclude only images whose position is unknown; report other gaps."""
    pending = [row for row in (original.get('readiness') or {}).get('items', [])
               if row.get('kind') == 'visual_scope']
    if pending:
        from partial_projection import project_available_material
        projected, _ = project_available_material(original, pending)
    else:
        projected = copy.deepcopy(original)
    gaps = [copy.deepcopy(issue) for issue in original.get('issues', [])
            if issue.get('code') in GAP_CODES]
    for row in pending:
        gap = {'code': 'image_position_missing', 'position': row.get('position') or {},
               'asset_id': row.get('asset_id'), 'message': row.get('description', '')}
        gaps.append(gap)
        projected.setdefault('issues', []).append(copy.deepcopy(gap))
    if gaps:
        _context(projected, '材料有已记录的提取或边界缺口；其他可用内容继续，未确认的范围请结合记录核对。')
    return projected, gaps


def save_projection(work, original, projected):
    """Bind the exact projection before sending; never replace a prior binding."""
    path = Path(work) / 'private' / 'prefect-reading-projection.json'
    value = {'schema': SCHEMA, 'original_sha256': identity(original),
             'projected_sha256': identity(projected), 'material': projected}
    if path.exists():
        if read_json(path) != value:
            raise ValueError('已有正文交接材料与原记录不一致；原请求保留')
    else:
        write_json(path, value)
    return projected


def saved_projection(work, original):
    path = Path(work) / 'private' / 'prefect-reading-projection.json'
    if not path.is_file():
        raise ValueError('已有模型请求缺少绑定的正文交接材料；原请求保留')
    value = read_json(path)
    material = value.get('material')
    if (value.get('schema') != SCHEMA or value.get('original_sha256') != identity(original)
            or value.get('projected_sha256') != identity(material)):
        raise ValueError('已有正文交接材料身份发生变化；原请求保留')
    return material


def add_image_gaps(bundle, projection, report):
    result = copy.deepcopy(bundle)
    originals = {row.get('id'): row for row in projection.get('assets', [])}
    gaps = []
    for row in report.get('images', []):
        if not row.get('held'):
            continue
        asset = originals.get(row.get('id')) or {}
        gap = {'code': 'image_capacity_unavailable', 'asset_id': row.get('id'),
               'position': asset.get('position') or {},
               'message': row.get('reason') or '图片无法适配当前模型容量，其他内容继续。'}
        gaps.append(gap)
        result.setdefault('issues', []).append(copy.deepcopy(gap))
    if gaps:
        _context(result, '部分图片无法适配当前模型容量，已记录位置；其他内容继续。')
    return result, gaps


def remove_impossible_images(adapted, report, impossible):
    """Record only individual images that cannot fit an otherwise empty request."""
    original_ids = {(a.get('image_adaptation') or {}).get('original_asset_id') or a['id']
                    for a in impossible}
    removed = {a['id'] for a in adapted.get('assets', [])
               if ((a.get('image_adaptation') or {}).get('original_asset_id') or a['id']) in original_ids}
    held_blocks = {bid for a in adapted.get('assets', []) if a['id'] in removed
                   for bid in a.get('block_ids', [])}
    adapted['assets'] = [a for a in adapted.get('assets', []) if a['id'] not in removed]
    still_used = {bid for a in adapted['assets'] for bid in a.get('block_ids', [])}
    adapted['blocks'] = [b for b in adapted.get('blocks', [])
                         if not (b.get('type') == 'image' and b.get('id') in held_blocks
                                 and b.get('id') not in still_used)]
    for block in adapted.get('blocks', []):
        if block.get('object_ids'):
            block['object_ids'] = [ident for ident in block['object_ids'] if ident not in removed]
    for row in report.get('images', []):
        if row.get('id') in original_ids:
            row.update(held=True, reason='图片单独加入时仍超过当前模型容量；其他内容继续。')
    report['held_count'] = sum(bool(row.get('held')) for row in report.get('images', []))
    report['image_count'] = sum(0 if row.get('held') else len(row.get('tiles', [])) or 1
                                for row in report.get('images', []))
    return adapted, report


def plan_with_available_images(projection, work, template, catalog, connection, planning_connection):
    from preextract_images import prepare_request_images
    from preextract_request import RequestCapacityError, build_request
    from .body_segment_plan import PART_RULES, _assess_capacity, plan_requests

    raw, report = prepare_request_images(projection, work, connection, continue_on_image_error=True)
    adapted, image_gaps = add_image_gaps(raw, projection, report)
    try:
        plan = plan_requests(adapted, template, catalog, planning_connection)
    except RequestCapacityError:
        # A fixed prompt or output budget that is already too large is not an
        # image failure. Remove only images which individually add a violation.
        empty = copy.deepcopy(adapted)
        empty.update(blocks=[], sections=[], assets=[], issues=[], formulas=[])
        scope = (empty.get('source') or {}).get('reading_scope')
        if isinstance(scope, dict):
            scope['extraction_issues'] = []
        _, baseline = build_request(empty, template, catalog, planning_connection,
                                    instruction=PART_RULES, enforce=False)
        baseline = set(_assess_capacity(baseline, planning_connection)['violations'])
        impossible = []
        for asset in raw.get('assets', []):
            if not asset.get('image_path'):
                continue
            probe = copy.deepcopy(empty)
            probe['assets'] = [copy.deepcopy(asset)]
            probe['assets'][0]['block_ids'] = []
            probe['assets'][0]['section_ids'] = []
            _, capacity = build_request(probe, template, catalog, planning_connection,
                                        instruction=PART_RULES, enforce=False)
            if set(_assess_capacity(capacity, planning_connection)['violations']) - baseline:
                impossible.append(asset)
        if not impossible:
            raise
        original_ids = {(a.get('image_adaptation') or {}).get('original_asset_id') or a['id']
                        for a in impossible}
        if not original_ids <= {row.get('id') for row in report.get('images', [])}:
            raise ValueError('无法定位超限图片的原始记录；原请求保留')
        raw, report = remove_impossible_images(raw, report, impossible)
        write_json(Path(work) / 'private' / 'request-images.json', report)
        adapted, image_gaps = add_image_gaps(raw, projection, report)
        plan = plan_requests(adapted, template, catalog, planning_connection)
    return plan, adapted, image_gaps


def save_gaps(work, original, gaps):
    path = Path(work) / 'private' / 'prefect-reading-gaps.json'
    value = {'schema': 'prefect-reading-gaps/1', 'original_sha256': identity(original), 'gaps': gaps}
    if path.exists() and read_json(path) != value:
        raise ValueError('已有正文缺口记录与本次材料不一致；原请求保留')
    if not path.exists():
        write_json(path, value)

