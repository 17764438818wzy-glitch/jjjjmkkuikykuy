"""Explicit handoff review requirements for locally prepared material."""


def build_readiness(assets, issues, scope, unresolved=()):
    items = []
    for i, issue in enumerate(issues):
        if issue.get('code') in {'ocr_page_incomplete', 'pdf_page_incomplete', 'scope_incomplete', 'ocr_scope_suspect', 'toc_transition_text', 'appendix_ocr_ambiguity'}:
            items.append({'id': f'ocr-boundary:issue:{i}', 'kind': 'unresolved_boundary',
                          'position': issue.get('position') or {},
                          'uncertain_from_page': issue.get('code') == 'ocr_scope_suspect',
                          'description': '此页及可能受影响的后续范围尚未确认，已保留本地来源；建议核对或重新识别。'})
        elif issue.get('code') == 'pdf_column_removed_review':
            items.append({'id': f'unresolved_boundary:issue:{i}', 'kind':'unresolved_boundary',
                          'position':issue.get('position') or {},
                          'description':'文末边界页此组可能包含被误删的正文；如有研究内容需补回并重新处理，只有确认整组均无应保留研究内容才能略去。'})
        elif issue.get('code') == 'post_ocr_scope_unverified':
            items.append({'id': f'post_ocr_scope:issue:{i}', 'kind': 'post_ocr_scope_warning',
                          'position': issue.get('position') or {},
                          'description': issue.get('message', '')})
        elif issue.get('code') == 'pdf_column_kept_review':
            items.append({'id': f'column_scope:issue:{i}', 'kind':'column_scope_review',
                          'position':issue.get('position') or {},
                          'description':'文末边界页此组可能混入排除内容；需确认整组均为允许的正文，否则应修正范围并重新处理。'})
    for asset in assets:
        if not asset.get('image_path'):continue
        position = asset.get('position') or {}
        located = bool(asset.get('block_ids') and asset.get('section_ids') and
                       (isinstance(position.get('body_index'), int) or
                        isinstance(position.get('page'), int) and position.get('bbox')))
        # The retained block and source position establish where the extracted
        # image belongs.  OCR text and the picture subtype are not prerequisites.
        if located:
            continue
        detail = '图片已提取，但尚不能对应到正文中的位置。'
        if (asset.get('image_scope_audit') or {}).get('status') == 'excluded_heading_detected':
            detail = 'OCR 发现疑似排除章节标题，确认属于误识别；如确实包含排除内容，需重新裁剪或移除后再交接。'
        if asset.get('kind') in {'docx_visual_crop', 'pdf_vector_crop'}:
            detail += ' 同时核对图表、箭头和标注没有被裁掉。'
        items.append({'id': 'visual:' + asset['id'], 'kind': 'visual_scope',
                      'asset_id': asset['id'], 'sha256': asset.get('sha256'),
                      'position': asset.get('position') or {}, 'description': detail})
    for issue in issues:
        if issue.get('code') not in {'ocr_page_annotation_review', 'pdf_annotation_review', 'table_continuation_review'}:continue
        if issue.get('code') == 'table_continuation_review':
            position = issue.get('position') or {}
            if any(asset.get('kind') == 'table' and (asset.get('position') or {}) == position
                   and any(cell.get('continuation_text') for cell in (asset.get('structure') or {}).get('cells', []))
                   for asset in assets):
                continue
        page = (issue.get('position') or {}).get('page')
        ident = f'{issue["code"]}:{page}'
        if issue.get('code') == 'table_continuation_review':
            ident += ':' + str((issue.get('position') or {}).get('body_index', len(items)))
        if not any(item['id'] == ident for item in items):
            items.append({'id': ident, 'kind': issue['code'], 'position': issue.get('position') or {},
                          'description': issue.get('message', '')})
    visual_codes = {'embedded_object_review','native_chart_visual','body_container','scanned_page',
                    'image_missing','formula_review','image_format_unreadable','pdf_vector_review','pdf_table_review',
                    'pdf_visual_incomplete','toc_transition_visual','pdf_scan_crop_scope_review'}
    for i, issue in enumerate(issues):
        if issue.get('code') in visual_codes:
            items.append({'id': f'unresolved_visual:issue:{i}', 'kind': 'unresolved_visual',
                          'position': issue.get('position') or {},
                          'description': '原文对象尚未完整恢复；需补齐，或复核确认它不含应保留的研究内容。'})
    for block in unresolved:
        items.append({'id': 'unresolved_visual:block:' + block['id'], 'kind': 'unresolved_visual',
                      'position': block.get('position') or {},
                      'description': ('受影响的单元格或表格字段含有未恢复的内联图片，需补全；只有确认该字段无应保留内容才能略去。'
                                      if block.get('inline_image_kind') == 'table' else
                                      '整段含有未恢复的内联图片，需补全该段；只有确认整段均无应保留的研究内容才能略去。'
                                      if block.get('inline_image_unresolved') else
                                      '此对象缺少完整图形；需补齐或确认无需保留。')})
    if scope.get('start_kind') == 'first_chapter_no_abstract':
        items.append({'id': 'start:no_abstract', 'kind': 'no_abstract',
                      'description': '确认原文确实没有摘要，且第一章以前没有应保留的研究内容。'})
    return {'schema': 'local-material-readiness/1',
            'state': 'review_required' if items else 'ready', 'items': items}
