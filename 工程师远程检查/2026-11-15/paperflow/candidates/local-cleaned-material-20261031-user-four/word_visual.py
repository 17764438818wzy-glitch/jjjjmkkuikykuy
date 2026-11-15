"""Recover Word objects only when a unique caption identifies rendered pixels."""
import re
from pathlib import Path


def _normalize(value):
    return re.sub(r'\s+', '', str(value or '')).casefold()


def _caption_after(kept, position):
    number = position.get('body_index')
    if not isinstance(number, int):return None
    for block in kept:
        pos = block.get('position') or {}
        if pos.get('part') != 'word/document.xml' or not isinstance(pos.get('body_index'), int):continue
        if not number < pos['body_index'] <= number+3:continue
        text = _normalize(block.get('text'))
        if not re.match(r'^(?:图\d|附图|fig(?:ure)?\.?\d|表\d|table\d)', text, re.I):continue
        labels = list(re.finditer(r'(?:图\d|fig(?:ure)?\.?\d|表\d|table\d)', text, re.I))
        if len(labels) > 1:text = text[:labels[1].start()]
        if len(text) >= 5:return text[:8]
    return None


def recover_word_visuals(source, conversion_dir, kept, removed, issues, resource, images):
    """Render a temporary PDF and recover only unambiguous body-image regions."""
    import fitz
    from evidence_render import render_office_pdf

    pending = []
    for issue in issues:
        if issue.get('code') not in {'embedded_object_review', 'native_chart_visual'}:continue
        position = issue.get('position') or {}
        ref = position.get('object_ref')
        anchor = next((b for b in kept if ref and (b.get('position') or {}).get('object_ref') == ref), None)
        caption = _caption_after(kept, position)
        if anchor is not None and caption:
            pending.append((issue, anchor, caption))
    if not pending:return
    try:
        from material_conversion import preparation_context
        with preparation_context(conversion_dir, enabled=False):
            rendered = render_office_pdf(source, Path(conversion_dir)/'visual_render')
        with fitz.open(rendered) as pdf:
            for issue, anchor, caption in pending:
                hits = []
                for page in pdf:
                    for row in page.get_text('blocks'):
                        if len(row) >= 5 and _normalize(row[4]).startswith(caption):
                            hits.append((page.number, fitz.Rect(row[:4])))
                if len(hits) != 1:continue
                number, cap_box = hits[0]
                page = pdf[number]
                if page.rotation:continue
                # A page containing an exclusion boundary needs explicit scope
                # mapping before any of its pixels can be used as evidence.
                lines = [line.strip() for line in page.get_text().splitlines() if line.strip()]
                boundary = r'(?:摘\s*要|abstract|目\s*录|contents|参\s*考\s*文\s*献|参考书目|references|bibliography|致\s*谢)\s*[:：]?'
                if any(re.fullmatch(boundary, line, re.I) for line in lines):continue
                excluded_text = []
                excluded_images = False
                for row in removed:
                    block = row.get('block') or {}
                    values = [block.get('text') or ''] + [cell.get('text') or '' for cell in block.get('cells', [])]
                    excluded_images = excluded_images or bool(block.get('type') == 'image' or
                        block.get('resource_id') or block.get('resource_ids') or
                        any('[[IMAGE:' in str(value) for value in values))
                    for value in values:
                        normalized = _normalize(re.sub(r'\[\[IMAGE:[^\]]+\]\]', '', str(value)))
                        if len(normalized) >= 4:excluded_text.append(normalized)
                def includes_excluded(box):
                    visible = [_normalize(line) for line in page.get_text(clip=box).splitlines()]
                    return any(len(line) >= 4 and (line in excluded or excluded in line or
                               any(excluded[-size:] in line for size in range(min(32, len(excluded)), 5, -1)))
                               for line in visible for excluded in excluded_text)
                boxes = []
                for info in page.get_image_info():
                    box = fitz.Rect(info.get('bbox') or [])
                    if box.is_empty or box.is_infinite or box not in page.rect:continue
                    gap = cap_box.y0-box.y1
                    if -2 <= gap <= 70:boxes.append(box)
                # If a unique caption identifies a body page but the object
                # is vector-only, retain that page as the visual fallback.
                # Pages with exclusion boundaries were already ruled out.
                page_fallback = not boxes
                if page_fallback:
                    anchor_position = anchor.get('position') or {}
                    anchor_index = anchor_position.get('body_index')
                    previous_removed = [(r.get('block') or {}).get('position') or {} for r in removed
                                        if r.get('reason') != 'repeated_page_margin']
                    previous_removed = [pos['body_index'] for pos in previous_removed
                                        if pos.get('part') == 'word/document.xml'
                                        and isinstance(pos.get('body_index'), int)
                                        and isinstance(anchor_index, int) and pos['body_index'] < anchor_index]
                    page_box = fitz.Rect(page.rect)
                    if previous_removed:
                        from reading_cleanup import heading_kind
                        last_removed = max(previous_removed)
                        candidates = []
                        for block in kept:
                            pos = block.get('position') or {}
                            index = pos.get('body_index')
                            title = str(block.get('text') or '').strip()
                            if (pos.get('part') == 'word/document.xml' and isinstance(index, int)
                                    and last_removed < index < anchor_index and title and len(title) <= 100
                                    and (block.get('heading_level') or heading_kind(title) in {'abstract', 'appendix', 'research_chapter'})):
                                candidates.append((index, title))
                        boundary_hit = None
                        for _, title in sorted(candidates, reverse=True):
                            title_hits = [(p.number, rect) for p in pdf for rect in p.search_for(title)]
                            if len(title_hits) == 1:
                                boundary_hit = title_hits[0]
                                break
                        if boundary_hit is None or boundary_hit[0] > number:continue
                        if boundary_hit[0] == number:
                            if boundary_hit[1].y0 >= cap_box.y0:continue
                            page_box.y0 = max(page_box.y0, boundary_hit[1].y0)
                            column = boundary_hit[1] | cap_box
                            separated = any(len(row) >= 5 and str(row[4]).strip() and
                                fitz.Rect(row[:4]).y1 > page_box.y0 and
                                (fitz.Rect(row[:4]).x1 < column.x0 - page.rect.width * .04 or
                                 fitz.Rect(row[:4]).x0 > column.x1 + page.rect.width * .04)
                                for row in page.get_text('blocks'))
                            if separated:continue
                    if includes_excluded(page_box):continue
                    if excluded_images and page.get_image_info():continue
                    boxes = [page_box]
                safe_boxes = [box for box in boxes if not includes_excluded(box)]
                if not safe_boxes:continue
                has_missing_boxes = len(safe_boxes) != len(boxes)
                boxes = safe_boxes
                anchor_position = anchor.get('position') or {}
                generated = []
                for i, box in enumerate(boxes):
                    pos = {**anchor_position, 'rendered_page': number+1, 'rendered_bbox': list(box)}
                    if page_fallback:pos['page_fallback'] = True
                    pix = page.get_pixmap(clip=box, matrix=fitz.Matrix(2,2), alpha=False, annots=False)
                    rid = f'docx_object_{anchor_position.get("object_ref")}_{i+1}'
                    r = resource(rid, pix.tobytes('png'), '.png', 'docx_visual_crop', pos)
                    item = {'id': f'{anchor["id"]}__visual_{i+1}', 'type': 'image', 'position': pos,
                            'resource_id': r['id'], 'caption': '', 'source': 'docx_visual_crop',
                            'ocr': {'status': 'not_run', 'verified': False}}
                    generated.append(item)
                    images.append(item)
                at = kept.index(anchor)
                kept[at:at+1] = generated
                if not has_missing_boxes:
                    issue['code'] = 'docx_visual_crop_review'
                    issue['message'] = ('正文图形已根据唯一图题保留所在页图。' if page_fallback else
                                        '正文图形已根据唯一图题保留局部渲染图。')
    except InterruptedError:
        raise
    except Exception as exc:
        # Existing pending issue remains and stops incomplete material; do not
        # replace it with an unbounded or unverified whole-page image.
        for issue, _, _ in pending:
            if issue.get('code') in {'embedded_object_review', 'native_chart_visual'}:
                issue['render_attempt'] = type(exc).__name__
