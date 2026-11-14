"""Create a limited local request projection while retaining pending evidence."""
import copy
import hashlib
import math


def pdf_image_clip_requirements(material):
    """Flag raw PDF image payloads whose page placement clips away pixels.

    This checks the page boundary and extraction identity. It does not claim
    to prove the absence of every PDF graphics clipping path.
    """
    source = material.get('source') or {}
    if (source.get('parsed_view') or {}).get('format') != 'pdf':
        return []
    candidates = [a for a in material.get('assets', [])
                  if a.get('kind') == 'image' and a.get('image_path') and not a.get('image_rendering')]
    if not candidates:
        return []
    reasons = {}
    try:
        import fitz
        with fitz.open(source.get('file')) as pdf:
            cached = {}
            for asset in candidates:
                pos = asset.get('position') or {}
                page, index = pos.get('page'), pos.get('block_index')
                try:
                    if not isinstance(page, int) or not 1 <= page <= len(pdf) or not isinstance(index, int) or index < 0:
                        raise ValueError('missing image source position')
                    if page not in cached:
                        cached[page] = pdf[page - 1].get_text('dict', sort=True)['blocks']
                    block = cached[page][index]
                    transform = block.get('transform')
                    if block.get('type') != 1 or not isinstance(transform, (list, tuple)) or len(transform) != 6:
                        raise ValueError('missing image transform')
                    if not all(isinstance(x, (int, float)) and math.isfinite(x) for x in transform):
                        raise ValueError('invalid image transform')
                    placed = fitz.Rect(0, 0, 1, 1) * fitz.Matrix(*transform)
                    visible = fitz.Rect(block.get('bbox') or [])
                    if placed.is_empty or placed.is_infinite or visible.is_empty or visible.is_infinite:
                        raise ValueError('invalid image rectangle')
                    tolerance = max(.05, max(placed.width, placed.height) * .00001)
                    if any(abs(a - b) > tolerance for a, b in zip(placed, visible)):
                        raise ValueError('raw image contains pixels outside its visible placement')
                    if hashlib.sha256(block['image']).hexdigest() != asset.get('sha256'):
                        raise ValueError('image source identity differs')
                except (IndexError, KeyError, TypeError, ValueError, RuntimeError):
                    reasons[asset.get('id')] = '图片原始像素与页面可见范围无法确认一致，建议检查原页。'
    except (ImportError, OSError, RuntimeError, TypeError, ValueError):
        reasons.update({a.get('id'): '无法核对 PDF 图片的可见范围，建议检查原页。' for a in candidates})
    return [{'id': 'pdf-image-clip:' + str(a.get('id')), 'kind': 'visual_scope',
             'asset_id': a.get('id'), 'position': a.get('position') or {}, 'description': reasons[a.get('id')]}
            for a in candidates if a.get('id') in reasons]


def project_available_material(material, pending):
    """Return a copy containing only content that is safe to plan locally.

    The original material/index remain unchanged. A page with a disputed
    scope is withheld in its entirety, including section titles and assets.
    Images with disputed scope are withheld independently from body text.
    """
    result = copy.deepcopy(material)
    held_images = {row.get('asset_id') for row in pending
                   if row.get('kind') == 'visual_scope' and row.get('asset_id')}
    held_pages = {(row.get('position') or {}).get('page') for row in pending
                  if row.get('kind') in {'unresolved_boundary', 'column_scope_review'}
                  and isinstance((row.get('position') or {}).get('page'), int)}
    from_pages = [(row.get('position') or {}).get('page') for row in pending
                  if row.get('uncertain_from_page')
                  and isinstance((row.get('position') or {}).get('page'), int)]
    def page_is_held(position):
        page = (position or {}).get('page')
        return isinstance(page, int) and (page in held_pages or any(page >= start for start in from_pages))

    # An individually unlocated image must not hide unrelated text or images.
    visual_rows = [row for row in pending if row.get('kind') == 'unresolved_visual']
    visual_positions = [row.get('position') or {} for row in visual_rows]
    unknown_visual = any(row.get('kind') == 'unresolved_visual'
                         and not isinstance((row.get('position') or {}).get('page'), int)
                         and not isinstance((row.get('position') or {}).get('body_index'), int)
                         for row in visual_rows)
    unknown_visual_ocr = any(not isinstance((row.get('position') or {}).get('page'), int)
                             and not isinstance((row.get('position') or {}).get('body_index'), int)
                             for row in visual_rows)

    def valid_box(box):
        return (isinstance(box, (list, tuple)) and len(box) == 4
                and all(isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) for x in box)
                and box[2] > box[0] and box[3] > box[1])

    def block_has_held_source(block):
        positions = [(block.get('position'), block.get('source')),
                     (block.get('source_position'), block.get('source'))]
        positions += [(segment.get('position'), segment.get('source') or block.get('source'))
                      for segment in (block.get('source_segments') or [])]
        return any(page_is_held(position) or
                   (source == 'cpu_ocr' or (position or {}).get('ocr'))
                   and overlaps_pending_visual(position) for position, source in positions)

    def overlaps_pending_visual(position, *, include_unlocated=True):
        position = position or {}
        if unknown_visual or include_unlocated and unknown_visual_ocr:
            return True
        page = position.get('page')
        for held in visual_positions:
            held_page = held.get('page')
            if not isinstance(held_page, int):
                continue
            if not isinstance(page, int):
                return True
            if held_page != page:
                continue
            a, b = position.get('bbox'), held.get('bbox')
            if not valid_box(a) or not valid_box(b):
                return True
            if min(a[2], b[2]) > max(a[0], b[0]) and min(a[3], b[3]) > max(a[1], b[1]):
                return True
        return False

    def same_visual_location(asset):
        position = asset.get('position') or {}
        if unknown_visual:
            return True
        for row in visual_rows:
            held = row.get('position') or {}
            if not held:
                continue
            if ('body_index' in held and position.get('body_index') == held['body_index']
                    and position.get('part') == held.get('part')):
                return True
            if isinstance(held.get('page'), int):
                if not isinstance(position.get('page'), int):
                    return True
                if position.get('page') == held['page']:
                    # Page-rendered pixels can include strokes that extend
                    # beyond a drawing's recorded path bounds.
                    rendered = asset.get('kind') in {'pdf_vector_crop', 'pdf_table_page_crop',
                                                     'ocr_page_crop', 'table_screenshot'} or asset.get('image_rendering') == 'pdf_masked_image_page_region'
                    if row.get('kind') == 'unresolved_visual' and rendered:
                        return True
                    if overlaps_pending_visual(position, include_unlocated=False):
                        return True
        return False

    assets = result.get('assets', [])
    held_images.update(asset.get('id') for asset in assets if asset.get('image_path')
                       and same_visual_location(asset))
    withheld_assets = [asset for asset in assets if asset.get('id') in held_images
                       or page_is_held(asset.get('position'))]
    withheld_block_ids = {bid for asset in withheld_assets for bid in asset.get('block_ids', [])}
    retained_visual_block_ids = {bid for asset in assets if asset not in withheld_assets
                                 for bid in asset.get('block_ids', [])}
    removed_blocks = [block for block in result.get('blocks', [])
                      if block_has_held_source(block)
                      or (block.get('type') == 'image' and block.get('id') in withheld_block_ids
                          and block.get('id') not in retained_visual_block_ids)]
    removed_ids = {block['id'] for block in removed_blocks}
    removed_sections = {block.get('section_id') for block in removed_blocks
                        if block.get('heading_level')}
    for section in result.get('sections', []):
        if any(block.get('section_id') == section.get('id') and block.get('text')
               and block.get('text') == section.get('title') for block in removed_blocks):
            removed_sections.add(section.get('id'))
    result['blocks'] = [block for block in result.get('blocks', []) if block.get('id') not in removed_ids]
    retained_ids = {block['id'] for block in result['blocks']}
    for block in result['blocks']:
        if block.get('object_ids'):
            block['object_ids'] = [x for x in block['object_ids'] if x not in held_images]
    result['assets'] = [asset for asset in assets if asset not in withheld_assets
                        and (not asset.get('block_ids') or set(asset['block_ids']) & retained_ids)]
    held_ocr = any(
        block.get('source') == 'cpu_ocr'
        or (block.get('position') or {}).get('ocr')
        or (block.get('source_position') or {}).get('ocr')
        or any((segment.get('position') or {}).get('ocr')
               for segment in block.get('source_segments', []))
        for block in removed_blocks
    )
    def may_have_unlocated_held_neighbor(position):
        # The parser uses adjacent blocks, which need not be on adjacent pages.
        return bool(held_pages or from_pages or held_ocr or visual_positions)

    def word_neighbors(left, right):
        a, b = left.get('body_index'), right.get('body_index')
        if not isinstance(a, int) or not isinstance(b, int):
            return False
        lp, rp = left.get('container_path') or [], right.get('container_path') or []
        if not lp and not rp:
            return abs(a - b) <= 1
        return (a == b and isinstance(lp, list) and isinstance(rp, list)
                and len(lp) == len(rp) and bool(lp) and lp[:-1] == rp[:-1]
                and isinstance(lp[-1], int) and isinstance(rp[-1], int) and abs(lp[-1] - rp[-1]) <= 1)

    def one_native_source(block):
        segments = block.get('source_segments') or []
        if not segments:
            return True
        if (any(s.get('synthetic_separator') or not s.get('source_block_id') for s in segments)
                or len({s.get('source_block_id') for s in segments}) != 1):
            return False
        cursor = 0
        for seg in segments:
            end = seg.get('target_end')
            if seg.get('target_start') != cursor or not isinstance(end, int) or end <= cursor:
                return False
            cursor = end
        return cursor == len(str(block.get('text') or ''))

    def caption_has_safe_native_source(row):
        caption = str(row.get('caption') or '').strip()
        position = row.get('position') or {}
        box, page = position.get('bbox'), position.get('page')
        part, body_index = position.get('part'), position.get('body_index')
        word_location = bool(part and isinstance(body_index, int))
        if not caption or not (word_location or isinstance(page, int) and valid_box(box)):
            return False
        for block in result.get('blocks', []):
            pos = block.get('source_position') or block.get('position') or {}
            cb = pos.get('bbox')
            if (block.get('type') != 'paragraph' or block.get('source') == 'cpu_ocr'
                    or pos.get('ocr') or str(block.get('text') or '').strip() != caption):
                continue
            sources = [p for p in (block.get('position'), block.get('source_position')) if p]
            segments = [s for s in (block.get('source_segments') or []) if not s.get('synthetic_separator')]
            if word_location:
                at = pos.get('body_index')
                if (block.get('source') == 'native_document' and pos.get('part') == part
                        and word_neighbors(position, pos) and one_native_source(block)
                        and all(not p.get('ocr') and p.get('part') == part and p.get('body_index') == at
                                and (p.get('container_path') or []) == (pos.get('container_path') or []) for p in sources)
                        and all(s.get('source') != 'cpu_ocr' and isinstance(s.get('position'), dict)
                                and not s['position'].get('ocr') and s['position'].get('part') == part
                                and s['position'].get('body_index') == at
                                and (s['position'].get('container_path') or []) == (pos.get('container_path') or [])
                                for s in segments)):
                    return True
                continue
            if pos.get('page') != page or not valid_box(cb):
                continue
            if (any(p.get('ocr') or p.get('page') != page or not valid_box(p.get('bbox')) for p in sources)
                    or any(s.get('source') == 'cpu_ocr' or not isinstance(s.get('position'), dict)
                           or s['position'].get('ocr') or s['position'].get('page') != page
                           or not valid_box(s['position'].get('bbox')) for s in segments)):
                continue
            # A nearby, retained native caption below this figure establishes
            # the text's own origin without relying on an unlocated inference.
            if -10 <= cb[1]-box[3] <= 48 and min(cb[2],box[2]) > max(cb[0],box[0]):
                return True
        return False

    # Captions and table notes were inferred by the upstream parser from
    # adjacent blocks but lack their own source positions. Near a withheld
    # page or OCR block anywhere in the document they cannot safely accompany
    # otherwise valid objects until their own source positions are established.
    cleared_derived_fields = 0
    for row in result['blocks'] + result['assets']:
        if not may_have_unlocated_held_neighbor(row.get('position')):
            continue
        if row.get('caption') and not caption_has_safe_native_source(row):
            row['caption'] = ''
            cleared_derived_fields += 1
        structure = row.get('structure') or {}
        for field in ('units', 'notes'):
            if structure.get(field):
                structure[field] = None
                cleared_derived_fields += 1
    for asset in result['assets']:
        if asset.get('block_ids'):
            asset['block_ids'] = [bid for bid in asset['block_ids'] if bid in retained_ids]
    for section in result.get('sections', []):
        if section.get('id') in removed_sections:
            section['title'] = '[章节标题待核]'
        if section.get('block_ids'):
            section['block_ids'] = [bid for bid in section['block_ids'] if bid in retained_ids]
    result['formulas'] = [formula for formula in result.get('formulas', [])
                          if not page_is_held(formula.get('position'))
                          and not ((formula.get('position') or {}).get('ocr')
                                   and overlaps_pending_visual(formula.get('position')))]
    return result, {'pending_count': len(pending), 'held_asset_count': len(withheld_assets),
                    'held_page_count': len(held_pages), 'held_block_count': len(removed_blocks),
                    'held_unlocated_caption_fields': cleared_derived_fields}
