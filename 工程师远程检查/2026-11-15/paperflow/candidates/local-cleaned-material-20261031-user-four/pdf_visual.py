"""Page-scoped PDF OCR and visual preservation for the isolated candidate."""
import re

from reading_cleanup import ScopeUncertain, heading_kind


def suspected_ocr_citation_page(lines):
    """Flag clear multi-entry citation shapes even if OCR missed the heading."""
    texts = [str(line.get('text') or '').strip() for line in lines]
    numbered = [t for t in texts if re.match(r'^(?:\[\s*\d+\s*\]|【\s*\d+\s*】)\s*\S', t)]
    numbered_year = [t for t in texts if re.match(r'^\d+[.、．)]\s*\D', t)
                     and re.search(r'\b(?:19|20)\d{2}\b', t)]
    author_year = [t for t in texts if re.match(r'^[A-Z][a-z]+,\s*[A-Z].*\((?:19|20)\d{2}\)', t)]
    strong_single = [t for t in numbered if re.search(r'\bdoi\s*:|\bet\s+al\.', t, re.I)]
    return len(numbered) >= 2 or len(numbered_year) >= 2 or bool(author_year or strong_single)


def open_pdf(source):
    import fitz
    return fitz.open(stream=source, filetype='pdf') if isinstance(source, bytes) else fitz.open(source)


def original_images_are_white(pdf, page):
    """Prove white from source pixels, not a downsampled page preview."""
    import io
    from PIL import Image
    images = page.get_images(full=True)
    if not images:
        return False
    for row in images:
        xref, mask = row[:2]
        if not xref or mask:
            return False
        details = pdf.extract_image(xref)
        if not details or not details.get('image'):
            return False
        with Image.open(io.BytesIO(details['image'])) as image:
            if image.width * image.height > 40000000 or 'A' in image.getbands():
                return False
            if any(extrema != (255, 255) for extrema in image.convert('RGB').getextrema()):
                return False
    return True


def _box(row):
    import fitz
    value = (row.get('position') or {}).get('bbox')
    if not isinstance(value, (list, tuple)) or len(value) != 4:
        return None
    rect = fitz.Rect(value)
    return rect if not rect.is_empty and not rect.is_infinite else None


def grouped_image_regions(page, info):
    """Bound nearby image tiles; whole-page scans are not figure evidence."""
    import fitz
    area = page.rect.get_area() or 1
    regions = []
    for item in info:
        rect = fitz.Rect(item.get('bbox') or [])
        if not rect.is_empty and not rect.is_infinite and rect.get_area() < area*.7:
            regions.append(rect)
    changed = True
    while changed:
        changed = False
        for i, region in enumerate(regions):
            expanded = region + (-12, -12, 12, 12)
            other = next((j for j in range(i+1, len(regions)) if not (expanded & regions[j]).is_empty), None)
            if other is not None:
                regions[i] = region | regions.pop(other)
                changed = True
                break
    return [region for region in regions if region.get_area() < area*.7]


def ocr_figure_region(page, info, point, *, regions=None):
    """A group bbox is evidence only where an actual member image exists."""
    import fitz
    area = page.rect.get_area() or 1
    members = [fitz.Rect(item['bbox']) for item in info if item.get('bbox')]
    if not any(not rect.is_empty and rect.get_area() < area*.7 and rect.contains(point) for rect in members):
        return None
    candidates = [region for region in (regions if regions is not None else grouped_image_regions(page, info))
                  if region.contains(point)]
    return min(candidates, key=lambda region: region.get_area()) if candidates else None


def prepare_pages(source, raw, images, resources, issues, *, continue_on_uncertainty=False):
    """OCR scan-like pages and retain all OCR provenance before scope cleaning."""
    import fitz
    import visual_ocr

    scan_pages, blank_pages = set(), []
    with open_pdf(source) as pdf:
        for page in pdf:
            number = page.number + 1
            try:
                native_text = page.get_text().strip()
                info = page.get_image_info()
                area = page.rect.get_area() or 1
                coverage = sum((fitz.Rect(item['bbox']) & page.rect).get_area()
                               for item in info if item.get('bbox')) / area
                image_boxes = [fitz.Rect(item['bbox']) for item in info if item.get('bbox')]
                centers = [(box.x0+box.x1)/2 for box in image_boxes]
                tiled = (len(image_boxes) >= 4 and max(centers)-min(centers) <= page.rect.width*.05
                         and max(box.y1 for box in image_boxes)-min(box.y0 for box in image_boxes) >= page.rect.height*.50)
                drawings = page.get_drawings()
                bboxlog = page.get_bboxlog()
                graphics = bool(info or drawings or any(
                    kind not in {'fill-text', 'stroke-text', 'ignore-text'}
                    for kind, *_ in bboxlog))
                compact = re.sub(r'\W+', '', native_text)
                scan_like = (graphics and ((not compact and (bool(info) or len(drawings) >= 80)) or coverage >= .55 or tiled
                    or (len(info) >= 3 and coverage >= .30)
                    or (len(compact) < 8 and coverage >= .12)))
                annotations = bool(any(page.annots() or []))
                if not native_text and not graphics and not annotations:
                    blank_pages.append(number)
                    continue
                if not native_text and info and not annotations and not drawings:
                    if (all(kind == 'fill-image' for kind, *_ in bboxlog)
                            and original_images_are_white(pdf, page)
                            and min(page.get_pixmap(colorspace=fitz.csGRAY, alpha=False, annots=False).samples) == 255):
                        blank_pages.append(number)
                        continue
            except (InterruptedError, MemoryError, PermissionError, OSError):
                raise
            except Exception as exc:
                if not continue_on_uncertainty:
                    raise ScopeUncertain(f'第 {number} 页检查失败：{type(exc).__name__}') from exc
                issues.append({'code':'pdf_page_incomplete', 'position':{'page':number},
                               'message':'该页检查未完成；其他页面继续解析，相关范围建议核对。',
                               'reason':type(exc).__name__})
                continue
            if not scan_like:
                continue
            if annotations:
                issues.append({'code': 'ocr_page_annotation_review', 'position': {'page': number},
                               'message': '扫描页包含批注，OCR 和页图均不含批注，需核对其是否属于正文。'})
            try:
                lines = visual_ocr.read_pdf_page(page)
            except (InterruptedError, MemoryError, PermissionError, OSError):
                raise
            except Exception as exc:
                if continue_on_uncertainty:
                    issues.append({'code': 'ocr_page_incomplete', 'position': {'page': number},
                        'message': '该页 OCR 未完成，相关范围需核对；建议检查原页或重新识别。',
                        'reason': type(exc).__name__})
                    continue
                raise ScopeUncertain(f'第 {number} 页 OCR 未能完成：{type(exc).__name__}') from exc
            if not lines:
                if continue_on_uncertainty:
                    issues.append({'code': 'ocr_page_incomplete', 'position': {'page': number},
                        'message': '该页没有识别到文字，相关范围需核对；建议区分空白页、图片和扫描页。'})
                    continue
                raise ScopeUncertain(f'第 {number} 页需要 OCR，但未得到可用文字')
            scan_pages.add(number)
            page_images = [b for b in images if (b.get('position') or {}).get('page') == number]
            image_ids = {b.get('resource_id') for b in page_images}
            images[:] = [b for b in images if b not in page_images]
            raw[:] = [b for b in raw if b not in page_images]
            resources[:] = [r for r in resources if r.get('id') not in image_ids]
            page_native = [b for b in raw if b.get('type') == 'paragraph'
                           and (b.get('position') or {}).get('page') == number
                           and not b.get('unresolved_object')]
            existing = re.sub(r'\s+', '', native_text).casefold()
            image_regions = grouped_image_regions(page, info)
            inserted = 0
            for i, line in enumerate(lines):
                text = str(line.get('text') or '').strip()
                box = line.get('bbox')
                if not text or not isinstance(box, (list, tuple)) or len(box) != 4:
                    continue
                # Avoid duplicating an existing native text layer on the same page.
                if re.sub(r'\s+', '', text).casefold() in existing or any(re.sub(r'\s+', '', text) == re.sub(r'\s+', '', str(b.get('text') or ''))
                       and _box(b) and (_box(b) & fitz.Rect(box)).get_area() > 0 for b in page_native):
                    continue
                position = {'page': number, 'bbox': list(box), 'ocr': True}
                center = fitz.Point((box[0]+box[2])/2, (box[1]+box[3])/2)
                containing = ocr_figure_region(page, info, center, regions=image_regions)
                if containing:
                    position['image_region'] = list(containing)
                raw.append({'id': f'ocr_p{number:04d}_{i:05d}', 'type': 'paragraph', 'text': text,
                            'heading_level': 0, 'source': 'cpu_ocr',
                            'ocr_confidence': float(line.get('confidence', 0)),
                            'position': position})
                inserted += 1
            if not inserted:
                recognized = [re.sub(r'\s+', '', str(line.get('text') or '')).casefold()
                              for line in lines if str(line.get('text') or '').strip()]
                if (continue_on_uncertainty and len(existing) >= 20 and recognized
                        and all(text in existing for text in recognized)):
                    issues.append({'code': 'ocr_duplicates_native', 'position': {'page': number},
                        'message': 'OCR 文字与现有文字层重复，已保留文字及原页证据；建议按需核对。'})
                    continue
                if continue_on_uncertainty:
                    issues.append({'code': 'ocr_page_incomplete', 'position': {'page': number},
                        'message': '该页 OCR 未增加可用文字，相关范围需核对；建议检查文字层与原页。'})
                    scan_pages.discard(number)
                    continue
                raise ScopeUncertain(f'第 {number} 页 OCR 未识别出图像中的新增文字，需核验扫描内容')
            issues[:] = [i for i in issues if not (i.get('code') == 'scanned_page'
                         and (i.get('position') or {}).get('page') == number)]
            issues.append({'code': 'ocr_text_review', 'position': {'page': number},
                           'message': '此页文字来自本机 CPU OCR，识别和正文边界仍需核验。'})
            if suspected_ocr_citation_page(lines):
                issues.append({'code': 'ocr_scope_suspect', 'position': {'page': number},
                    'message': '扫描页发现疑似多条参考文献；此页及后续范围暂不交接，建议核对。'})
    raw.sort(key=lambda b: ((b.get('position') or {}).get('page', 0),
                            ((b.get('position') or {}).get('bbox') or [0, 0])[1],
                            ((b.get('position') or {}).get('bbox') or [0, 0])[0]))
    return scan_pages, blank_pages


def preserve_scans(source, kept, removed, scan_pages, resource, images, *, issues=None, continue_on_uncertainty=False):
    """Crop scanned page evidence to the recognized permitted reading range."""
    import fitz
    with open_pdf(source) as pdf:
        for number in sorted(scan_pages):
            page = pdf[number-1]
            allowed = [b for b in kept if (b.get('position') or {}).get('page') == number and _box(b)]
            if not allowed:
                continue
            excluded = [r['block'] for r in removed
                        if (r['block'].get('position') or {}).get('page') == number and _box(r['block'])]
            top = min(_box(b).y0 for b in allowed)
            bottom = max(_box(b).y1 for b in allowed)
            above = [b for b in excluded if _box(b).y1 <= top]
            below = [b for b in excluded if _box(b).y0 >= bottom]
            if len(above) + len(below) != len(excluded):
                if continue_on_uncertainty:
                    issues.append({'code':'pdf_scan_crop_scope_review','position':{'page':number},
                        'message':'扫描页正文和排除范围交错；页图待核，已识别的其他文字继续处理。'})
                    continue
                raise ScopeUncertain(f'第 {number} 页 OCR 内容与排除范围交错，不能安全裁页')
            # OCR/source boxes use unrotated coordinates; pixmap clips use
            # displayed page coordinates. Keep these spaces separate.
            unrotated_page = page.rect * page.derotation_matrix
            y0 = top if above else unrotated_page.y0
            y1 = min(_box(b).y0 for b in below) if below else unrotated_page.y1
            clip = fitz.Rect(unrotated_page.x0, y0, unrotated_page.x1, y1) & unrotated_page
            if clip.is_empty or y1 <= bottom:
                if continue_on_uncertainty:
                    issues.append({'code':'pdf_scan_crop_scope_review','position':{'page':number},
                        'message':'扫描页正文范围无法安全裁剪；页图待核，已识别的其他文字继续处理。'})
                    continue
                raise ScopeUncertain(f'第 {number} 页无法安全保留扫描页正文')
            position = {'page': number, 'bbox': list(clip), 'ocr_page_crop': True}
            render_clip = clip * page.rotation_matrix
            try:
                pixels = page.get_pixmap(clip=render_clip, matrix=fitz.Matrix(2, 2), alpha=False, annots=False)
            except (RuntimeError, ValueError):
                if not continue_on_uncertainty:raise
                issues.append({'code':'pdf_visual_incomplete','position':position,
                               'message':'扫描页局部图片生成失败，其他页面继续；建议核对原页。'})
                continue
            r = resource(f'ocr_crop_{number:04d}', pixels.tobytes('png'), '.png', 'ocr_page_crop', position)
            block = {'id': f'ocr_crop_block_{number:04d}', 'type': 'image', 'position': position,
                     'resource_id': r['id'], 'caption': '', 'source': 'ocr_page_crop',
                     'ocr': {'status': 'completed_unverified', 'verified': False}}
            kept.append(block)
            images.append(block)


def preserve_vectors(source, kept, removed, issues, resource, images, *, continue_on_uncertainty=False):
    """Preserve bounded PDF vector targets as source-page crops, not guesses."""
    import fitz
    targets = [b for b in kept if b.get('unresolved_object')
               and (b.get('position') or {}).get('vector_region')]
    groups = []
    for target in targets:
        box = _box(target)
        number = (target.get('position') or {}).get('page')
        if not box or not isinstance(number, int):
            if continue_on_uncertainty:
                issues.append({'code':'pdf_visual_incomplete','position':target.get('position') or {},
                    'message':'矢量图范围不明；该图待核，其他内容继续处理。'})
                continue
            raise ScopeUncertain('矢量图缺少可靠页面范围，需 OCR 或人工核验')
        groups.append({'page': number, 'rect': box, 'blocks': [target]})
    changed = True
    while changed:
        changed = False
        for i, left in enumerate(groups):
            match = next((j for j in range(i+1, len(groups)) if groups[j]['page'] == left['page']
                          and not (left['rect'] & groups[j]['rect']).is_empty), None)
            if match is not None:
                other = groups.pop(match)
                left['rect'] |= other['rect']
                left['blocks'].extend(other['blocks'])
                changed = True
                break
    positions = set()
    with open_pdf(source) as pdf:
        for index, group in enumerate(groups):
            number, clip = group['page'], group['rect']
            page = pdf[number-1]
            unrotated_page = page.rect * page.derotation_matrix
            if not (clip in unrotated_page):
                if continue_on_uncertainty:
                    issues.append({'code':'pdf_visual_incomplete','position':{'page':number},
                        'message':'矢量图范围无法安全裁剪；该图待核，其他内容继续处理。'})
                    continue
                raise ScopeUncertain(f'第 {number} 页矢量范围无法安全裁剪，需 OCR 或人工核验')
            excluded = [r for r in removed
                        if (r['block'].get('position') or {}).get('page') == number and _box(r['block'])]
            conflicts = [r for r in excluded if (clip & _box(r['block'])).get_area() > 0]
            unsafe = [r for r in conflicts if r.get('reason') != 'repeated_page_margin']
            margins = [_box(r['block']) for r in conflicts if r.get('reason') == 'repeated_page_margin']
            retained_text = [_box(b) for b in kept if (b.get('position') or {}).get('page') == number
                             and b.get('type') == 'paragraph' and not b.get('unresolved_object') and _box(b)]
            if unsafe or any((m & box).get_area() > 0 for m in margins for box in retained_text):
                conflict = (unsafe or conflicts)[0]
                if continue_on_uncertainty:
                    issues.append({'code':'pdf_visual_incomplete','position':{'page':number,'bbox':list(clip)},
                        'message':'矢量图跨越排除范围；该图待核，其他内容继续处理。'})
                    continue
                raise ScopeUncertain(f'第 {number} 页矢量图跨越排除内容，需 OCR 或人工核验；'
                    f'图形 {list(clip)}，排除类型 {conflict.get("reason")}，位置 {conflict["block"].get("position")}')
            position = {'page': number, 'bbox': list(clip), 'vector_region': True}
            if margins:
                # Remove only the native repeated header/footer text in an
                # in-memory page copy; retain underlying images and drawings.
                temporary = fitz.open()
                try:
                    temporary.insert_pdf(pdf, from_page=number-1, to_page=number-1, annots=False)
                    clean_page = temporary[0]
                    before = (len(clean_page.get_drawings()), len(clean_page.get_image_info()))
                    for margin in margins:clean_page.add_redact_annot(margin, fill=False)
                    clean_page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE,
                        graphics=fitz.PDF_REDACT_LINE_ART_NONE, text=fitz.PDF_REDACT_TEXT_REMOVE)
                    after = (len(clean_page.get_drawings()), len(clean_page.get_image_info()))
                    if after != before:
                        if continue_on_uncertainty:
                            issues.append({'code':'pdf_visual_incomplete','position':{'page':number,'bbox':list(clip)},
                                'message':'去除页码时图形变化；该图待核，其他内容继续处理。'})
                            continue
                        raise ScopeUncertain(f'第 {number} 页去页码时图形发生变化')
                    pixels = clean_page.get_pixmap(clip=clip * clean_page.rotation_matrix, matrix=fitz.Matrix(2, 2), alpha=False, annots=False)
                except (RuntimeError, ValueError):
                    if not continue_on_uncertainty:raise
                    issues.append({'code':'pdf_visual_incomplete','position':position,
                                   'message':'矢量图处理失败，其他内容继续；建议核对原页。'})
                    continue
                finally:
                    temporary.close()
                position['suppressed_margin_boxes'] = [list(box) for box in margins]
            else:
                try:
                    pixels = page.get_pixmap(clip=clip * page.rotation_matrix, matrix=fitz.Matrix(2, 2), alpha=False, annots=False)
                except (RuntimeError, ValueError):
                    if not continue_on_uncertainty:raise
                    issues.append({'code':'pdf_visual_incomplete','position':position,
                                   'message':'矢量图生成失败，其他内容继续；建议核对原页。'})
                    continue
            r = resource(f'pdf_vector_crop_{number}_{index}', pixels.tobytes('png'), '.png',
                         'pdf_vector_crop', position)
            first = group['blocks'][0]
            for target in group['blocks']:
                pos = target['position']
                positions.add((pos.get('page'), tuple(pos.get('bbox') or ())))
                kept.remove(target)
            block = {'id': first['id'], 'type': 'image', 'position': position,
                     'resource_id': r['id'], 'caption': first.get('caption', ''),
                     'source': 'pdf_vector_crop',
                     'ocr': {'status': 'not_run', 'verified': False}}
            kept.append(block)
            images.append(block)
    for issue in issues:
        pos = issue.get('position') or {}
        if issue.get('code') == 'pdf_vector_review' and (pos.get('page'), tuple(pos.get('bbox') or ())) in positions:
            issue['code'] = 'pdf_vector_crop_review'
            issue['message'] = '正文矢量图已保留局部原页图，图中文字和关系仍需核对。'


def preserve_table_page_evidence(source, kept, removed, resources, images, resource, issue):
    """Group clear table-rule fragments into a readable, scope-checked crop."""
    import fitz
    with open_pdf(source) as pdf:
        for page in pdf:
            number = page.number + 1
            page_blocks = [b for b in kept if (b.get('position') or {}).get('page') == number]
            vectors = [b for b in page_blocks if b.get('source') == 'pdf_vector_crop' and _box(b)]
            page_bounds = page.rect * page.derotation_matrix
            thin = [b for b in vectors if _box(b).width >= page_bounds.width*.35 and _box(b).height <= 8]
            if len(thin) < 3:
                continue
            companions = [b for b in vectors if _box(b).width >= page_bounds.width*.35]
            clip = fitz.Rect(_box(companions[0]))
            for block in companions[1:]:
                clip |= _box(block)
            clip = (clip + (-3, -3, 3, 3)) & page_bounds
            text_inside = [b for b in page_blocks if b.get('type') == 'paragraph' and _box(b)
                           and (_box(b) & clip).get_area() > 0 and str(b.get('text') or '').strip()]
            if len(text_inside) < 2:
                continue
            conflicts = [r for r in removed if r.get('reason') != 'repeated_page_margin'
                         and (r['block'].get('position') or {}).get('page') == number
                         and _box(r['block']) and (clip & _box(r['block'])).get_area() > 0]
            if conflicts:
                continue
            position = {'page': number, 'bbox': list(clip), 'table_region': True}
            try:
                pixels = page.get_pixmap(clip=clip * page.rotation_matrix, matrix=fitz.Matrix(2, 2),
                                         alpha=False, annots=False)
            except (RuntimeError, ValueError):
                issue('pdf_visual_incomplete', '表格页图生成失败，原图形片段和正文继续保留。', position)
                continue
            retained = resource(f'pdf_table_page_{number}', pixels.tobytes('png'), '.png',
                                'pdf_table_page_crop', position)
            for block in thin:
                kept.remove(block)
                images[:] = [im for im in images if im.get('id') != block.get('id')]
                rid = block.get('resource_id')
                if not any(rid in (b.get('resource_ids') or []) or b.get('resource_id') == rid for b in kept):
                    resources[:] = [row for row in resources if row.get('id') != rid]
            block = {'id': f'pdf_table_page_block_{number}', 'type': 'image', 'position': position,
                     'resource_id': retained['id'], 'caption': '表格所在区域，结构待核',
                     'source': 'pdf_table_page_crop'}
            kept.append(block)
            images.append(block)
            issue('pdf_table_structure_unverified', '已保留表格区域图和文字；表格结构及顺序建议核对。', position)


def guard_scope_columns(source, raw, kept, removed, issues):
    """Keep potential column-boundary errors reviewable, never silently accepted."""
    import fitz
    def original_id(block):
        return (block.get('source_scope') or {}).get('block_id') or block.get('id')
    kept_ids = {original_id(b) for b in kept}
    removed_ids = {original_id(r['block']) for r in removed if r.get('reason') != 'repeated_page_margin'}
    pieces = {'retained': {}, 'excluded': {}}
    for disposition, blocks in [('retained', kept), ('excluded',
            [r['block'] for r in removed if r.get('reason') != 'repeated_page_margin'])]:
        for block in blocks:
            pieces[disposition].setdefault(original_id(block), []).append(block)
    tail_kinds = {'references', 'acknowledgements'}
    headings = [r['block'] for r in removed if r.get('reason') in tail_kinds
                and any(heading_kind(line) in tail_kinds
                        for line in str(r['block'].get('text') or '').splitlines())]
    with open_pdf(source) as pdf:
        for heading in headings:
            number = (heading.get('position') or {}).get('page')
            box = _box(heading)
            if not isinstance(number,int) or number < 1 or number > len(pdf):
                raise ScopeUncertain('PDF 文末标题缺少可核验的原页，需人工确定正文范围')
            peers = [b for b in raw if (b.get('position') or {}).get('page') == number
                     and (b.get('type') != 'paragraph' or str(b.get('text') or '').strip())]
            # A normal one-column page with consistently aligned prose and
            # top-to-bottom order needs no artificial boundary review. Keep
            # the old review for mixed blocks, objects and possible columns.
            ordinary = [b for b in peers if b.get('id') != original_id(heading)]
            boxes = [_box(b) for b in ordinary]
            width = pdf[number-1].rect.width
            shared_ids = kept_ids & removed_ids
            if (box and len(ordinary) >= 2 and not shared_ids.intersection(b.get('id') for b in peers)
                    and all(b.get('type') == 'paragraph' for b in peers)
                    and all(rect is not None for rect in boxes)):
                left = sorted(rect.x0 for rect in boxes)[len(boxes)//2]
                consistent_left = max(rect.x0 for rect in boxes) - min(rect.x0 for rect in boxes) <= width * .05
                intersection = min(rect.x1 for rect in boxes) - max(rect.x0 for rect in boxes)
                shared_horizontal = intersection >= min(rect.width for rect in boxes) * .60
                mixed_widths = min(rect.width for rect in boxes) < width * .30 and max(rect.width for rect in boxes) > width * .65
                in_order = all(a.y0 <= b.y0 + 2 for a,b in zip(boxes,boxes[1:]))
                heading_aligned = (abs(box.x0-left) <= width*.12 or
                                   abs((box.x0+box.x1)/2 - pdf[number-1].rect.x1/2) <= width*.10)
                if consistent_left and shared_horizontal and not mixed_widths and in_order and heading_aligned:
                    continue
            def in_boundary(block, disposition):
                bounds = _box(block)
                if bounds is None or box is None:return True
                if block.get('id') == original_id(heading) and '\n' not in str(block.get('text') or ''):
                    return block.get('id') in kept_ids and block.get('id') in removed_ids
                return bounds.y1 >= box.y0 if disposition == 'excluded' else bounds.y0 <= box.y1
            candidates = [
                ('pdf_column_removed_review', [b for b in peers if b.get('id') in removed_ids
                    and in_boundary(b, 'excluded')], 'excluded'),
                ('pdf_column_kept_review', [b for b in peers if b.get('id') in kept_ids
                    and in_boundary(b, 'retained')], 'retained')]
            for code, group, disposition in candidates:
                if not group:continue
                members=[]
                for block in group:
                    for piece in pieces[disposition].get(block['id'], []):
                        scope=piece.get('source_scope') or {}
                        paragraph=block.get('type') == 'paragraph'
                        members.append({'source_block_id':block['id'], 'type':block.get('type'),
                            'source_start':scope.get('start',0) if paragraph else None,
                            'source_end':scope.get('end',len(str(block.get('text') or ''))) if paragraph else None,
                            'resource_id':block.get('resource_id'), 'resource_ids':block.get('resource_ids'),
                            'screenshot_ids':block.get('screenshot_ids'),
                            'bbox':list(_box(block)) if _box(block) else None, 'disposition':disposition})
                issues.append({'code':code, 'message':'文末边界页需核对正文和排除内容的范围。',
                               'position':{'page':number, 'heading_bbox':list(box) if box else None, 'members':members}})


def check_native_visuals(source, raw, kept, removed, images, scan_pages, issues, *, continue_on_uncertainty=False):
    """Detect unrepresented PDF visuals and images crossing excluded text."""
    import fitz
    def problem(message, position):
        if not continue_on_uncertainty:
            raise ScopeUncertain(message)
        issues.append({'code': 'pdf_visual_incomplete', 'position': position,
                       'message': '页面有未确认的图形或图片范围，其他内容继续处理；建议核对原页。'})
    with open_pdf(source) as pdf:
        kept_pages = {(b.get('position') or {}).get('page') for b in kept}
        for page in pdf:
            number = page.number + 1
            if number in scan_pages or number not in kept_pages:
                continue
            native_images = [b for b in images if (b.get('position') or {}).get('page') == number]
            try:
                image_info = page.get_image_info()
                bboxlog = page.get_bboxlog()
                annotations = bool(any(page.annots() or []))
                drawings = page.get_drawings()
            except (RuntimeError, ValueError):
                problem('页面图形检查失败', {'page':number})
                continue
            for info in image_info:
                box = fitz.Rect(info.get('bbox') or [])
                if box.is_empty or box.is_infinite:
                    problem(f'第 {number} 页图像位置无法核实，需 OCR 或人工核验', {'page':number})
                    continue
                if not any(_box(b) and (_box(b) & box).get_area() >= box.get_area()*.90
                           for b in native_images):
                    problem(f'第 {number} 页存在未提取图像，需 OCR 或人工核验', {'page':number,'bbox':list(box)})
            if any(kind == 'fill-shade' for kind, *_ in bboxlog):
                problem(f'第 {number} 页有未解析的渐变图形，需 OCR 或人工核验', {'page':number})
            if annotations:
                issues.append({'code':'pdf_annotation_review','position':{'page':number},
                               'message':'页面包含批注，正文解析和图像不包含批注；需核对其是否为研究内容。'})
            known_regions = [_box(b) for b in raw if (b.get('position') or {}).get('page') == number
                             and (b.get('unresolved_object') or b.get('type') == 'table') and _box(b)]
            for drawing in drawings:
                rect = fitz.Rect(drawing.get('rect') or [])
                if rect.is_empty or rect.is_infinite or rect.width < 2 or rect.height < 2:
                    continue
                if not any((rect & area).get_area() >= rect.get_area()*.90 for area in known_regions):
                    problem(f'第 {number} 页有未提取矢量图形，需 OCR 或人工核验', {'page':number,'bbox':list(rect)})
            kept_boxes = [_box(b) for b in kept if (b.get('position') or {}).get('page') == number
                          and b.get('type') != 'image' and _box(b)]
            removed_boxes = [_box(r['block']) for r in removed
                             if (r['block'].get('position') or {}).get('page') == number
                             and r['block'].get('type') != 'image' and _box(r['block'])]
            kept_ids = {b.get('resource_id') for b in kept if b.get('type') == 'image'}
            for image in native_images:
                box = _box(image)
                if not box:
                    problem(f'第 {number} 页图像缺少坐标，需 OCR 或人工核验', {'page':number})
                    continue
                hit_kept = any((box & other).get_area() > 0 for other in kept_boxes)
                hit_removed = any((box & other).get_area() > 0 for other in removed_boxes)
                if (image.get('resource_id') in kept_ids and hit_removed) or (
                        image.get('resource_id') not in kept_ids and hit_kept):
                    problem(f'第 {number} 页图像跨越正文边界，需 OCR 或人工核验', {'page':number,'bbox':list(box)})
