"""Conservative PDF geometry helpers; uncertain content stays a visual target."""
from collections import defaultdict
import math
import re
from statistics import median

import fitz


_CAPTION = re.compile(r'^\s*(?:表\s*[\d一二三四五六七八九十]+(?:[.．-]\d+)*|Table\s+(?:[A-Z][.-])?\d+(?:[.-]\d+)*)', re.I)


def white_backdrop(drawing):
    """Ignore plain white rectangles only for discovery, never edit page pixels."""
    fill = drawing.get('fill')
    return (drawing.get('type') == 'f' and fill is not None
            and all(v >= .999 for v in fill)
            and bool(drawing.get('items'))
            and all(item[0] == 're' for item in drawing['items']))


def text_backdrop(drawing, text_boxes):
    """Recognize pale rectangles bounded by real text, without hiding pale charts."""
    fill=drawing.get('fill');items=drawing.get('items',[])
    if (drawing.get('type')!='f' or not fill or min(fill)<.95
            or max(fill)-min(fill)>.015 or not items
            or any(item[0]!='re' for item in items)):
        return False
    for item in items:
        rect=fitz.Rect(item[1])
        if rect.height<5 or not any(
                rect in (fitz.Rect(box)+(-2,-2,2,2))
                and .75*fitz.Rect(box).height<=rect.height<=1.5*fitz.Rect(box).height
                for box in text_boxes):
            return False
    return True


def adjacent_text_regions(page_blocks):
    """Join same-row text spans across word-sized gaps, never across wide chart gaps."""
    pending=[fitz.Rect(span['bbox']) for block in page_blocks for line in block.get('lines',[])
             for span in line.get('spans',[]) if span.get('text','').strip()]
    groups=[]
    while pending:
        area=pending.pop();changed=True
        while changed:
            changed=False
            for index in range(len(pending)-1,-1,-1):
                other=pending[index]
                gap=max(area.x0-other.x1,other.x0-area.x1,0)
                center_delta=abs((area.y0+area.y1-other.y0-other.y1)/2)
                if center_delta<=.25*min(area.height,other.height) and gap<=max(area.height,other.height)+2:
                    area=area|other;pending.pop(index);changed=True
        groups.append(area)
    return groups


def horizontal_segments(drawing):
    if white_backdrop(drawing):
        return []
    segments = []
    for item in drawing.get('items', []):
        if item[0] == 'l':
            a, b = item[1:3]
            if abs(a.y - b.y) <= 1 and abs(a.x - b.x) >= 1:
                segments.append(fitz.Rect(min(a.x, b.x), min(a.y, b.y), max(a.x, b.x), max(a.y, b.y)))
        elif item[0] == 're':
            rect = fitz.Rect(item[1])
            if rect.height <= 3 and rect.width >= rect.height:
                segments.append(rect)
    return segments


def bibliographic_footer_rule(page, drawings):
    """Recognize a lone first-page separator supported by publication metadata.

    This does not suppress text or alter the PDF. Multiple paths, nearby images,
    body rules and unsupported footer lines remain visual review targets.
    """
    if page.number != 0 or len(drawings) != 1:
        return set()
    drawing=drawings[0];items=drawing.get('items',[])
    if (drawing.get('type')!='s' or drawing.get('fill') is not None
            or (drawing.get('width') or 0)>1 or len(items)!=1 or items[0][0]!='l'):
        return set()
    rect=fitz.Rect(drawing['rect']);bounds=page.rect
    if (rect.height>.5 or not bounds.width*.4<=rect.width<=bounds.width*.85
            or not bounds.height*.88<=rect.y0<=bounds.height*.95):
        return set()
    blocks=page.get_text('dict')['blocks']
    if any(b['type']==1 and fitz.Rect(b['bbox']).y1>=rect.y0-12 for b in blocks):
        return set()
    lines=[line for block in blocks if block['type']==0 for line in block.get('lines',[])
           if rect.y0<=line['bbox'][1]<=rect.y0+35]
    lines.sort(key=lambda line:(line['bbox'][1],line['bbox'][0]))
    text=' '.join(''.join(span.get('text','') for span in line['spans']) for line in lines)
    if (not lines or lines[0]['bbox'][1]-rect.y0>10
            or not re.match(r'\s*Manuscript\s+Received\b',text,re.I)
            or not re.search(r'\bAccepted\b',text,re.I)
            or not re.search(r'\bDOI\s*:',text,re.I)):
        return set()
    return {0}


def text_supported_rules(page, drawings):
    """Recognize isolated rules only when native text establishes their role.

    Size alone is never evidence that a drawing is decorative. Curves, vertical
    marks, chart-adjacent lines and rules without the required text stay targets.
    The original text and page pixels are not changed.
    """
    blocks=page.get_text('dict')['blocks'];bounds=page.rect
    lines=[line for block in blocks if block.get('type')==0
           for line in block.get('lines',[]) if any(s.get('text','').strip() for s in line['spans'])]
    prose_regions=adjacent_text_regions(blocks)
    found=set()
    for index,drawing in enumerate(drawings):
        items=drawing.get('items',[])
        if len(items)!=1 or items[0][0] not in {'l','re'}:
            continue
        rect=fitz.Rect(drawing['rect'])
        color=drawing.get('fill') if drawing.get('type')=='f' else drawing.get('color')
        if (drawing.get('type') not in {'s','f'} or not color or max(color)>.1
                or (drawing.get('width') or 0)>1 or rect.height>.8 or rect.width<2
                or drawing.get('dashes') not in {None,'[] 0'}):
            continue
        # A nearby plot border, connector or image defeats the decorative proof.
        neighborhood=rect+(-12,-24,12,24)
        if (any(i!=index and not white_backdrop(other)
                and (fitz.Rect(other['rect'])+(-.1,-.1,.1,.1)).intersects(neighborhood)
                for i,other in enumerate(drawings))
                or any(b.get('type')==1 and fitz.Rect(b['bbox']).intersects(neighborhood) for b in blocks)):
            continue
        if bounds.height*.65<=rect.y0 and bounds.width*.12<=rect.width<=bounds.width*.4:
            below=[line for line in lines if 0<=line['bbox'][1]-rect.y1<=20
                   and abs(line['bbox'][0]-rect.x0)<=12]
            first=min(below,key=lambda line:line['bbox'][1]) if below else None
            body_sizes=[s['size'] for line in lines if bounds.height*.15<line['bbox'][1]<rect.y0
                        for s in line['spans'] if len(s.get('text','').strip())>=12]
            if first is not None and body_sizes:
                text=''.join(s.get('text','') for s in first['spans']).strip()
                spans=[s for s in first['spans'] if s.get('text','').strip()]
                # Word PDFs also use raised Arabic note references. Require a
                # separate, smaller marker span; ordinary numbered legends do
                # not establish a footnote separator merely by starting with 1.
                numeric_marker=(len(spans)>1
                    and re.fullmatch(r'\d{1,3}',spans[0]['text'].strip())
                    and re.match(r'^\d{1,3}\s',text)
                    and spans[0]['size']<max(s['size'] for s in spans[1:])*.85)
                if ((re.match(r'^[①-⑳]',text) or numeric_marker) and len(text)>=12
                        and max(s['size'] for s in first['spans'])<median(body_sizes)*.95):
                    found.add(index);continue
        if rect.y1<=bounds.height*.12 and bounds.width*.5<=rect.width<=bounds.width*.9:
            above=[line for line in lines if 0<=rect.y0-line['bbox'][3]<=20
                   and rect.x0-2<=line['bbox'][0] and line['bbox'][2]<=rect.x1+2]
            if any(re.search(r'毕业论文|学位论文|\b(?:thesis|dissertation)\b',
                             ''.join(s.get('text','') for s in line['spans']),re.I)
                   and max(s['size'] for s in line['spans'])<=18 for line in above):
                found.add(index);continue
        if 2<=rect.width<=36:
            for box in prose_regions:
                text=''.join(s.get('text','') for line in lines for s in line['spans']
                             if fitz.Rect(s['bbox']).intersects(box)).strip()
                if (len(text)>=20 and box.width>=bounds.width*.35
                        and box.x0<=rect.x0<rect.x1<=box.x1
                        and box.y0+box.height*.6<=rect.y0<=box.y1+1):
                    found.add(index);break
    return found


def repeated_margin_rules(pdf, drawings_by_page):
    """Require both repetition and a long, thin rule in the page margins."""
    occurrences = defaultdict(list)
    for page_index, drawings in enumerate(drawings_by_page):
        bounds = pdf[page_index].rect
        for drawing_index, drawing in enumerate(drawings):
            rect = fitz.Rect(drawing['rect'])
            if (rect.width < bounds.width * .5 or rect.height > 3
                    or not (rect.y1 <= bounds.height * .12 or rect.y0 >= bounds.height * .88)
                    or not horizontal_segments(drawing)
                    or any(item[0] not in {'l', 're'} for item in drawing.get('items', []))):
                continue
            key = tuple(round(value / size * 500) for value, size in
                        zip(rect, (bounds.width, bounds.height, bounds.width, bounds.height)))
            occurrences[key].append((page_index, drawing_index))
    bibliographic={(page_index,index) for page_index,drawings in enumerate(drawings_by_page)
                   for index in (bibliographic_footer_rule(pdf[page_index],drawings)
                                 | text_supported_rules(pdf[page_index],drawings))}
    minimum = max(2, math.ceil(len(pdf) / 2))
    return bibliographic | {pair for entries in occurrences.values()
            if len({p for p, _ in entries}) >= minimum for pair in entries}


def _joined_rules(drawings):
    bands = []
    for rect in sorted((r for d in drawings for r in horizontal_segments(d)), key=lambda r: (r.y0, r.x0)):
        band = next((b for b in bands if abs(b[0].y0 - rect.y0) <= 2), None)
        if band is None:
            bands.append([rect])
        else:
            band.append(rect)
    rules = []
    for band in bands:
        merged = []
        for rect in sorted(band, key=lambda r: r.x0):
            if merged and rect.x0 <= merged[-1].x1 + 3:
                # Rect union ignores zero-height stroke lines.
                old = merged[-1]
                merged[-1] = fitz.Rect(min(old.x0, rect.x0), min(old.y0, rect.y0),
                                      max(old.x1, rect.x1), max(old.y1, rect.y1))
            else:
                merged.append(fitz.Rect(rect))
        rules.extend(merged)
    return sorted(rules, key=lambda r: (r.y0, r.x0))


def _compact(text):
    return re.sub(r'\s+', '', text or '')


def validated_text_grid(page, clip, divider=None, column_edges=None):
    """Accept only complete, unambiguous native word-to-cell assignments."""
    # Read whole source words before clipping: a clip can silently cut a word
    # and make two equally truncated extraction paths appear to agree.
    words = [word for word in page.get_text('words') if fitz.Rect(word[:4]).intersects(clip)]
    if not words:
        return None
    settings = {'vertical_strategy': 'text', 'horizontal_strategy': 'text', 'min_words_vertical': 2}
    if column_edges is not None:
        # Text-generated horizontal edges end at the outermost words. Keep the
        # demonstrated interior columns, with outer edges at those same words.
        edges = [min(w[0] for w in words), *column_edges[1:-1], max(w[2] for w in words)]
        settings.update(vertical_strategy='explicit', vertical_lines=edges)
    found = page.find_tables(clip=clip, **settings).tables
    if len(found) != 1:
        return None
    table = found[0]
    values = table.extract()
    columns = table.col_count
    minimum_rows = 3 if divider is not None else 2
    if columns < 2 or len(values) < minimum_rows:
        return None
    kept = []
    for row_index, row in enumerate(values):
        boxes = table.rows[row_index].cells
        if len(row) != columns or len(boxes) != columns or any(box is None for box in boxes):
            return None
        rect = fitz.Rect(table.rows[row_index].bbox)
        row_words = [w for w in words if rect.y0 <= (w[1] + w[3]) / 2 <= rect.y1]
        if not any(_compact(v) for v in row):
            # The text strategy creates geometry between baselines. Drop only
            # empty intervals, never an empty cell in a populated source row.
            if row_words:
                return None
            continue
        if not row_words or sum(bool(_compact(v)) for v in row) < 2:
            return None
        kept.append((row, boxes, list(rect)))
    if len(kept) < minimum_rows:
        return None
    if divider is not None:
        headers = [index for index, (_, _, bbox) in enumerate(kept) if bbox[3] < divider.y0 + 2]
        # Multiple header levels or split cell lines need scoped visual recovery.
        if headers != [0] or kept[1][2][1] < divider.y1 - 2:
            return None
    text_height = median(w[3] - w[1] for w in words)
    body = kept[1:] if divider is not None else kept
    if any(current[2][1] - previous[2][3] < text_height * .35
           for previous, current in zip(body, body[1:])):
        return None
    assigned = [[] for _ in range(len(kept) * columns)]
    for word in words:
        cx, cy = (word[0] + word[2]) / 2, (word[1] + word[3]) / 2
        matches = [ri * columns + ci for ri, (_, boxes, _) in enumerate(kept)
                   for ci, box in enumerate(boxes)
                   if box[0] <= cx <= box[2] and box[1] <= cy <= box[3]
                   and word[0] >= box[0] - 1.5 and word[2] <= box[2] + 1.5]
        if len(matches) != 1:
            return None
        assigned[matches[0]].append(word)
    extents = [[] for _ in range(columns)]
    for ri, (row, _, _) in enumerate(kept):
        for ci, value in enumerate(row):
            original = sorted(assigned[ri * columns + ci], key=lambda w: (w[1], w[0]))
            if _compact(''.join(w[4] for w in original)) != _compact(value):
                return None
            extents[ci].extend(original)
    # Ordinary justified prose must not become a matrix of adjacent words.
    for left, right in zip(extents, extents[1:]):
        if not left or not right or min(w[0] for w in right) - max(w[2] for w in left) < 4:
            return None
    return {'values': [row for row, _, _ in kept],
            'cell_boxes': [[list(box) for box in boxes] for _, boxes, _ in kept],
            'row_boxes': [bbox for _, _, bbox in kept]}


def three_line_tables(page, drawings, existing_bounds):
    captions = []
    for block in page.get_text('dict')['blocks']:
        for line in block.get('lines', []):
            text = ''.join(span.get('text', '') for span in line['spans'])
            if len(text) <= 160 and _CAPTION.match(text):
                captions.append((fitz.Rect(line['bbox']), text))
    rules = [r for r in _joined_rules(drawings) if r.width >= max(80, page.rect.width * .15)]
    results, unresolved = [], []
    for top, middle, bottom in zip(rules, rules[1:], rules[2:]):
        tolerance = max(4, top.width * .03)
        if (any(abs(r.x0 - top.x0) > tolerance or abs(r.x1 - top.x1) > tolerance for r in (middle, bottom))
                or middle.y0 - top.y1 < 5 or bottom.y0 - middle.y1 < 15):
            continue
        nearby = [(r, text) for r, text in captions
                  if 0 <= top.y0 - r.y1 <= max(40, r.height * 4)
                  and top.x0 - tolerance <= (r.x0 + r.x1) / 2 <= top.x1 + tolerance]
        if not nearby:
            continue
        caption_rect, caption = max(nearby, key=lambda item: item[0].y1)
        if any(caption_rect.y1 < r.y0 < bottom.y1 for r, _ in captions):
            continue
        clip = fitz.Rect(min(top.x0, middle.x0, bottom.x0) - 1,
                         top.y0 - 1, max(top.x1, middle.x1, bottom.x1) + 1, bottom.y1 + 1)
        if any(clip.intersects(area) for area in existing_bounds):
            continue
        grid = validated_text_grid(page, clip, middle)
        if grid:
            results.append({**grid, 'bbox': list(clip), 'caption': caption,
                            'border_rules': [list(top), list(middle), list(bottom)]})
            existing_bounds = [*existing_bounds, clip]
        else:
            unresolved.append(_table_candidate(page, clip, caption))
    return results, unresolved


def _table_candidate(page, clip, caption='', previous=None):
    lines = []
    bbox = fitz.Rect(clip)
    for block in page.get_text('dict')['blocks']:
        for line in block.get('lines', []):
            rect = fitz.Rect(line['bbox'])
            if rect.intersects(clip):
                text = ''.join(span.get('text', '') for span in line['spans']).strip()
                if text:
                    lines.append(text)
                    bbox |= rect
    return {'bbox': list(bbox), 'caption': caption.strip(), 'locator_texts': lines,
            'table_candidate': True, 'table_fragment': previous is not None,
            'possible_previous_table': previous}


def continuation_fragments(page, drawings, previous_tables, previous_height, occupied):
    """Locate page-start fragments; preserve separately, without inferred headers."""
    previous = [table for table in previous_tables if table['position']['bbox'][3] >= previous_height * .7]
    if not previous:
        return []
    table = max(previous, key=lambda t: t['position']['bbox'][3])
    old = fitz.Rect(table['position']['bbox'])
    columns = table['columns']
    if columns < 2:
        return []
    starts = []
    for column in range(1, columns):
        boxes = [cell['position']['bbox'] for cell in table['cells']
                 if cell['column'] == column and cell.get('position', {}).get('bbox')]
        if not boxes:
            return []
        starts.append(min(box[0] for box in boxes))
    edges = [old.x0, *starts, old.x1]
    tolerance = max(4, old.width * .02)
    rules = [r for r in _joined_rules(drawings)
             if r.y0 < page.rect.height * .4 and abs(r.x0 - old.x0) <= tolerance
             and abs(r.x1 - old.x1) <= tolerance]
    fragments = []
    for top, bottom in zip(rules, rules[1:]):
        if top.y0 > page.rect.height * .2 or bottom.y0 - top.y1 < 15:
            continue
        clip = fitz.Rect(min(top.x0, bottom.x0) - 1, top.y0 - 1,
                         max(top.x1, bottom.x1) + 1, bottom.y1 + 1)
        if any(clip.intersects(fitz.Rect(area)) for area in occupied):
            continue
        words = [w for w in page.get_text('words') if fitz.Rect(w[:4]).intersects(clip)]
        if not words:
            continue
        height = median(w[3] - w[1] for w in words)
        rows = []
        for word in sorted(words, key=lambda w: ((w[1] + w[3]) / 2, w[0])):
            cy = (word[1] + word[3]) / 2
            if not rows or abs(cy - rows[-1][0]) > height * .5:
                rows.append((cy, []))
            rows[-1][1].append(word)
        extents = [[] for _ in range(columns)]
        valid = len(rows) >= 2
        for _, row in rows:
            occupied_columns = set()
            for word in row:
                matches = [ci for ci in range(columns)
                           if edges[ci] - 1.5 <= word[0] and word[2] <= edges[ci + 1] + 1.5]
                if len(matches) != 1:
                    valid = False
                    break
                ci = matches[0];occupied_columns.add(ci);extents[ci].append(word)
            if len(occupied_columns) < max(2, math.ceil(columns / 2)):
                valid = False
        if not valid or not all(extents):
            continue
        if any(min(w[0] for w in right) - max(w[2] for w in left) < 4
               for left, right in zip(extents, extents[1:])):
            continue
        fragments.append(_table_candidate(page, clip, previous=dict(table['position'])))
    return fragments


def cell_border_rules(cell_boxes):
    """Cell perimeter evidence, not the area enclosed by the whole table."""
    rules = []
    for box in cell_boxes:
        if box is None:
            continue
        x0, y0, x1, y1 = box
        rules.extend((fitz.Rect(x0, y0, x1, y0), fitz.Rect(x0, y1, x1, y1),
                      fitz.Rect(x0, y0, x0, y1), fitz.Rect(x1, y0, x1, y1)))
    return rules


def _on_boundary(segment, rules):
    orientations = []
    if segment.height <= 3:
        orientations.append((0, 1))
    if segment.width <= 3:
        orientations.append((1, 0))
    for along, across in orientations:
        intervals = []
        for rule in rules:
            if (rule[across + 2] - rule[across] <= 3
                    and abs((segment[across] + segment[across + 2]) / 2
                            - (rule[across] + rule[across + 2]) / 2) <= 1.5
                    and segment[across] >= rule[across] - 1.5
                    and segment[across + 2] <= rule[across + 2] + 1.5):
                intervals.append((rule[along], rule[along + 2]))
        covered = segment[along]
        for start, end in sorted(intervals):
            if end < covered - 1.5:
                continue
            if start > covered + 1.5:
                break
            covered = max(covered, end)
            if covered >= segment[along + 2] - 1.5:
                return True
    return False


def _confirmed_border(drawing, rules):
    """Suppress only paths wholly supported by known table boundary geometry."""
    items = drawing.get('items', [])
    if not items or (drawing.get('width') or 0) > 3:
        return False
    if drawing.get('closePath') and any(item[0] != 're' for item in items):
        return False
    segments = []
    for item in drawing.get('items', []):
        if item[0] == 'l':
            a, b = item[1:3]
            if abs(a.x - b.x) > .5 and abs(a.y - b.y) > .5:
                return False
            segments.append(fitz.Rect(min(a.x, b.x), min(a.y, b.y), max(a.x, b.x), max(a.y, b.y)))
        elif item[0] == 're':
            rect = fitz.Rect(item[1])
            if drawing.get('fill') is not None and drawing.get('type') in {'f', 'fs'}:
                if min(rect.width, rect.height) > 3:
                    return False
                segments.append(rect)
            else:
                segments.extend(cell_border_rules([rect]))
        else:
            return False
    return all(_on_boundary(segment, rules) for segment in segments)


def vector_regions(drawings, border_rules, unresolved_tables=(), text_boxes=()):
    """Group nearby graphics without merging unrelated objects across the page."""
    regions = [fitz.Rect(area) for area in unresolved_tables]
    for drawing in drawings:
        if white_backdrop(drawing) or text_backdrop(drawing,text_boxes):
            continue
        rect = fitz.Rect(drawing['rect'])
        if _confirmed_border(drawing, border_rules):
            continue
        regions.append(rect)
    groups = []
    while regions:
        area = regions.pop(0)
        changed = True
        while changed:
            changed = False
            for index in range(len(regions) - 1, -1, -1):
                other = regions[index]
                if (area.x0 <= other.x1 + 8 and other.x0 <= area.x1 + 8
                        and area.y0 <= other.y1 + 8 and other.y0 <= area.y1 + 8):
                    area = fitz.Rect(min(area.x0, other.x0), min(area.y0, other.y0),
                                     max(area.x1, other.x1), max(area.y1, other.y1))
                    regions.pop(index)
                    changed = True
        groups.append(area + (-2, -2, 2, 2))
    return groups
