"""Locate a native DOCX chart only when its original PDF clip is proven.

This optional locator neither interprets values nor declares privacy/completeness
passed. Unsupported source structures and ambiguous PDF matches return None.
"""
import hashlib
import math
import posixpath
import re
from urllib.parse import unquote, urlsplit
import xml.etree.ElementTree as ET
import zipfile

import fitz

from native_chart import points

W = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
WP = '{http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing}'
A = '{http://schemas.openxmlformats.org/drawingml/2006/main}'
C = '{http://schemas.openxmlformats.org/drawingml/2006/chart}'
R = '{http://schemas.openxmlformats.org/officeDocument/2006/relationships}'


def _compact(text):
    return re.sub(r'\s+', '', text)


def _source_chart(archive, position):
    if (position.get('part') != 'word/document.xml' or position.get('container_path')
            or type(position.get('body_index')) is not int or position['body_index'] < 0):
        return None
    body = ET.fromstring(archive.read('word/document.xml')).find(W + 'body')
    if body is None or position['body_index'] >= len(body):
        return None
    node = body[position['body_index']]
    if node.tag == W + 'tbl':
        row, column = position.get('row'), position.get('column')
        if type(row) is not int or type(column) is not int or min(row, column) < 0:
            return None
        rows = node.findall(W + 'tr')
        if row >= len(rows):
            return None
        cells = rows[row].findall(W + 'tc')
        # Grid offsets and merged cells need the full table layout resolver.
        if (rows[row].find('.//' + W + 'gridBefore') is not None
                or any(c.find('.//' + W + name) is not None
                       for c in cells for name in ('gridSpan', 'vMerge', 'tbl'))
                or column >= len(cells)):
            return None
        node = cells[column]
    elif node.tag != W + 'p' or 'row' in position or 'column' in position:
        return None
    drawings = list(node.iter(W + 'drawing'))
    if len(drawings) != 1 or any(n.tag in {W + 'pict', W + 'object'} for n in node.iter()):
        return None
    drawing = drawings[0]
    layouts = [n for n in drawing if n.tag in {WP + 'inline', WP + 'anchor'}]
    charts = list(drawing.iter(C + 'chart'))
    if (len(layouts) != 1 or len(charts) != 1
            or any(n.tag in {A + 'blip', W + 'txbxContent'} for n in drawing.iter())
            or any(n.tag.rsplit('}', 1)[-1] in {'group', 'wgp', 'grpSp', 'sp', 'wsp'} for n in drawing.iter())):
        return None
    if any(n.get('rot', '0') != '0' or n.get('flipH', '0') not in {'0', 'false'}
           or n.get('flipV', '0') not in {'0', 'false'} for n in drawing.iter(A + 'xfrm')):
        return None
    extent = layouts[0].find(WP + 'extent')
    if extent is None:
        return None
    size = [int(extent.get(key, '0')) / 12700 for key in ('cx', 'cy')]
    if not all(16 <= value <= 2000 for value in size):
        return None
    rid = charts[0].get(R + 'id')
    part = _chart_part(archive, 'word/document.xml', rid)
    if part is None:
        return None
    blob = archive.read(part)
    chart_root = ET.fromstring(blob)
    categories = _chart_categories(chart_root)
    if categories is None:
        return None
    label_evidence = _label_evidence(chart_root, categories)
    if label_evidence['locator_labels'] is None:
        return None
    return {'chart_part': part, 'relationship_id': rid, 'docx_extent_pt': size,
            'chart_sha256': hashlib.sha256(blob).hexdigest(), 'categories': categories,
            **label_evidence}


def _chart_part(archive, owner, rid):
    folder, name = posixpath.split(owner)
    rels = ET.fromstring(archive.read(posixpath.join(folder, '_rels', name + '.rels')))
    matches = [rel for rel in rels if rel.get('Id') == rid]
    if (not rid or len(matches) != 1 or matches[0].get('TargetMode', '').lower() == 'external'
            or matches[0].get('Type') != R[1:-1] + '/chart'):
        return None
    uri = urlsplit(matches[0].get('Target', ''))
    if uri.scheme or uri.netloc or uri.query or uri.fragment:
        return None
    path = unquote(uri.path)
    if not path or '\\' in path or ':' in path or any(ord(c) < 32 for c in path):
        return None
    part = posixpath.normpath(path[1:] if path.startswith('/') else posixpath.join(folder, path))
    if not part.startswith('word/charts/') or not part.endswith('.xml'):
        return None
    return part


def _chart_categories(root):
    categories = []
    series = root.findall('.//' + C + 'plotArea/*/' + C + 'ser')
    if not series:
        return None
    for series_node in series:
        category = series_node.find(C + 'cat')
        if category is None:
            return None
        caches = [n for n in category.iter() if n.tag in {C + 'strCache', C + 'strLit'}]
        multi = category.find(C + 'multiLvlStrRef/' + C + 'multiLvlStrCache')
        if multi is not None:
            levels = [level for level in multi.findall(C + 'lvl') if len(level)]
            count = multi.find(C + 'ptCount')
            if caches or len(levels) != 1 or count is None or any(n.tag != C + 'pt' for n in levels[0]):
                return None
            # Some DOCX files contain a complete label level plus an empty one.
            # Reuse index/count validation only for locating, never flatten data.
            locator_cache = ET.Element(C + 'strLit')
            locator_cache.append(count)
            locator_cache.extend(levels[0])
            labels, _ = points(locator_cache)
        else:
            if len(caches) != 1:
                return None
            labels, _ = points(category)
        if not labels or len(labels) > 256 or any(not _compact(label) for label in labels):
            return None
        categories.extend(_compact(label) for label in labels)
    categories = list(dict.fromkeys(categories))
    # Numeric ticks alone cannot identify an object safely.
    if len([c for c in categories if any(ch.isalpha() for ch in c)]) < 2:
        return None
    return categories


def _extent_tolerance(value):
    return max(.2, min(.75, value * .002))


def _label_evidence(root, categories):
    """Use literal custom labels without treating caches as rendered values.

    A default category/value pair is usable only with an explicit General label
    format and exact cache strings. Other number formats are not reconstructed.
    """
    result={'locator_labels':categories,
            'label_separators':list(dict.fromkeys(_compact(n.text or '')
                                     for n in root.iter(C+'separator') if _compact(n.text or '')))}
    series=root.findall('.//'+C+'plotArea/*/'+C+'ser')
    if len(series)!=1:
        return result
    group=series[0].find(C+'dLbls')
    if group is None:
        return result
    labels={}
    for label in group.findall(C+'dLbl'):
        index=label.find(C+'idx')
        if index is None or not index.get('val','').isdigit():
            return result
        number=int(index.get('val'))
        if number in labels or not 0<=number<len(categories):
            return result
        labels[number]=label
    if set(labels)!=set(range(len(categories))):
        return result
    custom=[_compact(''.join(t.text or '' for t in labels[i].findall(C+'tx/'+C+'rich//'+A+'t')))
            for i in range(len(categories))]
    if all(text and any(ch.isalpha() for ch in text) for text in custom):
        if len(set(custom))<2 or len(set(custom))!=len(custom):
            # Repeated labels are not independent location evidence. Reject
            # them rather than matching each repeated label to the same word.
            result['locator_labels']=None
            return result
        result['locator_labels']=custom
        result['label_source']='complete_native_custom_data_labels'
        return result
    try:
        values,_=points(series[0].find(C+'val'),numeric=True)
    except ValueError:
        return result
    if len(values)!=len(categories):
        return result
    for label in labels.values():
        def setting(name):
            node=label.find(C+name)
            return node if node is not None else group.find(C+name)
        fmt=setting('numFmt');cat=setting('showCatName');val=setting('showVal')
        percent=setting('showPercent')
        if (label.find(C+'tx') is not None or fmt is None or fmt.get('formatCode')!='General'
                or cat is None or cat.get('val') not in {'1','true'}
                or val is None or val.get('val') not in {'1','true'}
                or percent is not None and percent.get('val') in {'1','true'}):
            return result
    result['raw_category_value_labels']=[[label,value] for label,value in zip(categories,values)]
    return result


def _choice_label(text):
    # A PDF may split an option prefix into its own word. It cannot establish
    # two source identities merely because one cache stores "A." separately.
    return re.sub(r'^[A-Za-z][.、:：)]', '', _compact(text))


def _reference_extent(chart, parents):
    layout = parents.get(chart)
    while layout is not None and layout.tag not in {WP + 'inline', WP + 'anchor'}:
        layout = parents.get(layout)
    if layout is None or any(n.get('rot', '0') != '0' for n in layout.iter(A + 'xfrm')):
        return None
    extent = layout.find(WP + 'extent')
    if extent is None:
        return None
    try:
        size = [int(extent.get(key, '0')) / 12700 for key in ('cx', 'cy')]
    except ValueError:
        return None
    return size if all(16 <= value <= 2000 for value in size) else None


def _source_is_unique(archive, source):
    """Count all possible owners, including hidden/container/header drawings.

    A single PDF match cannot distinguish two original objects sharing its
    locator signature. Unsupported references cannot be assumed absent.
    """
    candidates = []
    for owner in archive.namelist():
        if not owner.startswith('word/') or owner.startswith('word/charts/') or not owner.endswith('.xml'):
            continue
        root = ET.fromstring(archive.read(owner))
        charts = list(root.iter(C + 'chart'))
        if not charts:
            continue
        parents = {child: parent for parent in root.iter() for child in parent}
        for chart in charts:
            size = _reference_extent(chart, parents)
            if size is not None and any(abs(a-b) > _extent_tolerance(a) + _extent_tolerance(b)
                                        for a, b in zip(size, source['docx_extent_pt'])):
                continue
            rid = chart.get(R + 'id')
            try:
                part = _chart_part(archive, owner, rid)
                root_chart = ET.fromstring(archive.read(part)) if part else None
                labels = _chart_categories(root_chart) if root_chart is not None else None
                if labels is not None and source.get('label_source')=='complete_native_custom_data_labels':
                    labels = _label_evidence(root_chart, labels)['locator_labels']
            except (ET.ParseError, KeyError, ValueError):
                labels = None
            wanted = {_choice_label(label) for label in source.get('locator_labels',source['categories'])}
            if labels is not None and not wanted.issubset({_choice_label(label) for label in labels}):
                continue
            if labels is None or size is None:
                return False
            candidates.append((owner, rid))
            if len(candidates) > 1:
                return False
    return candidates == [('word/document.xml', source['relationship_id'])]


def _labels_match(categories, words, separators=(), raw_pairs=()):
    """Match complete words, allowing labels wrapped across adjacent text runs."""
    tokens = [_compact(word[4]) for word in words]
    pairs = dict(raw_pairs)
    for label in categories:
        alternatives={label,*(label+separator for separator in separators)}
        if label in pairs:
            # LibreOffice's default rendered category/value delimiter is ';'.
            # Require the exact raw value as well; punctuation alone is no proof.
            alternatives.add(label+';'+pairs[label])
        limit=max(map(len,alternatives))
        occurrences = 0
        for start in range(len(tokens)):
            joined = ''
            for token in tokens[start:]:
                joined += token
                if joined in alternatives:
                    occurrences += 1
                    break
                if len(joined) >= limit:
                    break
        if occurrences != 1:
            return False
    return True


def find_native_chart_region(docx_path, target, pdf):
    """Return {page_index, bbox, provenance}, or None without changing inputs.

    ``pdf`` is an open fitz.Document rendered from the same DOCX. ``bbox`` is
    normalized to the unrotated original page. The source rectangle is never
    expanded. Captions and table questions outside the chart stay separate.
    """
    if any(len(target.get(key) or []) > 1 for key in ('graphic_members', 'member_positions')):
        return None
    position = target.get('position', {})
    if not isinstance(position, dict):
        return None
    try:
        with zipfile.ZipFile(docx_path) as archive:
            source = _source_chart(archive, position)
            if source is not None and not _source_is_unique(archive, source):
                return None
    except (OSError, zipfile.BadZipFile, ET.ParseError, KeyError, ValueError):
        return None
    if source is None:
        return None
    matches = []
    for page in pdf:
        # Skipping unsupported pages could incorrectly turn two matches into one.
        if page.rotation:
            return None
        words = page.get_text('words')
        seen = set()
        for index, drawing in enumerate(page.get_drawings(extended=True)):
            items = drawing.get('items', [])
            if drawing['type'] != 'clip' or len(items) != 1 or items[0][0] != 're':
                continue
            box = fitz.Rect(drawing['scissor'])
            if (not all(math.isfinite(v) for v in box) or box.is_empty or not box in page.rect
                    or (box.width >= page.rect.width * .95 and box.height >= page.rect.height * .95)
                    or any(abs(a-b) > .001 for a, b in zip(box, items[0][1]))):
                continue
            key = tuple(round(v, 4) for v in box)
            if key in seen:
                continue
            seen.add(key)
            if any(abs(actual-expected) > _extent_tolerance(expected)
                   for actual, expected in zip((box.width, box.height), source['docx_extent_pt'])):
                continue
            # Font ascent/descent boxes may extend beyond the rendered glyphs.
            # The original PDF clip, not a text-box union, defines visible pixels.
            inside = [w for w in words if box.contains(fitz.Point((w[0]+w[2])/2, (w[1]+w[3])/2))]
            if not _labels_match(source['locator_labels'], inside,
                                 source.get('label_separators',()),
                                 source.get('raw_category_value_labels',())):
                continue
            matches.append({'page_index': page.number,
                            'bbox': [box.x0/page.rect.width, box.y0/page.rect.height,
                                     box.x1/page.rect.width, box.y1/page.rect.height],
                            'provenance': {**source, 'method': 'native_chart_unique_pdf_clip',
                                           'bbox_pt': list(box), 'pdf_clip_index': index}})
            if len(matches) > 1:
                return None
    return matches[0] if len(matches) == 1 else None
