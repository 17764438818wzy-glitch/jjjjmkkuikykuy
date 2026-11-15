"""Source-bound reading-scope cleanup for the isolated candidate.

Only structurally bounded sections are excluded.  The caller owns image/page
validation and must not treat a missing abstract as an empty document.
"""

import copy
import math
import re


class ScopeUncertain(ValueError):
    """The permitted reading boundary cannot be established safely."""


INLINE_IMAGE = re.compile(r'!\s*\[|<(?:img|image|picture|svg)\b|data\s*:\s*image\s*/', re.I)


def contains_inline_image(value):
    if isinstance(value, str):return bool(INLINE_IMAGE.search(value))
    if isinstance(value, dict):return any(contains_inline_image(v) for v in value.values())
    if isinstance(value, (list, tuple)):return any(contains_inline_image(v) for v in value)
    return False


def normalize_layout(blocks, *, page_heights=None):
    """Normalize only proven prose whitespace; retain explicit source mappings."""
    heights = page_heights or {}
    result, changes = [], []

    def safe_prose(row):
        text = str(row.get('text') or '')
        return (row.get('type') == 'paragraph' and not row.get('heading_level')
                and not row.get('list') and not row.get('note_label')
                and not row.get('resource_ids') and not row.get('resource_id')
                and not row.get('unresolved_object')
                and not re.search(r'(?m)^(?: {2,}|\t)\S', text)
                and not re.search(r'```|\[\[(?:IMAGE|OBJECT):|\$|\\\(|\\\[', text)
                and not re.match(r'^\s*(?:第\s*[零〇一二三四五六七八九十百两\d]+\s*[章篇]|chapter\s+[0-9ivxlc]+\b)', text, re.I)
                and not re.match(r'^\s*(?:摘要|摘\s+要|abstract)\s*(?:\n|[:：])', text, re.I))

    def rewrite(row):
        text = str(row.get('text') or '')
        edits = []
        for match in re.finditer(r'[ \t]*\r?\n[ \t]*|[ \t\u3000]{2,}', text):
            old = match.group()
            start, end = match.span()
            before = text[start - 1] if start else ''
            after = text[end] if end < len(text) else ''
            if '\n' in old:
                if not before or not after or before in '.!?。！？；;：:-' or after in '\r\n':
                    continue
                remainder = text[end:]
                if re.match(r'(?:[-*•]|\d+[.)、])\s+', remainder):continue
                replacement = ' ' if before.isascii() and after.isascii() and before.isalnum() and after.isalnum() else ''
                kind = 'wrapped_line'
            else:
                replacement, kind = ' ', 'extra_spaces'
            edits.append((start, end, replacement, kind))
        output, mapping, cursor, target = [], [], 0, 0
        for start, end, replacement, kind in edits:
            unchanged = text[cursor:start]
            if unchanged:
                output.append(unchanged)
                mapping.append({'target_start': target, 'target_end': target+len(unchanged),
                                'source_start': cursor, 'source_end': start})
                target += len(unchanged)
            output.append(replacement)
            mapping.append({'target_start': target, 'target_end': target+len(replacement),
                            'source_start': start, 'source_end': end})
            changes.append({'kind': kind, 'block_id': row['id'], 'source_start': start,
                            'source_end': end, 'replacement': replacement})
            target += len(replacement)
            cursor = end
        tail = text[cursor:]
        if tail:
            output.append(tail)
            mapping.append({'target_start': target, 'target_end': target+len(tail),
                            'source_start': cursor, 'source_end': len(text)})
        if edits:
            row['text'] = ''.join(output)
            row['layout_mapping'] = mapping
        return row

    for original in blocks:
        row = copy.deepcopy(original)
        if (row.get('type') == 'paragraph' and not str(row.get('text') or '').strip()
                and not row.get('resource_ids') and not row.get('resource_id')
                and not row.get('unresolved_object') and not row.get('note_label')):
            changes.append({'kind': 'empty_paragraph', 'block_id': row['id'],
                            'position': copy.deepcopy(row.get('position') or {})})
            continue
        if safe_prose(row):row = rewrite(row)
        result.append(row)

    def continuation(left, right):
        if not safe_prose(left) or not safe_prose(right):return False
        last = (left.get('source_segments') or [{}])[-1]
        lp, rp = last.get('position') or left.get('position') or {}, right.get('position') or {}
        p, q = lp.get('page'), rp.get('page')
        a, b = lp.get('bbox'), rp.get('bbox')
        if not (isinstance(p, int) and q == p+1 and p in heights and q in heights
                and isinstance(a, (list, tuple)) and isinstance(b, (list, tuple))
                and len(a)==4 and len(b)==4):return False
        if not (a[3]>=heights[p]*.80 and b[1]<=heights[q]*.20):return False
        if abs(a[0]-b[0]) > max(8, min(a[2]-a[0],b[2]-b[0])*.08):return False
        # A comma at the end plus matching page geometry is a conservative
        # continuation signal; other page boundaries remain unchanged.
        return bool(re.search(r'[，,]\s*$', str(left.get('text') or '')) and str(right.get('text') or '').strip())

    joined = []
    for row in result:
        if joined and continuation(joined[-1], row):
            left = joined[-1]
            chinese = (re.search(r'，$', str(left.get('text') or ''))
                       and re.match(r'^[\u4e00-\u9fff]', str(row.get('text') or '')))
            english = (re.search(r',$', str(left.get('text') or ''))
                       and re.match(r'^[a-z]', str(row.get('text') or '')))
            if chinese or english:
                def segments(block, offset):
                    if block.get('source_segments'):
                        return [{**copy.deepcopy(m), 'target_start': offset+m['target_start'],
                                 'target_end': offset+m['target_end']} for m in block['source_segments']]
                    scope = block.get('source_scope') or {}
                    source_id = scope.get('block_id') or block['id']
                    base = int(scope.get('start', 0))
                    mapping = block.get('layout_mapping') or [dict(target_start=0,
                        target_end=len(block['text']), source_start=0, source_end=len(block['text']))]
                    return [dict(target_start=offset+m['target_start'], target_end=offset+m['target_end'],
                                 source_block_id=source_id, source_start=base+m['source_start'],
                                 source_end=base+m['source_end'], position=copy.deepcopy(block.get('position') or {}))
                            for m in mapping if m['target_end'] > m['target_start']]
                combined = copy.deepcopy(left)
                separator = ' ' if english else ''
                combined['source_segments'] = segments(left, 0)
                if separator:
                    combined['source_segments'].append({'target_start':len(left['text']),
                        'target_end':len(left['text'])+1,'synthetic_separator':True})
                combined['source_segments'] += segments(row, len(left['text'])+len(separator))
                combined.pop('layout_mapping', None)
                combined.pop('source_scope', None)
                combined['text'] = left['text'] + separator + row['text']
                joined[-1] = combined
                changes.append({'kind': 'cross_page_join', 'block_id': left['id'],
                                'continuation_block_id': row['id']})
                continue
            changes.append({'kind': 'possible_cross_page_continuation', 'block_id': left['id'],
                            'continuation_block_id': row['id']})
        joined.append(row)
    return joined, changes


def map_clean_range(block, start, end):
    """Map a cleaned prose interval to original parser-block offsets.

    Replaced whitespace maps to its complete original interval. Deleted
    whitespace can leave gaps between returned spans. Tables are intentionally
    excluded because their text is assembled from multiple original cells.
    """
    text = str(block.get('text') or '')
    if block.get('type') != 'paragraph' or not (isinstance(start, int) and isinstance(end, int) and 0 <= start < end <= len(text)):
        raise ValueError('不能将此范围可靠映射到原文段落')
    if block.get('source_segments'):
        spans, covered = [], 0
        for row in block['source_segments']:
            a, b = row['target_start'], row['target_end']
            x, y = max(start, a), min(end, b)
            if x >= y:continue
            if row.get('synthetic_separator'):
                covered += y-x
                continue
            sa, sb = row['source_start'], row['source_end']
            source_start, source_end = (sa+x-a, sa+y-a) if b-a == sb-sa else (sa, sb)
            spans.append(dict(source_block_id=row['source_block_id'], source_start=source_start,
                              source_end=source_end, position=copy.deepcopy(row.get('position') or {})))
            covered += y-x
        if covered != end-start:raise ValueError('多来源块缺少完整来源映射')
        return spans
    scope = block.get('source_scope') or {}
    source_id = scope.get('block_id') or block.get('source_block_id') or block.get('id')
    base = int(scope.get('start', 0))
    mapping = block.get('layout_mapping')
    if not mapping:
        mapping = [dict(target_start=0, target_end=len(text), source_start=0, source_end=len(text))]
    spans = []
    covered = 0
    for row in mapping:
        a, b = row['target_start'], row['target_end']
        x, y = max(start, a), min(end, b)
        if x >= y:
            continue
        sa, sb = row['source_start'], row['source_end']
        if b-a == sb-sa:
            source_start, source_end = sa+x-a, sa+y-a
        else:
            source_start, source_end = sa, sb
        spans.append(dict(source_block_id=source_id, source_start=base+source_start,
                          source_end=base+source_end, position=copy.deepcopy(block.get('source_position') or block.get('position') or {})))
        covered += y-x
    if covered != end-start:
        raise ValueError('清理范围缺少完整来源映射')
    return spans


def heading_kind(line):
    line = re.sub(r'^\s*#{1,6}\s+', '', line).strip()
    line = re.sub(r'^[`·•]+\s*(?=(?:目\s*录|contents|table\s+of\s+contents)\s*[:：]?\s*$)', '', line, flags=re.I)
    if not line or re.search(r'(?:\.{2,}|[·…]{2,})\s*(?:\d+|[ivxlc]+)\s*$', line, re.I):
        return None
    if re.fullmatch(r'(?:(?:中\s*文|内\s*容|论\s*文)\s*)?摘\s*要\s*(?:[:：].*)?|内\s*容\s*提\s*要\s*(?:[:：].*)?', line, re.I):
        return 'abstract'
    if re.fullmatch(r'abstract\s*(?:[:：].*)?', line, re.I):
        return 'abstract'
    prefix = r'(?:(?:第\s*[零〇一二三四五六七八九十百两\d]+\s*[章篇]|\d+(?:[.．]\d+)*[.、．]?)\s*)?'
    if re.fullmatch(prefix + r'(?:(?:主要)?参\s*考\s*文\s*献(?:及注释)?(?:\s*[（(]references[)）])?|参考书目|参考资料|references(?:\s+cited)?|works\s+cited|bibliography)\s*[:：]?', line, re.I):
        return 'references'
    if re.fullmatch(prefix + r'(?:致\s*谢|致\s*謝|鸣\s*谢|谢\s*辞|acknowledg(?:e)?ments?)\s*[:：]?', line, re.I):
        return 'acknowledgements'
    if not re.search(r'[。；;？！?!]', line) and re.fullmatch(prefix + r'附\s*录(?:\s*[A-Za-z0-9一二三四五六七八九十]+[.．]?)?(?:\s+.*|\s*[:：].*)?', line, re.I):
        return 'appendix'
    if not re.search(r'[。；;？！?!]', line) and re.fullmatch(r'appendi(?:x|ces)(?:\s+[A-Za-z0-9]+)?(?:\s+.*|\s*[:：].*)?', line, re.I):
        return 'appendix'
    if re.fullmatch(r'(?:目\s*录|contents|table\s+of\s+contents)\s*[:：]?', line, re.I):
        return 'toc'
    if re.match(r'^第\s*[零〇一二三四五六七八九十百两\d]+\s*[章篇](?:\s|\S)', line):
        return 'research_chapter'
    if re.match(r'^chapter\s+[0-9ivxlc]+\b', line, re.I):
        return 'research_chapter'
    if re.match(r'^(?:1[89]|20|21)\d{2}\s*年', line):
        return None
    if re.match(r'^\d+(?:[.．]\d+)*[.、．]?\s+\S', line) or re.fullmatch(r'绪论|引言|结论|结语',line):
        return 'research_chapter'
    return None


def scope_lines(text):
    """Identify a few line-wrapped headings without changing source offsets."""
    raw = str(text or '').splitlines(keepends=True) or ['']
    starts, offset = [], 0
    for line in raw:
        starts.append(offset)
        offset += len(line)
    patterns = (('摘要', 'abstract'), ('目录', 'toc'), ('参考文献', 'references'),
                ('致谢', 'acknowledgements'), ('附录', 'appendix'))
    result, index = [], 0
    while index < len(raw):
        merged = None
        for name, expected in patterns:
            for width in range(2, min(len(name), len(raw) - index) + 1):
                parts = [part.strip() for part in raw[index:index + width]]
                if not all(parts):
                    continue
                prefix = re.sub(r'\s+', '', ''.join(parts[:-1]))
                candidate = ''.join(parts)
                plain = re.sub(r'\s+', '', candidate)
                next_line = raw[index + width].strip() if index + width < len(raw) else ''
                if (prefix and len(prefix) < len(name) and name.startswith(prefix)
                        and plain.startswith(name) and heading_kind(candidate) == expected
                        and not re.fullmatch(r'(?:\.{2,}|[·…]{2,})\s*(?:\d+|[ivxlc]+)\s*', next_line, re.I)):
                    merged = candidate
                    result.append((starts[index], merged))
                    index += width
                    break
            if merged is not None:
                break
        if merged is None:
            result.append((starts[index], raw[index]))
            index += 1
    return result


def scope_heading_events(text):
    """Return headings outside inline images, objects and formulas."""
    text = str(text or '')
    protected = [(m.start(), m.end()) for m in re.finditer(
        r'\[\[(?:IMAGE|OBJECT):[^\]]+\]\]|\$\$[\s\S]*?\$\$|(?<!\$)\$[^$]+\$|\\\([\s\S]*?\\\)|\\\[[\s\S]*?\\\]', text)]
    events = []
    for offset, line in scope_lines(text):
        if any(start < offset < end for start, end in protected):
            continue
        tag = heading_kind(line.rstrip('\r\n'))
        if tag:
            events.append((offset, tag))
    return events


def clean_body_blocks(blocks, *, continue_on_uncertainty=False):
    """Return kept blocks, privately recorded excluded blocks and decisions."""
    if not isinstance(blocks, list):
        raise ScopeUncertain('正文块列表无效')
    kept, removed = [], []
    phase = 'before_abstract'
    in_appendix = False
    abstract_source = None
    abstract_language = None
    duplicate_abstract_count = 0
    in_toc = False
    toc_last_page = None
    toc_evidence = False
    toc_labels = set()
    toc_styles = {}
    page_suffix=r'(?:\.{2,}|[·…]{2,}|\t|\s{2,})\s*(?:\d+|[ivxlc]+)\s*$'
    front_toc = False
    research_started = False
    transition_visuals = []
    transition_fragments = []
    transition_expected = None
    tail_ocr_ambiguities = []
    notes = []
    source_order = 0

    kind = heading_kind

    def toc_entries(lines):
        complete, unfinished = [], ''
        for line in lines:
            unfinished += line.strip()
            if re.search(page_suffix, unfinished, re.I):
                complete.append(unfinished)
                unfinished = ''
        return complete, unfinished

    def append_piece(block, start, end, reason):
        original = str(block.get('text') or '')
        if block.get('type') == 'paragraph' and start == end and original:
            return
        row = copy.deepcopy(block)
        if block.get('type') == 'paragraph':
            row['text'] = original[start:end]
            if start or end != len(original):
                if (not continue_on_uncertainty and
                        (block.get('resource_ids') or block.get('resource_id') or block.get('unresolved_object')
                         or re.search(r'\[\[(?:IMAGE|OBJECT):|\$|\\\(|\\\[', original))):
                    raise ScopeUncertain('清理边界与图片或原文对象处于同一块，无法安全分离')
                if continue_on_uncertainty:
                    piece_ids = set(re.findall(r'\[\[IMAGE:([^\]]+)\]\]', row['text']))
                    if row.get('resource_ids'):
                        row['resource_ids'] = [ident for ident in row['resource_ids'] if ident in piece_ids]
                    if row.get('resource_id') and row['resource_id'] not in piece_ids:
                        row.pop('resource_id', None)
                row['source_scope'] = {'block_id': block['id'], 'start': start, 'end': end}
                row['id'] = f"{block['id']}__scope_{start}_{end}"
        if reason is None:
            kept.append(row)
        else:
            removed.append({'source_order': source_order, 'reason': reason, 'block': row})

    # Only a clearly styled first chapter plus substantial prose is a fallback
    # when no recognizable abstract exists anywhere in the native text.
    has_abstract = any(kind(line.strip()) == 'abstract' for b in blocks if isinstance(b, dict)
                       and b.get('type') == 'paragraph' for _, line in scope_lines(b.get('text') or ''))
    fallback_index = None
    if not has_abstract:
        candidates = []
        for i, b in enumerate(blocks):
            title = str(b.get('text') or '').strip()
            page = (b.get('position') or {}).get('page')
            is_first_chapter = bool(re.match(r'^(?:第\s*[一1]\s*章|chapter\s+1\b)', title, re.I))
            is_introduction = bool(continue_on_uncertainty and re.fullmatch(
                r'(?:绪论|引言|1[.、．]?\s*(?:绪论|引言))', title, re.I))
            is_design_start = bool(continue_on_uncertainty and isinstance(page, int)
                and re.fullmatch(r'(?:壹|一|1)\s*(?:选题背景|研究背景)', title)
                and any((q.get('position') or {}).get('page', page) < page
                        and any(kind(line.strip()) == 'toc' for line in str(q.get('text') or '').splitlines())
                        for q in blocks[:i] if isinstance((q.get('position') or {}).get('page'), int)))
            styled_start = b.get('heading_level') == 1 and (is_first_chapter or is_introduction)
            if (b.get('type') != 'paragraph' or b.get('source') == 'cpu_ocr'
                    or not (styled_start or is_design_start)):
                continue
            following = [str(x.get('text') or '').strip() for x in blocks[i+1:i+(21 if is_design_start else 5)]
                         if x.get('type') == 'paragraph' and not x.get('heading_level')]
            has_prose = any(re.search(r'[。！？.!?]', t) for t in following)
            short_clear_start = (continue_on_uncertainty and styled_start and has_prose and
                                 all(not re.search(page_suffix, t, re.I) for t in following))
            if has_prose and (sum(map(len, following)) >= 70 or short_clear_start):
                candidates.append(i)
        if len(candidates) == 1:fallback_index = candidates[0]
    fallback_source = None
    for source_order, block in enumerate(blocks):
        if not isinstance(block, dict) or not isinstance(block.get('id'), str):
            raise ScopeUncertain('正文块身份缺失')
        if block.get('note_label') or (block.get('position') or {}).get('part') in {'word/footnotes.xml', 'word/endnotes.xml'}:
            notes.append((source_order, block))
            continue
        if fallback_index is not None and source_order < fallback_index:
            position = block.get('position') or {}
            start_page = (blocks[fallback_index].get('position') or {}).get('page')
            if (position.get('vector_region') and isinstance(start_page, int)
                    and position.get('page') == start_page):
                transition_visuals.append({'source_block_id':block['id'], 'position':copy.deepcopy(position)})
            append_piece(block, 0, len(str(block.get('text') or '')), 'before_first_chapter')
            continue
        if fallback_index is not None and source_order == fallback_index:
            phase, fallback_source = 'body', block['id']
        if block.get('type') != 'paragraph':
            if block.get('type') == 'table' and phase in {'before_abstract', 'body'}:
                cell_lines = [line.strip() for cell in block.get('cells', [])
                              for line in str(cell.get('text') or '').splitlines() if line.strip()]
                tags = {kind(line) for line in cell_lines}
                if continue_on_uncertainty and phase == 'body' and cell_lines:
                    first = kind(cell_lines[0])
                    clearly_refs = first == 'references' and any(
                        re.match(r'^\s*(?:\[\d+\]|\d+[.、）)])\s*\S', line) for line in cell_lines[1:])
                    clearly_ack = first == 'acknowledgements' and any(
                        re.search(r'感谢|衷心|thanks?\b|grateful', line, re.I) for line in cell_lines[1:])
                    if clearly_refs or clearly_ack:
                        phase, in_appendix = first, False
                        append_piece(block, 0, 0, first)
                        continue
                if (not continue_on_uncertainty
                        and tags.intersection({'abstract','references','acknowledgements','toc'})):
                    raise ScopeUncertain('表格内出现摘要或排除章节标题，需核对表格范围')
            if in_toc:
                position = block.get('position') or {}
                box = position.get('bbox')
                if (continue_on_uncertainty and block.get('type') == 'image'
                        and isinstance(box, (list, tuple)) and len(box) == 4
                        and all(isinstance(n, (int, float)) and math.isfinite(n) for n in box)
                        and box[2] - box[0] >= 100 and 0 < box[3] - box[1] <= 2):
                    append_piece(block, 0, 0, 'toc_decorative_line')
                    continue
                if continue_on_uncertainty and block.get('type') == 'image':
                    transition_visuals.append({'source_block_id': block['id'],
                                               'position': copy.deepcopy(position)})
                    append_piece(block, 0, 0, None)
                    continue
                if continue_on_uncertainty:
                    transition_fragments.append({'source_block_id': block['id'],
                                                 'position': copy.deepcopy(position)})
                    append_piece(block, 0, 0, None)
                    continue
                raise ScopeUncertain('目录与正文对象的边界不明确')
            append_piece(block, 0, 0, phase if phase != 'body' else None)
            continue
        text = str(block.get('text') or '')
        logical_lines = scope_lines(text)
        lines = [line for _, line in logical_lines]
        line_at = dict(logical_lines)
        events = scope_heading_events(text)

        if front_toc:
            # TOC labels can themselves say "abstract". Only leave the front
            # TOC when an actual abstract is followed by research-like prose.
            is_abstract = bool(events and events[0][1] == 'abstract' and events[0][0] == 0)
            following = [str(b.get('text') or '').strip() for b in blocks[source_order+1:source_order+7]
                         if b.get('type') == 'paragraph' and str(b.get('text') or '').strip()]
            same_block = '\n'.join(line.rstrip('\r\n') for line in lines[1:]).strip()
            next_text = same_block or (following[0] if following else '')
            admin = re.match(r'^(?:声明|学术|独创|原创|授权|签名|作者|学号|指导教师|目录)', next_text)
            toc_entry = re.search(r'(?:\.{2,}|[·…]{2,}|\t|\s{2,})\s*(?:\d+|[ivxlc]+)\s*$', next_text, re.I)
            plausible = bool(next_text and not admin and kind(next_text) is None
                             and not toc_entry and re.search(r'[。！？.!?]', next_text))
            if not (is_abstract and plausible):
                append_piece(block, 0, len(text), 'before_abstract')
                continue
            front_toc = False

        if in_toc:
            stripped = [line.strip() for line in lines if line.strip()]
            if not stripped:
                append_piece(block, 0, len(text), 'confirmed_table_of_contents')
                continue
            position = block.get('position') or {}
            page = position.get('page')
            transition_vector = (position.get('vector_region') and isinstance(page, int)
                                 and isinstance(toc_last_page, int) and page >= toc_last_page)
            transition_word_fragment = (isinstance(position.get('body_index'), int)
                                        and not events
                                        and not re.search(page_suffix, text.strip(), re.I)
                                        and not block.get('resource_id') and not block.get('resource_ids'))
            if continue_on_uncertainty and (transition_vector or transition_word_fragment):
                following = [b for b in blocks[source_order+1:source_order+13]
                             if b.get('type') == 'paragraph' and str(b.get('text') or '').strip()]
                next_index = next((n for n, row in enumerate(following)
                                   if re.sub(r'\s+', '', str(row.get('text') or '')).casefold() in toc_labels), None)
                title = re.sub(r'\s+', '', str(following[next_index].get('text') or '')).casefold() if next_index is not None else ''
                between = following[:next_index] if next_index is not None else []
                prose = [str(b.get('text') or '').strip() for b in following[next_index+1:]] if next_index is not None else []
                if (title in toc_labels and len(toc_labels) >= 2 and prose and len(between) <= 6
                        and all(not re.search(page_suffix, str(row.get('text') or ''), re.I)
                                and not any(kind(line.strip()) for line in str(row.get('text') or '').splitlines())
                                and not row.get('resource_id') and not row.get('resource_ids') for row in between)
                        and (any(len(t) >= 40 for t in prose) or sum(map(len, prose)) >= 70)
                        and all(not re.search(page_suffix, t, re.I) for t in prose)):
                    record = {'source_block_id':block['id'], 'position':copy.deepcopy(position)}
                    if transition_vector:
                        transition_visuals.append(record)
                    else:
                        transition_fragments.append(record)
                    transition_expected = title
                    append_piece(block, 0, len(text), None)
                    continue
            toc_lines, unfinished = toc_entries(stripped)
            toc_rows = bool(toc_lines) and not unfinished
            if toc_rows:
                toc_evidence = True
                for line in toc_lines:
                    label=re.sub(page_suffix, '', line, flags=re.I)
                    key=re.sub(r'\s+', '', label).casefold()
                    toc_labels.add(key)
                    toc_styles.setdefault(key,set()).add((block.get('style',''),block.get('heading_level',0)))
                page = (block.get('position') or {}).get('page')
                if isinstance(page, int): toc_last_page = page
                append_piece(block, 0, len(text), 'confirmed_table_of_contents')
                continue
            first_tag = events[0][1] if events else None
            page = (block.get('position') or {}).get('page')
            candidate = re.sub(r'\s+', '', text).casefold()
            following = [b for b in blocks[source_order+1:source_order+8]
                         if b.get('type') == 'paragraph' and str(b.get('text') or '').strip()]
            follow_text=[str(b.get('text') or '').strip() for b in following[:7]]
            has_body = bool(follow_text and all(not re.search(page_suffix, t, re.I) for t in follow_text)
                            and (any(len(t)>=40 for t in follow_text) or sum(map(len, follow_text))>=70
                                 or continue_on_uncertainty and any(re.search(r'[。！？!?]', t) for t in follow_text)))
            later_page = isinstance(page,int) and isinstance(toc_last_page,int) and page>toc_last_page
            distinct_style = (page is None and block.get('heading_level') in {1,2,3}
                              and (block.get('style',''),block.get('heading_level',0)) not in toc_styles.get(candidate,set()))
            # Unstyled Word paragraphs still have sequential body positions.
            # Require actual prose punctuation as well as a matched TOC label;
            # long TOC titles alone are not sufficient evidence of a boundary.
            position = block.get('position') or {}
            unstyled_word = (page is None and isinstance(position.get('body_index'), int)
                             and first_tag == 'research_chapter'
                             and any(re.search(r'[。！？!?]', t) for t in follow_text))
            if len(toc_labels)>=2 and candidate in toc_labels and has_body and (later_page or distinct_style or unstyled_word or candidate == transition_expected):
                in_toc = False
            elif (continue_on_uncertainty and first_tag == 'research_chapter' and has_body
                  and (later_page or distinct_style or unstyled_word or block.get('heading_level'))):
                in_toc = False
            elif (continue_on_uncertainty and first_tag in {'abstract','references','acknowledgements','appendix'}
                  and len(events) == 1 and len(text.strip()) <= 60):
                append_piece(block, 0, len(text), 'confirmed_table_of_contents')
                continue
            elif continue_on_uncertainty and toc_evidence and first_tag in {None, 'research_chapter'}:
                transition_fragments.append({'source_block_id':block['id'],
                                             'position':copy.deepcopy(block.get('position') or {})})
                append_piece(block, 0, len(text), None)
                continue
            else:
                raise ScopeUncertain('目录项与真实章节边界不明确')

        cursor = 0
        for start, tag in events:
            # Appendix text is part of the reading material.  When already in
            # the body there is no scope boundary to split or disambiguate.
            if tag == 'appendix' and phase == 'body':
                current = line_at[start].strip()
                if (block.get('heading_level') or not re.match(
                        r'^附\s*录\s*[A-Za-z0-9一二三四五六七八九十]*\s*(?:中|里的|中的|所列|显示|表明|证明|数据)', current)):
                    in_appendix = True
                continue
            if tag == 'appendix' and phase in {'before_abstract', 'body'} and not block.get('heading_level'):
                normalized_line = line_at[start].strip()
                if not re.fullmatch(r'附\s*录(?:\s*[A-Za-z0-9一二三四五六七八九十]+)?\s*[:：]?', normalized_line, re.I):
                    if (continue_on_uncertainty and phase == 'body' and
                            re.match(r'^附\s*录\s*[A-Za-z0-9一二三四五六七八九十]*\s*(?:中|里的|中的|所列|显示|表明|证明|数据)', normalized_line)):
                        continue
                    raise ScopeUncertain('附录标题与正文提及无法可靠区分')
            if tag == 'toc':
                if phase not in {'before_abstract', 'body'}:
                    continue
                if in_appendix:
                    # A lone occurrence of “目录” inside an appendix need not
                    # be a table of contents.  Look for actual numbered TOC
                    # entries before entering the TOC-removal state.
                    following = [str(b.get('text') or '').strip()
                                 for b in blocks[source_order+1:source_order+9]
                                 if b.get('type') == 'paragraph']
                    candidates = [line.strip() for line in lines[1:] if line.strip()] + following
                    strong_entries = [entry for entry in candidates if (
                        re.search(r'(?:\.{2,}|[·…]{2,}|\t)\s*(?:\d+|[ivxlc]+)\s*$', entry, re.I)
                        or re.search(page_suffix, entry, re.I) and re.match(
                            r'^(?:第\s*[一二三四五六七八九十\d]+\s*章|\d+(?:\.\d+)*\s+\S|附\s*录|参考文献|致谢|chapter\s+\d+)',
                            entry, re.I))]
                    if len(strong_entries) < 2:
                        continue
                if continue_on_uncertainty and phase == 'body' and research_started and block.get('source') == 'cpu_ocr':
                    position = block.get('position') or {}
                    page = position.get('page')
                    region = position.get('image_region')
                    region = region if isinstance(region, (list, tuple)) and len(region) == 4 else None
                    nearby = blocks[source_order+1:source_order+50]
                    figure_caption = any(region and (candidate.get('position') or {}).get('page') == page
                        and candidate.get('source') != 'cpu_ocr'
                        and len(((candidate.get('position') or {}).get('bbox') or [])) == 4
                        and (candidate['position']['bbox'][2] > region[0] and candidate['position']['bbox'][0] < region[2])
                        and region[3]-12 <= candidate['position']['bbox'][1] <= region[3]+50
                        and re.match(r'^\s*(?:图|fig(?:ure)?\.?)\s*\d', str(candidate.get('text') or ''), re.I)
                        for candidate in nearby)
                    if figure_caption:
                        continue
                if continue_on_uncertainty and phase == 'before_abstract' and len(events) == 1:
                    front_toc = True
                    append_piece(block, 0, len(text), 'before_abstract')
                    cursor = len(text)
                    break
                parts = [line.strip() for line in lines if line.strip()]
                combined_toc, unfinished = toc_entries(parts[1:]) if len(parts) > 1 else ([], '')
                if (continue_on_uncertainty and phase == 'body' and len(events) == 1 and start == 0
                        and parts and kind(parts[0]) == 'toc'
                        and combined_toc and not unfinished):
                    for entry in combined_toc:
                        label = re.sub(page_suffix, '', entry, flags=re.I)
                        key = re.sub(r'\s+', '', label).casefold()
                        toc_labels.add(key)
                        toc_styles.setdefault(key, set()).add((block.get('style', ''), block.get('heading_level', 0)))
                    in_toc, toc_evidence = True, True
                    toc_last_page = (block.get('position') or {}).get('page')
                    append_piece(block, 0, len(text), 'confirmed_table_of_contents')
                    cursor = len(text)
                    break
                if phase == 'before_abstract' or len(events) != 1 or kind(text.strip()) != 'toc':
                    raise ScopeUncertain('目录与摘要或正文边界不明确')
                in_toc = True
                toc_last_page = (block.get('position') or {}).get('page')
                append_piece(block, 0, len(text), 'confirmed_table_of_contents')
                cursor = len(text)
                break
            if tag == 'abstract' and phase == 'before_abstract':
                abstract_line = line_at[start].strip()
                if (re.search(r'[:：]', abstract_line) and re.split(r'[:：]', abstract_line, maxsplit=1)[-1].strip()
                        and not block.get('heading_level')):
                    inline = re.split(r'[:：]', abstract_line, maxsplit=1)[-1].strip()
                    following = [str(b.get('text') or '').strip() for b in blocks[source_order+1:source_order+7]
                                 if b.get('type') == 'paragraph']
                    has_keywords = any(re.match(r'^(?:关键词|關鍵詞|key\s*words?)\s*[:：]', t, re.I)
                                       for t in following)
                    repeated_abstract = any(kind(t) == 'abstract' and
                        bool(re.match(r'abstract\b', t, re.I)) == bool(re.match(r'abstract\b', abstract_line, re.I))
                        for t in following)
                    original_evidence = (len(inline) >= 60 and len(re.findall(r'[。！？.!?]', inline)) >= 2
                                         and has_keywords and not repeated_abstract)
                    placeholder = re.fullmatch(r'(?:请填写|待填写|待补充|未填写|填写摘要|无|空|略)[。.!！]*', inline)
                    short_evidence = (continue_on_uncertainty and len(inline) >= 2
                                      and not placeholder and not repeated_abstract)
                    if not (original_evidence or short_evidence):
                        raise ScopeUncertain('行内摘要标记可能属于封面表单，需确认')
                if start > cursor:
                    append_piece(block, cursor, start, 'before_abstract')
                phase, abstract_source, cursor = 'body', block['id'], start
                abstract_language = 'en' if re.match(r'abstract\b', abstract_line, re.I) else 'zh'
            elif tag == 'abstract' and phase == 'body':
                current = text[start:].splitlines()[0].strip()
                # The normal Chinese + English pair is permitted; repeated
                # same-language headings need review (cover forms / TOCs).
                current_english = bool(re.match(r'abstract\b', current, re.I))
                if (abstract_language == 'en') == current_english:
                    if (continue_on_uncertainty and len(events) == 1 and start == 0
                            and kind(text.strip()) == 'abstract' and not re.search(r'[:：]', text)
                            and not block.get('resource_ids') and not block.get('resource_id')):
                        append_piece(block, 0, len(text), 'duplicate_abstract_heading')
                        duplicate_abstract_count += 1
                        cursor = len(text)
                        break
                    if continue_on_uncertainty:
                        continue
                    raise ScopeUncertain('存在多个同语种摘要标记，真实起点不明确')
            elif phase != 'before_abstract' and tag in {'references', 'appendix', 'acknowledgements'}:
                current_line = text[start:].splitlines()[0].strip()
                if (continue_on_uncertainty and phase == 'body' and tag == 'references'
                        and re.fullmatch(r'参考资料|参考书目', current_line)):
                    following = blocks[source_order+1:source_order+30]
                    before_chapter = []
                    has_research_chapter = False
                    for candidate_block in following:
                        candidate_text = str(candidate_block.get('text') or '').strip()
                        candidate_kind = kind(candidate_text)
                        if candidate_kind in {'appendix','acknowledgements','references'}:
                            break
                        if candidate_kind == 'research_chapter' and candidate_block.get('heading_level'):
                            has_research_chapter = True
                            break
                        if candidate_text:
                            before_chapter.append(candidate_text)
                    prose = bool(before_chapter and len(before_chapter[0]) >= 12
                                 and re.search(r'[。！？.!?]', before_chapter[0])
                                 and not re.match(r'^(?:\[\d+\]|\d+[.、])', before_chapter[0]))
                    if has_research_chapter and prose:
                        continue
                if start > cursor:
                    append_piece(block, cursor, start, phase if phase != 'body' else None)
                # References and acknowledgements are excluded; a later
                # appendix reopens the retained reading scope.
                phase, cursor = ('body' if tag == 'appendix' else tag), start
                in_appendix = tag == 'appendix'
            elif phase not in {'before_abstract', 'body'} and tag == 'research_chapter':
                if (continue_on_uncertainty and phase == 'appendix' and block.get('source') == 'cpu_ocr'
                        and float(block.get('ocr_confidence') or 0) < .75 and len(text.strip()) <= 12
                        and re.match(r'^\s*\d{3,}\s+\S', text)):
                    tail_ocr_ambiguities.append({'source_block_id':block['id'],
                                                 'position':copy.deepcopy(block.get('position') or {})})
                    continue
                current_line = text[start:].splitlines()[0].strip()
                if (phase == 'references' and not block.get('heading_level')
                        and re.fullmatch(r'(?:1[89]|20|21)\d{2}\s*[.．,，]\s*\d+\s*[-–~～—]\s*\d+\s*[.．]?', current_line)):
                    continue
                following_lines = [line.strip() for line in text[start:].splitlines()[1:] if line.strip()]
                following_lines += [str(b.get('text') or '').strip() for b in blocks[source_order+1:]
                                    if b.get('type') == 'paragraph' and str(b.get('text') or '').strip()]
                next_is_citation = False
                for following_line in following_lines:
                    if kind(following_line) in {'appendix', 'references', 'acknowledgements'}:
                        break
                    if re.match(r'^(?:\[\d+\]|\d+[.、）)]\s*\S)', following_line):
                        next_is_citation = True
                        break
                if (continue_on_uncertainty and block.get('heading_level') and not next_is_citation
                        and re.match(r'^(?:第\s*[零〇一二三四五六七八九十百两\d]+\s*章|chapter\s+[0-9ivxlc]+\b)', current_line, re.I)):
                    if start > cursor:
                        append_piece(block, cursor, start, phase)
                    phase, cursor, research_started = 'body', start, True
                    continue
                if continue_on_uncertainty and phase == 'references':
                    continue
                raise ScopeUncertain(f'排除章节 {phase} 内出现研究章节标题，无法确定章节归属；块 {block["id"]}，位置 {block.get("position") or {}}')
            elif phase == 'body' and tag == 'research_chapter':
                current_line = text[start:].splitlines()[0].strip()
                if re.match(r'^(?:第\s*[零〇一二三四五六七八九十百两\d]+\s*[章篇]|chapter\s+[0-9ivxlc]+\b|(?:1[.、．]?\s*)?(?:绪论|引言|前言)(?:\s|$))', current_line, re.I):
                    research_started = True
        if cursor < len(text) or not text:
            append_piece(block, cursor, len(text), phase if phase != 'body' else None)

    if abstract_source is None and fallback_source is None:
        raise ScopeUncertain('未可靠定位摘要；不能确定正文交接范围')
    if in_toc:
        if not continue_on_uncertainty:
            raise ScopeUncertain('目录结束位置不明确')
        if not transition_fragments:
            transition_fragments.append({'source_block_id': blocks[-1]['id'],
                                         'position': copy.deepcopy(blocks[-1].get('position') or {})})
    referenced = set()
    for row in kept:
        referenced.update(re.findall(r'\[\^((?:footnoteReference|endnoteReference)\d+)\]', str(row.get('text') or '')))
    present = set()
    for source_order, note in notes:
        label = note.get('note_label')
        if not isinstance(label, str):
            raise ScopeUncertain('脚注或尾注缺少来源标识')
        present.add(label)
        append_piece(note, 0, len(str(note.get('text') or '')),
                     None if label in referenced else 'note_outside_reading_scope')
    if referenced - present and not continue_on_uncertainty:
        raise ScopeUncertain('正文引用的脚注或尾注缺失，无法完整交接')
    return kept, removed, {'abstract_source_id': abstract_source, 'start_source_id': abstract_source or fallback_source,
                           'start_kind': 'abstract' if abstract_source else 'first_chapter_no_abstract', 'version': 'reading-scope/25',
                           'end_phase': phase, 'end_in_appendix': in_appendix,
                           'fallback_evidence': 'unique_styled_chapter_with_prose' if fallback_source else None,
                           'duplicate_abstract_count': duplicate_abstract_count,
                           'transition_visuals': transition_visuals,
                           'transition_fragments': transition_fragments,
                           'tail_ocr_ambiguities': tail_ocr_ambiguities,
                           'missing_note_labels': sorted(referenced - present)}
