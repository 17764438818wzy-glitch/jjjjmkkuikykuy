"""Conservative document-wide structure rules shared by readable and machine exports."""
import re
import unicodedata


def heading_text(text):
    return re.sub(r'^\s*#{1,6}\s+', '', text).strip()


def normalized(text):
    return unicodedata.normalize('NFKC', heading_text(text))


def chinese_number(text):
    if text.isdigit():
        return int(text)
    digits = dict(zip('零〇一二三四五六七八九两', [0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 2]))
    total = current = 0
    for char in text:
        if char in digits:
            current = digits[char]
        elif char in '十百':
            total += (current or 1) * (10 if char == '十' else 100)
            current = 0
        else:
            return None
    return total + current


def heading_info(block):
    """Numbered headings outrank Word styles, without treating dates/lists as headings."""
    if block.get('type') != 'paragraph':
        return 0, None
    raw = block.get('text', '')
    text = normalized(raw)
    explicit = re.match(r'^\s*(#{1,6})\s+\S', raw)
    style = block.get('heading_level', 0)
    style = min(style, 6) if type(style) is int and 1 <= style <= 6 else 0
    punctuation_text = re.sub(r'[？！?!]$', '', text) if style or explicit else text
    if not text or len(text) > 100 or '\n' in text or re.search(r'[。；;？！?!]', punctuation_text):
        return 0, None
    compact = re.sub(r'\s+', '', text).lower()
    if compact in {'摘要', 'abstract', '结论', '结语', '参考文献', 'references', 'bibliography'}:
        return 1, None
    chapter = re.match(r'^第([零〇一二三四五六七八九十百两\d]+)[章篇]\s*\S', text)
    english = re.match(r'^chapter\s+(\d{1,2})\b', text, re.I)
    if chapter or english:
        number = chinese_number((chapter or english)[1])
        return 1, (number,) if number is not None else None
    numeric = re.match(r'^(\d{1,2}(?:\.\d{1,2}){0,4})(?:\s+|(?=[\u4e00-\u9fffA-Za-z]))(\S.*)$', text)
    punctuated = re.match(r'^(\d{1,2})[.、]\s*([^\d\s.].*)$', text)
    # Real numbered list items are not a section merely because they start with a number.
    listed = block.get('list', {}).get('format') in {'decimal', 'bullet', 'lowerLetter', 'upperLetter'}
    # Financial amounts, percentages and counts are ordinary content. Unspaced
    # decimal section titles remain supported, but not number+measurement units.
    quantity = re.match(r'^\d+(?:\.\d+)?(?:[%％]|[亿万千百](?:美元|港元|元)|美元|港元|元(?=$|\s))', text)
    if quantity and not style and not explicit:
        return 0, None
    # Questionnaire items often look like unstyled numbered headings, but an
    # empty answer slot identifies a form prompt rather than a chapter.
    if (re.match(r'^\d{1,2}[.、]', text)
            and re.search(r'\(\s*\)\s*$', text)):
        return 0, None
    # The length guard above uses stripped text. Word alignment padding can
    # still leave thousands of spaces in raw; searching that suffix causes
    # cubic backtracking and holds the GIL, freezing the desktop event loop.
    # Keep interior spacing for data-row detection without changing the source.
    if not style and not explicit and re.search(r'(?:\t| {2,})\s*[+-]?\d+(?:\.\d+)?%?\s*$', raw.strip()):
        return 0, None
    if punctuated and (not listed or style or explicit):
        return 1, (int(punctuated[1]),)
    if numeric and (not listed or style or explicit):
        numbers = tuple(int(v) for v in numeric[1].split('.'))
        return len(numbers), numbers
    chinese = re.match(r'^([一二三四五六七八九十百]+)、\s*\S', text)
    if chinese and (style or explicit or not listed):
        return 1, (chinese_number(chinese[1]),)
    if listed and not style and not explicit:
        return 0, None
    return (len(explicit[1]) if explicit else style), None


def heading_level(block):
    return heading_info(block)[0]


def toc_title(text):
    return re.sub(r'\s+', '', normalized(text)).lower() in {
        '目录', 'contents', 'tableofcontents', '图目录', '表目录', '图表目录',
        'listoffigures', 'listoftables',
    }


def toc_label(text, loose=False):
    """Page numbers need a heading-like label, never just a line ending in a number."""
    text = normalized(text)
    separator = r'(?:\t+|[.·…]{2,}| {2,})' if not loose else r'(?:\t+|[.·…]{2,}| +)'
    match = re.fullmatch(r'(.+?)' + separator + r'\s*(?:\d{1,3}|[ivxlcIVXLC]{1,8})', text)
    if not match:
        return None
    label = match[1].strip()
    # Explicit leaders inside a contents region also support unnumbered titles.
    leader = bool(re.search(r'[.·…]{2,}\s*(?:\d{1,3}|[ivxlcIVXLC]+)$', text))
    if loose and leader and len(label) <= 100 and not re.search(r'[。；;？！?!]', label):
        return label
    level, _ = heading_info({'type': 'paragraph', 'text': label})
    valid = bool(level or re.match(r'^(?:图|表|Figure|Table)\s*\d', label, re.I)
                 or re.sub(r'\s+', '', label).lower() in {'致谢', '附录', 'appendix', 'acknowledgements'})
    return label if valid else None


def analyze_blocks(blocks):
    """Keep doubtful text. Only confirmed contents rows are excluded from delivery."""
    analysis = []
    for block in blocks:
        level, number = heading_info(block)
        analysis.append({'heading_level': level, 'number': number, 'role': 'body'})
    def key(text):return re.sub(r'\s+', '', normalized(text))
    body_titles = {key(b.get('text', '')) for b, a in zip(blocks, analysis)
                   if a['heading_level'] and not toc_label(b.get('text', ''))}
    active = False
    candidates = []
    for index, block in enumerate(blocks):
        text = block.get('text', '')
        if block.get('type') != 'paragraph' or '[[IMAGE:' in text:
            active = False
            candidates = []
            continue
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        style = re.sub(r'[\s_-]', '', block.get('style', '')).lower()
        styled = bool(re.fullmatch(r'(?:toc|contents)\d+', style)) and len(text) <= 160
        listed = block.get('list', {}).get('format') in {'decimal','bullet','lowerLetter','upperLetter'}
        if listed and not styled and not block.get('heading_level'):
            active = False
            candidates = []
            continue
        header = bool(lines and toc_title(lines[0]))
        page_lines = lines[1:] if header else lines
        labels = [toc_label(line, loose=active or header) for line in page_lines]
        entries = bool(labels) and all(labels)
        strong = entries and (all(key(label) in body_titles for label in labels)
                              or all(re.search(r'[.·…]{2,}\s*(?:\d{1,3}|[ivxlcIVXLC]+)$', normalized(line)) for line in page_lines))
        if styled or (header and (not page_lines or entries)) or (active and entries):
            analysis[index].update(role='table_of_contents', heading_level=0, number=None)
            active = True
            candidates = []
        elif not lines:
            if active:
                analysis[index].update(role='table_of_contents', heading_level=0, number=None)
        elif entries and strong:
            candidates.append(index)
            if len(candidates) >= 2 or len(lines) >= 2:
                for candidate in candidates:
                    analysis[candidate].update(role='table_of_contents', heading_level=0, number=None)
                active = True
                candidates = []
        else:
            active = False
            candidates = []
    return analysis


def body_blocks(blocks):
    return [b for b, info in zip(blocks, analyze_blocks(blocks)) if info['role'] != 'table_of_contents']
