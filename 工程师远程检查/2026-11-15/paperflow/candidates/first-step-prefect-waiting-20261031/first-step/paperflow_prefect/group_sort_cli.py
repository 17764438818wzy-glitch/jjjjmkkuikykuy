"""Explicit local classification entry point; does not invoke the five-stage workflow."""
import argparse
import json
from pathlib import Path
import re
import shutil
import sys
import tempfile
import unicodedata

from .group_classification import COVER_FIELD, TEMPLATE_NOTE, MAX_GROUPS, _title_proof, classify_batch, read_json
from .group_evidence import detect_format, read_evidence
from .nimble_group import NimbleJudge
from .settings import PROJECT, Settings, first_step_shared_data


def _read_for_sort(path, *, work_root=None, task_id=None, stop_requested=None):
    kind, basis = detect_format(path)
    if kind not in {'doc', 'wps', 'rtf', 'odt'}:
        return read_evidence(path, detected=(kind, basis))
    from .legacy import bind
    settings = Settings.load()
    bind(settings)
    from .first_step_conversion import FirstStepConverter
    scratch = Path(work_root) / 'tmp' if work_root is not None else PROJECT / 'runtime' / 'group-conversion-tmp'
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='sort-', dir=scratch) as temporary:
        source = Path(path)
        expected = '.' + kind
        if source.suffix.lower() != expected and not (kind == 'doc' and source.suffix.lower() == '.wps'):
            # The converter may use the suffix; rename only a temporary copy.
            source = Path(temporary) / ('source' + expected)
            shutil.copyfile(path, source)
        conversion_root = Path(work_root) / 'office' if work_root is not None else PROJECT / 'runtime' / 'group-conversion'
        channel_root = first_step_shared_data(settings) / 'control-locks' / 'first-step-office'
        converter = FirstStepConverter(conversion_root, channel_root, task_id=task_id,
                                       offline=task_id is None and work_root is None,
                                       stop_requested=stop_requested)
        return read_evidence(path, detected=(kind, basis), convert_office=lambda _:
                             converter.convert(source, Path(temporary) / 'converted'))


def _obvious_non_title(value):
    compact = re.sub(r'\s+', '', unicodedata.normalize('NFKC', value))
    date = r'(?:[零〇一二三四五六七八九十]{2,8}|(?:19|20)\d{2})年(?:[零〇一二三四五六七八九十]{1,3}|\d{1,2})月(?:[零〇一二三四五六七八九十\d]{1,3}日)?'
    cover = r'(?:本科|专科|硕士|博士)(?:生)?(?:毕业|学位)(?:论文|设计|设计论文|设计说明书)(?:\((?:初稿|终稿|定稿|修改稿)\))?'
    return bool(re.fullmatch(date, compact) or re.fullmatch(cover, compact)
                or re.match(r'^(?:学生姓名|职称或学位|学号|指导教师|完成时间|完成日期)[:：]?.+', compact)
                or compact.endswith('人才培养项目'))


def _explicit_cover_title(text):
    pattern = (r'(?:^|\n)[ \t]*(?:论文)?题[ \t]*目[ \t]*[:：]?[ \t]*(?:\r?\n)?'
               r'(?P<title>.*?)(?=\r?\n[ \t]*' + COVER_FIELD + r'(?:[ \t]*[:：]|[ \t]*\r?\n|$))')
    match = re.search(pattern, text, re.S | re.I)
    if not match:
        return None
    excerpt = match.group('title').strip()
    lines = [line.strip() for line in excerpt.splitlines() if line.strip()]
    first_note = next((i for i, line in enumerate(lines) if re.fullmatch(TEMPLATE_NOTE, line)), None)
    if first_note is not None:
        if any(not re.fullmatch(TEMPLATE_NOTE, line) for line in lines[first_note:]): return None
        lines = lines[:first_note]
        excerpt = '\n'.join(lines)
    if not lines or len(lines) > 3:
        return None
    title = ''
    for line in lines:
        separator = ' ' if title and re.search(r'[A-Za-z0-9]$', title) and re.match(r'[A-Za-z0-9]', line) else ''
        title += separator + line
    if len(title) > 160 or _obvious_non_title(title):
        return None
    return title, excerpt


def _page_reference(pages, title, fallback):
    wanted = ''.join(c for c in unicodedata.normalize('NFKC', title) if not c.isspace())
    for page in pages:
        normalized = ''.join(c for c in unicodedata.normalize('NFKC', page.get('text', '')) if not c.isspace())
        if wanted in normalized:
            return f"正文 PDF 第{page['page']}页文字"
    return fallback


def _pdf_cover_title_lines(evidence):
    """Keep a wrapped first-page cover title as an unconfirmed candidate."""
    if evidence.get('format') != 'pdf':
        return None
    first = next((page for page in evidence.get('pages', []) if page.get('page') == 1), None)
    if not first:
        return None
    field = re.compile(r'^(?:姓\s*名|学生姓名|作者姓名|学\s*号|指导(?:老师|教师)|'
                       r'院\s*系|学\s*院|专\s*业|班\s*级)(?:\s*[:：].*)?\s*$')
    lines = []
    for original in first.get('text', '').splitlines():
        line = original.strip()
        if not line:
            continue
        if field.fullmatch(line):
            if not 2 <= len(lines) <= 3:
                return None
            break
        if not lines and _obvious_non_title(line):
            continue
        if (len(lines) >= 3 or _obvious_non_title(line)
                or re.match(r'^(?:摘\s*要|abstract|目\s*录|contents)\s*$', line, re.I)
                or re.match(r'^(?:论文)?题\s*目\s*[:：]', line)):
            return None
        lines.append(line)
    else:
        return None
    title = ''
    for line in lines:
        separator = ' ' if title and re.search(r'[A-Za-z0-9]$', title) and re.match(r'[A-Za-z0-9]', line) else ''
        title += separator + line
    if len(title) < 6 or len(title) > 160 or _obvious_non_title(title):
        return None
    return title, '\n'.join(lines)


def _title_from_body(record, evidence):
    from .legacy import bind
    bind(Settings.load())
    from content_rules import detect_title
    pages = []
    if evidence.get('format') == 'pdf':
        pages = [page for page in evidence.get('pages', []) if page['page'] <= 3]
        text = '\n'.join(page.get('text', '') for page in pages)
        reference = '正文 PDF 前三页文本'
    else:
        text = evidence.get('text', '').split('\n...\n', 1)[0]
        reference = '正文文本开头' if evidence.get('format') == 'text' else '正文 Word 开头文本'
    text = re.split(r'(?im)^\s*(?:摘\s*要|abstract|目\s*录|contents)\s*$', text, maxsplit=1)[0]
    cover = evidence.get('cover_table_title')
    if cover and not _obvious_non_title(cover['title']):
        reference = '正文 Word 封面表格单元格'
        if _title_proof(evidence, cover['title'], cover['excerpt'], labelled=True, reference=reference):
            return cover['title'], reference, cover['excerpt'], True
    explicit = _explicit_cover_title(text)
    if explicit:
        title, excerpt = explicit
        exact_reference = _page_reference(pages, title, reference)
        if _title_proof(evidence, title, excerpt, labelled=True, reference=exact_reference):
            return title, exact_reference, excerpt, True
    wrapped_cover = _pdf_cover_title_lines(evidence)
    if wrapped_cover:
        title, excerpt = wrapped_cover
        exact_reference = '正文 PDF 第1页文字'
        if _title_proof(evidence, title, excerpt, labelled=False, reference=exact_reference):
            return title, exact_reference, excerpt, False
    # Empty filename prevents the native helper from using a filename as a title.
    # Cover titles are often split by empty Word paragraphs. The native
    # detector can join the title once only empty lines are removed.
    compact_lines = '\n'.join(line for line in text.splitlines()
                              if line.strip() and not _obvious_non_title(line))
    title = detect_title(compact_lines, '')
    if not title:
        return '', reference, '', False
    compact, offsets = [], []
    for index, character in enumerate(text):
        for normalized in unicodedata.normalize('NFKC', character):
            if not normalized.isspace():
                compact.append(normalized)
                offsets.append(index)
    wanted = ''.join(c for c in unicodedata.normalize('NFKC', title) if not c.isspace())
    start = ''.join(compact).find(wanted)
    if start < 0:
        return '', reference, '', False
    first, last = offsets[start], offsets[start + len(wanted) - 1] + 1
    labelled = bool(re.search(r'题\s*目\s*[：:]?\s*$', text[max(0, first - 80):first]))
    if (not labelled and len(wanted) >= 16 and len(wanted) % 2 == 0
            and wanted[:len(wanted) // 2] == wanted[len(wanted) // 2:]):
        wanted = wanted[:len(wanted) // 2]
        last = offsets[start + len(wanted) - 1] + 1
        title = text[first:last].strip()
    if _obvious_non_title(title):
        return '', reference, '', False
    excerpt = text[first:last]
    reference = _page_reference(pages, title, reference)
    return title, reference, excerpt, labelled


def main(argv=None):
    print('独立分类入口已移除，请使用任务页面的开始、暂停和删除。', file=sys.stderr)
    return 2


if __name__ == '__main__':
    raise SystemExit(main())
