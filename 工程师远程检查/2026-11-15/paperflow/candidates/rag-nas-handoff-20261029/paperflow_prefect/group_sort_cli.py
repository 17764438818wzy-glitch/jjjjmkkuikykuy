"""Explicit local classification entry point; does not invoke the five-stage workflow."""
import argparse
import json
from pathlib import Path
import re
import sys
import tempfile
import unicodedata

from .group_classification import COVER_FIELD, _title_proof, classify_batch, read_json
from .group_evidence import read_evidence
from .nimble_group import NimbleJudge
from .settings import PROJECT, Settings


def _read_for_sort(path):
    if Path(path).suffix.lower() not in {'.doc', '.wps', '.rtf', '.odt'}:
        return read_evidence(path)
    from .legacy import bind
    settings = Settings.load()
    bind(settings)
    from engine import convert_office
    from material_conversion import preparation_context
    scratch = PROJECT / 'runtime' / 'group-conversion-tmp'
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='sort-', dir=scratch) as temporary:
        with preparation_context(PROJECT / 'runtime' / 'group-conversion', enabled=True):
            return read_evidence(path, convert_office=lambda source:
                                 convert_office(source, Path(temporary) / 'converted'))


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
        reference = '正文 Word 开头文本'
    explicit = _explicit_cover_title(text)
    if explicit:
        title, excerpt = explicit
        exact_reference = _page_reference(pages, title, reference)
        if _title_proof(evidence, title, excerpt, labelled=True, reference=exact_reference):
            return title, exact_reference, excerpt, True
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
    parser = argparse.ArgumentParser(description='按明确指定的资料文件夹逐组分类，不运行下游流程。')
    parser.add_argument('--library', required=True, help='资料库根目录')
    group_source = parser.add_mutually_exclusive_group(required=True)
    group_source.add_argument('--group', action='append', help='一个资料组目录，可重复指定')
    group_source.add_argument('--groups-json', help='明确指定资料组目录的 JSON 字符串数组')
    parser.add_argument('--output-dir', required=True, help='原始资料库以外的交接记录目录')
    parser.add_argument('--known-paper-ids', help='已确认的资料组路径到历史论文编号的 JSON 映射')
    parser.add_argument('--verified-new-group', action='append', default=[],
                        help='经历史台账确认从未发送分类请求的资料组，可重复指定')
    parser.add_argument('--endpoint', default='http://127.0.0.1:11434/v1/systemone')
    args = parser.parse_args(argv)
    try:
        groups = args.group if args.group is not None else read_json(args.groups_json)
        if not isinstance(groups, list) or not groups or len(groups) > 10000 or not all(isinstance(p, str) for p in groups):
            raise ValueError('资料组清单必须包含 1 至 10000 个文件夹路径')
        known = read_json(args.known_paper_ids) if args.known_paper_ids else {}
        if not isinstance(known, dict): raise ValueError('已有论文编号映射必须是 JSON 对象')
        results = classify_batch(groups, args.library, args.output_dir,
                                 lambda: NimbleJudge(endpoint=args.endpoint), _read_for_sort,
                                 title_reader=_title_from_body, known_paper_ids=known,
                                 verified_new_groups=args.verified_new_group)
        for item in results:
            if 'handoff' in item or 'unreceived' in item:
                handoff = read_json(item.get('handoff') or item['unreceived'])
                item['unresolved_count'] = len(handoff['unresolved'])
        print(json.dumps(results, ensure_ascii=False, indent=2))
        return 1 if any('error' in item or 'unreceived' in item or item.get('evidence_errors') for item in results) else 0
    except Exception as exc:
        print('分类未启动：' + type(exc).__name__ + '：' + str(exc), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
