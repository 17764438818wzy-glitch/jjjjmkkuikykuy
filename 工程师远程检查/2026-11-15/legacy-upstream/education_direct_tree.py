"""Build an education paper's chapter tree from its actual Markdown headings."""

from __future__ import annotations

import copy
import hashlib
import re


HEADING = re.compile(r'^(#{1,6})[ \t\u3000]+(.+?)[ \t\u3000]*$')
SECTION_LINK = re.compile(r'资料关联[（(]程序生成[）)][：:][ \t\u3000]*(section_[0-9a-fA-F]+)')
PAPER_ID = re.compile(r'paper_[0-9a-f]{24}\Z')
ASSET_HEADINGS = {'图表/表格写作参考结构', '图片检索投影'}
QUESTION_HEADING = re.compile(
    r'^\d{1,3}[.、．]\s*.+[（(](?:单选题|多选题|判断题|填空题|问答题)[）)]\s*$'
)
APPENDIX = re.compile(r'^附\s*[录件](?:\s*[A-Za-zＡ-Ｚ\d一二三四五六七八九十]+)?(?:\s|[：:]|$)')
MATERIAL_ITEM = re.compile(r'^(?:\d{1,3}[.、．](?!\s*\d)|[A-ZＡ-Ｚ][.、．])')
QUESTION_WITHOUT_SEPARATOR = re.compile(r'^\d{1,3}(?=[你您我])')
QUESTIONNAIRE_FORM_LINES = {'亲爱的同学', '尊敬的老师', '您好', '姓名', '指导老师'}
CHAPTER = re.compile(r'^第[\d一二三四五六七八九十百]+[章节]')
MATERIAL_LIST = re.compile(r'(?:阅读材料|教材篇目|古诗词|课文|篇目|材料)(?:清单|目录|全部)$')


def _compact(value: str) -> str:
    return re.sub(r'\s+', '', value)


def extract_tree(ingest_text: str, paper_id: str, source_doc_id: str) -> dict:
    """Preserve original heading text, order, depth, and section IDs without a model."""
    if not PAPER_ID.fullmatch(paper_id):
        raise ValueError('Tree 需要有效的稳定 paper_id')
    if not source_doc_id:
        raise ValueError('Tree 需要来源文档 ID')
    lines = ingest_text.splitlines()
    nodes = []
    relations = []
    stack = []
    used_ids = set()
    in_appendix = False
    questionnaire = False
    material_list = False
    for line_number, line in enumerate(lines):
        match = HEADING.match(line)
        title = match.group(2).strip() if match else line.strip()
        if match and title in ASSET_HEADINGS:
            break
        # Some exports keep appendix labels and material titles as plain text.
        # They establish context, but only original Markdown headings become nodes.
        if APPENDIX.match(title):
            in_appendix = True
            questionnaire = bool(re.search(r'(?:问卷|量表)', title))
            material_list = bool(MATERIAL_LIST.search(title))
        elif match and (CHAPTER.match(title) or _compact(title) in {'参考文献', '致谢', '结论'}):
            in_appendix = False
            questionnaire = False
            material_list = False
        elif in_appendix and len(title) <= 100 and re.search(r'(?:问卷|量表)[）)]?$', title):
            questionnaire = True
        elif in_appendix and len(title) <= 100 and MATERIAL_LIST.search(title):
            material_list = True
        if ((questionnaire or material_list) and MATERIAL_ITEM.match(title)
                or questionnaire and QUESTION_WITHOUT_SEPARATOR.match(title)):
            continue
        if questionnaire and _compact(title).rstrip('：:！!') in QUESTIONNAIRE_FORM_LINES:
            continue
        if not match:
            continue
        if QUESTION_HEADING.match(title):
            continue
        level = len(match.group(1))
        section_id = ''
        for following in lines[line_number + 1:line_number + 6]:
            link = SECTION_LINK.search(following.strip())
            if link:
                section_id = link.group(1)
                break
            if following.strip() and not following.strip().startswith('资料关联'):
                break
        node_id = f'{paper_id}:{section_id or "heading_" + str(line_number + 1)}'
        if node_id in used_ids:
            raise ValueError('Tree 中存在重复章节 ID：' + node_id)
        used_ids.add(node_id)
        while stack and stack[-1]['level'] >= level:
            stack.pop()
        parent_id = stack[-1]['id'] if stack else None
        node = {'id': node_id, 'order': len(nodes), 'level': level,
                'title': title, 'section_id': section_id, 'parent_id': parent_id,
                'source_chunk_ids': []}
        nodes.append(node)
        if parent_id:
            relations.append({'type': 'include', 'from': parent_id, 'to': node_id})
        stack.append(node)
    return {'schema': 'education-direct-tree/1.0', 'paper_id': paper_id,
            'source_doc_id': source_doc_id,
            'ingest_sha256': hashlib.sha256(ingest_text.encode('utf-8')).hexdigest(),
            'nodes': nodes, 'relations': relations, 'issues': [],
            'unlinked_headings': len(nodes), 'order_basis': 'ingest.md'}


def bind_tree_chunks(tree: dict, chunks: list[dict]) -> dict:
    """Attach existing body chunk IDs to headings; never manufacture evidence."""
    result = copy.deepcopy(tree)
    body = [chunk for chunk in chunks if chunk.get('available') is True
            and 'role=body' in (chunk.get('important_keywords') or [])
            and chunk.get('id')]
    if any(chunk.get('document_id') not in (None, tree['source_doc_id']) for chunk in body):
        raise ValueError('Tree 正文切片来自其他文档')
    by_section = {}
    heading_lines = []
    for index, chunk in enumerate(body):
        for keyword in chunk.get('important_keywords') or []:
            if keyword.startswith('section_id='):
                by_section.setdefault(keyword.split('=', 1)[1], []).append((index, chunk['id']))
        for line in (chunk.get('content') or '').splitlines():
            match = HEADING.match(line)
            if match:
                heading_lines.append((index, len(match.group(1)), _compact(match.group(2)), chunk['id']))
    cursor = 0
    unlinked = 0
    for node in result['nodes']:
        direct = by_section.get(node['section_id'], []) if node['section_id'] else []
        if direct:
            node['source_chunk_ids'] = list(dict.fromkeys(chunk_id for _, chunk_id in direct))
            cursor = max(cursor, direct[0][0])
            continue
        matches = [(index, chunk_id) for index, level, title, chunk_id in heading_lines
                   if index >= cursor and level == node['level']
                   and title == _compact(node['title'])]
        if matches:
            cursor, chunk_id = matches[0]
            node['source_chunk_ids'] = [chunk_id]
        else:
            unlinked += 1
    result['unlinked_headings'] = unlinked
    by_id = {node['id']: node for node in result['nodes']}
    for relation in result['relations']:
        relation['source_doc_id'] = tree['source_doc_id']
        relation['source_chunk_ids'] = list(by_id[relation['to']]['source_chunk_ids'])
    if unlinked:
        result['issues'].append('章节标题未能定位正文切片')
    if not result['nodes']:
        result['issues'].append('正文未发现章节标题')
    return result
