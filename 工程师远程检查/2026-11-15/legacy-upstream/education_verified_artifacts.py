"""Evidence-checked education Wiki/Graph/Tree projection for trial review.

RAGFlow's generated artifacts remain available as raw diagnostic output. This
projection is the publishable layer: it has one path per paper and never treats
a model's page-level source list or relation endpoints as edge evidence.
"""

from __future__ import annotations

import hashlib
import json
import re


TAXONOMY = {
    '调查诊断': {'现状调查与对策', '需求／问题分析', '多主体比较调查'},
    '设计与实践': {'教学／活动设计论证', '教学／活动设计与实施', '行动研究'},
    '干预评估': {'单组前后测', '有比较组的干预', '实施效果的混合方法评估'},
    '个案与叙事': {'课堂观察研究', '儿童行为观察研究', '学生／教师／学校个案', '教育叙事'},
    '课例与文本分析': {'单课例分析', '多课例分析', '课例比较'},
    '文本与课程分析': {'教材／绘本内容分析', '课程或活动资源分析', '政策／制度文本分析'},
    '理论与文献分析': {'教育思想／理论阐释', '规范的文献综述'},
}
TIERS = {'函授', '普通本科', '重点本科'}


def validate_assignments(selection: list[dict], assignments: dict[str, list[str]]) -> None:
    known = {item['stable_paper_id'] for item in selection}
    unknown = set(assignments) - known
    if unknown:
        raise ValueError('分类表含未知稳定论文编号：' + ','.join(sorted(unknown)))


def _compact(text: str) -> str:
    return re.sub(r'\s+', '', text)


def _node_id(paper_id: str, kind: str, name: str) -> str:
    return f"{paper_id}:{kind}:{hashlib.sha256(name.encode('utf-8')).hexdigest()[:12]}"


def is_questionnaire_heading(title: str) -> bool:
    return bool(re.match(r'^\d{1,2}[.、]', title)
                and re.search(r'[（(]\s*[）)]\s*$', title))


def audit_source_tree(ingest_text: str, chunks: list[dict]) -> dict:
    """Keep the handoff's heading order and expose ambiguous source structure."""
    body = {chunk['id']: chunk['content'] for chunk in chunks
            if chunk.get('available') and 'role=body' in (chunk.get('important_keywords') or [])}
    headings = []
    issues = []
    for line in ingest_text.splitlines():
        match = re.match(r'^(#{1,6})\s+(.+?)\s*$', line)
        if not match:
            continue
        title = match[2]
        if title in ('图表/表格写作参考结构', '图片检索投影'):
            break
        ids = sorted(chunk_id for chunk_id, content in body.items()
                     if _compact(title) in _compact(content))
        headings.append({'order': len(headings), 'level': len(match[1]),
                         'title': title, 'source_chunk_ids': ids})
        if not ids and '章节标题未能定位正文切片' not in issues:
            issues.append('章节标题未能定位正文切片')
        if (is_questionnaire_heading(title)
                and '疑似问卷题项被标为章节' not in issues):
            issues.append('疑似问卷题项被标为章节')
    if not headings:
        issues.append('正文未发现章节标题')
    first_chapter = next((i for i, heading in enumerate(headings)
                          if re.match(r'^(?:第[一二三四五六七八九十\d]+章|1[.、])', heading['title'])), None)
    if first_chapter is not None and any(
            heading['title'].replace(' ', '') == '参考文献' for heading in headings[:first_chapter]):
        issues.append('参考文献位于正文第一章之前')
    return {'headings': headings, 'issues': issues, 'order_basis': 'ingest.md'}


def project_document(item: dict, doc: dict, chunks: list[dict], structure: dict,
                     taxonomy: tuple[str, str] | tuple[str, str, str],
                     *, reviewed_step_quotes: dict[str, str] | None = None,
                     direct_tree: dict | None = None) -> dict:
    """Build one auditable projection; reject incomplete or inconsistent input."""
    paper_id = item['stable_paper_id']
    meta = doc.get('meta_fields') or {}
    if not re.fullmatch(r'paper_[0-9a-f]{24}', paper_id) or meta.get('education_paper_id') != paper_id:
        raise ValueError('稳定论文身份与远端元数据不一致')
    if doc.get('run') != 'DONE' or meta.get('education_admission') != '入库':
        raise ValueError('论文未完成解析或未通过准入')
    tier = meta.get('artifact_example_tier')
    if tier not in (TIERS | {''}):
        raise ValueError('范例候选档位无效')
    tier = tier or '待核实'
    if len(taxonomy) not in (2, 3):
        raise ValueError('四级分类需要 L1、L2 和可选的人工核对 L3')
    l1, l2 = taxonomy[:2]
    if l2 not in TAXONOMY.get(l1, set()):
        raise ValueError('四级分类未在受控词表中')
    if any(chunk.get('document_id') not in (None, doc['id']) for chunk in chunks):
        raise ValueError('正文切片来源文档与远端文档不一致')
    body = {chunk['id']: chunk['content'] for chunk in chunks
            if chunk.get('available') and 'role=body' in (chunk.get('important_keywords') or [])}
    if not body:
        raise ValueError('没有可用正文切片')
    if direct_tree is not None:
        if (direct_tree.get('schema') != 'education-direct-tree/1.0'
                or direct_tree.get('paper_id') != paper_id
                or direct_tree.get('source_doc_id') != doc['id']
                or direct_tree.get('issues') or not direct_tree.get('nodes')):
            raise ValueError('Tree 身份、结构或来源检查未通过')
        nodes = direct_tree['nodes']
        if len({node.get('id') for node in nodes}) != len(nodes):
            raise ValueError('Tree 章节节点 ID 重复')
        entities = [{'id': node['id'], 'name': node['title'], 'type': 'title',
                     'order': node['order'], 'level': node['level'],
                     'source_doc_id': doc['id'],
                     'source_chunk_ids': node['source_chunk_ids']} for node in nodes]
        tree_projection = {'schema': direct_tree['schema'], 'entities': entities,
                           'relations': direct_tree.get('relations') or []}
    else:
        page_index = next((template for template in structure.get('templates', [])
                           if template.get('kind') == 'page_index'), None)
        if page_index is None or not page_index.get('entities'):
            raise ValueError('Tree / PageIndex 缺失')
        tree_projection = {'entities': page_index['entities'],
                           'relations': page_index.get('relations') or []}
    for entity in tree_projection['entities']:
        ids = set(entity.get('source_chunk_ids') or [])
        if not ids or not ids <= body.keys():
            raise ValueError('Tree 含不可用正文切片来源')
    try:
        steps = json.loads(meta.get('education_research_steps') or '[]')
    except (TypeError, ValueError) as exc:
        raise ValueError('实际步骤不是有效 JSON') from exc
    actual = [step for step in steps if step.get('status') == '实际执行']
    if not actual:
        raise ValueError('入库论文没有已执行步骤')
    if len({step.get('name') for step in actual}) != len(actual):
        raise ValueError('实际步骤名称重复，需核对循环研究或模型重复抽取')
    reviewed_step_quotes = reviewed_step_quotes or {}
    if set(reviewed_step_quotes) - {step['name'] for step in actual}:
        raise ValueError('人工引文指向不存在的实际研究步骤')
    resolved = []
    for step in actual:
        evidence_quote = reviewed_step_quotes.get(step['name'], step.get('quote') or '')
        quote = _compact(evidence_quote)
        if not quote:
            raise ValueError('实际步骤缺少正文引文')
        ids = [chunk_id for chunk_id, content in body.items() if quote in _compact(content)]
        if not ids:
            raise ValueError('实际步骤引文无法在正文切片定位')
        resolved.append((step, ids, evidence_quote))
    observed_path = ' → '.join(step['name'] for step, _, _ in resolved)
    path = taxonomy[2].strip() if len(taxonomy) == 3 else observed_path
    if not path or len(path) > 150:
        raise ValueError('归一研究路径为空或过长')
    paper_type = str(meta.get('artifact_paper_type') or '').strip()
    if not paper_type or len(paper_type) > 120:
        raise ValueError('具体论文类型缺失或过长')
    nodes = {}
    edges = {}

    def node(kind, name, source_ids):
        key = _node_id(paper_id, kind, name)
        row = nodes.setdefault(key, {'id': key, 'type': kind, 'name': name,
                                     'paper_id': paper_id, 'source_doc_id': doc['id'],
                                     'source_chunk_ids': []})
        row['source_chunk_ids'] = sorted(set(row['source_chunk_ids']) | set(source_ids))
        return key

    def edge(kind, source, target, source_ids):
        key = (kind, source, target)
        row = edges.setdefault(key, {'type': kind, 'from': source, 'to': target,
                                     'paper_id': paper_id, 'source_doc_id': doc['id'],
                                     'source_chunk_ids': []})
        row['source_chunk_ids'] = sorted(set(row['source_chunk_ids']) | set(source_ids))

    all_ids = sorted({chunk_id for _, ids, _ in resolved for chunk_id in ids})
    section_numbers = sorted({keyword.split('=', 1)[1]
                              for chunk in chunks if chunk['id'] in all_ids
                              for keyword in chunk.get('important_keywords') or []
                              if keyword.startswith('section_no=') and keyword.split('=', 1)[1]})
    type_id = node('paper_type', paper_type, all_ids)
    path_id = node('research_path', path, all_ids)
    edge('uses_path', type_id, path_id, all_ids)
    for step, ids, evidence_quote in resolved:
        step_id = node('research_step', step['name'], ids)
        edge('has_step', path_id, step_id, ids)
        quote = _compact(evidence_quote)
        for field, kind, relation in (
            ('materials', 'material', 'uses_material'),
            ('analysis_elements', 'analysis_element', 'measures'),
        ):
            for value in step.get(field) or []:
                value = str(value).strip()
                # A gate-validated step does not automatically prove every
                # material/indicator supplied by the model. Require the same
                # located quotation to mention the value explicitly.
                if len(_compact(value)) < 2 or _compact(value) not in quote:
                    continue
                target_id = node(kind, value, ids)
                edge(relation, step_id, target_id, ids)
    for row in list(nodes.values()) + list(edges.values()):
        if not set(row['source_chunk_ids']) <= body.keys():
            raise ValueError('投影包含不可用正文来源')
    return {
        'paper_id': paper_id, 'document_id': item['document_id'],
        'source_doc_id': doc['id'], 'title': item['title'], 'tier': tier,
        'taxonomy_version': 'education-wiki/1.0',
        'wiki': {'l1': l1, 'l2': l2, 'l3': path, 'l4': paper_id,
                 'observed_steps': [step['name'] for step, _, _ in resolved],
                 'section_numbers': section_numbers,
                 'source_chunk_ids': all_ids},
        'graph': {'nodes': list(nodes.values()), 'edges': list(edges.values())},
        'tree': tree_projection,
        'excluded_unquoted_materials': sum(
            len(step.get(field) or []) - sum(
                len(_compact(str(value))) >= 2 and _compact(str(value)) in _compact(evidence_quote)
                for value in step.get(field) or [])
            for step, _, evidence_quote in resolved for field in ('materials', 'analysis_elements')),
    }


def render_wiki(projections: list[dict]) -> str:
    """Render an evidence-only four-level navigation Markdown document."""
    seen = set()
    lines = ['# 教育论文四级研究范例导航', '',
             '范例档位均为候选。旧论文的样本、访谈、观察、教学实施、前后测、数据和结论必须针对新研究对象重新采集与验证。', '']
    previous = None
    for row in sorted(projections, key=lambda x: (x['wiki']['l1'], x['wiki']['l2'], x['wiki']['l3'], x['paper_id'])):
        if row['paper_id'] in seen:
            raise ValueError('重复稳定论文编号')
        seen.add(row['paper_id'])
        wiki = row['wiki']
        group = (wiki['l1'], wiki['l2'], wiki['l3'])
        if previous is None or previous[0] != group[0]:
            lines.extend([f"## L1 {group[0]}", ''])
        if previous is None or previous[:2] != group[:2]:
            lines.extend([f"### L2 {group[1]}", ''])
        if previous != group:
            lines.extend([f"#### L3 {group[2]}", ''])
        chapters = ('；章节：' + '、'.join(wiki['section_numbers'])) if wiki['section_numbers'] else ''
        lines.append(f"- **L4**《{row['title']}》；`{row['paper_id']}`；档位候选：{row['tier']}；"
                     f"RAGFlow 文档 `{row['source_doc_id']}`{chapters}；正文切片："
                     + '、'.join(f'`{chunk_id}`' for chunk_id in wiki['source_chunk_ids']) + '。')
        lines.append('')
        previous = group
    return '\n'.join(lines).rstrip() + '\n'
