"""Build a small, source-linked method graph from validated education annotations."""

from __future__ import annotations

import hashlib
import json
import re


METHOD_CUES = {
    '文献研究法': ('文献', '资料检索', '文献梳理'),
    '问卷调查法': ('问卷',),
    '访谈法': ('访谈',),
    '课堂观察法': ('课堂观察', '课堂记录', '观察课堂'),
    '观察法': ('观察', '行为记录'),
    '教学设计法': ('教学设计', '教案', '方案设计', '活动设计'),
    '行动研究法': ('行动研究', '轮教学', '轮次反思'),
    '实验法': ('实验', '干预', '对照组'),
    '单一被试实验法': ('单一被试', '单被试', '多基线', '跨行为倒返'),
    '测验法': ('测验', '试题测试', '学习测试'),
    '前后测比较法': ('前后测', '前测', '后测'),
    '案例研究法': ('案例', '个案'),
    '课例研究法': ('课例', '教学实录'),
    '逻辑分析法': ('逻辑分析', '归纳推理', '演绎推理', '归纳演绎'),
    '统计分析法': ('统计', '数据分析', '频数分析'),
    '文本分析法': ('文本分析', '文本编码', '教材分析'),
}
ALIASES = {
    '问卷法': '问卷调查法', '问卷调查': '问卷调查法',
    '访谈': '访谈法', '课堂观察': '课堂观察法',
    '教学设计': '教学设计法', '行动研究': '行动研究法',
    '前后测': '前后测比较法', '案例研究': '案例研究法',
    '描述统计': '统计分析法', '文献分析法': '文献研究法',
    '个案研究法': '案例研究法', '案例分析法': '案例研究法',
}
PAPER_ID = re.compile(r'paper_[0-9a-f]{24}\Z')


def _rows(value, field: str) -> list:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError as exc:
            raise ValueError(field + ' 不是有效 JSON') from exc
    if not isinstance(value, list):
        raise ValueError(field + ' 不是列表')
    return value


def _compact(value: str) -> str:
    return ''.join(value.split())


def canonical_method(name: str) -> str | None:
    if not isinstance(name, str):
        return None
    name = name.strip()
    name = ALIASES.get(name, name)
    return name if name in METHOD_CUES else None


def _method_id(name: str) -> str:
    return 'education_method_' + hashlib.sha256(name.encode('utf-8')).hexdigest()[:16]


def build_method_graph(paper_id: str, source_doc_id: str,
                       metadata: dict, chunks: list[dict]) -> dict:
    """Only executed, located steps can connect a paper to a shared method."""
    if not PAPER_ID.fullmatch(paper_id) or not source_doc_id:
        raise ValueError('方法 Graph 的稳定论文身份无效')
    if (metadata.get('education_paper_id') != paper_id
            or metadata.get('education_admission') != '入库'):
        raise ValueError('方法 Graph 的入库或论文身份不一致')
    if any(chunk.get('document_id') not in (None, source_doc_id) for chunk in chunks):
        raise ValueError('方法 Graph 切片来自其他来源文档')
    body = [(chunk['id'], _compact(chunk.get('content') or '')) for chunk in chunks
            if chunk.get('available') is True
            and 'role=body' in (chunk.get('important_keywords') or [])
            and chunk.get('id')]
    if not body:
        raise ValueError('方法 Graph 没有可核验的正文切片')
    raw_methods = metadata.get('education_method_details')
    if raw_methods is None:
        raw_methods = metadata.get('education_research_methods') or []
    methods = _rows(raw_methods, '研究方法')
    steps = _rows(metadata.get('education_research_steps') or [], '实际研究步骤')
    declared = []
    for row in methods:
        name = row.get('name') if isinstance(row, dict) else row
        canonical = canonical_method(name)
        if canonical and canonical not in declared:
            declared.append(canonical)
    result = {'schema': 'education-method-graph/1.0',
              'paper': {'id': paper_id, 'source_doc_id': source_doc_id},
              'methods': [], 'edges': [], 'unlinked_methods': []}
    for method in declared:
        source_ids = set()
        step_indexes = []
        for index, step in enumerate(steps):
            if not isinstance(step, dict) or step.get('status') != '实际执行':
                continue
            step_name = step.get('name') or ''
            if not isinstance(step_name, str):
                continue
            quote = step.get('quote') or ''
            if not isinstance(quote, str) or len(_compact(quote)) < 8:
                continue
            # Steps can be titled "实施康复训练" while their actual source
            # quotation explicitly identifies a case or single-subject design.
            # A declared method still needs an executed, located source step.
            if not any(cue in step_name or cue in quote for cue in METHOD_CUES[method]):
                continue
            matched = [chunk_id for chunk_id, content in body if _compact(quote) in content]
            if matched:
                source_ids.update(matched)
                step_indexes.append(index)
        if not source_ids:
            result['unlinked_methods'].append(method)
            continue
        method_id = _method_id(method)
        result['methods'].append({'id': method_id, 'name': method})
        result['edges'].append({'id': paper_id + ':uses_method:' + method_id,
                                'type': 'uses_method', 'paper_id': paper_id,
                                'method_id': method_id, 'source_doc_id': source_doc_id,
                                'source_chunk_ids': sorted(source_ids),
                                'evidence_step_indexes': step_indexes})
    return result


def merge_method_graphs(graphs: list[dict]) -> dict:
    """Create a cross-paper index by shared method ID, with no paper-paper edges."""
    papers = {}
    methods = {}
    edges = {}
    for graph in graphs:
        if graph.get('schema') != 'education-method-graph/1.0':
            raise ValueError('方法 Graph 版本不一致')
        paper = graph['paper']
        if paper['id'] in papers and papers[paper['id']] != paper:
            raise ValueError('稳定论文编号对应多个远端文档')
        papers[paper['id']] = paper
        for method in graph['methods']:
            if method['id'] in methods and methods[method['id']] != method:
                raise ValueError('方法节点 ID 冲突')
            methods[method['id']] = method
        for edge in graph['edges']:
            if edge['paper_id'] != paper['id'] or edge['method_id'] not in methods:
                raise ValueError('方法关系端点不一致')
            if edge['id'] in edges and edges[edge['id']] != edge:
                raise ValueError('重复方法关系不一致')
            edges[edge['id']] = edge
    ordered_edges = sorted(edges.values(), key=lambda row: (row['method_id'], row['paper_id']))
    return {'schema': 'education-method-index/1.0',
            'papers': sorted(papers.values(), key=lambda row: row['id']),
            'methods': sorted(methods.values(), key=lambda row: row['name']),
            'edges': ordered_edges}


def lookup_method(index: dict, name: str) -> list[dict]:
    method = canonical_method(name)
    if method is None:
        return []
    method_id = _method_id(method)
    return [row for row in index.get('edges') or [] if row['method_id'] == method_id]
