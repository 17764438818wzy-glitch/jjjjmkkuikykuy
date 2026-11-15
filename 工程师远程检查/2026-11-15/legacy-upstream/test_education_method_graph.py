import json

import pytest

from education_method_graph import build_method_graph, merge_method_graphs, lookup_method


def sample(paper_id, doc_id, subject):
    quote = f'在{subject}向教师发放问卷并回收有效问卷20份。'
    meta = {
        'education_paper_id': paper_id, 'education_admission': '入库',
        'education_method_details': json.dumps([
            {'name': '问卷调查法', 'implementation': quote},
            {'name': '访谈法', 'implementation': '计划访谈教师。'},
            {'name': '混合研究法', 'implementation': '问卷与访谈组合。'}], ensure_ascii=False),
        'education_research_steps': json.dumps([
            {'name': '发放并回收教师问卷', 'status': '实际执行', 'quote': quote,
             'materials': ['有效问卷20份'], 'analysis_elements': []},
            {'name': '访谈教师', 'status': '计划', 'quote': '计划访谈教师。',
             'materials': [], 'analysis_elements': []}], ensure_ascii=False),
    }
    chunks = [
        {'id': doc_id + '-body', 'document_id': doc_id, 'available': True,
         'important_keywords': ['role=body'], 'content': '# 研究过程\n' + quote},
        {'id': doc_id + '-index', 'document_id': doc_id, 'available': False,
         'important_keywords': ['role=index'], 'content': '计划访谈教师。'},
    ]
    return meta, chunks


def test_different_subjects_share_method_without_copying_research_data():
    first_id = 'paper_' + 'a' * 24
    second_id = 'paper_' + 'b' * 24
    first = build_method_graph(first_id, 'doc-one', *sample(first_id, 'doc-one', '甲校'))
    second = build_method_graph(second_id, 'doc-two', *sample(second_id, 'doc-two', '乙校'))
    merged = merge_method_graphs([first, second])
    assert len(merged['methods']) == 1
    assert merged['methods'][0]['name'] == '问卷调查法'
    assert len(merged['edges']) == 2
    assert {edge['source_chunk_ids'][0] for edge in merged['edges']} == {'doc-one-body', 'doc-two-body'}
    assert {edge['paper_id'] for edge in lookup_method(merged, '问卷调查法')} == {first_id, second_id}
    content = json.dumps(merged, ensure_ascii=False)
    assert all(value not in content for value in ('甲校', '乙校', '20份', '计划访谈'))


def test_planned_or_unlocated_method_does_not_create_edge():
    paper_id = 'paper_' + 'c' * 24
    meta, chunks = sample(paper_id, 'doc-three', '丙校')
    steps = json.loads(meta['education_research_steps'])
    steps[0]['quote'] = '正文中不存在的问卷发放记录。'
    meta['education_research_steps'] = json.dumps(steps, ensure_ascii=False)
    result = build_method_graph(paper_id, 'doc-three', meta, chunks)
    assert result['edges'] == []
    assert '问卷调查法' in result['unlinked_methods']
    assert '访谈法' in result['unlinked_methods']
    assert '混合研究法' not in result['unlinked_methods']


def test_foreign_body_chunk_fails_closed():
    paper_id = 'paper_' + 'd' * 24
    meta, chunks = sample(paper_id, 'doc-four', '丁校')
    chunks[0]['document_id'] = 'foreign-doc'
    with pytest.raises(ValueError, match='来源文档'):
        build_method_graph(paper_id, 'doc-four', meta, chunks)


def test_multiple_actual_steps_merge_into_one_method_edge():
    paper_id = 'paper_' + 'e' * 24
    meta, chunks = sample(paper_id, 'doc-five', '戊校')
    steps = json.loads(meta['education_research_steps'])
    steps.append({'name': '分析问卷结果', 'status': '实际执行',
                  'quote': '随后分析问卷结果并归纳主要问题。',
                  'materials': [], 'analysis_elements': []})
    meta['education_research_steps'] = json.dumps(steps, ensure_ascii=False)
    chunks.append({'id': 'doc-five-body2', 'document_id': 'doc-five', 'available': True,
                   'important_keywords': ['role=body'],
                   'content': '随后分析问卷结果并归纳主要问题。'})
    graph = build_method_graph(paper_id, 'doc-five', meta, chunks)
    assert len(graph['edges']) == 1
    assert graph['edges'][0]['source_chunk_ids'] == ['doc-five-body', 'doc-five-body2']
    assert graph['edges'][0]['evidence_step_indexes'] == [0, 2]


@pytest.mark.parametrize('method,step,quote,canonical', [
    ('课例研究法', '教学实录对比', '选取七位教师的课例并对教学实录中的评价语进行逐项比较。', '课例研究法'),
    ('逻辑分析法', '归纳推理', '运用逻辑分析法对调查中发现的问题进行归纳推理，分析其原因。', '逻辑分析法'),
    ('测验法', '学习能力测验', '使用同一套试题对学生进行学习能力测验并记录得分。', '测验法'),
    ('观察法', '基线期记录', '对儿童进行为期三周的观察记录，中间不提供干预。', '观察法'),
    ('单一被试实验法', '跨行为倒返实验干预与记录',
     '采用单一被试研究法之跨行为倒返实验设计记录课堂行为的发生次数。', '单一被试实验法'),
    ('个案研究法', '实施康复训练', '根据个案前测结果制定训练方案，并实施三个阶段的康复训练。', '案例研究法'),
])
def test_repaired_methods_link_to_located_executed_step(method, step, quote, canonical):
    paper = 'paper_' + 'f' * 24
    meta = {'education_paper_id': paper, 'education_admission': '入库',
            'education_method_details': [{'name': method, 'implementation': quote}],
            'education_research_steps': [{'name': step, 'status': '实际执行', 'quote': quote}]}
    chunks = [{'id': 'located', 'document_id': 'doc', 'available': True,
               'important_keywords': ['role=body'], 'content': quote}]
    graph = build_method_graph(paper, 'doc', meta, chunks)
    assert [m['name'] for m in graph['methods']] == [canonical]
    assert lookup_method(merge_method_graphs([graph]), method)[0]['source_chunk_ids'] == ['located']
    meta['education_research_steps'][0]['status'] = '计划'
    assert build_method_graph(paper, 'doc', meta, chunks)['edges'] == []
    meta['education_research_steps'][0]['status'] = '实际执行'
    chunks[0]['content'] = '另一篇完全不同的材料。'
    assert build_method_graph(paper, 'doc', meta, chunks)['edges'] == []
