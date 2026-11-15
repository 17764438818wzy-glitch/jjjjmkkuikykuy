import json

import pytest

from education_verified_artifacts import (audit_source_tree, is_questionnaire_heading,
                                          project_document, render_wiki, validate_assignments)


def fixture():
    item = {'document_id': 'AUTO_abc', 'stable_paper_id': 'paper_' + 'a' * 24, 'title': '小学数学作业调查'}
    steps = [
        {'name': '发放教师问卷', 'status': '实际执行', 'quote': '向教师发放问卷20份，回收18份。',
         'materials': ['教师问卷20份', '访谈记录'], 'analysis_elements': ['回收18份']},
        {'name': '统计回收问卷', 'status': '实际执行', 'quote': '对18份有效问卷进行描述统计。',
         'materials': ['18份有效问卷'], 'analysis_elements': ['描述统计']},
        {'name': '准备课堂干预', 'status': '计划', 'quote': '计划开展课堂干预。',
         'materials': ['干预教案'], 'analysis_elements': []},
    ]
    doc = {'id': 'remote-1', 'run': 'DONE', 'meta_fields': {
        'education_paper_id': item['stable_paper_id'], 'education_admission': '入库',
        'artifact_example_tier': '普通本科', 'artifact_paper_type': '小学数学作业现状调查论文',
        'education_research_steps': json.dumps(steps, ensure_ascii=False),
    }}
    chunks = [
        {'id': '1' * 16, 'available': True, 'important_keywords': ['role=body', 'section_no=2.1'], 'content': '向教师发放问卷20份，回收18份。'},
        {'id': '2' * 16, 'available': True, 'important_keywords': ['role=body', 'section_no=3.1'], 'content': '对18份有效问卷进行描述统计。'},
        {'id': '3' * 16, 'available': False, 'important_keywords': ['role=index'], 'content': '计划开展课堂干预。'},
    ]
    tree = {'templates': [{'kind': 'page_index', 'entities': [
        {'name': '研究方法', 'source_chunk_ids': ['1' * 16]}], 'relations': []},
        {'kind': 'knowledge_graph', 'entities': [
            {'name': '近义路径甲', 'type': 'research_path'}, {'name': '近义路径乙', 'type': 'research_path'}],
         'relations': []}]}
    return item, doc, chunks, tree


def test_projection_uses_validated_steps_and_body_evidence_only():
    item, doc, chunks, tree = fixture()
    result = project_document(item, doc, chunks, tree, ('调查诊断', '现状调查与对策'))
    assert result['paper_id'] == item['stable_paper_id']
    assert result['wiki']['l3'] == '发放教师问卷 → 统计回收问卷'
    assert result['wiki']['section_numbers'] == ['2.1', '3.1']
    assert len([n for n in result['graph']['nodes'] if n['type'] == 'research_path']) == 1
    assert [n['name'] for n in result['graph']['nodes'] if n['type'] == 'paper_type'] == [
        '小学数学作业现状调查论文']
    assert len([n for n in result['graph']['nodes'] if n['type'] == 'research_step']) == 2
    assert all(edge['source_chunk_ids'] for edge in result['graph']['edges'])
    assert all(set(edge['source_chunk_ids']) <= {'1' * 16, '2' * 16} for edge in result['graph']['edges'])
    assert not any('干预' in n['name'] for n in result['graph']['nodes'])
    assert not any(n['name'] == '访谈记录' for n in result['graph']['nodes'])
    assert result['tree']['entities'][0]['name'] == '研究方法'
    wiki = render_wiki([result])
    assert item['stable_paper_id'] in wiki and '1' * 16 in wiki and '普通本科' in wiki
    assert '章节：2.1、3.1' in wiki


def test_projection_accepts_direct_directory_tree_without_native_page_index():
    item, doc, chunks, structure = fixture()
    direct = {'schema': 'education-direct-tree/1.0', 'paper_id': item['stable_paper_id'],
              'source_doc_id': doc['id'], 'issues': [], 'nodes': [
        {'id': item['stable_paper_id'] + ':section_1234', 'title': '研究方法', 'level': 1,
         'order': 0, 'parent_id': None, 'source_chunk_ids': ['1' * 16]}], 'relations': []}
    structure['templates'] = [structure['templates'][1]]
    projected = project_document(item, doc, chunks, structure,
                                 ('调查诊断', '现状调查与对策'), direct_tree=direct)
    assert projected['tree']['entities'][0]['name'] == '研究方法'
    assert projected['tree']['schema'] == 'education-direct-tree/1.0'
    direct['nodes'][0]['source_chunk_ids'] = ['3' * 16]
    with pytest.raises(ValueError, match='Tree'):
        project_document(item, doc, chunks, structure,
                         ('调查诊断', '现状调查与对策'), direct_tree=direct)


def test_unreviewed_profile_tier_can_be_linked_as_pending():
    item, doc, chunks, structure = fixture()
    doc['meta_fields']['artifact_example_tier'] = ''
    projected = project_document(item, doc, chunks, structure,
                                 ('调查诊断', '现状调查与对策'))
    assert projected['tier'] == '待核实'
    assert '档位候选：待核实' in render_wiki([projected])


def test_projection_fails_closed_when_step_quote_is_not_in_body():
    item, doc, chunks, tree = fixture()
    doc['meta_fields']['education_research_steps'] = json.dumps([
        {'name': '实施前后测', 'status': '实际执行', 'quote': '不存在的前后测结果',
         'materials': [], 'analysis_elements': []}], ensure_ascii=False)
    with pytest.raises(ValueError, match='正文切片'):
        project_document(item, doc, chunks, tree, ('干预评估', '单组前后测'))


def test_projection_rejects_unverified_identity_and_tree_source():
    item, doc, chunks, tree = fixture()
    doc['meta_fields']['education_paper_id'] = 'paper_' + 'b' * 24
    with pytest.raises(ValueError, match='身份'):
        project_document(item, doc, chunks, tree, ('调查诊断', '现状调查与对策'))
    doc['meta_fields']['education_paper_id'] = item['stable_paper_id']
    tree['templates'][0]['entities'][0]['source_chunk_ids'] = ['3' * 16]
    with pytest.raises(ValueError, match='Tree'):
        project_document(item, doc, chunks, tree, ('调查诊断', '现状调查与对策'))


def test_projection_rejects_foreign_body_chunk_and_repeated_step_name():
    item, doc, chunks, tree = fixture()
    chunks[0]['document_id'] = 'some-other-document'
    with pytest.raises(ValueError, match='正文切片来源文档'):
        project_document(item, doc, chunks, tree, ('调查诊断', '现状调查与对策'))
    chunks[0]['document_id'] = doc['id']
    steps = json.loads(doc['meta_fields']['education_research_steps'])
    steps[1]['name'] = steps[0]['name']
    doc['meta_fields']['education_research_steps'] = json.dumps(steps, ensure_ascii=False)
    with pytest.raises(ValueError, match='重复'):
        project_document(item, doc, chunks, tree, ('调查诊断', '现状调查与对策'))


def test_reviewed_canonical_path_groups_wording_variants_without_losing_steps():
    item, doc, chunks, tree = fixture()
    canonical = '教师问卷 → 描述统计 → 问题归纳'
    result = project_document(item, doc, chunks, tree, ('调查诊断', '现状调查与对策', canonical))
    assert result['wiki']['l3'] == canonical
    assert [n['name'] for n in result['graph']['nodes'] if n['type'] == 'research_path'] == [canonical]
    assert len([n for n in result['graph']['nodes'] if n['type'] == 'research_step']) == 2


def test_source_tree_preserves_ingest_order_and_flags_suspicious_front_matter():
    ingest = '# 标题\n\n# 参考文献\n\n# 1.前言\n\n## 1.1研究对象\n'
    chunks = [
        {'id': '1' * 16, 'available': True, 'important_keywords': ['role=body'],
         'content': '# 标题\n# 参考文献\n'},
        {'id': '2' * 16, 'available': True, 'important_keywords': ['role=body'],
         'content': '# 1.前言\n## 1.1研究对象\n'},
    ]
    result = audit_source_tree(ingest, chunks)
    assert [h['title'] for h in result['headings']] == ['标题', '参考文献', '1.前言', '1.1研究对象']
    assert result['headings'][2]['source_chunk_ids'] == ['2' * 16]
    assert '参考文献位于正文第一章之前' in result['issues']


def test_source_tree_flags_questionnaire_heading_and_missing_body_source():
    ingest = '# 标题\n\n# 1.您的性别（ ）\n\n# 2.研究对象\n'
    chunks = [{'id': '1' * 16, 'available': True, 'important_keywords': ['role=body'],
               'content': '# 标题\n# 2.研究对象'}]
    result = audit_source_tree(ingest, chunks)
    assert '疑似问卷题项被标为章节' in result['issues']
    assert result['headings'][1]['source_chunk_ids'] == []
    assert '章节标题未能定位正文切片' in result['issues']


def test_questionnaire_heading_recognizes_neutral_and_polite_pronouns():
    assert is_questionnaire_heading('4.你在体育教学中受伤的频数 （  ）')
    assert is_questionnaire_heading('1.您的性别（ ）')
    assert not is_questionnaire_heading('4.1 教学安全问题分析')


def test_taxonomy_assignment_must_use_selected_stable_paper_id():
    item, _, _, _ = fixture()
    with pytest.raises(ValueError, match='未知稳定论文编号'):
        validate_assignments([item], {'paper_' + 'b' * 24: ['调查诊断', '现状调查与对策']})


def test_case_comparison_has_its_own_reviewed_taxonomy_slot():
    item, doc, chunks, tree = fixture()
    result = project_document(item, doc, chunks, tree,
                              ('课例与文本分析', '课例比较', '三节课例→目标与过程比较→教学启示'))
    assert result['wiki']['l1'] == '课例与文本分析'
    assert result['wiki']['l2'] == '课例比较'


def test_multiple_case_analysis_does_not_claim_cross_case_comparison():
    item, doc, chunks, tree = fixture()
    result = project_document(item, doc, chunks, tree,
                              ('课例与文本分析', '多课例分析', '三节识字课例分析→教学启示'))
    assert result['wiki']['l2'] == '多课例分析'


def test_child_behavior_observation_has_a_distinct_taxonomy_slot():
    item, doc, chunks, tree = fixture()
    result = project_document(item, doc, chunks, tree,
                              ('个案与叙事', '儿童行为观察研究', '活动观察→事件编码→原因分析'))
    assert result['wiki']['l2'] == '儿童行为观察研究'


def test_reviewer_can_replace_a_steps_citation_with_an_exact_body_quote():
    item, doc, chunks, tree = fixture()
    steps = json.loads(doc['meta_fields']['education_research_steps'])
    steps[0]['name'] = '随机分组并发放问卷'
    doc['meta_fields']['education_research_steps'] = json.dumps(steps, ensure_ascii=False)
    chunks.append({'id': '4' * 16, 'available': True, 'important_keywords': ['role=body'],
                   'content': '将20名学生随机分为两组并发放问卷。'})
    corrected = '将20名学生随机分为两组并发放问卷。'
    result = project_document(item, doc, chunks, tree,
                              ('调查诊断', '现状调查与对策'),
                              reviewed_step_quotes={'随机分组并发放问卷': corrected})
    step = next(node for node in result['graph']['nodes']
                if node['type'] == 'research_step' and node['name'] == '随机分组并发放问卷')
    assert step['source_chunk_ids'] == ['4' * 16]
    with pytest.raises(ValueError, match='正文切片'):
        project_document(item, doc, chunks, tree, ('调查诊断', '现状调查与对策'),
                         reviewed_step_quotes={'随机分组并发放问卷': '不存在的随机分组说明'})
    with pytest.raises(ValueError, match='人工引文'):
        project_document(item, doc, chunks, tree, ('调查诊断', '现状调查与对策'),
                         reviewed_step_quotes={'不存在的步骤': corrected})
