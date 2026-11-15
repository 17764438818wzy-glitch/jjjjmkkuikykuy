"""Execute the bounded education scope repair against the sanitized DSL."""
import copy
import hashlib
import json
from pathlib import Path

import pytest

from education_failure_repair import repair_dsl
from education_pipeline_builder import ANNOTATION_PROMPT


PIPELINES = ('72ff5454b8f711f1a8846d29005ec004', '2df63160b98911f199ca358d29021dae')
PREPARE = 'CodeExec:PrepareEducation'
GATE = 'CodeExec:EducationGate'
ANNOTATION = 'Extractor:EducationAnnotation'


def reference():
    source = json.loads((Path(__file__).parent / 'fixtures' /
                         'education_failure_reference.json').read_text(encoding='utf-8'))
    source['components'][ANNOTATION]['obj']['params']['sys_prompt'] = ANNOTATION_PROMPT
    for node in source['graph']['nodes']:
        if node['id'] == ANNOTATION:
            node['data']['form'] = copy.deepcopy(source['components'][ANNOTATION]['obj']['params'])
    return repair_dsl(source)


def repaired(pipeline=PIPELINES[0]):
    from education_scope_repair import repair_scope_dsl
    return repair_scope_dsl(reference(), pipeline)


def namespace(dsl, node=GATE):
    result = {}
    exec(dsl['components'][node]['obj']['params']['script'], result)
    return result


def annotation():
    quote = '本研究比较两组语言教材的词语说明与练习安排，逐项记录教材差异，发现情境练习有助于呈现不同词语的使用条件。'
    text = '# 教材比较研究\n\n' + quote + '\n'
    info = {'paper_id': 'paper_' + 'a' * 24, 'coverage_token': 'b' * 32,
            'source_sha256': hashlib.sha256(text.encode()).hexdigest(),
            'classification': {'major': '汉语言教育'}}
    record = {
        'paper_id': info['paper_id'], 'coverage_token': info['coverage_token'],
        'research_subject': {'name': '语言教材', 'scope': '教材说明与练习'},
        'education_context': {'stage': '', 'subject': '汉语', 'setting': '教材'},
        'research_methods': [{'name': '比较研究法', 'implementation': '逐项比较教材说明与练习。'}],
        'research_steps': [{'name': '教材比较', 'status': '实际执行', 'materials': ['两组教材'],
                            'analysis_elements': ['说明', '练习'], 'quote': quote}],
        'research_content': {'question': '教材编排如何支持词语使用',
                             'analysis': ['比较说明和练习安排'], 'findings': ['情境练习呈现使用条件']},
        'research_directions': ['课程与教学设计'], 'audience_tags': ['普通本科', '函授'],
        'quality': {'decision': '入库', 'reason': '完成教材比较并保留原文证据', 'issues': []},
        'judgement_basis': '依据正文实际完成的教材比较。', 'uncertainty_reason': '',
        'evidence': [{'quote': quote, 'supports': ['research_subject', 'research_methods',
                                                 'research_content', 'quality']}],
        'artifact_profile': {'paper_type': '教材比较研究', 'research_path': '比较教材说明与练习',
                             'required_materials': ['两组教材'], 'example_tier': '普通本科',
                             'tier_basis': '完成比较分析', 'evidence_quotes': [quote]},
    }
    return text, info, record


@pytest.mark.parametrize('pipeline', PIPELINES)
def test_candidate_preserves_local_language_identity_and_finance_exclusion(pipeline):
    dsl = repaired(pipeline)
    for node in (PREPARE, GATE):
        ns = namespace(dsl, node)
        assert ns['major_admission']('汉语言教育') == ('汉语言教育', '汉语言教育', None)
        assert ns['major_admission']('汉语国际教育')[0] == '汉语国际教育'
        assert ns['major_admission']('金融学')[0] is None


def test_real_gate_accepts_new_methods_and_preserves_implementations():
    gate = namespace(repaired())
    text, info, record = annotation()
    record['research_methods'] = [
        {'name': '案例分析法', 'implementation': '逐项分析教材案例。'},
        {'name': '案例研究法', 'implementation': '保留不同案例的说明。'},
        {'name': '比较研究法', 'implementation': '比较教材的共同点与差异。'},
        {'name': '历史法', 'implementation': '结合历史语境梳理教材说明的变化。'},
    ]
    before = copy.deepcopy(record)
    checked = gate['validate'](json.dumps(record, ensure_ascii=False), text, info)
    assert record == before
    assert checked['research_methods'] == [
        {'name': '案例研究法', 'implementation': '逐项分析教材案例。\n保留不同案例的说明。'},
        {'name': '比较研究法', 'implementation': '比较教材的共同点与差异。'},
        {'name': '历史研究法', 'implementation': '结合历史语境梳理教材说明的变化。'},
    ]
    assert checked['quality']['decision'] == '入库'
    assert checked['research_steps'][0]['quote'] == record['research_steps'][0]['quote']


@pytest.mark.parametrize('broken, expected', [
    ('quality', '原文证据未覆盖'), ('token', '全文结尾校验不匹配'),
    ('step', '没有可证实的实际研究步骤'), ('quote', '原文证据未覆盖'),
])
def test_repair_keeps_mandatory_evidence_and_token_checks(broken, expected):
    gate = namespace(repaired())
    text, info, record = annotation()
    if broken == 'quality': record['evidence'][0]['supports'].remove('quality')
    if broken == 'token': record['coverage_token'] = 'c' * 32
    if broken == 'step': record['research_steps'][0]['quote'] = '这里是论文中不存在的步骤引文。'
    if broken == 'quote': record['evidence'][0]['quote'] = '这里是论文中不存在的核心引文。'
    with pytest.raises(ValueError, match=expected):
        gate['validate'](json.dumps(record, ensure_ascii=False), text, info)


def test_repair_does_not_override_rejection_or_source_hash():
    gate = namespace(repaired())
    text, info, record = annotation()
    record['quality']['decision'] = '剔除'
    record['quality']['reason'] = '正文未建立教育研究问题'
    checked = gate['validate'](json.dumps(record, ensure_ascii=False), text, info)
    assert checked['quality']['decision'] == '剔除'
    info.update(text='untrusted payload', source_sha256='0' * 64)
    with pytest.raises(RuntimeError, match='指纹不一致'):
        gate['main']([], [{'text': text}], [info])


def test_repair_is_idempotent_keeps_model_binding_and_other_nodes():
    from education_scope_repair import repair_scope_dsl
    source = reference(); before = copy.deepcopy(source)
    fixed = repair_scope_dsl(source, PIPELINES[0])
    assert source == before
    assert repair_scope_dsl(fixed, PIPELINES[0]) == fixed
    changed = {PREPARE, GATE, ANNOTATION}
    for name, component in source['components'].items():
        if name not in changed: assert fixed['components'][name] == component
    assert fixed['components'][ANNOTATION]['obj']['params']['llm_id'] == source['components'][ANNOTATION]['obj']['params']['llm_id']
    for node in fixed['graph']['nodes']:
        if node['id'] in changed:
            assert node['data']['form'] == fixed['components'][node['id']]['obj']['params']


def test_repair_rejects_other_pipeline_and_changed_or_partial_dsl():
    from education_scope_repair import repair_scope_dsl
    with pytest.raises(ValueError): repair_scope_dsl(reference(), 'finance-pipeline')
    bad = reference()
    bad['components'][GATE]['obj']['params']['script'] += '\n# unexpected alias\naliases={}\n'
    with pytest.raises(ValueError): repair_scope_dsl(bad, PIPELINES[0])
    partial = repaired()
    partial['components'][PREPARE] = reference()['components'][PREPARE]
    for node in partial['graph']['nodes']:
        if node['id'] == PREPARE:
            node['data']['form'] = copy.deepcopy(partial['components'][PREPARE]['obj']['params'])
    with pytest.raises(ValueError, match='Partially'):
        repair_scope_dsl(partial, PIPELINES[0])


def test_repair_rejects_anchor_change_even_with_consistent_graph():
    from education_scope_repair import repair_scope_dsl
    source = reference()
    params = source['components'][GATE]['obj']['params']
    params['script'] = params['script'].replace("'文献资料法':'文献研究法'", "'changed':'文献研究法'")
    for node in source['graph']['nodes']:
        if node['id'] == GATE: node['data']['form'] = copy.deepcopy(params)
    with pytest.raises(ValueError, match='anchor'):
        repair_scope_dsl(source, PIPELINES[0])


def test_graph_aliasing_cannot_erase_executable_node_params():
    from education_scope_repair import repair_scope_dsl
    source = reference()
    for node in source['graph']['nodes']:
        if node['id'] in (PREPARE, GATE, ANNOTATION):
            node['data']['form'] = source['components'][node['id']]['obj']['params']
    fixed = repair_scope_dsl(source, PIPELINES[0])
    assert namespace(fixed)['major_admission']('汉语言教育')[0] == '汉语言教育'
