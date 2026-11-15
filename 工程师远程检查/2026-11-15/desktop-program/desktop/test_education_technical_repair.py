"""Contract regressions for the 14-paper technical failure cohort."""
import copy
import hashlib
import json

import pytest

from test_education_scope_repair import annotation, namespace, repaired as scope_repaired
from education_scope_repair import PREPARE, GATE, ANNOTATION
from education_technical_repair import repair_technical_dsl, repair_assessment_dsl

PIPELINES = ('72ff5454b8f711f1a8846d29005ec004', '2df63160b98911f199ca358d29021dae')


def repaired():
    return repair_technical_dsl(scope_repaired(), PIPELINES[0])


def test_education_assessment_is_a_distinct_evidence_checked_method():
    source = repaired()
    fixed = repair_assessment_dsl(source, PIPELINES[0])
    gate = namespace(fixed)
    text, info, record = annotation()
    record['research_methods'] = [{'name': '测验法', 'implementation': '通过试题记录学习掌握情况。'}]
    result = gate['validate'](json.dumps(record, ensure_ascii=False), text, info)
    assert result['research_methods'][0]['name'] == '测验法'
    assert repair_assessment_dsl(fixed, PIPELINES[0]) == fixed
    assert source == repaired()
    assert fixed['components'][PREPARE] == source['components'][PREPARE]
    with pytest.raises(ValueError): repair_assessment_dsl(source, 'finance')


def source_text(body):
    handoff = {'schema': 'paper-handoff/1.0', 'document_id': 'AUTO_0123456789ab',
               'stable_paper_id': 'paper_' + 'a' * 24, 'paper_id': 'paper_' + 'a' * 24,
               'major': '小学教育', 'classification': {'major': '小学教育'},
               'privacy_status': 'ai_passed', 'preprocessing_complete': True}
    return '# 教材比较研究\n[PAPER_PIPELINE_METADATA_V1]\n' + json.dumps(handoff, ensure_ascii=False) + '\n[/PAPER_PIPELINE_METADATA_V1]\n\n# 研究结果\n' + body


@pytest.mark.parametrize('major', ['数学教育', '人文教育'])
def test_new_education_candidates_preserve_identity(major):
    for node in (PREPARE, GATE):
        gate = namespace(repaired(), node)
        assert gate['major_admission'](major) == (major, major, None)
        assert gate['major_admission']('金融学')[0] is None


@pytest.mark.parametrize('name,canonical', [('个案研究法', '案例研究法'),
                                          ('观察法', '观察法'),
                                          ('单一被试实验法', '单一被试实验法')])
def test_actual_method_details_survive_normalization(name, canonical):
    gate = namespace(repaired())
    text, info, record = annotation()
    record['research_methods'] = [{'name': name, 'implementation': '按原文记录对象、步骤和分析方法。'}]
    result = gate['validate'](json.dumps(record, ensure_ascii=False), text, info)
    assert result['research_methods'] == [{'name': canonical, 'implementation': '按原文记录对象、步骤和分析方法。'}]


def test_full_large_payload_is_preserved_and_limit_remains_bounded():
    prepare = namespace(repaired(), PREPARE)
    text = source_text('完整表格材料\n' * 120000)
    assert len(text.encode()) > 1024 * 1024
    result = prepare['main']([{'text': text}])[0]
    assert '<EDUCATION_SOURCE>\n' + text + '\n</EDUCATION_SOURCE>' in result['text']
    assert result['source_sha256'] == hashlib.sha256(text.encode()).hexdigest()
    assert json.loads(result['text'].splitlines()[-1]) == {'coverage_token': result['coverage_token']}
    assert len(result['coverage_token']) == 32
    with pytest.raises(RuntimeError, match='单次传输保护上限'):
        prepare['main']([{'text': source_text('x' * (3 * 1024 * 1024))}])


def test_new_tail_is_verified_by_gate_with_original_source_and_annotation():
    dsl = repaired(); prepare = namespace(dsl, PREPARE); gate = namespace(dsl)
    _, _, record = annotation()
    text = source_text(record['evidence'][0]['quote'])
    info = prepare['main']([{'text': text}])[0]
    record['paper_id'] = info['paper_id']; record['coverage_token'] = info['coverage_token']
    annotated = [{**info, 'summary': json.dumps(record, ensure_ascii=False)}]
    assert gate['main'](annotated, [{'text': text}], [info])
    bad = {**info, 'text': info['text'] + ' extra'}
    with pytest.raises(RuntimeError, match='完整模型输入载荷不匹配'):
        gate['main'](annotated, [{'text': text}], [bad])


@pytest.mark.parametrize('defect,message', [('token', '全文结尾校验不匹配'),
                                          ('quality', '原文证据未覆盖'),
                                          ('quote', '原文证据未覆盖')])
def test_no_silent_token_or_evidence_repair(defect, message):
    gate = namespace(repaired()); text, info, record = annotation()
    if defect == 'token': record['coverage_token'] = 'c' * 32
    elif defect == 'quality': record['evidence'][0]['supports'].remove('quality')
    else: record['evidence'][0]['quote'] = '这个研究汇总的结论在论文原文中完全不存在。'
    with pytest.raises(ValueError, match=message):
        gate['validate'](json.dumps(record, ensure_ascii=False), text, info)


@pytest.mark.parametrize('pipeline', PIPELINES)
def test_only_scoped_nodes_change_and_install_is_idempotent(pipeline):
    source = scope_repaired(pipeline); before = copy.deepcopy(source)
    fixed = repair_technical_dsl(source, pipeline)
    assert source == before
    assert repair_technical_dsl(fixed, pipeline) == fixed
    for name, node in source['components'].items():
        if name not in (PREPARE, GATE, ANNOTATION): assert fixed['components'][name] == node
    assert fixed['components'][ANNOTATION]['obj']['params']['llm_id'] == source['components'][ANNOTATION]['obj']['params']['llm_id']
    for node in fixed['graph']['nodes']:
        if node['id'] in (PREPARE, GATE, ANNOTATION):
            assert node['data']['form'] == fixed['components'][node['id']]['obj']['params']
    with pytest.raises(ValueError): repair_technical_dsl(source, 'finance')
    bad = copy.deepcopy(source)
    bad['components'][PREPARE]['obj']['params']['script'] += '\n# unexpected drift'
    with pytest.raises(ValueError): repair_technical_dsl(bad, pipeline)
