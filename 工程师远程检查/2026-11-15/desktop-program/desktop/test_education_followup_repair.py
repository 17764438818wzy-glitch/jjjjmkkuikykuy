"""Regressions for the six annotation failures observed on September 28."""
import copy
import json
import pytest
import education_technical_repair as repair
from test_education_technical_repair import repaired, PIPELINES
from test_education_scope_repair import annotation, namespace
from education_scope_repair import PREPARE, GATE, ANNOTATION

def candidate(pipeline=PIPELINES[0]):
    return repair.repair_followup_dsl(repair.repair_assessment_dsl(repaired(), pipeline), pipeline)

def test_english_education_is_only_a_candidate_and_retains_its_identity():
    for node in (PREPARE, GATE):
        ns=namespace(candidate(),node)
        assert ns['major_admission']('英语教育') == ('英语教育','英语教育',None)
        assert ns['major_admission']('金融学')[0] is None

@pytest.mark.parametrize('method',['逻辑分析法','课例研究法'])
def test_valid_executed_methods_keep_their_implementation(method):
    ns=namespace(candidate());text,info,record=annotation()
    record['research_methods']=[{'name':method,'implementation':'保留逐项比较、归纳及实际分析过程。'}]
    checked=ns['validate'](json.dumps(record,ensure_ascii=False),text,info)
    assert checked['research_methods']==record['research_methods']

@pytest.mark.parametrize('defect',['unclosed','nested','quote','coverage'])
def test_new_prompt_never_bypasses_structure_or_source_checks(defect):
    ns=namespace(candidate());text,info,record=annotation()
    if defect=='nested':record['research_content']['quality']=record.pop('quality')
    if defect=='quote':record['evidence'][0]['quote']='这是改写后而不是原文连续出现的研究对象说明。'
    if defect=='coverage':record['coverage_token']='0'*32
    raw=json.dumps(record,ensure_ascii=False)
    if defect=='unclosed':raw=raw[:-1]
    with pytest.raises(ValueError):ns['validate'](raw,text,info)

@pytest.mark.parametrize('pipeline',PIPELINES)
def test_followup_is_scoped_idempotent_and_retains_model_and_non_target_nodes(pipeline):
    source=repair.repair_assessment_dsl(repaired(),pipeline);before=copy.deepcopy(source)
    fixed=repair.repair_followup_dsl(source,pipeline)
    assert source==before
    assert repair.repair_followup_dsl(fixed,pipeline)==fixed
    for name in source['components']:
        if name not in (PREPARE,GATE,ANNOTATION):assert fixed['components'][name]==source['components'][name]
    assert fixed['components'][ANNOTATION]['obj']['params']['llm_id']==source['components'][ANNOTATION]['obj']['params']['llm_id']
    for node in fixed['graph']['nodes']:
        if node['id'] in (PREPARE,GATE,ANNOTATION):assert node['data']['form']==fixed['components'][node['id']]['obj']['params']
    with pytest.raises(ValueError):repair.repair_followup_dsl(source,'finance')

def test_structure_example_has_all_top_level_fields_without_changing_validator():
    source=candidate()
    fixed=repair.repair_structure_dsl(source,PIPELINES[0])
    prompt=fixed['components'][ANNOTATION]['obj']['params']['sys_prompt']
    example=json.loads(prompt.split('<EDUCATION_JSON_STRUCTURE>')[1].split('</EDUCATION_JSON_STRUCTURE>')[0])
    assert set(example)==set(annotation()[2])
    assert set(example['research_content'])=={'question','analysis','findings'}
    assert set(example['artifact_profile'])==set(annotation()[2]['artifact_profile'])
    assert fixed['components'][GATE]==source['components'][GATE]
    assert repair.repair_structure_dsl(fixed,PIPELINES[0])==fixed
    with pytest.raises(ValueError):repair.repair_structure_dsl(source,'finance')
