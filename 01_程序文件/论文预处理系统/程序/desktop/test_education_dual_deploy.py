import copy
import pytest


def source_dsl():
    return {'components': {'Extractor:EducationAnnotation': {'obj': {'params': {
        'llm_id': 'school-model', 'sys_prompt': 'unchanged', 'max_tokens': 8192}}}},
        'graph': {'nodes': [{'id': 'Extractor:EducationAnnotation', 'data': {
            'form': {'llm_id': 'school-model', 'max_tokens': 8192}}}]}}


def test_model_binding_changes_without_touching_prompt_or_original():
    from education_dual_deploy import model_only_dsl
    original = source_dsl()
    result = model_only_dsl(original, 'school-model', 'official-model')
    assert original['components']['Extractor:EducationAnnotation']['obj']['params']['llm_id'] == 'school-model'
    assert result['components']['Extractor:EducationAnnotation']['obj']['params'] == {
        'llm_id': 'official-model', 'sys_prompt': 'unchanged', 'max_tokens': 8192}
    assert result['graph']['nodes'][0]['data']['form']['llm_id'] == 'official-model'


def test_unknown_generation_node_or_mismatched_ui_binding_is_rejected():
    from education_dual_deploy import model_only_dsl
    original = source_dsl()
    original['components']['Other:LLM'] = copy.deepcopy(original['components']['Extractor:EducationAnnotation'])
    with pytest.raises(ValueError):
        model_only_dsl(original, 'school-model', 'official-model')
    original = source_dsl()
    original['graph']['nodes'][0]['data']['form']['llm_id'] = 'unexpected'
    with pytest.raises(ValueError):
        model_only_dsl(original, 'school-model', 'official-model')


def test_contract_hash_is_same_for_equivalent_routes_and_changes_with_prompt():
    from education_dual_deploy import contract_hash, model_only_dsl
    original = source_dsl()
    official = model_only_dsl(original, 'school-model', 'official-model')
    assert contract_hash(original) == contract_hash(official)
    official['components']['Extractor:EducationAnnotation']['obj']['params']['sys_prompt'] = 'different'
    assert contract_hash(original) != contract_hash(official)

def test_output_reminder_keeps_schema_and_other_components_identical():
    from education_dual_deploy import annotation_limits_dsl,model_only_dsl,contract_hash
    source=source_dsl();source['graph']['nodes'][0]['data']['form']['sys_prompt']='unchanged'
    updated=annotation_limits_dsl(source)
    prompt=updated['components']['Extractor:EducationAnnotation']['obj']['params']['sys_prompt']
    assert '最多12项' in prompt
    assert updated['graph']['nodes'][0]['data']['form']['sys_prompt']==prompt
    assert annotation_limits_dsl(updated)==updated
    official=model_only_dsl(updated,'school-model','official-model')
    assert contract_hash(official)==contract_hash(updated)
    assert source['components']['Extractor:EducationAnnotation']['obj']['params']['sys_prompt']=='unchanged'
