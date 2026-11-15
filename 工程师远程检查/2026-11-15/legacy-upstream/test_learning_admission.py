import importlib.util
import pytest
import research_profile as p

def test_scalar_rating_format_repairs_only_affected_dimension_and_preserves_unknown():
    original={'units':[{'assessments':[{'grades':{'content':'充分','materials':None,'operation':{'grade':None,'reason':'原文未知'}}}]}]}
    targets=p.repairable_scores(original)
    assert len(targets)==2
    repaired=p.apply_score_repairs(original,targets,{'ratings':[{'path':t['path'],'grade':None,'reason':'尚未核实'} for t in targets]})
    assert repaired['units'][0]['assessments'][0]['grades']['operation']==original['units'][0]['assessments'][0]['grades']['operation']
    assert repaired['units'][0]['assessments'][0]['grades']['content']['grade'] is None
    assert original['units'][0]['assessments'][0]['grades']['content']=='充分'


def test_independent_verified_use_survives_invalid_core():
    profile = {'summary': {'decision': 'disabled'}, 'units': [
        {'id': 'bad', 'core': {'value': True, 'verification': 'verified'},
         'assessments': [{'aspect': 'execution_steps', 'verification': 'verified', 'decision': 'disabled'}]},
        {'id': 'structure', 'core': {'value': False, 'verification': 'verified'},
         'assessments': [{'aspect': 'execution_steps', 'verification': 'verified', 'decision': 'recommended'}]}]}
    result = p.admission(profile)
    assert result['status'] == 'limited'
    assert result['core_invalid'] is True
    assert result['usable_aspects'] == 1
    assert profile['units'][0]['assessments'][0]['decision'] == 'disabled'


def test_complete_description_is_separate_from_research_path():
    sections = [{'id': 's1'}]
    coverage = [{'section_id': 's1', 'status': 'verified', 'purpose': '说明本章实际内容',
                 'evidence_ids': ['e1'], 'missing_information': [{'kind': 'source_not_stated', 'field': '样本依据'}]}]
    result = p.completeness(sections, coverage, [], [], {'status': 'partial', 'verification': 'pending'})
    assert result['status'] == 'complete'
    assert result['research_route_status'] == 'partial'
    coverage[0]['processing_gaps'] = ['尚未读取必要小节']
    assert p.completeness(sections, coverage, [], [])['status'] == 'partial'


def intake_module():
    assert importlib.util.find_spec('material_intake'), 'missing early material intake seam'
    return __import__('material_intake')


def test_early_intake_accepts_useful_material_without_quote_gate(tmp_path):
    m = intake_module()
    class Agent:
        config = {'text_model': 'saved'}
        def __init__(self): self.calls = []
        def call(self, task, payload, images=()):
            self.calls.append((task, payload, images))
            return {'decision': 'pass', 'intake': 'keep', 'kind': '研究代码', 'route': 'text', 'reason': '包含设计实现代码'}
    agent = Agent()
    result = m.classify_material(agent, '源码及设计说明', source_sha='a'*64, cache_root=tmp_path)
    assert result['intake'] == 'keep' and result['route'] == 'text'
    assert len(agent.calls) == 1
    assert not any(k in agent.calls[0][1] for k in ('evidence', 'span_ids'))
    again = m.classify_material(agent, '源码及设计说明', source_sha='a'*64, cache_root=tmp_path)
    assert again['intake'] == 'keep' and len(agent.calls) == 1


@pytest.mark.parametrize('intake,route', [('exclude','none'), ('pending','pending'), ('keep','image')])
def test_material_decisions_remain_distinct(tmp_path,intake,route):
    m = intake_module()
    class Agent:
        config = {'text_model': 'saved'}
        def call(self,*args,**kwargs):
            return {'decision': 'pass', 'intake': intake, 'kind': '材料', 'route': route, 'reason': '按实际内容决定去向'}
    result = m.classify_material(Agent(), '材料实际内容', source_sha='b'*64, cache_root=tmp_path)
    assert result['intake'] == intake


def test_read_failure_never_becomes_nonpaper(tmp_path):
    m = intake_module()
    path=tmp_path/'drawing.dwg';path.write_bytes(b'AC1027\x00\x00binary')
    material=m.read_material(path,tmp_path/'work')
    assert material['status']=='pending'
    assert path.read_bytes()==b'AC1027\x00\x00binary'


def test_source_code_is_read_as_data_not_executed(tmp_path):
    m = intake_module()
    path=tmp_path/'project.py';path.write_text("raise RuntimeError('must not execute')",encoding='utf8')
    result=m.read_material(path,tmp_path/'work')
    assert result['status']=='readable' and 'must not execute' in result['text']


def test_existing_unknown_rating_stays_unknown():
    profile={'summary':{'decision':'pending'}, 'units':[{'core':{'value':True,'verification':'pending'},
        'assessments':[{'verification':'pending','decision':'pending'}]}]}
    assert p.admission(profile)['status']=='pending'


def test_invalid_method_blocks_dependent_interpretation_but_not_independent_structure():
    profile={'summary':{'decision':'disabled'},'units':[
        {'id':'bad','core':{'value':True,'verification':'verified'},'assessments':[
            {'aspect':'execution_steps','verification':'verified','decision':'disabled'}]},
        {'id':'dependent','assessments':[
            {'aspect':'result_interpretation','verification':'verified','decision':'recommended'}]},
        {'id':'independent','assessments':[
            {'aspect':'section_structure','verification':'verified','decision':'recommended'}]}],
        'relations':[{'from':['bad'],'to':['dependent'],'assessment':{'verification':'verified','decision':'recommended'}}]}
    assert p.admission(profile)['usable_aspects']==1
    assert p.admission(profile)['status']=='limited'
    assert p.learning_uses(profile)['dependency_restricted'][0]['unit_id']=='dependent'
    assert profile['units'][1]['assessments'][0]['decision']=='recommended'


def test_format_repair_does_not_regrade_valid_or_unknown_dimensions():
    report={'units':[{'assessments':[{'grades':{'a':{'grade':'合格','reason':'invalid format'},
        'b':{'grade':'sufficient','reason':'supported'},'c':{'grade':None,'reason':'unknown'}}}]}]}
    targets=p.repairable_scores(report)
    assert len(targets)==1
    fixed=p.apply_score_repairs(report,targets,{'ratings':[{'path':targets[0]['path'],'grade':None,'reason':'不能确认'}]})
    assert fixed['units'][0]['assessments'][0]['grades']['b']==report['units'][0]['assessments'][0]['grades']['b']
    assert fixed['units'][0]['assessments'][0]['grades']['c']==report['units'][0]['assessments'][0]['grades']['c']
    assert fixed['units'][0]['assessments'][0]['grades']['a']['grade'] is None


def test_front_material_business_decision_is_separate_from_execution_protocol():
    from agent_client import validate_result
    raw={'decision':'exclude','kind':'plagiarism_report','route':'none','reason':'辅助流程材料排除'}
    result=validate_result('material_intake',raw)
    assert result['decision']=='pass' and result['intake']=='exclude'
    assert raw['decision']=='exclude'


def test_legacy_admission_keeps_original_count_until_explicit_upgrade():
    profile={'summary':{'decision':'limited'},'units':[
        {'id':'bad','assessments':[{'aspect':'execution_steps','verification':'verified','decision':'disabled'}]},
        {'id':'dependent','assessments':[{'aspect':'result_interpretation','verification':'verified','decision':'recommended'}]},
        {'id':'independent','assessments':[{'aspect':'section_structure','verification':'verified','decision':'limited'}]}],
        'relations':[{'from':['bad'],'to':['dependent'],'assessment':{'verification':'verified','decision':'recommended'}}]}
    assert p.admission(profile,'verified-learning-uses/1')['usable_aspects']==2
    assert p.admission(profile,'verified-learning-uses/2')['usable_aspects']==1
    with pytest.raises(ValueError,match='policy_unknown'):p.admission(profile,'verified-learning-uses/v2-typo')

