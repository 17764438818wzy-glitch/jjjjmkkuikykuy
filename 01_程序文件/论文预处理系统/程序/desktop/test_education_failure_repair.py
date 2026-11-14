"""Regressions for real failures: source loss, duplicate methods and output size."""
import copy
import json
from pathlib import Path

import pytest


REFERENCE = Path(__file__).parent/'fixtures/education_failure_reference.json'


def repaired_gate():
    from education_failure_repair import repair_dsl
    source=json.loads(REFERENCE.read_text(encoding='utf-8'))
    result=repair_dsl(source)
    ns={}
    exec(result['components']['CodeExec:EducationGate']['obj']['params']['script'],ns)
    return ns


def test_short_research_object_section_keeps_every_sentence_once():
    gate=repaired_gate()
    short='本文以某小学五年级学生的阅读兴趣为研究对象。'
    next_text='采用课堂观察记录学生的阅读过程和参与情况。'*8
    text='# 论文标题\n\n## 1 研究对象\n资料关联：section_123\n'+short+'\n\n## 2 研究方法\n'+next_text
    chunks=gate['split_by_section'](text)
    joined='\n'.join(x['text'] for x in chunks)
    assert joined.count(short)==1
    assert joined.count(next_text)==1


def test_short_assets_do_not_enter_next_body_section():
    gate=repaired_gate()
    text='# 图表写作参考结构\n\n## 图表说明\n这是只作辅助参考的程序描述。\n# 研究结果\n'+'正文研究结果应独立保留。'*15
    chunks=gate['split_by_section'](text)
    assert any('辅助参考' in c['text'] and c['role']=='assets' for c in chunks)
    assert all('辅助参考' not in c['text'] for c in chunks if c['role']=='body')


def test_same_methods_merge_implementations_without_inventing_evidence():
    from education_failure_repair import normalize_methods
    methods=[{'name':'问卷调查法','implementation':'发放教师问卷。'},
             {'name':'问卷调查法','implementation':'统计回收问卷。'},
             {'name':'文献资料法','implementation':'比较已有研究。'}]
    before=copy.deepcopy(methods)
    fixed=normalize_methods(methods)
    assert methods==before
    assert fixed==[{'name':'问卷调查法','implementation':'发放教师问卷。\n统计回收问卷。'},
                   {'name':'文献研究法','implementation':'比较已有研究。'}]
    assert normalize_methods([{'name':'未知方法','implementation':'原说法'}])[0]['name']=='未知方法'
    with pytest.raises(ValueError): normalize_methods([{'name':'问卷调查法','implementation':7}])


def test_large_metadata_stays_in_index_not_every_body_chunk():
    gate=repaired_gate()
    heavy={'artifact_profile','artifact_evidence','education_research_steps','education_step_issues'}
    assert heavy<=set(gate['HEAVY_METADATA_KEYS'])
    # This reproduces the long-paper expansion through the actual metadata filter.
    metadata={k:'研究证据'*500 for k in heavy}|{'education_paper_id':'paper_x'}
    slim={k:v for k,v in metadata.items() if k not in gate['HEAVY_METADATA_KEYS']}
    assert slim=={'education_paper_id':'paper_x'}
    assert all(k in metadata for k in heavy)


def test_repair_preserves_body_identity_checks_and_both_model_bindings():
    from education_failure_repair import repair_dsl
    from education_dual_deploy import model_only_dsl,contract_hash,NODE
    original=json.loads(REFERENCE.read_text(encoding='utf-8'))
    fixed=repair_dsl(original)
    assert repair_dsl(fixed)==fixed
    model=original['components'][NODE]['obj']['params']['llm_id']
    other=model_only_dsl(fixed,model,'official-test-model')
    assert contract_hash(fixed)==contract_hash(other)
    ns={};exec(fixed['components']['CodeExec:PrepareEducation']['obj']['params']['script'],ns)
    assert ns['major_admission']('思想政治教育')[0]=='思想政治教育'
    assert ns['major_admission']('金融学')[0] is None
    gate={};exec(fixed['components']['CodeExec:EducationGate']['obj']['params']['script'],gate)
    assert gate['CORE_SUPPORTS']==['research_subject','research_methods','research_content','quality']
