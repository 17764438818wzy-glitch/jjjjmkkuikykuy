import json
from pathlib import Path

import pytest

from education_pipeline_builder import ANNOTATION_PROMPT,build_dsl,build_template_groups


REFERENCE=Path(__file__).parent/'data/private/education_pilot/finance_pipeline_reference.json'
TRIAL=Path(__file__).parent/'data/private/education_pilot/canonical_packages'


def test_education_templates_have_actual_steps_and_four_level_wiki():
    assert '俱乐部发展为主' in ANNOTATION_PROMPT
    assert '赛事技战术分析' in ANNOTATION_PROMPT
    root=Path(__file__).parent/'data/private/education_pilot'
    structure=json.loads((root/'structure_template_reference.json').read_text(encoding='utf-8'))
    wiki=json.loads((root/'wiki_template_reference.json').read_text(encoding='utf-8'))
    file_group,wiki_group=build_template_groups(structure,wiki)
    graph=next(t['config'] for t in file_group['templates'] if t['kind']=='knowledge_graph')
    assert {f['type'] for f in graph['entity']['fields']}=={
        'paper_type','research_path','education_context','research_step','material','analysis_element'}
    assert {f['type'] for f in graph['relation']['fields']}=={
        'uses_path','has_step','uses_material','measures','precedes','applies_to'}
    graph_fields={f['type']:f for f in graph['entity']['fields']}
    assert '每篇最多一个' in graph_fields['paper_type']['rule']
    assert '禁止使用论文标题' in graph_fields['paper_type']['rule']
    assert '各章节只是该路径上的步骤' in graph_fields['research_path']['rule']
    assert '问卷本身' in graph_fields['research_step']['rule']
    assert all('财务' not in json.dumps(group,ensure_ascii=False) for group in (file_group,wiki_group))
    instruction=wiki_group['templates'][0]['config']['instruction']
    assert all(f'L{i}' in instruction for i in range(1,5))


@pytest.mark.skipif(not REFERENCE.exists(),reason='requires private RAGFlow reference snapshot')
def test_education_pipeline_uses_stable_identity_and_new_templates():
    finance=json.loads(REFERENCE.read_text(encoding='utf-8'))['dsl']
    dsl=build_dsl(finance,'a'*32,'b'*32)
    assert len(dsl['components'])==len(finance['components'])
    assert set(dsl['components']).isdisjoint({'CodeExec:PrepareAccounting','CodeExec:AccountingGate'})
    assert dsl['components']['Compiler:PaperStructure']['obj']['params']['compilation_template_group_id']=='a'*32
    assert dsl['components']['Compiler:PaperWiki']['obj']['params']['compilation_template_group_id']=='b'*32
    assert next(n for n in dsl['graph']['nodes'] if n['id']=='Compiler:PaperWiki')['data']['name']=='Wiki 模板资格；页面由知识库任务生成'
    for name in ('CodeExec:PrepareEducation','CodeExec:EducationGate'):
        node=next(n for n in dsl['graph']['nodes'] if n['id']==name)
        assert node['data']['form']['script']==dsl['components'][name]['obj']['params']['script']
    prepare=dsl['components']['CodeExec:PrepareEducation']['obj']['params']['script']
    namespace={}
    exec(prepare,namespace)
    for path in TRIAL.iterdir():
        if not (path/'ingest.md').exists():continue
        text=(path/'ingest.md').read_text(encoding='utf-8')
        output=namespace['main']([{'text':text}],filename=path.name+'.md')
        manifest=json.loads((path/'manifest.json').read_text(encoding='utf-8'))
        assert output[0]['paper_id']==manifest['paper_id']
        assert '<EDUCATION_SOURCE>' in output[0]['text']


@pytest.mark.skipif(not REFERENCE.exists(),reason='requires private RAGFlow reference snapshot')
def test_education_prepare_rejects_legacy_package_without_stable_id():
    finance=json.loads(REFERENCE.read_text(encoding='utf-8'))['dsl']
    script=build_dsl(finance,'a'*32,'b'*32)['components']['CodeExec:PrepareEducation']['obj']['params']['script']
    ns={};exec(script,ns)
    path=next(p for p in TRIAL.iterdir() if (p/'ingest.md').exists())
    text=(path/'ingest.md').read_text(encoding='utf-8')
    text=text.replace('"stable_paper_id"','"removed_paper_id"',1)
    with pytest.raises(RuntimeError,match='稳定论文编号'):
        ns['main']([{'text':text}],filename=path.name+'.md')


@pytest.mark.skipif(not REFERENCE.exists(),reason='requires private RAGFlow reference snapshot')
def test_education_prepare_requires_canonical_paper_id_but_separate_document_id(tmp_path):
    from evidence import export_package
    from test_knowledge_package import sample
    meta=sample(tmp_path)
    meta['major']='小学教育'
    meta['classification']={'major':'小学教育','method':'original_rules','source':'front_matter','confidence':'high'}
    export_package(tmp_path,meta,canonical_paper_id=True)
    manifest=json.loads((tmp_path/'manifest.json').read_text(encoding='utf-8'))
    text=(tmp_path/'ingest.md').read_text(encoding='utf-8')
    finance=json.loads(REFERENCE.read_text(encoding='utf-8'))['dsl']
    script=build_dsl(finance,'a'*32,'b'*32)['components']['CodeExec:PrepareEducation']['obj']['params']['script']
    ns={};exec(script,ns)
    result=ns['main']([{'text':text}],filename='AUTO_test.md')[0]
    assert result['paper_id']==manifest['paper_id']
    start='[PAPER_PIPELINE_METADATA_V1]';end='[/PAPER_PIPELINE_METADATA_V1]'
    before,rest=text.split(start,1);encoded,after=rest.split(end,1)
    handoff=json.loads(encoded);handoff['paper_id']=handoff['document_id']
    legacy=before+start+json.dumps(handoff,ensure_ascii=False)+end+after
    with pytest.raises(RuntimeError,match='清单|稳定论文编号|交接'):
        ns['main']([{'text':legacy}],filename='AUTO_test.md')


@pytest.mark.skipif(not REFERENCE.exists(),reason='requires private RAGFlow reference snapshot')
def test_education_gate_keeps_out_of_scope_decision_separate_from_technical_failure():
    finance=json.loads(REFERENCE.read_text(encoding='utf-8'))['dsl']
    components=build_dsl(finance,'a'*32,'b'*32)['components']
    prep={};exec(components['CodeExec:PrepareEducation']['obj']['params']['script'],prep)
    gate={};exec(components['CodeExec:EducationGate']['obj']['params']['script'],gate)
    path=TRIAL/'AUTO_866b8e0c927f431eb5c2'
    text=(path/'ingest.md').read_text(encoding='utf-8')
    info=prep['main']([{'text':text}],filename=path.name+'.md')[0]
    body='\n'.join(b['text'] for b in gate['split_by_section'](gate['source_for_index'](text)) if b['role']=='body')
    quote=next(line.strip()[:60] for line in body.splitlines() if len(line.strip())>=30)
    record={
        'paper_id':info['paper_id'],'coverage_token':info['coverage_token'],
        'research_subject':{'name':'','scope':''},
        'education_context':{'stage':'','subject':'','setting':''},
        'research_methods':[],'research_steps':[],
        'research_content':{'question':'','analysis':[],'findings':[]},
        'research_directions':[],'audience_tags':[],
        'quality':{'decision':'剔除','reason':'缺少教育研究问题','issues':[]},
        'judgement_basis':'正文仅为竞赛内容，未出现教育实施或研究问题。',
        'uncertainty_reason':'',
        'evidence':[{'quote':quote,'supports':['quality']}],
        'artifact_profile':{'paper_type':'范围外','research_path':'不适用',
                            'required_materials':[],'example_tier':'',
                            'tier_basis':'无教育研究路径','evidence_quotes':[quote]},
    }
    with pytest.raises(RuntimeError,match='EDUCATION_REJECTED'):
        gate['main']([{'text':info['text'],'summary':json.dumps(record,ensure_ascii=False)}],
                     [{'text':text}],[info])


@pytest.mark.skipif(not REFERENCE.exists(),reason='requires private RAGFlow reference snapshot')
def test_education_gate_requires_source_quote_for_completed_step():
    finance=json.loads(REFERENCE.read_text(encoding='utf-8'))['dsl']
    components=build_dsl(finance,'a'*32,'b'*32)['components']
    prep={};exec(components['CodeExec:PrepareEducation']['obj']['params']['script'],prep)
    gate={};exec(components['CodeExec:EducationGate']['obj']['params']['script'],gate)
    path=TRIAL/'AUTO_62cf4e1128e349d8a42a'
    text=(path/'ingest.md').read_text(encoding='utf-8')
    info=prep['main']([{'text':text}],filename=path.name+'.md')[0]
    body='\n'.join(b['text'] for b in gate['split_by_section'](gate['source_for_index'](text)) if b['role']=='body')
    quote=next(line.strip()[:60] for line in body.splitlines() if len(line.strip())>=30)
    record={
        'paper_id':info['paper_id'],'coverage_token':info['coverage_token'],
        'research_subject':{'name':'中学体育教学','scope':'课堂教学'},
        'education_context':{'stage':'初中','subject':'体育','setting':'课堂'},
        'research_methods':[{'name':'问卷调查法','implementation':'调查课堂体育游戏应用'}],
        'research_steps':[{'name':'现状调查','status':'实际执行','materials':['问卷'],
                           'analysis_elements':['应用情况'],'quote':'这是论文中没有的伪造引文'}],
        'research_content':{'question':'体育游戏如何应用','analysis':['应用情况分析'],'findings':['应用存在差异']},
        'research_directions':['教育调查'],'audience_tags':['普通本科','函授'],
        'quality':{'decision':'入库','reason':'有教育研究问题与实际调查','issues':[]},
        'judgement_basis':'以课堂应用情况为依据。','uncertainty_reason':'',
        'evidence':[{'quote':quote,'supports':['research_subject','research_methods','research_content','quality']}],
        'artifact_profile':{'paper_type':'体育游戏教学现状调查','research_path':'调查并分析课堂应用情况',
                            'required_materials':['问卷'],'example_tier':'普通本科',
                            'tier_basis':'完成调查分析','evidence_quotes':[quote]},
    }
    with pytest.raises(RuntimeError,match='没有可证实的实际研究步骤'):
        gate['main']([{'text':info['text'],'summary':json.dumps(record,ensure_ascii=False)}],
                     [{'text':text}],[info])
    record['research_steps'][0]['quote']=quote
    output=gate['main']([{'text':info['text'],'summary':json.dumps(record,ensure_ascii=False)}],
                        [{'text':text}],[info])
    assert any(c['extra']['education_chunk_role']=='body' for c in output)
    assert any(c['extra']['education_chunk_role']=='index' for c in output)
    record['research_steps'][0]['quote']='随机发放问卷,教师20份,回收20份,有效问卷20份,回收率100%。'
    checked=gate['validate'](json.dumps(record,ensure_ascii=False),text,info)
    assert checked['research_steps'][0]['quote']=='随机发放问卷，教师20份，回收20份，有效问卷20份，回收率100%。'
    record['research_steps'].append({'name':'无法核实的访谈','status':'实际执行','materials':['访谈记录'],
                                     'analysis_elements':[],'quote':'这是论文中没有的伪造引文'})
    checked=gate['validate'](json.dumps(record,ensure_ascii=False),text,info)
    assert [step['status'] for step in checked['research_steps']]==['实际执行','不明']
    assert checked['education_step_issues'][0]['name']=='无法核实的访谈'
