"""A retained source is not a successfully validated whole-paper profile."""
import copy
import hashlib
import json
from pathlib import Path
import pytest
from test_body_routing import fixture,reference,save,FixtureCounter,RELEASE


def complete_fixture(tmp_path,parts=1):
    evidence,catalog=fixture(tmp_path,parts=parts)
    if parts>1:
        rank=save(tmp_path/'whole-paper-ranking.json',{'routing_candidates':[{
            'code':'080901','name':'计算机科学与技术','reason':'已保存的整篇排序',
            'unit_ids':['P001_U001','P002_U001'],'source_refs':[]}],'limitations':[]})
        evidence['whole_paper_ranking']={'response':rank,'coverage':['part-001','part-002']}
    return evidence,catalog


def replace_response(row,mutate):
    path=Path(row['response']['path'])
    value=json.loads(path.read_text(encoding='utf-8'))
    mutate(value)
    raw=json.dumps(value,ensure_ascii=False)
    path.write_text(raw,encoding='utf-8');row['response']=reference(path)
    state_path=Path(row['state']['path']);state=json.loads(state_path.read_text(encoding='utf-8'))
    state['response_sha256']=row['response']['sha256'];row['state']=save(state_path,state)
    row['events']=save(Path(row['events']['path']),{'choices':[{'message':{'content':raw},'finish_reason':'stop'}]})
    return raw


def process(evidence,catalog,output):
    from paperflow_v3.stages.body_adapter import process_body
    return process_body(evidence,output,expected_identity=evidence['identity'],catalog=catalog,
                        paperflow_root=RELEASE/'core',counter=FixtureCounter())


def test_same_paper_different_material_descriptions_merge_without_losing_units(tmp_path):
    evidence,catalog=complete_fixture(tmp_path,parts=2)
    labels=['论文正文（案例分析型）','论文正文（局部）']
    raw=[replace_response(row,lambda c,label=label:c['profile'].update(material_type=label))
         for row,label in zip(evidence['responses'],labels)]
    result=process(evidence,catalog,tmp_path/'processed')
    assert result['admission_policy']=='learning'
    profile=json.loads((Path(result['package_path'])/'profile.json').read_text(encoding='utf-8'))
    assert len(profile['learning_units'])==2
    assert all(label in profile['material_type'] for label in labels)
    assert [Path(r['response']['path']).read_text(encoding='utf-8') for r in evidence['responses']]==raw


def test_new_invalid_structure_blocks_completion_and_keeps_original_report(tmp_path):
    from paperflow_v3.stages.body_adapter import BodyValidationError
    evidence,catalog=fixture(tmp_path)
    row=evidence['responses'][0]
    raw=replace_response(row,lambda c:c['profile'].update(learning_units=[]))
    output=tmp_path/'processed'
    with pytest.raises(BodyValidationError,match='candidate_validation_failed') as raised:
        process(evidence,catalog,output)
    diagnostic=raised.value.diagnostic
    report=Path(diagnostic['path'])
    assert hashlib.sha256(report.read_bytes()).hexdigest()==diagnostic['sha256']
    assert json.loads(report.read_text(encoding='utf-8'))['valid'] is False
    assert not (output/'body-result.json').exists()
    assert not (output/'publication').exists()
    assert Path(row['response']['path']).read_text(encoding='utf-8')==raw


def test_same_source_title_variants_do_not_block_completion(tmp_path):
    evidence,catalog=complete_fixture(tmp_path,parts=2)
    titles=['研究题名','Research title']
    for row,title in zip(evidence['responses'],titles):
        replace_response(row,lambda c,title=title:c['profile']['article'].update(title=title))
    result=process(evidence,catalog,tmp_path/'processed')
    profile=json.loads((Path(result['package_path'])/'profile.json').read_text(encoding='utf8'))
    assert result['admission_policy']=='learning'
    assert result['validation']['body_exact_reassembly'] is True
    assert len(profile['learning_units'])==2
    assert all(title in profile['article']['title'] for title in titles)


def test_existing_sealed_retention_package_remains_reusable(tmp_path,monkeypatch):
    from paperflow_v3.stages import body_adapter as m
    from paperflow_v3.stages.raw_publication import raw_publication
    evidence,catalog=fixture(tmp_path)
    plan,bundle,verified,aliases=m._project_inputs(evidence,m._bound_inputs(evidence,evidence['identity']))
    output=tmp_path/'processed';output.mkdir()
    fingerprint=m._digest(dict(evidence=evidence,catalog=catalog,paperflow_root=str((RELEASE/'core').resolve())))
    m._save(output/'input-identity.json',{'identity':evidence['identity'],'input_fingerprint':fingerprint})
    original=raw_publication(evidence,output,identity=evidence['identity'],fingerprint=fingerprint,
                             bundle=bundle,catalog=catalog,counter=FixtureCounter())
    before={str(p.relative_to(output)):hashlib.sha256(p.read_bytes()).hexdigest() for p in output.rglob('*') if p.is_file()}
    def unavailable(*args):raise ValueError('historical_projection_not_available')
    monkeypatch.setattr(m,'_project_inputs',unavailable)
    assert process(evidence,catalog,output)==original
    assert before=={str(p.relative_to(output)):hashlib.sha256(p.read_bytes()).hexdigest() for p in output.rglob('*') if p.is_file()}
