"""Actual native publication should retain unresolved ranking refs as learning limits."""
from pathlib import Path
import json
import pytest
from test_body_routing import fixture, reference
from test_body_adapter import FixtureCounter, RELEASE


@pytest.mark.parametrize('unresolved', ['U001', 'not-a-known-learning-unit'])
def test_global_ranking_reference_uses_existing_learning_admission(tmp_path, unresolved):
    from paperflow_v3.stages.body_adapter import process_body
    evidence, catalog = fixture(tmp_path, parts=2)
    ranking = {'routing_candidates':[{'code':'080901','name':'计算机科学与技术',
        'reason':'全文依据','unit_ids':[unresolved],'source_refs':[]}], 'limitations':[]}
    path=tmp_path/'whole-ranking/response.txt'
    path.parent.mkdir()
    raw=json.dumps(ranking,ensure_ascii=False)
    path.write_text(raw,encoding='utf8')
    evidence['whole_paper_ranking']={'response':reference(path),'coverage':['part-001','part-002']}
    result=process_body(evidence,tmp_path/'processed',expected_identity=evidence['identity'],
        catalog=catalog,paperflow_root=RELEASE/'core',counter=FixtureCounter())
    profile=json.loads((Path(result['package_path'])/'profile.json').read_text(encoding='utf8'))
    assert result['admission_policy']=='learning' and result['validation']['body_exact_reassembly'] is True
    assert len(profile['learning_units'])==2
    assert profile['routing_candidates'][0]['unit_ids']==[unresolved]
    assert result['assembly']['routing_candidates_original']==ranking['routing_candidates']
    validation=json.loads((tmp_path/'processed/private/validation.json').read_text(encoding='utf8'))
    assert validation['valid'] is True
    assert any(row['code']=='learning_limit_record_reference' and '/routing_candidates/' in row['path'] for row in validation['warnings'])
    assert any('提取记录有局部格式' in gap.get('use_limit','') for gap in profile['gaps'])
    assert path.read_text(encoding='utf8')==raw
