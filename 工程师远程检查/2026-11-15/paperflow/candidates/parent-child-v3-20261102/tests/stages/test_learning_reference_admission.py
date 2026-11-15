"""Reference diagnostics must use the existing learning admission policy."""
import copy,json
from pathlib import Path
import pytest
from test_profile_completion_guard import complete_fixture,process,replace_response
from test_body_routing import fixture


@pytest.mark.parametrize('reading,located',[(True,False),(False,True)])
def test_neighbor_chapter_validated_against_whole_material_is_not_rejected_again(tmp_path,reading,located):
    from paperflow_v3.stages.body_adapter import _derive
    from preextract_contract import load_template,_blank
    from preextract_image_alias import AliasContext
    evidence,catalog=complete_fixture(tmp_path,parts=2)
    plan=json.loads(Path(evidence['plan']['path']).read_text(encoding='utf8'))
    bundle=json.loads(Path(evidence['reading_material']['path']).read_text(encoding='utf8'))
    verified=[]
    for index,row in enumerate(evidence['responses']):
        candidate=json.loads(Path(row['response']['path']).read_text(encoding='utf8'))
        subset=copy.deepcopy(plan['parts'][index]['bundle'])
        if index==0:
            neighbor=copy.deepcopy(bundle['blocks'][1]);neighbor['reading_role']='context'
            subset['blocks'].append(neighbor)
            # The native validator safely adds this supplied context block's
            # real chapter to its temporary index, never to frozen request bytes.
            if reading:candidate['source']['reading_scope']['read_refs'].append(neighbor['section_id'])
            if located:
                unit=candidate['profile']['learning_units'][0]
                ref=copy.deepcopy(unit['source_refs'][0])
                ref.update(block_id=neighbor['id'],section_id=neighbor['section_id'],start=None,end=None)
                unit['source_refs'].append(ref)
                gap=_blank(load_template()[1]['gaps'][0])
                gap.update(id='neighbor-note',type='uncertain',claim_status='uncertain',
                    target_pointer='/profile/learning_units/0/source_refs/1/section_id',
                    description='context chapter declaration',use_limit='原文位置需回看核实',source_refs=[])
                candidate['gaps'].append(gap)
        verified.append((json.dumps(candidate,ensure_ascii=False),subset))
    ranking=Path(evidence['whole_paper_ranking']['response']['path']).read_text(encoding='utf8')
    artifacts,_,_=_derive(plan,bundle,verified,AliasContext(),catalog,ranking_raw=ranking)
    assert artifacts['validation']['valid']
    assert len(artifacts['candidate']['profile']['learning_units'])==2
    assert artifacts['assembly']['learning_units_preserved']==2
    assert all(section['id']!='S002' for section in verified[0][1]['sections'])


def test_unlocated_reference_is_a_visible_limit_and_does_not_block_whole_profile(tmp_path):
    from preextract_contract import load_template,_blank
    evidence,catalog=fixture(tmp_path)
    row=evidence['responses'][0]
    def add_unlocated(candidate):
        unit=candidate['profile']['learning_units'][0]
        extra=copy.deepcopy(unit['source_refs'][0]);extra.update(block_id='not-supplied',section_id=None,start=None,end=None)
        unit['source_refs'].append(extra)
        gap=_blank(load_template()[1]['gaps'][0])
        gap.update(id='unlocated-note',type='uncertain',claim_status='uncertain',
            target_pointer='/profile/learning_units/0/source_refs/1/block_id',
            description='原文定位待核实',use_limit='此处来源位置待核实',source_refs=[])
        candidate['gaps'].append(gap)
        candidate['source']['reading_scope']['read_refs'].append('not-supplied-reading')
    raw=replace_response(row,add_unlocated)
    result=process(evidence,catalog,tmp_path/'processed')
    profile=json.loads((Path(result['package_path'])/'profile.json').read_text(encoding='utf8'))
    assert result['admission_policy']=='learning'
    assert result['validation']['body_exact_reassembly'] is True
    assert len(profile['learning_units'])==1
    assert any('定位' in gap.get('use_limit','') for gap in profile['gaps'])
    assert Path(row['response']['path']).read_text(encoding='utf8')==raw
