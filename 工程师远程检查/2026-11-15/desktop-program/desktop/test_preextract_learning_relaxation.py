"""Only traceable learning and genuine identity/publication integrity gate admission."""
import copy
import pytest

from preextract_contract import validate_candidate
from test_preextract_contract import fixture
from test_preextract_workbench_flow import flow


def validate(data, bundle, catalog):
    return validate_candidate(data, bundle, catalog, redaction_policy='best_effort', admission_policy='learning')


def test_identical_duplicate_json_value_is_lossless_with_audited_location():
    from preextract_contract import parse_candidate
    repairs=[]
    result=parse_candidate('{"assets":{"items":[{"source_image_id":"image_0007","source_image_id":"image_0007"}]}}',repairs=repairs)
    assert result=={'assets':{'items':[{'source_image_id':'image_0007'}]}}
    assert any(r.get('policy')=='collapse_identical_duplicate_key' and r.get('path')=='/assets/items/0/source_image_id'
               and r.get('conflicting_values_overwritten') is False for r in repairs)


@pytest.mark.parametrize('left,right',[('"image_0007"','"image_0008"'),('true','1'),('1','1.0')])
def test_duplicate_conflicting_values_or_different_json_types_are_never_selected(left,right):
    from preextract_contract import parse_candidate
    with pytest.raises(ValueError,match='duplicate JSON key'):
        parse_candidate('{"source_image_id":'+left+',"source_image_id":'+right+'}')


@pytest.mark.parametrize('ref', [
    'PDF pages 2-3 (no page-image asset provided)',
    'PDF pages 2-3 的页图未提供',
    'page_0002-page_0003（未提供页图）',
    'page_0002',
])
def test_unprovided_page_images_are_limits_and_never_claimed_read(ref):
    data, bundle, catalog = fixture()
    bundle['blocks'][0]['position'] = {'page': 1}
    bundle['blocks'].extend(dict(id=f'b{p}', text=f'第{p}页正文', section_id='s1', position={'page': p}) for p in (2, 3))
    data['source']['reading_scope']['unread_refs'] = [ref]
    before = copy.deepcopy(data)
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    reading = result['candidate']['source']['reading_scope']
    assert reading['unread_refs'] == [] and not any(v.startswith('page_') for v in reading['read_refs'])
    assert ref in reading['notes']
    assert any(ref in g.get('use_limit', '') for g in result['candidate']['gaps'])
    assert result['candidate']['profile']['learning_units'] == before['profile']['learning_units']
    assert data == before
    again = validate(result['candidate'], bundle, catalog)
    assert again['valid'] and again['candidate'] == result['candidate']


@pytest.mark.parametrize('ref', ['text:all_blocks', 'main:text:all_blocks'])
def test_all_text_alias_expands_only_actual_blocks(ref):
    data, bundle, catalog = fixture()
    bundle['file_ref'] = 'main'
    data['source']['reading_scope']['read_refs'] = [ref]
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    assert result['candidate']['source']['reading_scope']['read_refs'] == ['b1']


@pytest.mark.parametrize('value', [[], {}, None, '', '补充说明原文'])
def test_extra_fields_never_reject_otherwise_usable_learning(value):
    data, bundle, catalog = fixture()
    data['tree']['nodes'][0]['source_refs_extra'] = value
    before = copy.deepcopy(data)
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    assert result['candidate']['tree']['nodes'][0]['source_refs_extra'] == value
    assert result['candidate']['profile']['learning_units'] == data['profile']['learning_units']
    assert data == before


@pytest.mark.parametrize('key', ['tree', 'assets', 'redaction_plan', 'gaps', 'segmentation'])
def test_optional_sections_are_not_admission_requirements(key):
    data, bundle, catalog = fixture()
    del data[key]
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']


@pytest.mark.parametrize('fault', ['material_type', 'title', 'reason', 'exclude', 'uncertain_enum'])
def test_auxiliary_metadata_never_rejects_a_traceable_learning_chain(fault):
    data, bundle, catalog = fixture()
    if fault == 'material_type':data['profile']['material_type'] = None
    elif fault == 'title':data['profile']['article']['title'] = None
    elif fault == 'reason':data['profile']['material_disposition']['reason'] = None
    elif fault == 'exclude':data['profile']['material_disposition']['decision'] = 'exclude'
    else:data['profile']['learning_units'][0]['work_attribution'] = '作者可能采用'
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    assert result['admission']['usable_unit_ids'] == ['U1']
    if fault == 'uncertain_enum':
        assert result['candidate']['profile']['learning_units'][0]['work_attribution'] == 'uncertain'


def test_imprecise_offsets_retain_actual_paragraph_and_disclose_precision_loss():
    data, bundle, catalog = fixture()
    ref=data['profile']['learning_units'][0]['source_refs'][0]
    ref.update(start=400, end=500)
    result=validate(data, bundle, catalog)
    assert result['valid'],result['errors']
    kept=result['candidate']['profile']['learning_units'][0]['source_refs'][0]
    assert kept['block_id']=='b1' and kept['start'] is None and kept['end'] is None
    assert any(g.get('use_limit') for g in result['candidate']['gaps'])


def test_empty_omitted_table_is_not_a_text_evidence_requirement():
    data,bundle,catalog=fixture()
    bundle['assets']=[dict(id='a1',kind='table',text='')]
    data['assets']['items']=[dict(id='a1',kind='table',storage='omit',evidence_basis='table_text',meaning='空表')]
    result=validate(data,bundle,catalog)
    assert result['valid'],result['errors']
    assert result['candidate']['assets']['items'][0]['evidence_basis'] is None


def test_unselected_asset_and_missing_storage_do_not_reject_learning():
    data,bundle,catalog=fixture()
    bundle['assets']=[dict(id='a1',kind='table',text='数据一 10'),dict(id='a2',kind='table',text='数据二 20')]
    data['assets']['items']=[dict(id='a1',kind='table',storage=None,meaning='记录数据')]
    result=validate(data,bundle,catalog)
    assert result['valid'],result['errors']
    assert result['candidate']['assets']['items'][0]['storage']=='description'


def test_bad_gap_pointer_becomes_general_limit_without_losing_explanation():
    data,bundle,catalog=fixture()
    data['gaps']=[dict(id='G1',type='uncertain',target_pointer='/missing/field',description='定位不精确',use_limit='不要照搬参数')]
    result=validate(data,bundle,catalog)
    assert result['valid'],result['errors']
    row=next(g for g in result['candidate']['gaps'] if g['id']=='G1')
    assert row['target_pointer'] is None and '不要照搬参数' in row['use_limit']
    assert '/missing/field' in row['use_limit']


def test_related_specialties_and_arbitrary_count_do_not_block_real_targets():
    data,bundle,catalog=fixture()
    template=data['profile']['routing_candidates'][0]
    catalog['majors']=[dict(code=str(800001+i),name=f'专业{i}') for i in range(4)]
    data['profile']['routing_candidates']=[dict(template,code=row['code'],name=row['name']) for row in catalog['majors']]
    data['profile']['additional_related_specialties']=[dict(template,code='not-in-catalog',name='原始相关说明')]
    result=validate(data,bundle,catalog)
    assert result['valid'],result['errors']
    assert len(result['candidate']['profile']['routing_candidates'])==4
    assert result['candidate']['profile']['additional_related_specialties']==[]
    assert any('原始相关说明' in g.get('use_limit','') for g in result['candidate']['gaps'])


@pytest.mark.parametrize('fault',['no_steps','unread_only_source','source_hash','wrong_target','only_missing_paragraph'])
def test_three_required_gates_still_reject_unusable_or_misbound_content(fault):
    data,bundle,catalog=fixture()
    if fault=='no_steps':data['profile']['learning_units'][0]['steps']=[]
    elif fault=='unread_only_source':data['source']['reading_scope']['unread_refs']=['b1']
    elif fault=='source_hash':data['source']['source_sha256']='b'*64
    elif fault=='wrong_target':data['profile']['routing_candidates'][0]['code']='not-real'
    else:data['profile']['learning_units'][0]['source_refs'][0].update(block_id='not-real',section_id=None)
    result=validate(data,bundle,catalog)
    assert not result['valid']


def test_explicit_unread_whitespace_cannot_bypass_the_learning_gate():
    data,bundle,catalog=fixture()
    data['source']['reading_scope']['unread_refs']=[' b1 ']
    assert not validate(data,bundle,catalog)['valid']


def test_empty_asset_cannot_be_the_only_learning_evidence():
    data,bundle,catalog=fixture()
    bundle['assets']=[dict(id='a1',kind='table',text='')]
    unit=data['profile']['learning_units'][0]
    unit['source_refs']=[dict(asset_id='a1')]
    assert not validate(data,bundle,catalog)['valid']


def test_normalization_does_not_erase_a_real_source_identity_conflict():
    data,bundle,catalog=fixture()
    data['source']['source_sha256']='b'*64
    first=validate(data,bundle,catalog)
    assert not first['valid']
    assert not validate(first['candidate'],bundle,catalog)['valid']


def test_empty_learning_ids_are_program_identifiers_not_content_requirements():
    data,bundle,catalog=fixture()
    data['profile']['learning_units'][0]['id']=None
    first=validate(data,bundle,catalog)
    assert first['valid']
    assert isinstance(first['admission']['usable_unit_ids'][0],str)
    again=validate(first['candidate'],bundle,catalog)
    assert again['valid'] and first['candidate']==again['candidate']


def test_nonstandard_explanation_type_keeps_its_complete_text():
    data,bundle,catalog=fixture()
    data['profile']['learning_units'][0]['interpretation']={'result':'完整解释不可丢'}
    first=validate(data,bundle,catalog)
    assert first['valid']
    assert '完整解释不可丢' in first['candidate']['profile']['learning_units'][0]['interpretation']


@pytest.mark.parametrize('fault',['exclude','extras','optional_group','empty_id','duplicate_unit'])
def test_relaxed_admission_reaches_real_sealed_publication(flow,fault):
    from pathlib import Path
    processor,opener,sources=flow
    def transform(c):
        if fault=='exclude':c['profile']['material_disposition']['decision']='exclude'
        elif fault=='extras':
            c['profile']['extra_method']='完整补充方法不可丢'
            c['root_note']='根字段补充不可丢'
            c['profile']['learning_units'][0]['interpretation']={'result':'完整解释不可丢'}
        elif fault=='optional_group':c['segmentation']['semantic_groups']=[dict(id='S1',source_refs=[dict(block_id='missing')])]
        elif fault=='empty_id':c['profile']['learning_units'][0]['id']=None
        else:c['profile']['learning_units'].append(copy.deepcopy(c['profile']['learning_units'][0]))
    opener.transform=transform
    manifest=processor.run(sources[:1]);item=manifest['items'][0]
    assert item['status']=='pass',item.get('reason')
    assert len(opener.calls)==1
    root=Path(item['package_path'])
    assert (root/'manifest.json').is_file()
    if fault=='extras':
        published=(root/'profile.json').read_text(encoding='utf8')
        for phrase in ['完整补充方法不可丢','根字段补充不可丢','完整解释不可丢']:assert phrase in published

