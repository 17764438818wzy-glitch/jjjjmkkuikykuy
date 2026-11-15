"""Method-reuse admission keeps usable content and exposes bounded limitations."""
import copy
import json
from pathlib import Path

import pytest

from preextract_contract import load_template, validate_candidate
from test_preextract_contract import empty, fixture


def validate(data, bundle, catalog):
    return validate_candidate(data, bundle, catalog, redaction_policy='best_effort',
                              admission_policy='learning')


@pytest.mark.parametrize('suffix', ['T', 'K', 'TK'])
def test_missing_specialty_suffix_requires_exact_name_and_unique_catalog_match(suffix):
    data, bundle, catalog = fixture()
    route = data['profile']['routing_candidates'][0]
    route.update(code='130212', name='音乐教育')
    catalog['majors'] = [dict(code='130212' + suffix, name='音乐教育')]
    original = copy.deepcopy(data)
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    assert result['candidate']['profile']['routing_candidates'][0]['code'] == '130212' + suffix
    assert result['candidate']['profile']['learning_units'] == original['profile']['learning_units']
    assert data == original
    assert any(row['code'] == 'normalized_specialty_suffix' for row in result['compatibility_report'])
    assert not validate_candidate(data, bundle, catalog)['valid']
    route['name'] = '音乐表演'
    assert not validate(data, bundle, catalog)['valid']
    route['name'] = '音乐教育'
    catalog['majors'].append(dict(code='130212X', name='音乐教育'))
    assert not validate(data, bundle, catalog)['valid']


def test_primary_text_alias_and_supplied_catalog_keep_all_learning_content():
    data, bundle, catalog = fixture()
    bundle['source'].update(text_file=r'C:\private\body.txt', catalog_sha256='c' * 64)
    data['source']['text_file'] = 'main'
    data['source']['reading_scope']['read_refs'] = ['main', 'catalog']
    bundle['file_ref'] = 'main'
    original = copy.deepcopy(data)
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    assert result['candidate']['source']['text_file'] == bundle['source']['text_file']
    assert result['candidate']['profile']['learning_units'] == original['profile']['learning_units']
    assert data == original
    again = validate(result['candidate'], bundle, catalog)
    assert again['valid'] and again['candidate'] == result['candidate']
    bad = copy.deepcopy(data)
    bad['source']['text_file'] = 'unrelated.txt'
    assert not validate(bad, bundle, catalog)['valid']
    bundle['source']['catalog_sha256'] = None
    assert not validate(data, bundle, catalog)['valid']


def test_step_output_can_reference_its_existing_material_without_rewriting_it():
    data, bundle, catalog = fixture()
    unit = data['profile']['learning_units'][0]
    unit['steps'][0]['output_refs'] = ['M1']
    original = copy.deepcopy(data)
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    assert result['candidate']['profile']['learning_units'] == original['profile']['learning_units']
    assert any(r['code'] == 'compatible_material_output' for r in result['compatibility_report'])
    assert data == original
    strict = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
    assert not strict['valid']
    unit['steps'][0]['output_refs'] = ['absent_material']
    assert not validate(data, bundle, catalog)['valid']


def test_unread_embedded_object_uses_exact_program_location_and_keeps_limit():
    data, bundle, catalog = fixture()
    position = dict(part='word/document.xml', body_index=8, object_ref='object_0002')
    bundle['blocks'].append(dict(id='b2', text='[解析说明：此位置的原文图形需定向识别。]',
                                 section_id='s1', position=position))
    bundle['issues'] = [dict(code='embedded_object_review', position=copy.deepcopy(position))]
    data['source']['reading_scope']['unread_refs'] = ['object_0002']
    original = copy.deepcopy(data)
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    assert result['candidate']['source']['reading_scope']['unread_refs'] == ['b2']
    assert result['candidate']['profile']['learning_units'] == original['profile']['learning_units']
    assert any(g.get('target_pointer') == '/source/reading_scope/unread_refs'
               and g.get('use_limit') for g in result['candidate']['gaps'])
    assert data == original
    again = validate(result['candidate'], bundle, catalog)
    assert again['valid'] and again['candidate'] == result['candidate']
    bundle['blocks'].append(dict(id='b3', text='另一个对象', section_id='s1',
                                 position=dict(position, part='word/header1.xml')))
    assert not validate(data, bundle, catalog)['valid']


@pytest.mark.parametrize('refs', [['main:text'], ['blocks:*'], ['all_text_blocks'], ['b1..b2']])
def test_exact_reading_group_shorthand_keeps_the_complete_learning_units(refs):
    data, bundle, catalog = fixture()
    bundle['file_ref'] = 'main'
    bundle['blocks'].append(dict(id='b2', text='完整补充正文', section_id='s1'))
    data['source']['reading_scope']['read_refs'] = refs
    original = copy.deepcopy(data)
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    assert result['candidate']['source']['reading_scope']['read_refs'] == ['b1', 'b2']
    assert result['candidate']['profile']['learning_units'] == original['profile']['learning_units']
    assert data == original
    again = validate(result['candidate'], bundle, catalog)
    assert again['valid'] and again['candidate'] == result['candidate']


@pytest.mark.parametrize('ref', ['unknown:*', 'b2..b1', 'b1..missing'])
def test_unknown_or_reversed_reading_range_is_still_rejected(ref):
    data, bundle, catalog = fixture()
    bundle['blocks'].append(dict(id='b2', text='补充正文', section_id='s1'))
    data['source']['reading_scope']['read_refs'] = [ref]
    result = validate(data, bundle, catalog)
    assert not result['valid']
    assert 'source_reference' in {row['code'] for row in result['errors']}


@pytest.mark.parametrize('fault', [None, 'direct_read', 'real_text', 'learning_reference', 'real_asset'])
def test_broad_read_of_placeholder_never_overrides_an_explicitly_unread_object(fault):
    data, bundle, catalog = fixture()
    position = dict(part='word/document.xml', body_index=8, object_ref='object_0002')
    bundle['blocks'].append(dict(id='b2', type='paragraph',
        text='[解析说明：此位置的原文图形需定向识别。]', section_id='s1', position=position))
    bundle['issues'] = [dict(code='embedded_object_review', position=copy.deepcopy(position))]
    data['source']['reading_scope'].update(read_refs=['all_text_blocks'], unread_refs=['object_0002'])
    if fault == 'direct_read': data['source']['reading_scope']['read_refs'].append('b2')
    if fault == 'real_text': bundle['blocks'][-1]['text'] = '实际研究结论。'
    if fault == 'learning_reference':
        data['profile']['learning_units'][0]['steps'][0]['source_refs'].append(dict(block_id='b2'))
    if fault == 'real_asset':
        bundle['assets'].append(dict(id='im1', kind='image', position=copy.deepcopy(position)))
        data['assets']['items'].append(dict(id='im1', kind='image', storage='omit'))
    original = copy.deepcopy(data)
    result = validate(data, bundle, catalog)
    if fault is not None:
        assert not result['valid']
        assert 'reading_conflict' in {row['code'] for row in result['errors']}
        return
    assert result['valid'], result['errors']
    scope = result['candidate']['source']['reading_scope']
    assert scope['read_refs'] == ['b1'] and scope['unread_refs'] == ['b2']
    assert 'object_0002' in scope['notes']
    assert result['candidate']['profile']['learning_units'] == original['profile']['learning_units']
    assert any(g.get('target_pointer') == '/source/reading_scope/unread_refs' and g.get('use_limit')
               for g in result['candidate']['gaps'])
    assert data == original
    again = validate(result['candidate'], bundle, catalog)
    assert again['valid'] and again['candidate'] == result['candidate']


def test_exact_block_controls_redundant_chapter_without_changing_the_claim():
    data, bundle, catalog = fixture()
    bundle['sections'].append(dict(id='s2', title='另一个章节', parent_id=None, order=2))
    data['profile']['learning_units'][0]['steps'][0]['source_refs'] = copy.deepcopy(
        data['profile']['learning_units'][0]['steps'][0]['source_refs'])
    ref = data['profile']['learning_units'][0]['steps'][0]['source_refs'][0]
    ref['section_id'] = 's2'
    original = copy.deepcopy(data)
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    got = result['candidate']['profile']['learning_units'][0]['steps'][0]
    assert got['operation'] == original['profile']['learning_units'][0]['steps'][0]['operation']
    assert got['source_refs'][0]['block_id'] == 'b1'
    assert got['source_refs'][0]['section_id'] == 's1'
    assert data == original
    ref['block_id'] = 'does_not_exist'
    assert not validate(data, bundle, catalog)['valid']


def test_unprovided_full_page_images_remain_an_explicit_limit_not_unread_text():
    data, bundle, catalog = fixture()
    bundle['blocks'][0]['position'] = {'page': 1}
    bundle['blocks'].append(dict(id='b4', text='第四页正文', section_id='s1', position={'page': 4}))
    declaration = 'PDF pages 1,3-4 as full-page images'
    data['source']['reading_scope']['unread_refs'] = [declaration]
    original_units = copy.deepcopy(data['profile']['learning_units'])
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    assert result['candidate']['source']['reading_scope']['unread_refs'] == []
    assert declaration in result['candidate']['source']['reading_scope']['notes']
    assert any(declaration in g.get('use_limit', '') for g in result['candidate']['gaps'])
    assert result['candidate']['profile']['learning_units'] == original_units
    again = validate(result['candidate'], bundle, catalog)
    assert again['valid'] and again['candidate'] == result['candidate']
    data['source']['reading_scope']['unread_refs'] = ['PDF pages 99 as full-page images']
    assert not validate(data, bundle, catalog)['valid']


def omitted_page_fixture():
    data, bundle, catalog = fixture()
    data['source']['reading_scope']['unread_refs'] = ['page_0002']
    bundle['removed'] = [dict(reason='bounded_nonresearch_section',
                              block=dict(id='src_ack', type='paragraph', text='致谢'))]
    bundle['cleanup'] = dict(page_evidence=dict(
        method='exact_source_text_or_pdf_position', pages=[dict(
            page=2, action='omit_nonresearch_page', source_block_ids=['src_ack'],
            boxes=[[10, 10, 100, 40]], uncertain_boxes=[])]))
    return data, bundle, catalog


def test_explicitly_omitted_nonresearch_page_keeps_unread_claim_in_notes():
    data, bundle, catalog = omitted_page_fixture()
    original = copy.deepcopy(data)
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    reading = result['candidate']['source']['reading_scope']
    assert reading['unread_refs'] == [] and 'page_0002' not in reading['read_refs']
    assert 'page_0002' in reading['notes'] and '非研究页' in reading['notes']
    assert result['candidate']['profile']['learning_units'] == original['profile']['learning_units']
    assert any(r['code'] == 'normalized_excluded_page_declaration' for r in result['compatibility_report'])
    assert data == original
    again = validate(result['candidate'], bundle, catalog)
    assert again['valid'] and again['candidate'] == result['candidate']
    assert not validate_candidate(data, bundle, catalog)['valid']


@pytest.mark.parametrize('problem', [
    'unknown_page', 'no_cleaning_record', 'partial_page', 'uncertain', 'missing_block',
    'research_appendix', 'retained_block', 'actual_page_text', 'conflicting_page_record', 'claimed_read',
])
def test_omitted_page_exception_requires_complete_nonresearch_evidence(problem):
    data, bundle, catalog = omitted_page_fixture()
    cleanup = bundle['cleanup']['page_evidence']
    row = cleanup['pages'][0]
    if problem == 'unknown_page':data['source']['reading_scope']['unread_refs'] = ['page_0003']
    elif problem == 'no_cleaning_record':bundle['cleanup'] = {}
    elif problem == 'partial_page':row['action'] = 'mask_nonresearch_regions'
    elif problem == 'uncertain':row['uncertain_boxes'] = [[10, 10, 20, 20]]
    elif problem == 'missing_block':bundle['removed'] = []
    elif problem == 'research_appendix':bundle['removed'][0]['reason'] = 'research_appendix'
    elif problem == 'retained_block':bundle['blocks'][0]['source_block_id'] = 'src_ack'
    elif problem == 'actual_page_text':bundle['blocks'][0]['position'] = {'page': 2}
    elif problem == 'conflicting_page_record':cleanup['pages'].append(dict(row, action='retain'))
    elif problem == 'claimed_read':data['source']['reading_scope']['read_refs'] = ['page_0002']
    original = copy.deepcopy(data)
    result = validate(data, bundle, catalog)
    assert not result['valid'] and 'source_reference' in {r['code'] for r in result['errors']}
    assert data == original


def test_all_provided_images_and_tables_alias_uses_actual_asset_ids():
    data, bundle, catalog = fixture()
    _, template = load_template()
    item = empty(template['assets']['items'][0])
    item.update(id='a1', kind='table', storage='description', evidence_basis='table_text',
                meaning='排程结果表', unit_ids=['U1'], section_ids=['s1'])
    bundle['assets'] = [dict(id='a1', kind='table', text='方法一 10；方法二 8')]
    data['assets']['items'] = [item]
    data['source']['reading_scope']['read_refs'] = ['assets:all_provided_images_and_tables']
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    assert result['candidate']['source']['reading_scope']['read_refs'] == ['a1']
    data['source']['reading_scope']['unread_refs'] = ['object_missing']
    assert not validate(data, bundle, catalog)['valid']


def gap(pointer=None, **values):
    _, template = load_template()
    row = empty(template['gaps'][0])
    row.update(id='G1', type='extraction_missing', target_pointer=pointer,
               description='局部图形结构没有读清', use_limit='只学习文字中的研究步骤，图纸尺寸必须回看原图')
    row.update(values)
    return row


def test_exact_basename_is_compatible_but_source_hash_and_other_names_are_not():
    data, bundle, catalog = fixture()
    bundle['source']['file'] = r'C:\private\input.docx'
    data['source']['file'] = 'input.docx'
    original = copy.deepcopy(data)
    assert validate(data, bundle, catalog)['valid']
    assert data == original
    for field, value in [('source_sha256', 'b' * 64), ('file', 'another.docx')]:
        bad = copy.deepcopy(data)
        bad['source'][field] = value
        result = validate(bad, bundle, catalog)
        assert not result['valid']
        assert 'source_mismatch' in {e['code'] for e in result['errors']}


def test_misnested_fields_move_without_changing_learning_or_hiding_conflicts():
    data, bundle, catalog = fixture()
    original = copy.deepcopy(data)
    for key in ('profile', 'tree', 'assets', 'gaps', 'attachments', 'redaction_plan', 'segmentation'):
        data['source'][key] = data.pop(key)
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    assert result['candidate']['profile']['learning_units'] == original['profile']['learning_units']
    again = validate(result['candidate'], bundle, catalog)
    assert again['valid'] and again['candidate'] == result['candidate']
    data['profile'] = dict(original['profile'], material_type='冲突值')
    assert not validate(data, bundle, catalog)['valid']


def test_qualified_reading_ranges_expand_in_supplied_order_and_keep_claims_private():
    data, bundle, catalog = fixture()
    bundle['blocks'] += [dict(bundle['blocks'][0], id='hash_z'), dict(bundle['blocks'][0], id='hash_a')]
    data['source']['reading_scope']['read_refs'] = ['main:text_blocks:all', 'blocks:hash_z..hash_a', 'main:sections']
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    assert result['candidate']['source']['reading_scope']['read_refs'] == ['b1', 'hash_z', 'hash_a', 's1']
    for bad in ('blocks:hash_a..hash_z', 'blocks:hash_z..missing', 'unknown:b1'):
        data['source']['reading_scope']['read_refs'] = [bad]
        assert not validate(data, bundle, catalog)['valid']


def test_dotted_gap_pointer_resolves_exactly_and_bad_pointer_stays_blocked():
    data, bundle, catalog = fixture()
    data['gaps'] = [gap('profile.learning_units[0].steps[0].operation')]
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    assert result['candidate']['gaps'][0]['target_pointer'] == '/profile/learning_units/0/steps/0/operation'
    data['gaps'][0]['target_pointer'] = 'profile.learning_units[9].steps[0].operation'
    assert not validate(data, bundle, catalog)['valid']


def test_missing_program_chapters_are_restored_without_inventing_their_function():
    data, bundle, catalog = fixture()
    bundle['sections'].append(dict(id='s2', title='参考文献条目', parent_id='s1', order=2, block_ids=[]))
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    added = result['candidate']['tree']['nodes'][1]
    assert (added['id'], added['title'], added['parent_id'], added['order']) == ('s2', '参考文献条目', 's1', 2)
    assert not added.get('function')
    assert result['candidate']['tree']['nodes'][0] == data['tree']['nodes'][0]


def test_existing_asset_relation_can_supply_missing_unit_reference():
    data, bundle, catalog = fixture()
    _, template = load_template()
    item = empty(template['assets']['items'][0])
    item.update(id='a1', kind='table', storage='description', evidence_basis='table_text',
                meaning='两种排程的比较表', unit_ids=['U1'], section_ids=['s1'])
    data['assets']['items'] = [item]
    bundle['assets'] = [dict(id='a1', kind='table', text='方法一 10；方法二 8')]
    unit = data['profile']['learning_units'][0]
    unit.update(source_refs=[], asset_ids=['a1'])
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    assert result['candidate']['profile']['learning_units'][0]['source_refs'][0]['asset_id'] == 'a1'
    assert result['candidate']['profile']['learning_units'][0]['steps'] == unit['steps']


def test_local_reading_gaps_and_partial_label_do_not_reject_usable_learning():
    data, bundle, catalog = fixture()
    original_units = copy.deepcopy(data['profile']['learning_units'])
    data['record_kind'] = 'partial_candidate'
    data['gaps'] = [gap('/assets/items'), gap(None, id='G2')]
    bundle['issues'] = [dict(code='formula_review', position={'paragraph': 3})]
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    assert result['admission']['allow_partial']
    assert result['candidate']['record_kind'] == 'partial_candidate'
    assert result['candidate']['profile']['learning_units'] == original_units
    assert any('公式' in g.get('use_limit', '') for g in result['candidate']['gaps'])
    again = validate(result['candidate'], bundle, catalog)
    assert again['valid'] and again['candidate'] == result['candidate']


def test_empty_learning_chain_is_still_rejected():
    data, bundle, catalog = fixture()
    data['profile']['learning_units'][0]['steps'] = []
    data['gaps'] = [gap('/profile/learning_units/0/steps')]
    result = validate(data, bundle, catalog)
    assert not result['valid']
    assert 'learning_unusable' in {r['code'] for r in result['errors']}


@pytest.mark.parametrize('unread', ['b1', 's1', 'source-1'])
def test_only_explicitly_unread_evidence_does_not_establish_a_usable_chain(unread):
    data, bundle, catalog = fixture()
    data['record_kind'] = 'partial_candidate'
    data['source']['reading_scope']['unread_refs'] = [unread]
    result = validate(data, bundle, catalog)
    assert not result['valid']
    assert 'learning_unusable' in {r['code'] for r in result['errors']}


def test_unread_other_unit_keeps_a_public_limit_when_independent_learning_exists():
    data, bundle, catalog = fixture()
    other = copy.deepcopy(data['profile']['learning_units'][0])
    other.update(id='U2', source_refs=[dict(other['source_refs'][0], block_id='b2')], materials=[], steps=[], outputs=[])
    data['profile']['learning_units'].append(other)
    bundle['blocks'].append(dict(bundle['blocks'][0], id='b2'))
    data['source']['reading_scope']['unread_refs'] = ['b2']
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    assert result['admission']['usable_unit_ids'] == ['U1']
    assert any('未读' in g.get('use_limit', '') and g['target_pointer']=='/profile/learning_units/1' for g in result['candidate']['gaps'])


def test_program_blocking_material_failure_is_not_relabelled_as_formula_review():
    data, bundle, catalog = fixture()
    bundle['issues'] = [dict(code='source_unreadable', critical=True)]
    assert not validate(data, bundle, catalog)['valid']


def test_global_and_collection_limits_reach_every_related_learning_slice(tmp_path):
    from preextract_publish import prepare_publication
    from test_preextract_publish import PAPERFLOW, Counter
    data, bundle, catalog = fixture()
    data['profile']['learning_units'][0]['steps'][0]['operation'] *= 160
    data['gaps'] = [gap('/assets/items'), gap(None, id='G2', use_limit='记录不全的位置不能作为精确引文')]
    result = validate(data, bundle, catalog)
    assert result['valid'], result['errors']
    catalog['majors'][0].update(paper_dataset_id='paper_ds', image_dataset_id='image_ds')
    prepared = prepare_publication(result['candidate'], bundle, tmp_path / 'public',
        paperflow_root=PAPERFLOW, catalog=catalog, counter=Counter(),
        redaction_policy='best_effort', candidate_validation=result, admission_policy='learning')
    chunks = json.loads((Path(prepared['package_path']) / 'chunks.json').read_text(encoding='utf8'))['chunks']
    learning = [c for c in chunks if c['kind'] == 'learning']
    assert len(learning) > 1
    assert all('图纸尺寸必须回看原图' in c['content'] and '不能作为精确引文' in c['content'] for c in learning)
    profile = json.loads((Path(prepared['package_path']) / 'profile.json').read_text(encoding='utf8'))
    for actual, expected in zip(profile['learning_units'][0]['steps'], result['candidate']['profile']['learning_units'][0]['steps'], strict=True):
        assert {k: v for k, v in actual.items() if k != 'source_refs'} == {k: v for k, v in expected.items() if k != 'source_refs'}
    assert prepared['validation']['admission']['policy'] == 'learning'
