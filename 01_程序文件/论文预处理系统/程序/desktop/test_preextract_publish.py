import copy
import hashlib
import json
from pathlib import Path

import pytest


PAPERFLOW = Path(r'E:\rag mid\langgraph_workflow')


def test_special_segmentation_cases_are_reported_not_silently_dropped(tmp_path):
    c,b,_=fixture()
    c['segmentation']={'special_cases':[{'source_refs':[], 'issue':'跨表说明需要核对连续性'}]}
    result=prepare(tmp_path,c,b)
    warning=result['validation']['segmentation_warnings'][0]
    assert warning['issue']=='跨表说明需要核对连续性'
    assert warning['execution']=='reported_not_interpreted'


class Counter:
    model_budget = 2000
    cl_limit = 2038

    def count_many(self, texts):
        return [{'model_tokens': len(text), 'cl100k_tokens': len(text)} for text in texts]


def fixture():
    text = '研究者张三采用控制模型。\n参数为 2，不能照搬结论。'
    source = dict(source_id='source_test', source_sha256='a' * 64,
                  file=r'C:\private\张三论文.docx', text_sha256=hashlib.sha256(text.encode()).hexdigest())
    ref = dict(file_ref='main', block_id='B1', section_id='S1', asset_id=None,
               start=0, end=len(text), source_kind='author_text', locator_hint=None)
    unit = dict(id='U1', title='控制步骤', question='怎样控制', method='控制模型',
                materials=[dict(id='M1', description='记录速度与角度')],
                steps=[dict(id='T1', operation='进行计算' * 800, input_refs=['M1'], output_refs=['O1'])],
                outputs=[dict(id='O1', content='轨迹')], interpretation='解释波动',
                formulas_parameters=[dict(name='参数', expression_or_value='2', role='author_choice')],
                conditions=['同等输入'], limitations=['必须重新取材'], new_material_needs=['新速度'],
                section_ids=['S1'], asset_ids=[], source_refs=[ref], upstream_unit_ids=[])
    candidate = dict(schema_version='paper-preextract/1.0', record_kind='filled_candidate', source=source,
        profile=dict(material_disposition=dict(decision='keep', reason='研究论文', source_refs=[ref]),
                     article=dict(title='控制模型研究', source_refs=[ref]),
                     research_profile=dict(problem='怎样控制'), learning_units=[unit],
                     routing_candidates=[dict(code='080801', name='自动化', reason='控制', source_refs=[ref])]),
        tree=dict(nodes=[dict(id='S1', title='方法', parent_id=None, order=0, function='论证方法', unit_ids=['U1'])]),
        assets=dict(items=[]), redaction_plan=[dict(id='R1', entity_key='person1', role='author',
            scope='source_text', action='replace', replacement='研究者甲', expected_text='张三',
            source_refs=[dict(ref, start=3, end=5)], location_status='located')],
        gaps=[], segmentation={}, attachments=[])
    bundle = dict(version='preextract-material/1.0', source=source,
                  blocks=[dict(id='B1', source_block_id='B1', type='paragraph', text=text,
                               section_id='S1', position={}, order=0)],
                  sections=[dict(id='S1', title='方法', parent_id=None, level=1, order=0, block_ids=['B1'])],
                  assets=[], issues=[], attachments=[])
    catalog = dict(majors=[dict(code='080801', name='自动化', paper_dataset_id='paper_ds', image_dataset_id='image_ds')])
    return candidate, bundle, catalog


def prepare(tmp_path, candidate=None, bundle=None, counter=None):
    from preextract_publish import prepare_publication
    c, b, catalog = fixture()
    return prepare_publication(candidate or c, bundle or b, tmp_path / 'public',
        paperflow_root=PAPERFLOW, catalog=catalog, counter=counter or Counter())


def test_publication_exact_body_full_learning_and_private_exclusion(tmp_path):
    c, b, _ = fixture()
    original = copy.deepcopy((c, b))
    result = prepare(tmp_path, c, b)
    root = Path(result['package_path'])
    text = (root / 'article.redacted.md').read_text(encoding='utf8')
    chunks = json.loads((root / 'chunks.json').read_text(encoding='utf8'))['chunks']
    assert ''.join(row['content'] for row in chunks if row['kind'] == 'body') == text
    assert '张三' not in text and '研究者甲' in text
    units = [row for row in chunks if row['kind'] == 'learning']
    assert len(units) > 1
    assert all(row['unit_ids'] == ['U1'] and '控制步骤' in row['content'] for row in units)
    combined = '\n'.join(row['content'] for row in units)
    assert all(value in combined for value in ['记录速度与角度', '解释波动', 'expression_or_value', '同等输入', '新速度', '必须重新取材'])
    public = '\n'.join(p.read_text(encoding='utf8') for p in root.rglob('*') if p.suffix in ('.json', '.md'))
    assert 'C:\\\\private' not in public and '张三' not in public and 'redaction_plan' not in public
    assert original == (c, b)
    assert result['validation']['body_exact_reassembly']
    assert result['validation']['embedding']['admission_verified']
    assert any(row['kind'] == 'chapter' and '论证方法' in row['content'] for row in chunks)


def test_linked_limits_follow_every_learning_slice_without_removing_content(tmp_path):
    c, b, _ = fixture()
    c['gaps'] = [dict(id='G1', type='uncertain', target_pointer='/profile/learning_units/0/source_refs',
                     description='一处引用仅有章节定位', use_limit='来源精度有限，不能作为精确逐句引文', source_refs=[])]
    result = prepare(tmp_path, c, b)
    root = Path(result['package_path'])
    chunks = json.loads((root / 'chunks.json').read_text(encoding='utf8'))['chunks']
    learning = [r for r in chunks if r['kind'] == 'learning']
    assert len(learning) > 1
    assert all('来源精度有限，不能作为精确逐句引文' in r['content'] for r in learning)
    assert '进行计算' * 100 in '\n'.join(r['content'] for r in learning)
    profile = json.loads((root / 'profile.json').read_text(encoding='utf8'))
    assert profile['learning_units'][0]['steps'] == c['profile']['learning_units'][0]['steps']


def test_asset_limit_follows_related_learning_and_chart_slices(tmp_path):
    c, b, _ = fixture()
    c['assets']['items'] = [dict(id='A1', kind='table', storage='description',
        meaning='已有图表说明' * 300, unit_ids=['U1'], section_ids=['S1'], limits=[])]
    c['gaps'] = [dict(id='G1', type='uncertain', target_pointer='/assets/items/0/evidence_basis',
                     description='原生表格文字没有取得', use_limit='数值须回看原图核对', source_refs=[])]
    result = prepare(tmp_path, c, b)
    chunks = json.loads((Path(result['package_path']) / 'chunks.json').read_text(encoding='utf8'))['chunks']
    rows = [r for r in chunks if r['kind'] in ('learning', 'chart')]
    assert any(r['kind'] == 'chart' for r in rows)
    assert all('数值须回看原图核对' in r['content'] for r in rows)


def test_bad_redaction_location_fails_before_public_package(tmp_path):
    c, b, _ = fixture()
    c['redaction_plan'][0]['source_refs'][0]['start'] = 0
    with pytest.raises(ValueError, match='redaction'):
        prepare(tmp_path, c, b)
    assert not (tmp_path / 'public' / 'READY.json').exists()


def test_generated_field_uses_exact_pointer_and_preserves_nonidentity_names(tmp_path):
    c, b, _ = fixture()
    c['profile']['article']['summary'] = '张三的研究；参考作者李四。'
    c['redaction_plan'].append(dict(id='R2', entity_key='person1', scope='generated_field', action='replace',
        replacement='研究者甲', expected_text='张三', target_pointer='/profile/article/summary', location_status='located'))
    result = prepare(tmp_path, c, b)
    profile = json.loads((Path(result['package_path']) / 'profile.json').read_text(encoding='utf8'))
    assert profile['article']['summary'] == '研究者甲的研究；参考作者李四。'


def test_unplanned_private_identity_in_generated_field_is_reported(tmp_path):
    c, b, _ = fixture()
    c['profile']['article']['summary'] = '张三研究了控制模型。'
    with pytest.raises(ValueError, match='original remains'):
        prepare(tmp_path, c, b)
    assert not (tmp_path / 'public' / 'READY.json').exists()


def test_unplanned_repeat_of_private_identity_in_body_is_reported(tmp_path):
    c,b,_=fixture()
    b['blocks'][0]['text']+='同一研究者张三再次复核结果。'
    with pytest.raises(ValueError,match='original remains in body block'):
        prepare(tmp_path,c,b)
    assert not (tmp_path/'public'/'READY.json').exists()


def test_cited_author_same_name_is_preserved_with_actual_source_location(tmp_path):
    c, b, _ = fixture()
    citation = '张三. 控制综述. 2020.'
    b['blocks'].append(dict(id='B2', source_block_id='B2', type='paragraph', text=citation,
                            section_id='S1', position={}, order=1))
    c['profile']['learning_units'][0]['interpretation'] = citation
    c['profile']['learning_units'][0]['source_refs'].append(dict(file_ref='main', block_id='B2', section_id='S1',
        start=0, end=len(citation), source_kind='cited_work', asset_id=None, locator_hint=None))
    result = prepare(tmp_path, c, b)
    assert citation in (Path(result['package_path']) / 'article.redacted.md').read_text(encoding='utf8')
    assert result['packet']['profile']['learning_units'][0]['interpretation'] == citation


def test_keep_together_fits_without_reordering_or_cutting_group(tmp_path):
    c, b, _ = fixture()
    b['blocks'][0]['text'] += '甲' * 860
    for index in (2, 3):
        b['blocks'].append(dict(id='B' + str(index), source_block_id='B' + str(index), type='paragraph',
            text=str(index) * 280, section_id='S1', position={}, order=index))
    refs = [dict(file_ref='main', block_id='B' + str(i), section_id='S1', asset_id=None,
                 start=None, end=None, source_kind='author_text', locator_hint=None) for i in (2, 3)]
    c['segmentation'] = dict(keep_together=[dict(source_refs=refs, reason='输入输出应相邻')],
                            semantic_groups=[dict(source_refs=refs, unit_ids=['U1'], reason='完整步骤')])
    result = prepare(tmp_path, c, b)
    body = [r for r in result['packet']['chunks'] if r['kind'] == 'body']
    assert any('2' * 280 in r['content'] and '3' * 280 in r['content'] for r in body)
    assert any(r.get('group_ids') for r in body)
    assert any(r['kind'] == 'continuity' and '输入输出应相邻' in r['content'] for r in result['packet']['chunks'])


def test_conflicting_entity_replacement_is_rejected(tmp_path):
    c, b, _ = fixture()
    c['profile']['article']['summary'] = '张三'
    c['redaction_plan'].append(dict(id='R2', entity_key='person1', scope='generated_field', action='replace',
        replacement='研究者乙', expected_text='张三', target_pointer='/profile/article/summary', location_status='located'))
    with pytest.raises(ValueError, match='replacement'):
        prepare(tmp_path, c, b)


def test_delete_and_replace_same_entity_are_compatible(tmp_path):
    c, b, _ = fixture()
    c['profile']['article']['summary'] = '原作者张三'
    c['redaction_plan'].append(dict(id='R2', entity_key='person1', scope='generated_field', action='delete',
        replacement=None, expected_text='张三', target_pointer='/profile/article/summary', location_status='located'))
    result = prepare(tmp_path, c, b)
    assert result['packet']['profile']['article']['summary'] == '原作者'


def test_private_filename_fragment_and_metadata_are_omitted(tmp_path):
    c, b, _ = fixture()
    selector = dict(file_ref='main', part='docProps/core.xml', selector='creator', attribute=None)
    b['metadata'] = [dict(selector=selector, value='张三')]
    c['redaction_plan'].extend([
        dict(id='R2', entity_key='person1', scope='filename', action='replace', replacement='研究者甲',
             expected_text='张三', location_status='located'),
        dict(id='R3', entity_key='person1', scope='metadata', action='delete', replacement=None,
             expected_text='张三', metadata_selector=selector, location_status='located')])
    result = prepare(tmp_path, c, b)
    assert result['validation']['redaction_actions'] == 3
    public = '\n'.join(p.read_text(encoding='utf8') for p in Path(result['package_path']).rglob('*.json'))
    assert '张三' not in public and 'docProps/core.xml' not in public


def test_unprovided_metadata_selector_cannot_be_executed(tmp_path):
    c, b, _ = fixture()
    c['redaction_plan'].append(dict(id='R2', scope='metadata', action='delete', expected_text='张三',
        metadata_selector=dict(file_ref='main', part='docProps/core.xml', selector='creator', attribute=None),
        location_status='located'))
    with pytest.raises(ValueError, match='metadata'):
        prepare(tmp_path, c, b)


def test_best_effort_publication_applies_good_edits_and_existing_frontmatter_rules(tmp_path):
    from test_preextract_contract import fixture as contract_fixture, empty
    from preextract_contract import load_template, validate_candidate
    from preextract_publish import prepare_publication
    c, b, catalog = contract_fixture()
    catalog['majors'][0].update(paper_dataset_id='paper_ds', image_dataset_id='image_ds')
    _, skeleton = load_template()
    ref = c['profile']['article']['source_refs'][0]
    row = empty(skeleton['redaction_plan'][0])
    row.update(id='R1', entity_key='person', scope='source_text', action='replace', expected_text='张三',
               replacement='研究者甲', location_status='uncertain', source_refs=[dict(ref), dict(ref, block_id='missing')])
    broken = dict(row, id='R2', scope='generated_field', source_refs=[], target_pointer='/profile/article/absent')
    metadata = empty(skeleton['redaction_plan'][0])
    metadata.update(id='R3', scope='metadata', action='replace', location_status='unavailable')
    c['redaction_plan'] = [row, broken, metadata]
    c['profile']['article']['summary'] = '张三研究了排序，后续须重新取材。'
    b['blocks'].insert(0, dict(id='b0', type='paragraph', text='作者：李四\n学号：2020123456', section_id='s1', order=-1, position={}))
    original = copy.deepcopy((c, b))
    validated = validate_candidate(c, b, catalog, redaction_policy='best_effort')
    assert validated['valid'], validated['errors']
    result = prepare_publication(validated['candidate'], b, tmp_path / 'best',
        paperflow_root=PAPERFLOW, catalog=catalog, counter=Counter(), redaction_policy='best_effort',
        candidate_validation=validated)
    text = (Path(result['package_path']) / 'article.redacted.md').read_text(encoding='utf8')
    assert '研究者甲' in text and '张三' not in text
    assert '李四' not in text and '2020123456' not in text
    assert result['validation']['redaction']['frontmatter_blocks'] == 1
    assert result['validation']['redaction']['warnings']
    assert result['validation']['redaction']['residual_findings']
    assert result['validation']['body_exact_reassembly']
    mapping = json.loads((Path(result['package_path']) / 'source-map.json').read_text(encoding='utf8'))
    originals = {block['id']: block['text'] for block in b['blocks']}
    for position in mapping['blocks']:
        bid = position['block_id']
        final = text[position['public_start']:position['public_end']]
        edits = sorted((row for row in mapping['text_edits'] if row['block_id'] == bid),
                       key=lambda row: row['original_start'])
        old_end = public_end = 0
        for edit in edits:
            assert originals[bid][old_end:edit['original_start']] == final[public_end:edit['public_start']]
            old_end, public_end = edit['original_end'], edit['public_end']
        assert originals[bid][old_end:] == final[public_end:]
    assert original == (c, b)


def test_best_effort_keeps_model_partial_label_while_using_verified_program_admission(tmp_path):
    from test_preextract_contract import fixture as contract_fixture, empty
    from preextract_contract import load_template, validate_candidate
    from preextract_publish import prepare_publication
    c, b, catalog = contract_fixture()
    catalog['majors'][0].update(paper_dataset_id='paper_ds', image_dataset_id='image_ds')
    c['record_kind'] = 'partial_candidate'
    _, skeleton = load_template()
    gap = empty(skeleton['gaps'][0])
    gap.update(id='G1', type='source_not_stated', target_pointer='/profile/education_level', description='学历未记载')
    c['gaps'] = [gap]
    validated = validate_candidate(c, b, catalog, redaction_policy='best_effort')
    result = prepare_publication(validated['candidate'], b, tmp_path / 'best',
        paperflow_root=PAPERFLOW, catalog=catalog, counter=Counter(), redaction_policy='best_effort',
        candidate_validation=validated)
    assert c['record_kind'] == 'partial_candidate'
    assert result['validation']['model_record_kind'] == 'partial_candidate'
    assert result['validation']['redaction']['policy'] == 'best_effort'
    with pytest.raises(ValueError, match='validation|partial'):
        prepare_publication(validated['candidate'], b, tmp_path / 'unverified',
            paperflow_root=PAPERFLOW, catalog=catalog, counter=Counter(), redaction_policy='best_effort')


def test_optional_bad_split_hints_do_not_block_actual_package_or_drop_body(tmp_path):
    from test_preextract_contract import fixture as contract_fixture
    from preextract_contract import validate_candidate
    from preextract_publish import prepare_publication
    c, b, catalog = contract_fixture()
    catalog['majors'][0].update(paper_dataset_id='paper_ds', image_dataset_id='image_ds')
    c['segmentation']['split_suggestions'] = [dict(source_refs=[], reason='可选建议', split_points=[
        dict(block_id='b1', offset=0), dict(block_id='b1', offset=len(b['blocks'][0]['text'])),
        dict(block_id='b1', offset=-1), dict(block_id='absent', offset=5), dict(block_id='b1', offset='bad')])]
    original = copy.deepcopy((c, b))
    validated = validate_candidate(c, b, catalog, redaction_policy='best_effort')
    assert validated['valid'], validated['errors']
    # The publisher recompiles from current source facts, not an edited plan.
    validated['executable_split_points'].append(dict(block_id='absent', offset=999))
    result = prepare_publication(validated['candidate'], b, tmp_path / 'best',
        paperflow_root=PAPERFLOW, catalog=catalog, counter=Counter(), redaction_policy='best_effort',
        candidate_validation=validated)
    assert result['validation']['body_exact_reassembly']
    assert result['validation']['segmentation_warnings']
    assert original == (c, b)


def test_normalized_bad_plan_retains_prior_partial_admission_but_rechecks_current_material(tmp_path):
    from test_preextract_contract import fixture as contract_fixture
    from preextract_contract import validate_candidate
    from preextract_publish import prepare_publication
    c, b, catalog = contract_fixture()
    catalog['majors'][0].update(paper_dataset_id='paper_ds', image_dataset_id='image_ds')
    c.update(record_kind='partial_candidate', redaction_plan={})
    validated = validate_candidate(c, b, catalog, redaction_policy='best_effort')
    assert validated['valid'] and validated['admission']['allow_partial']
    assert validated['candidate']['redaction_plan'] == []
    result = prepare_publication(validated['candidate'], b, tmp_path / 'best',
        paperflow_root=PAPERFLOW, catalog=catalog, counter=Counter(), redaction_policy='best_effort',
        candidate_validation=validated)
    assert result['validation']['model_record_kind'] == 'partial_candidate'
    assert result['validation']['redaction']['warnings']
    b['issues'].append(dict(code='missing_core_body', blocking=True))
    with pytest.raises(ValueError, match='validation changed'):
        prepare_publication(validated['candidate'], b, tmp_path / 'drift',
            paperflow_root=PAPERFLOW, catalog=catalog, counter=Counter(), redaction_policy='best_effort',
            candidate_validation=validated)


def test_best_effort_executes_good_image_box_and_skips_bad_box(tmp_path):
    from PIL import Image
    from test_preextract_contract import fixture as contract_fixture, empty
    from preextract_contract import load_template, validate_candidate
    from preextract_publish import prepare_publication
    c, b, catalog = contract_fixture()
    catalog['majors'][0].update(paper_dataset_id='paper_ds', image_dataset_id='image_ds')
    path = tmp_path / 'original.png'
    Image.new('RGB', (20, 10), 'white').save(path)
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    b['assets'] = [dict(id='A1', image_path=str(path), sha256=sha, width=20, height=10)]
    _, skeleton = load_template()
    asset = empty(skeleton['assets']['items'][0])
    asset.update(id='A1', kind='figure', source_image_id='A1', storage='image', evidence_basis='pixels',
                 caption='实验结构', meaning='连接方式', section_ids=['s1'], unit_ids=['U1'])
    c['assets']['items'] = [asset]
    row = empty(skeleton['redaction_plan'][0])
    row.update(id='R1', entity_key='signature', scope='image', action='mask', location_status='located',
        image_ref=dict(asset_id='A1', rendition_sha256=sha, width=20, height=10),
        redactions=[dict(x0=0, y0=0, x1=5, y1=5), dict(x0=0, y0=0, x1=25, y1=5)])
    c['redaction_plan'] = [row]
    validated = validate_candidate(c, b, catalog, redaction_policy='best_effort')
    assert validated['valid']
    result = prepare_publication(validated['candidate'], b, tmp_path / 'best',
        paperflow_root=PAPERFLOW, catalog=catalog, counter=Counter(), redaction_policy='best_effort',
        candidate_validation=validated)
    output = Path(result['package_path']) / result['packet']['assets'][0]['file']
    with Image.open(output) as image:
        assert image.getpixel((1, 1)) == (0, 0, 0)
        assert image.getpixel((8, 1)) == (255, 255, 255)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == sha


def test_frontmatter_mask_updates_group_boundaries_and_skips_obsolete_split_hint(tmp_path):
    from test_preextract_contract import fixture as contract_fixture
    from preextract_contract import validate_candidate
    from preextract_publish import prepare_publication
    c, b, catalog = contract_fixture()
    catalog['majors'][0].update(paper_dataset_id='paper_ds', image_dataset_id='image_ds')
    b['blocks'].insert(0, dict(id='b0', type='paragraph', text='作者：李四', section_id='s1', order=-1, position={}))
    ref = dict(c['profile']['article']['source_refs'][0], block_id='b0', start=4, end=5)
    c['segmentation']['semantic_groups'] = [dict(source_refs=[ref], unit_ids=[], reason='保持原材料连续')]
    c['segmentation']['split_suggestions'] = [dict(source_refs=[ref],
        split_points=[dict(block_id='b0', offset=4)], reason='可选分段点')]
    validated = validate_candidate(c, b, catalog, redaction_policy='best_effort')
    assert validated['valid'], validated['errors']
    result = prepare_publication(validated['candidate'], b, tmp_path / 'best',
        paperflow_root=PAPERFLOW, catalog=catalog, counter=Counter(), redaction_policy='best_effort',
        candidate_validation=validated)
    assert result['validation']['body_exact_reassembly']
    assert len(result['validation']['segmentation_warnings']) == 2
    mapping = json.loads((Path(result['package_path']) / 'source-map.json').read_text(encoding='utf8'))
    group = mapping['groups'][0]
    article = (Path(result['package_path']) / 'article.redacted.md').read_text(encoding='utf8')
    assert article[group['public_start']:group['public_end']] == '[已脱敏]'
    assert '李四' not in article


@pytest.mark.parametrize('field', ['original_image_path', 'local_path'])
def test_best_effort_keeps_known_private_asset_paths_as_hard_publication_errors(tmp_path, field):
    from test_preextract_contract import fixture as contract_fixture, empty
    from preextract_contract import load_template, validate_candidate
    from preextract_publish import prepare_publication
    c, b, catalog = contract_fixture()
    catalog['majors'][0].update(paper_dataset_id='paper_ds', image_dataset_id='image_ds')
    original = tmp_path / 'private-original.bmp'
    original.write_bytes(b'private original')
    b['assets'] = [dict(id='A1', **{field:str(original)})]
    _, skeleton = load_template()
    asset = empty(skeleton['assets']['items'][0])
    asset.update(id='A1', source_image_id='A1', storage='omit', evidence_basis='caption_context_only')
    c['assets']['items'] = [asset]
    c['profile']['article']['summary'] = str(original)
    validated = validate_candidate(c, b, catalog, redaction_policy='best_effort')
    assert validated['valid']
    with pytest.raises(ValueError, match='private source path'):
        prepare_publication(validated['candidate'], b, tmp_path / 'best',
            paperflow_root=PAPERFLOW, catalog=catalog, counter=Counter(), redaction_policy='best_effort',
            candidate_validation=validated)
    assert not (tmp_path / 'best/READY.json').exists()


def test_partial_candidate_stops_and_noncritical_missing_information_is_preserved(tmp_path):
    c, b, _ = fixture()
    c['record_kind'] = 'partial_candidate'
    with pytest.raises(ValueError, match='partial'):
        prepare(tmp_path, c, b)
    c['record_kind'] = 'filled_candidate'
    c['gaps'] = [dict(id='G1', type='source_not_stated', description='作者未交代样本取得日期',
                       impact='critical', use_limit='新研究须另取材料')]
    result = prepare(tmp_path, c, b)
    assert result['packet']['profile']['gaps'] == c['gaps']


def test_image_with_program_asset_id_needs_no_duplicate_source_image_id(tmp_path):
    from PIL import Image
    c, b, _ = fixture()
    image_path = tmp_path / 'image.png'
    Image.new('RGB', (10, 10), 'white').save(image_path)
    b['assets'] = [dict(id='A1', image_path=str(image_path), sha256=hashlib.sha256(image_path.read_bytes()).hexdigest(), width=10, height=10)]
    c['assets']['items'] = [dict(id='A1', source_image_id=None, storage='image', section_ids=['S1'], meaning='结构关系')]
    result = prepare(tmp_path, c, b)
    assert (Path(result['package_path']) / result['packet']['assets'][0]['file']).is_file()


def test_retained_image_is_version_bound_and_masked(tmp_path):
    from PIL import Image
    c, b, _ = fixture()
    image_path = tmp_path / 'private.png'
    Image.new('RGB', (20, 20), 'white').save(image_path)
    digest = hashlib.sha256(image_path.read_bytes()).hexdigest()
    b['assets'] = [dict(id='I1', kind='image', image_path=str(image_path), sha256=digest, width=20, height=20)]
    c['assets']['items'] = [dict(id='A1', kind='figure', caption='控制图', source_image_id='I1',
        storage='image', section_ids=['S1'], unit_ids=['U1'], meaning='控制关系', purpose='复现步骤', materials_needed=['角度'])]
    c['redaction_plan'].append(dict(id='R2', entity_key='signature', scope='image', action='mask',
        image_ref=dict(asset_id='I1', rendition_sha256=digest, width=20, height=20),
        redactions=[dict(x0=0, y0=0, x1=5, y1=5)], location_status='located'))
    result = prepare(tmp_path, c, b)
    root = Path(result['package_path'])
    asset = json.loads((root / 'assets.json').read_text(encoding='utf8'))['items'][0]
    with Image.open(root / asset['file']) as im:
        assert im.getpixel((1, 1)) == (0, 0, 0)
        assert im.getpixel((10, 10)) == (255, 255, 255)
    assert hashlib.sha256(image_path.read_bytes()).hexdigest() == digest


def test_drifted_image_fails_closed(tmp_path):
    from PIL import Image
    c, b, _ = fixture()
    image_path = tmp_path / 'image.png'
    Image.new('RGB', (20, 20), 'white').save(image_path)
    b['assets'] = [dict(id='I1', image_path=str(image_path), sha256='b' * 64, width=20, height=20)]
    c['assets']['items'] = [dict(id='A1', source_image_id='I1', storage='image', section_ids=['S1'], meaning='结构')]
    with pytest.raises(ValueError, match='image'):
        prepare(tmp_path, c, b)


def test_delivery_pending_receipt_never_rewrites(tmp_path):
    from preextract_publish import deliver_publication
    receipt = tmp_path / 'receipt.json'
    receipt.write_text(json.dumps(dict(status='pending')), encoding='utf8')
    with pytest.raises(ValueError, match='pending|uncertain'):
        deliver_publication({}, config=None, receipt_path=receipt)


def test_long_asset_learning_is_split_without_losing_meaning(tmp_path):
    c, b, _ = fixture()
    meaning = '图示控制过程。' * 600
    c['assets']['items'] = [dict(id='A1', kind='figure', caption='控制图', storage='description',
        meaning=meaning, purpose='复现步骤', materials_needed=['角度'], section_ids=['S1'], unit_ids=['U1'])]
    result = prepare(tmp_path, c, b)
    packet = result['packet']
    assert packet['assets'][0]['meaning'] == meaning
    assert len([r for r in packet['chunks'] if r['kind'] == 'chart']) > 1
    assert all(r['asset_ids'] == ['A1'] for r in packet['chunks'] if r['kind'] == 'chart')


def test_delivery_persists_failure_and_success_order(tmp_path, monkeypatch):
    from preextract_publish import deliver_publication
    result = prepare(tmp_path)
    import paperflow.rag as rag
    calls = []
    class Publisher(rag.RagPublisher):
        def __init__(self, client, on_change=None):
            super().__init__(client, on_change)
        def publish(self, task, packet, receipts):
            calls.append('publish')
            receipts['080801'] = {'body': {'document_id': 'owned-document'}}
            self.on_change(receipts)
        def verify(self, *args):
            calls.append('verify')
        def set_enabled(self, *args):
            calls.append('enable')
        def verify_retrieval(self, *args):
            calls.append('retrieval')
    monkeypatch.setattr(rag, 'RagPublisher', Publisher)
    monkeypatch.setattr(rag, 'native_client', lambda config: object())
    receipt = tmp_path / 'complete.json'
    outcome = deliver_publication(result, config=object(), receipt_path=receipt)
    assert calls == ['publish', 'verify', 'enable', 'retrieval']
    assert outcome['status'] == 'complete'
    class FailedPublisher(Publisher):
        def publish(self, *args):
            super().publish(*args)
            raise ConnectionError('connection lost after write')
    monkeypatch.setattr(rag, 'RagPublisher', FailedPublisher)
    failed = tmp_path / 'failed.json'
    with pytest.raises(ConnectionError):
        deliver_publication(result, config=object(), receipt_path=failed)
    recorded = json.loads(failed.read_text(encoding='utf8'))
    assert recorded['status'] == 'uncertain'
    assert recorded['targets']['080801']['body']['document_id'] == 'owned-document'
    before = len(calls)
    with pytest.raises(ValueError, match='uncertain'):
        deliver_publication(result, config=object(), receipt_path=failed)
    assert len(calls) == before
