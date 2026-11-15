"""Candidate contract tests: failures must retain evidence, never invent it."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from preextract_contract import load_template, parse_candidate, validate_candidate


TEMPLATE = Path(r'E:\rag mid\exports\论文预提取_空白模板_v1.md')


def empty(value):
    if isinstance(value, dict):
        return {key: empty(item) for key, item in value.items()}
    if isinstance(value, list):
        return []
    return value


def fixture():
    _, skeleton = load_template(TEMPLATE)
    candidate = empty(skeleton)
    candidate['record_kind'] = 'filled_candidate'
    candidate['profile']['material_type'] = '论文正文'
    ref = dict(file_ref='source-1', block_id='b1', section_id='s1', asset_id=None,
               start=None, end=None, source_kind='author_text', locator_hint=None)
    candidate['profile']['material_disposition'] = dict(decision='keep', reason='有完整研究过程', source_refs=[ref])
    candidate['profile']['article'].update(title='排程研究', summary='比较两种排程方法', source_refs=[ref])
    candidate['profile']['research_profile'].update(problem='如何缩短等待', object='排程', overall_path='构造案例并比较', unit_sequence=['U1'], source_refs=[ref])
    unit = empty(skeleton['profile']['learning_units'][0])
    unit.update(id='U1', title='比较排程', question='哪种等待更少', what_to_learn='组织比较实验',
                method='对比', is_core=True, work_attribution='author_work', source_refs=[ref], section_ids=['s1'])
    unit['method_evidence'].update(status='documented', detail='有比较过程', source_refs=[ref])
    unit['materials'] = [dict(id='M1', type='案例', description='输入作业集', key_fields=['作业时长'], time_scope=None, comparison_objects=[], organization=None, source_refs=[ref])]
    unit['steps'] = [dict(id='ST1', operation='按两种规则排序并计时', input_refs=['M1'], output_refs=['O1'], source_refs=[ref])]
    unit['outputs'] = [dict(id='O1', type='比较', content='形成两组等待时间', source_refs=[ref])]
    candidate['profile']['learning_units'] = [unit]
    candidate['profile']['routing_candidates'] = [dict(code='080901', name='错误名称可规范化', reason='算法比较', unit_ids=['U1'], source_refs=[ref])]
    section = empty(skeleton['tree']['nodes'][0])
    section.update(id='s1', title='实验', order=1, source_refs=[ref], unit_ids=['U1'])
    candidate['tree']['nodes'] = [section]
    bundle = dict(version='preextract-material/1.0', source=dict(candidate['source'], source_id='source-1', file='input.docx', source_sha256='a'*64),
                  blocks=[dict(id='b1', source_block_id='p3', type='paragraph', text='作者😀张三使用两种排序方案比较等待时间。', section_id='s1', position={'paragraph':3}, order=1)],
                  sections=[dict(id='s1', title='实验', parent_id=None, level=1, order=1, block_ids=['b1'])],
                  assets=[], issues=[], attachments=[])
    catalog = dict(majors=[dict(code='080901', name='计算机科学与技术')])
    return candidate, bundle, catalog


class FormatCompatibilityTests(unittest.TestCase):
    def test_empty_string_relation_is_an_empty_array_not_an_empty_identifier(self):
        data, bundle, catalog = fixture()
        data['profile']['learning_units'][0]['section_ids'] = ''
        original = copy.deepcopy(data)
        result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertTrue(result['valid'], result['errors'])
        self.assertEqual(result['candidate']['profile']['learning_units'][0]['section_ids'], [])
        self.assertEqual(result['candidate']['profile']['learning_units'][0]['source_refs'], original['profile']['learning_units'][0]['source_refs'])
        self.assertEqual(data, original)

    def test_moved_placeholder_cannot_supply_the_only_core_evidence(self):
        for placeholder in ('[解析说明：此位置的原文图形需定向识别。]',
                            '[解析说明：表格内图形需定向识别。]',
                            '[解析说明：此位置存在未解析的正文对象。]', ''):
            with self.subTest(placeholder=placeholder):
                data, bundle, catalog = fixture()
                unit = data['profile']['learning_units'][0]
                unit['source_refs'] = []
                unit['asset_ids'] = ['b1']
                bundle['blocks'][0]['text'] = placeholder
                original = copy.deepcopy(data)
                result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
                self.assertFalse(result['valid'])
                self.assertIn('required_evidence', {x['code'] for x in result['errors']})
                self.assertEqual(result['candidate']['profile']['learning_units'][0]['steps'], unit['steps'])
                self.assertEqual(data, original)
                again = validate_candidate(result['candidate'], bundle, catalog, redaction_policy='best_effort')
                self.assertFalse(again['valid'])
                self.assertEqual(again['candidate'], result['candidate'])

    def test_placeholder_cannot_justify_discarding_an_unresolved_core_reference(self):
        data, bundle, catalog = fixture()
        unit = data['profile']['learning_units'][0]
        unit['source_refs'] = copy.deepcopy(unit['source_refs']) + [dict(block_id='missing', section_id='s1')]
        bundle['blocks'][0]['text'] = '[解析说明：此位置的原文图形需定向识别。]'
        result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertFalse(result['valid'])
        self.assertIn('source_reference', {x['code'] for x in result['errors']})
        self.assertEqual(result['candidate']['profile']['learning_units'][0]['source_refs'][1]['block_id'], 'missing')
        # A real independent source still permits local reference precision loss.
        bundle['blocks'].append(dict(id='b2', text='两种规则分别测得等待时间。', section_id='s1'))
        unit['source_refs'].append(dict(block_id='b2', section_id='s1'))
        self.assertTrue(validate_candidate(data, bundle, catalog, redaction_policy='best_effort')['valid'])

    def test_unknown_extension_with_malformed_references_stays_a_structured_error(self):
        data, bundle, catalog = fixture()
        data['unknown_extension'] = dict(source_refs=[dict(block_id='missing', section_id='s1'),
                                                     dict(block_id={}, section_id='s1')])
        result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertFalse(result['valid'])
        self.assertIn('unknown_field', {x['code'] for x in result['errors']})
        self.assertEqual(result['candidate']['unknown_extension'], data['unknown_extension'])

    def test_table_compatibility_respects_explicitly_unread_selected_image(self):
        from PIL import Image
        data, bundle, catalog = fixture()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'table.png'
            Image.new('RGB', (20, 10), 'white').save(path)
            raw = dict(kind='image', image_path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                       width=20, height=10, text='', block_ids=['b1'], section_ids=['s1'])
            bundle['assets'] = [dict(raw, id='im1'), dict(raw, id='im2')]
            data['assets']['items'] = [dict(id='im1', kind='image', storage='image', source_image_id='im2',
                evidence_basis='table_text', meaning='保留原回答', section_ids=['s1'], unit_ids=['U1'],
                source_refs=[dict(asset_id='im1')], limits=[]), dict(id='im2', kind='image', storage='omit')]
            data['source']['reading_scope']['unread_refs'] = ['im2']
            result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
            self.assertFalse(result['valid'])
            self.assertIn('table_evidence', {x['code'] for x in result['errors']})
            data['source']['reading_scope']['unread_refs'] = []
            accepted = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
            self.assertTrue(accepted['valid'], accepted['errors'])
            self.assertEqual(accepted['candidate']['assets']['items'][0]['source_image_id'], 'im2')
            self.assertEqual(accepted['candidate']['assets']['items'][0]['meaning'], '保留原回答')

    def test_single_object_pointer_and_moved_asset_pointer_keep_same_target(self):
        data, bundle, catalog = fixture()
        unit = data['profile']['learning_units'][0]
        unit['materials'] = unit['materials'][0]
        unit['asset_ids'] = ['b1']
        data['gaps'] = [dict(id='G1', type='uncertain', target_pointer='/profile/learning_units/0/materials/description',
            description='原限制', use_limit='保持原文'),
            dict(id='G2', type='uncertain', target_pointer='/profile/learning_units/0/asset_ids/0',
                 description='正文关联', use_limit='不充当图片')]
        result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertTrue(result['valid'], result['errors'])
        from preextract_contract import resolve_pointer
        self.assertEqual(resolve_pointer(result['candidate'], result['candidate']['gaps'][0]['target_pointer']), '输入作业集')
        self.assertEqual(resolve_pointer(result['candidate'], result['candidate']['gaps'][1]['target_pointer']), 'b1')
        again = validate_candidate(result['candidate'], bundle, catalog, redaction_policy='best_effort')
        self.assertEqual(result['candidate'], again['candidate'])

    def test_no_reference_fallback_across_fields_or_invalid_ranges_or_files(self):
        for fault in ('range', 'file', 'other_field'):
            with self.subTest(fault=fault):
                data, bundle, catalog = fixture()
                unit = data['profile']['learning_units'][0]
                broken = dict(block_id='missing', section_id='s1')
                if fault == 'range':broken.update(start=0, end=2)
                if fault == 'file':broken['file_ref'] = 'another-document'
                unit['source_refs'] = [broken] if fault == 'other_field' else copy.deepcopy(unit['source_refs']) + [broken]
                result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
                self.assertFalse(result['valid'])
                self.assertTrue(any(r['code'] == 'source_reference' for r in result['errors']))

    def test_ambiguous_number_match_cannot_supply_the_only_core_source(self):
        data, bundle, catalog = fixture()
        bad = 'block_90da76df6f2bdce8591'
        for ident in ('block_090da76df6f2bdce8591', 'block_190da76df6f2bdce8591'):
            bundle['blocks'].append(dict(id=ident, text='目\n录', section_id='s1'))
        data['profile']['learning_units'][0]['source_refs'] = [dict(file_ref='source-1',
            block_id=bad, section_id='s1', locator_hint='目录标题')]
        result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertFalse(result['valid'])
        self.assertEqual(result['candidate']['profile']['learning_units'][0]['source_refs'][0]['block_id'], bad)

    def test_reference_arrays_and_integer_strings_are_losslessly_normalized(self):
        data, bundle, catalog = fixture()
        data['profile']['article']['source_refs'] = data['profile']['article']['source_refs'][0]
        data['profile']['learning_units'][0]['section_ids'] = 's1'
        data['tree']['nodes'][0]['order'] = '1'
        original = copy.deepcopy(data)
        result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertTrue(result['valid'], result['errors'])
        self.assertEqual(result['candidate']['profile']['article']['source_refs'], [original['profile']['article']['source_refs']])
        self.assertEqual(result['candidate']['profile']['learning_units'][0]['section_ids'], ['s1'])
        self.assertEqual(data, original)
        self.assertTrue(result['compatibility_report'])

    def test_source_classification_unknown_preserves_attribution_and_content(self):
        data, bundle, catalog = fixture()
        data = json.loads(json.dumps(data))
        unit = data['profile']['learning_units'][0]
        unit['work_attribution'] = 'mixed'
        unit['source_refs'][0]['source_kind'] = 'mixed'
        original = copy.deepcopy(data)
        result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertTrue(result['valid'], result['errors'])
        saved = result['candidate']['profile']['learning_units'][0]
        self.assertIsNone(saved['source_refs'][0]['source_kind'])
        self.assertEqual(saved['work_attribution'], 'mixed')
        for key in ('materials', 'steps', 'outputs', 'method', 'question'):
            self.assertEqual(saved[key], unit[key])
        self.assertTrue(result['candidate']['gaps'])
        again = validate_candidate(result['candidate'], bundle, catalog, redaction_policy='best_effort')
        self.assertTrue(again['valid'], again['errors'])
        self.assertEqual(again['candidate'], result['candidate'])
        self.assertEqual(data, original)

    def test_number_typo_needs_unique_same_section_and_exact_locator(self):
        data, bundle, catalog = fixture()
        known = 'block_090da76df6f2bdce8591'
        bad = 'block_90da76df6f2bdce8591'
        bundle['blocks'].append(dict(id=known, text='目\n录', section_id='s1'))
        data['tree']['nodes'][0]['source_refs'].append(dict(file_ref='source-1', block_id=bad,
            section_id='s1', locator_hint='目录标题'))
        result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertTrue(result['valid'], result['errors'])
        self.assertEqual(result['candidate']['tree']['nodes'][0]['source_refs'][-1]['block_id'], known)
        # A similar hash alone does not identify a research source.
        data['profile']['learning_units'][0]['source_refs'] = [dict(file_ref='source-1',
            block_id=bad, section_id='s1', locator_hint='不知道')]
        blocked = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertFalse(blocked['valid'])
        self.assertIn('source_reference', {x['code'] for x in blocked['errors']})

    def test_redundant_broken_reference_is_local_but_sole_core_reference_still_blocks(self):
        data, bundle, catalog = fixture()
        unit = data['profile']['learning_units'][0]
        original = copy.deepcopy(unit)
        unit['source_refs'] = copy.deepcopy(unit['source_refs']) + [dict(block_id='missing', section_id='s1')]
        result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertTrue(result['valid'], result['errors'])
        saved = result['candidate']['profile']['learning_units'][0]
        self.assertEqual(saved['source_refs'][0], original['source_refs'][0])
        self.assertIsNone(saved['source_refs'][1]['block_id'])
        self.assertEqual(saved['outputs'], original['outputs'])
        self.assertTrue(result['candidate']['gaps'])
        unit['source_refs'] = [dict(block_id='missing', section_id='s1')]
        self.assertFalse(validate_candidate(data, bundle, catalog, redaction_policy='best_effort')['valid'])

    def test_block_in_asset_list_moves_to_real_source_without_inventing_image(self):
        data, bundle, catalog = fixture()
        unit = data['profile']['learning_units'][0]
        unit['asset_ids'] = ['b1']
        original = copy.deepcopy(data)
        result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertTrue(result['valid'], result['errors'])
        saved = result['candidate']['profile']['learning_units'][0]
        self.assertEqual(saved['asset_ids'], [])
        self.assertTrue(any(r.get('block_id') == 'b1' for r in saved['source_refs']))
        self.assertEqual(saved['materials'], unit['materials'])
        self.assertEqual(result['candidate']['assets']['items'], [])
        self.assertTrue(result['candidate']['gaps'])
        self.assertEqual(data, original)

    def test_image_table_label_becomes_limited_unknown_not_claimed_pixel_reading(self):
        from PIL import Image
        data, bundle, catalog = fixture()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'table.png'
            Image.new('RGB', (20, 10), 'white').save(path)
            bundle['assets'] = [dict(id='im1', kind='image', image_path=str(path),
                sha256=hashlib.sha256(path.read_bytes()).hexdigest(), width=20, height=10,
                text='', block_ids=['b1'], section_ids=['s1'])]
            data['assets']['items'] = [dict(id='im1', kind='image', storage='image',
                evidence_basis='table_text', meaning='原回答的研究说明', purpose='比较',
                section_ids=['s1'], unit_ids=['U1'], source_refs=[dict(asset_id='im1')], limits=[])]
            result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
            self.assertTrue(result['valid'], result['errors'])
            saved = result['candidate']['assets']['items'][0]
            self.assertIsNone(saved['evidence_basis'])
            self.assertEqual(saved['meaning'], '原回答的研究说明')
            self.assertEqual(saved['storage'], 'image')
            self.assertTrue(saved['limits'])
            self.assertEqual(result['candidate']['source']['reading_scope']['read_refs'], [])
            again = validate_candidate(result['candidate'], bundle, catalog, redaction_policy='best_effort')
            self.assertTrue(again['valid'], again['errors'])
            self.assertEqual(again['candidate'], result['candidate'])
            data['source']['reading_scope']['unread_refs'] = ['im1']
            self.assertFalse(validate_candidate(data, bundle, catalog, redaction_policy='best_effort')['valid'])

    def test_duplicate_reference_keys_merge_without_last_value_wins(self):
        repairs = []
        raw = '{"source_refs":[{"block_id":"b1"}],"source_refs":[],"x":{"source_refs":[{"block_id":"b2"}],"source_refs":[{"block_id":"b3"}]}}'
        result = parse_candidate(raw, repairs=repairs)
        self.assertEqual(result['source_refs'], [dict(block_id='b1')])
        self.assertEqual(result['x']['source_refs'], [dict(block_id='b2'), dict(block_id='b3')])
        self.assertTrue(repairs)
        for invalid in ('{"title":"A","title":"B"}',
                        '{"source_refs":[],"source_refs":"b1"}',
                        '{"source_refs":[{"block_id":"b1"}],"source_refs":[{"block_id":"b2"}],"target_pointer":"/source_refs/0/block_id"}',
                        '{"content":"固定"user"校验"}'):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                parse_candidate(invalid)


class ContractTests(unittest.TestCase):
    def test_optional_split_points_accept_boundaries_and_skip_only_unusable_hints(self):
        data, bundle, catalog = fixture()
        data['segmentation']['split_suggestions'] = [dict(source_refs=[], reason='可选边界',
            split_points=[dict(block_id='b1', offset=n) for n in (0, 2, len(bundle['blocks'][0]['text']), -1, '3')]
                + [dict(block_id='absent', offset=1)]),
            dict(source_refs=[dict(block_id='invented')], reason='来源错误的建议',
                 split_points=[dict(block_id='b1', offset=4)])]
        original = copy.deepcopy(data)
        result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertTrue(result['valid'], result['errors'])
        self.assertEqual(result['executable_split_points'], [dict(block_id='b1', offset=n)
            for n in (0, 2, len(bundle['blocks'][0]['text']), 3)])
        self.assertTrue(result['segmentation_warnings'])
        self.assertEqual(data, original)
        data['tree']['nodes'][0]['source_refs'] = [dict(block_id='invented')]
        self.assertFalse(validate_candidate(data, bundle, catalog, redaction_policy='best_effort')['valid'])

    def test_bounded_local_gap_does_not_make_available_core_material_unusable(self):
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        gap = empty(skeleton['gaps'][0])
        gap.update(id='G1', type='not_read', target_pointer='/profile/learning_units/0/materials',
                   description='附加版式未核对', use_limit='只学习已记录的材料与步骤')
        data.update(record_kind='partial_candidate', gaps=[gap])
        result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertTrue(result['valid'], result['errors'])
        self.assertEqual(result['candidate']['record_kind'], 'partial_candidate')
        self.assertEqual(result['candidate']['gaps'], data['gaps'])
        self.assertTrue(result['admission']['allow_partial'])
        # An empty core target is different from a local limitation on content
        # which is actually present. No prose is interpreted as a repair.
        gap['target_pointer'] = '/profile/learning_units/0/formulas_parameters'
        blocked = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertIn('critical_gap', {row['code'] for row in blocked['errors']})

    def test_unread_core_unit_or_whole_research_is_not_a_local_gap_even_with_prose_limit(self):
        for pointer in ('/profile/research_profile', '/profile/learning_units', '/profile/learning_units/0'):
            data, bundle, catalog = fixture()
            _, skeleton = load_template(TEMPLATE)
            gap = empty(skeleton['gaps'][0])
            gap.update(id='G1', type='not_read', target_pointer=pointer,
                       description='核心部分未阅读', use_limit='请自行核对原稿')
            data['gaps'] = [gap]
            result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
            self.assertIn('critical_gap', {r['code'] for r in result['errors']})

    def test_bad_optional_reference_type_does_not_crash_or_discard_other_groups(self):
        for bad in ({'bad':'value'}, ['main'], 2, False):
            data, bundle, catalog = fixture()
            data['segmentation']['split_suggestions'] = [dict(source_refs=[dict(file_ref=bad)],
                split_points=[dict(block_id='b1', offset=1)], reason='坏建议'),
                dict(source_refs=[], split_points=[dict(block_id='b1', offset=2)], reason='有效建议')]
            result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
            self.assertTrue(result['valid'], result['errors'])
            self.assertEqual(result['executable_split_points'], [dict(block_id='b1', offset=2)])

    def test_whole_core_image_needs_reading_and_actual_meaning_before_local_gap_admission(self):
        from PIL import Image
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'image.png'
            Image.new('RGB', (20, 10), 'white').save(path)
            bundle['assets'] = [dict(id='image_1', kind='image', image_path=str(path),
                sha256=hashlib.sha256(path.read_bytes()).hexdigest(), width=20, height=10)]
            asset = empty(skeleton['assets']['items'][0])
            asset.update(id='image_1', kind='figure', storage='image', evidence_basis='pixels', unit_ids=['U1'])
            data['assets']['items'] = [asset]
            gap = empty(skeleton['gaps'][0])
            gap.update(id='G1', type='not_read', target_pointer='/assets/items/0', use_limit='回看原图')
            data.update(record_kind='partial_candidate', gaps=[gap])
            for kind in ('not_read', 'extraction_missing'):
                gap['type'] = kind
                result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
                self.assertIn('critical_gap', {row['code'] for row in result['errors']})
            asset['meaning'] = '比较实验的两组轨迹'
            limited = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
            self.assertTrue(limited['valid'], limited['errors'])
            gap['type'] = 'not_read'
            self.assertFalse(validate_candidate(data, bundle, catalog, redaction_policy='best_effort')['valid'])

    def test_identifiers_alone_are_not_learning_content_for_local_gap_admission(self):
        for field, rows in (('materials', [dict(id='M1')]), ('steps', [dict(id='ST1')]),
                            ('outputs', [dict(id='O1')]), ('formulas_parameters', [dict(name='公式1')])):
            data, bundle, catalog = fixture()
            _, skeleton = load_template(TEMPLATE)
            data['profile']['learning_units'][0][field] = rows
            gap = empty(skeleton['gaps'][0])
            gap.update(id='G1', type='not_read', target_pointer=f'/profile/learning_units/0/{field}',
                       description='只有索引未取得内容', use_limit='需要回看原稿')
            data.update(record_kind='partial_candidate', gaps=[gap])
            result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
            self.assertIn('critical_gap', {row['code'] for row in result['errors']}, field)
            # The same content predicate protects a partially extracted whole
            # unit, without turning populated fields into new requirements.
            if field in ('materials', 'steps', 'outputs'):
                gap.update(type='extraction_missing', target_pointer='/profile/learning_units/0')
                result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
                self.assertIn('critical_gap', {row['code'] for row in result['errors']}, field)

    def test_fallback_pages_are_optional_evidence_not_a_read_every_page_gate(self):
        from PIL import Image
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'page.png'
            Image.new('RGB', (20, 10), 'white').save(path)
            sha = hashlib.sha256(path.read_bytes()).hexdigest()
            bundle['assets'] = [dict(id=f'page_{i:04}', kind='source_page', image_path=str(path),
                                     sha256=sha, width=20, height=10, text='') for i in (1, 2)]
            bundle['issues'] = [dict(code='embedded_object_review', position={'part':'word/document.xml', 'object_ref':'object_1'},
                page_evidence_ids=['page_0001', 'page_0002'], page_evidence_status='provided_full_document_fallback')]
            data['source']['reading_scope']['unread_refs'] = ['page_0002']
            gap = empty(skeleton['gaps'][0])
            gap.update(id='G1', type='not_read', target_pointer='/profile/learning_units/0/source_refs',
                       description='整页备用图未全部复核', use_limit='依已提取正文学习，不声称全部图形已核验')
            data.update(record_kind='partial_candidate', gaps=[gap])
            result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
            self.assertTrue(result['valid'], result['errors'])
            self.assertEqual(result['candidate']['source']['reading_scope']['unread_refs'], ['page_0002'])
            codes = {r['code'] for r in result['warnings']}
            self.assertIn('optional_page_disposition', codes)
            self.assertIn('material_review_limit', codes)
            self.assertNotIn('material_visual_evidence', codes)
            # A real figure is not an optional whole-page duplicate.
            bundle['assets'].append(dict(id='image_1', kind='image', image_path=str(path), sha256=sha, width=20, height=10))
            self.assertIn('asset_coverage', {r['code'] for r in validate_candidate(data, bundle, catalog, redaction_policy='best_effort')['errors']})
            bundle['assets'].pop()
            bundle['issues'][0]['critical'] = True
            self.assertIn('material_gap', {r['code'] for r in validate_candidate(data, bundle, catalog, redaction_policy='best_effort')['errors']})
            bundle['issues'][0].pop('critical')
            path.write_bytes(b'changed')
            self.assertIn('image_version', {r['code'] for r in validate_candidate(data, bundle, catalog, redaction_policy='best_effort')['errors']})

    def test_rendered_review_hint_is_not_proof_that_research_content_is_missing(self):
        from PIL import Image
        data, bundle, catalog = fixture()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'page.png'
            Image.new('RGB', (20, 10), 'white').save(path)
            bundle['assets'] = [dict(id='page_1', kind='source_page', image_path=str(path),
                sha256=hashlib.sha256(path.read_bytes()).hexdigest(), width=20, height=10)]
            issue = dict(code='pdf_vector_review', position={'page':1, 'bbox':[1, 2, 19, 3]},
                         page_evidence_ids=['page_1'], page_evidence_status='provided')
            bundle['issues'] = [issue]
            result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
            self.assertTrue(result['valid'], result['errors'])
            self.assertIn('material_review_limit', {r['code'] for r in result['warnings']})
            for code in ('formula_review', 'image_missing', 'page_render_failed'):
                issue['code'] = code
                self.assertIn('material_gap', {r['code'] for r in validate_candidate(data, bundle, catalog, redaction_policy='best_effort')['errors']})

    def test_deterministically_removed_object_issue_is_not_a_research_reading_gap(self):
        data, bundle, catalog = fixture()
        position = dict(part='word/document.xml', body_index=1, object_ref='object_1')
        bundle['removed'] = [dict(reason='bounded_cover_before_abstract', block=dict(position=position))]
        issue = dict(code='embedded_object_review', position=copy.deepcopy(position))
        bundle['issues'] = [issue]
        result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertTrue(result['valid'], result['errors'])
        self.assertIn('removed_material_issue', {r['code'] for r in result['warnings']})
        issue['position']['object_ref'] = 'object_2'
        self.assertFalse(validate_candidate(data, bundle, catalog, redaction_policy='best_effort')['valid'])

    def test_best_effort_preserves_good_source_locations_and_omits_private_metadata(self):
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        row = empty(skeleton['redaction_plan'][0])
        ref = dict(data['profile']['article']['source_refs'][0], start=None, end=None)
        row.update(id='R1', entity_key='author', scope='source_text', action='replace',
                   replacement='研究者甲', expected_text='张三', source_refs=[ref, dict(ref, block_id='missing')],
                   location_status='uncertain')
        meta = empty(skeleton['redaction_plan'][0])
        meta.update(id='R2', scope='metadata', action='delete', location_status='unavailable')
        data['redaction_plan'] = [row, meta]
        original = copy.deepcopy(data)
        result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertTrue(result['valid'], result['errors'])
        actions = result['executable_redactions']
        self.assertEqual(len(actions), 1)
        self.assertEqual(len(actions[0]['source_refs']), 1)
        self.assertEqual(actions[0]['source_refs'][0]['start'], 3)
        self.assertEqual(actions[0]['location_status'], 'located')
        self.assertTrue(result['redaction_report']['warnings'])
        self.assertEqual(data, original)
        # A public pointer must be checked/executed, never mistaken for private
        # source metadata which the publisher omits by construction.
        data['profile']['article']['title'] = '张三的排序研究'
        data['redaction_plan'] = [dict(row, scope='filename', source_refs=[],
            target_pointer='/profile/article/title')]
        pointed = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertTrue(pointed['valid'])
        self.assertEqual(pointed['executable_redactions'][0]['scope'], 'generated_field')
        data['redaction_plan'][0]['target_pointer'] = '/profile/article/absent'
        bad = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertTrue(bad['valid'])
        self.assertFalse(bad['executable_redactions'])
        self.assertTrue(bad['redaction_report']['warnings'])

    def test_best_effort_partial_is_usable_only_with_bounded_noncritical_issues(self):
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        data['record_kind'] = 'partial_candidate'
        self.assertFalse(validate_candidate(data, bundle, catalog, redaction_policy='best_effort')['valid'])
        gap = empty(skeleton['gaps'][0])
        gap.update(id='G1', type='source_not_stated', target_pointer='/profile/education_level', description='学历未说明')
        data['gaps'] = [gap]
        result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertTrue(result['valid'], result['errors'])
        self.assertEqual(result['candidate']['record_kind'], 'partial_candidate')
        self.assertTrue(result['admission']['allow_partial'])
        gap.update(type='not_read', target_pointer='/profile/learning_units/0')
        self.assertFalse(validate_candidate(data, bundle, catalog, redaction_policy='best_effort')['valid'])

    def test_identical_tree_node_dedup_keeps_pointer_on_intended_later_node(self):
        data, bundle, catalog = fixture()
        first = data['tree']['nodes'][0]
        later = copy.deepcopy(first)
        later.update(id='s2', title='张三总结', parent_id='invented', order=999,
                     source_refs=[dict(first['source_refs'][0], block_id=None, section_id='s2')])
        data['tree']['nodes'] = [first, copy.deepcopy(first), later]
        bundle['sections'].append(dict(id='s2', title='张三总结', parent_id='s1', order=1, block_ids=[]))
        _, skeleton = load_template(TEMPLATE)
        row = empty(skeleton['redaction_plan'][0])
        row.update(id='R1', entity_key='author', scope='generated_field', action='replace',
                   expected_text='张三', replacement='研究者甲', location_status='located', target_pointer='/tree/nodes/2/title')
        data['redaction_plan'] = [row]
        result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
        self.assertTrue(result['valid'], result['errors'])
        self.assertEqual(len(result['candidate']['tree']['nodes']), 2)
        self.assertEqual(result['candidate']['tree']['nodes'][1]['parent_id'], 's1')
        self.assertEqual(result['candidate']['tree']['nodes'][1]['order'], 1)
        self.assertEqual(result['executable_redactions'][0]['target_pointer'], '/tree/nodes/1/title')
        later['function'] = '不同解释'
        data['tree']['nodes'].append(dict(first, function='不一样'))
        self.assertIn('duplicate_id', {e['code'] for e in validate_candidate(data, bundle, catalog, redaction_policy='best_effort')['errors']})

    def test_best_effort_keeps_good_image_boxes_but_blocks_changed_actual_pixels(self):
        from PIL import Image
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'image.png'
            Image.new('RGB', (20, 10), 'white').save(path)
            sha = hashlib.sha256(path.read_bytes()).hexdigest()
            bundle['assets'] = [dict(id='A1', image_path=str(path), sha256=sha, width=20, height=10)]
            asset = empty(skeleton['assets']['items'][0])
            asset.update(id='A1', source_image_id='A1', storage='image', evidence_basis='pixels')
            data['assets']['items'] = [asset]
            row = empty(skeleton['redaction_plan'][0])
            row.update(id='R1', entity_key='signature', scope='image', action='mask', location_status='located',
                       image_ref=dict(asset_id='A1', rendition_sha256=sha, width=20, height=10),
                       redactions=[dict(x0=0, y0=0, x1=5, y1=5), dict(x0=0, y0=0, x1=25, y1=5)])
            data['redaction_plan'] = [row]
            result = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
            self.assertTrue(result['valid'], result['errors'])
            self.assertEqual(len(result['executable_redactions'][0]['redactions']), 1)
            bundle['assets'][0]['width'] = row['image_ref']['width'] = None
            missing = validate_candidate(data, bundle, catalog, redaction_policy='best_effort')
            self.assertFalse(missing['valid'])
            self.assertFalse(missing['executable_redactions'])
            bundle['assets'][0]['width'] = row['image_ref']['width'] = 20
            Image.new('RGB', (20, 10), 'blue').save(path)
            self.assertIn('image_version', {e['code'] for e in validate_candidate(data, bundle, catalog, redaction_policy='best_effort')['errors']})

    def test_marked_md_and_json_roundtrip_and_duplicate_keys_rejected(self):
        data, _, _ = fixture()
        marked = '<!-- paper-preextract:data:start -->\n```json\n' + json.dumps(data) + '\n```\n<!-- paper-preextract:data:end -->'
        self.assertEqual(parse_candidate(marked), data)
        self.assertEqual(parse_candidate(json.dumps(data)), data)
        with self.assertRaises(ValueError):
            parse_candidate('{"schema_version":"paper-preextract/1.0","schema_version":"other"}')

    def test_normalizes_only_missing_optional_fields_and_authoritative_source(self):
        data, bundle, catalog = fixture()
        del data['profile']['education_level']
        before = copy.deepcopy(data)
        result = validate_candidate(data, bundle, catalog)
        self.assertTrue(result['valid'], result['errors'])
        self.assertEqual(result['candidate']['source']['source_sha256'], 'a'*64)
        self.assertIsNone(result['candidate']['profile']['education_level']['major_as_stated'])
        self.assertEqual(result['candidate']['profile']['routing_candidates'][0]['name'], '计算机科学与技术')
        self.assertEqual(data, before)

    def test_existing_syntax_recovery_preserves_all_scalars_and_records_its_use(self):
        original='{"schema_version":"paper-preextract/1.0","redaction_plan":[]} ,"gaps":[],"attachments":[]}'
        repairs=[];candidate=parse_candidate(original,repairs=repairs)
        from profile_json import _scalars
        self.assertEqual(_scalars(original),_scalars(json.dumps(candidate,ensure_ascii=False)))
        self.assertEqual(candidate['gaps'],[])
        self.assertTrue(repairs[0]['scalar_sequence_preserved'])

    def test_source_conflict_and_nonstring_code_are_blocked(self):
        data, bundle, catalog = fixture()
        data['source']['source_sha256'] = 'b'*64
        data['profile']['routing_candidates'][0]['code'] = 80901
        result = validate_candidate(data, bundle, catalog)
        self.assertFalse(result['valid'])
        self.assertIn('source_mismatch', {r['code'] for r in result['errors']})
        self.assertIn('field_type', {r['code'] for r in result['errors']})

    def test_program_source_file_alias_resolves_to_private_path_without_changing_answer(self):
        for alias in ('main', 'primary'):
            with self.subTest(alias=alias):
                data, bundle, catalog = fixture()
                bundle['file_ref'] = alias
                data['source']['file'] = alias
                data['source']['source_sha256'] = bundle['source']['source_sha256']
                before = copy.deepcopy(data)
                result = validate_candidate(data, bundle, catalog)
                self.assertTrue(result['valid'], result['errors'])
                self.assertEqual(result['candidate']['source']['file'], bundle['source']['file'])
                self.assertIn('normalized_source_reference', {r['code'] for r in result['warnings']})
                self.assertEqual(data, before)

    def test_blank_program_source_facts_are_missing_but_real_conflicts_still_block(self):
        data, bundle, catalog = fixture()
        for key in data['source']:
            if key != 'reading_scope':
                data['source'][key] = ' \t '
        before = copy.deepcopy(data)
        result = validate_candidate(data, bundle, catalog)
        self.assertTrue(result['valid'], result['errors'])
        self.assertEqual(result['candidate']['source']['source_sha256'], 'a' * 64)
        self.assertEqual(data, before)
        for key, value in (('file', 'other.docx'), ('source_id', 'other-source'), ('source_sha256', 'b' * 64)):
            with self.subTest(key=key):
                broken = copy.deepcopy(data)
                broken['source'][key] = value
                result = validate_candidate(broken, bundle, catalog)
                self.assertIn('/source/' + key, {r['path'] for r in result['errors'] if r['code'] == 'source_mismatch'})

    def test_file_alias_must_belong_to_the_primary_material_not_an_attachment(self):
        data, bundle, catalog = fixture()
        bundle['file_ref'] = 'primary'
        bundle['attachments'] = [dict(file_ref='main')]
        data['source']['file'] = 'main'
        result = validate_candidate(data, bundle, catalog)
        self.assertFalse(result['valid'])
        self.assertIn('source_mismatch', {r['code'] for r in result['errors']})

    def test_program_file_ref_main_is_distinct_from_source_identity(self):
        data, bundle, catalog = fixture()
        bundle['file_ref'] = 'main'
        data['profile']['article']['source_refs'][0]['file_ref'] = 'main'
        self.assertTrue(validate_candidate(data, bundle, catalog)['valid'])

    def test_reading_declarations_survive_but_extraction_facts_come_from_program(self):
        data, bundle, catalog = fixture()
        bundle['file_ref'] = 'main'
        bundle['source']['reading_scope'] = dict(read_refs=[], unread_refs=[], extraction_issues=['原生表格已取得'], notes='尚未发送')
        data['source']['reading_scope'] = dict(read_refs=['main', 'source-1', 'b1', 's1'], unread_refs=[], extraction_issues=['模型随意改写的工具事实'], notes='根据提供的材料完成阅读')
        result = validate_candidate(data, bundle, catalog)
        self.assertTrue(result['valid'], result['errors'])
        scope = result['candidate']['source']['reading_scope']
        self.assertEqual(scope['read_refs'], ['main', 'source-1', 'b1', 's1'])
        self.assertEqual(scope['notes'], '根据提供的材料完成阅读')
        self.assertEqual(scope['extraction_issues'], ['原生表格已取得'])

    def test_core_evidence_cannot_be_only_locator_hint_or_file_name(self):
        for ref in ({'locator_hint':'正文某处'}, {'file_ref':'source-1'}):
            data, bundle, catalog = fixture()
            data['profile']['learning_units'][0]['source_refs'] = [ref]
            result = validate_candidate(data, bundle, catalog)
            self.assertFalse(result['valid'])
            self.assertIn('required_evidence', {r['code'] for r in result['errors']})

    def test_unscoped_unread_gap_is_blocking_without_guessing_impact_prose(self):
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        gap = empty(skeleton['gaps'][0])
        gap.update(id='G1', type='not_read', target_pointer=None, description='未读公式', impact='无法核验核心计算', claim_status='explicit')
        data['gaps'] = [gap]
        result = validate_candidate(data, bundle, catalog)
        self.assertFalse(result['valid'])
        self.assertIn('unscoped_gap', {r['code'] for r in result['errors']})

    def test_unrendered_formula_and_failed_page_render_block_publication(self):
        for issue in (dict(code='formula_review', message='公式未完整取得', position={}),
                      dict(code='page_render_failed', message='页面渲染失败', position={})):
            data, bundle, catalog = fixture()
            bundle['issues'] = [issue]
            result = validate_candidate(data, bundle, catalog)
            self.assertFalse(result['valid'])
            self.assertIn('material_gap', {r['code'] for r in result['errors']})

    def test_rendered_issue_requires_explicit_pixel_reading_for_its_actual_page(self):
        from PIL import Image
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'page.png'
            Image.new('RGB', (20, 10), 'white').save(path)
            sha = hashlib.sha256(path.read_bytes()).hexdigest()
            bundle['assets'] = [dict(id='page_0001', kind='source_page', caption='第1页', block_ids=[], section_ids=[], image_path=str(path), sha256=sha, width=20, height=10, text='')]
            bundle['issues'] = [dict(code='formula_review', message='公式原生读取缺失', position={}, page_evidence_ids=['page_0001'])]
            asset = empty(skeleton['assets']['items'][0])
            asset.update(id='page_0001', kind='source_page', source_image_id='page_0001', storage='omit', evidence_basis='pixels')
            data['assets']['items'] = [asset]
            result = validate_candidate(data, bundle, catalog)
            self.assertTrue(result['valid'], result['errors'])
            asset['evidence_basis'] = 'caption_context_only'
            self.assertFalse(validate_candidate(data, bundle, catalog)['valid'])

    def test_noncore_located_missing_education_is_only_a_warning(self):
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        gap = empty(skeleton['gaps'][0])
        gap.update(id='G1', type='extraction_missing', target_pointer='/profile/education_level', description='封面学历字段未取得', claim_status='explicit')
        data['gaps'] = [gap]
        result = validate_candidate(data, bundle, catalog)
        self.assertTrue(result['valid'], result['errors'])

    def test_duplicate_ids_and_unknown_source_reference_are_rejected(self):
        data, bundle, catalog = fixture()
        data['profile']['learning_units'][0]['steps'][0]['id'] = 'M1'
        data['tree']['nodes'][0]['source_refs'][0]['block_id'] = 'made-up'
        result = validate_candidate(data, bundle, catalog)
        self.assertFalse(result['valid'])
        codes = {r['code'] for r in result['errors']}
        self.assertIn('duplicate_id', codes)
        self.assertIn('source_reference', codes)

    def test_dangling_unit_step_and_section_relations_are_blocked(self):
        data, bundle, catalog = fixture()
        data['profile']['research_profile']['unit_sequence'] = ['U999']
        data['profile']['learning_units'][0]['steps'][0]['input_refs'] = ['M999']
        data['tree']['nodes'][0]['parent_id'] = 's999'
        result = validate_candidate(data, bundle, catalog)
        self.assertFalse(result['valid'])
        self.assertIn('record_reference', {r['code'] for r in result['errors']})

    def test_bad_pointer_does_not_resolve_ids_or_wildcards_as_array_positions(self):
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        gap = empty(skeleton['gaps'][0])
        gap.update(id='G1', target_pointer='/profile/learning_units/U1/limitations', type='source_not_stated', description='原文未写', claim_status='explicit')
        data['gaps'] = [gap]
        self.assertFalse(validate_candidate(data, bundle, catalog)['valid'])
        gap['target_pointer'] = '/profile/learning_units/0/limitations'
        self.assertTrue(validate_candidate(data, bundle, catalog)['valid'])

    def test_core_unread_gap_and_partial_candidate_are_retained_but_block_publication(self):
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        gap = empty(skeleton['gaps'][0])
        gap.update(id='G1', target_pointer='/profile/learning_units/0/formulas_parameters', type='not_read', description='公式图片未读', claim_status='explicit')
        data['gaps'] = [gap]
        result = validate_candidate(data, bundle, catalog)
        self.assertFalse(result['valid'])
        self.assertEqual(result['candidate']['record_kind'], 'partial_candidate')
        self.assertEqual(result['candidate']['gaps'][0]['description'], '公式图片未读')

    def test_source_conflict_is_preserved_as_use_limit_without_reviewing_paper_quality(self):
        data,bundle,catalog=fixture();_,skeleton=load_template(TEMPLATE)
        gap=empty(skeleton['gaps'][0])
        gap.update(id='G1',target_pointer='/profile/learning_units/0/limitations',type='source_conflict',
                   description='原文两处数值口径不同',use_limit='只能学习流程，不沿用具体数值',claim_status='explicit')
        data['gaps']=[gap]
        result=validate_candidate(data,bundle,catalog)
        self.assertTrue(result['valid'],result['errors'])
        self.assertEqual(result['candidate']['gaps'][0]['description'],gap['description'])

    def test_unicode_text_redaction_is_exact_and_overlaps_are_blocked(self):
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        row = empty(skeleton['redaction_plan'][0])
        ref = dict(data['profile']['article']['source_refs'][0], start=3, end=5)
        row.update(id='R1', entity_key='author', scope='source_text', action='replace', replacement='研究者', expected_text='张三', source_refs=[ref], location_status='located')
        data['redaction_plan'] = [row]
        self.assertTrue(validate_candidate(data, bundle, catalog)['valid'])
        row['source_refs'][0]['start'] = 2
        result=validate_candidate(data,bundle,catalog)
        self.assertTrue(result['valid'],result['errors'])
        self.assertEqual(result['candidate']['redaction_plan'][0]['source_refs'][0]['start'],3)
        self.assertIn('normalized_redaction_range',{x['code'] for x in result['warnings']})
        row['source_refs'][0]['start'] = 3
        conflicting = copy.deepcopy(row)
        conflicting.update(id='R2', entity_key='different', replacement='另一人')
        data['redaction_plan'].append(conflicting)
        self.assertFalse(validate_candidate(data, bundle, catalog)['valid'])

    def test_generated_redaction_requires_real_string_pointer_and_matching_text(self):
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        row = empty(skeleton['redaction_plan'][0])
        row.update(id='R1', entity_key='object', scope='generated_field', action='replace', replacement='方法研究', expected_text='排程研究', target_pointer='/profile/article/title', location_status='located')
        data['redaction_plan'] = [row]
        self.assertTrue(validate_candidate(data, bundle, catalog)['valid'])
        row['expected_text'] = '不存在'
        self.assertFalse(validate_candidate(data, bundle, catalog)['valid'])

    def test_unique_redaction_range_is_normalized_before_source_range_validation(self):
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        row = empty(skeleton['redaction_plan'][0])
        ref = dict(data['profile']['article']['source_refs'][0], start=999, end=1001)
        row.update(id='R1', entity_key='author', scope='source_text', action='replace',
                   replacement='研究者', expected_text='张三', source_refs=[ref], location_status='located')
        data['redaction_plan'] = [row]
        result = validate_candidate(data, bundle, catalog)
        self.assertTrue(result['valid'], result['errors'])
        normalized = result['candidate']['redaction_plan'][0]['source_refs'][0]
        self.assertEqual((normalized['start'], normalized['end']), (3, 5))
        self.assertEqual((ref['start'], ref['end']), (999, 1001))
        data['profile']['article']['source_refs'][0].update(start=999, end=1001)
        self.assertIn('source_range', {r['code'] for r in validate_candidate(data, bundle, catalog)['errors']})

    def test_generated_redaction_can_target_published_gap_metadata(self):
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        gap = empty(skeleton['gaps'][0])
        gap.update(id='G1', type='source_not_stated', description='张三未说明日期', claim_status='explicit')
        data['gaps'] = [gap]
        row = empty(skeleton['redaction_plan'][0])
        row.update(id='R1', entity_key='author', scope='generated_field', action='replace', replacement='作者甲', expected_text='张三', target_pointer='/gaps/0/description', location_status='located')
        data['redaction_plan'] = [row]
        self.assertTrue(validate_candidate(data, bundle, catalog)['valid'])

    def test_image_redaction_binds_real_pixels_hash_dimensions_and_bounds(self):
        from PIL import Image
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'figure.png'
            Image.new('RGB', (20, 10), 'white').save(path)
            sha = hashlib.sha256(path.read_bytes()).hexdigest()
            bundle['assets'] = [dict(id='A1', kind='figure', caption='图一', block_ids=['b1'], section_ids=['s1'], image_path=str(path), sha256=sha, width=20, height=10, text='')]
            asset = empty(skeleton['assets']['items'][0])
            asset.update(id='A1', kind='figure', caption='图一', source_image_id='A1', section_ids=['s1'], unit_ids=['U1'], storage='image', evidence_basis='pixels')
            data['assets']['items'] = [asset]
            row = empty(skeleton['redaction_plan'][0])
            row.update(id='R1', entity_key='author', scope='image', action='mask', image_ref=dict(asset_id='A1', rendition_sha256=sha, width=20, height=10), redactions=[dict(x0=0,y0=0,x1=5,y1=5)], location_status='located')
            data['redaction_plan'] = [row]
            self.assertTrue(validate_candidate(data, bundle, catalog)['valid'])
            row['image_ref']['width'] = 19
            self.assertFalse(validate_candidate(data, bundle, catalog)['valid'])
            row['image_ref']['width'] = 20
            row['redactions'][0]['x1'] = 21
            self.assertFalse(validate_candidate(data, bundle, catalog)['valid'])
            asset['storage']='omit';row['location_status']='uncertain'
            result=validate_candidate(data,bundle,catalog)
            self.assertTrue(result['valid'],result['errors'])
            self.assertEqual(result['candidate']['redaction_plan'],[])
            self.assertEqual(data['redaction_plan'][0]['location_status'],'uncertain')
            self.assertIn('redaction_image_not_published',{x['code'] for x in result['warnings']})

    def test_shared_source_pixels_keep_mask_when_any_asset_publishes_them(self):
        from PIL import Image
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'figure.png'
            Image.new('RGB', (20, 10), 'white').save(path)
            sha = hashlib.sha256(path.read_bytes()).hexdigest()
            raw = dict(id='A1', kind='figure', block_ids=['b1'], section_ids=['s1'],
                       image_path=str(path), sha256=sha, width=20, height=10, text='')
            bundle['assets'] = [raw, dict(raw, id='A2')]
            asset = empty(skeleton['assets']['items'][0])
            asset.update(id='A1', source_image_id='A1', storage='omit', evidence_basis='pixels')
            data['assets']['items'] = [asset, dict(asset, id='A2', storage='image')]
            row = empty(skeleton['redaction_plan'][0])
            row.update(id='R1', entity_key='author', scope='image', action='mask',
                       image_ref=dict(asset_id='A1', rendition_sha256=sha, width=20, height=10),
                       redactions=[dict(x0=0, y0=0, x1=5, y1=5)], location_status='located')
            data['redaction_plan'] = [row]
            result = validate_candidate(data, bundle, catalog)
            self.assertTrue(result['valid'], result['errors'])
            self.assertEqual(len(result['candidate']['redaction_plan']), 1)
            row['redactions'][0]['x1'] = 21
            result = validate_candidate(data, bundle, catalog)
            self.assertIn('redaction_box', {r['code'] for r in result['errors']})

    def test_unknown_assets_pixels_without_image_and_omitted_tree_are_blocked(self):
        data, bundle, catalog = fixture()
        data['tree']['nodes'] = []
        self.assertFalse(validate_candidate(data, bundle, catalog)['valid'])
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        asset = empty(skeleton['assets']['items'][0])
        asset.update(id='invented', storage='image', evidence_basis='pixels')
        data['assets']['items'] = [asset]
        self.assertFalse(validate_candidate(data, bundle, catalog)['valid'])

    def test_empty_excluded_material_is_accepted_without_forced_research(self):
        data, bundle, catalog = fixture()
        data['profile']['material_disposition']['decision'] = 'exclude'
        data['profile']['learning_units'] = []
        data['profile']['research_profile']['unit_sequence'] = []
        data['profile']['routing_candidates'] = []
        data['tree']['nodes'] = []
        self.assertTrue(validate_candidate(data, bundle, catalog)['valid'])

    def test_malformed_scalar_or_relation_is_reported_without_crashing(self):
        for mutate in (
            lambda c: c['tree']['nodes'][0].update(parent_id=[]),
            lambda c: c['profile']['article']['source_refs'][0].update(file_ref={}),
            lambda c: c['profile']['learning_units'][0].update(is_core='true'),
        ):
            data, bundle, catalog = fixture()
            mutate(data)
            result = validate_candidate(data, bundle, catalog)
            self.assertFalse(result['valid'])
            self.assertIn('field_type', {r['code'] for r in result['errors']})

    def test_missing_redaction_plan_does_not_mean_no_private_information(self):
        data, bundle, catalog = fixture()
        del data['redaction_plan']
        self.assertFalse(validate_candidate(data, bundle, catalog)['valid'])

    def test_rendition_changed_after_material_preparation_is_rejected(self):
        from PIL import Image
        data, bundle, catalog = fixture()
        _, skeleton = load_template(TEMPLATE)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'figure.png'
            Image.new('RGB', (20, 10), 'white').save(path)
            sha = hashlib.sha256(path.read_bytes()).hexdigest()
            bundle['assets'] = [dict(id='A1', kind='figure', caption='图一', block_ids=['b1'], section_ids=['s1'], image_path=str(path), sha256=sha, width=20, height=10, text='')]
            asset = empty(skeleton['assets']['items'][0])
            asset.update(id='A1', kind='figure', source_image_id='A1', storage='image', evidence_basis='pixels')
            data['assets']['items'] = [asset]
            Image.new('RGB', (20, 10), 'black').save(path)
            self.assertFalse(validate_candidate(data, bundle, catalog)['valid'])


if __name__ == '__main__':
    unittest.main()
