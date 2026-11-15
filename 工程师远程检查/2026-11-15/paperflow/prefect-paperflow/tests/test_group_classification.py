import copy
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

try:
    from paperflow_prefect import group_classification as grouping
except ImportError:
    grouping = None


class FixtureJudge:
    def choose_body(self, records):
        body = next((r for r in records if r['relative'] == '论文.docx'), None)
        return {'choice': body['file_id'] if body else None}

    def choose_other(self, record, body):
        return {'choice': 'trash' if '报告' in record['relative'] else 'attachment'}


def readable_fixture(path):
    return {'text': '本地文件证据', 'limitations': []}


class GroupClassificationTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(grouping, '同组分类及交接模块尚不存在')
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'library'
        self.root.mkdir()
        self.judge = FixtureJudge()

    def make_group(self, name, attachment):
        group = self.root / 'school' / name
        group.mkdir(parents=True)
        (group / '论文.docx').write_bytes(b'same body')
        (group / attachment).write_bytes(b'same attachment bytes')
        (group / '报告.pdf').write_bytes(b'report')
        return group

    def classify(self, group):
        return grouping.classify_group(group, self.root, self.judge, readable_fixture)

    def test_identical_names_and_content_stay_in_their_own_group(self):
        first = self.make_group('张三', '张三附件.zip')
        second = self.make_group('李四', '李四附件.zip')
        a, b = self.classify(first), self.classify(second)
        self.assertNotEqual(a['group_id'], b['group_id'])
        self.assertNotEqual(a['body']['file_id'], b['body']['file_id'])
        self.assertEqual(Path(a['body']['path']), (first / '论文.docx').resolve())
        self.assertEqual([r['relative'] for r in a['attachments']], ['张三附件.zip'])
        self.assertEqual([r['relative'] for r in b['attachments']], ['李四附件.zip'])
        self.assertEqual([r['relative'] for r in a['trash']], ['报告.pdf'])
        self.assertTrue(grouping.verify_handoff(a))
        self.assertTrue(grouping.verify_handoff(b))

    def test_cross_group_attachment_is_rejected_before_handoff(self):
        first = self.make_group('张三', '附件.zip')
        second = self.make_group('李四', '附件.zip')
        a, b = self.classify(first), self.classify(second)
        swapped = copy.deepcopy(a)
        swapped['attachments'][0] = b['attachments'][0]
        with self.assertRaises(ValueError):
            grouping.verify_handoff(swapped)

    def test_changed_or_added_file_invalidates_the_group_handoff(self):
        group = self.make_group('张三', '附件.zip')
        handoff = self.classify(group)
        (group / '论文.docx').write_bytes(b'different body')
        with self.assertRaises(ValueError):
            grouping.verify_handoff(handoff)
        fresh = self.classify(group)
        (group / '补交.pdf').write_bytes(b'later')
        with self.assertRaises(ValueError):
            grouping.verify_handoff(fresh)

    def test_empty_group_is_not_confused_with_unreadable_file(self):
        group = self.root / 'empty'
        group.mkdir()
        empty = self.classify(group)
        self.assertEqual(empty['attachment_status'], '空白')
        self.assertIsNone(empty['body'])
        (group / '空文件.docx').write_bytes(b'')
        unreadable = self.classify(group)
        self.assertEqual(unreadable['attachment_status'], '尚未确认')
        self.assertEqual(len(unreadable['unresolved']), 1)
        self.assertEqual(unreadable['unresolved'][0]['relative'], '空文件.docx')
        with self.assertRaises(ValueError): grouping.verify_handoff(unreadable)
        self.assertFalse(grouping.verify_handoff(unreadable, allow_incomplete=True))

    def test_repeated_classification_keeps_same_handoff_identity(self):
        group = self.make_group('张三', '附件.zip')
        a, b = self.classify(group), self.classify(group)
        self.assertEqual(a['handoff_id'], b['handoff_id'])
        self.assertEqual(a['result_version'], b['result_version'])

    def test_ppt_is_an_attachment_even_if_its_name_mentions_defense(self):
        group = self.root / 'person'
        group.mkdir()
        (group / '论文.docx').write_bytes(b'thesis')
        (group / '答辩记录.pptx').write_bytes(b'slide file')
        class TrashJudge(FixtureJudge):
            def choose_other(self, record, body): return {'choice': 'trash'}
        handoff = grouping.classify_group(group, self.root, TrashJudge(), readable_fixture)
        self.assertEqual([r['relative'] for r in handoff['attachments']], ['答辩记录.pptx'])
        self.assertEqual(handoff['trash'], [])

    def test_unreadable_archive_is_preserved_as_attachment(self):
        group = self.root / 'person'
        group.mkdir()
        (group / '论文.docx').write_bytes(b'thesis')
        (group / '未知.zip').write_bytes(b'broken or protected archive')
        def reader(path):
            return ({'archive_type': 'zip', 'read_error': '无法读取目录'}
                    if path.suffix == '.zip' else readable_fixture(path))
        class TrashJudge(FixtureJudge):
            def choose_other(self, record, body): return {'choice': 'trash'}
        handoff = grouping.classify_group(group, self.root, TrashJudge(), reader)
        self.assertEqual([r['relative'] for r in handoff['attachments']], ['未知.zip'])
        self.assertIn('无法读取', handoff['attachments'][0]['reason'])
        self.assertEqual(handoff['trash'], [])

    def test_archive_member_verification_error_is_not_called_directory_failure(self):
        group = self.root / 'person'
        group.mkdir()
        (group / '论文.docx').write_bytes(b'thesis')
        (group / '材料.zip').write_bytes(b'archive')
        def reader(path):
            return ({'archive_type': 'zip', 'read_error': '包内正文加密',
                     '_handoff': {'archive_listing': {'status': 'complete'}}}
                    if path.suffix == '.zip' else readable_fixture(path))
        handoff = grouping.classify_group(group, self.root, self.judge, reader)
        self.assertIn('包内正文无法核验', handoff['attachments'][0]['reason'])
        self.assertNotIn('目录无法读取', handoff['attachments'][0]['reason'])

    def test_one_unreadable_file_does_not_erase_other_group_results(self):
        group = self.make_group('张三', '附件.zip')
        (group / '故障.docx').write_bytes(b'bad')
        def reader(path):
            if path.name == '故障.docx': raise ValueError('unreadable')
            return readable_fixture(path)
        handoff = grouping.classify_group(group, self.root, self.judge, reader)
        self.assertEqual(handoff['body']['relative'], '论文.docx')
        self.assertEqual([r['relative'] for r in handoff['attachments']], ['附件.zip'])
        self.assertEqual([r['relative'] for r in handoff['unresolved']], ['故障.docx'])
        with self.assertRaises(ValueError): grouping.verify_handoff(handoff)
        self.assertFalse(grouping.verify_handoff(handoff, allow_incomplete=True))

    def test_handoff_file_is_read_back_and_never_overwrites_a_different_result(self):
        group = self.make_group('张三', '附件.zip')
        handoff = self.classify(group)
        target = Path(self.temp.name) / 'handoffs' / 'first.json'
        self.assertTrue(hasattr(grouping, 'write_handoff'), '缺少可核验的交接保存入口')
        grouping.write_handoff(target, handoff)
        saved = target.read_bytes()
        grouping.write_handoff(target, handoff)
        self.assertEqual(target.read_bytes(), saved)
        changed = copy.deepcopy(handoff)
        changed['paper_id'] = 'PF_existing'
        with self.assertRaises(ValueError): grouping.write_handoff(target, changed)
        self.assertEqual(target.read_bytes(), saved)
        with self.assertRaises(ValueError): grouping.write_handoff(group / 'handoff.json', handoff)

    def test_changing_the_recorded_reason_invalidates_the_result(self):
        group = self.make_group('张三', '附件.zip')
        handoff = self.classify(group)
        handoff['trash'][0]['reason'] = '事后篡改理由'
        with self.assertRaises(ValueError): grouping.verify_handoff(handoff)

    def test_archive_with_only_the_verified_same_body_is_trash_but_retained(self):
        from paperflow_prefect.group_evidence import read_evidence
        group = self.root / 'person'
        group.mkdir()
        (group / '论文.docx').write_bytes(b'the same document')
        archive_path = group / '重复.zip'
        with zipfile.ZipFile(archive_path, 'w') as archive:
            archive.writestr('论文.docx', b'the same document')
        def reader(path):
            return read_evidence(path) if path.suffix == '.zip' else readable_fixture(path)
        handoff = grouping.classify_group(group, self.root, self.judge, reader)
        self.assertEqual([r['relative'] for r in handoff['trash']], ['重复.zip'])
        self.assertTrue(archive_path.is_file())
        self.assertEqual(handoff['attachments'], [])
        self.assertTrue(grouping.verify_handoff(handoff))

    def test_nested_or_truncated_archive_is_kept_when_inspection_is_incomplete(self):
        group = self.root / 'person'
        group.mkdir()
        (group / '论文.docx').write_bytes(b'thesis')
        (group / '资料.zip').write_bytes(b'archive fixture')
        def reader(path):
            return ({'archive_type': 'zip', 'members': [{'name': '内层.zip'}], 'nested': True}
                    if path.suffix == '.zip' else readable_fixture(path))
        class TrashJudge(FixtureJudge):
            def choose_other(self, record, body): return {'choice': 'trash'}
        handoff = grouping.classify_group(group, self.root, TrashJudge(), reader)
        self.assertEqual([r['relative'] for r in handoff['attachments']], ['资料.zip'])
        self.assertEqual(handoff['trash'], [])

    def test_batch_rejects_overlapping_groups_and_continues_an_independent_group(self):
        parent = self.root / 'school' / 'parent'
        child = parent / 'child'
        child.mkdir(parents=True)
        (child / '论文.docx').write_bytes(b'document')
        independent = self.make_group('李四', '李四附件.zip')
        output = Path(self.temp.name) / 'handoffs'
        self.assertTrue(hasattr(grouping, 'classify_batch'), '缺少独立组批处理入口')
        results = grouping.classify_batch([parent, child, independent], self.root, output,
                                          FixtureJudge, readable_fixture, verified_new_groups=[independent])
        self.assertEqual(len(results), 3)
        self.assertIn('error', results[0])
        self.assertIn('error', results[1])
        self.assertIn('handoff', results[2])
        saved = grouping.read_json(results[2]['handoff'])
        self.assertEqual(saved['group_root'], str(independent.resolve()))
        self.assertEqual([r['relative'] for r in saved['attachments']], ['李四附件.zip'])

    def test_same_group_is_not_run_twice_in_one_batch(self):
        group = self.make_group('张三', '附件.zip')
        output = Path(self.temp.name) / 'handoffs'
        self.assertTrue(hasattr(grouping, 'classify_batch'), '缺少独立组批处理入口')
        results = grouping.classify_batch([group, group], self.root, output,
                                          FixtureJudge, readable_fixture, verified_new_groups=[group])
        self.assertTrue(all('error' in result for result in results))
        self.assertEqual(list(output.glob('*.json')), [])

    def test_missing_title_is_explicit_and_cannot_come_from_an_attachment(self):
        group = self.make_group('张三', '附件.zip')
        handoff = self.classify(group)
        self.assertIsNone(handoff.get('paper_title'))
        self.assertEqual(handoff.get('title_note'), '未从正文中确认论文题目')
        self.assertTrue(grouping.verify_handoff(handoff))

    def test_a_parent_folder_with_multiple_person_subfolders_is_rejected(self):
        parent = self.root / '学院'
        (parent / '甲').mkdir(parents=True)
        (parent / '乙').mkdir()
        (parent / '甲' / '论文.docx').write_bytes(b'thesis')
        (parent / '乙' / '附件.zip').write_bytes(b'attachment')
        with self.assertRaises(ValueError): self.classify(parent)

    def test_explicit_category_subfolders_stay_in_the_same_group(self):
        group = self.root / 'person'
        for name in ('正文', '附件', '垃圾材料'):
            (group / name).mkdir(parents=True)
        (group / '正文' / '论文.docx').write_bytes(b'thesis')
        (group / '附件' / '资料.zip').write_bytes(b'archive')
        class CategoryJudge(FixtureJudge):
            def choose_body(self, records):
                return {'choice': next(r['file_id'] for r in records if r['relative'] == '正文/论文.docx')}
        handoff = grouping.classify_group(group, self.root, CategoryJudge(), readable_fixture)
        self.assertEqual(handoff['body']['relative'], '正文/论文.docx')
        self.assertEqual([r['relative'] for r in handoff['attachments']], ['附件/资料.zip'])
        self.assertTrue(grouping.verify_handoff(handoff))

    def test_person_subfolders_inside_category_folders_cannot_be_mixed(self):
        group = self.root / 'school'
        (group / '正文' / '甲').mkdir(parents=True)
        (group / '附件' / '乙').mkdir(parents=True)
        (group / '正文' / '甲' / '论文.docx').write_bytes(b'thesis')
        (group / '附件' / '乙' / '资料.zip').write_bytes(b'archive')
        with self.assertRaises(ValueError): self.classify(group)

    def test_no_chosen_body_does_not_turn_unknown_paper_into_trash_or_blank(self):
        group = self.root / 'person'
        group.mkdir()
        (group / '论文.docx').write_bytes(b'thesis')
        class NoBodyJudge(FixtureJudge):
            def choose_body(self, records): return {'choice': None}
            def choose_other(self, record, body): return {'choice': 'trash'}
        handoff = grouping.classify_group(group, self.root, NoBodyJudge(), readable_fixture)
        self.assertIsNone(handoff['body'])
        self.assertEqual(handoff['trash'], [])
        self.assertEqual([r['relative'] for r in handoff['unresolved']], ['论文.docx'])
        self.assertEqual(handoff['attachment_status'], '尚未确认')

    def test_a_proven_process_only_group_can_remain_trash_without_a_body(self):
        group = self.root / 'person'
        group.mkdir()
        (group / '开题报告.docx').write_bytes(b'doc')
        handoff = grouping.classify_group(group, self.root, self.judge,
                                          lambda path: {'text': '开题报告\n内容', 'truncated': False, 'limitations': []})
        self.assertIsNone(handoff['body'])
        self.assertEqual([r['relative'] for r in handoff['trash']], ['开题报告.docx'])
        self.assertEqual(handoff['attachment_status'], '空白')

    def test_truncated_process_document_with_paper_text_is_not_excluded(self):
        group = self.root / 'person'
        group.mkdir()
        (group / '开题报告.docx').write_bytes(b'mixed')
        handoff = grouping.classify_group(group, self.root, self.judge,
            lambda path: {'text': '开题报告\n摘要\n本文研究内容', 'truncated': True})
        self.assertEqual(handoff['trash'], [])
        self.assertEqual([r['relative'] for r in handoff['unresolved']], ['开题报告.docx'])

    def test_archive_more_than_model_preview_is_kept_as_an_attachment(self):
        group = self.root / 'person'
        group.mkdir()
        (group / '论文.docx').write_bytes(b'thesis')
        (group / '资料.zip').write_bytes(b'archive')
        def reader(path):
            return ({'archive_type': 'zip', 'members': [{'name': f'{n}.pdf'} for n in range(26)],
                     'truncated': False, 'nested': False} if path.suffix == '.zip' else readable_fixture(path))
        class TrashJudge(FixtureJudge):
            def choose_other(self, record, body): return {'choice': 'trash'}
        handoff = grouping.classify_group(group, self.root, TrashJudge(), reader)
        self.assertEqual([r['relative'] for r in handoff['attachments']], ['资料.zip'])
        self.assertEqual(handoff['trash'], [])

    def test_unconfirmed_cover_title_does_not_fill_the_confirmed_title_field(self):
        group = self.make_group('张三', '附件.zip')
        def candidate_title(row, evidence):
            return '企业绩效评价研究', '正文 Word 开头文本', '企业绩效评价研究', False
        def evidence(path): return {'text': '企业绩效评价研究\n摘要', 'limitations': []}
        handoff = grouping.classify_group(group, self.root, self.judge, evidence,
                                          title_reader=candidate_title)
        self.assertIsNone(handoff['paper_title'])
        self.assertEqual(handoff['paper_title_candidate'], '企业绩效评价研究')
        self.assertEqual(handoff['title_candidate_source']['file_id'], handoff['body']['file_id'])

    def test_title_reader_cannot_invent_a_title_missing_from_body_evidence(self):
        group = self.make_group('张三', '附件.zip')
        def invented(row, evidence):
            return '根本不存在的论文题目', '正文开头', '根本不存在的论文题目', True
        handoff = grouping.classify_group(group, self.root, self.judge, readable_fixture,
                                          title_reader=invented)
        self.assertIsNone(handoff['paper_title'])
        self.assertIsNone(handoff['paper_title_candidate'])

    def test_missing_file_hash_cannot_be_published_as_a_complete_handoff(self):
        group = self.make_group('张三', '附件.zip')
        real_sha = grouping.sha_file
        def sha_with_denied_file(path):
            if Path(path).name == '附件.zip': raise PermissionError('unreadable')
            return real_sha(path)
        with patch.object(grouping, 'sha_file', side_effect=sha_with_denied_file):
            results = grouping.classify_batch([group], self.root, Path(self.temp.name) / 'handoffs',
                                              FixtureJudge, readable_fixture, verified_new_groups=[group])
        self.assertNotIn('handoff', results[0])
        self.assertIn('unreceived', results[0])
        pending = grouping.read_json(results[0]['unreceived'])
        self.assertEqual(pending['body']['relative'], '论文.docx')
        self.assertIsNone(pending['unresolved'][0]['sha256'])
        (group / '附件.zip').write_bytes(b'z' * len(b'same attachment bytes'))
        with self.assertRaises(ValueError): grouping.verify_handoff(pending)

    def test_known_paper_id_is_attached_only_to_its_own_source_group(self):
        a = self.make_group('张三', '张三附件.zip')
        b = self.make_group('李四', '李四附件.zip')
        results = grouping.classify_batch([a, b], self.root, Path(self.temp.name) / 'handoffs',
                                          FixtureJudge, readable_fixture,
                                          known_paper_ids={str(a): 'PF_existing_a'}, verified_new_groups=[a, b])
        records = [grouping.read_json(item['handoff']) for item in results]
        self.assertEqual(records[0].get('existing_paper_id'), 'PF_existing_a')
        self.assertIsNone(records[0]['paper_id'])
        self.assertIsNone(records[1]['existing_paper_id'])

    def test_same_existing_number_in_two_groups_requires_identity_review(self):
        a = self.make_group('张三', '张三附件.zip')
        b = self.make_group('李四', '李四附件.zip')
        c = self.make_group('王五', '王五附件.zip')
        results = grouping.classify_batch([a, b, c], self.root, Path(self.temp.name) / 'handoffs',
            FixtureJudge, readable_fixture, known_paper_ids={str(a): 'PF_shared', str(b): 'PF_shared'},
            verified_new_groups=[a, b, c])
        self.assertIn('error', results[0])
        self.assertIn('error', results[1])
        self.assertIn('handoff', results[2])

    def test_foreign_body_path_is_rejected_before_any_title_reader_access(self):
        group = self.make_group('张三', '附件.zip')
        other = self.make_group('李四', '附件.zip')
        def evidence(path): return {'text': '题目：企业绩效评价研究\n摘要', 'format': 'docx'}
        def title(row, preview): return '企业绩效评价研究', '正文 Word 开头文本', '企业绩效评价研究', True
        handoff = grouping.classify_group(group, self.root, self.judge, evidence, title_reader=title)
        handoff['body']['path'] = str((other / '论文.docx').resolve())
        accessed = []
        def monitor(path):
            accessed.append(path)
            return evidence(path)
        with self.assertRaises(ValueError): grouping.verify_handoff(handoff, evidence_reader=monitor)
        self.assertEqual(accessed, [])

    def test_title_reference_must_match_the_actual_pdf_page(self):
        group = self.root / 'person'
        group.mkdir()
        (group / '论文.pdf').write_bytes(b'pdf')
        preview = {'format': 'pdf', 'pages': [{'page': 1, 'text': '毕业论文'},
                   {'page': 2, 'text': '题目：企业绩效评价研究\n摘要'}]}
        class PdfJudge(FixtureJudge):
            def choose_body(self, records): return {'choice': records[0]['file_id']}
        def evidence(path): return preview
        def title(row, value): return '企业绩效评价研究', '正文 PDF 第2页文字', '企业绩效评价研究', True
        handoff = grouping.classify_group(group, self.root, PdfJudge(), evidence, title_reader=title)
        self.assertTrue(grouping.verify_handoff(handoff, evidence_reader=evidence))
        handoff['title_source']['reference'] = '正文 PDF 第1页文字'
        handoff['result_version'] = grouping._version(handoff)
        with self.assertRaises(ValueError): grouping.verify_handoff(handoff, evidence_reader=evidence)

    def test_title_reference_cannot_be_missing_or_empty(self):
        group = self.make_group('张三', '附件.zip')
        def evidence(path): return {'format': 'docx', 'text': '题目：企业绩效评价研究\n摘要'}
        def title(row, preview): return '企业绩效评价研究', '正文 Word 开头文本', '企业绩效评价研究', True
        original = grouping.classify_group(group, self.root, self.judge, evidence, title_reader=title)
        self.assertTrue(grouping.verify_handoff(original, evidence_reader=evidence))
        for reference in (None, ''):
            handoff = copy.deepcopy(original)
            handoff['title_source']['reference'] = reference
            handoff['result_version'] = grouping._version(handoff)
            with self.subTest(reference=reference):
                with self.assertRaises(ValueError):
                    grouping.verify_handoff(handoff, evidence_reader=evidence)

    def test_process_document_with_unread_drawing_is_not_automatically_discarded(self):
        from paperflow_prefect.group_evidence import read_evidence
        group = self.root / 'person'
        group.mkdir()
        path = group / '开题报告.docx'
        xml = ('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
               '<w:body><w:p><w:r><w:t>开题报告</w:t></w:r>'
               '<w:r><w:drawing/></w:r></w:p></w:body></w:document>')
        with zipfile.ZipFile(path, 'w') as archive:
            archive.writestr('word/document.xml', xml)
        preview = read_evidence(path)
        self.assertTrue(preview.get('has_unread_visual_content'))
        handoff = grouping.classify_group(group, self.root, self.judge, read_evidence)
        self.assertEqual(handoff['trash'], [])
        self.assertEqual([row['relative'] for row in handoff['unresolved']], ['开题报告.docx'])
        self.assertEqual(handoff['attachment_status'], '尚未确认')

    def test_mostly_scanned_body_is_left_unresolved(self):
        group = self.root / 'person'
        group.mkdir()
        (group / '论文.pdf').write_bytes(b'pdf')
        class PdfJudge(FixtureJudge):
            def choose_body(self, records): return {'choice': records[0]['file_id']}
        handoff = grouping.classify_group(group, self.root, PdfJudge(),
            lambda path: {'format': 'pdf', 'scan_heavy': True, 'text_unavailable': False})
        self.assertIsNone(handoff['body'])
        self.assertEqual([row['relative'] for row in handoff['unresolved']], ['论文.pdf'])
        self.assertFalse(handoff['ready_for_receipt'])

    def test_model_trash_keeps_bounded_decision_evidence(self):
        group = self.make_group('张三', '附件.zip')
        class AuditableJudge(FixtureJudge):
            def choose_other(self, row, body):
                return {'choice': 'trash', 'model': 'local-test', 'probabilities': {'trash': 0.9}}
        def evidence(path):
            return {'text': '报告开头' + '文' * 5000 + '报告结尾', 'limitations': ['仅供分类']}
        handoff = grouping.classify_group(group, self.root, AuditableJudge(), evidence)
        report = next(row for row in handoff['trash'] if row['relative'] == '报告.pdf')
        basis = report['decision_basis']
        self.assertEqual(basis['method'], 'local_model')
        self.assertEqual(basis['decision']['choice'], 'trash')
        self.assertEqual(basis['decision']['model'], 'local-test')
        self.assertIn('报告开头', basis['evidence']['text'])
        self.assertIn('报告结尾', basis['evidence']['text'])
        self.assertLess(len(basis['evidence']['text']), 4000)

    def test_only_a_fragment_of_a_title_cannot_become_the_confirmed_title(self):
        group = self.make_group('张三', '附件.zip')
        def evidence(path): return {'format': 'docx', 'text': '题目：某系统优化设计研究\n摘要'}
        def partial(row, value): return '系统', '正文 Word 开头文本', '某系统优化设计研究', True
        handoff = grouping.classify_group(group, self.root, self.judge, evidence, title_reader=partial)
        self.assertIsNone(handoff['paper_title'])

    def test_title_matching_first_line_only_is_not_confirmed_when_field_continues(self):
        group = self.make_group('张三', '附件.zip')
        def evidence(path):
            return {'format': 'docx', 'text': '题目：武汉市黄陂区木兰暖村\n\n游客中心建筑方案设计\n学 院：城市建设学院\n摘要'}
        def partial(row, value): return '武汉市黄陂区木兰暖村', '正文 Word 开头文本', '武汉市黄陂区木兰暖村', True
        handoff = grouping.classify_group(group, self.root, self.judge, evidence, title_reader=partial)
        self.assertIsNone(handoff['paper_title'])

    def test_labelled_title_allows_a_following_cover_label_split_over_lines(self):
        group = self.root / 'person'
        group.mkdir()
        (group / '论文.pdf').write_bytes(b'pdf')
        preview = {'format': 'pdf', 'pages': [{'page': 1,
            'text': '题目：\n武汉市天兴洲景观影响评价研究\n学\n院:\n资源与环境学院'}]}
        class PdfJudge(FixtureJudge):
            def choose_body(self, records): return {'choice': records[0]['file_id']}
        def title(row, value): return '武汉市天兴洲景观影响评价研究', '正文 PDF 第1页文字', '武汉市天兴洲景观影响评价研究', True
        handoff = grouping.classify_group(group, self.root, PdfJudge(), lambda path: preview, title_reader=title)
        self.assertEqual(handoff['paper_title'], '武汉市天兴洲景观影响评价研究')
        self.assertTrue(grouping.verify_handoff(handoff, evidence_reader=lambda path: preview))

    def test_title_cannot_include_a_following_cover_field(self):
        group = self.make_group('张三', '附件.zip')
        for label in ('论文编号', '作者姓名', '完成日期'):
            text = f'题目：基于神经网络的图像分类研究\n{label}：20200123\n学院：计算机学院'
            def evidence(path): return {'format': 'docx', 'text': text}
            def polluted(row, value):
                title = f'基于神经网络的图像分类研究{label}：20200123'
                return title, '正文 Word 开头文本', f'基于神经网络的图像分类研究\n{label}：20200123', True
            with self.subTest(label=label):
                handoff = grouping.classify_group(group, self.root, self.judge, evidence, title_reader=polluted)
                self.assertIsNone(handoff['paper_title'])

    def test_attachment_only_group_without_confirmed_paper_is_not_a_complete_handoff(self):
        group = self.root / 'person'
        group.mkdir()
        (group / '资料.zip').write_bytes(b'archive')
        output = Path(self.temp.name) / 'handoffs'
        results = grouping.classify_batch([group], self.root, output, FixtureJudge, readable_fixture,
                                          verified_new_groups=[group])
        self.assertNotIn('handoff', results[0])
        self.assertIn('unreceived', results[0])
        saved = grouping.read_json(results[0]['unreceived'])
        self.assertEqual([row['relative'] for row in saved['attachments']], ['资料.zip'])
        self.assertFalse(saved['ready_for_receipt'])

    def test_graphic_heavy_landscape_pdf_is_preserved_after_body_is_selected(self):
        group = self.root / 'person'
        group.mkdir()
        (group / '论文.docx').write_bytes(b'thesis')
        (group / '毕业设计手册.pdf').write_bytes(b'visual pdf')
        def reader(path):
            return ({'format': 'pdf', 'visual_heavy': True, 'pages': [{'text': '设计展示'}]}
                    if path.suffix == '.pdf' else readable_fixture(path))
        class TrashJudge(FixtureJudge):
            def choose_other(self, record, body): return {'choice': 'trash'}
        handoff = grouping.classify_group(group, self.root, TrashJudge(), reader)
        self.assertEqual([r['relative'] for r in handoff['attachments']], ['毕业设计手册.pdf'])
        self.assertEqual(handoff['trash'], [])


if __name__ == '__main__':
    unittest.main()
