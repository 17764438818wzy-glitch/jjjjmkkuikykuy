import hashlib
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

try:
    from paperflow_prefect import group_evidence
except ImportError:
    group_evidence = None


class GroupEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(group_evidence, '分类用的本地证据读取器尚不存在')
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_word_body_evidence_contains_beginning_and_end(self):
        source = self.root / 'paper.docx'
        xml = ('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
               '<w:body><w:p><w:r><w:t>摘要开头</w:t></w:r></w:p>'
               + '<w:p><w:r><w:t>' + ('中' * 6000) + '</w:t></w:r></w:p>'
               + '<w:p><w:r><w:t>参考文献末尾</w:t></w:r></w:p></w:body></w:document>')
        with zipfile.ZipFile(source, 'w') as archive:
            archive.writestr('word/document.xml', xml)
        result = group_evidence.read_evidence(source)
        self.assertIn('摘要开头', result['text'])
        self.assertIn('参考文献末尾', result['text'])
        self.assertLess(len(result['text']), 5000)

    def test_workbook_is_not_reported_as_a_zip_member_list(self):
        source = self.root / 'book.xlsx'
        with zipfile.ZipFile(source, 'w') as archive:
            archive.writestr('xl/workbook.xml', 'private spreadsheet internals')
        result = group_evidence.read_evidence(source)
        self.assertEqual(result['format'], 'workbook')
        self.assertNotIn('members', result)

    def test_word_without_text_is_recorded_as_unreadable_for_classification(self):
        source = self.root / 'image-only.docx'
        xml = ('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
               '<w:body><w:p/></w:body></w:document>')
        with zipfile.ZipFile(source, 'w') as archive:
            archive.writestr('word/document.xml', xml)
        result = group_evidence.read_evidence(source)
        self.assertTrue(result.get('text_unavailable'))

    def test_word_xml_slightly_above_eight_megabytes_stays_bounded_and_readable(self):
        source = self.root / 'large-word.docx'
        xml = ('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
               '<w:body><w:p><w:r><w:t>摘要开头</w:t></w:r></w:p>'
               + '<w:p><w:r><w:t>' + ('x' * (8 * 1024 * 1024 + 100)) + '</w:t></w:r></w:p>'
               + '<w:p><w:r><w:t>参考文献末尾</w:t></w:r></w:p></w:body></w:document>')
        with zipfile.ZipFile(source, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr('word/document.xml', xml)
        result = group_evidence.read_evidence(source)
        self.assertIn('摘要开头', result['text'])
        self.assertIn('参考文献末尾', result['text'])
        self.assertLess(len(result['text']), 5000)

    def test_word_preserves_split_runs_within_one_title_paragraph(self):
        source = self.root / 'split-title.docx'
        xml = ('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
               '<w:body><w:p><w:r><w:t>题</w:t></w:r><w:r><w:t>目：</w:t></w:r>'
               '<w:r><w:t>企业绩效评价研究</w:t></w:r></w:p></w:body></w:document>')
        with zipfile.ZipFile(source, 'w') as archive:
            archive.writestr('word/document.xml', xml)
        result = group_evidence.read_evidence(source)
        self.assertIn('题目：企业绩效评价研究', result['text'])

    def test_mostly_scanned_pdf_is_not_treated_as_text_only(self):
        import fitz
        source = self.root / 'scan.pdf'
        pixmap = fitz.Pixmap(fitz.csRGB, fitz.IRect(0, 0, 3, 3), False)
        pixmap.clear_with(255)
        picture = pixmap.tobytes('png')
        document = fitz.open()
        for index in range(6):
            page = document.new_page(width=595, height=842)
            if index == 0:
                page.insert_text((72, 72), 'Cover')
            else:
                page.insert_image(page.rect, stream=picture)
        document.save(source)
        document.close()
        result = group_evidence.read_evidence(source)
        self.assertFalse(result['text_unavailable'])
        self.assertFalse(result['visual_heavy'])
        self.assertTrue(result['scan_heavy'])

    def test_archive_directory_is_bounded_and_nested_archive_is_not_opened(self):
        source = self.root / 'many.zip'
        with zipfile.ZipFile(source, 'w') as archive:
            archive.writestr('nested.zip', b'must not execute or expand')
            for number in range(150): archive.writestr(f'{number:04}.txt', 'x')
        result = group_evidence.read_evidence(source)
        self.assertEqual(result['archive_type'], 'zip')
        self.assertLessEqual(len(result['members']), 80)
        self.assertTrue(result['truncated'])
        self.assertTrue(result['nested'])
        self.assertFalse((self.root / 'nested.zip').exists())

    def test_archive_retains_real_directory_entries_without_changing_classification_members(self):
        source = self.root / 'source.zip'
        with zipfile.ZipFile(source, 'w') as archive:
            archive.writestr('src/', b'')
            archive.writestr('src/main.py', b'print(1)')
        result = group_evidence.read_evidence(source)
        self.assertEqual([member['name'].replace('\\', '/') for member in result['members']], ['src/main.py'])
        listing = result['_handoff']['archive_listing']
        self.assertEqual(listing['status'], 'complete')
        self.assertEqual(listing['total_entries'], 2)
        self.assertEqual([(row['path'].replace('\\', '/').rstrip('/'), row['type']) for row in listing['entries']],
                         [('src', 'directory'), ('src/main.py', 'file')])

    def test_word_previews_preserve_start_and_end_without_invented_ellipsis(self):
        source = self.root / 'paper.docx'
        xml = ('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
               '<w:body><w:p><w:r><w:t>开始' + ('中' * 5000) + '结束</w:t></w:r></w:p></w:body></w:document>')
        with zipfile.ZipFile(source, 'w') as archive:
            archive.writestr('word/document.xml', xml)
        result = group_evidence.read_evidence(source)
        samples = result['_handoff']['text_samples']
        self.assertEqual(len(samples), 2)
        self.assertTrue(samples[0]['text'].startswith('开始'))
        self.assertTrue(samples[1]['text'].endswith('结束'))
        self.assertTrue(all(sample['truncated'] is True for sample in samples))
        self.assertTrue(all('\n...\n' not in sample['text'] for sample in samples))

    def test_pdf_page_sample_records_real_truncation_not_picture_count_as_preview(self):
        import fitz
        source = self.root / 'paper.pdf'
        document = fitz.open()
        page = document.new_page(width=595, height=842)
        for index in range(20):
            page.insert_text((20, 30 + 20 * index), 'abcdefghijklmnopqrstuvwxyz' * 2)
        document.save(source)
        document.close()
        result = group_evidence.read_evidence(source)
        samples = result['_handoff']['text_samples']
        self.assertEqual(samples[0]['location']['page'], 1)
        self.assertTrue(samples[0]['truncated'])
        self.assertEqual(result['_handoff'].get('images'), [])

    def test_7z_with_zip_extension_is_read_by_actual_signature(self):
        seven = Path(r'C:\Program Files\NVIDIA Corporation\NVIDIA App\7z.exe')
        if not seven.is_file(): self.skipTest('7z reader unavailable')
        (self.root / 'source.txt').write_text('original', encoding='utf8')
        genuine = self.root / 'archive.7z'
        subprocess.run([str(seven), 'a', '-t7z', str(genuine), 'source.txt'],
                       cwd=self.root, capture_output=True, check=True, timeout=20)
        disguised = self.root / 'archive.zip'
        genuine.rename(disguised)
        before = hashlib.sha256(disguised.read_bytes()).hexdigest()
        result = group_evidence.read_evidence(disguised)
        self.assertEqual(result['archive_type'], '7z')
        self.assertEqual([row['name'] for row in result['members']], ['source.txt'])
        self.assertEqual(hashlib.sha256(disguised.read_bytes()).hexdigest(), before)

    def test_single_7z_document_is_hashed_in_memory_without_creating_a_file(self):
        seven = Path(r'C:\Program Files\NVIDIA Corporation\NVIDIA App\7z.exe')
        if not seven.is_file(): self.skipTest('7z reader unavailable')
        inputs = self.root / 'inputs'
        inputs.mkdir()
        body = inputs / '论文.docx'
        body.write_bytes(b'paper-content-for-hash')
        source = self.root / 'archive.7z'
        subprocess.run([str(seven), 'a', '-t7z', str(source), body.name],
                       cwd=inputs, capture_output=True, check=True, timeout=20)
        before = hashlib.sha256(source.read_bytes()).hexdigest()
        result = group_evidence.read_evidence(source)
        self.assertEqual(result['single_document_sha256'], hashlib.sha256(body.read_bytes()).hexdigest())
        self.assertFalse((self.root / body.name).exists())
        self.assertEqual(hashlib.sha256(source.read_bytes()).hexdigest(), before)
        self.assertIsNone(group_evidence._bounded_archive_sha(source, max_bytes=5))

    def test_document_archive_missing_size_cannot_be_treated_as_verified(self):
        source = self.root / 'unknown.rar'
        source.write_bytes(b'archive')
        listing = b'Path = paper.docx\nSize = \n\n'
        with patch.object(group_evidence, '_archive_type', return_value='rar'), \
             patch.object(group_evidence, '_limited_listing', return_value=(listing, False, None)):
            result = group_evidence.read_evidence(source)
        self.assertNotIn('single_document_sha256', result)
        self.assertTrue(result.get('read_error'))

    def test_archive_timeout_preserves_entries_already_read_as_partial(self):
        source = self.root / 'timeout.rar'
        source.write_bytes(b'archive')
        raw = b'Path = src/main.py\nFolder = -\nSize = 12\n\n'
        with patch.object(group_evidence, '_archive_type', return_value='rar'), \
             patch.object(group_evidence, '_limited_listing', return_value=(raw, True, '目录读取超时')):
            result = group_evidence.read_evidence(source)
        listing = result['_handoff']['archive_listing']
        self.assertEqual(listing['status'], 'partial')
        self.assertEqual(listing['entries'][0]['path'], 'src/main.py')
        self.assertIsNone(listing['total_entries'])
        self.assertIn('目录读取超时', result['_handoff']['errors'])

    def test_archive_timeout_does_not_invent_a_truncated_member_name(self):
        source = self.root / 'timeout.rar'
        source.write_bytes(b'archive')
        raw = b'Path = complete.txt\nFolder = -\nSize = 12\n\nPath = cut/off'
        with patch.object(group_evidence, '_archive_type', return_value='rar'), \
             patch.object(group_evidence, '_limited_listing', return_value=(raw, True, '目录读取超时')):
            result = group_evidence.read_evidence(source)
        self.assertEqual([item['path'] for item in result['_handoff']['archive_listing']['entries']],
                         ['complete.txt'])

    def test_encrypted_single_document_archive_is_recorded_as_unverified(self):
        source = self.root / 'encrypted.zip'
        with zipfile.ZipFile(source, 'w') as archive:
            archive.writestr('paper.docx', b'document')
        original = zipfile.ZipFile.infolist
        def encrypted(archive):
            rows = original(archive)
            for row in rows: row.flag_bits |= 1
            return rows
        with patch.object(zipfile.ZipFile, 'infolist', new=encrypted):
            result = group_evidence.read_evidence(source)
        self.assertNotIn('single_document_sha256', result)
        self.assertTrue(result.get('read_error'))


if __name__ == '__main__':
    unittest.main()
