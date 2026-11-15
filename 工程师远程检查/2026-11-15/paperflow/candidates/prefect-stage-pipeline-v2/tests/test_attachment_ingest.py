import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


def digest(data):
    return hashlib.sha256(data).hexdigest()


def handoff(root, *, archive=False, status=None, total=None, rows=()):
    source = Path(root) / ('sample.zip' if archive else 'code.txt')
    source.write_bytes(b'print(1)')
    meta = {'schema': 'attachment-card/1', 'paper_id': 'PF_sample',
            'attachment_id': 'ATT_sample', 'source_name': source.name,
            'source_format': source.suffix, 'source_bytes': 8,
            'source_sha256': digest(source.read_bytes())}
    status = status or ('complete' if archive else 'not_archive')
    lines = ['---'] + [k + ': ' + json.dumps(v, ensure_ascii=False) for k, v in meta.items()]
    lines += ['---', '', '# 附件交付说明', '', '## 1. 附件类型', '', '源码文件', '',
              '## 2. 简单说明', '', '包含一段程序。', '', '## 3. 读取依据与限制', '',
              '- 实际读取范围：文本预览', '- 判断依据：已有说明引用 text-preview',
              '- 未读取或未确认内容：未执行程序', '', '## 4. 包内目录', '',
              '- 目录状态：' + status, '- 总条目数：' + ('null' if total is None else str(total)),
              '- 已列条目数：' + str(len(rows)), '- 状态说明：测试证据', '',
              '| 相对路径 | 条目类型 | 大小（字节） |', '| --- | --- | --- |']
    lines += ['| ' + ' | '.join(row) + ' |' for row in rows]
    md = Path(root) / 'attachment.md'; md.write_text('\n'.join(lines) + '\n', encoding='utf8')
    return {'paper_id': meta['paper_id'], 'attachment_id': meta['attachment_id'],
            'source_path': str(source), 'source_sha256': meta['source_sha256'],
            'md_path': str(md), 'md_sha256': digest(md.read_bytes()), 'delivery_version': 'd'*64}


def edit_card(item, old, new):
    path = Path(item['md_path']); path.write_text(path.read_text(encoding='utf8').replace(old, new), encoding='utf8')
    item['md_sha256'] = digest(path.read_bytes())


class AttachmentIngestTests(unittest.TestCase):
    def reader(self):
        self.assertIsNotNone(importlib.util.find_spec('paperflow_prefect.attachment_ingest'),
                             'stage four MD receiver is missing')
        from paperflow_prefect.attachment_ingest import read_attachment_delivery
        return read_attachment_delivery

    def test_integrated_md_is_sufficient_without_description_json(self):
        with tempfile.TemporaryDirectory() as root:
            item = handoff(root); card = self.reader()(item)
            self.assertEqual(card['kind'], '源码文件')
            self.assertEqual(card['summary'], '包含一段程序。')
            self.assertEqual(card['listing']['entries'], [])
            self.assertEqual(card['metadata']['source_bytes'], 8)

    def test_changed_original_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            item = handoff(root); Path(item['source_path']).write_bytes(b'changed!')
            with self.assertRaises(ValueError): self.reader()(item)

    def test_wrong_paper_or_attachment_identity_is_rejected(self):
        for key in ['paper_id', 'attachment_id']:
            with self.subTest(key=key), tempfile.TemporaryDirectory() as root:
                item = handoff(root); item[key] = 'other'
                with self.assertRaises(ValueError): self.reader()(item)

    def test_duplicate_frontmatter_key_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            item = handoff(root); edit_card(item, 'paper_id: "PF_sample"', 'paper_id: "PF_sample"\npaper_id: "PF_sample"')
            with self.assertRaises(ValueError): self.reader()(item)

    def test_directory_paths_decode_once_and_retain_literal_percent(self):
        with tempfile.TemporaryDirectory() as root:
            item = handoff(root, archive=True, total=3, rows=[
                ('src/a%250Ab.py', 'file', '8'), ('src/a%0Ab.py', 'file', 'null'),
                ('src/a&amp;#124;b&#124;c.py', 'file', '0')])
            entries = self.reader()(item)['listing']['entries']
            self.assertEqual([row['path'] for row in entries], ['src/a%0Ab.py', 'src/a\nb.py', 'src/a&#124;b|c.py'])

    def test_directory_traversal_and_absolute_paths_rejected_after_decoding(self):
        for path in ['../bad.py', 'C:bad.py', '/bad.py', '&#92;&#92;host/file', 'src/../bad.py']:
            with self.subTest(path=path), tempfile.TemporaryDirectory() as root:
                item = handoff(root, archive=True, total=1, rows=[(path, 'file', '8')])
                with self.assertRaises(ValueError): self.reader()(item)

    def test_directory_counts_and_unknown_state_are_not_empty_success(self):
        for status, total, rows in [('complete', None, []), ('complete', 2, [('a','file','8')]),
                                    ('partial', 0, []), ('unavailable', None, [('a','file','8')]),
                                    ('not_read', None, [('a','file','8')])]:
            with self.subTest(status=status, total=total), tempfile.TemporaryDirectory() as root:
                item = handoff(root, archive=True, status=status, total=total, rows=rows)
                with self.assertRaises(ValueError): self.reader()(item)

    def test_known_empty_archive_and_unknown_history_are_distinct(self):
        for status, total in [('complete', 0), ('unavailable', None), ('not_read', None)]:
            with self.subTest(status=status), tempfile.TemporaryDirectory() as root:
                card = self.reader()(handoff(root, archive=True, status=status, total=total))
                self.assertEqual(card['listing']['status'], status)
                self.assertEqual(card['listing']['total_entries'], total)

    def test_directory_type_and_byte_count_are_strict(self):
        for row in [('a','unknown','8'), ('a','file','-1'), ('a','directory','8')]:
            with self.subTest(row=row), tempfile.TemporaryDirectory() as root:
                item = handoff(root, archive=True, total=1, rows=[row])
                with self.assertRaises(ValueError): self.reader()(item)

    def test_md_updates_keep_raw_identity(self):
        with tempfile.TemporaryDirectory() as root:
            item = handoff(root); first = self.reader()(item)
            edit_card(item, '包含一段程序。', '包含打印语句；未执行。')
            second = self.reader()(item)
            self.assertEqual(first['raw_identity'], second['raw_identity'])
            self.assertNotEqual(first['delivery']['md_sha256'], second['delivery']['md_sha256'])


if __name__ == '__main__': unittest.main()
