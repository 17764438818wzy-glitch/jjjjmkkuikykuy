import hashlib
import tempfile
import unittest
import os
import zipfile
import json
from contextlib import nullcontext
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from paperflow_prefect.backend import Backend
from paperflow_prefect.intake_handoff import _display
from paperflow_prefect.named_delivery import NamedDeliveryError, materialize_named_input


class _Settings:
    def require_execution(self):
        pass

    def path(self, key):
        return Path('catalog.json')

    def __getitem__(self, key):
        return 160


class NamedDownstreamTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.source = self.base / 'original.txt'
        self.source.write_bytes(b'original bytes')
        self.hash = hashlib.sha256(self.source.read_bytes()).hexdigest()
        self.work = self.base / 'work'
        self.paper = {'paper_id': 'PF_test', 'revision': 'revision1', 'body': {
            'path': str(self.source), 'name': self.source.name, 'relative': self.source.name,
            'file_id': 'BODY_test', 'kind': 'body', 'sha256': self.hash,
            'bytes': self.source.stat().st_size, 'display_name': '论文题目.txt'}}
        self.backend = Backend.__new__(Backend)
        self.backend.settings = _Settings()
        self.backend.root = self.base / 'data'
        self.backend.body_work = lambda paper: self.work

    def tearDown(self):
        self.temp.cleanup()

    def test_fresh_body_is_prepared_from_named_copy_without_changing_original(self):
        called = []
        def prepare(source, work, *args, **kwargs):
            called.append(Path(source))
            (Path(work) / 'material.json').write_text('{}', encoding='utf8')
        with patch('paperflow_prefect.backend.bind_work'), patch('paperflow_prefect.backend.body.prepare_body', side_effect=prepare):
            self.backend.prepare(self.paper)
        self.assertEqual(called[0].name, '论文题目.txt')
        self.assertNotEqual(called[0], self.source)
        self.assertEqual(called[0].read_bytes(), self.source.read_bytes())
        self.assertEqual(self.paper['body']['path'], str(self.source))
        self.assertEqual(self.source.read_bytes(), b'original bytes')

    def test_windows_reserved_name_with_embedded_dot_gets_safe_prefix(self):
        self.assertEqual(_display('CON.foo', 'body', None, '.docx'), '_CON.foo.docx')

    def test_cover_table_uses_title_column_not_adjacent_other_title(self):
        from paperflow_prefect.group_evidence import read_evidence
        from paperflow_prefect.group_sort_cli import _title_from_body
        def cell(value): return '<w:tc><w:p><w:r><w:t>'+value+'</w:t></w:r></w:p></w:tc>'
        xml = ('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
               '<w:body><w:tbl><w:tr>'+cell('题目：')+cell('基于Python的图书馆管理')+
               cell('基于BERT的不文明文本分类系统')+'</w:tr><w:tr>'+cell('')+cell('系统设计')+cell('')+
               '</w:tr></w:tbl><w:p><w:r><w:t>学院：计算机学院</w:t></w:r></w:p></w:body></w:document>')
        source = self.base / 'cover.docx'
        with zipfile.ZipFile(source, 'w') as archive: archive.writestr('word/document.xml', xml)
        evidence = read_evidence(source)
        title, reference, excerpt, confirmed = _title_from_body({'file_id': 'body'}, evidence)
        self.assertEqual(title, '基于Python的图书馆管理系统设计')
        self.assertTrue(confirmed)
        from paperflow_prefect.group_classification import _title_proof
        self.assertTrue(_title_proof(evidence, title, excerpt, labelled=True, reference=reference))

    def test_cover_table_removes_decorative_glyph_and_leading_colon(self):
        from paperflow_prefect.group_evidence import read_evidence
        from paperflow_prefect.group_sort_cli import _title_from_body
        def cell(value): return '<w:tc><w:p><w:r><w:t>'+value+'</w:t></w:r></w:p></w:tc>'
        xml = ('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
               '<w:body><w:tbl><w:tr>'+cell('题目')+cell('： \uf0b6乡村信贷研究')+'</w:tr></w:tbl>'
               '<w:p><w:r><w:t>学院：经济学院</w:t></w:r></w:p></w:body></w:document>')
        source = self.base / 'cover.docx'
        with zipfile.ZipFile(source, 'w') as archive: archive.writestr('word/document.xml', xml)
        title, _, _, confirmed = _title_from_body({'file_id': 'body'}, read_evidence(source))
        self.assertEqual(title, '乡村信贷研究')
        self.assertTrue(confirmed)

    def test_cover_title_excludes_template_formatting_instructions(self):
        from paperflow_prefect.group_sort_cli import _title_from_body
        from paperflow_prefect.group_classification import _title_proof
        evidence = {'format':'docx', 'text':'题目：新营销理念对企业市场营销策略的研究\n\n'
            '（格式全校一样，封面可在教务处网上下载）\n\n'
            '（以下六项均为四号字体，其余为宋体）\n\n学院：管理学院\n摘要'}
        title, reference, excerpt, confirmed = _title_from_body({'file_id':'body'}, evidence)
        self.assertEqual(title, '新营销理念对企业市场营销策略的研究')
        self.assertTrue(confirmed)
        self.assertTrue(_title_proof(evidence,title,excerpt,labelled=True,reference=reference))

    def test_title_field_in_a_body_table_after_abstract_is_not_a_cover(self):
        from paperflow_prefect.group_evidence import read_evidence
        from paperflow_prefect.group_sort_cli import _title_from_body
        def cell(value): return '<w:tc><w:p><w:r><w:t>'+value+'</w:t></w:r></w:p></w:tc>'
        xml = ('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
               '<w:body><w:p><w:r><w:t>摘要</w:t></w:r></w:p><w:tbl><w:tr>'+
               cell('题目：')+cell('实验问卷的标题')+'</w:tr></w:tbl>'
               '<w:p><w:r><w:t>学院：经济学院</w:t></w:r></w:p></w:body></w:document>')
        source = self.base / 'body-table.docx'
        with zipfile.ZipFile(source, 'w') as archive: archive.writestr('word/document.xml', xml)
        title, _, _, confirmed = _title_from_body({'file_id':'body'}, read_evidence(source))
        self.assertFalse(confirmed)
        self.assertNotEqual(title, '实验问卷的标题')

    def test_cover_title_wraps_inside_one_cell_without_newline_in_filename(self):
        from paperflow_prefect.group_evidence import read_evidence
        from paperflow_prefect.group_sort_cli import _title_from_body
        xml=('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
             '<w:body><w:tbl><w:tr><w:tc><w:p><w:r><w:t>题目：</w:t></w:r></w:p></w:tc>'
             '<w:tc><w:p><w:r><w:t>研究问题</w:t></w:r></w:p><w:p><w:r><w:t>的解决方法</w:t></w:r></w:p>'
             '</w:tc></w:tr></w:tbl></w:body></w:document>')
        source=self.base/'wrapped.docx'
        with zipfile.ZipFile(source,'w') as z:z.writestr('word/document.xml',xml)
        title,_,_,confirmed=_title_from_body({},read_evidence(source))
        self.assertEqual(title,'研究问题的解决方法');self.assertTrue(confirmed)

    def test_cover_title_continuation_in_short_row_or_following_paragraph_is_kept(self):
        from paperflow_prefect.group_evidence import read_evidence
        from paperflow_prefect.group_sort_cli import _title_from_body
        def cell(v):return '<w:tc><w:p><w:r><w:t>'+v+'</w:t></w:r></w:p></w:tc>'
        for outside in (False,True):
            tail=('</w:tr><w:tr>'+cell('方法研究')+'</w:tr></w:tbl>' if not outside else
                  '</w:tr></w:tbl><w:p><w:r><w:t>方法研究</w:t></w:r></w:p>')
            xml=('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
                 '<w:body><w:tbl><w:tr>'+cell('题目：')+cell('供应链管理')+tail+
                 '<w:p><w:r><w:t>学院：管理学院</w:t></w:r></w:p></w:body></w:document>')
            source=self.base/('outside.docx' if outside else 'shortrow.docx')
            with zipfile.ZipFile(source,'w') as z:z.writestr('word/document.xml',xml)
            title,_,_,confirmed=_title_from_body({},read_evidence(source))
            with self.subTest(outside=outside):self.assertEqual(title,'供应链管理方法研究');self.assertTrue(confirmed)

    def test_missing_title_continues_with_original(self):
        self.paper['body']['display_name'] = None
        seen = []
        def prepare(source, work, *args, **kwargs):
            seen.append(Path(source))
            (Path(work) / 'material.json').write_text('{}', encoding='utf8')
        with patch('paperflow_prefect.backend.bind_work'), patch('paperflow_prefect.backend.body.prepare_body', side_effect=prepare):
            self.backend.prepare(self.paper)
        self.assertEqual(seen, [self.source])

    def test_retry_after_partial_preparation_reuses_named_input(self):
        paths = []
        def interrupted(source, work, *args, **kwargs):
            paths.append(Path(source))
            (Path(work) / 'private').mkdir(exist_ok=True)
            raise RuntimeError('interrupted')
        def finish(source, work, *args, **kwargs):
            paths.append(Path(source))
            (Path(work) / 'material.json').write_text('{}', encoding='utf8')
        with patch('paperflow_prefect.backend.bind_work'):
            with patch('paperflow_prefect.backend.body.prepare_body', side_effect=interrupted):
                with self.assertRaises(RuntimeError): self.backend.prepare(self.paper)
            with patch('paperflow_prefect.backend.body.prepare_body', side_effect=finish):
                self.backend.prepare(self.paper)
        self.assertEqual(paths[0], paths[1])
        self.assertEqual(paths[1].name, '论文题目.txt')

    def test_existing_model_request_does_not_rebind_input(self):
        (self.work / 'model').mkdir(parents=True)
        (self.work / 'model' / 'request.json').write_text('{}', encoding='utf8')
        seen = []
        def prepare(source, work, *args, **kwargs):
            seen.append(Path(source))
            (Path(work) / 'material.json').write_text('{}', encoding='utf8')
        with patch('paperflow_prefect.backend.bind_work'), patch('paperflow_prefect.backend.body.prepare_body', side_effect=prepare):
            self.backend.prepare(self.paper)
        self.assertEqual(seen, [self.source])
        self.assertFalse((self.work / 'named-input').exists())

    def test_fresh_attachment_reader_receives_title_based_copy(self):
        row = dict(self.paper['body'], file_id='ATT_test', kind='attachment',
                   display_name='论文题目_附件01.txt')
        self.backend.attachment_work = lambda paper, item: self.work
        self.backend.local_scope = nullcontext
        self.backend._invoke = lambda *args: None
        self.backend.attachment_card = lambda *args: None
        seen = []
        def identify(item, paper_id, work, *args, **kwargs):
            seen.append(Path(kwargs.get('input_source', item['path'])))
            (Path(work) / 'description.json').write_text('{}', encoding='utf8')
            return {'ok': True}
        with patch('paperflow_prefect.backend.require_model_history'), patch('paperflow_prefect.backend.bind_work'), patch('paperflow_prefect.backend.verify_cached_description'), patch('paperflow_prefect.backend.identify_attachment', side_effect=identify):
            self.backend.attachment(self.paper, row)
        self.assertEqual(seen[0].name, '论文题目_附件01.txt')
        self.assertEqual(seen[0].read_bytes(), self.source.read_bytes())
        self.assertEqual(row['path'], str(self.source))

    def test_attachment_handoff_delivers_named_copy_and_keeps_raw_identity(self):
        from paperflow_prefect.unified_attachments import delivery_for
        from paperflow_prefect.attachment_ingest import read_attachment_delivery
        from test_attachment_ingest import handoff
        item = handoff(self.base)
        original = Path(item['source_path'])
        row = {'path': str(original), 'name': original.name, 'relative': original.name,
               'file_id': item['attachment_id'], 'kind': 'attachment', 'bytes': original.stat().st_size,
               'sha256': item['source_sha256'], 'display_name': '论文题目_附件01.txt'}
        paper = {'paper_id': item['paper_id']}
        card = {'original': str(original), 'source_sha256': row['sha256'], 'path': item['md_path'],
                'md_sha256': item['md_sha256'], 'version': item['delivery_version']}
        self.backend.attachment_work = lambda p, r: self.work
        self.backend.attachment_card = lambda p, r: card
        delivered = delivery_for(self.backend, paper, row)
        self.assertEqual(Path(delivered['source_path']).name, '论文题目_附件01.txt')
        self.assertNotEqual(Path(delivered['source_path']), original)
        checked = read_attachment_delivery(delivered)
        self.assertEqual(checked['metadata']['source_name'], original.name)
        self.assertEqual(checked['metadata']['source_sha256'], row['sha256'])
        self.assertEqual(Path(checked['delivery']['source_path']).read_bytes(), original.read_bytes())

    def test_attachment_handoff_rejects_changed_named_copy(self):
        from paperflow_prefect.unified_attachments import delivery_for
        from paperflow_prefect.attachment_ingest import read_attachment_delivery
        from test_attachment_ingest import handoff
        item = handoff(self.base); original = Path(item['source_path'])
        row = {'path': str(original), 'name': original.name, 'relative': original.name,
               'file_id': item['attachment_id'], 'kind': 'attachment', 'bytes': original.stat().st_size,
               'sha256': item['source_sha256'], 'display_name': '论文题目_附件01.txt'}
        paper = {'paper_id': item['paper_id']}
        self.backend.attachment_work = lambda p, r: self.work
        self.backend.attachment_card = lambda p, r: {'original':str(original),'source_sha256':row['sha256'],
            'path':item['md_path'],'md_sha256':item['md_sha256'],'version':item['delivery_version']}
        delivered = delivery_for(self.backend, paper, row)
        named = Path(delivered['source_path'])
        self.assertNotEqual(named, original)
        named.write_bytes(b'tampered')
        with self.assertRaises(ValueError): read_attachment_delivery(delivered)
        self.assertEqual(original.read_bytes(), b'print(1)')

    def test_named_attachment_evidence_uses_the_copy_but_keeps_source_hash(self):
        from paperflow_prefect.attachments import evidence_for
        row = dict(self.paper['body'], file_id='ATT_test', kind='attachment', display_name='论文题目_附件01.txt')
        named = materialize_named_input(self.paper, row, self.work)
        evidence = evidence_for(row, self.work / 'read', input_source=named)
        self.assertEqual(evidence['name'], '论文题目_附件01.txt')
        self.assertEqual(evidence['source_sha256'], self.hash)
        self.assertEqual(evidence['text'], 'original bytes')

    def test_existing_attachment_handoff_can_preserve_legacy_path(self):
        from paperflow_prefect.unified_attachments import delivery_for
        from paperflow_prefect.attachment_ingest import read_attachment_delivery
        from test_attachment_ingest import handoff
        item = handoff(self.base); original = Path(item['source_path'])
        row = {'path': str(original), 'name': original.name, 'relative': original.name,
               'file_id': item['attachment_id'], 'kind': 'attachment', 'bytes': original.stat().st_size,
               'sha256': item['source_sha256'], 'display_name': '论文题目_附件01.txt'}
        paper = {'paper_id': item['paper_id']}
        self.backend.attachment_work = lambda p, r: self.work
        self.backend.attachment_card = lambda p, r: {'original':str(original),'source_sha256':row['sha256'],
            'path':item['md_path'],'md_sha256':item['md_sha256'],'version':item['delivery_version']}
        delivered = delivery_for(self.backend, paper, row, use_named=False)
        self.assertEqual(delivered['source_path'], str(original))
        read_attachment_delivery(delivered)
        self.assertFalse((self.work / 'named-input').exists())

    def test_named_attachment_publication_reuses_one_remote_file(self):
        from paperflow_prefect.legacy import bind
        from paperflow_prefect.settings import Settings
        from paperflow_prefect.unified_attachments import delivery_for
        from paperflow_prefect.attachment_publish import ingest_attachment
        from test_attachment_ingest import handoff
        from test_attachment_publish import FakeStore, route, Counter
        bind(Settings.load())
        from preextract_publish import _Budget
        item = handoff(self.base); original = Path(item['source_path'])
        row = {'path': str(original), 'name': original.name, 'relative': original.name,
               'file_id': item['attachment_id'], 'kind': 'attachment', 'bytes': original.stat().st_size,
               'sha256': item['source_sha256'], 'display_name': '论文题目_附件01.txt'}
        paper = {'paper_id': item['paper_id']}
        self.backend.attachment_work = lambda p, r: self.work
        self.backend.attachment_card = lambda p, r: {'original':str(original),'source_sha256':row['sha256'],
            'path':item['md_path'],'md_sha256':item['md_sha256'],'version':item['delivery_version']}
        delivered = delivery_for(self.backend, paper, row)
        client = FakeStore(); receipt_root = self.base / 'receipts'
        first = ingest_attachment(delivered, route(), client, receipt_root, _Budget(Counter()))
        writes = list(client.sent)
        second = ingest_attachment(delivered, route(), client, receipt_root, _Budget(Counter()))
        self.assertEqual(first['delivery_input_kind'], 'named-delivery-input/1')
        self.assertEqual(first['raw']['file_id'], second['raw']['file_id'])
        self.assertEqual(client.sent, writes)
        self.assertEqual(len(client.files), 1)
        self.assertEqual(next(iter(client.files.values()))['_bytes'], original.read_bytes())

    def test_old_material_does_not_rebind_or_create_an_alias(self):
        self.work.mkdir()
        (self.work / 'material.json').write_text('{}', encoding='utf8')
        seen = []
        with patch('paperflow_prefect.backend.bind_work'), patch('paperflow_prefect.backend.body.prepare_body', side_effect=lambda source, *args, **kwargs: seen.append(Path(source))):
            self.backend.prepare(self.paper)
        self.assertEqual(seen, [self.source])
        self.assertFalse((self.work / 'named-input').exists())

    def test_copy_is_reused_and_tampered_copy_is_not_overwritten(self):
        target = materialize_named_input(self.paper, self.paper['body'], self.work)
        self.assertEqual(materialize_named_input(self.paper, self.paper['body'], self.work), target)
        target.write_bytes(b'changed')
        with self.assertRaises(NamedDeliveryError):
            materialize_named_input(self.paper, self.paper['body'], self.work)
        self.assertEqual(target.read_bytes(), b'changed')
        self.assertEqual(self.source.read_bytes(), b'original bytes')

    def test_existing_delivery_file_symlink_is_never_followed(self):
        directory = self.work / 'named-input'; directory.mkdir(parents=True)
        outside = self.base / 'outside.txt'; outside.write_bytes(self.source.read_bytes())
        try:
            (directory / '论文题目.txt').symlink_to(outside)
        except OSError as exc:
            self.skipTest('cannot create symlink on this Windows host: ' + str(exc))
        with self.assertRaises(NamedDeliveryError):
            materialize_named_input(self.paper, self.paper['body'], self.work)
        self.assertEqual(outside.read_bytes(), self.source.read_bytes())

    def test_manifest_publication_interruption_never_exposes_partial_json(self):
        real_link = os.link
        def fail_manifest(source, target, *args, **kwargs):
            if Path(target).name == 'named-input-proof.json':
                raise OSError('simulated interruption before atomic publication')
            return real_link(source, target, *args, **kwargs)
        with patch('paperflow_prefect.named_delivery.os.link', side_effect=fail_manifest):
            with self.assertRaises(OSError):
                materialize_named_input(self.paper, self.paper['body'], self.work)
        self.assertFalse((self.work / 'named-input-proof.json').exists())
        target = materialize_named_input(self.paper, self.paper['body'], self.work)
        self.assertEqual(target.read_bytes(), self.source.read_bytes())

    def test_concurrent_delivery_reuses_one_complete_mapping(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(materialize_named_input, self.paper, self.paper['body'], self.work) for _ in range(2)]
            paths = [f.result() for f in futures]
        self.assertEqual(paths[0], paths[1])
        self.assertEqual(paths[0].read_bytes(), self.source.read_bytes())

    def test_rejects_path_traversal_and_extension_change(self):
        for invalid in ('../escape.txt', 'something.pdf', 'NUL.txt', 'dir\\escape.txt'):
            with self.subTest(invalid=invalid):
                self.paper['body']['display_name'] = invalid
                with self.assertRaises(NamedDeliveryError):
                    materialize_named_input(self.paper, self.paper['body'], self.work)

    def test_source_verified_title_override_replaces_incorrect_frozen_display_name(self):
        from paperflow_prefect.named_delivery import resolved_delivery_row
        source = self.base / 'cover.docx'
        xml = ('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
               '<w:body><w:tbl><w:tr><w:tc><w:p><w:r><w:t>题目：</w:t></w:r></w:p></w:tc>'
               '<w:tc><w:p><w:r><w:t>正确论文题目</w:t></w:r></w:p></w:tc></w:tr></w:tbl>'
               '</w:body></w:document>')
        with zipfile.ZipFile(source, 'w') as archive: archive.writestr('word/document.xml', xml)
        body = dict(self.paper['body'], path=str(source), sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                    bytes=source.stat().st_size, display_name='之前的错误题目.docx')
        paper = dict(self.paper, body=body, group_id='group-1')
        data = {'schema':'paperflow-title-correction/1','paper_id':paper['paper_id'],
                'group_id':'group-1','body_file_id':body['file_id'],'source_sha256':body['sha256'],
                'source_path':str(source),'title':'正确论文题目','reference':'正文 Word 封面表格单元格',
                'excerpt':'正确论文题目'}
        path=self.backend.root/'naming-overrides'/paper['paper_id']/'title.json';path.parent.mkdir(parents=True)
        path.write_text(json.dumps(data,ensure_ascii=False),encoding='utf8')
        fixed=resolved_delivery_row(paper,body,self.backend.root)
        self.assertEqual(fixed['display_name'],'正确论文题目.docx')
        self.assertEqual(body['display_name'],'之前的错误题目.docx')
        source.write_bytes(b'tampered')
        unresolved=resolved_delivery_row(paper,body,self.backend.root)
        self.assertIsNone(unresolved['display_name'])

    def test_title_override_can_reuse_hash_bound_doc_conversion(self):
        from paperflow_prefect.named_delivery import resolved_delivery_row
        source = self.base/'original.doc'; source.write_bytes(b'legacy original')
        digest=hashlib.sha256(source.read_bytes()).hexdigest()
        body=dict(self.paper['body'],path=str(source),sha256=digest,bytes=source.stat().st_size,display_name=None)
        paper=dict(self.paper,body=body,group_id='group-1')
        work=self.backend.root/'papers'/paper['paper_id']/'body'/'original-work'
        converted=work/'private'/'converted'/'original.docx'; converted.parent.mkdir(parents=True)
        xml=('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
             '<w:body><w:tbl><w:tr><w:tc><w:p><w:r><w:t>题目：</w:t></w:r></w:p></w:tc>'
             '<w:tc><w:p><w:r><w:t>已核验的论文题目</w:t></w:r></w:p></w:tc></w:tr></w:tbl>'
             '</w:body></w:document>')
        with zipfile.ZipFile(converted,'w') as archive: archive.writestr('word/document.xml',xml)
        convsha=hashlib.sha256(converted.read_bytes()).hexdigest()
        material=work/'material.json';material.write_text(json.dumps({'source':{'source_sha256':digest},
            'file_hashes':{str(converted.resolve()):convsha}},ensure_ascii=False),encoding='utf8')
        obj={'schema':'paperflow-title-correction/1','paper_id':paper['paper_id'],'group_id':'group-1',
             'body_file_id':body['file_id'],'source_sha256':digest,'source_path':str(source),
             'title':'已核验的论文题目','reference':'正文 Word 封面表格单元格','excerpt':'已核验的论文题目',
             'converted_evidence':{'path':str(converted),'sha256':convsha,'material_path':str(material),
                 'material_sha256':hashlib.sha256(material.read_bytes()).hexdigest()}}
        path=self.backend.root/'naming-overrides'/paper['paper_id']/'title.json';path.parent.mkdir(parents=True)
        path.write_text(json.dumps(obj,ensure_ascii=False),encoding='utf8')
        self.assertEqual(resolved_delivery_row(paper,body,self.backend.root)['display_name'],'已核验的论文题目.doc')
        converted.write_bytes(b'changed')
        self.assertIsNone(resolved_delivery_row(paper,body,self.backend.root)['display_name'])

    def test_title_override_source_changing_during_read_is_not_applied(self):
        from paperflow_prefect.named_delivery import resolved_delivery_row
        source = self.base/'cover.docx'; source.write_bytes(b'original evidence')
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        body = dict(self.paper['body'], path=str(source), sha256=digest, bytes=source.stat().st_size)
        paper = dict(self.paper, body=body, group_id='group-1')
        data = {'schema':'paperflow-title-correction/1','paper_id':paper['paper_id'],
                'group_id':'group-1','body_file_id':body['file_id'],'source_sha256':digest,
                'source_path':str(source),'title':'正确论文题目','reference':'正文 Word 封面表格单元格',
                'excerpt':'正确论文题目'}
        path=self.backend.root/'naming-overrides'/paper['paper_id']/'title.json';path.parent.mkdir(parents=True)
        path.write_text(json.dumps(data,ensure_ascii=False),encoding='utf8')
        def changed(_):
            source.write_bytes(b'evidence changed while reading')
            return {'format':'docx','cover_table_title':{'title':'正确论文题目','excerpt':'正确论文题目'}}
        with patch('paperflow_prefect.group_sort_cli._read_for_sort', side_effect=changed):
            self.assertIsNone(resolved_delivery_row(paper,body,self.backend.root)['display_name'])

    def test_title_override_record_changing_during_read_is_not_applied(self):
        from paperflow_prefect.named_delivery import resolved_delivery_row
        source = self.base/'cover.docx'; source.write_bytes(b'original evidence')
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        body = dict(self.paper['body'], path=str(source), sha256=digest, bytes=source.stat().st_size)
        paper = dict(self.paper, body=body, group_id='group-1')
        data = {'schema':'paperflow-title-correction/1','paper_id':paper['paper_id'],
                'group_id':'group-1','body_file_id':body['file_id'],'source_sha256':digest,
                'source_path':str(source),'title':'正确论文题目','reference':'正文 Word 封面表格单元格',
                'excerpt':'正确论文题目'}
        path=self.backend.root/'naming-overrides'/paper['paper_id']/'title.json';path.parent.mkdir(parents=True)
        path.write_text(json.dumps(data,ensure_ascii=False),encoding='utf8')
        def changed(_):
            path.write_text(json.dumps(dict(data,title='另一个题目'),ensure_ascii=False),encoding='utf8')
            return {'format':'docx','cover_table_title':{'title':'正确论文题目','excerpt':'正确论文题目'}}
        with patch('paperflow_prefect.group_sort_cli._read_for_sort', side_effect=changed):
            self.assertIsNone(resolved_delivery_row(paper,body,self.backend.root)['display_name'])

    def test_separate_cover_override_is_bound_to_frozen_handoff_and_both_sources(self):
        from paperflow_prefect.named_delivery import resolved_delivery_row
        group = self.base/'刘婉婷_201913137111'; group.mkdir()
        source=group/'正文.docx'; source.write_bytes(b'original body')
        cover=group/'毕业论文封面.docx'
        paragraphs = ['题目：基于Java的宠物领养系统开发','学院：计算机学院',
                      '学号：201913137111','学生姓名：刘婉婷']
        xml = ('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>'+
               ''.join('<w:p><w:r><w:t>'+v+'</w:t></w:r></w:p>' for v in paragraphs)+
               '</w:body></w:document>')
        with zipfile.ZipFile(cover,'w') as archive: archive.writestr('word/document.xml',xml)
        body_sha=hashlib.sha256(source.read_bytes()).hexdigest()
        cover_sha=hashlib.sha256(cover.read_bytes()).hexdigest()
        body=dict(self.paper['body'],path=str(source),relative=source.name,sha256=body_sha,
                  bytes=source.stat().st_size,upstream_file_id='groupbody',display_name=None)
        handoff={'group_id':'group-1','handoff_id':'snap-1','result_version':'version-1',
                 'body':{'file_id':'groupbody','relative':source.name,'sha256':body_sha},
                 'attachments':[], 'trash':[{'file_id':'groupcover','relative':cover.name,'sha256':cover_sha}],
                 'unresolved':[]}
        handoff_path=self.base/'handoff.json';handoff_path.write_text(json.dumps(handoff,ensure_ascii=False),encoding='utf8')
        handoff_sha=hashlib.sha256(handoff_path.read_bytes()).hexdigest()
        paper=dict(self.paper,body=body,group_id='group-1',origin_group_id='group-1',
                   handoff_id='snap-1',result_version='version-1',
                   handoff_ref={'path':str(handoff_path),'sha256':handoff_sha,'hash_kind':'file-sha256'})
        record={'schema':'paperflow-title-correction/2','paper_id':paper['paper_id'],
                'group_id':'group-1','body_file_id':body['file_id'],'source_sha256':body_sha,
                'source_path':str(source),'title':'基于Java的宠物领养系统开发',
                'reference':'同组独立封面','excerpt':'基于Java的宠物领养系统开发',
                'cover_reference':'正文 Word 开头文本','cover_file_id':'groupcover',
                'cover_source_path':str(cover),'cover_sha256':cover_sha,
                'handoff_path':str(handoff_path),'handoff_sha256':handoff_sha}
        dest=self.backend.root/'naming-overrides'/paper['paper_id']/'title.json';dest.parent.mkdir(parents=True)
        dest.write_text(json.dumps(record,ensure_ascii=False),encoding='utf8')
        fixed=resolved_delivery_row(paper,body,self.backend.root)
        self.assertEqual(fixed['display_name'],'基于Java的宠物领养系统开发.docx')
        self.assertIsNone(body['display_name'])
        cover_bytes=cover.read_bytes();handoff_bytes=handoff_path.read_bytes()
        cover.write_bytes(b'changed cover')
        self.assertIsNone(resolved_delivery_row(paper,body,self.backend.root)['display_name'])
        cover.write_bytes(cover_bytes)
        handoff_path.write_bytes(b'changed handoff')
        self.assertIsNone(resolved_delivery_row(paper,body,self.backend.root)['display_name'])
        handoff_path.write_bytes(handoff_bytes)
        source.write_bytes(b'changed body')
        self.assertIsNone(resolved_delivery_row(paper,body,self.backend.root)['display_name'])


if __name__ == '__main__':
    unittest.main()
