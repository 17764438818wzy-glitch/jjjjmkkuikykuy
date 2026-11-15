import json
import subprocess
import sys
import tempfile
import threading
import unittest
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

try:
    from paperflow_prefect import group_sort_cli
except ImportError:
    group_sort_cli = None


class LocalDecision(BaseHTTPRequestHandler):
    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        keys = payload['questions']['decision']['criteria']
        choice = 'file_01' if 'file_01' in keys else 'attachment'
        answer = {'answers': {'decision': {'type': 'choice', 'choice': choice,
            'probabilities': {key: float(key == choice) for key in keys}, 'confidence': 1.0}}}
        data = json.dumps(answer).encode()
        self.send_response(200)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)
    def log_message(self, *_): pass


class GroupSortCliTests(unittest.TestCase):
    def test_explicit_group_produces_a_readable_handoff_without_moving_files(self):
        self.assertIsNotNone(group_sort_cli, '尚未提供分类入口')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            library, group = root / 'library', root / 'library' / 'school' / 'person'
            group.mkdir(parents=True)
            body, attachment = group / '论文.docx', group / '答辩.pptx'
            xml = ('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
                   '<w:body><w:p><w:r><w:t>论文题目：客户消费习惯研究</w:t></w:r></w:p>'
                   '<w:p><w:r><w:t>摘要</w:t></w:r></w:p></w:body></w:document>')
            with zipfile.ZipFile(body, 'w') as archive: archive.writestr('word/document.xml', xml)
            attachment.write_bytes(b'slides')
            originals = {p.name: p.read_bytes() for p in (body, attachment)}
            server = ThreadingHTTPServer(('127.0.0.1', 0), LocalDecision)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            try:
                endpoint = f'http://127.0.0.1:{server.server_port}/v1/systemone'
                result = subprocess.run([sys.executable, '-X', 'utf8', '-m', 'paperflow_prefect.group_sort_cli',
                    '--library', str(library), '--group', str(group), '--output-dir', str(root / 'out'),
                    '--endpoint', endpoint, '--verified-new-group', str(group)],
                    capture_output=True, text=True, encoding='utf8', timeout=20)
            finally:
                server.shutdown()
                server.server_close()
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(result.stdout)
            self.assertEqual(len(report), 1)
            handoff = json.loads(Path(report[0]['handoff']).read_text(encoding='utf8'))
            self.assertEqual(handoff['body']['relative'], '论文.docx')
            self.assertEqual(handoff.get('paper_title'), '客户消费习惯研究')
            self.assertEqual(handoff['title_source']['file_id'], handoff['body']['file_id'])
            self.assertIn('excerpt', handoff['title_source'])
            self.assertIn('客户消费习惯研究', handoff['title_source']['excerpt'])
            self.assertIsNone(handoff['title_note'])
            self.assertEqual([row['relative'] for row in handoff['attachments']], ['答辩.pptx'])
            self.assertEqual({p.name: p.read_bytes() for p in (body, attachment)}, originals)

    def test_unlabelled_cover_title_is_marked_as_a_candidate(self):
        self.assertIsNotNone(group_sort_cli)
        result = group_sort_cli._title_from_body(
            {'file_id': 'a'}, {'format': 'docx', 'text': '本科毕业论文\n企业绩效评价研究\n摘要\n正文'})
        self.assertEqual(len(result), 4)
        title, reference, excerpt, confirmed = result
        self.assertEqual(title, '企业绩效评价研究')
        self.assertIn(title, excerpt)
        self.assertFalse(confirmed)

    def test_labelled_title_continuation_survives_blank_paragraphs(self):
        evidence = {'format': 'docx', 'text': '本科毕业设计\n题目\n武汉市黄陂区木兰暖村\n\n游客中心建筑方案设计\n\n学 院：城市建设学院\n摘要'}
        title, reference, excerpt, confirmed = group_sort_cli._title_from_body({'file_id': 'a'}, evidence)
        self.assertEqual(title, '武汉市黄陂区木兰暖村游客中心建筑方案设计')
        self.assertEqual(''.join(excerpt.split()), title)
        self.assertTrue(confirmed)

    def test_cover_date_is_not_suggested_as_a_paper_title(self):
        evidence = {'format': 'docx', 'text': '二零二二年十一月\n摘要'}
        title, _, _, _ = group_sort_cli._title_from_body({'file_id': 'a'}, evidence)
        self.assertEqual(title, '')

    def test_obviously_repeated_cover_title_is_not_duplicated(self):
        actual = '校园即时服务系统的设计与实现'
        evidence = {'format': 'docx', 'text': actual * 2 + '\n摘要'}
        title, _, excerpt, confirmed = group_sort_cli._title_from_body({'file_id': 'a'}, evidence)
        self.assertEqual(title, actual)
        self.assertEqual(excerpt, actual)
        self.assertFalse(confirmed)

    def test_obvious_cover_label_does_not_hide_the_next_candidate_title(self):
        evidence = {'format': 'pdf', 'pages': [{'page': 1, 'text':
            '本科生毕业设计\n基于微信小程序的移动学习平台的设计与实现\n姓\n名：\n李同学'}]}
        title, _, _, confirmed = group_sort_cli._title_from_body({'file_id': 'a'}, evidence)
        self.assertEqual(title, '基于微信小程序的移动学习平台的设计与实现')
        self.assertFalse(confirmed)

    def test_labelled_pdf_title_continues_until_the_next_cover_field(self):
        from paperflow_prefect.group_classification import _title_proof
        evidence = {'format': 'pdf', 'pages': [{'page': 1, 'text':
            '题目：\n年产20 万吨硫酸厂转化工段工艺\n设计\n学\n院:\n化学与化工学院'}]}
        title, reference, excerpt, confirmed = group_sort_cli._title_from_body({'file_id': 'a'}, evidence)
        self.assertEqual(title, '年产20 万吨硫酸厂转化工段工艺设计')
        self.assertTrue(confirmed)
        self.assertTrue(_title_proof(evidence, title, excerpt, labelled=True, reference=reference))

    def test_administrative_cover_lines_do_not_hide_the_title(self):
        for administrative in ('完 成 时 间 2022 年9 月30 日', '“旺工淡学”海南省旅游业人才培养项目'):
            evidence = {'format': 'pdf', 'pages': [{'page': 1,
                'text': administrative + '\n基于二维码的商品溯源系统设计\n摘要'}]}
            with self.subTest(administrative=administrative):
                title, _, _, confirmed = group_sort_cli._title_from_body({'file_id': 'a'}, evidence)
                self.assertEqual(title, '基于二维码的商品溯源系统设计')
                self.assertFalse(confirmed)

    def test_labelled_title_stops_before_other_cover_fields(self):
        for label in ('论文编号', '作者姓名', '完成日期'):
            evidence = {'format': 'docx', 'text':
                f'题目：基于神经网络的图像分类研究\n{label}：20200123\n学院：计算机学院'}
            with self.subTest(label=label):
                title, _, _, confirmed = group_sort_cli._title_from_body({'file_id': 'a'}, evidence)
                self.assertEqual(title, '基于神经网络的图像分类研究')
                self.assertTrue(confirmed)


if __name__ == '__main__': unittest.main()
