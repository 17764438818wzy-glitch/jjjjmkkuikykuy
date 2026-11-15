import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    from paperflow_prefect import nimble_group
except ImportError:
    nimble_group = None


class DecisionHandler(BaseHTTPRequestHandler):
    payloads = []
    response_choice = 'file_01'
    def do_POST(self):
        length = int(self.headers['Content-Length'])
        payload = json.loads(self.rfile.read(length).decode('utf8'))
        type(self).payloads.append(payload)
        keys = list(payload['questions']['decision']['criteria'])
        probabilities = {key: float(key == type(self).response_choice) for key in keys}
        response = json.dumps({'answers': {'decision': {'type': 'choice',
            'choice': type(self).response_choice, 'probabilities': probabilities, 'confidence': 1.0}}}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(response)))
        self.end_headers()
        self.wfile.write(response)
    def log_message(self, *_): pass


class NimbleGroupTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(nimble_group, 'Nimble 按组判断尚不存在')
        DecisionHandler.payloads = []
        DecisionHandler.response_choice = 'file_01'
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), DecisionHandler)
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        endpoint = f'http://127.0.0.1:{self.server.server_port}/v1/systemone'
        self.judge = nimble_group.NimbleJudge(endpoint=endpoint)
        self.records = [
            {'file_id': 'a-body', 'relative': '论文.docx', 'evidence': {'text': '摘要'}},
            {'file_id': 'a-report', 'relative': '开题报告.docx', 'evidence': {'text': '开题报告'}},
        ]

    def test_body_choice_maps_only_to_current_group_candidates(self):
        result = self.judge.choose_body(self.records)
        self.assertEqual(result['choice'], 'a-body')
        request = DecisionHandler.payloads[0]
        self.assertEqual(request['model'], 'nimble:9b-q4_K_M')
        self.assertEqual(set(request['questions']['decision']['criteria']), {'file_01', 'file_02', 'none'})
        self.assertIn('论文.docx', json.dumps(request['state'], ensure_ascii=False))
        self.assertNotIn('李四附件.zip', json.dumps(request, ensure_ascii=False))

    def test_unknown_model_choice_cannot_be_used_as_another_groups_file(self):
        DecisionHandler.response_choice = 'other_group_id'
        with self.assertRaises(ValueError): self.judge.choose_body(self.records)

    def test_attachment_or_trash_is_one_decision_for_the_whole_file(self):
        DecisionHandler.response_choice = 'attachment'
        result = self.judge.choose_other(
            {'file_id': 'archive', 'relative': '附件.zip', 'evidence': {'members': [{'name': 'a.py'}, {'name': 'b.pdf'}]}},
            'a-body')
        self.assertEqual(result['choice'], 'attachment')
        self.assertEqual(len(DecisionHandler.payloads), 1)
        self.assertEqual(set(DecisionHandler.payloads[0]['questions']['decision']['criteria']),
                         {'attachment', 'trash', 'uncertain'})

    def test_model_can_decline_to_classify_an_unreadable_document(self):
        DecisionHandler.response_choice = 'uncertain'
        try:
            result = self.judge.choose_other(
                {'file_id': 'scan', 'relative': '扫描件.pdf', 'evidence': {'pages': [{'text': ''}]}},
                'a-body')
        except ValueError as exc:
            self.fail('模型必须能选择无法判断: ' + str(exc))
        self.assertEqual(result['choice'], 'uncertain')

    def test_nonlocal_endpoint_is_rejected(self):
        with self.assertRaises(ValueError):
            nimble_group.NimbleJudge(endpoint='http://example.com/v1/systemone')

    def test_model_evidence_discloses_truncated_archive_directory(self):
        DecisionHandler.response_choice = 'attachment'
        members = [{'name': f'{i:03}.txt'} for i in range(40)]
        self.judge.choose_other({'file_id': 'archive', 'relative': '资料.zip',
                                 'evidence': {'members': members}}, None)
        evidence = DecisionHandler.payloads[0]['state']['当前文件']['本地读取证据']
        self.assertEqual(len(evidence['members']), 25)
        self.assertTrue(evidence.get('truncated'))


if __name__ == '__main__': unittest.main()
