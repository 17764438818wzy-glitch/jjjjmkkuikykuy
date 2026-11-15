import json
import sqlite3
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import urlsplit

from fastapi.testclient import TestClient
from test_registered_input import registered


def test_upstream_reservation_to_local_pool_and_handoff_over_real_http(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import FirstStepHTTPSource, discover_once
    from waiting_api import create_app

    _, receipt_root, receipt, item = registered
    task_id = '00000000-0000-4000-8000-000000000001'
    retention_id = str(uuid.uuid4())
    queue = LocalQueue(tmp_path / 'queue.sqlite3', require_retention=True)
    calls = []
    identity = {}

    class Upstream(BaseHTTPRequestHandler):
        def send_json(self, data):
            body = json.dumps(data, ensure_ascii=False).encode('utf8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = urlsplit(self.path).path
            if path == '/api/v1/first-step/tasks':
                return self.send_json({'items': [{'task_id': task_id, 'control_revision': 1}],
                                       'page': 1, 'page_size': 200, 'total': 1, 'has_next': False})
            if path == '/api/v1/first-step/tasks/' + task_id:
                return self.send_json({'task_id': task_id, 'control_revision': 1, 'page': 1,
                                       'page_size': 200, 'list_total': 1, 'items': [item]})
            if path == '/api/v1/first-step/tasks/' + task_id + '/local-retentions':
                refs = [{**identity, 'state': 'accepted' if 'confirm' in calls else 'reserved'}] if identity else []
                return self.send_json({'task_id': task_id, 'items': refs, 'page': 1, 'page_size': 200,
                                       'list_total': len(refs), 'has_next': False, 'revision': len(calls)})
            self.send_error(404)

        def do_POST(self):
            path = urlsplit(self.path).path
            data = json.loads(self.rfile.read(int(self.headers['Content-Length'])).decode('utf8'))
            base = '/api/v1/first-step/tasks/' + task_id + '/local-retentions'
            if path == base:
                calls.append('reserve')
                identity.update({'task_id': task_id, **data, 'retention_id': retention_id})
                return self.send_json({**identity, 'state': 'reserved', 'created': True})
            if path == base + '/' + retention_id + '/confirm':
                calls.append('confirm')
                with sqlite3.connect(queue.path) as db:
                    row = db.execute('SELECT paper_id,receipt_id,state FROM groups WHERE task_id=? AND group_id=?',
                                     (task_id, item['group_id'])).fetchone()
                assert row == (item['paper_id'], item['receipt_id'], 'waiting')
                assert data == {'consumer_id': 'local-intake', 'receipt_id': item['receipt_id']}
                return self.send_json({**identity, 'state': 'accepted', 'created': False})
            self.send_error(404)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        source = FirstStepHTTPSource(f'http://127.0.0.1:{server.server_port}')
        result = discover_once(source, queue, receipt_root=receipt_root, retention_client=source)
        assert result['complete'] is True
        assert calls == ['reserve', 'confirm']
        with TestClient(create_app(queue, enable_claims=True)) as client:
            assert client.get('/api/v1/local-intake/preprocessing/summary').json()['waiting_count'] == 1
            handoff = client.post('/api/v1/local-intake/preprocessing/claims',
                                  json={'consumer_id': 'preprocessor', 'attempt_id': 'test-attempt'})
            assert handoff.status_code == 200
            assert handoff.json()['body']['file_id'] == receipt['inventory']['body']['file_id']
            assert [row['file_id'] for row in handoff.json()['attachments']] == [
                row['file_id'] for row in receipt['inventory']['attachments']
            ]
            assert set(handoff.json()) >= {'body', 'attachments', 'paper_id', 'receipt_id'}
            assert client.get('/api/v1/local-intake/preprocessing/summary').json()['waiting_count'] == 0
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
