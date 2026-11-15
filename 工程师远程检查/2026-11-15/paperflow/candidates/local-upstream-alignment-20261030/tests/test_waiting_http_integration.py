import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import parse_qs, urlsplit

from fastapi.testclient import TestClient

from test_registered_input import registered


def test_local_http_source_to_waiting_api_with_real_synthetic_receipt(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import FirstStepHTTPSource, discover_once
    from waiting_api import create_app

    _, receipt_root, _, item = registered
    task_id = '00000000-0000-4000-8000-000000000001'

    class SyntheticUpstream(BaseHTTPRequestHandler):
        def do_GET(self):
            target = urlsplit(self.path)
            params = parse_qs(target.query)
            assert params['page'] == ['1'] and params['page_size'] == ['200']
            if target.path == '/api/v1/first-step/tasks':
                payload = {'items': [{'task_id': task_id, 'control_revision': 7}],
                           'page': 1, 'page_size': 200, 'total': 1, 'has_next': False}
            elif target.path == '/api/v1/first-step/tasks/' + task_id:
                assert params['filter'] == ['passed']
                payload = {'task_id': task_id, 'control_revision': 7, 'page': 1,
                           'page_size': 200, 'list_total': 1, 'items': [item]}
            else:
                self.send_error(404)
                return
            body = json.dumps(payload, ensure_ascii=False).encode('utf8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), SyntheticUpstream)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
        source = FirstStepHTTPSource(f'http://127.0.0.1:{server.server_port}')
        assert discover_once(source, queue, receipt_root=receipt_root)['complete'] is True
        with TestClient(create_app(queue)) as client:
            response = client.get('/api/v1/local-intake/preprocessing/summary')
        assert response.status_code == 200
        assert response.json()['waiting_count'] == 1
        assert response.json()['waiting_available'] is True
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
