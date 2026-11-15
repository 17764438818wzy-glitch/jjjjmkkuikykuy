"""LAN allowlist, request safety, and Windows connection settings regressions."""
import io
import json
import os
import ssl
import subprocess
import sys
import threading
import tkinter as tk
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, HTTPSHandler

import pytest

import ragflow_client as client_module
from ragflow_client import RagflowClient

LAN = 'http://192.168.1.120:8088'
KEY = 'fixture-ragflow-key'


@pytest.mark.parametrize('url', [
    LAN, LAN + '/', ' HTTP://192.168.1.120:8088/ ',
    'http://localhost', 'http://localhost:8080', 'http://LOCALHOST:8080',
    'http://127.0.0.1:8080', 'http://[::1]:8080',
    'https://example.test', 'https://192.168.1.121:8088',
])
def test_allowed_service_addresses(url):
    assert RagflowClient(url, KEY).base_url


@pytest.mark.parametrize('url', [
    'http://192.168.1.120', 'http://192.168.1.120:80',
    'http://192.168.1.120:8089', 'http://192.168.1.121:8088',
    'http://192.168.1.120.evil.test:8088', 'http://192.168.1.120:80880',
    'http://example.test:8088', 'http://127.0.0.2:8088',
    'http://localhost.evil.test', 'http://[::ffff:192.168.1.120]:8088',
    'http://192.168.1.120:<REDACTED_PASSWORD>@evil.test', 'http://evil.test@192.168.1.120:8088',
    'http://@192.168.1.120:8088', 'http://user:<REDACTED_PASSWORD>@localhost',
    LAN + '?url=http://localhost', LAN + '#fragment',
    'ftp://192.168.1.120:8088', '192.168.1.120:8088', '',
    'http://localhost:bad', 'https://example.test:65536', 'http://[::1',
])
def test_disallowed_addresses_fail_before_any_request(url):
    with pytest.raises(ValueError, match='地址'):
        RagflowClient(url, KEY)


class Transport:
    """Replace only network I/O; run the real request construction and policy."""
    def __init__(self, *results):
        self.results = list(results)
        self.calls = []

    def open(self, request, timeout):
        self.calls.append((request, timeout))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return io.BytesIO(json.dumps(result).encode())


@pytest.fixture
def client(monkeypatch):
    # The local address keeps error/retry tests independent of the LAN exception.
    instance = RagflowClient('http://localhost', KEY)
    monkeypatch.setattr(client_module, 'sleep', lambda _: None, raising=False)
    return instance


@pytest.mark.parametrize('suffix', ['', '/', '/api/v1', '/api/v1/', '/api/v1/api/v1/'])
def test_api_prefix_is_added_exactly_once(suffix):
    client = RagflowClient(LAN + suffix, KEY)
    client.opener = Transport({'code': 0, 'data': []})
    assert client.datasets() == []
    assert client.base_url == LAN
    assert client.opener.calls[0][0].full_url == LAN + '/api/v1/datasets?page=1&page_size=100'


def test_gateway_path_is_preserved():
    client = RagflowClient('https://example.test/gateway/api/v1/', KEY)
    assert client.base_url == 'https://example.test/gateway'


@pytest.mark.parametrize('scheme', ['http', 'https'])
def test_lan_requests_ignore_system_and_environment_proxies(monkeypatch, scheme):
    monkeypatch.setattr('urllib.request.getproxies', lambda: {
        'http': 'http://127.0.0.1:1', 'https': 'http://127.0.0.1:1'})
    before = dict(os.environ)
    client = RagflowClient(scheme + '://192.168.1.120:8088', KEY)
    assert not any(isinstance(handler, ProxyHandler) and handler.proxies
                   for handler in client.opener.handlers)
    assert dict(os.environ) == before
    assert any(isinstance(handler, HTTPSHandler) for handler in client.opener.handlers)


def test_read_retries_transient_failures_then_returns_data(client):
    client.opener = Transport(URLError(ConnectionRefusedError()), TimeoutError(),
                              {'code': 0, 'data': [{'id': 'one', 'name': 'test'}]})
    assert client.datasets() == [{'id': 'one', 'name': 'test'}]
    assert len(client.opener.calls) == 3


@pytest.mark.parametrize('failure,category', [
    (URLError(ConnectionRefusedError()), '连接失败'),
    (ConnectionResetError(), '连接失败'),
    (TimeoutError(), '超时'),
    (URLError(TimeoutError()), '超时'),
])
def test_read_failures_are_classified_and_bounded(client, failure, category):
    client.opener = Transport(failure, failure, failure)
    with pytest.raises(RuntimeError, match=category):
        client.datasets()
    assert len(client.opener.calls) == 3


@pytest.mark.parametrize('method', ['POST', 'PUT', 'PATCH', 'DELETE'])
@pytest.mark.parametrize('failure,category', [
    (TimeoutError(), '超时'), (URLError(ConnectionRefusedError()), '连接失败'),
    (HTTPError(LAN, 503, 'unavailable', {}, None), 'HTTP 503'),
])
def test_writes_are_never_automatically_retried(client, method, failure, category):
    client.opener = Transport(failure)
    with pytest.raises(RuntimeError, match=category):
        client.request(method, '/datasets/test/documents', b'fixture')
    assert len(client.opener.calls) == 1


@pytest.mark.parametrize('status', [401, 403])
def test_http_auth_failure_is_clear_and_not_retried(client, status):
    client.opener = Transport(HTTPError(LAN, status, KEY, {}, None))
    with pytest.raises(RuntimeError, match='认证失败') as caught:
        client.datasets()
    assert KEY not in str(caught.value)
    assert caught.value.status_code == status
    assert len(client.opener.calls) == 1


@pytest.mark.parametrize('code', [109, 401, 403])
def test_json_auth_failure_is_clear_and_not_retried(client, code):
    client.opener = Transport({'code': code, 'message': KEY, 'data': None})
    with pytest.raises(RuntimeError, match='认证失败') as caught:
        client.datasets()
    assert KEY not in str(caught.value)
    assert len(client.opener.calls) == 1


@pytest.mark.parametrize('status', [502, 503, 504])
def test_transient_http_read_failure_can_recover(client, status):
    client.opener = Transport(HTTPError(LAN, status, 'transient', {}, None),
                              {'code': 0, 'data': []})
    assert client.datasets() == []
    assert len(client.opener.calls) == 2


def test_tls_verification_failure_is_not_retried(client):
    client.opener = Transport(URLError(ssl.SSLCertVerificationError('invalid cert')))
    with pytest.raises(RuntimeError, match='证书'):
        client.datasets()
    assert len(client.opener.calls) == 1


def test_other_business_error_redacts_key(client):
    client.opener = Transport({'code': 102, 'message': 'error ' + KEY})
    with pytest.raises(RuntimeError) as caught:
        client.datasets()
    assert KEY not in str(caught.value)
    assert len(client.opener.calls) == 1


@pytest.fixture
def local_server(monkeypatch):
    monkeypatch.setattr('urllib.request.getproxies', lambda: {})
    monkeypatch.setattr(client_module, 'sleep', lambda _: None)
    responses, calls = [], []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            calls.append((self.command, self.path))
            status, payload, headers = responses.pop(0) if responses else (500, {}, {})
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(payload if isinstance(payload, bytes) else json.dumps(payload).encode())

        def do_POST(self):
            self.rfile.read(int(self.headers['Content-Length']))
            self.do_GET()

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=.05), daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}', responses, calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize('status', [301, 302, 303, 307, 308])
def test_real_http_redirect_is_never_followed_or_retried(local_server, status):
    url, responses, calls = local_server
    responses.append((status, {}, {'Location': url + '/redirect-target'}))
    with pytest.raises(RuntimeError, match='重定向'):
        RagflowClient(url, KEY).datasets()
    assert calls == [('GET', '/api/v1/datasets?page=1&page_size=100')]


def test_dataset_pagination_retries_only_the_failed_page(local_server):
    url, responses, calls = local_server
    first = [{'id': str(i), 'name': 'fixture'} for i in range(100)]
    responses.extend([(200, {'code': 0, 'data': first}, {}), (503, {}, {}),
                      (200, {'code': 0, 'data': [{'id': 'last', 'name': 'fixture'}]}, {})])
    rows = RagflowClient(url + '/api/v1/', KEY).datasets()
    assert len(rows) == 101
    assert len({row['id'] for row in rows}) == 101
    assert calls == [('GET', '/api/v1/datasets?page=1&page_size=100'),
                     ('GET', '/api/v1/datasets?page=2&page_size=100'),
                     ('GET', '/api/v1/datasets?page=2&page_size=100')]


def test_real_upload_failure_does_not_resend_document(local_server, tmp_path):
    url, responses, calls = local_server
    responses.append((503, {}, {}))
    document = tmp_path / 'fixture.md'
    document.write_text('fixture', encoding='utf-8')
    with pytest.raises(RuntimeError, match='未自动重试'):
        RagflowClient(url, KEY).upload('fixture', document)
    assert calls == [('POST', '/api/v1/datasets/fixture/documents')]


def test_upload_preserves_asset_filename_and_mime_type(tmp_path):
    client=RagflowClient('http://localhost',KEY)
    transport=Transport({'code':0,'data':[{'id':'asset-doc'}]})
    client.opener=transport
    image=tmp_path/'figure.png';image.write_bytes(b'PNG-fixture')
    assert client.upload('fixture',image,remote_name='paper__asset_figure.png')=='asset-doc'
    request=transport.calls[0][0]
    assert 'multipart/form-data; boundary=' in request.headers['Content-type']
    assert b'filename="paper__asset_figure.png"' in request.data
    assert b'Content-Type: image/png' in request.data
    assert b'PNG-fixture' in request.data


def test_non_json_response_is_not_retried(local_server):
    url, responses, calls = local_server
    responses.append((200, b'<html>Not an API response</html>', {}))
    with pytest.raises(RuntimeError, match='JSON'):
        RagflowClient(url, KEY).datasets()
    assert len(calls) == 1


@pytest.mark.parametrize('save_action', ['connect', 'close'])
def test_ui_saves_url_and_dpapi_key_and_restores_in_new_process(tmp_path, monkeypatch, save_action):
    from app import Workbench
    from auto_config import load_key

    monkeypatch.delenv('RAGFLOW_API_KEY', raising=False)
    root = tk.Tk()
    root.withdraw()
    app = Workbench(root, tmp_path)
    try:
        assert app.url.get() == LAN
        chosen_url = LAN if save_action == 'connect' else 'https://example.test:8443'
        app.url.set(chosen_url + '/api/v1/' if save_action == 'connect' else chosen_url)
        app.key.set(KEY)
        if save_action == 'connect':
            monkeypatch.setattr(RagflowClient, 'datasets', lambda _: [])
            monkeypatch.setattr(app, 'async_task', lambda action, done: done(action()))
            monkeypatch.setattr(app, 'error', lambda message: pytest.fail(str(message)))
            app.connect()
            assert app.connected_url == LAN
            assert app.url.get() == LAN
        else:
            for callback in root.tk.call('after', 'info'):
                root.after_cancel(callback)
            app.close()
            root = None
        assert json.loads((tmp_path / 'settings.json').read_text(encoding='utf-8'))['ragflow_url'] == chosen_url
        assert load_key(tmp_path, 'ragflow') == KEY
        assert KEY.encode() not in (tmp_path / 'ragflow_key.dpapi').read_bytes()
        assert KEY not in (tmp_path / 'settings.json').read_text(encoding='utf-8')
        script = '''
import sys, tkinter as tk
from pathlib import Path
from app import Workbench
root = tk.Tk(); root.withdraw()
app = Workbench(root, Path(sys.argv[1]))
assert app.url.get() == sys.argv[2]
assert app.key.get() == 'fixture-ragflow-key'
for callback in root.tk.call('after', 'info'): root.after_cancel(callback)
root.destroy()
print('URL and encrypted key restored in a new process')
'''
        result = subprocess.run([sys.executable, '-B', '-c', script, str(tmp_path), chosen_url],
                                cwd=Path(__file__).parent, capture_output=True, text=True, timeout=30,
                                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        assert result.returncode == 0, result.stderr
    finally:
        if root is not None:
            for callback in root.tk.call('after', 'info'):
                root.after_cancel(callback)
            root.destroy()
