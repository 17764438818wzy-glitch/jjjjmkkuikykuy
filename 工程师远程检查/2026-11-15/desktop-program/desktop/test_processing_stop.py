"""Cancellation must stop real work, not just release the UI."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest


def test_stop_during_intake_is_not_cleared(tmp_path):
    from auto_pipeline import AutoProcessor
    from engine import Store
    source = tmp_path / 'untitled.bin'
    source.write_bytes(b'fixture')
    processor = AutoProcessor(Store(tmp_path / 'data'), {'extraction_mode': 'paper-preextract/1.0'})
    observed = []
    def progress(value):
        if value.get('intake_progress'):
            observed.append(value)
            processor.stop.set()
    result = processor.run([{'path': str(source)}], progress)
    assert observed
    assert processor.stop.is_set(), 'The intake stage erased the user stop request'
    assert result['status'] == 'stopped'
    assert result['processed_sources'] == 0
    assert result['inputs'] == [{'path': str(source)}]
    assert not result['items']


def test_stop_before_worker_entry_does_not_start_intake(tmp_path, monkeypatch):
    import associated_materials
    from auto_pipeline import AutoProcessor
    from engine import Store
    source = tmp_path / 'untitled.bin'
    source.write_bytes(b'fixture')
    calls = []
    monkeypatch.setattr(associated_materials, 'material_kind', lambda r: calls.append(r) or 'unknown')
    processor = AutoProcessor(Store(tmp_path / 'data'), {'extraction_mode': 'paper-preextract/1.0'})
    processor.stop.set()
    result = processor.run([{'path': str(source)}])
    assert not calls, 'Already stopped work still entered classification'
    assert result['status'] == 'stopped' and result['processed_sources'] == 0


@pytest.mark.parametrize('mode', ['headers', 'body', 'sse'])
def test_stop_interrupts_real_http_without_replaying_request(tmp_path, mode):
    from agent_client import AgentClient
    from preextract_request import request_once
    entered, release = threading.Event(), threading.Event()
    requests = []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_POST(self):
            self.rfile.read(int(self.headers['Content-Length']))
            requests.append(self.path)
            try:
                if mode != 'headers':
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/event-stream' if mode == 'sse' else 'application/json')
                    self.send_header('Content-Length', '100000')
                    self.end_headers()
                    if mode == 'sse':
                        self.wfile.write(b'data: {"choices":[{"delta":{"content":"saved partial"}}]}\n\n')
                    else:
                        self.wfile.write(b'{"choices":[')
                    self.wfile.flush()
                entered.set()
                release.wait(5)
            except (OSError, ConnectionError): pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    serving = threading.Thread(target=server.serve_forever, daemon=True)
    serving.start()
    stop = threading.Event()
    client = AgentClient({}, 'fixture', stop)
    body = {'model': 'fixture', 'messages': [], 'stream': True}
    connection = {'base_url': f'http://127.0.0.1:{server.server_port}/v1', 'api_key': 'fixture', 'timeout': 10}
    errors = []
    def run():
        try: request_once(body, connection, tmp_path / 'model', opener=client.opener, stop=stop)
        except BaseException as exc: errors.append(exc)
    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    try:
        assert entered.wait(2), 'Local request did not reach the stall point'
        if mode in {'sse', 'body'}:
            import time
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                p = tmp_path / ('model/response.txt' if mode == 'sse' else 'model/response-events.jsonl')
                expected = b'saved partial' if mode == 'sse' else b'{"choices":['
                if p.exists() and expected in p.read_bytes(): break
                time.sleep(.01)
            assert expected in p.read_bytes()
        stop.set()
        worker.join(1)
        assert not worker.is_alive(), f'Stop left a real HTTP request blocked on {mode}'
        assert errors and isinstance(errors[0], InterruptedError)
        state = json.loads((tmp_path / 'model/request-state.json').read_text(encoding='utf8'))
        assert state['status'] == 'request_failed_or_uncertain'
        if mode == 'sse':
            assert 'saved partial' in (tmp_path / 'model/response.txt').read_text(encoding='utf8')
        if mode == 'body':
            assert (tmp_path/'model/response-events.jsonl').read_bytes() == b'{"choices":['
        stop.clear()
        with pytest.raises(RuntimeError):
            request_once(body, connection, tmp_path / 'model', opener=client.opener, stop=stop)
        assert len(requests) == 1, 'Stopping must not create an automatic paid retry'
    finally:
        release.set()
        worker.join(3)
        server.shutdown()
        server.server_close()
        serving.join(1)


def test_stop_interrupts_owned_helper_and_preserves_unrelated_process(tmp_path):
    import subprocess
    import sys
    import time
    import psutil
    from processing_stop import run_process
    stop = threading.Event()
    ready = tmp_path / 'helper.pid'
    command = [sys.executable, '-c',
        'import os,time;from pathlib import Path;'
        f'Path({str(ready)!r}).write_text(str(os.getpid()));time.sleep(15)']
    other = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(15)'])
    errors = []
    def run():
        try: run_process(command, timeout=20, stop=stop, capture_output=True)
        except BaseException as exc: errors.append(exc)
    worker = threading.Thread(target=run)
    worker.start()
    try:
        deadline = time.monotonic() + 2
        while not ready.exists() and time.monotonic() < deadline: time.sleep(.01)
        pid = int(ready.read_text())
        stop.set()
        worker.join(2)
        assert not worker.is_alive()
        assert errors and isinstance(errors[0], InterruptedError)
        assert not psutil.pid_exists(pid)
        assert other.poll() is None, 'Unrelated processes must stay alive'
    finally:
        stop.set(); worker.join(3)
        other.kill(); other.wait(3)


def test_stop_interrupts_wps_lock_wait_without_starting_office(tmp_path, monkeypatch):
    import office_wps
    from filelock import FileLock
    from processing_stop import stop_scope
    lock_path = tmp_path / 'wps.lock'
    source = tmp_path / 'paper.doc'
    source.write_bytes(b'original')
    monkeypatch.setattr(office_wps, '_lock_path', lambda: lock_path)
    stop, entered = threading.Event(), threading.Event()
    errors = []
    def run():
        try:
            with stop_scope(stop):
                entered.set()
                office_wps.convert(source, tmp_path / 'out', timeout=10)
        except BaseException as exc: errors.append(exc)
    with FileLock(str(lock_path)):
        worker = threading.Thread(target=run)
        worker.start()
        assert entered.wait(1)
        stop.set(); worker.join(1)
    assert not worker.is_alive()
    assert errors and isinstance(errors[0], InterruptedError)
    assert source.read_bytes() == b'original'
    assert not (tmp_path / 'out/paper.docx').exists()


def test_native_stop_button_finishes_before_window_can_close(tmp_path, monkeypatch):
    import time
    import tkinter as tk
    import app as app_module
    from auto_pipeline import AutoProcessor
    root = tk.Tk(); root.withdraw()
    gui = app_module.Workbench(root, tmp_path / 'data', recover=False)
    destroyed = threading.Event()
    original_destroy = root.destroy
    monkeypatch.setattr(root, 'destroy', destroyed.set)
    processor = AutoProcessor(gui.store, {'extraction_mode': 'paper-preextract/1.0'})
    gui.processor = processor
    source = tmp_path / 'untitled.bin'; source.write_bytes(b'fixture')
    entered = threading.Event()
    notices = []
    monkeypatch.setattr(app_module.messagebox, 'showinfo', lambda *a, **kw: notices.append(a))
    monkeypatch.setattr(app_module.messagebox, 'showerror', lambda *a, **kw: pytest.fail(str(a)))
    def progress(value):
        if value.get('intake_progress') and not entered.is_set():
            entered.set(); processor.stop.wait(3)
        gui.events.put(('progress', value))
    gui.async_task(lambda: processor.run([{'path': str(source)}], progress), gui.finished, kind='processing')
    try:
        deadline = time.monotonic() + 2
        while not entered.is_set() and time.monotonic() < deadline: root.update(); time.sleep(.01)
        assert entered.is_set()
        gui.close()
        assert gui.busy and not destroyed.is_set() and notices
        gui.stop_button.invoke()
        assert processor.stop.is_set()
        deadline = time.monotonic() + 2
        while gui.busy and time.monotonic() < deadline: root.update(); time.sleep(.01)
        assert not gui.busy
        assert gui.current['status'] == 'stopped' and not gui.current['items']
        for callback in root.tk.call('after', 'info'): root.after_cancel(callback)
        gui.close()
        assert destroyed.is_set()
    finally:
        processor.stop.set()
        try: original_destroy()
        except tk.TclError: pass


def test_stop_interrupts_tls_handshake(tmp_path):
    import socketserver
    from urllib.request import Request
    from agent_client import AgentClient
    entered, release, stop = threading.Event(), threading.Event(), threading.Event()
    class Handler(socketserver.BaseRequestHandler):
        def handle(self): entered.set(); release.wait(5)
    server = socketserver.ThreadingTCPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    serving = threading.Thread(target=server.serve_forever, daemon=True); serving.start()
    errors = []
    def run():
        try:
            with AgentClient({}, 'fixture', stop).opener.open(
                    Request(f'https://127.0.0.1:{server.server_address[1]}/'), timeout=10): pass
        except BaseException as exc: errors.append(exc)
    worker = threading.Thread(target=run, daemon=True); worker.start()
    try:
        assert entered.wait(2)
        stop.set(); worker.join(1)
        assert not worker.is_alive(), 'TLS handshake ignored the stop signal'
        assert errors and isinstance(errors[0], InterruptedError)
        tb = errors[0].__traceback__
        while tb:
            scope = tb.tb_frame.f_locals.get('scope')
            if hasattr(scope, 'sockets'):
                assert all(sock.fileno() == -1 for sock in scope.sockets), 'Cancelled TLS socket leaked'
            tb = tb.tb_next
    finally:
        release.set(); worker.join(3)
        server.shutdown(); server.server_close(); serving.join(1)


@pytest.mark.parametrize('scheme', ['http', 'https'])
@pytest.mark.parametrize('mode', ['json', 'sse', 'chunked'])
def test_normal_slow_responses_keep_all_content(tmp_path, monkeypatch, scheme, mode):
    import datetime
    import ipaddress
    import ssl
    import time
    from agent_client import AgentClient
    from preextract_request import request_once
    text = '完整正文、公式、图表说明与学习步骤。' * 1600
    if mode == 'sse':
        payload = b''.join(b'data: ' + json.dumps({'choices': [{'delta': {'content': part},
            'finish_reason': 'stop' if i == 2 else None}]}, ensure_ascii=False).encode() + b'\n\n'
            for i, part in enumerate((text[:10000], text[10000:20000], text[20000:]))) + b'data: [DONE]\n\n'
    else:
        payload = json.dumps({'choices': [{'message': {'content': text}, 'finish_reason': 'stop'}]}, ensure_ascii=False).encode()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_POST(self):
            self.rfile.read(int(self.headers['Content-Length']))
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream' if mode == 'sse' else 'application/json')
            self.send_header('Transfer-Encoding' if mode == 'chunked' else 'Content-Length',
                             'chunked' if mode == 'chunked' else str(len(payload)))
            self.end_headers()
            for n in range(0, len(payload), 20000):
                part = payload[n:n+20000]
                self.wfile.write((f'{len(part):x}\r\n'.encode()+part+b'\r\n') if mode == 'chunked' else part)
                self.wfile.flush(); time.sleep(.12)
            if mode == 'chunked': self.wfile.write(b'0\r\n\r\n'); self.wfile.flush()
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    if scheme == 'https':
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, 'localhost')])
        now = datetime.datetime.now(datetime.timezone.utc)
        cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(1).not_valid_before(now-datetime.timedelta(days=1)).not_valid_after(now+datetime.timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]), False)
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), True).sign(key, hashes.SHA256()))
        pem = tmp_path/'cert.pem'; pem.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        keyfile = tmp_path/'key.pem'; keyfile.write_bytes(key.private_bytes(serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); context.load_cert_chain(pem, keyfile)
        server.socket = context.wrap_socket(server.socket, server_side=True)
        monkeypatch.setattr(ssl, '_create_default_https_context', lambda **kwargs: ssl.create_default_context(cafile=str(pem)))
    serving = threading.Thread(target=server.serve_forever, daemon=True); serving.start()
    stop = threading.Event()
    connection = dict(base_url=f'{scheme}://127.0.0.1:{server.server_port}/v1', api_key='fixture', timeout=3)
    try:
        answer = request_once({'model': 'fixture', 'messages': [], 'stream': True}, connection,
            tmp_path/'model', opener=AgentClient({}, 'fixture', stop).opener, stop=stop)
        assert answer == text, 'Cancellation support changed or truncated normal model output'
        state = json.loads((tmp_path/'model/request-state.json').read_text(encoding='utf8'))
        assert state['status'] == 'response_complete'
    finally:
        server.shutdown(); server.server_close(); serving.join(1)


def test_twenty_shared_opener_requests_all_stop_and_save(tmp_path):
    from agent_client import AgentClient
    from preextract_request import request_once
    stop, all_entered, release = threading.Event(), threading.Event(), threading.Event()
    lock = threading.Lock(); requests = []; errors = []
    class Server(ThreadingHTTPServer): request_queue_size = 64
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_POST(self):
            self.rfile.read(int(self.headers['Content-Length']))
            with lock:
                requests.append(self.path)
                if len(requests) == 20: all_entered.set()
            release.wait(5)
    server = Server(('127.0.0.1', 0), Handler); server.daemon_threads = True
    serving = threading.Thread(target=server.serve_forever, daemon=True); serving.start()
    client = AgentClient({}, 'fixture', stop)
    connection = dict(base_url=f'http://127.0.0.1:{server.server_port}/v1', api_key='fixture', timeout=10)
    def run(index):
        try:
            request_once({'model':'fixture','messages':[],'stream':True}, connection,
                tmp_path/str(index), opener=client.opener, stop=stop)
        except BaseException as exc:
            with lock: errors.append(exc)
    workers = [threading.Thread(target=run, args=(i,), daemon=True) for i in range(20)]
    for worker in workers: worker.start()
    try:
        assert all_entered.wait(3), f'Only {len(requests)}/20 local requests reached the server'
        stop.set()
        for worker in workers: worker.join(1)
        assert not any(worker.is_alive() for worker in workers)
        assert len(errors) == 20 and all(isinstance(exc, InterruptedError) for exc in errors)
        for i in range(20):
            state = json.loads((tmp_path/str(i)/'request-state.json').read_text(encoding='utf8'))
            assert state['status'] == 'request_failed_or_uncertain'
        assert len(requests) == 20
    finally:
        stop.set(); release.set()
        for worker in workers: worker.join(2)
        server.shutdown(); server.server_close(); serving.join(1)


def test_stop_interrupts_pending_tcp_connect(monkeypatch):
    import errno
    import socket
    import time
    import stop_http
    stop, entered = threading.Event(), threading.Event()
    errors = []
    def pending(self, target): entered.set(); return errno.EINPROGRESS
    def not_ready(*args): time.sleep(.01); return [], [], []
    monkeypatch.setattr(socket.socket, 'connect_ex', pending)
    monkeypatch.setattr(stop_http.select, 'select', not_ready)
    def run():
        scope = stop_http._Scope(stop)
        try: stop_http._connect(scope, ('127.0.0.1', 9), timeout=1800)
        except BaseException as exc: errors.append(exc)
        finally:
            scope.finish()
            assert all(sock.fileno() == -1 for sock in scope.sockets)
    worker = threading.Thread(target=run, daemon=True); worker.start()
    assert entered.wait(1)
    stop.set(); worker.join(1)
    assert not worker.is_alive()
    assert errors and isinstance(errors[0], InterruptedError)
