import importlib
import json
import threading
import subprocess
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest


def binding_module():
    try:
        return importlib.import_module('paperflow_prefect.nimble_dependency')
    except ModuleNotFoundError as exc:
        pytest.fail(f'Nimble dependency binding is unavailable: {exc}', pytrace=False)


def test_service_probe_requires_registered_model_without_sending_inference():
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            calls.append(self.path)
            body = ({'version': 'test-version'} if self.path == '/api/version' else
                    {'models': [{'name': 'nimble:9b-q4_K_M'}]})
            raw = json.dumps(body).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        module = binding_module()
        module.probe_service('127.0.0.1', server.server_port, 'nimble:9b-q4_K_M')
        assert calls == ['/api/version', '/api/tags']
        with pytest.raises(module.NimbleUnavailable):
            module.probe_service('127.0.0.1', server.server_port, 'missing:model')
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)


def test_binding_rejects_wrong_port_before_starting_service(tmp_path, monkeypatch):
    module = binding_module()
    invoked = []
    monkeypatch.setattr(module, '_run_start_script', lambda *_: invoked.append(True))
    with pytest.raises(module.NimbleUnavailable):
        module.ensure_nimble('http://127.0.0.1:11434/v1/systemone', tmp_path, tmp_path / 'start.ps1')
    assert invoked == []


def test_starting_service_preserves_previous_logs(tmp_path, monkeypatch):
    module = binding_module()
    runtime = tmp_path / 'runtime'
    runtime.mkdir()
    (runtime / 'nimble.stdout.log').write_bytes(b'old stdout')
    (runtime / 'nimble.stderr.log').write_bytes(b'old stderr')
    (runtime / 'nimble-process.json').write_bytes(b'old process record')
    script = tmp_path / 'start-nimble.ps1'
    script.write_text('synthetic', encoding='utf8')
    listeners = iter([None, 123])
    monkeypatch.setattr(module, '_listener_pid', lambda _: next(listeners))
    monkeypatch.setattr(module, '_verify_listener', lambda *_: None)

    def start(_):
        (runtime / 'nimble.stdout.log').write_bytes(b'new stdout')
        (runtime / 'nimble.stderr.log').write_bytes(b'new stderr')
        (runtime / 'nimble-process.json').write_bytes(b'new process record')

    monkeypatch.setattr(module, '_run_start_script', start)
    monkeypatch.setattr(module, 'probe_service', lambda *_: None)
    module.ensure_nimble('http://127.0.0.1:11435/v1/systemone', tmp_path, script)
    archives = list((runtime / 'nimble-log-backups').iterdir())
    assert len(archives) == 1
    assert (archives[0] / 'nimble.stdout.log').read_bytes() == b'old stdout'
    assert (archives[0] / 'nimble.stderr.log').read_bytes() == b'old stderr'
    assert (archives[0] / 'nimble-process.json').read_bytes() == b'old process record'


def test_verify_listener_accepts_windows_powershell_utf8_record(tmp_path, monkeypatch):
    module = binding_module()
    runtime = tmp_path / 'runtime'
    runtime.mkdir()
    started = datetime.now(timezone.utc)
    (runtime / 'nimble-process.json').write_text(
        json.dumps({'pid': 123, 'endpoint': 'http://127.0.0.1:11435', 'started_at': started.isoformat()}),
        encoding='utf-8-sig')

    class Process:
        def exe(self):
            return str(module.OLLAMA_EXE)
        def create_time(self):
            return started.timestamp()

    monkeypatch.setattr(module.psutil, 'Process', lambda _: Process())
    module._verify_listener(123, tmp_path)


def test_cancelled_task_does_not_start_the_service(tmp_path, monkeypatch):
    module = binding_module()
    (tmp_path / 'runtime').mkdir()
    script = tmp_path / 'start-nimble.ps1'
    script.write_text('synthetic', encoding='utf8')
    started = []
    monkeypatch.setattr(module, '_listener_pid', lambda _: None)
    monkeypatch.setattr(module, '_run_start_script', lambda *_: started.append(True))
    module.ensure_nimble('http://127.0.0.1:11435/v1/systemone', tmp_path, script,
                         is_needed=lambda: False)
    assert started == []


def test_launcher_uses_no_pipe_inherited_by_background_service(tmp_path, monkeypatch):
    module = binding_module()
    seen = []
    monkeypatch.setattr(module.subprocess, 'run', lambda *args, **kwargs: seen.append(kwargs))
    module._run_start_script(tmp_path / 'start-nimble.ps1')
    assert seen[0]['stdout'] == subprocess.DEVNULL
    assert seen[0]['stderr'] == subprocess.DEVNULL


def test_old_cached_http_error_becomes_a_fenced_unknown(tmp_path):
    import hashlib
    from urllib.request import Request
    from test_control_admission import ready_control
    from paperflow_prefect.controlled_nimble import ControlledNimbleJudge
    from paperflow_prefect.first_step_control import UnknownRequest
    control, scheduler, task_id = ready_control(tmp_path, groups=1)
    generation = scheduler.calls[-1][1]
    seq, _ = control.claim_next(task_id, generation)
    data = b'{}'
    response = json.dumps({'status': 503, 'body': 'e30='})
    with control.summary._connect() as db, db:
        db.execute("INSERT INTO model_decisions(task_id,seq,step,snapshot_id,payload_hash,state,response) "
                   "VALUES (?,?,?,?,?,'complete',?)",
                   (task_id, seq, 'body', 'snapshot', hashlib.sha256(data).hexdigest(), response))
    judge = ControlledNimbleJudge(control, task_id, seq, 'snapshot', generation)
    judge._step = 'body'
    request = Request('http://127.0.0.1:11435/v1/systemone', data=data)
    with pytest.raises(UnknownRequest):
        judge._cached_response(request)
    with control.summary._connect() as db:
        row = db.execute('SELECT state,response FROM model_decisions WHERE task_id=?', (task_id,)).fetchone()
    assert row['state'] == 'unknown' and json.loads(row['response'])['status'] == 503


def test_invalid_cached_response_is_fenced_before_other_processes_enter(tmp_path, monkeypatch):
    import base64
    import hashlib
    from filelock import FileLock
    from urllib.request import Request
    from test_control_admission import ready_control
    from paperflow_prefect.controlled_nimble import ControlledNimbleJudge
    from paperflow_prefect.first_step_control import UnknownRequest
    from paperflow_prefect.nimble_group import NimbleJudge
    control, scheduler, task_id = ready_control(tmp_path, groups=1)
    generation = scheduler.calls[-1][1]
    seq, _ = control.claim_next(task_id, generation)
    data = json.dumps({'questions': {'decision': {'criteria': {'a': 'one'}}}}).encode()
    response = json.dumps({'status': 200, 'body': base64.b64encode(b'not json').decode()})
    with control.summary._connect() as db, db:
        db.execute("INSERT INTO model_decisions(task_id,seq,step,snapshot_id,payload_hash,state,response) "
                   "VALUES (?,?,?,?,?,'complete',?)",
                   (task_id, seq, 'body', 'snapshot', hashlib.sha256(data).hexdigest(), response))
    judge = ControlledNimbleJudge(control, task_id, seq, 'snapshot', generation)
    judge._step = 'body'
    request = Request('http://127.0.0.1:11435/v1/systemone', data=data)
    entered, release, contender_entered = threading.Event(), threading.Event(), threading.Event()
    observed = []
    original_parse = NimbleJudge._parse_response

    def slow_parse(self, *args):
        entered.set()
        assert release.wait(5)
        return original_parse(self, *args)

    monkeypatch.setattr(NimbleJudge, '_parse_response', slow_parse)

    def cached():
        with pytest.raises(UnknownRequest):
            judge._cached_response(request)

    def contender():
        with FileLock(str(control.data / 'control-locks' / 'nimble-requests.lock')):
            with control.summary._connect() as db:
                observed.append(db.execute('SELECT state FROM model_decisions WHERE task_id=?', (task_id,)).fetchone()[0])
            contender_entered.set()

    first = threading.Thread(target=cached)
    first.start()
    try:
        assert entered.wait(5)
        second = threading.Thread(target=contender)
        second.start()
        assert not contender_entered.wait(.2)
        release.set()
        first.join(5)
        second.join(5)
        assert not first.is_alive() and not second.is_alive()
        assert observed == ['unknown']
    finally:
        release.set()
        first.join(5)
