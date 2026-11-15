"""Use the actual legacy request journal with an in-memory fake model."""
from contextlib import contextmanager
import importlib.util
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sqlite3
import sys
import threading
from uuid import uuid4

from filelock import FileLock
import pytest


DESKTOP = Path('F:/论文预处理系统/程序/updates/unified-prefect-20261029-material-r02/desktop')
sys.path[:0] = [str(DESKTOP), 'F:/论文预处理系统/程序/desktop', str(DESKTOP.parent / 'core')]
from preextract_request import request_once, IncompleteModelResponse

spec = importlib.util.spec_from_file_location('raw_request', Path(__file__).parent.parent / 'raw_request.py')
raw_request = importlib.util.module_from_spec(spec)
spec.loader.exec_module(raw_request)


class FakeResponse:
    headers = {'Content-Type': 'application/json'}
    def __init__(self, text, finish):
        self.data = json.dumps({'choices': [{'message': {'content': text}, 'finish_reason': finish}]}).encode()
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def read(self): return self.data


class FakeOpener:
    def __init__(self, finish='stop'):
        self.calls = 0
        self.finish = finish
    def open(self, *_args, **_kwargs):
        self.calls += 1
        return FakeResponse('原始回复，不要求它是 JSON', self.finish)


class FakeAdmission:
    def start_transport(self, mark_started):
        mark_started()


@pytest.fixture
def arguments(tmp_path):
    observations = []
    @contextmanager
    def lease(kind, stop):
        observations.append(('lease', kind))
        yield
    @contextmanager
    def admit(transport):
        observations.append(('admit', transport['http_invoked']))
        yield FakeAdmission()
    def check_history(): observations.append(('history',))
    def check_source(): observations.append(('source',))
    return dict(kind='body', request_body_factory=lambda: {'model': 'fake', 'messages': []},
                connection={'base_url': 'http://127.0.0.1/fake', 'api_key': 'not-a-real-key'},
                work=tmp_path / 'work', stop=threading.Event(),
                request_once=request_once, lease=lease, admit=admit,
                check_history=check_history, check_source=check_source), observations


@pytest.mark.parametrize('kind', ['body', 'attachment'])
def test_saves_raw_answer_without_parsing_and_does_not_resend(arguments, kind):
    args, observed = arguments
    args['kind'] = kind
    opener = FakeOpener()
    result = raw_request.run_raw_request(**args, opener=opener)
    assert result['status'] == 'raw_response_saved'
    assert Path(result['response_path']).read_text(encoding='utf8') == '原始回复，不要求它是 JSON'
    assert ('lease', kind) in observed and ('admit', False) in observed
    assert opener.calls == 1
    assert not (args['work'] / 'description.json').exists()
    assert not (args['work'] / 'private' / 'candidate.json').exists()
    assert not list(args['work'].rglob('*.md'))
    again = raw_request.run_raw_request(**args, opener=opener)
    assert again == result and opener.calls == 1


def test_pause_prevents_new_send(arguments):
    args, _ = arguments
    args['stop'].set()
    opener = FakeOpener()
    with pytest.raises(InterruptedError):
        raw_request.run_raw_request(**args, opener=opener)
    assert opener.calls == 0
    assert not (args['work'] / 'model' / 'request-state.json').exists()


def test_incomplete_response_is_preserved_and_never_resent(arguments):
    args, _ = arguments
    opener = FakeOpener(finish='length')
    with pytest.raises(IncompleteModelResponse):
        raw_request.run_raw_request(**args, opener=opener)
    assert opener.calls == 1
    assert (args['work'] / 'model' / 'response.txt').exists()
    with pytest.raises(RuntimeError):
        raw_request.run_raw_request(**args, opener=opener)
    assert opener.calls == 1


def test_existing_partition_evidence_never_starts_a_new_whole_body_request(arguments):
    args, _ = arguments
    model = args['work'] / 'model'
    model.mkdir(parents=True)
    (model / 'part-001').mkdir()
    opener = FakeOpener()
    with pytest.raises(ValueError, match='分段'):
        raw_request.run_raw_request(**args, opener=opener)
    assert opener.calls == 0
    assert not (model / 'request-state.json').exists()


@pytest.mark.parametrize('corruption', ['events', 'stop'])
def test_saved_response_with_conflicting_evidence_is_not_accepted(arguments, corruption):
    args, _ = arguments
    opener = FakeOpener()
    raw_request.run_raw_request(**args, opener=opener)
    model = args['work'] / 'model'
    if corruption == 'events':
        (model / 'response-events.jsonl').write_text('damaged', encoding='utf8')
    else:
        state_path = model / 'request-state.json'
        state = json.loads(state_path.read_text(encoding='utf8'))
        state['error_kind'] = 'user_stop'
        state_path.write_text(json.dumps(state), encoding='utf8')
    with pytest.raises(ValueError, match='证据'):
        raw_request.run_raw_request(**args, opener=opener)
    assert opener.calls == 1


@pytest.mark.parametrize('scenario', ['normal', 'crashed_after_response', 'damaged_events'])
def test_pause_and_request_boundary_share_admission_record(arguments, tmp_path, scenario):
    args, _ = arguments
    admission_spec = importlib.util.spec_from_file_location(
        'pause_admission', Path(__file__).parent.parent / 'pause_admission.py')
    admission = importlib.util.module_from_spec(admission_spec)
    admission_spec.loader.exec_module(admission)
    db_path, lock_root = tmp_path / 'control.sqlite', tmp_path / 'control-locks'
    lock_root.mkdir()
    task_id = str(uuid4())
    with sqlite3.connect(db_path) as db:
        db.executescript('''
            CREATE TABLE tasks(task_id TEXT PRIMARY KEY, desired_state TEXT,
                               start_generation INTEGER, start_pending INTEGER, task_status TEXT);
            CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY);
        ''')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?)', (task_id, 'paused', 1, 0, 'complete'))
    guard = admission.AdmissionGuard(db_path, lock_root)
    guard.initialize()
    args['stop'] = admission.ControlStop(db_path, task_id, 1)
    request_hash = raw_request._body_hash(args['request_body_factory']())
    task_run_id = str(uuid4())
    args['admit'] = lambda transport: guard.admit(
        task_id, 1, 'a' * 64, request_hash, kind='body', model_dir=transport['work_dir'],
        receipt_id='f' * 64, task_run_id=task_run_id, source_sha256='e' * 64)
    opener = FakeOpener()
    with pytest.raises(InterruptedError):
        raw_request.run_raw_request(**args, opener=opener)
    assert opener.calls == 0
    assert not (args['work'] / 'model' / 'request-state.json').exists()
    with sqlite3.connect(db_path) as db:
        db.execute("UPDATE tasks SET desired_state='running' WHERE task_id=?", (task_id,))
    result = raw_request.run_raw_request(**args, opener=opener)
    assert opener.calls == 1
    with sqlite3.connect(db_path) as db:
        assert guard.counts(db, task_id)['inflight'] == 1
        if scenario == 'crashed_after_response':
            db.execute("UPDATE llm_request_admissions SET state='admitted' WHERE task_id=?", (task_id,))
    if scenario == 'damaged_events':
        (args['work'] / 'model' / 'response-events.jsonl').write_text('damaged', encoding='utf8')
        with pytest.raises(ValueError, match='证据'):
            guard.complete(task_id, 'a' * 64, result['response_sha256'])
        with sqlite3.connect(db_path) as db:
            assert guard.counts(db, task_id)['inflight'] == 1
        return
    guard.complete(task_id, 'a' * 64, result['response_sha256'])
    with sqlite3.connect(db_path) as db:
        assert guard.counts(db, task_id) == {'inflight': 0, 'unknown': 0}


def test_pause_after_admission_but_before_transport_blocks_open(arguments, tmp_path):
    args, _ = arguments
    admission_spec = importlib.util.spec_from_file_location(
        'pause_admission', Path(__file__).parent.parent / 'pause_admission.py')
    admission = importlib.util.module_from_spec(admission_spec)
    admission_spec.loader.exec_module(admission)
    db_path, lock_root = tmp_path / 'control.sqlite', tmp_path / 'control-locks'
    lock_root.mkdir()
    task_id = str(uuid4())
    with sqlite3.connect(db_path) as db:
        db.executescript('''
            CREATE TABLE tasks(task_id TEXT PRIMARY KEY, desired_state TEXT,
                               start_generation INTEGER, start_pending INTEGER, task_status TEXT);
            CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY);
        ''')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?)', (task_id, 'running', 1, 0, 'complete'))
    guard = admission.AdmissionGuard(db_path, lock_root)
    guard.initialize()
    args['stop'] = admission.ControlStop(db_path, task_id, 1)
    args['admit'] = lambda transport: guard.admit(
        task_id, 1, 'a' * 64, transport['request_sha256'], kind='body', model_dir=transport['work_dir'],
        receipt_id='f' * 64, task_run_id=str(uuid4()), source_sha256='e' * 64)
    entered, release = threading.Event(), threading.Event()
    checks = []
    def check_source():
        checks.append(1)
        if len(checks) == 3:
            entered.set()
            assert release.wait(5)
    args['check_source'] = check_source
    opener = FakeOpener()
    errors = []
    def run():
        try:
            raw_request.run_raw_request(**args, opener=opener)
        except BaseException as exc:
            errors.append(exc)
    thread = threading.Thread(target=run)
    thread.start()
    assert entered.wait(5)
    with FileLock(str(lock_root / ('preprocessing-' + task_id + '.lock')), timeout=1):
        with sqlite3.connect(db_path) as db:
            db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (task_id,))
    release.set()
    thread.join(5)
    assert not thread.is_alive()
    assert len(errors) == 1 and isinstance(errors[0], InterruptedError)
    assert opener.calls == 0
    assert not (args['work'] / 'model' / 'request-state.json').exists()


@pytest.mark.parametrize('kind', ['body', 'attachment'])
def test_pause_drains_started_llm_without_new_requests_or_disconnect(arguments, tmp_path, kind):
    args, _ = arguments
    args['kind'] = kind
    admission_spec = importlib.util.spec_from_file_location(
        'pause_admission_drain', Path(__file__).parent.parent / 'pause_admission.py')
    admission = importlib.util.module_from_spec(admission_spec)
    admission_spec.loader.exec_module(admission)
    db_path, lock_root = tmp_path / 'control.sqlite', tmp_path / 'control-locks'
    lock_root.mkdir()
    task_id, task_run = str(uuid4()), str(uuid4())
    with sqlite3.connect(db_path) as db:
        db.executescript('''
            CREATE TABLE tasks(task_id TEXT PRIMARY KEY, desired_state TEXT,
                               start_generation INTEGER, start_pending INTEGER, task_status TEXT);
            CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY);
        ''')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?)', (task_id, 'running', 1, 0, 'complete'))
    guard = admission.AdmissionGuard(db_path, lock_root)
    guard.initialize()
    args['stop'] = admission.ControlStop(db_path, task_id, 1)
    args['admit'] = lambda transport: guard.admit(
        task_id, 1, 'a' * 64, transport['request_sha256'], kind=kind, model_dir=transport['work_dir'],
        receipt_id='f' * 64, task_run_id=task_run, source_sha256='e' * 64)
    entered, release = threading.Event(), threading.Event()
    def event(text, finish):
        return ('data: ' + json.dumps({'choices': [{'delta': {'content': text}, 'finish_reason': finish}]},
                                      ensure_ascii=False) + '\n').encode('utf8')
    class Stream:
        headers = {'Content-Type': 'text/event-stream'}
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def __iter__(self):
            yield event('前半', None)
            entered.set()
            assert release.wait(5)
            yield event('后半', 'stop')
            yield b'data: [DONE]\n'
    class Opener:
        calls = 0
        def open(self, *_args, **_kwargs):
            self.calls += 1
            return Stream()
    opener, results, errors = Opener(), [], []
    def run():
        try:
            results.append(raw_request.run_raw_request(**args, opener=opener))
        except BaseException as exc:
            errors.append(exc)
    thread = threading.Thread(target=run)
    thread.start()
    assert entered.wait(5)
    with FileLock(str(lock_root / ('preprocessing-' + task_id + '.lock')), timeout=1):
        with sqlite3.connect(db_path) as db:
            db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (task_id,))
            assert guard.counts(db, task_id)['inflight'] == 1
    with pytest.raises(InterruptedError):
        with guard.admit(task_id, 1, 'b' * 64, 'c' * 64, kind='body',
                         model_dir=tmp_path / 'new/model', receipt_id='f' * 64,
                         task_run_id=task_run, source_sha256='e' * 64):
            pass
    release.set()
    thread.join(5)
    assert not thread.is_alive() and not errors and len(results) == 1 and opener.calls == 1
    assert Path(results[0]['response_path']).read_text(encoding='utf8') == '前半后半'
    guard.complete(task_id, 'a' * 64, results[0]['response_sha256'])
    with sqlite3.connect(db_path) as db:
        assert guard.counts(db, task_id) == {'inflight': 0, 'unknown': 0}


def test_pause_drains_body_and_attachment_concurrently(tmp_path):
    admission_spec = importlib.util.spec_from_file_location(
        'pause_admission_multi', Path(__file__).parent.parent / 'pause_admission.py')
    admission = importlib.util.module_from_spec(admission_spec)
    admission_spec.loader.exec_module(admission)
    db_path, lock_root = tmp_path / 'control.sqlite', tmp_path / 'locks'
    lock_root.mkdir()
    task_id, ticket = str(uuid4()), str(uuid4())
    with sqlite3.connect(db_path) as db:
        db.executescript('''
            CREATE TABLE tasks(task_id TEXT PRIMARY KEY, desired_state TEXT,
                               start_generation INTEGER, start_pending INTEGER, task_status TEXT);
            CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY);
        ''')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?)', (task_id, 'running', 2, 0, 'complete'))
    guard = admission.AdmissionGuard(db_path, lock_root)
    guard.initialize()
    stop = admission.ControlStop(db_path, task_id, 2)
    entered = {kind: threading.Event() for kind in ('body', 'attachment')}
    release = threading.Event()
    results, errors = {}, []
    @contextmanager
    def lease(*_): yield
    def run(kind, component):
        class Stream:
            headers = {'Content-Type': 'text/event-stream'}
            def __enter__(self): return self
            def __exit__(self, *_): return False
            def __iter__(self):
                chunk = json.dumps({'choices': [{'delta': {'content': kind}, 'finish_reason': 'stop'}]})
                entered[kind].set()
                assert release.wait(5)
                yield ('data: ' + chunk + '\n').encode()
                yield b'data: [DONE]\n'
        class Opener:
            def open(self, *_args, **_kwargs): return Stream()
        try:
            results[kind] = raw_request.run_raw_request(
                kind=kind, request_body_factory=lambda: {'model': 'fake', 'messages': []},
                connection={'base_url': 'http://127.0.0.1/fake', 'api_key': 'not-a-real-key'},
                work=tmp_path / kind, stop=stop, request_once=request_once, lease=lease,
                admit=lambda transport: guard.admit(task_id, 2, component, transport['request_sha256'],
                    kind=kind, model_dir=transport['work_dir'], receipt_id='e' * 64,
                    task_run_id=ticket, source_sha256='f' * 64),
                check_history=lambda: None, check_source=lambda: None, opener=Opener())
        except BaseException as exc:
            errors.append(exc)
    threads = [threading.Thread(target=run, args=('body', 'a' * 64)),
               threading.Thread(target=run, args=('attachment', 'b' * 64))]
    for thread in threads: thread.start()
    assert all(event.wait(5) for event in entered.values())
    with FileLock(str(lock_root / ('preprocessing-' + task_id + '.lock')), timeout=1):
        with sqlite3.connect(db_path) as db:
            db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (task_id,))
            assert guard.counts(db, task_id)['inflight'] == 2
    release.set()
    for thread in threads: thread.join(5)
    assert not errors and all(not thread.is_alive() for thread in threads)
    assert set(results) == {'body', 'attachment'}
    for kind, component in [('body', 'a' * 64), ('attachment', 'b' * 64)]:
        assert Path(results[kind]['response_path']).read_text(encoding='utf8') == kind
        guard.complete(task_id, component, results[kind]['response_sha256'])
    with sqlite3.connect(db_path) as db:
        assert guard.counts(db, task_id) == {'inflight': 0, 'unknown': 0}


def test_pause_does_not_abort_local_streaming_transport(arguments, tmp_path):
    """Exercise the real cancellable socket against an in-process fake server."""
    args, _ = arguments
    admission_spec = importlib.util.spec_from_file_location(
        'pause_admission_socket', Path(__file__).parent.parent / 'pause_admission.py')
    admission = importlib.util.module_from_spec(admission_spec)
    admission_spec.loader.exec_module(admission)
    database, lock_root = tmp_path / 'control.sqlite', tmp_path / 'socket-locks'
    lock_root.mkdir()
    task_id, ticket = str(uuid4()), str(uuid4())
    with sqlite3.connect(database) as db:
        db.executescript('''
            CREATE TABLE tasks(task_id TEXT PRIMARY KEY, desired_state TEXT,
                               start_generation INTEGER, start_pending INTEGER, task_status TEXT);
            CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY);
        ''')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?)', (task_id, 'running', 1, 0, 'complete'))
    guard = admission.AdmissionGuard(database, lock_root)
    guard.initialize()
    args['stop'] = admission.ControlStop(database, task_id, 1)
    args['admit'] = lambda transport: guard.admit(
        task_id, 1, 'a' * 64, transport['request_sha256'], kind='body', model_dir=transport['work_dir'],
        receipt_id='e' * 64, task_run_id=ticket, source_sha256='f' * 64)
    entered, release = threading.Event(), threading.Event()
    errors, results = [], []
    def event(text, finish):
        return ('data: ' + json.dumps({'choices': [{'delta': {'content': text}, 'finish_reason': finish}]}) + '\n').encode()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_): pass
        def do_POST(self):
            self.rfile.read(int(self.headers['Content-Length']))
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.end_headers()
            self.wfile.write(event('first', None))
            self.wfile.flush()
            entered.set()
            if release.wait(5):
                try:
                    self.wfile.write(event('second', 'stop') + b'data: [DONE]\n')
                    self.wfile.flush()
                except OSError as exc:
                    errors.append(exc)
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    args['connection'] = {'base_url': f'http://127.0.0.1:{server.server_port}',
                          'api_key': 'synthetic-only', 'timeout': 5}
    def run():
        try:
            results.append(raw_request.run_raw_request(**args))
        except BaseException as exc:
            errors.append(exc)
    worker = threading.Thread(target=run)
    try:
        worker.start()
        assert entered.wait(5)
        with FileLock(str(lock_root / ('preprocessing-' + task_id + '.lock')), timeout=1):
            with sqlite3.connect(database) as db:
                db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (task_id,))
                assert guard.counts(db, task_id)['inflight'] == 1
        threading.Event().wait(.25)  # let the real socket stop watcher observe the paused state
        release.set()
        worker.join(5)
        assert not worker.is_alive() and not errors and len(results) == 1
        assert Path(results[0]['response_path']).read_text(encoding='utf8') == 'firstsecond'
        guard.complete(task_id, 'a' * 64, results[0]['response_sha256'])
    finally:
        release.set()
        worker.join(5)
        server.shutdown()
        server.server_close()
        server_thread.join(5)
