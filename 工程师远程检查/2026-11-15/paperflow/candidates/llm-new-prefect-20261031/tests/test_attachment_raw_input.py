"""Existing attachment evidence and request construction with fake HTTP."""
from contextlib import contextmanager
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
import threading
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parents[1]
DESKTOP = Path('F:/论文预处理系统/程序/updates/unified-prefect-20261029-material-r02/desktop')
sys.path[:0] = [str(HERE), str(ROOT / 'prefect-paperflow'), str(DESKTOP),
                'F:/论文预处理系统/程序/desktop', str(DESKTOP.parent / 'core')]
from paperflow_prefect import attachments, attachment_input
from preextract_request import request_once


def load(name):
    spec = importlib.util.spec_from_file_location(name, HERE / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = load('attachment_raw_input')
raw = load('raw_request')


class Response:
    headers = {'Content-Type': 'application/json'}
    def __enter__(self): return self
    def __exit__(self, *_): return False
    def read(self):
        return json.dumps({'choices': [{'message': {'content': '原始附件答复'},
                                        'finish_reason': 'stop'}]}).encode()


class Opener:
    calls = 0
    def open(self, *_args, **_kwargs):
        self.calls += 1
        return Response()


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    source = tmp_path / 'attachment.txt'
    source.write_text('synthetic attachment', encoding='utf8')
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    row = {'file_id': 'attachment-1', 'path': str(source), 'relative': source.name,
           'name': source.name, 'sha256': digest}
    evidence = {'format': '.txt', 'name': source.name, 'source_sha256': digest,
                'text': 'synthetic evidence', 'images': [], 'refs': ['text-1'], 'limitations': []}
    monkeypatch.setattr(attachments, 'evidence_for', lambda _: dict(evidence))
    monkeypatch.setattr(attachment_input, 'require_upstream_history_located', lambda _: None)
    connection = {'base_url': 'http://127.0.0.1/fake', 'api_key': 'not-a-real-key',
                  'model': 'fake', 'max_tokens': 2048}
    return row, tmp_path / 'work', connection


def test_attachment_raw_response_is_saved_without_card_or_description(fixture):
    row, work, connection = fixture
    @contextmanager
    def lease(*_): yield
    @contextmanager
    def admit(*_):
        class FakeAdmission:
            def start_transport(self, mark_started): mark_started()
        yield FakeAdmission()
    opener = Opener()
    result = raw.run_raw_request(kind='attachment',
        request_body_factory=lambda: builder.prepare_attachment_body(row, work, connection),
        connection=connection, work=work, stop=threading.Event(), request_once=request_once,
        lease=lease, admit=admit, check_history=lambda: None,
        check_source=lambda: builder.check_attachment_source(row), opener=opener)
    assert result['status'] == 'raw_response_saved' and opener.calls == 1
    assert (work / 'evidence.json').is_file()
    assert not (work / 'description.json').exists() and not list(work.rglob('*.md'))
    again = raw.run_raw_request(kind='attachment',
        request_body_factory=lambda: builder.prepare_attachment_body(row, work, connection),
        connection=connection, work=work, stop=threading.Event(), request_once=request_once,
        lease=lease, admit=admit, check_history=lambda: None,
        check_source=lambda: builder.check_attachment_source(row), opener=opener)
    assert result == again and opener.calls == 1


def test_changed_source_and_unresolved_prior_history_fail_closed(fixture):
    row, work, connection = fixture
    Path(row['path']).write_text('changed', encoding='utf8')
    with pytest.raises(ValueError):
        builder.prepare_attachment_body(row, work, connection)
    Path(row['path']).write_text('synthetic attachment', encoding='utf8')
    row['history'] = {'status': 'unknown'}
    with pytest.raises(ValueError):
        builder.prepare_attachment_body(row, work, connection)


def test_prior_revision_is_not_treated_as_a_new_attachment_request(fixture):
    row, work, connection = fixture
    work.mkdir()
    (work / 'revision.json').write_text('{}', encoding='utf8')
    with pytest.raises(ValueError):
        builder.prepare_attachment_body(row, work, connection)


def test_attachment_runner_binds_folder_and_preserves_raw_response(fixture, tmp_path):
    row, work, connection = fixture
    runner, admission = load('attachment_runner'), load('pause_admission')
    identity = {'task_id': str(uuid4()), 'group_id': uuid4().hex, 'paper_id': 'paper-1',
                'receipt_id': 'e' * 64, 'attempt_id': 'attempt-1',
                'parent_run_id': str(uuid4()), 'task_run_id': str(uuid4())}
    manifest = {'schema': 'paperflow-local-preprocessing-handoff/1',
                **{k: identity[k] for k in ('task_id','group_id','paper_id','receipt_id','attempt_id')},
                'body': {'file_id': 'body-1', 'sha256': 'a' * 64}, 'attachments': [row],
                'reading_material': {'sha256': 'b' * 64},
                'capacity': {'status': 'planned', 'mode': 'single', 'parts': 1,
                    'plan_identity': {'schema': 'paperflow-body-input-plan/1', 'source_sha256': 'a' * 64,
                       'reading_sha256': 'b' * 64, 'plan_sha256': 'c' * 64, 'coverage_sha256': 'd' * 64,
                       'mode': 'single', 'part_count': 1,
                       'parts': [{'id': 'part-001', 'request_sha256': '1' * 64}]}}}
    database, locks = tmp_path / 'control.sqlite', tmp_path / 'locks'
    locks.mkdir()
    with sqlite3.connect(database) as db:
        db.executescript('''
            CREATE TABLE tasks(task_id TEXT PRIMARY KEY, desired_state TEXT,
                               start_generation INTEGER, start_pending INTEGER, task_status TEXT);
            CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY);
        ''')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?)', (identity['task_id'], 'running', 1, 0, 'complete'))
    guard = admission.AdmissionGuard(database, locks)
    guard.initialize()
    class Upstream:
        def for_execution(self, given):
            assert given == identity
            return json.loads(json.dumps(manifest, ensure_ascii=False))
    @contextmanager
    def lease(*_): yield
    opener, checked = Opener(), []
    args = {'identity': identity, 'generation': 1, 'manifest': manifest, 'row': row, 'work': work,
            'connection': connection, 'stop': admission.ControlStop(database, identity['task_id'], 1),
            'guard': guard, 'upstream': Upstream(), 'lease': lease,
            'verify_work': lambda current_row, path: checked.append((current_row['file_id'], path)),
            'check_history': lambda: None, 'request_once': request_once, 'opener': opener}
    first = runner.run_attachment_raw(**args)
    again = runner.run_attachment_raw(**args)
    assert first == again and opener.calls == 1 and checked
    assert Path(first['response_path']).read_text(encoding='utf8') == '原始附件答复'
    assert not (work / 'description.json').exists() and not list(work.rglob('*.md'))
    with sqlite3.connect(database) as db:
        assert db.execute("SELECT COUNT(*) FROM llm_request_admissions WHERE state='raw_complete'").fetchone()[0] == 1


def test_attachment_runner_rejects_foreign_folder_item(fixture):
    row, work, connection = fixture
    runner = load('attachment_runner')
    with pytest.raises(ValueError):
        runner.select_attachment({'attachments': []}, row)
