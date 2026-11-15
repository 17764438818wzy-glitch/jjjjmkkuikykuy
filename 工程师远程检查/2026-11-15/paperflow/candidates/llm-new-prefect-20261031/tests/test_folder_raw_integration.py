"""One folder: body and attachment raw evidence before waiting postprocessing."""
from contextlib import contextmanager
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
from uuid import uuid4


HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parents[1]
DESKTOP = Path('F:/论文预处理系统/程序/updates/unified-prefect-20261029-material-r02/desktop')
sys.path[:0] = [str(HERE), str(ROOT / 'prefect-paperflow/paperflow_prefect/clean_overlay'),
                str(ROOT / 'prefect-paperflow'), str(DESKTOP),
                'F:/论文预处理系统/程序/desktop', str(DESKTOP.parent / 'core')]
from preextract_request import request_once
from preextract_partition_run import create_plan
from paperflow_prefect import attachments, attachment_input
from body_runner import run_body_raw
from attachment_runner import run_attachment_raw
from folder_ledger import FolderLedger
from pause_admission import AdmissionGuard, ControlStop
from segment_plan_contract import describe_plan


def write_ref(path, value):
    data = json.dumps(value, ensure_ascii=False).encode('utf8')
    path.write_bytes(data)
    return {'path': str(path), 'sha256': hashlib.sha256(data).hexdigest()}


def test_one_folder_waits_for_both_branches_without_postprocessing(tmp_path, monkeypatch):
    source, attachment = tmp_path / 'body.txt', tmp_path / 'attachment.txt'
    source.write_text('body fixture', encoding='utf8')
    attachment.write_text('attachment fixture', encoding='utf8')
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    body = {'file_id': 'body-1', 'path': str(source), 'sha256': digest(source)}
    att = {'file_id': 'att-1', 'path': str(attachment), 'sha256': digest(attachment),
           'relative': attachment.name, 'name': attachment.name}
    material = {'source': {'source_sha256': body['sha256']}, 'blocks': [], 'assets': []}
    mat_ref = write_ref(tmp_path / 'material.json', material)
    read_ref = write_ref(tmp_path / 'reading.json', material)
    span = {'block_id': 'b1', 'start': 0, 'end': 3}
    plan = {'schema': 'preextract-input-plan/1', 'mode': 'single',
            'parts': [{'ranges': [span], 'asset_ids': [], 'capacity': {}, 'bundle': material,
                       'body': {'model': 'fake', 'messages': []}}],
            'coverage': {'complete': True, 'block_count': 1, 'character_count': 3,
                         'ranges': [span], 'asset_ids': []}}
    identity = {'task_id': str(uuid4()), 'group_id': uuid4().hex, 'paper_id': 'paper-1',
                'receipt_id': 'e' * 64, 'attempt_id': 'attempt-1',
                'parent_run_id': str(uuid4()), 'task_run_id': str(uuid4())}
    body_work, att_work = tmp_path / 'body-work', tmp_path / 'attachment-work'
    body_work.mkdir()
    receipt = describe_plan(plan, source_sha256=body['sha256'], reading_sha256=read_ref['sha256'])
    manifest = {'schema': 'paperflow-local-preprocessing-handoff/1',
                **{k: identity[k] for k in ('task_id','group_id','paper_id','receipt_id','attempt_id')},
                'body': body, 'attachments': [att], 'body_work': str(body_work),
                'material': mat_ref, 'reading_material': read_ref,
                'capacity': {'status': 'planned', 'mode': 'single', 'parts': 1, 'plan_identity': receipt}}
    database, locks = tmp_path / 'control.sqlite', tmp_path / 'locks'
    locks.mkdir()
    with sqlite3.connect(database) as db:
        db.executescript('''
            CREATE TABLE tasks(task_id TEXT PRIMARY KEY, desired_state TEXT,
                               start_generation INTEGER, start_pending INTEGER, task_status TEXT);
            CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY);
        ''')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?)', (identity['task_id'], 'running', 1, 0, 'complete'))
    guard, ledger = AdmissionGuard(database, locks), FolderLedger(database, locks)
    guard.initialize()
    ledger.initialize()
    class Upstream:
        def __call__(self, given):
            assert given == identity
            return copy.deepcopy(manifest)
        def for_execution(self, given): return self(given)
    upstream = Upstream()
    class Response:
        headers = {'Content-Type': 'application/json'}
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def read(self): return json.dumps({'choices': [{'message': {'content': 'raw only'},
                                                         'finish_reason': 'stop'}]}).encode()
    class Opener:
        calls = 0
        def open(self, *_args, **_kwargs):
            self.calls += 1
            return Response()
    opener = Opener()
    @contextmanager
    def lease(*_): yield
    evidence = {'format': '.txt', 'name': attachment.name, 'source_sha256': att['sha256'],
                'text': 'synthetic evidence', 'images': [], 'refs': ['text-1'], 'limitations': []}
    monkeypatch.setattr(attachments, 'evidence_for', lambda _: dict(evidence))
    monkeypatch.setattr(attachment_input, 'require_upstream_history_located', lambda _: None)
    stop = ControlStop(database, identity['task_id'], 1)
    connection = {'base_url': 'http://127.0.0.1/fake', 'api_key': 'not-a-real-key',
                  'model': 'fake', 'max_tokens': 2048}
    assert ledger.register(identity, upstream)['state'] == 'waiting_llm'
    ledger.begin(identity, 1, upstream)
    body_result = run_body_raw(identity=identity, generation=1, manifest=manifest, work=body_work,
        connection=connection, template='synthetic', stop=stop, guard=guard, upstream=upstream,
        lease=lease, check_history=lambda: None,
        new_plan_factory=lambda: (copy.deepcopy(plan), copy.deepcopy(material), copy.deepcopy(material)),
        verify_images=lambda *_: None, request_once=request_once, create_plan=create_plan, opener=opener)
    assert ledger.finish(identity, upstream, guard)['state'] == 'llm_processing'
    assert ledger.counts(identity['task_id'])['waiting_postprocessing'] == 0
    att_result = run_attachment_raw(identity=identity, generation=1, manifest=manifest, row=att,
        work=att_work, connection=connection, stop=stop, guard=guard, upstream=upstream, lease=lease,
        verify_work=lambda *_: None, check_history=lambda: None, request_once=request_once, opener=opener)
    assert ledger.finish(identity, upstream, guard)['state'] == 'waiting_postprocessing'
    assert ledger.counts(identity['task_id'])['waiting_postprocessing'] == 1
    assert opener.calls == 2
    assert Path(body_result['parts'][0]['response_path']).read_text(encoding='utf8') == 'raw only'
    assert Path(att_result['response_path']).read_text(encoding='utf8') == 'raw only'
    assert not list(tmp_path.rglob('*.md')) and not list(tmp_path.rglob('candidate.json'))
    assert not list(tmp_path.rglob('description.json'))
