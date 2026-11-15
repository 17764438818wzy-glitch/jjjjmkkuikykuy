from contextlib import contextmanager
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[3]
DESKTOP = Path('F:/论文预处理系统/程序/updates/unified-prefect-20261029-material-r02/desktop')
sys.path[:0] = [str(ROOT / 'prefect-paperflow'), str(ROOT / 'prefect-paperflow/paperflow_prefect/clean_overlay'),
                str(DESKTOP), 'F:/论文预处理系统/程序/desktop', str(DESKTOP.parent / 'core')]
from paperflow_prefect import body_clean_handoff, request_admission
from paperflow_prefect.llm_queue.folder_completion import expected_components
from paperflow_prefect.llm_queue.pause_admission import AdmissionGuard, ControlStop
from paperflow_prefect.llm_queue.segment_plan_contract import describe_plan
import preextract_contract

HERE = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('paperflow_prefect.llm_queue._candidate_manual_operations',
    HERE / 'paperflow_prefect/llm_queue/flow_model_operations.py')
operations_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(operations_module)
spec = importlib.util.spec_from_file_location('_retry_real_body_candidate', HERE / 'retry_service.py')
retry_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(retry_module)


def test_retry_runs_the_real_frozen_body_adapter_with_fake_model(monkeypatch, tmp_path):
    db, locks, work = tmp_path / 'control.db', tmp_path / 'locks', tmp_path / 'work'
    locks.mkdir()
    (work / 'model').mkdir(parents=True)
    (work / 'private').mkdir()
    source = tmp_path / 'paper.txt'
    source.write_text('synthetic paper', encoding='utf8')
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    task, folder = str(uuid4()), str(uuid4())
    receipt = 'b' * 64
    reading = {'assets': [], 'blocks': [], 'sections': [], 'issues': []}
    bundle = {'assets': []}
    body = {'model': 'synthetic', 'messages': [
        {'role': 'system', 'content': 'synthetic'},
        {'role': 'user', 'content': [{'type': 'text', 'text': json.dumps({'material': bundle})}]},
    ]}
    plan = {'schema': 'preextract-input-plan/1', 'mode': 'single', 'parts': [{
        'id': 'part-001', 'ranges': [], 'asset_ids': [], 'capacity': {}, 'body': body, 'bundle': bundle,
    }], 'coverage': {'complete': True, 'ranges': [], 'asset_ids': [], 'block_count': 0, 'character_count': 0}}
    reading_sha = 'd' * 64
    plan_identity = describe_plan(plan, source_sha256=source_sha, reading_sha256=reading_sha)
    (work / 'private' / 'llm-frozen-single-plan.json').write_text(json.dumps({
        'schema': 'paperflow-raw-frozen-single-plan/1', 'plan_sha256': plan_identity['plan_sha256'], 'plan': plan,
    }), encoding='utf8')
    manifest = {'task_id': task, 'group_id': '1' * 32, 'receipt_id': receipt,
        'body': {'file_id': 'body', 'sha256': source_sha}, 'attachments': [], 'body_work': str(work),
        'reading_material': {'sha256': reading_sha},
        'capacity': {'status': 'planned', 'parts': 1, 'mode': 'single', 'plan_identity': plan_identity}}
    paper = {'body': {'file_id': 'body', 'path': str(source), 'sha256': source_sha, 'kind': 'body'}}
    identity = {'task_id': task, 'task_run_id': folder, 'receipt_id': receipt}
    component = expected_components(manifest, folder)[0]
    with sqlite3.connect(db) as conn:
        conn.executescript('''CREATE TABLE tasks(task_id TEXT,desired_state TEXT,start_generation INTEGER,
          start_pending INTEGER,task_status TEXT); CREATE TABLE task_deletions(task_id TEXT);''')
        conn.execute('INSERT INTO tasks VALUES(?,?,?,?,?)', (task, 'running', 2, 0, 'complete'))
    guard = AdmissionGuard(db, locks)
    guard.initialize()
    (work / 'model' / 'request.json').write_text('old failure', encoding='utf8')
    with sqlite3.connect(db) as conn:
        conn.execute('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (task, component['component_id'], 1, component['request_sha256'], 'unknown', None, 'body',
             str(work), str(work / 'model'), receipt, folder, source_sha, 1, 2))
    reset = retry_module.RetryService.__new__(retry_module.RetryService)
    reset.database = db
    reset._task = lambda *_: {'start_generation': 2, 'prefect_run_id': 'parent'}
    reset._reset_failed({'task_id': task}, {'start_generation': 2, 'prefect_run_id': 'parent'}, folder)
    monkeypatch.setattr(request_admission, 'bind_work', lambda *_: None)
    monkeypatch.setattr(body_clean_handoff, 'save_projection', lambda *_: None)
    monkeypatch.setattr(preextract_contract, 'load_template', lambda _: ('synthetic', None))
    current = {'manifest': manifest, 'paper': paper, 'reading_material': reading, 'material': reading}
    binding = SimpleNamespace(load=lambda *_args, **_kwargs: current)
    backend = SimpleNamespace(body_work=lambda _: work, root=tmp_path, catalog={})
    settings = SimpleNamespace(path=lambda _: tmp_path / 'template')
    class Response:
        headers = {'Content-Type': 'application/json'}
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def read(self):
            return json.dumps({'choices': [{'message': {'content': 'synthetic result'},
                                            'finish_reason': 'stop'}]}).encode()
    class Opener:
        calls = 0
        def open(self, *_args, **_kwargs):
            self.calls += 1
            return Response()
    opener = Opener()
    ops = operations_module.FolderModelOperations(identity, 2, binding, backend, settings, guard,
        ControlStop(db, task, 2), {'base_url': 'http://127.0.0.1/fake', 'api_key': 'synthetic'},
        'http://fake-prefect', lambda *_: None, opener=opener, allow_manual_retry=True)
    @contextmanager
    def lease(*_): yield
    ops._lease = lease
    result = ops.body()
    assert opener.calls == 1
    assert Path(result['response_path']).read_text(encoding='utf8') == 'synthetic result'
    with sqlite3.connect(db) as conn:
        assert conn.execute('SELECT state FROM llm_request_admissions').fetchone()[0] == 'raw_complete'
