from contextlib import contextmanager
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
from urllib.error import HTTPError
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[3]
DESKTOP = Path('F:/论文预处理系统/程序/updates/unified-prefect-20261029-material-r02/desktop')
sys.path[:0] = [str(ROOT / 'prefect-paperflow'), str(ROOT / 'prefect-paperflow/paperflow_prefect/clean_overlay'),
                str(DESKTOP), 'F:/论文预处理系统/程序/desktop', str(DESKTOP.parent / 'core')]
from paperflow_prefect.llm_queue.flow_executor import execute_folder
from paperflow_prefect.llm_queue.folder_completion import expected_components
from paperflow_prefect.llm_queue.folder_ledger import FolderLedger
from paperflow_prefect.llm_queue.pause_admission import AdmissionGuard, ControlStop
from paperflow_prefect.llm_queue.raw_request import _body_hash, run_raw_request
from preextract_request import request_once

source = Path(__file__).resolve().parents[1] / 'retry_service.py'
spec = importlib.util.spec_from_file_location('retry_outcome_candidate', source)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_failed_retry_then_success_moves_folder_to_waiting(tmp_path):
    dbpath, locks, work = tmp_path / 'control.db', tmp_path / 'locks', tmp_path / 'body'
    locks.mkdir()
    model = work / 'model'
    model.mkdir(parents=True)
    task, parent, folder = (str(uuid4()) for _ in range(3))
    identity = dict(task_id=task, group_id='1' * 32, paper_id='paper', receipt_id='b' * 64,
                    attempt_id='first', parent_run_id=parent, task_run_id=folder)
    body = {'model': 'fake', 'messages': []}
    digest = _body_hash(body)
    plan = dict(schema='paperflow-body-input-plan/1', source_sha256='c' * 64,
                reading_sha256='d' * 64, plan_sha256='e' * 64, coverage_sha256='f' * 64,
                mode='single', part_count=1, parts=[{'id': 'part-001', 'request_sha256': digest}])
    manifest = dict(schema='paperflow-local-preprocessing-handoff/1',
                    **{k: identity[k] for k in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
                    body={'file_id': 'body', 'sha256': 'c' * 64}, attachments=[],
                    reading_material={'sha256': 'd' * 64},
                    capacity={'status': 'planned', 'parts': 1, 'mode': 'single', 'plan_identity': plan})
    component = expected_components(manifest, folder)[0]['component_id']
    with sqlite3.connect(dbpath) as conn:
        conn.executescript('''CREATE TABLE tasks(task_id TEXT,desired_state TEXT,start_generation INTEGER,
                start_pending INTEGER,task_status TEXT); CREATE TABLE task_deletions(task_id TEXT);''')
        conn.execute('INSERT INTO tasks VALUES(?,?,?,?,?)', (task, 'running', 2, 0, 'complete'))
    guard, ledger = AdmissionGuard(dbpath, locks), FolderLedger(dbpath)
    guard.initialize()
    ledger.initialize()
    (model / 'request.json').write_text('old failed request', encoding='utf8')
    with sqlite3.connect(dbpath) as conn:
        conn.execute('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (task, component, 1, digest, 'unknown', None, 'body', str(work), str(model),
             identity['receipt_id'], folder, 'c' * 64, 1, 2))
    service = module.RetryService.__new__(module.RetryService)
    service.database = dbpath
    service._task = lambda _db, _item: {'start_generation': 2, 'prefect_run_id': parent}
    class Binding:
        def __call__(self, _): return manifest
        def for_execution(self, _): return manifest
    class Response:
        headers = {'Content-Type': 'application/json'}
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def read(self):
            return json.dumps({'choices': [{'message': {'content': 'synthetic reply'},
                                            'finish_reason': 'stop'}]}).encode()
    class Opener:
        def __init__(self, fail): self.fail, self.calls = fail, 0
        def open(self, *_args, **_kwargs):
            self.calls += 1
            if self.fail: raise HTTPError('http://invalid', 502, 'synthetic', {}, None)
            return Response()
    @contextmanager
    def lease(_kind, _stop): yield
    class Operations:
        def __init__(self, opener): self.opener = opener
        def body(self):
            result = run_raw_request(kind='body', request_body_factory=lambda: body,
                connection={'base_url': 'http://127.0.0.1/fake', 'api_key': 'synthetic'}, work=work,
                stop=ControlStop(dbpath, task, 2), request_once=request_once, lease=lease,
                admit=lambda transport: guard.admit(task, 2, component, transport['request_sha256'],
                    kind='body', model_dir=transport['work_dir'], receipt_id=identity['receipt_id'],
                    task_run_id=folder, source_sha256='c' * 64),
                check_history=lambda: None, check_source=lambda: None, opener=self.opener)
            guard.complete(task, component, result['response_sha256'])
            return result
        def attachment(self, _): raise AssertionError('no attachments')
    def reset():
        service._reset_failed({'task_id': task}, {'start_generation': 2, 'prefect_run_id': parent}, folder)
    reset()
    failed = Opener(True)
    with pytest.raises(Exception):
        execute_folder(identity, 2, ledger, Binding(), Operations(failed), guard, ControlStop(dbpath, task, 2))
    assert failed.calls == 1
    with sqlite3.connect(dbpath) as conn:
        assert conn.execute('SELECT state FROM llm_folder_queue').fetchone()[0] == 'llm_processing'
        assert conn.execute('SELECT state FROM llm_request_admissions').fetchone()[0] == 'unknown'
    reset()
    passed = Opener(False)
    result = execute_folder(identity, 2, ledger, Binding(), Operations(passed), guard, ControlStop(dbpath, task, 2))
    assert passed.calls == 1
    assert result['state'] == 'waiting_postprocessing'
    with sqlite3.connect(dbpath) as conn:
        assert conn.execute('SELECT state FROM llm_request_admissions').fetchone()[0] == 'raw_complete'
