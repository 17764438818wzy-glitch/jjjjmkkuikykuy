"""Connect actual raw request journals to a folder ledger using fake HTTP."""
from contextlib import closing, contextmanager
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import threading
from uuid import uuid4

import pytest
from prefect.settings import PREFECT_API_URL, temporary_settings

HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parents[1]
DESKTOP = Path('F:/论文预处理系统/程序/updates/unified-prefect-20261029-material-r02/desktop')
sys.path[:0] = [str(HERE), str(ROOT / 'prefect-paperflow'),
                str(ROOT / 'prefect-paperflow/paperflow_prefect/clean_overlay'),
                str(DESKTOP), 'F:/论文预处理系统/程序/desktop', str(DESKTOP.parent / 'core')]

import flow_model_operations as module
from flow_executor import execute_folder
from folder_ledger import FolderLedger
from pause_admission import AdmissionGuard, ControlStop
from segment_plan_contract import describe_plan


class Response:
    headers = {'Content-Type': 'application/json'}
    def __enter__(self): return self
    def __exit__(self, *_): return False
    def read(self):
        return json.dumps({'choices': [{'message': {'content': '合成的原始回复'},
                                        'finish_reason': 'stop'}]}).encode()


class Opener:
    def __init__(self): self.calls, self.lock = 0, threading.Lock()
    def open(self, *_args, **_kwargs):
        with self.lock: self.calls += 1
        return Response()


@pytest.mark.parametrize('body_parts', [1, 2])
def test_folders_run_body_and_attachment_once_and_keep_raw_responses(tmp_path, monkeypatch, body_parts):
    from paperflow_prefect import body_clean_handoff, body_segment_tokenizer
    import preextract_contract

    task_id, group_id, folder_id = str(uuid4()), uuid4().hex, str(uuid4())
    identity = {'task_id': task_id, 'group_id': group_id, 'paper_id': 'paper-1', 'receipt_id': 'e' * 64,
                'attempt_id': 'attempt-1', 'parent_run_id': str(uuid4()), 'task_run_id': folder_id}
    body_file, attachment_file = tmp_path / 'body.txt', tmp_path / 'attachment.txt'
    body_file.write_text('合成正文', encoding='utf8')
    attachment_file.write_text('合成附件', encoding='utf8')
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    body = {'file_id':'body-1', 'kind':'body', 'sha256':digest(body_file), 'path':str(body_file),
            'relative':'body.txt'}
    attachment = {'file_id':'attachment-1', 'kind':'attachment', 'sha256':digest(attachment_file),
                  'path':str(attachment_file)}
    work = tmp_path / 'body-work'
    work.mkdir()
    material = {'source': {'source_sha256': body['sha256']}, 'blocks': [], 'assets': []}
    reading = copy.deepcopy(material)
    ranges = [{'block_id': f'b{i}', 'start':0, 'end':3} for i in range(body_parts)]
    parts = []
    for i in range(body_parts):
        request = {'model':'fake', 'messages': [{'role':'system','content':'synthetic'},
                   {'role':'user','content':[{'type':'text','text':json.dumps({'request_layout':'legacy'})}]}]}
        parts.append({'id':f'part-{i + 1:03d}', 'ranges':[ranges[i]], 'asset_ids':[], 'capacity':{},
                      'body':request, 'bundle':copy.deepcopy(material)})
    plan = {'schema':'preextract-input-plan/1', 'mode':'single' if body_parts == 1 else 'partitioned',
            'parts':parts, 'coverage': {'complete':True, 'ranges':ranges, 'asset_ids':[],
                                        'block_count':body_parts, 'character_count':3 * body_parts}}
    reading_hash = 'd' * 64
    plan_identity = describe_plan(plan, source_sha256=body['sha256'], reading_sha256=reading_hash)
    paper = {'paper_id':'paper-1', 'body':body, 'attachments':[attachment]}
    manifest = {'schema':'paperflow-local-preprocessing-handoff/1',
        **{k: identity[k] for k in ('task_id','group_id','paper_id','receipt_id','attempt_id')},
        'body':body, 'attachments':[attachment], 'body_work':str(work),
        'reading_material':{'sha256':reading_hash}, 'capacity': {'status':'planned',
            'mode':plan['mode'], 'parts':body_parts, 'plan_identity':plan_identity}}
    class Binding:
        def load(self, _, require_running=False):
            return {'manifest':manifest, 'paper':paper, 'material':material, 'reading_material':reading}
        def __call__(self, _): return manifest
        def for_execution(self, _): return manifest
    class Backend:
        root, catalog = tmp_path / 'runtime', {}
        def body_work(self, _): return work
        def attachment_work(self, _paper, _row): return tmp_path / 'attachment-work'
        def confirm_history(self, *_): pass
    config_path, template_path = tmp_path / 'model.json', tmp_path / 'template.txt'
    config_path.write_text(json.dumps({'text_model':'fake', 'base_url':'http://127.0.0.1/fake',
        'preextract_limits':{'model':'fake', 'base_url':'http://127.0.0.1/fake', 'supports_images':True,
                             'context_window':100000}}), encoding='utf8')
    template_path.write_text('synthetic template', encoding='utf8')
    class Settings:
        def path(self, key): return {'model_config':config_path, 'template_path':template_path}[key]
    monkeypatch.setattr(preextract_contract, 'load_template', lambda _: ('synthetic template', None))
    monkeypatch.setattr(body_segment_tokenizer, 'with_body_token_counter', lambda config: config)
    monkeypatch.setattr(body_clean_handoff, 'plan_with_available_images',
                        lambda *_: (copy.deepcopy(plan), copy.deepcopy(material), []))
    monkeypatch.setattr(module, 'verify_body_images', lambda *_: None)
    monkeypatch.setattr(module, 'prepare_attachment_body', lambda *_: {'model':'fake', 'messages':[]})
    database, locks = tmp_path / 'control.db', tmp_path / 'locks'
    locks.mkdir()
    with closing(sqlite3.connect(database)) as db, db:
        db.executescript('CREATE TABLE tasks(task_id TEXT,desired_state TEXT,start_generation INTEGER,'
                         'start_pending INTEGER,task_status TEXT); CREATE TABLE task_deletions(task_id TEXT);')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?)', (task_id, 'running', 3, 0, 'complete'))
    guard, ledger = AdmissionGuard(database, locks), FolderLedger(database, locks)
    guard.initialize()
    ledger.initialize()
    stop, seen, opener = ControlStop(database, task_id, 3), [], Opener()
    @contextmanager
    def acquire(names, current_stop, *, expected):
        assert set(names) == set(expected) and not current_stop.is_set()
        seen.append(tuple(names))
        yield
    binding = Binding()
    connection = {'base_url':'http://127.0.0.1/fake', 'model':'fake', 'api_key':'synthetic'}
    ops = module.FolderModelOperations(identity, 3, binding, Backend(), Settings(), guard, stop,
        connection, 'http://synthetic-prefect/api', acquire, opener=opener)
    with temporary_settings({PREFECT_API_URL:'http://synthetic-prefect/api'}):
        result = execute_folder(identity, 3, ledger, binding, ops, guard, stop)
    assert result['state'] == 'waiting_postprocessing' and opener.calls == body_parts + 1
    assert len(seen) == body_parts + 1
    assert ledger.counts(task_id)['waiting_postprocessing'] == 1
    with closing(sqlite3.connect(database)) as db:
        assert db.execute("SELECT COUNT(*) FROM llm_request_admissions WHERE state='raw_complete'").fetchone()[0] == body_parts + 1
    assert not list(tmp_path.rglob('*.md'))
    assert not list(tmp_path.rglob('description.json'))
    assert not list(tmp_path.rglob('candidate.json'))
