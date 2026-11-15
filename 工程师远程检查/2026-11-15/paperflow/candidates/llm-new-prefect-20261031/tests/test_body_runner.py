"""Synthetic end-to-end body branch through real request journaling and admission."""
from contextlib import contextmanager
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
from uuid import uuid4

import pytest


HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parents[1]
DESKTOP = Path('F:/论文预处理系统/程序/updates/unified-prefect-20261029-material-r02/desktop')
sys.path[:0] = [str(HERE), str(ROOT / 'prefect-paperflow/paperflow_prefect/clean_overlay'),
                str(DESKTOP), 'F:/论文预处理系统/程序/desktop', str(DESKTOP.parent / 'core')]
from preextract_request import request_once
from preextract_partition_run import create_plan


def load(name):
    spec = importlib.util.spec_from_file_location(name, HERE / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


runner = load('body_runner')
contract = load('segment_plan_contract')
admission = load('pause_admission')


def write_ref(path, value):
    data = json.dumps(value, ensure_ascii=False).encode('utf8')
    path.write_bytes(data)
    return {'path': str(path), 'sha256': hashlib.sha256(data).hexdigest()}


@pytest.fixture
def work(tmp_path):
    source = tmp_path / 'delivery.txt'
    source.write_text('synthetic paper', encoding='utf8')
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    material = {'source': {'source_sha256': source_hash}, 'blocks': [], 'assets': []}
    reading = copy.deepcopy(material)
    material_ref = write_ref(tmp_path / 'material.json', material)
    reading_ref = write_ref(tmp_path / 'reading.json', reading)
    span = {'block_id': 'b1', 'start': 0, 'end': 3}
    plan = {'schema': 'preextract-input-plan/1', 'mode': 'single',
            'parts': [{'ranges': [span], 'asset_ids': [], 'capacity': {}, 'bundle': reading,
                       'body': {'model': 'fake', 'messages': []}}],
            'coverage': {'complete': True, 'block_count': 1, 'character_count': 3,
                         'ranges': [span], 'asset_ids': []}}
    identity = {'task_id': str(uuid4()), 'group_id': uuid4().hex, 'paper_id': 'paper-1',
                'receipt_id': 'e' * 64, 'attempt_id': 'attempt-1',
                'parent_run_id': str(uuid4()), 'task_run_id': str(uuid4())}
    body = {'file_id': 'body-1', 'sha256': source_hash, 'path': str(source)}
    folder = tmp_path / 'body-work'
    folder.mkdir()
    receipt = contract.describe_plan(plan, source_sha256=source_hash, reading_sha256=reading_ref['sha256'])
    manifest = {'schema': 'paperflow-local-preprocessing-handoff/1',
                **{k: identity[k] for k in ('task_id','group_id','paper_id','receipt_id','attempt_id')},
                'body': body, 'attachments': [], 'body_work': str(folder), 'material': material_ref,
                'reading_material': reading_ref,
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
    guard = admission.AdmissionGuard(database, locks)
    guard.initialize()
    stop = admission.ControlStop(database, identity['task_id'], 1)
    class Upstream:
        def for_execution(self, given):
            assert given == identity
            return copy.deepcopy(manifest)
    @contextmanager
    def lease(*_): yield
    images, history = [], []
    class Response:
        headers = {'Content-Type': 'application/json'}
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def read(self):
            return json.dumps({'choices': [{'message': {'content': '保留原始回复，非 JSON'},
                                            'finish_reason': 'stop'}]}, ensure_ascii=False).encode()
    class Opener:
        calls = 0
        def open(self, *_args, **_kwargs):
            self.calls += 1
            return Response()
    opener = Opener()
    args = {'identity': identity, 'generation': 1, 'manifest': manifest,
            'work': folder, 'connection': {'base_url': 'http://127.0.0.1/fake', 'api_key': 'not-a-real-key'},
            'template': 'synthetic', 'stop': stop, 'guard': guard, 'upstream': Upstream(),
            'lease': lease, 'check_history': lambda: history.append(1),
            'new_plan_factory': lambda: (copy.deepcopy(plan), copy.deepcopy(material), copy.deepcopy(reading)),
            'verify_images': lambda *_: images.append(1), 'request_once': request_once,
            'create_plan': create_plan, 'opener': opener}
    return args, opener, images, history, database


def test_single_body_saves_raw_once_and_never_parses_or_creates_md(work):
    args, opener, images, history, database = work
    first = runner.run_body_raw(**args)
    second = runner.run_body_raw(**args)
    assert first == second and opener.calls == 1
    assert Path(first['parts'][0]['response_path']).read_text(encoding='utf8') == '保留原始回复，非 JSON'
    assert images and history and not list(args['work'].rglob('*.md'))
    assert not list(args['work'].rglob('candidate.json'))
    with sqlite3.connect(database) as db:
        assert db.execute("SELECT COUNT(*) FROM llm_request_admissions WHERE state='raw_complete'").fetchone()[0] == 1


def test_changed_delivery_or_pause_cannot_start_body(work):
    args, opener, _, _, database = work
    Path(args['manifest']['body']['path']).write_text('changed', encoding='utf8')
    with pytest.raises(ValueError, match='正文'):
        runner.run_body_raw(**args)
    assert opener.calls == 0
    with sqlite3.connect(database) as db:
        db.execute("UPDATE tasks SET desired_state='paused'")
    with pytest.raises(InterruptedError):
        runner.run_body_raw(**args)
    assert opener.calls == 0


def test_partitioned_body_saves_every_raw_segment_and_reuses_on_restart(work):
    args, opener, _, _, database = work
    material = json.loads(Path(args['manifest']['material']['path']).read_text(encoding='utf8'))
    parts, ranges = [], []
    for number in (1, 2):
        span = {'block_id': f'b{number}', 'start': 0, 'end': 3}
        request = {'model': 'fake', 'messages': [{'role': 'system', 'content': 'synthetic'},
            {'role': 'user', 'content': [{'type': 'text', 'text': json.dumps({'part': number})}]}]}
        parts.append({'id': f'part-{number:03d}', 'ranges': [span], 'asset_ids': [],
                      'capacity': {}, 'bundle': copy.deepcopy(material), 'body': request})
        ranges.append(span)
    plan = {'schema': 'preextract-input-plan/1', 'mode': 'partitioned', 'parts': parts,
            'coverage': {'complete': True, 'block_count': 2, 'character_count': 6,
                         'ranges': ranges, 'asset_ids': []}}
    receipt = contract.describe_plan(plan, source_sha256=args['manifest']['body']['sha256'],
                                     reading_sha256=args['manifest']['reading_material']['sha256'])
    args['manifest']['capacity'] = {'status': 'planned', 'mode': 'partitioned',
                                    'parts': 2, 'plan_identity': receipt}
    args['new_plan_factory'] = lambda: (copy.deepcopy(plan), copy.deepcopy(material), copy.deepcopy(material))
    first = runner.run_body_raw(**args)
    again = runner.run_body_raw(**args)
    assert first == again and len(first['parts']) == 2 and opener.calls == 2
    assert all(Path(part['response_path']).read_text(encoding='utf8') == '保留原始回复，非 JSON'
               for part in first['parts'])
    assert not list(args['work'].rglob('*.md')) and not list(args['work'].rglob('candidate.json'))
    with sqlite3.connect(database) as db:
        assert db.execute("SELECT COUNT(*) FROM llm_request_admissions WHERE state='raw_complete'").fetchone()[0] == 2
