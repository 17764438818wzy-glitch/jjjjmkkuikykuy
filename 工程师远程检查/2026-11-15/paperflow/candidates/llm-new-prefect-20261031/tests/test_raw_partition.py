"""Frozen multi-part requests using the existing request journal and fake HTTP."""
from contextlib import contextmanager
import copy
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
sys.path[:0] = [str(HERE), str(ROOT / 'prefect-paperflow/paperflow_prefect/clean_overlay'),
                str(DESKTOP), 'F:/论文预处理系统/程序/desktop', str(DESKTOP.parent / 'core')]
from preextract_partition_run import create_plan
from preextract_request import request_once, IncompleteModelResponse


def load(name):
    spec = importlib.util.spec_from_file_location(name, HERE / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


contract = load('segment_plan_contract')
partition = load('raw_partition')


class Response:
    headers = {'Content-Type': 'application/json'}
    def __init__(self, finish): self.finish = finish
    def __enter__(self): return self
    def __exit__(self, *_): return False
    def read(self):
        return json.dumps({'choices': [{'message': {'content': '原始回复'},
                                        'finish_reason': self.finish}]}).encode()


class Opener:
    def __init__(self, finish='stop'): self.calls, self.finish = 0, finish
    def open(self, *_args, **_kwargs):
        self.calls += 1
        return Response(self.finish)


@pytest.fixture
def args(tmp_path):
    original = {'source': {'source_sha256': 'a' * 64}, 'blocks': []}
    adapted = copy.deepcopy(original)
    parts = []
    ranges = []
    for number in (1, 2):
        span = {'block_id': f'b{number}', 'start': 0, 'end': 3}
        body = {'model': 'fake', 'messages': [
            {'role': 'system', 'content': 'synthetic'},
            {'role': 'user', 'content': [{'type': 'text', 'text':
                json.dumps({'request_layout': 'legacy', 'part': number})}]}]}
        parts.append({'id': f'part-{number:03d}', 'ranges': [span], 'asset_ids': [],
                      'bundle': {'source': original['source'], 'blocks': []},
                      'body': body, 'capacity': {}})
        ranges.append(span)
    plan = {'schema': 'preextract-input-plan/1', 'mode': 'partitioned', 'parts': parts,
            'coverage': {'complete': True, 'block_count': 2, 'character_count': 6,
                         'ranges': ranges, 'asset_ids': []}}
    receipt = contract.describe_plan(plan, source_sha256='a' * 64, reading_sha256='b' * 64)
    observed = []
    @contextmanager
    def lease(kind, stop):
        observed.append(('lease', kind))
        yield
    @contextmanager
    def admit(part_id, transport):
        observed.append(('admit', part_id, transport['http_invoked']))
        class FakeAdmission:
            def start_transport(self, mark_started): mark_started()
        yield FakeAdmission()
    def accept(part_id, result): observed.append(('accept', part_id))
    return dict(plan_factory=lambda: (copy.deepcopy(plan), copy.deepcopy(original), copy.deepcopy(adapted)),
                plan_receipt=receipt, source_sha256='a' * 64, reading_sha256='b' * 64,
                template='synthetic template', work=tmp_path / 'work', stop=threading.Event(),
                connection={'base_url': 'http://127.0.0.1/fake', 'model': 'fake', 'api_key': 'not-a-real-key'},
                request_once=request_once, lease=lease, admit=admit, accept=accept,
                check_history=lambda: None, check_source=lambda: None, check_part=lambda _: None,
                create_plan=create_plan), observed


def test_each_part_is_recorded_once_without_parsing_or_synthesis(args):
    kwargs, observed = args
    opener = Opener()
    result = partition.run_partition_raw(**kwargs, opener=opener)
    assert result['status'] == 'body_parts_raw_saved' and len(result['parts']) == 2
    assert opener.calls == 2
    assert [row[1] for row in observed if row[0] == 'admit'] == ['part-001', 'part-002']
    root = kwargs['work'] / 'model'
    state = json.loads((root / 'partition-state.json').read_text(encoding='utf8'))
    assert state['status'] == 'parts_pending' and len(state['requests']) == 2
    assert not list(root.glob('global-*')) and not (root / 'assembled-candidate.json').exists()
    assert not list(kwargs['work'].rglob('*.md'))
    again = partition.run_partition_raw(**kwargs, opener=opener)
    assert again == result and opener.calls == 2


def test_pause_after_one_complete_part_resumes_without_resending_it(args):
    kwargs, _ = args
    opener = Opener()
    stop = kwargs['stop']
    kwargs['accept'] = lambda part_id, result: stop.set() if part_id == 'part-001' else None
    with pytest.raises(InterruptedError):
        partition.run_partition_raw(**kwargs, opener=opener)
    assert opener.calls == 1
    stop.clear()
    kwargs['accept'] = lambda *_: None
    result = partition.run_partition_raw(**kwargs, opener=opener)
    assert len(result['parts']) == 2 and opener.calls == 2


def test_unknown_or_incomplete_part_is_never_sent_again(args):
    kwargs, _ = args
    opener = Opener(finish='length')
    with pytest.raises(IncompleteModelResponse):
        partition.run_partition_raw(**kwargs, opener=opener)
    assert opener.calls == 1
    with pytest.raises(ValueError, match='分段.*未确认'):
        partition.run_partition_raw(**kwargs, opener=opener)
    assert opener.calls == 1


def test_changed_plan_is_rejected_before_a_request(args):
    kwargs, _ = args
    factory = kwargs['plan_factory']
    def changed():
        plan, original, adapted = factory()
        plan['parts'][0]['body']['model'] = 'different'
        return plan, original, adapted
    kwargs['plan_factory'] = changed
    opener = Opener()
    with pytest.raises(ValueError, match='分段计划'):
        partition.run_partition_raw(**kwargs, opener=opener)
    assert opener.calls == 0


def test_changed_image_at_the_next_part_never_reaches_transport(args):
    kwargs, _ = args
    opener = Opener()
    def check_part(part):
        if part['id'] == 'part-002':
            raise ValueError('图片版本发生变化')
    kwargs['check_part'] = check_part
    with pytest.raises(ValueError, match='图片'):
        partition.run_partition_raw(**kwargs, opener=opener)
    assert opener.calls == 1
    state = json.loads((kwargs['work'] / 'model/partition-state.json').read_text(encoding='utf8'))
    assert [row['id'] for row in state['requests']] == ['part-001']


def test_resume_reads_frozen_plan_without_replanning_or_overwriting_image_record(args):
    kwargs, _ = args
    frozen = load('frozen_body_plan')
    factory = kwargs['plan_factory']
    original = factory()[1]
    attempts = []
    image_record = kwargs['work'] / 'private/request-images.json'
    def first_plan():
        attempts.append(1)
        if len(attempts) > 1:
            raise AssertionError('resume must never rebuild image adaptations')
        image_record.parent.mkdir(parents=True, exist_ok=True)
        image_record.write_text('original-image-record', encoding='utf8')
        return factory()
    kwargs['plan_factory'] = lambda: frozen.partition_plan(
        kwargs['work'], kwargs['plan_receipt'], kwargs['source_sha256'], kwargs['reading_sha256'],
        original_factory=lambda: copy.deepcopy(original), new_plan_factory=first_plan)
    opener = Opener()
    kwargs['accept'] = lambda part_id, _: kwargs['stop'].set() if part_id == 'part-001' else None
    with pytest.raises(InterruptedError):
        partition.run_partition_raw(**kwargs, opener=opener)
    assert opener.calls == 1 and attempts == [1]
    kwargs['stop'].clear()
    kwargs['accept'] = lambda *_: None
    result = partition.run_partition_raw(**kwargs, opener=opener)
    assert len(result['parts']) == 2 and opener.calls == 2 and attempts == [1]
    assert image_record.read_text(encoding='utf8') == 'original-image-record'


def test_changed_frozen_plan_metadata_stops_resume_without_a_new_request(args):
    kwargs, _ = args
    frozen = load('frozen_body_plan')
    factory = kwargs['plan_factory']
    original = factory()[1]
    kwargs['plan_factory'] = lambda: frozen.partition_plan(
        kwargs['work'], kwargs['plan_receipt'], kwargs['source_sha256'], kwargs['reading_sha256'],
        original_factory=lambda: copy.deepcopy(original), new_plan_factory=factory)
    opener = Opener()
    kwargs['accept'] = lambda part_id, _: kwargs['stop'].set() if part_id == 'part-001' else None
    with pytest.raises(InterruptedError):
        partition.run_partition_raw(**kwargs, opener=opener)
    metadata_path = kwargs['work'] / 'private/llm-frozen-plan-metadata.json'
    metadata = json.loads(metadata_path.read_text(encoding='utf8'))
    metadata['top']['mode'] = 'single'
    metadata_path.write_text(json.dumps(metadata), encoding='utf8')
    kwargs['stop'].clear()
    with pytest.raises(ValueError, match='分段计划'):
        partition.run_partition_raw(**kwargs, opener=opener)
    assert opener.calls == 1


def test_part_responses_reconcile_real_admission_under_root_work_locks(args, tmp_path):
    kwargs, _ = args
    admission = load('pause_admission')
    database, lock_root = tmp_path / 'control.sqlite3', tmp_path / 'control-locks'
    lock_root.mkdir()
    task_id, task_run = str(uuid4()), str(uuid4())
    with sqlite3.connect(database) as db:
        db.executescript('''
            CREATE TABLE tasks(task_id TEXT PRIMARY KEY, desired_state TEXT,
                               start_generation INTEGER, start_pending INTEGER, task_status TEXT);
            CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY);
        ''')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?)', (task_id, 'running', 1, 0, 'complete'))
    guard = admission.AdmissionGuard(database, lock_root)
    guard.initialize()
    kwargs['stop'] = admission.ControlStop(database, task_id, 1)
    folder_completion = load('folder_completion')
    manifest = {'schema': 'paperflow-local-preprocessing-handoff/1',
                'task_id': task_id, 'group_id': 'a' * 32, 'paper_id': 'paper-1',
                'attempt_id': 'attempt-1', 'receipt_id': 'f' * 64,
                'body': {'file_id': 'body-1', 'sha256': 'a' * 64}, 'attachments': [],
                'reading_material': {'sha256': 'b' * 64},
                'capacity': {'status': 'planned', 'mode': 'partitioned', 'parts': 2,
                             'plan_identity': kwargs['plan_receipt']}}
    identity = {key: manifest[key] for key in ('task_id', 'group_id', 'paper_id', 'attempt_id', 'receipt_id')}
    identity.update(parent_run_id=str(uuid4()), task_run_id=task_run)
    ledger = load('folder_ledger').FolderLedger(database, lock_root)
    ledger.initialize()
    class FakeUpstream:
        def __call__(self, _): return copy.deepcopy(manifest)
        def for_execution(self, identity): return self(identity)
    upstream = FakeUpstream()
    ledger.register(identity, upstream)
    ledger.begin(identity, 1, upstream)
    components = folder_completion.expected_components(manifest, task_run)
    identifiers = {item['part_id']: item['component_id'] for item in components}
    component = lambda part: identifiers[part]
    kwargs['admit'] = lambda part, transport: guard.admit(
        task_id, 1, component(part), transport['request_sha256'], kind='body',
        work_dir=kwargs['work'], model_dir=transport['work_dir'], receipt_id='f' * 64,
        task_run_id=task_run, source_sha256='a' * 64)
    kwargs['accept'] = lambda part, result: guard.complete(task_id, component(part), result['response_sha256'])
    opener = Opener()
    result = partition.run_partition_raw(**kwargs, opener=opener)
    assert len(result['parts']) == 2 and opener.calls == 2
    with sqlite3.connect(database) as db:
        db.row_factory = sqlite3.Row
        assert guard.counts(db, task_id) == {'inflight': 0, 'unknown': 0}
        assert db.execute("SELECT COUNT(*) FROM llm_request_admissions WHERE state='raw_complete'").fetchone()[0] == 2
        rows = db.execute('SELECT * FROM llm_request_admissions WHERE task_id=?', (task_id,)).fetchall()
    verified = folder_completion.inspect_folder(manifest, task_run, rows,
        lambda row: guard.complete(task_id, row['component_id'], row['response_sha256']))
    assert verified['status'] == 'raw_complete' and verified['body_parts'] == 2
    assert ledger.finish(identity, upstream, guard)['state'] == 'waiting_postprocessing'
    assert ledger.counts(task_id)['waiting_postprocessing'] == 1
    manifest['attachments'].append({'file_id': 'still-waiting', 'sha256': 'c' * 64})
    assert folder_completion.inspect_folder(manifest, task_run, rows,
        lambda row: guard.complete(task_id, row['component_id'], row['response_sha256']))['status'] == 'incomplete'
