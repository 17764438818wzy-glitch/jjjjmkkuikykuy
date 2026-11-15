from contextlib import nullcontext
import hashlib
import importlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
import time
from threading import Event, Thread
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest


ROOT = Path(__file__).resolve().parents[2]
PROJECT = ROOT.parents[1]


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def modules():
    return SimpleNamespace(
        projection=load(ROOT / 'scan_rebind_next' / 'projections.py', 'isolated_projections'),
        stages=load(ROOT / 'folder_state.py', 'isolated_stage_store'),
        results=load(ROOT / 'results_next' / 'postprocessing_results.py', 'isolated_results_store'),
        runtime=load(ROOT / 'runtime_next' / 'stage_runtime.py', 'isolated_stage_runtime'),
        orders=load(PROJECT / 'candidates' / 'local-intake-live-link-20261031' / 'prefect_orders.py',
                    'isolated_prefect_orders'),
    )


class State:
    def __init__(self, kind):
        self.kind = kind
        self.state_details = SimpleNamespace(pause_reschedule=True)
    def is_running(self): return self.kind == 'RUNNING'
    def is_paused(self): return self.kind == 'PAUSED'
    def is_completed(self): return self.kind == 'COMPLETED'
    def is_final(self): return self.kind in {'COMPLETED', 'FAILED', 'CANCELLED'}
    def is_cancelling(self): return self.kind == 'CANCELLING'


class FakeClient:
    def __init__(self, item):
        self.api_url = 'http://synthetic-prefect/api'
        self.root = SimpleNamespace(id=uuid4(), tags=['paperflow-order-v2-live-20261031'])
        self.folder_deployment = SimpleNamespace(id=uuid4(), tags=['paperflow-order-v2-live-20261031'],
                                                  work_pool_name='paperflow-unified-preprocessing')
        parent = SimpleNamespace(id=UUID(item['parent_run_id']), deployment_id=self.root.id,
            parent_task_run_id=None, state=State('PAUSED'), idempotency_key='first-step-order-v2-' + item['task_id'],
            parameters={'task_id': item['task_id']})
        folder = SimpleNamespace(id=UUID(item['task_run_id']), deployment_id=self.folder_deployment.id,
            parent_task_run_id=None, state=State('PAUSED'), name='合成文件夹 等待后处理',
            idempotency_key='folder-waiting-v2-' + item['task_id'] + '-' + item['group_id'],
            work_queue_name='task-' + item['task_id'],
            parameters={key: item[key] for key in ('task_id', 'group_id', 'receipt_id', 'attempt_id')})
        self.flows = {str(value.id): value for value in (parent, folder)}
        self.stages = {}
        self.during_read = None
        self.during_task_read = None
        self.task_reads = 0

    def __enter__(self): return self
    def __exit__(self, *_): pass
    def read_deployment_by_name(self, _): return self.root
    def read_flow_runs(self, flow_run_filter, limit):
        if self.during_read:
            callback, self.during_read = self.during_read, None
            callback()
        return [self.flows[str(key)] for key in flow_run_filter.id.any_ if str(key) in self.flows]
    def read_task_runs(self, task_run_filter, limit):
        self.task_reads += 1
        if self.during_task_read:
            self.during_task_read(self.task_reads)
        return [self.stages[str(key)] for key in task_run_filter.id.any_ if str(key) in self.stages]


class FakeQueue:
    def __init__(self, path, item, reference):
        self.path, self.item, self.reference = path, item, reference
        self.consumer_id = 'local-intake'
        self.unavailable_count = 0
        self.snap = dict(receiver_status='running', receiver_session_id='receiver-test',
            scan_source_instance='source-test', waiting_available=True, upstream_scan_verified=True,
            upstream_scan_completed_at='2026-11-01T00:00:00Z',
            waiting_llm_total=1, as_of='2026-11-01T00:00:00Z')
    def snapshot(self, **_): return dict(self.snap)
    def list_waiting_llm(self):
        raw = Path(self.reference['manifest_path']).read_bytes()
        if hashlib.sha256(raw).hexdigest() != self.reference['manifest_sha256']:
            return dict(waiting_count=0, unavailable_count=1, items=[])
        return dict(waiting_count=1, unavailable_count=self.unavailable_count,
                    items=[{**{key: self.item[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id')},
                            **self.reference}])


class FakeObserver:
    def __init__(self, queue, index, client, orders):
        self.queue, self.index, self.client = queue, index, client
        self.api_url = client.api_url
        self.receipt_root = index.parent
        self.source_ok = True
        self.title_calls = 0
        self._llm_handoff = orders.FolderOrderService._llm_handoff
    def _source_matches(self, _): return self.source_ok
    def _client(self): return self.client
    def _safe_deployment(self, _): return self.client.folder_deployment
    def _add_result_titles(self, items):
        self.title_calls += 1
        for item in items:
            item.update(title_status='已提取', paper_title='合成论文题目')


@pytest.fixture
def sample(tmp_path, modules):
    item = dict(task_id=str(uuid4()), group_id='a' * 32, paper_id='synthetic-paper', receipt_id='b' * 64,
                attempt_id='attempt-test', parent_run_id=str(uuid4()), task_run_id=str(uuid4()))
    manifest = dict(schema='paperflow-local-preprocessing-handoff/1', consumer_id='prefect-local-preprocessing',
                    **{key: item[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')})
    path = tmp_path / 'synthetic-manifest.json'
    raw = json.dumps(manifest, ensure_ascii=False).encode('utf8')
    path.write_bytes(raw)
    reference = dict(manifest_path=str(path), manifest_sha256=hashlib.sha256(raw).hexdigest())
    digest = hashlib.sha256(json.dumps(manifest, ensure_ascii=False, sort_keys=True,
                            separators=(',', ':'), allow_nan=False).encode('utf8')).hexdigest()
    control = tmp_path / 'control.sqlite3'
    with sqlite3.connect(control) as db:
        db.execute('CREATE TABLE tasks(task_id TEXT PRIMARY KEY,desired_state TEXT,start_generation INTEGER,'
                   'start_pending INTEGER,task_status TEXT,prefect_run_id TEXT,prefect_order_model TEXT,name TEXT)')
        db.execute('CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY)')
        db.execute('CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,'
                   'attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,manifest_sha256 TEXT,state TEXT,'
                   'generation INTEGER,PRIMARY KEY(task_id,group_id))')
        db.execute('CREATE TABLE paperflow_postprocess_stage_jobs ('
                   'job_id TEXT PRIMARY KEY,task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,'
                   'attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,folder_run_id TEXT,generation INTEGER,'
                   'manifest_sha256 TEXT,task_key TEXT,state TEXT,error_type TEXT,owner_pid INTEGER,'
                   'owner_started REAL,created_at_ns INTEGER,updated_at_ns INTEGER,source_llm_generation INTEGER)')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?)',
                   (item['task_id'], 'running', 1, 0, 'running', item['parent_run_id'], 'v2', '合成任务'))
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?)',
                   (*item.values(), digest, 'waiting_postprocessing', 1))
    local = tmp_path / 'queue.sqlite3'
    with sqlite3.connect(local) as db:
        db.execute('CREATE TABLE preprocessing_folder_orders(task_id TEXT,group_id TEXT,receipt_id TEXT,'
                   'attempt_id TEXT,prefect_run_id TEXT)')
        db.executescript('CREATE TABLE metadata(key TEXT,value TEXT); CREATE TABLE cancelled_tasks(task_id TEXT);'
            'CREATE TABLE groups(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,state TEXT,checked_session TEXT);'
            'CREATE TABLE claims(consumer_id TEXT,attempt_id TEXT,task_id TEXT,group_id TEXT,receipt_id TEXT,state TEXT);'
            'CREATE TABLE preprocessing_handoffs(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,'
            'consumer_id TEXT,attempt_id TEXT,state TEXT,handoff TEXT);'
            'CREATE TABLE retention_requests(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,'
            'consumer_id TEXT,state TEXT,retention_id TEXT,last_error TEXT,checked_session TEXT,updated_at TEXT);')
        db.execute('INSERT INTO metadata VALUES(?,?)', ('receiver_heartbeat_at', 'heartbeat-1'))
        db.execute('INSERT INTO preprocessing_folder_orders VALUES(?,?,?,?,?)',
                   (item['task_id'], item['group_id'], item['receipt_id'], item['attempt_id'], item['task_run_id']))
    stages = modules.stages.FolderStageStore(control, observe_run=lambda _: True)
    stages.initialize()
    results = modules.results.ResultsStore(control, admission_gate=lambda _: nullcontext(),
                                            inspect_receipt=modules.stages.FolderStageStore._inspect_receipt)
    results.initialize()
    queue = FakeQueue(local, item, reference)
    client = FakeClient(item)
    observer = FakeObserver(queue, control, client, modules.orders)
    class Binding:
        def load(self, actual, **_):
            assert actual == item
            return {'manifest': json.loads(path.read_bytes())}
    projector = modules.projection.Projections(queue, observer, modules.runtime, stages, results,
        binding_factory=lambda _: Binding(), process_alive=lambda _pid, _start: True)
    return SimpleNamespace(item=item, control=control, path=path, digest=digest, stages=stages,
        results=results, queue=queue, client=client, observer=observer, projector=projector,
        modules=modules, tmp_path=tmp_path)


def insert_job(sample, state, *, task_state='RUNNING', generation=1, source_llm_generation=1):
    m, item = sample.modules, sample.item
    job_id = m.runtime._job_id(item['task_run_id'])
    task_key = m.runtime.task_key_for_job(job_id)
    with sqlite3.connect(sample.control) as db:
        db.execute('INSERT INTO paperflow_postprocess_stage_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (job_id, *item.values(), item['task_run_id'], generation, sample.digest, task_key, state, None,
             1234 if state == 'executing' else None, 111.25 if state == 'executing' else None, 1, 2,
             source_llm_generation))
    sample.client.stages[job_id] = SimpleNamespace(id=UUID(job_id), flow_run_id=None, task_key=task_key,
        dynamic_key='paperflow-postprocess-' + job_id, tags=[m.runtime.TAG], state=State(task_state))
    return job_id


def save_pass(sample, *, held=False):
    item = sample.item
    result_id = sample.results.reserve(item, 1, sample.digest)
    claim = sample.stages.claim(item, 1, sample.digest)
    artifact = sample.tmp_path / 'synthetic-output.json'
    artifact.write_text('{"test":true}', encoding='utf8')
    if held:
        with sqlite3.connect(sample.control) as db:
            db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (item['task_id'],))
    receipt = dict(schema='paperflow-postprocessing-receipt/1', identity=item, manifest_sha256=sample.digest,
        claim_id=claim['claim_id'], validation_status='passed',
        body=dict(package_path=str(sample.tmp_path / 'publication'), manifest_sha256='d' * 64),
        attachments=[], artifacts=[dict(path=str(artifact.resolve()), sha256=hashlib.sha256(artifact.read_bytes()).hexdigest())])
    sample.stages.complete(item, claim['claim_id'], receipt, validate_receipt=lambda _: True)
    sample.results.record_pass(item, result_id, claim['claim_id'], validate_receipt=lambda _: True)
    return artifact


def test_unsubmitted_retained_folder_is_waiting(sample):
    stage = sample.projector.stage_projection(sample.queue.snapshot())
    assert stage['available'] is True and stage['basis'] == 'folder-stage-receipts/1'
    assert stage['counts']['waiting_postprocessing'] == 1
    assert stage['counts']['postprocessing_in_progress'] == 0
    assert stage['counts']['waiting_rag_upload'] == 0


@pytest.mark.parametrize('state', ['reserved', 'submitting', 'submitted'])
def test_reserved_or_submitted_is_not_counted_as_waiting_again(sample, state):
    insert_job(sample, state, task_state='SCHEDULED')
    stage = sample.projector.stage_projection(sample.queue.snapshot())
    assert stage['counts']['waiting_postprocessing'] == 0
    assert stage['counts']['postprocessing_in_progress'] is None
    assert stage['counts']['postprocessing_unresolved'] == 1


@pytest.mark.parametrize('state', ['stopped_before_submit', 'cancel_fenced_reserved',
    'cancel_fenced_submitting', 'cancel_fenced_submitted', 'identity_unknown', 'delivery_unknown'])
def test_stopped_fenced_and_unknown_jobs_are_neither_idle_nor_passed(sample, state):
    insert_job(sample, state, task_state='SCHEDULED')
    stage = sample.projector.stage_projection(sample.queue.snapshot())
    assert stage['available'] is True
    assert stage['counts']['waiting_postprocessing'] == 0
    assert stage['counts']['postprocessing_in_progress'] is None
    assert stage['counts']['waiting_rag_upload'] == 0
    assert stage['counts']['postprocessing_unresolved'] == 1


def test_running_requires_exact_prefect_task_and_live_process(sample):
    insert_job(sample, 'executing')
    stage = sample.projector.stage_projection(sample.queue.snapshot())
    assert stage['counts']['postprocessing_in_progress'] == 1
    sample.projector.process_alive = lambda *_: None
    stage = sample.projector.stage_projection(sample.queue.snapshot())
    assert stage['counts']['postprocessing_in_progress'] is None
    assert stage['counts']['postprocessing_unresolved'] == 1


def test_running_after_resume_uses_frozen_llm_generation(sample):
    insert_job(sample, 'executing', generation=2, source_llm_generation=1)
    with sqlite3.connect(sample.control) as db:
        db.execute('UPDATE tasks SET start_generation=2 WHERE task_id=?', (sample.item['task_id'],))
    stage = sample.projector.stage_projection(sample.queue.snapshot())
    assert stage['available'] is True
    assert stage['counts']['postprocessing_in_progress'] == 1


@pytest.mark.parametrize('generation,source', [(2, 2), (0, 1), (1, 0), (1, None), (1, 'invalid')])
def test_invalid_frozen_llm_generation_is_unknown(sample, generation, source):
    insert_job(sample, 'executing', generation=generation, source_llm_generation=source)
    assert sample.projector.stage_projection(sample.queue.snapshot())['available'] is False


def test_bad_stage_task_key_is_not_accepted(sample):
    job_id = insert_job(sample, 'executing')
    sample.client.stages[job_id].task_key = sample.modules.runtime.TASK_KEY
    stage = sample.projector.stage_projection(sample.queue.snapshot())
    assert stage['available'] is False


@pytest.mark.parametrize('field,value', [
    ('dynamic_key', 'wrong'), ('tags', ['wrong']), ('flow_run_id', uuid4()),
])
def test_unrelated_prefect_task_cannot_be_used_for_processing(sample, field, value):
    job_id = insert_job(sample, 'executing')
    setattr(sample.client.stages[job_id], field, value)
    assert sample.projector.stage_projection(sample.queue.snapshot())['available'] is False


def test_only_verified_passed_receipt_waits_for_rag_and_titles_are_reused(sample):
    save_pass(sample)
    insert_job(sample, 'finished', task_state='COMPLETED')
    stage = sample.projector.stage_projection(sample.queue.snapshot())
    assert stage['counts']['waiting_rag_upload'] == 1
    value = sample.projector.results_projection('passed', 1, 20)
    assert value['counts']['passed'] == 1
    assert value['list']['items'][0]['paper_title'] == '合成论文题目'
    assert sample.observer.title_calls == 1


def test_receipt_held_is_never_counted_as_waiting_rag(sample):
    save_pass(sample, held=True)
    insert_job(sample, 'finished', task_state='COMPLETED')
    stage = sample.projector.stage_projection(sample.queue.snapshot())
    assert stage['counts']['waiting_rag_upload'] == 0
    assert stage['counts']['postprocessing_held'] == 1


def test_changed_receipt_file_invalidates_both_projections(sample):
    artifact = save_pass(sample)
    insert_job(sample, 'finished', task_state='COMPLETED')
    artifact.write_text('{"changed":true}', encoding='utf8')
    assert sample.projector.stage_projection(sample.queue.snapshot())['available'] is False
    assert sample.projector.results_projection('passed', 1, 20)['available'] is False


def test_control_change_during_prefect_observation_invalidates_snapshot(sample):
    def mutate():
        with sqlite3.connect(sample.control) as db:
            db.execute("UPDATE tasks SET name='changed' WHERE task_id=?", (sample.item['task_id'],))
    sample.client.during_read = mutate
    assert sample.projector.stage_projection(sample.queue.snapshot())['available'] is False


def test_unrelated_queue_heartbeat_does_not_hide_verified_stages(sample):
    def heartbeat():
        with sqlite3.connect(sample.queue.path) as db:
            db.execute("UPDATE metadata SET value='heartbeat-2' WHERE key='receiver_heartbeat_at'")
    sample.client.during_read = heartbeat
    assert sample.projector.stage_projection(sample.queue.snapshot())['counts']['waiting_postprocessing'] == 1


def test_queue_business_change_during_prefect_observation_is_unknown(sample):
    def mutate():
        with sqlite3.connect(sample.queue.path) as db:
            db.execute("INSERT INTO groups VALUES(?,?,?,?,?,?)",
                       (sample.item['task_id'], sample.item['group_id'], sample.item['paper_id'],
                        sample.item['receipt_id'], 'needs_review', 'receiver-test'))
    sample.client.during_read = mutate
    assert sample.projector.stage_projection(sample.queue.snapshot())['available'] is False


@pytest.mark.parametrize('field,new_value,available', [
    ('updated_at', 'later', True), ('state', 'invalid', False), ('checked_session', 'different', False),
    ('retention_id', 'different', False), ('last_error', 'different', False),
])
def test_retention_rechecks_ignore_only_update_time(sample, field, new_value, available):
    with sqlite3.connect(sample.queue.path) as db:
        db.execute('INSERT INTO retention_requests VALUES(?,?,?,?,?,?,?,?,?,?)',
                   (sample.item['task_id'], sample.item['group_id'], sample.item['paper_id'],
                    sample.item['receipt_id'], 'local-intake', 'accepted', 'retention', None, 'session', 'early'))
    def mutate():
        with sqlite3.connect(sample.queue.path) as db:
            db.execute('UPDATE retention_requests SET ' + field + '=?', (new_value,))
    sample.client.during_read = mutate
    assert sample.projector.stage_projection(sample.queue.snapshot())['available'] is available


def test_source_or_handoff_cannot_be_verified_returns_unknown(sample):
    sample.observer.source_ok = False
    assert sample.projector.stage_projection(sample.queue.snapshot())['available'] is False
    sample.observer.source_ok = True
    sample.queue.unavailable_count = 1
    assert sample.projector.stage_projection(sample.queue.snapshot())['available'] is False
    sample.queue.unavailable_count = 0
    sample.path.write_text('{}', encoding='utf8')
    assert sample.projector.results_projection('passed', 1, 20)['available'] is False


@pytest.mark.parametrize('change', ['new_completed_scan', 'failed_scan'])
def test_scan_change_or_failure_during_full_verification_is_unknown(sample, change):
    def mutate():
        if change == 'new_completed_scan':
            sample.queue.snap['upstream_scan_completed_at'] = '2026-11-01T00:00:01Z'
        else:
            sample.queue.snap.update(waiting_available=False, upstream_scan_verified=False)
    sample.client.during_read = mutate
    assert sample.projector.stage_projection(sample.queue.snapshot())['available'] is False


@pytest.mark.parametrize('change', ['parent', 'folder', 'order'])
def test_unrelated_parent_folder_or_local_order_never_becomes_waiting(sample, change):
    if change == 'parent':
        sample.client.flows[sample.item['parent_run_id']].idempotency_key = 'wrong'
    elif change == 'folder':
        sample.client.flows[sample.item['task_run_id']].work_queue_name = 'wrong'
    else:
        with sqlite3.connect(sample.queue.path) as db:
            db.execute("UPDATE preprocessing_folder_orders SET receipt_id='wrong'")
    assert sample.projector.stage_projection(sample.queue.snapshot())['available'] is False


def test_reserved_job_cannot_publish_a_passed_result(sample):
    save_pass(sample)
    insert_job(sample, 'reserved', task_state='SCHEDULED')
    assert sample.projector.results_projection('passed', 1, 20)['available'] is False


def test_prefect_stage_transition_during_observation_is_unknown(sample):
    job = insert_job(sample, 'executing')
    def mutate(number):
        if number == 2:
            sample.client.stages[job].state = State('FAILED')
    sample.client.during_task_read = mutate
    assert sample.projector.stage_projection(sample.queue.snapshot())['available'] is False


def test_process_liveness_is_rechecked_after_receipt_observation(sample):
    insert_job(sample, 'executing')
    checks = []
    def process(_pid, _started):
        checks.append(True)
        return len(checks) == 1
    sample.projector.process_alive = process
    stage = sample.projector.stage_projection(sample.queue.snapshot())
    assert stage['counts']['postprocessing_in_progress'] is None
    assert stage['counts']['postprocessing_unresolved'] == 1


def enable_cache_witness(sample):
    record = {**sample.item, 'receipt_path': str(sample.path), 'delivery_dir': str(sample.tmp_path)}
    with sqlite3.connect(sample.queue.path) as db:
        db.execute('ALTER TABLE groups ADD COLUMN record TEXT')
        db.execute('INSERT INTO groups VALUES(?,?,?,?,?,?,?)',
                   (sample.item['task_id'], sample.item['group_id'], sample.item['paper_id'],
                    sample.item['receipt_id'], 'ready_for_llm', 'receiver-test', json.dumps(record)))
    def delivery_fingerprint(_record, _root):
        stat = sample.path.stat()
        return (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
    sample.projector.delivery_fingerprint = delivery_fingerprint


def test_final_successful_scan_can_rebind_full_witness(sample, monkeypatch):
    enable_cache_witness(sample)
    clock = [0.0]
    monkeypatch.setattr(sample.modules.projection, '_monotonic', lambda: clock[0])
    old_snapshot = sample.queue.snapshot()
    def scan():
        clock[0] = 30.0
        sample.queue.snap.update(waiting_available=False, upstream_scan_verified=False)
        clock[0] = 35.0
        sample.queue.snap.update(waiting_available=True, upstream_scan_verified=True,
                                 upstream_scan_completed_at='2026-11-01T00:00:01Z')
    sample.client.during_read = scan
    stage = sample.projector.stage_projection(old_snapshot)
    assert stage['available'] is True
    assert stage._cache_witness.source == sample.modules.projection._source_key(sample.queue.snapshot())
    assert stage._cache_witness.full_observed_at == 0.0
    assert stage._cache_witness.remote_observed_at == 35.0


@pytest.mark.parametrize('change', ['receiver_session_id', 'scan_source_instance', 'waiting_llm_total'])
def test_cross_scan_does_not_rebind_changed_identity_or_population(sample, change):
    enable_cache_witness(sample)
    old_snapshot = sample.queue.snapshot()
    def scan():
        sample.queue.snap['upstream_scan_completed_at'] = '2026-11-01T00:00:01Z'
        sample.queue.snap[change] = 2 if change == 'waiting_llm_total' else 'changed'
    sample.client.during_read = scan
    assert sample.projector.stage_projection(old_snapshot)['available'] is False


def test_cross_scan_rejects_another_scan_during_final_check(sample):
    enable_cache_witness(sample)
    old_snapshot = sample.queue.snapshot()
    calls = []
    original = sample.queue.snapshot
    def snapshot(**kwargs):
        calls.append(True)
        if len(calls) == 2:
            sample.queue.snap['upstream_scan_completed_at'] = '2026-11-01T00:00:02Z'
        return original(**kwargs)
    sample.queue.snapshot = snapshot
    sample.client.during_read = lambda: sample.queue.snap.update(
        upstream_scan_completed_at='2026-11-01T00:00:01Z')
    assert sample.projector.stage_projection(old_snapshot)['available'] is False
    assert len(calls) >= 2


def test_final_prefect_observation_must_start_after_final_successful_scan(sample):
    enable_cache_witness(sample)
    old_snapshot = sample.queue.snapshot()
    original = sample.projector._prefect
    calls = []
    def prefect(*args):
        calls.append(True)
        if len(calls) == 2:
            sample.queue.snap['upstream_scan_completed_at'] = '2026-11-01T00:00:01Z'
        return original(*args)
    sample.projector._prefect = prefect
    assert sample.projector.stage_projection(old_snapshot)['available'] is False
    assert len(calls) == 2


def test_http_rebinds_across_scan_even_when_incomplete_was_observed(sample, monkeypatch):
    from fastapi.testclient import TestClient
    enable_cache_witness(sample)
    api = load(ROOT / 'scan_rebind_next' / 'waiting_api.py', 'cross_scan_end_to_end_api')
    clock = [0.0]
    monkeypatch.setattr(sample.modules.projection, '_monotonic', lambda: clock[0])
    monkeypatch.setattr(api, 'projection_clock', lambda: clock[0])
    monkeypatch.setitem(sys.modules, 'llm_queue_summary', SimpleNamespace(summarize_llm=lambda *_: {}))
    monkeypatch.setattr(sample.observer, 'summary', lambda _: dict(source='prefect', scope='all_tasks',
        available=True, counts={}), raising=False)
    monkeypatch.setattr(sample.observer, 'preprocessing_results', lambda *_: {'available': True}, raising=False)
    monkeypatch.setattr(sample.observer, 'llm_results', lambda *_: {'available': True}, raising=False)
    started, release, later_release = Event(), Event(), Event()
    observes = []
    original = sample.projector._observe
    def observe(*args, **kwargs):
        observes.append(True)
        if len(observes) > 1:
            assert later_release.wait(6)
        return original(*args, **kwargs)
    sample.projector._observe = observe
    def block_initial_prefect():
        started.set()
        assert release.wait(6)
    sample.client.during_read = block_initial_prefect
    app = api.create_app(sample.queue, ticket_observer=sample.observer,
                         stage_projection=sample.projector.stage_projection)
    endpoint = '/api/v1/local-intake/preprocessing/summary'
    with TestClient(app) as client:
        assert client.get(endpoint).json()['prefect_ticket_summary'].get('postprocessing_basis') is None
        assert started.wait(4)
        clock[0] = 30.0
        sample.queue.snap.update(waiting_available=False, upstream_scan_verified=False)
        assert client.get(endpoint).json()['prefect_ticket_summary'].get('postprocessing_basis') is None
        clock[0] = 35.0
        sample.queue.snap.update(waiting_available=True, upstream_scan_verified=True,
                                 upstream_scan_completed_at='2026-11-01T00:00:01Z')
        assert client.get(endpoint).json()['prefect_ticket_summary'].get('postprocessing_basis') is None
        release.set()
        until = time.monotonic() + 2
        while time.monotonic() < until:
            response = client.get(endpoint).json()['prefect_ticket_summary']
            if response.get('postprocessing_basis'):
                break
            time.sleep(.01)
        else:
            pytest.fail('final successful source was never rebound')
        assert response['counts']['waiting_postprocessing'] == 1
        later_release.set()


def test_full_witness_records_start_of_last_prefect_read(sample, monkeypatch):
    enable_cache_witness(sample)
    clock = [100.0]
    monkeypatch.setattr(sample.modules.projection, '_monotonic', lambda: clock[0], raising=False)
    original = sample.projector._prefect
    calls = []
    def read(*args):
        calls.append(True)
        if len(calls) == 1:
            clock[0] = 101.0
        else:
            clock[0] = 110.0
        return original(*args)
    sample.projector._prefect = read
    result = sample.projector.stage_projection(sample.queue.snapshot())
    assert result['available'] is True
    assert result._cache_witness.full_observed_at == 100.0
    assert result._cache_witness.remote_observed_at == 101.0


def test_stage_and_unfiltered_results_share_one_inflight_full_verification(sample):
    original = sample.projector._observe
    started, release = Event(), Event()
    calls, values = [], []
    def observe(*args, **kwargs):
        calls.append(True)
        started.set()
        assert release.wait(3)
        return original(*args, **kwargs)
    sample.projector._observe = observe
    stage = Thread(target=lambda: values.append(sample.projector.stage_projection(sample.queue.snapshot())))
    results = Thread(target=lambda: values.append(sample.projector.results_projection()))
    stage.start()
    assert started.wait(1)
    results.start()
    time.sleep(.1)
    release.set()
    stage.join(3)
    results.join(3)
    assert not stage.is_alive() and not results.is_alive()
    assert len(calls) == 1
    assert len(values) == 2 and all(value['available'] is True for value in values)


@pytest.mark.parametrize('change', ['control', 'queue', 'file', 'pid', 'prefect'])
def test_cache_witness_rejects_changed_business_file_process_or_prefect(sample, change):
    enable_cache_witness(sample)
    job = insert_job(sample, 'executing')
    result = sample.projector.stage_projection(sample.queue.snapshot())
    witness = result._cache_witness
    assert witness.local(sample.queue.snapshot()) is True
    assert witness.remote() is True
    if change == 'control':
        with sqlite3.connect(sample.control) as db:
            db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='failed'")
    elif change == 'queue':
        with sqlite3.connect(sample.queue.path) as db:
            db.execute("UPDATE groups SET state='needs_review'")
    elif change == 'file':
        sample.path.write_text('{"changed":true}', encoding='utf8')
    elif change == 'pid':
        sample.projector.process_alive = lambda *_: None
    else:
        sample.client.stages[job].state = State('FAILED')
    if change == 'prefect':
        assert witness.remote() is False
    else:
        assert witness.local(sample.queue.snapshot()) is False


def test_output_file_changing_during_second_prefect_read_is_unknown(sample):
    artifact = save_pass(sample)
    insert_job(sample, 'finished', task_state='COMPLETED')
    def mutate(number):
        if number == 2:
            artifact.write_text('{"changed":true}', encoding='utf8')
    sample.client.during_task_read = mutate
    assert sample.projector.stage_projection(sample.queue.snapshot())['available'] is False


def test_build_projections_uses_real_read_only_adapters_without_initializing_schema(modules, tmp_path, monkeypatch):
    live_root = PROJECT / 'candidates' / 'local-intake-live-link-20261031'
    monkeypatch.syspath_prepend(str(live_root))
    from local_queue import LocalQueue
    queue = LocalQueue(tmp_path / 'queue.sqlite3')
    control = tmp_path / 'control.sqlite3'
    with sqlite3.connect(control) as db:
        db.execute('CREATE TABLE sentinel(value INTEGER)')
    observer = SimpleNamespace(queue=queue, index=control, api_url='http://synthetic-prefect/api',
                               receipt_root=tmp_path)
    before = (hashlib.sha256(control.read_bytes()).hexdigest(), hashlib.sha256(queue.path.read_bytes()).hexdigest())
    stage, results = modules.projection.build_projections(queue, observer)
    after = (hashlib.sha256(control.read_bytes()).hexdigest(), hashlib.sha256(queue.path.read_bytes()).hexdigest())
    assert callable(stage) and callable(results) and before == after


def test_actual_upstream_binding_checks_a_synthetic_retained_handoff(sample):
    item, queue = sample.item, sample.queue
    def ref(path, value):
        raw = json.dumps(value, ensure_ascii=False).encode('utf8')
        path.write_bytes(raw)
        return dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest())
    body = dict(file_id='body-test', sha256='f' * 64)
    paper = dict(paper_id=item['paper_id'], revision='revision-test', body=body, attachments=[])
    receipt = ref(sample.tmp_path / 'synthetic-receipt.json', {'receipt': 'synthetic'})
    material = ref(sample.tmp_path / 'material.json', {'source': {'source_sha256': body['sha256']}})
    reading = ref(sample.tmp_path / 'reading.json', {'source': {'source_sha256': body['sha256']}, 'blocks': []})
    manifest = dict(schema='paperflow-local-preprocessing-handoff/1', consumer_id='prefect-local-preprocessing',
        **{key: item[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
        revision=paper['revision'], body=body, attachments=[], receipt=receipt, material=material,
        reading_material=reading, body_work=str(sample.tmp_path), body_text=None, index=None, gaps=[])
    raw = json.dumps(manifest, ensure_ascii=False).encode('utf8')
    sample.path.write_bytes(raw)
    queue.reference['manifest_sha256'] = hashlib.sha256(raw).hexdigest()
    digest = hashlib.sha256(json.dumps(manifest, ensure_ascii=False, sort_keys=True,
                            separators=(',', ':'), allow_nan=False).encode('utf8')).hexdigest()
    with sqlite3.connect(sample.control) as db:
        db.execute('UPDATE llm_folder_queue SET manifest_sha256=?', (digest,))
    with sqlite3.connect(queue.path) as db:
        db.execute('INSERT INTO metadata VALUES(?,?)', ('queue_schema', 'paperflow-local-waiting/1'))
        db.execute('INSERT INTO groups VALUES(?,?,?,?,?,?)', (item['task_id'], item['group_id'], item['paper_id'],
                   item['receipt_id'], 'ready_for_llm', 'receiver-test'))
        db.execute('INSERT INTO claims VALUES(?,?,?,?,?,?)', ('prefect-local-preprocessing', item['attempt_id'],
                   item['task_id'], item['group_id'], item['receipt_id'], 'completed'))
        db.execute('INSERT INTO preprocessing_handoffs VALUES(?,?,?,?,?,?,?,?)',
                   (item['task_id'], item['group_id'], item['paper_id'], item['receipt_id'],
                    'prefect-local-preprocessing', item['attempt_id'], 'waiting', json.dumps(queue.reference)))
        db.execute('INSERT INTO retention_requests VALUES(?,?,?,?,?,?,?,?,?,?)',
                   (item['task_id'], item['group_id'], item['paper_id'], item['receipt_id'],
                    'local-intake', 'accepted', 'retained', None, 'receiver-test', 'early'))
    package = PROJECT / 'prefect-paperflow' / 'paperflow_prefect'
    sample.modules.projection._load_module('_synthetic_upstream_package', package / '__init__.py', package_dir=package)
    upstream = importlib.import_module('_synthetic_upstream_package.llm_queue.upstream_flow_binding')
    verified = {**item, 'receipt_path': receipt['path'], 'receipt_sha256': receipt['sha256'], 'paper': paper}
    sample.projector.binding_factory = lambda client: upstream.UpstreamFlowBinding(
        queue.path, sample.control, client, sample.observer.api_url, queue.consumer_id,
        lambda *_: verified, receiver_session=lambda _: 'receiver-test')
    assert sample.projector.stage_projection(queue.snapshot())['counts']['waiting_postprocessing'] == 1
    Path(reading['path']).write_text('{}', encoding='utf8')
    assert sample.projector.stage_projection(queue.snapshot())['available'] is False


def test_projectors_connect_to_candidate_api_without_dispatch(sample, monkeypatch):
    from fastapi.testclient import TestClient
    api = load(ROOT / 'scan_rebind_next' / 'waiting_api.py', 'projection_end_to_end_api')
    monkeypatch.setitem(sys.modules, 'llm_queue_summary', SimpleNamespace(summarize_llm=lambda *_: dict(
        waiting_llm=0, llm_body_in_progress=0, llm_attachment_in_progress=0, waiting_llm_partial=False,
        llm_body_in_progress_partial=False, llm_attachment_in_progress_partial=False,
        as_of='2026-11-01T00:00:00Z')))
    monkeypatch.setattr(sample.observer, 'summary', lambda _: dict(source='prefect', scope='all_tasks',
        available=True, counts={}), raising=False)
    monkeypatch.setattr(sample.observer, 'preprocessing_results', lambda *_: {'available': True}, raising=False)
    monkeypatch.setattr(sample.observer, 'llm_results', lambda *_: {'available': True}, raising=False)
    app = api.create_app(sample.queue, ticket_observer=sample.observer,
        stage_projection=sample.projector.stage_projection, results_projection=sample.projector.results_projection)
    with TestClient(app) as client:
        until = time.monotonic() + 5
        while time.monotonic() < until:
            response = client.get('/api/v1/local-intake/preprocessing/summary').json()
            if (response['prefect_ticket_summary'].get('postprocessing_basis') == 'folder-stage-receipts/1'
                    and response['postprocessing_results']['available'] is True):
                break
            time.sleep(.02)
        else:
            pytest.fail('synthetic API projection did not become available')
    assert response['prefect_ticket_summary']['counts']['waiting_postprocessing'] == 1
    assert response['prefect_ticket_summary']['counts']['waiting_rag_upload'] == 0
    assert response['postprocessing_results']['counts']['passed'] == 0
