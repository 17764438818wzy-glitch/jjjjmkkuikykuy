"""Exercise copied current control code using synthetic SQLite/Python objects."""
from contextlib import closing, nullcontext
import importlib
import importlib.util
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import threading
import time
from types import ModuleType, SimpleNamespace
from uuid import uuid4

from filelock import FileLock
import pytest


HERE = Path(__file__).resolve().parents[2]
ROOT = HERE.parents[1]
LEGACY = ROOT / 'candidates/first-step-control-20261029'
NAME = '_postprocess_control_next_test'
pkg = ModuleType(NAME)
pkg.__path__ = [str(HERE / 'control_next'), str(LEGACY / 'paperflow_prefect_v2'),
                str(LEGACY / 'paperflow_prefect')]
pkg.__package__ = NAME
sys.modules[NAME] = pkg
control_module = importlib.import_module(NAME + '.first_step_control')
delete_module = importlib.import_module(NAME + '.first_step_delete')
counts_module = importlib.import_module(NAME + '.llm_admission_state')


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


stage = load('_postprocess_control_stage_test', HERE / 'folder_state.py')
runtime = load('_postprocess_control_runtime_test', HERE / 'generation_next/stage_runtime.py')
results = load('_postprocess_control_results_test', HERE / 'results_next/postprocessing_results.py')
scheduler_module = importlib.import_module(NAME + '.first_step_scheduler')


@pytest.fixture
def setup(tmp_path):
    ctl = control_module.FirstStepControl(tmp_path / 'output', tmp_path / 'data')
    task_id = ctl.create_task('synthetic task')['task_id']
    parent = str(uuid4())
    with closing(ctl.summary._connect()) as db, db:
        db.execute("UPDATE tasks SET desired_state='running',task_status='complete',start_generation=7,"
                   "prefect_run_id=?,start_pending=0 WHERE task_id=?", (parent, task_id))
    stage.FolderStageStore(ctl.summary.path).initialize()
    # Explicitly construct the pre-migration schema; runtime_next can be merged
    # during deployment and is not a stable source for an old-schema fixture.
    with closing(ctl.summary._connect()) as db, db:
        db.execute('''CREATE TABLE paperflow_postprocess_stage_jobs (
            job_id TEXT PRIMARY KEY,task_id TEXT NOT NULL,group_id TEXT NOT NULL,paper_id TEXT NOT NULL,
            receipt_id TEXT NOT NULL,attempt_id TEXT NOT NULL,parent_run_id TEXT NOT NULL,
            task_run_id TEXT NOT NULL UNIQUE,folder_run_id TEXT NOT NULL,generation INTEGER NOT NULL,
            manifest_sha256 TEXT NOT NULL,task_key TEXT NOT NULL,state TEXT NOT NULL,error_type TEXT,
            owner_pid INTEGER,owner_started REAL,created_at_ns INTEGER NOT NULL,updated_at_ns INTEGER NOT NULL,
            UNIQUE(task_id,group_id))''')
    runner = runtime.Coordinator(ctl.summary.path, task_gate=lambda *_: nullcontext(),
        folder_gate=lambda *_: nullcontext(), postprocess_is_open=lambda: False, rag_is_held=lambda: True,
        verify_upstream=lambda *_: {}, verify_execution=lambda *_: True, slot=lambda: None,
        process_folder=lambda *_: None, results=SimpleNamespace(reserve=lambda *_: None,
            success=lambda *_: None, failure=lambda *_: None, unknown=lambda *_: None))
    # Exercise migration of the real prior schema: its added column is nullable,
    # so invalid/null evidence must be handled conservatively by control reads.
    runner.initialize()
    results.ResultsStore(ctl.summary.path, admission_gate=lambda *_: nullcontext(),
                         inspect_receipt=stage.FolderStageStore._inspect_receipt).initialize()
    return ctl, task_id, parent, tmp_path


def add_job(ctl, task_id, parent, state, *, claim_state=None):
    group, folder, claim_id = uuid4().hex, str(uuid4()), str(uuid4())
    job_id = runtime._job_id(folder)
    with closing(ctl.summary._connect()) as db, db:
        db.execute('INSERT INTO paperflow_postprocess_stage_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (job_id, task_id, group, 'paper', 'a' * 64, 'attempt-' + group, parent, folder, folder,
             7, 'b' * 64, runtime.task_key_for_job(job_id), state, None, None, None, 1, 1, 5, None, None))
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?,?)',
            (task_id, group, 'paper', 'a' * 64, 'attempt-' + group, parent, folder, 'b' * 64,
             'waiting_postprocessing', 5, 1))
        if claim_state:
            db.execute('INSERT INTO postprocessing_claims VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (task_id, group, 'paper', 'a' * 64, 'attempt-' + group, parent, folder, 'b' * 64,
                 claim_id, 7, claim_state, None, 1, 1))
    return job_id, group


def job_state(ctl, job_id):
    with closing(ctl.summary._connect()) as db:
        return db.execute('SELECT state FROM paperflow_postprocess_stage_jobs WHERE job_id=?', (job_id,)).fetchone()[0]


@pytest.mark.parametrize('job,expected', [('submission_unknown', 'unknown'), ('execution_unknown', 'unknown')])
def test_unknown_execution_never_reports_paused(setup, job, expected):
    ctl, task_id, parent, _ = setup
    add_job(ctl, task_id, parent, job)
    result = ctl._pause(task_id)
    assert result['pause_status'] == expected
    assert ctl.summary.read(task_id, None, 1, 20)['pause_status'] == expected
    with pytest.raises(control_module.ControlError):
        delete_module._wait_first_step(ctl, task_id)


def test_reserved_job_is_fenced_in_same_pause_transaction(setup):
    ctl, task_id, parent, _ = setup
    job, _ = add_job(ctl, task_id, parent, 'reserved')
    result = ctl._pause(task_id)
    assert result['pause_status'] == 'paused'
    assert job_state(ctl, job) == 'stopped_before_submit'


@pytest.mark.parametrize('bad', [None, 0, -1, 8, 'bad'])
def test_invalid_frozen_source_generation_is_unknown(setup, bad):
    ctl, task_id, parent, _ = setup
    job, _ = add_job(ctl, task_id, parent, 'stopped_before_submit')
    with closing(ctl.summary._connect()) as db, db:
        db.execute('UPDATE paperflow_postprocess_stage_jobs SET source_llm_generation=? WHERE job_id=?', (bad, job))
    assert ctl._pause(task_id)['pause_status'] == 'unknown'


def test_changed_upstream_source_generation_is_unknown(setup):
    ctl, task_id, parent, _ = setup
    _, group = add_job(ctl, task_id, parent, 'stopped_before_submit')
    with closing(ctl.summary._connect()) as db, db:
        db.execute('UPDATE llm_folder_queue SET generation=6 WHERE task_id=? AND group_id=?', (task_id, group))
    assert ctl._pause(task_id)['pause_status'] == 'unknown'


def test_missing_source_generation_schema_is_unknown_even_if_empty(setup):
    ctl, task_id, _, _ = setup
    with closing(ctl.summary._connect()) as db, db:
        db.execute('ALTER TABLE paperflow_postprocess_stage_jobs DROP COLUMN source_llm_generation')
    result = ctl._pause(task_id)
    assert result['pause_status'] == 'unknown' and result['unknown_requests'] is None


@pytest.mark.parametrize('state', ['submitting', 'submitted'])
def test_possibly_submitted_job_is_fenced_but_remains_unknown(setup, state):
    ctl, task_id, parent, _ = setup
    job, _ = add_job(ctl, task_id, parent, state)
    result = ctl._pause(task_id)
    assert job_state(ctl, job) == 'cancel_fenced_' + state
    assert result['pause_status'] == 'unknown'


def test_job_and_claim_of_same_running_folder_count_once(setup):
    ctl, task_id, parent, _ = setup
    add_job(ctl, task_id, parent, 'executing', claim_state='processing')
    result = ctl._pause(task_id)
    assert result['pause_status'] == 'pausing' and result['inflight_requests'] == 1
    summary = ctl.summary.read(task_id, None, 1, 20)
    assert summary['pause_status'] == 'pausing' and summary['inflight_requests'] == 1


@pytest.mark.parametrize('name', ['paperflow_postprocess_stage_jobs', 'postprocessing_claims',
                                  'postprocessing_results', 'llm_request_admissions'])
def test_missing_stage_evidence_preserves_pause_but_never_reports_stopped(setup, name):
    ctl, task_id, _, _ = setup
    with closing(ctl.summary._connect()) as db, db:
        db.execute('DROP TABLE ' + name)
    result = ctl._pause(task_id)
    assert result['desired_state'] == 'paused' and result['pause_status'] == 'unknown'
    assert result['unknown_requests'] is None and result['inflight_requests'] is None
    summary = ctl.summary.read(task_id, None, 1, 20)
    assert summary['desired_state'] == 'paused' and summary['pause_status'] == 'unknown'
    assert summary['inflight_requests'] is None and summary['unknown_requests'] is None
    with pytest.raises((ValueError, sqlite3.Error, control_module.ControlError)):
        delete_module._wait_first_step(ctl, task_id)


def test_postprocessing_unknown_blocks_next_generation(setup):
    ctl, task_id, parent, _ = setup
    add_job(ctl, task_id, parent, 'execution_unknown')
    with closing(ctl.summary._connect()) as db:
        revision = ctl._task(db, task_id)['control_revision']
    with pytest.raises(control_module.ControlError):
        ctl._start(task_id, revision)


def test_mismatched_folder_identity_never_reports_stopped(setup):
    ctl, task_id, parent, _ = setup
    job, _ = add_job(ctl, task_id, parent, 'stopped_before_submit')
    with closing(ctl.summary._connect()) as db, db:
        db.execute('UPDATE paperflow_postprocess_stage_jobs SET folder_run_id=? WHERE job_id=?', (str(uuid4()), job))
    assert ctl._pause(task_id)['pause_status'] == 'unknown'


def test_forged_job_key_never_reports_stopped(setup):
    ctl, task_id, parent, _ = setup
    job, _ = add_job(ctl, task_id, parent, 'stopped_before_submit')
    with closing(ctl.summary._connect()) as db, db:
        db.execute('UPDATE paperflow_postprocess_stage_jobs SET task_key=? WHERE job_id=?', ('forged', job))
    assert ctl._pause(task_id)['pause_status'] == 'unknown'


def test_noncanonical_folder_identity_never_reports_stopped(setup):
    ctl, task_id, parent, _ = setup
    job, _ = add_job(ctl, task_id, parent, 'stopped_before_submit')
    with closing(ctl.summary._connect()) as db, db:
        old = db.execute('SELECT folder_run_id FROM paperflow_postprocess_stage_jobs WHERE job_id=?', (job,)).fetchone()[0]
        db.execute('UPDATE paperflow_postprocess_stage_jobs SET folder_run_id=?,task_run_id=? WHERE job_id=?',
                   (old.replace('-', ''), old.replace('-', ''), job))
    assert ctl._pause(task_id)['pause_status'] == 'unknown'


def test_control_task_key_matches_runtime_and_rejects_noncanonical_job_id():
    from uuid import UUID
    job = str(uuid4())
    assert counts_module.postprocess_task_key(job) == runtime.task_key_for_job(job)
    with pytest.raises(ValueError):
        counts_module.postprocess_task_key(UUID(job).hex)


def test_finished_job_is_safe_only_with_matching_accepted_stage_receipt(setup):
    ctl, task_id, parent, _ = setup
    job_id, group = add_job(ctl, task_id, parent, 'finished', claim_state='waiting_rag')
    assert ctl._pause(task_id)['pause_status'] == 'unknown'
    with closing(ctl.summary._connect()) as db, db:
        job = db.execute('SELECT * FROM paperflow_postprocess_stage_jobs WHERE job_id=?', (job_id,)).fetchone()
        claim = db.execute('SELECT * FROM postprocessing_claims WHERE task_id=? AND group_id=?',
                           (task_id, group)).fetchone()
        fields = ('task_id','group_id','paper_id','receipt_id','attempt_id','parent_run_id','task_run_id')
        receipt = dict(schema='paperflow-postprocessing-receipt/1', validation_status='passed',
                       identity={k: job[k] for k in fields}, claim_id=claim['claim_id'], manifest_sha256='b' * 64)
        encoded = json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        digest = hashlib.sha256(encoded.encode('utf8')).hexdigest()
        db.execute('INSERT INTO postprocessing_receipts VALUES(?,?,?,?)', (claim['claim_id'], encoded, digest, 1))
        db.execute('INSERT INTO postprocessing_results VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (*(job[k] for k in fields), 'b' * 64, 7, str(uuid4()), 'passed', 'passed',
             claim['claim_id'], digest, None, None, 1, 1))
    with closing(ctl.summary._connect()) as db:
        assert counts_module.counts(db, task_id) == {'inflight': 0, 'unknown': 0}
    with closing(ctl.summary._connect()) as db, db:
        db.execute("UPDATE postprocessing_receipts SET receipt_sha256=?", ('c' * 64,))
    with closing(ctl.summary._connect()) as db:
        assert counts_module.counts(db, task_id)['unknown'] == 1


def test_delete_wait_does_not_hold_short_task_gate_while_worker_finishes(setup):
    ctl, task_id, parent, tmp_path = setup
    job_id, _ = add_job(ctl, task_id, parent, 'executing', claim_state='processing')
    folder_lock = FileLock(str(tmp_path / 'folder-order.lock'), thread_local=False)
    holding, release = threading.Event(), threading.Event()
    def worker():
        with folder_lock:
            holding.set()
            assert release.wait(3)
            with ctl._lock('preprocessing', task_id):
                with closing(ctl.summary._connect()) as db, db:
                    db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='failed' WHERE job_id=?", (job_id,))
                    db.execute("UPDATE postprocessing_claims SET state='failed' WHERE task_id=?", (task_id,))
    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert holding.wait(2)
        delete_module._request_delete(ctl, task_id)
        release.set()
        thread.join(timeout=3)
        assert not thread.is_alive(), 'worker could not obtain the short admission gate'
        with pytest.raises(control_module.ControlError):
            delete_module._wait_first_step(ctl, task_id)
    finally:
        release.set()
        thread.join(timeout=3)


@pytest.mark.parametrize('when', ['before_plan_commit', 'during_cleanup'])
def test_delete_rechecks_unknown_postprocessing_before_cleanup_and_removal(setup, monkeypatch, when):
    ctl, task_id, parent, _ = setup
    delete_module._request_delete(ctl, task_id)
    plan = {'paths': [{'synthetic': True}], 'shared_groups': [], 'receipt_records': []}
    monkeypatch.setattr(delete_module, '_check_conversion_processes', lambda *args: None)
    monkeypatch.setattr(delete_module, '_cancel_local_task', lambda *args: None)
    monkeypatch.setattr(delete_module, '_assert_originals_outside_plan', lambda *args: None)
    retention = importlib.import_module(NAME + '.first_step_retention')
    registration = importlib.import_module(NAME + '.first_step_registration')
    monkeypatch.setattr(retention, 'assert_deletion_safe', lambda *args, **kwargs: None)
    monkeypatch.setattr(registration, 'remove_task_receipt_events', lambda *args: None)
    cleaned = []
    def build(*_):
        if when == 'before_plan_commit':
            add_job(ctl, task_id, parent, 'execution_unknown')
        return plan
    def remove(*_):
        cleaned.append(True)
        if when == 'during_cleanup':
            add_job(ctl, task_id, parent, 'execution_unknown')
    monkeypatch.setattr(delete_module, '_build_plan', build)
    monkeypatch.setattr(delete_module, '_remove_path', remove)
    with closing(ctl.summary._connect()) as db:
        with pytest.raises(control_module.ControlError):
            delete_module._delete_locked(ctl, db, task_id)
        assert ctl._task(db, task_id)
    if when == 'before_plan_commit':
        assert not cleaned


def test_deletion_keeps_llm_fence_and_passes_postprocess_jobs_to_scheduler(setup):
    ctl, task_id, parent, tmp_path = setup
    post_id, group = add_job(ctl, task_id, parent, 'reserved')
    with closing(ctl.summary._connect()) as db, db:
        db.execute('CREATE TABLE paperflow_llm_stage_jobs(job_id TEXT,task_id TEXT,group_id TEXT,'
                   'folder_run_id TEXT,receipt_id TEXT,attempt_id TEXT,parent_run_id TEXT,state TEXT,updated_at_ns INTEGER)')
        db.execute('INSERT INTO paperflow_llm_stage_jobs VALUES(?,?,?,?,?,?,?,?,?)',
                   ('llm-job', task_id, group, 'folder', 'a' * 64, 'attempt', parent, 'submitted', 1))
    local = tmp_path / 'local.sqlite3'
    ctl.local_intake_queue_db = local
    with sqlite3.connect(local) as db:
        db.execute('CREATE TABLE preprocessing_folder_orders(task_id TEXT,group_id TEXT,receipt_id TEXT,'
                   'attempt_id TEXT,prefect_run_id TEXT,folder_label TEXT)')
    seen = {}
    def cancel(task, run, orders, *, stage_jobs, postprocess_jobs):
        seen.update(stage_jobs=stage_jobs, postprocess_jobs=postprocess_jobs)
    ctl.scheduler = SimpleNamespace(cancel_task=cancel)
    delete_module._cancel_prefect_orders(ctl, task_id)
    assert seen['stage_jobs'][0]['job_id'] == 'llm-job'
    assert seen['postprocess_jobs'][0]['job_id'] == post_id
    with closing(ctl.summary._connect()) as db:
        assert db.execute('SELECT state FROM paperflow_llm_stage_jobs').fetchone()[0] == 'cancel_fenced_submitted'
        assert db.execute('SELECT state FROM paperflow_postprocess_stage_jobs').fetchone()[0] == 'cancel_fenced_reserved'


@pytest.mark.parametrize('scenario', ['normal', 'wrong_tag', 'wrong_key', 'submission_missing'])
def test_scheduler_cancels_only_identity_checked_llm_and_postprocess_tasks(monkeypatch, scenario):
    from uuid import UUID
    from prefect.exceptions import ObjectNotFound
    task_id, root_id, folder_id = str(uuid4()), str(uuid4()), str(uuid4())
    group, receipt_id, attempt = uuid4().hex, 'a' * 64, 'attempt'
    order = dict(task_id=task_id, group_id=group, receipt_id=receipt_id,
                 attempt_id=attempt, prefect_run_id=folder_id, folder_label='synthetic')
    llm_id, post_id = str(uuid4()), runtime._job_id(folder_id)
    base = dict(task_id=task_id, group_id=group, receipt_id=receipt_id, attempt_id=attempt,
                parent_run_id=root_id, folder_run_id=folder_id, state='submitted')
    llm = dict(base, job_id=llm_id, task_key='paperflow-folder-llm-stage-v1')
    post = dict(base, job_id=post_id, task_run_id=folder_id, task_key=runtime.task_key_for_job(post_id))
    if scenario == 'wrong_key':
        post['task_key'] = runtime.task_key_for_job(str(uuid4()))
    class State:
        def __init__(self, value): self.value = value
        def is_final(self): return self.value == 'final'
        def is_scheduled(self): return self.value == 'scheduled'
        def is_pending(self): return False
        def is_paused(self): return self.value == 'paused'
    root_deploy, folder_deploy = uuid4(), uuid4()
    root = SimpleNamespace(id=UUID(root_id), deployment_id=root_deploy, parameters={'task_id': task_id},
        idempotency_key='first-step-order-v2-' + task_id, parent_task_run_id=None, state=State('paused'))
    folder = SimpleNamespace(id=UUID(folder_id), deployment_id=folder_deploy, parameters={
        k: order[k] for k in ('task_id', 'group_id', 'receipt_id', 'attempt_id')},
        idempotency_key='folder-waiting-v2-' + task_id + '-' + group, parent_task_run_id=None,
        work_queue_name='task-' + task_id, state=State('paused'))
    stages = {UUID(llm_id): SimpleNamespace(id=UUID(llm_id), flow_run_id=None, task_key=llm['task_key'],
        dynamic_key='paperflow-llm-' + llm_id, tags=['paperflow-llm-stage-v1'], state=State('scheduled')),
        UUID(post_id): SimpleNamespace(id=UUID(post_id), flow_run_id=None, task_key=post['task_key'],
        dynamic_key='paperflow-postprocess-' + post_id, tags=[runtime.TAG], state=State('scheduled'))}
    cancelled = []
    class Client:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def read_deployment_by_name(self, name):
            return (SimpleNamespace(id=folder_deploy, work_pool_name='paperflow-unified-preprocessing',
                                   tags=['paperflow-order-v2-live-20261031']) if '资料文件夹' in name else
                    SimpleNamespace(id=root_deploy, tags=['paperflow-order-v2-live-20261031']))
        def read_flow_run(self, run_id): return root if run_id == root.id else folder
        def read_work_pool(self, name):
            return SimpleNamespace(type='process', description='paperflow-order-v2-live-20261031')
        def read_work_queue_by_name(self, *args, **kwargs): return SimpleNamespace(is_paused=True)
        def read_flow_runs(self, flow_run_filter, limit):
            return [flow for flow in (root, folder) if flow.id in flow_run_filter.id.any_]
        def read_task_run(self, run_id):
            if scenario == 'submission_missing' and run_id == UUID(post_id):
                raise ObjectNotFound(Exception('synthetic missing task'))
            if scenario == 'wrong_tag' and run_id == UUID(post_id):
                stages[run_id].tags = []
            return stages[run_id]
        def set_task_run_state(self, run_id, state, force=False):
            cancelled.append(run_id)
            stages[run_id].state = State('final')
    sched = scheduler_module.PrefectScheduler('http://synthetic.invalid')
    monkeypatch.setattr(sched, '_client', lambda *args, **kwargs: Client())
    def request(path, payload, timeout):
        for flow in (root, folder): flow.state = State('final')
        return {'results': [{'flow_run_id': value} for value in payload['flow_runs']['id']['any_']]}
    monkeypatch.setattr(sched, '_request', request)
    if scenario == 'normal':
        sched.cancel_task(task_id, root_id, [order], stage_jobs=[llm], postprocess_jobs=[post])
        assert set(cancelled) == {UUID(llm_id), UUID(post_id)}
    else:
        with pytest.raises(scheduler_module.SchedulerNotSubmitted):
            sched.cancel_task(task_id, root_id, [order], stage_jobs=[llm], postprocess_jobs=[post])
        assert not cancelled
