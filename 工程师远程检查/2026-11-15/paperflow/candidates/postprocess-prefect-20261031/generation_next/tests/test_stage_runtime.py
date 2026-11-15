import asyncio
from contextlib import contextmanager, asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from filelock import FileLock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[4] / 'prefect-paperflow'))
from stage_runtime import (ConfirmedFailure, Coordinator, StageBoundHandoff, StageConflict, TASK_KEY, TAG,
                           build_task, task_key_for_job, make_one_worker, validate_prefect_stage)
from stage_gates import QueueBarriers


@pytest.fixture
def context(tmp_path):
    database = tmp_path / 'control.db'
    identity = dict(task_id=str(uuid4()), group_id='a' * 32, paper_id='paper-1',
                    receipt_id='b' * 64, attempt_id='attempt-1', parent_run_id=str(uuid4()),
                    task_run_id=str(uuid4()))
    with sqlite3.connect(database) as db:
        db.executescript('''CREATE TABLE tasks(task_id TEXT PRIMARY KEY,desired_state TEXT,
                start_generation INTEGER,start_pending INTEGER,task_status TEXT,
                prefect_order_model TEXT,prefect_run_id TEXT);
            CREATE TABLE task_deletions(task_id TEXT);
            CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,
                attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,manifest_sha256 TEXT,state TEXT,generation INTEGER);
            CREATE TABLE llm_request_admissions(task_id TEXT,task_run_id TEXT,component_id TEXT,state TEXT,kind TEXT);
            CREATE TABLE postprocessing_claims(task_id TEXT,group_id TEXT);
            CREATE TABLE postprocessing_results(task_id TEXT,group_id TEXT);
        ''')
        db.execute('INSERT INTO tasks VALUES (?,?,?,?,?,?,?)', (identity['task_id'], 'running', 1, 0,
                   'complete', 'v2', identity['parent_run_id']))
        db.execute('INSERT INTO llm_folder_queue VALUES (?,?,?,?,?,?,?,?,?,?)',
                   tuple(identity.values()) + ('c'*64, 'waiting_postprocessing', 1))
        db.execute('INSERT INTO llm_request_admissions VALUES (?,?,?,?,?)',
                   (identity['task_id'], identity['task_run_id'], 'body', 'raw_complete', 'body'))

    switches = {'open': False, 'held': True}
    observed = []
    class Results:
        def reserve(self, item, generation, manifest_sha256):
            observed.append(('result-reserved', item['task_run_id'], generation, manifest_sha256))
            return 'synthetic-result-id'
        def success(self, item, result_id, result, validate_receipt):
            assert callable(validate_receipt)
            observed.append(('result-success', item['task_run_id'], result_id))
        def failure(self, item, result_id, error): observed.append(('result-failure', item['task_run_id'], result_id))
        def unknown(self, item, result_id, error): observed.append(('result-unknown', item['task_run_id'], result_id))
    def task_gate(task_id):
        return FileLock(tmp_path / ('task-' + task_id + '.lock'), timeout=3)
    def folder_gate(item):
        return FileLock(tmp_path / ('folder-' + item['attempt_id'] + '.lock'), timeout=3)
    def verify_upstream(item, generation):
        return dict(identity=dict(item), generation=generation, source_llm_generation=1, manifest_sha256='c'*64,
                    raw_complete=True, session_current=True, not_deleted=True)
    def verify_execution(item, task_context):
        return task_context == item['job_id']
    @asynccontextmanager
    async def slot():
        observed.append('acquired')
        try: yield
        finally: observed.append('released')
    def process(item, generation, job, accepted_hook, context_task):
        assert str(getattr(context_task, 'id', context_task)) == job['job_id']
        observed.append(('processed', item['task_run_id'], generation))
        result = {'state': 'waiting_rag', 'identity': item, 'claim_id': 'sample-claim'}
        accepted_hook(result, lambda _receipt: True)
        return result
    coordinator = Coordinator(database, task_gate=task_gate, folder_gate=folder_gate,
        postprocess_is_open=lambda: switches['open'], rag_is_held=lambda: switches['held'],
        verify_upstream=verify_upstream, verify_execution=verify_execution, slot=slot,
        process_folder=process, results=Results())
    coordinator.initialize()
    return SimpleNamespace(db=database, identity=identity, switches=switches,
                           observed=observed, coordinator=coordinator)


def test_default_closed_and_rag_gate_required(context):
    with pytest.raises(StageConflict): context.coordinator.reserve(context.identity, 1)
    context.switches.update(open=True, held=False)
    with pytest.raises(StageConflict): context.coordinator.reserve(context.identity, 1)
    with sqlite3.connect(context.db) as db:
        assert db.execute('select count(*) from paperflow_postprocess_stage_jobs').fetchone()[0] == 0


def test_real_durable_barriers_control_the_candidate_reservation(context):
    barriers = QueueBarriers(context.db)
    barriers.initialize()
    context.coordinator.rag_is_held = barriers.rag_is_held
    context.coordinator.postprocess_is_open = barriers.postprocess_is_open
    with pytest.raises(StageConflict): context.coordinator.reserve(context.identity, 1)
    barriers.open_postprocessing()
    assert context.coordinator.reserve(context.identity, 1)['state'] == 'reserved'
    assert barriers.rag_is_held() is True


@pytest.mark.parametrize('value', [None, 1, 'yes'])
def test_gate_only_accepts_the_boolean_true(context, value):
    context.coordinator.postprocess_is_open = lambda: value
    with pytest.raises(StageConflict): context.coordinator.reserve(context.identity, 1)


def test_two_workers_cannot_reserve_one_folder_twice(context):
    context.switches['open'] = True
    def reserve():
        try: return context.coordinator.reserve(context.identity, 1)
        except StageConflict: return None
    with ThreadPoolExecutor(max_workers=2) as pool:
        items = list(pool.map(lambda _: reserve(), range(2)))
    assert sum(item is not None for item in items) == 1
    with sqlite3.connect(context.db) as db:
        assert db.execute('select count(*) from paperflow_postprocess_stage_jobs').fetchone()[0] == 1


def test_stage_reservation_immediately_removes_the_folder_from_waiting(context):
    context.switches['open'] = True
    item = context.coordinator.reserve(context.identity, 1)
    with sqlite3.connect(context.db) as db:
        waiting = db.execute('''select count(*) from llm_folder_queue f
            left join paperflow_postprocess_stage_jobs j on j.task_id=f.task_id and j.group_id=f.group_id
            where f.state='waiting_postprocessing' and j.job_id is null''').fetchone()[0]
        reservation = db.execute('select state from paperflow_postprocess_stage_jobs').fetchone()[0]
    assert waiting == 0 and reservation == 'reserved'
    assert context.coordinator.jobs_for_task(context.identity['task_id'])[0]['job_id'] == item['job_id']
    assert context.observed == []  # Neither parsing nor result reservation has started.


@pytest.mark.parametrize('change', ['unknown', 'cancelled', 'paused', 'different_generation'])
def test_ineligible_evidence_cannot_reserve(context, change):
    context.switches['open'] = True
    with sqlite3.connect(context.db) as db:
        if change == 'unknown': db.execute("update llm_request_admissions set state='unknown'")
        if change == 'cancelled': db.execute('insert into task_deletions values (?)', (context.identity['task_id'],))
        if change == 'paused': db.execute("update tasks set desired_state='paused'")
        if change == 'different_generation': db.execute('update tasks set start_generation=2')
    with pytest.raises(StageConflict): context.coordinator.reserve(context.identity, 1)


def test_prior_result_intent_prevents_a_new_stage_reservation(context):
    context.switches['open'] = True
    with sqlite3.connect(context.db) as db:
        db.execute('INSERT INTO postprocessing_results VALUES (?,?)',
                   (context.identity['task_id'], context.identity['group_id']))
    with pytest.raises(StageConflict): context.coordinator.reserve(context.identity, 1)


def test_failed_submit_is_unknown_and_never_resent(context):
    context.switches['open'] = True
    item = context.coordinator.reserve(context.identity, 1)
    calls = []
    class Client:
        def create_task_run(self, **kw):
            calls.append(kw)
            raise TimeoutError('unknown outcome')
    with pytest.raises(TimeoutError): context.coordinator.submit(item['job_id'], Client(), object())
    with pytest.raises(StageConflict): context.coordinator.submit(item['job_id'], Client(), object())
    assert len(calls) == 1
    assert context.coordinator.job(item['job_id'])['state'] == 'submission_unknown'


def test_execution_requires_both_gates_and_same_task_identity(context):
    context.switches['open'] = True
    item = context.coordinator.reserve(context.identity, 1)
    with sqlite3.connect(context.db) as db:
        db.execute("update paperflow_postprocess_stage_jobs set state='submitted'")
    with pytest.raises(StageConflict): asyncio.run(context.coordinator.execute(item['job_id'], 'different'))
    assert not any(isinstance(x, tuple) and x[0] == 'processed' for x in context.observed)


def test_execution_holds_slot_and_folder_gate_and_never_reexecutes(context):
    context.switches['open'] = True
    item = context.coordinator.reserve(context.identity, 1)
    with sqlite3.connect(context.db) as db:
        db.execute("update paperflow_postprocess_stage_jobs set state='submitted'")
    result = asyncio.run(context.coordinator.execute(item['job_id'], item['job_id']))
    assert result['state'] == 'waiting_rag'
    assert context.observed == ['acquired', ('result-reserved', context.identity['task_run_id'], 1, 'c'*64),
                                ('processed', context.identity['task_run_id'], 1),
                                ('result-success', context.identity['task_run_id'], 'synthetic-result-id'), 'released']
    with pytest.raises(StageConflict): asyncio.run(context.coordinator.execute(item['job_id'], item['job_id']))
    assert context.coordinator.job(item['job_id'])['state'] == 'finished'


def test_gate_closing_after_claim_prevents_processing(context):
    context.switches['open'] = True
    item = context.coordinator.reserve(context.identity, 1)
    with sqlite3.connect(context.db) as db:
        db.execute("update paperflow_postprocess_stage_jobs set state='submitted'")
    checks = 0
    def held():
        nonlocal checks
        checks += 1
        return checks < 4
    context.coordinator.rag_is_held = held
    with pytest.raises(StageConflict): asyncio.run(context.coordinator.execute(item['job_id'], item['job_id']))
    assert not any(isinstance(x, tuple) and x[0] == 'processed' for x in context.observed)
    assert context.coordinator.job(item['job_id'])['state'] == 'execution_unknown'


@pytest.mark.parametrize('confirmed', [True, False])
def test_processing_errors_persist_correct_outcome_without_retry(context, confirmed):
    context.switches['open'] = True
    item = context.coordinator.reserve(context.identity, 1)
    with sqlite3.connect(context.db) as db:
        db.execute("update paperflow_postprocess_stage_jobs set state='submitted'")
    def fail(*args):
        raise ConfirmedFailure('known failure') if confirmed else TimeoutError('unknown outcome')
    context.coordinator.process_folder = fail
    with pytest.raises((ConfirmedFailure, TimeoutError)):
        asyncio.run(context.coordinator.execute(item['job_id'], item['job_id']))
    events = [x[0] for x in context.observed if isinstance(x, tuple)]
    assert events == ['result-reserved', 'result-failure' if confirmed else 'result-unknown']
    assert context.observed[-2][-1] == 'synthetic-result-id'
    assert context.coordinator.job(item['job_id'])['state'] == ('failed' if confirmed else 'execution_unknown')


def test_cancellation_cannot_release_capacity_while_business_thread_is_running(context):
    context.switches['open'] = True
    item = context.coordinator.reserve(context.identity, 1)
    with sqlite3.connect(context.db) as db:
        db.execute("update paperflow_postprocess_stage_jobs set state='submitted'")
    started, release = threading.Event(), threading.Event()
    def slow(identity, generation, job, accepted_hook, context_task):
        started.set()
        if not release.wait(3): raise RuntimeError('test timed out')
        result = {'state': 'waiting_rag', 'identity': identity, 'claim_id': 'sample-claim'}
        accepted_hook(result, lambda _receipt: True)
        return result
    context.coordinator.process_folder = slow
    async def run():
        worker = asyncio.create_task(context.coordinator.execute(item['job_id'], item['job_id']))
        assert await asyncio.to_thread(started.wait, 2)
        worker.cancel()
        try:
            await asyncio.sleep(.05)
            assert 'released' not in context.observed
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError): await worker
    asyncio.run(run())


def test_missing_accepted_receipt_hook_cannot_produce_a_pass(context):
    context.switches['open'] = True
    item = context.coordinator.reserve(context.identity, 1)
    with sqlite3.connect(context.db) as db:
        db.execute("update paperflow_postprocess_stage_jobs set state='submitted'")
    context.coordinator.process_folder = lambda identity, *_: {
        'state': 'waiting_rag', 'identity': identity, 'claim_id': 'sample-claim'}
    with pytest.raises(StageConflict): asyncio.run(context.coordinator.execute(item['job_id'], item['job_id']))
    assert context.coordinator.job(item['job_id'])['state'] == 'execution_unknown'
    assert not any(isinstance(x, tuple) and x[0] == 'result-success' for x in context.observed)


def test_real_prefect_task_definition_subscribes_to_exactly_one_job():
    selected, other = str(uuid4()), str(uuid4())
    stage = build_task(lambda: None, selected)
    assert stage.task_key == task_key_for_job(selected)
    assert stage.task_key != task_key_for_job(other)
    assert stage.retries == 0
    assert TAG in stage.tags


def test_worker_cloned_task_keeps_the_one_folder_subscription(monkeypatch):
    import prefect.task_worker
    class FakeWorker:
        def __init__(self, stage, limit):
            self.task_keys, self.tasks, self.limit = {stage.task_key}, [SimpleNamespace(task_key='recomputed')], limit
    monkeypatch.setattr(prefect.task_worker, 'TaskWorker', FakeWorker)
    selected, other = str(uuid4()), str(uuid4())
    stage = build_task(lambda: None, selected)
    worker = make_one_worker(stage, selected)
    assert worker.limit == 1
    assert worker.task_keys == {task_key_for_job(selected)}
    assert worker.tasks[0].task_key == task_key_for_job(selected)
    assert task_key_for_job(other) not in worker.task_keys


def test_prefect_stage_validation_checks_all_identity_fields():
    stage_id, folder_id = str(uuid4()), str(uuid4())
    running = SimpleNamespace(is_running=lambda: True)
    paused = SimpleNamespace(is_paused=lambda: True, state_details=SimpleNamespace(pause_reschedule=True))
    item = dict(job_id=stage_id, task_key=task_key_for_job(stage_id), folder_run_id=folder_id)
    stage = SimpleNamespace(id=stage_id, flow_run_id=None, task_key=item['task_key'],
        dynamic_key='paperflow-postprocess-' + stage_id, tags=[TAG], state=running)
    folder = SimpleNamespace(id=folder_id, name='folder · 等待后处理', state=paused)
    class Client:
        api_url = 'http://prefect.test/api'
        def read_task_run(self, _): return stage
        def read_flow_run(self, _): return folder
    assert validate_prefect_stage(item, stage, Client(), expected_api_url='http://prefect.test/api') is True
    with pytest.raises(StageConflict):
        validate_prefect_stage(item, stage, Client(), expected_api_url='http://other.test/api')
    stage.dynamic_key = 'incorrect'
    with pytest.raises(StageConflict):
        validate_prefect_stage(item, stage, Client(), expected_api_url='http://prefect.test/api')


def test_running_stage_binding_reuses_upstream_validation_and_rechecks_prefect(context):
    context.switches['open'] = True
    item = context.coordinator.reserve(context.identity, 1)
    running = SimpleNamespace(is_running=lambda: True)
    paused = SimpleNamespace(is_paused=lambda: True, state_details=SimpleNamespace(pause_reschedule=True))
    stage = SimpleNamespace(id=item['job_id'], flow_run_id=None, task_key=item['task_key'],
        dynamic_key='paperflow-postprocess-' + item['job_id'], tags=[TAG], state=running)
    folder = SimpleNamespace(id=item['folder_run_id'], name='folder · 等待后处理', state=paused)
    class Client:
        api_url = 'http://prefect.test/api'
        def read_task_run(self, _): return stage
        def read_flow_run(self, _): return folder
    class Upstream:
        api_url = 'http://prefect.test/api'
        def load(self, identity, *, require_running):
            assert require_running is False
            return {'identity': identity}
    binding = StageBoundHandoff(Upstream(), Client(), item, stage)
    assert binding.load(context.identity, require_running=True)['identity'] == context.identity
    folder.name = 'folder · 等待 LLM'
    with pytest.raises(StageConflict): binding.load(context.identity, require_running=True)


@pytest.mark.parametrize('old_state', ['FAILED', 'CRASHED'])
def test_completed_retry_keeps_terminal_folder_identity_for_postprocessing(context, old_state):
    """The original failed flow is history; a verified retry supplies the input."""
    context.switches['open'] = True
    item = context.coordinator.reserve(context.identity, 1)
    item['source_retry_sha256'] = 'a' * 64
    item['source_retry_prefect_sha256'] = 'e' * 64
    running = SimpleNamespace(is_running=lambda: True)
    failed = SimpleNamespace(is_paused=lambda: False,
        is_failed=lambda: old_state == 'FAILED', is_crashed=lambda: old_state == 'CRASHED')
    stage = SimpleNamespace(id=item['job_id'], flow_run_id=None, task_key=item['task_key'],
        dynamic_key='paperflow-postprocess-' + item['job_id'], tags=[TAG], state=running)
    folder = SimpleNamespace(id=item['folder_run_id'], name='原文件夹 · 失败', state=failed)
    class Client:
        api_url = 'http://prefect.test/api'
        def read_task_run(self, _): return stage
        def read_flow_run(self, _): return folder
    class Upstream:
        api_url = 'http://prefect.test/api'
        def load(self, identity, *, require_running):
            assert not require_running
            return {'identity': identity, 'source_retry_sha256': 'a' * 64,
                    'source_retry_prefect_sha256': 'e' * 64}
    binding = StageBoundHandoff(Upstream(), Client(), item, stage)
    assert binding.load(context.identity, require_running=True)['identity'] == context.identity
    assert folder.state is failed


def test_completed_retry_cannot_replace_the_frozen_retry_identity(context):
    context.switches['open'] = True
    item = context.coordinator.reserve(context.identity, 1)
    item['source_retry_sha256'] = 'a' * 64
    item['source_retry_prefect_sha256'] = 'e' * 64
    running = SimpleNamespace(is_running=lambda: True)
    failed = SimpleNamespace(is_paused=lambda: False, is_failed=lambda: True, is_crashed=lambda: False)
    stage = SimpleNamespace(id=item['job_id'], flow_run_id=None, task_key=item['task_key'],
        dynamic_key='paperflow-postprocess-' + item['job_id'], tags=[TAG], state=running)
    folder = SimpleNamespace(id=item['folder_run_id'], name='原文件夹 · 失败', state=failed)
    class Client:
        api_url = 'http://prefect.test/api'
        def read_task_run(self, _): return stage
        def read_flow_run(self, _): return folder
    class Upstream:
        api_url = 'http://prefect.test/api'
        def load(self, identity, *, require_running):
            return {'identity': identity, 'source_retry_sha256': 'b' * 64,
                    'source_retry_prefect_sha256': 'e' * 64}
    with pytest.raises(StageConflict):
        StageBoundHandoff(Upstream(), Client(), item, stage).load(context.identity, require_running=True)


def test_reservation_persists_the_selected_retry_identity(context):
    from test_retry_source import install_retry_records
    from stage_runtime import retry_source_snapshot
    install_retry_records(context.db, context.identity, existing_admissions=True)
    context.switches['open'] = True
    original = context.coordinator.verify_upstream
    context.coordinator.verify_upstream = lambda item, generation: {
        **original(item, generation), 'source_retry_sha256': retry_source_snapshot(context.db, item, 1)['sha256'],
        'source_retry_prefect_sha256': 'd' * 64}
    item = context.coordinator.reserve(context.identity, 1)
    assert item.get('source_retry_sha256') == retry_source_snapshot(context.db, context.identity, 1)['sha256']


def test_changed_retry_after_reservation_cannot_start_postprocessing(context):
    from test_retry_source import install_retry_records
    from stage_runtime import retry_source_snapshot
    install_retry_records(context.db, context.identity, existing_admissions=True)
    context.switches['open'] = True
    original = context.coordinator.verify_upstream
    context.coordinator.verify_upstream = lambda item, generation: {
        **original(item, generation), 'source_retry_sha256': retry_source_snapshot(context.db, item, 1)['sha256'],
        'source_retry_prefect_sha256': 'd' * 64}
    item = context.coordinator.reserve(context.identity, 1)
    with sqlite3.connect(context.db) as db:
        db.execute("update paperflow_postprocess_stage_jobs set state='submitted'")
        db.execute('UPDATE llm_request_admissions SET response_sha256=?', ('f' * 64,))
    with pytest.raises(StageConflict):
        asyncio.run(context.coordinator.execute(item['job_id'], item['job_id']))
    assert not any(isinstance(event, tuple) and event[0] == 'processed' for event in context.observed)


def test_changed_retry_prefect_completion_after_reservation_cannot_start(context):
    from test_retry_source import install_retry_records
    from stage_runtime import retry_source_snapshot, reserved_source_matches
    install_retry_records(context.db, context.identity, existing_admissions=True)
    context.switches['open'] = True
    original = context.coordinator.verify_upstream
    prefect_proof = ['d' * 64]
    def verified(item, generation):
        return {**original(item, generation),
                'source_retry_sha256': retry_source_snapshot(context.db, item, 1)['sha256'],
                'source_retry_prefect_sha256': prefect_proof[0]}
    context.coordinator.verify_upstream = verified
    job = context.coordinator.reserve(context.identity, 1)
    assert job['source_retry_prefect_sha256'] == prefect_proof[0]
    prefect_proof[0] = 'e' * 64
    assert not reserved_source_matches(context.db, context.identity, verified(context.identity, 1))
    with sqlite3.connect(context.db) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted'")
    with pytest.raises(StageConflict):
        asyncio.run(context.coordinator.execute(job['job_id'], job['job_id']))
    assert not any(isinstance(event, tuple) and event[0] == 'processed' for event in context.observed)


def test_changed_retry_prefect_completion_prevents_stage_submission(context):
    from test_retry_source import install_retry_records
    from stage_runtime import retry_source_snapshot
    install_retry_records(context.db, context.identity, existing_admissions=True)
    context.switches['open'] = True
    original, prefect_proof = context.coordinator.verify_upstream, ['d' * 64]
    context.coordinator.verify_upstream = lambda item, generation: {
        **original(item, generation), 'source_retry_sha256': retry_source_snapshot(context.db, item, 1)['sha256'],
        'source_retry_prefect_sha256': prefect_proof[0]}
    job = context.coordinator.reserve(context.identity, 1)
    prefect_proof[0] = 'e' * 64
    calls = []
    client = SimpleNamespace(create_task_run=lambda **kwargs: calls.append(kwargs))
    with pytest.raises(StageConflict):
        context.coordinator.submit(job['job_id'], client, object())
    assert calls == []


def test_retry_change_between_source_check_and_reservation_is_rejected(context):
    from test_retry_source import install_retry_records
    from stage_runtime import retry_source_snapshot
    install_retry_records(context.db, context.identity, existing_admissions=True)
    context.switches['open'] = True
    original = context.coordinator.verify_upstream
    def checked_then_changed(item, generation):
        proof = retry_source_snapshot(context.db, item, 1)['sha256']
        with sqlite3.connect(context.db) as db:
            db.execute('UPDATE llm_request_admissions SET response_sha256=?', ('f' * 64,))
        return {**original(item, generation), 'source_retry_sha256': proof, 'source_retry_prefect_sha256': 'd' * 64}
    context.coordinator.verify_upstream = checked_then_changed
    with pytest.raises(StageConflict):
        context.coordinator.reserve(context.identity, 1)
    with sqlite3.connect(context.db) as db:
        assert db.execute('SELECT COUNT(*) FROM paperflow_postprocess_stage_jobs').fetchone()[0] == 0


def test_receipt_observer_rejects_new_retry_source_after_reservation(context):
    from test_retry_source import install_retry_records
    import stage_runtime
    install_retry_records(context.db, context.identity, existing_admissions=True)
    context.switches['open'] = True
    original = context.coordinator.verify_upstream
    def verified(item, generation):
        return {**original(item, generation),
                'source_retry_sha256': stage_runtime.retry_source_snapshot(context.db, item, 1)['sha256'],
                'source_retry_prefect_sha256': 'd' * 64}
    context.coordinator.verify_upstream = verified
    item = context.coordinator.reserve(context.identity, 1)
    check = getattr(stage_runtime, 'reserved_source_matches', None)
    assert callable(check), 'receipt publication must compare the reserved retry source'
    assert check(context.db, context.identity, verified(context.identity, 1)) is True
    with sqlite3.connect(context.db) as db:
        db.execute('UPDATE llm_request_admissions SET response_sha256=?', ('f' * 64,))
    assert check(context.db, context.identity, verified(context.identity, 1)) is False


def test_claim_transaction_rejects_retry_source_changed_after_observation(context):
    from test_retry_source import install_retry_records
    import stage_runtime
    from folder_state import FolderStageStore, StageConflict as FolderConflict
    install_retry_records(context.db, context.identity, existing_admissions=True)
    context.switches['open'] = True
    original = context.coordinator.verify_upstream
    context.coordinator.verify_upstream = lambda item, generation: {
        **original(item, generation),
        'source_retry_sha256': stage_runtime.retry_source_snapshot(context.db, item, 1)['sha256'],
        'source_retry_prefect_sha256': 'd' * 64}
    context.coordinator.reserve(context.identity, 1)
    with sqlite3.connect(context.db) as db:
        db.execute('UPDATE llm_request_admissions SET response_sha256=?', ('f' * 64,))
    with sqlite3.connect(context.db) as db:
        db.row_factory = sqlite3.Row
        with pytest.raises(FolderConflict):
            FolderStageStore._upstream(db, context.identity, 'c' * 64)


def test_explicit_schema_update_preserves_existing_stage_without_inventing_retry(context):
    context.switches['open'] = True
    item = context.coordinator.reserve(context.identity, 1)
    with sqlite3.connect(context.db) as db:
        db.execute('ALTER TABLE paperflow_postprocess_stage_jobs DROP COLUMN source_retry_sha256')
        before = db.execute('SELECT job_id,state,source_llm_generation,manifest_sha256 '
                            'FROM paperflow_postprocess_stage_jobs').fetchone()
    context.coordinator.initialize()
    with sqlite3.connect(context.db) as db:
        after = db.execute('SELECT job_id,state,source_llm_generation,manifest_sha256,source_retry_sha256 '
                           'FROM paperflow_postprocess_stage_jobs').fetchone()
    assert before == after[:4]
    assert after[0] == item['job_id'] and after[4] is None
