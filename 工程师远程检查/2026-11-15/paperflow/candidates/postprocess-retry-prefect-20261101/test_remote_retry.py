import importlib
import importlib.util
import asyncio
from contextlib import asynccontextmanager
import hashlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import sys
import threading
import time
from uuid import UUID, uuid4

import pytest

root = Path(__file__).resolve().parents[2]
candidate = Path(__file__).resolve().parent
server = root / 'candidates/postprocess-server-20261101'
upstream = root / 'candidates/postprocess-prefect-20261031'
sys.path[:0] = [str(candidate), str(server / 'tests'), str(server), str(upstream / 'tests'),
                str(upstream / 'runtime_next'), str(upstream / 'results_next'), str(upstream),
                str(root / 'prefect-paperflow')]
for module in ('stage_runtime', 'folder_state', 'postprocessing_results', 'remote_jobs', 'remote_coordinator', 'snapshot_prepare', 'remote_runtime_adapter'):
    loaded = importlib.import_module(module)
    assert Path(loaded.__file__).resolve().parent == candidate

from test_remote_runtime_adapter import sample, make
spec = importlib.util.spec_from_file_location('candidate_retry_worker', candidate / 'run_remote_postprocess_worker.py')
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)


def test_new_attempt_reuses_same_folder_without_overwriting_old_remote_records(sample):
    identity, database = sample['evidence'][0]['identity'], sample['database']
    original_wait = sample['options']['wait_for_result']
    def lost_result(_job):
        raise TimeoutError('synthetic old server result unavailable')
    sample['options']['wait_for_result'] = lost_result
    old_runtime = make(sample)
    old = old_runtime.reserve(identity, 1)
    with sqlite3.connect(database) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (old['job_id'],))
    with pytest.raises(TimeoutError):
        old_runtime._execute_sync(old['job_id'], old['job_id'])
    assert old_runtime.job(old['job_id'])['state'] == 'execution_unknown'
    assert sample['journal'].get(old['job_id']) is not None

    with sqlite3.connect(database) as db:
        db.execute('UPDATE tasks SET start_generation=2 WHERE task_id=?', (identity['task_id'],))
    def completed_result(job):
        value = original_wait(job)
        request = {key: value[key] for key in ('job', 'server_identity', 'receipt_json', 'receipt_sha256')}
        value['server_receipt'] = sample['attestation'](request)
        return value
    sample['options']['wait_for_result'] = completed_result
    sample['options']['validate_result'] = lambda job, result: result['server_receipt']['job'] == job
    original_load = sample['options']['load_inputs']
    sample['options']['load_inputs'] = lambda identity, _generation, item, context: original_load(identity, 1, item, context)
    sample['wait_release'].set()
    new_runtime = make(sample)
    new = new_runtime.reserve(identity, 2, retry_job_id=old['job_id'])
    with sqlite3.connect(database) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (new['job_id'],))
    result = new_runtime._execute_sync(new['job_id'], new['job_id'])

    assert result['state'] == 'waiting_rag'
    assert new['job_id'] != old['job_id']
    assert new['folder_run_id'] == old['folder_run_id'] == identity['task_run_id']
    assert sample['journal'].get(old['job_id']) is not None
    assert sample['journal'].get(new['job_id']) is not None
    assert [event[0] for event in sample['events']] == ['transfer', 'submit', 'transfer', 'submit']
    with sqlite3.connect(database) as db:
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] == 'passed'
        assert db.execute('SELECT job_id FROM postprocessing_attempt_history').fetchone()[0] == old['job_id']


@pytest.mark.parametrize('phase', ['load_inputs', 'transfer_snapshot'])
def test_pause_after_stage_claim_completes_current_server_postprocessing(sample, phase):
    identity, database = sample['evidence'][0]['identity'], sample['database']
    original = sample['options'][phase]
    def pause_after(*args, **kwargs):
        value = original(*args, **kwargs)
        with sqlite3.connect(database) as db:
            db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (identity['task_id'],))
        return value
    sample['options'][phase] = pause_after
    original_wait = sample['options']['wait_for_result']
    def completed_result(job):
        value = original_wait(job)
        request = {key: value[key] for key in ('job', 'server_identity', 'receipt_json', 'receipt_sha256')}
        value['server_receipt'] = sample['attestation'](request)
        return value
    sample['options']['wait_for_result'] = completed_result
    sample['options']['validate_result'] = lambda job, result: result['server_receipt']['job'] == job
    sample['wait_release'].set()
    runtime = make(sample)
    job = runtime.reserve(identity, 1)
    with sqlite3.connect(database) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    result = runtime._execute_sync(job['job_id'], job['job_id'])
    assert result['state'] == 'waiting_rag'
    with sqlite3.connect(database) as db:
        assert db.execute('SELECT desired_state FROM tasks WHERE task_id=?', (identity['task_id'],)).fetchone()[0] == 'paused'
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] == 'passed'


def test_only_authorized_attempt_is_selected_and_policy_is_prepared_before_it_is_archived(sample):
    identity, database, base = sample['evidence'][0]['identity'], sample['database'], sample['base']
    old = base.reserve(identity, 1)
    with sqlite3.connect(database) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='execution_unknown' WHERE job_id=?", (old['job_id'],))
        db.execute("UPDATE tasks SET desired_state='paused',start_generation=2 WHERE task_id=?", (identity['task_id'],))
    assert worker.snapshot_retryable(database, identity['task_id'], 8, [old['job_id']]) == []
    with sqlite3.connect(database) as db:
        db.execute("UPDATE tasks SET desired_state='running' WHERE task_id=?", (identity['task_id'],))
    assert worker.snapshot_retryable(database, identity['task_id'], 8, []) == []
    assert worker.snapshot_retryable(database, identity['task_id'], 8, [str(uuid4())]) == []
    assert worker.snapshot_retryable(database, identity['task_id'], 8, [old['job_id']]) == [(identity, 2, old['job_id'])]
    class Launcher:
        def prepare(self, actual, generation, *, retry_job_id):
            with sqlite3.connect(database) as db:
                assert db.execute('SELECT state FROM paperflow_postprocess_stage_jobs WHERE job_id=?',
                                  (retry_job_id,)).fetchone() == ('execution_unknown',)
            return retry_job_id
        async def launch(self, actual, generation, prepared):
            new = base.reserve(actual, generation, retry_job_id=prepared)
            assert new['folder_run_id'] == old['folder_run_id']
            return 'finished'
    assert asyncio.run(worker.run_folder(Launcher(), identity, 2, retry_job_id=old['job_id'])) == 'finished'
    assert worker.snapshot_retryable(database, identity['task_id'], 8, [old['job_id']]) == []


def test_failed_policy_preparation_keeps_original_attempt(sample):
    identity, database, base = sample['evidence'][0]['identity'], sample['database'], sample['base']
    old = base.reserve(identity, 1)
    with sqlite3.connect(database) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='execution_unknown' WHERE job_id=?", (old['job_id'],))
        db.execute('UPDATE tasks SET start_generation=2 WHERE task_id=?', (identity['task_id'],))
    class Launcher:
        def prepare(self, *_args, **_kwargs): raise ValueError('synthetic policy failure')
    with pytest.raises(ValueError):
        asyncio.run(worker.run_folder(Launcher(), identity, 2, retry_job_id=old['job_id']))
    assert base.job(old['job_id'])['state'] == 'execution_unknown'


def test_reserved_retry_is_resumed_after_worker_stops_before_prefect_submission(sample):
    from prefect.exceptions import ObjectNotFound
    identity, database, base = sample['evidence'][0]['identity'], sample['database'], sample['base']
    old = base.reserve(identity, 1)
    with sqlite3.connect(database) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='execution_unknown' WHERE job_id=?", (old['job_id'],))
        db.execute('UPDATE tasks SET start_generation=2 WHERE task_id=?', (identity['task_id'],))
    new = base.reserve(identity, 2, retry_job_id=old['job_id'])
    assert worker.snapshot_stopped(database, identity['task_id'], 8) == [(identity, 2, new['job_id'])]
    class Client:
        api_url = 'http://synthetic-prefect/api'
        def read_task_run(self, unused): raise ObjectNotFound('no task was submitted')
    resumed = base.rearm_stopped(new['job_id'], 2, client=Client(), expected_api_url=Client.api_url)
    assert resumed['job_id'] == new['job_id'] and resumed['folder_run_id'] == identity['task_run_id']
    with sqlite3.connect(database) as db:
        assert db.execute('SELECT job_id FROM postprocessing_attempt_history').fetchone()[0] == old['job_id']


def test_submitted_stage_waits_during_pause_and_continues_after_start(sample):
    identity, database = sample['evidence'][0]['identity'], sample['database']
    runtime = make(sample)
    job = runtime.reserve(identity, 1)
    with sqlite3.connect(database) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    paused = threading.Event()
    verify = runtime.verify_execution
    def pause_before_claim(item, actual):
        if not paused.is_set():
            with sqlite3.connect(database) as db:
                db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (identity['task_id'],))
            paused.set()
        return verify(item, actual)
    runtime.verify_execution = pause_before_claim
    original_load = sample['options']['load_inputs']
    runtime._load_inputs = lambda actual, _generation, item, context: original_load(actual, 1, item, context)
    original_wait = sample['options']['wait_for_result']
    def completed_result(item):
        value = original_wait(item)
        request = {key: value[key] for key in ('job', 'server_identity', 'receipt_json', 'receipt_sha256')}
        value['server_receipt'] = sample['attestation'](request)
        return value
    runtime._wait = completed_result
    runtime._validate_result = lambda item, result: result['server_receipt']['job'] == item
    sample['wait_release'].set()
    slots = []
    original_slot = runtime.slot
    @asynccontextmanager
    async def tracked_slot():
        async with original_slot():
            slots.append('acquired')
            try:
                yield
            finally:
                slots.append('released')
    runtime.slot = tracked_slot
    with ThreadPoolExecutor(max_workers=1) as pool:
        result = pool.submit(lambda: asyncio.run(runtime.execute(job['job_id'], job['job_id'])))
        assert paused.wait(3)
        time.sleep(.7)
        assert runtime.job(job['job_id'])['state'] == 'submitted'
        assert not sample['events']
        assert slots == ['acquired', 'released']
        with sqlite3.connect(database) as db:
            db.execute("UPDATE tasks SET desired_state='running',start_generation=2 WHERE task_id=?", (identity['task_id'],))
        assert result.result(timeout=10)['state'] == 'waiting_rag'
    assert runtime.job(job['job_id'])['generation'] == 2
    assert slots == ['acquired', 'released', 'acquired', 'released']


def test_paused_parent_does_not_block_another_parents_coordinator_capacity(monkeypatch):
    paused = threading.Event()
    launched = []
    monkeypatch.setattr(worker, 'POLL_SECONDS', .01)
    monkeypatch.setattr(worker, 'running_tasks', lambda _: ([('B', 1)] if paused.is_set() else [('A', 1)], []))
    monkeypatch.setattr(worker, 'snapshot_stopped', lambda *_: [])
    monkeypatch.setattr(worker, 'snapshot_retryable', lambda *_: [])
    class Batch:
        def snapshot_waiting(self, _database, task_id, _count):
            count = 8 if task_id == 'A' else 1
            return [({'task_id': task_id, 'group_id': str(i)}, 1) for i in range(count)]
    class Runtime:
        original_runtime = {'database': 'unused'}
        batch = Batch()
    class Launcher:
        def prepare(self, identity, generation): return identity
        async def launch(self, identity, generation, prepared):
            launched.append(identity['task_id'])
            if launched.count('A') == 8: paused.set()
            await asyncio.Event().wait()
    async def check():
        coordinator = asyncio.create_task(worker.serve(Runtime(), Launcher(), None))
        try:
            for _ in range(300):
                if 'B' in launched: break
                await asyncio.sleep(.01)
            assert launched.count('A') == 8 and 'B' in launched
        finally:
            coordinator.cancel()
            children = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
            for task in children: task.cancel()
            await asyncio.gather(*children, return_exceptions=True)
    asyncio.run(check())


def test_restart_resumes_the_same_prefect_postprocessing_task(sample):
    from contextlib import contextmanager
    from prefect.client.orchestration import SyncPrefectClient, get_client
    from prefect.states import Running, Scheduled
    from prefect.testing.utilities import prefect_test_harness
    from stage_runtime import build_task
    live = importlib.import_module('live_runtime')
    assert Path(live.__file__).resolve().parent == candidate
    identity, database = sample['evidence'][0]['identity'], sample['database']
    raw = Path(sample['evidence'][0]['body']['responses'][0]['response']['path'])
    original_response_sha = hashlib.sha256(raw.read_bytes()).hexdigest()
    runtime = make(sample)
    job = runtime.reserve(identity, 1)
    with sqlite3.connect(database) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    runtime.verify_execution = lambda item, actual: str(actual.id) == item['job_id']
    original_load = sample['options']['load_inputs']
    runtime._load_inputs = lambda actual, generation, item, context: original_load(actual, 1, item, item['job_id'])
    original_wait = sample['options']['wait_for_result']
    def completed_result(item):
        value = original_wait(item)
        request = {key: value[key] for key in ('job', 'server_identity', 'receipt_json', 'receipt_sha256')}
        value['server_receipt'] = sample['attestation'](request)
        return value
    runtime._wait = completed_result
    runtime._validate_result = lambda item, result: result['server_receipt']['job'] == item
    sample['wait_release'].set()
    async def check():
        stage = build_task(lambda: runtime, job['job_id'])
        scheduled = Scheduled(); scheduled.state_details.deferred = True
        async with get_client() as client:
            await client.create_task_run(stage, flow_run_id=None, id=UUID(job['job_id']),
                dynamic_key='paperflow-postprocess-' + job['job_id'], state=scheduled)
            await client.set_task_run_state(UUID(job['job_id']), Running())
            api = str(client.api_url)
        @contextmanager
        def sync_client():
            with SyncPrefectClient(api) as client:
                yield client
        result = await asyncio.wait_for(live.resume_stopped(runtime, job['job_id'], 1,
            client_factory=sync_client, expected_api_url=api, runtime_factory=lambda: runtime), timeout=45)
        async with get_client() as client:
            actual = await client.read_task_run(UUID(job['job_id']))
        assert actual.state.is_completed() and str(actual.id) == job['job_id']
        assert result['state'] == 'finished' and result['folder_run_id'] == identity['task_run_id']
    with prefect_test_harness():
        asyncio.run(check())
    assert [event[0] for event in sample['events']] == ['transfer', 'submit']
    assert hashlib.sha256(raw.read_bytes()).hexdigest() == original_response_sha
