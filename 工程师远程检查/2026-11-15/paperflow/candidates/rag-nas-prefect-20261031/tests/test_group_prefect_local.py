"""An isolated Prefect server, fake paper evidence, and one real TaskRun."""
import asyncio
from contextlib import closing, contextmanager
from pathlib import Path
import sqlite3
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'prefect-paperflow'))
from test_group_stage import control
from test_group_prefect import evidence, read
from group_stage import reserve_group


def test_actual_prefect_task_run_claims_one_folder(control, tmp_path):
    from prefect.client.orchestration import get_client
    from prefect.testing.utilities import prefect_test_harness
    from group_prefect import advance_stage, build_task, claim_stage, finish_rag, make_worker, submit_stage

    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    copy, receipt = evidence(tmp_path, ticket, job)
    observed = []

    async def execute(stage, task_run):
        observed.append(str(task_run.id))
        await asyncio.to_thread(claim_stage, database, ticket['task_id'], ticket['group_id'], stage, task_run)
        await asyncio.to_thread(finish_rag, database, ticket['task_id'], ticket['group_id'], receipt,
                                copy_receipt_path=copy)
        return {'verified': True}

    async def run():
        task = build_task('rag', job['rag_task_run_id'], execute)
        worker = make_worker([task])
        async with worker:
            subscription = asyncio.create_task(worker.astart())
            try:
                await asyncio.sleep(0)
                def submit():
                    with get_client(sync_client=True) as api:
                        return submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', api, task)
                await asyncio.to_thread(submit)
                deadline = time.monotonic() + 30
                while not observed and time.monotonic() < deadline:
                    await asyncio.sleep(.2)
                assert observed == [job['rag_task_run_id']]
                while not worker.finished_task_runs.get(task.task_key, 0) and time.monotonic() < deadline:
                    await asyncio.sleep(.2)
                assert worker.finished_task_runs.get(task.task_key, 0) == 1
                def advance():
                    from uuid import UUID
                    with get_client(sync_client=True) as api:
                        run = api.read_task_run(UUID(job['rag_task_run_id']))
                    return advance_stage(database, ticket['task_id'], ticket['group_id'], 'rag', run)
                await asyncio.to_thread(advance)
            finally:
                subscription.cancel()
                try: await subscription
                except asyncio.CancelledError: pass

    with prefect_test_harness():
        asyncio.run(run())


def test_existing_prefect_service_runs_one_folder_stage(control, tmp_path):
    from contextlib import nullcontext
    from prefect.testing.utilities import prefect_test_harness
    from group_prefect import execute_prefect_stage
    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    copy, receipt = evidence(tmp_path, ticket, job)
    def operation(_stage, _row):
        return {'rag_receipt_path': str(receipt), 'copy_receipt_path': str(copy)}
    with prefect_test_harness():
        asyncio.run(execute_prefect_stage(database, job, 'rag', operation, lambda _: nullcontext()))
    assert (read(database)['stage'], read(database)['state']) == ('nas', 'waiting')


def test_unready_native_executor_waits_in_the_same_prefect_task(control, tmp_path):
    from contextlib import nullcontext
    from prefect.testing.utilities import prefect_test_harness
    from folder_native_client import NativeNotReady
    from group_prefect import execute_prefect_stage
    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    copy, receipt = evidence(tmp_path, ticket, job)
    attempts = []
    def operation(_stage, row):
        attempts.append((row['state'], row['rag_task_run_id']))
        if len(attempts) == 1:
            raise NativeNotReady('native executor is temporarily unavailable')
        return {'rag_receipt_path': str(receipt), 'copy_receipt_path': str(copy)}
    with prefect_test_harness():
        asyncio.run(execute_prefect_stage(database, job, 'rag', operation, lambda _: nullcontext()))
    assert attempts == [('running', job['rag_task_run_id']), ('running', job['rag_task_run_id'])]
    assert (read(database)['stage'], read(database)['state']) == ('nas', 'waiting')


def test_actual_prefect_pause_drains_and_resumes_same_task_without_reingest(control, tmp_path):
    from prefect.client.orchestration import get_client
    from prefect.testing.utilities import prefect_test_harness
    from effect_journal import EffectJournal
    from folder_native_client import FolderNativeClient
    from group_prefect import build_task, controlled_executor, make_worker, submit_stage
    from task_effect_admission import task_effect_admission
    from test_folder_native_client import Raw

    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    copy, receipt = evidence(tmp_path, ticket, job)
    journal = EffectJournal(tmp_path / 'effects.sqlite3'); journal.initialize()
    raw = Raw()
    attempts = []

    @contextmanager
    def slot(_stage):
        yield

    def operation(stage, row):
        attempts.append(row['run_generation'])
        client = FolderNativeClient(raw, journal, {'test-dataset'}, 'pipeline',
            lambda *_: task_effect_admission(database, ticket['task_id'], row['run_generation']),
            lambda: {'ready': True, 'ready_valid_until': time.time() + 10})
        client.ingest('test-dataset', 'doc-1', 'pipeline')
        if len(attempts) == 1:
            with closing(sqlite3.connect(database)) as db, db:
                db.execute("UPDATE tasks SET desired_state='paused'")
            client.upload_bytes('test-dataset', 'blocked', b'not sent')
        return {'rag_receipt_path': str(receipt), 'copy_receipt_path': str(copy)}

    async def run():
        execute = controlled_executor(database, ticket['task_id'], ticket['group_id'], operation, slot)
        task = build_task('rag', job['rag_task_run_id'], execute)
        worker = make_worker([task])
        async with worker:
            subscription = asyncio.create_task(worker.astart())
            try:
                await asyncio.sleep(0)
                def submit():
                    with get_client(sync_client=True) as api:
                        return submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', api, task)
                await asyncio.to_thread(submit)
                deadline = time.monotonic() + 35
                while read(database)['state'] != 'paused' and time.monotonic() < deadline:
                    await asyncio.sleep(.2)
                assert read(database)['state'] == 'paused'
                assert raw.posts == 1 and raw.calls == 0
                with closing(sqlite3.connect(database)) as db, db:
                    db.execute("UPDATE tasks SET desired_state='running',start_generation=6")
                while not worker.finished_task_runs.get(task.task_key, 0) and time.monotonic() < deadline:
                    await asyncio.sleep(.2)
                assert worker.finished_task_runs.get(task.task_key, 0) == 1
                assert attempts == [5, 6]
                assert raw.posts == 1 and raw.calls == 0
                assert read(database)['rag_submission'] == 'finished'
            finally:
                subscription.cancel()
                try: await subscription
                except asyncio.CancelledError: pass

    with prefect_test_harness():
        asyncio.run(run())


def test_actual_prefect_paused_task_exits_after_parent_deletion(control):
    from prefect.client.orchestration import get_client
    from prefect.testing.utilities import prefect_test_harness
    from group_prefect import build_task, controlled_executor, make_worker, submit_stage
    from task_effect_admission import TaskNotRunning
    from uuid import UUID

    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    calls = []
    @contextmanager
    def slot(_stage):
        yield
    def operation(_stage, _row):
        calls.append(1)
        with closing(sqlite3.connect(database)) as db, db:
            db.execute("UPDATE tasks SET desired_state='paused'")
        raise TaskNotRunning('parent paused')

    async def run():
        task = build_task('rag', job['rag_task_run_id'],
                          controlled_executor(database, ticket['task_id'], ticket['group_id'], operation, slot))
        worker = make_worker([task])
        async with worker:
            subscription = asyncio.create_task(worker.astart())
            try:
                await asyncio.sleep(0)
                def submit():
                    with get_client(sync_client=True) as api:
                        return submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', api, task)
                await asyncio.to_thread(submit)
                deadline = time.monotonic() + 35
                while read(database)['state'] != 'paused' and time.monotonic() < deadline:
                    await asyncio.sleep(.2)
                assert read(database)['state'] == 'paused'
                with closing(sqlite3.connect(database)) as db, db:
                    db.execute('INSERT INTO task_deletions VALUES(?)', (ticket['task_id'],))
                while not worker.finished_task_runs.get(task.task_key, 0) and time.monotonic() < deadline:
                    await asyncio.sleep(.2)
                assert worker.finished_task_runs.get(task.task_key, 0) == 1
                def state():
                    with get_client(sync_client=True) as api:
                        return api.read_task_run(UUID(job['rag_task_run_id'])).state
                assert (await asyncio.to_thread(state)).is_final()
                assert calls == [1]
            finally:
                subscription.cancel()
                try: await subscription
                except asyncio.CancelledError: pass

    with prefect_test_harness():
        asyncio.run(run())


def test_actual_prefect_running_task_drains_after_parent_deletion(control, tmp_path):
    import threading
    from prefect.client.orchestration import get_client
    from prefect.testing.utilities import prefect_test_harness
    from group_prefect import build_task, controlled_executor, make_worker, observed_concurrency, submit_stage

    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    copy, receipt = evidence(tmp_path, ticket, job)
    entered, release = threading.Event(), threading.Event()
    @contextmanager
    def slot(_stage):
        yield
    def operation(_stage, _row):
        entered.set()
        assert release.wait(20)
        return {'rag_receipt_path': str(receipt), 'copy_receipt_path': str(copy)}

    async def run():
        task = build_task('rag', job['rag_task_run_id'],
                          controlled_executor(database, ticket['task_id'], ticket['group_id'], operation, slot))
        worker = make_worker([task])
        async with worker:
            subscription = asyncio.create_task(worker.astart())
            try:
                await asyncio.sleep(0)
                def submit():
                    with get_client(sync_client=True) as api:
                        return submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', api, task)
                await asyncio.to_thread(submit)
                assert await asyncio.to_thread(entered.wait, 20)
                with closing(sqlite3.connect(database)) as db, db:
                    db.execute('INSERT INTO task_deletions VALUES(?)', (ticket['task_id'],))
                release.set()
                deadline = time.monotonic() + 20
                while not worker.finished_task_runs.get(task.task_key, 0) and time.monotonic() < deadline:
                    await asyncio.sleep(.2)
                assert worker.finished_task_runs.get(task.task_key, 0) == 1
                assert (read(database)['stage'], read(database)['state']) == ('rag', 'cancelled')
                assert observed_concurrency(database, ticket['task_id'], 'rag')['open_intervals'] == 0
            finally:
                release.set()
                subscription.cancel()
                try: await subscription
                except asyncio.CancelledError: pass

    with prefect_test_harness():
        asyncio.run(run())
