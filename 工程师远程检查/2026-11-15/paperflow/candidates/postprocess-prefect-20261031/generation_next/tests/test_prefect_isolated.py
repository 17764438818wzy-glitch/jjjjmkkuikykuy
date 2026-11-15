"""Exercises a local disposable Prefect server, never the configured server."""
import asyncio
from pathlib import Path
import sqlite3
import sys
from uuid import UUID, uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from live_runtime import run_selected
from batch_next import run_batch
from stage_runtime import build_task
from test_stage_runtime import context


def test_one_worker_never_consumes_the_other_scheduled_folder(context, monkeypatch):
    from prefect.client.orchestration import get_client
    from prefect.settings import PREFECT_API_URL
    from prefect.testing.utilities import prefect_test_harness

    monkeypatch.setenv('PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS', 'false')
    context.switches['open'] = True
    second = dict(context.identity, group_id='d'*32, paper_id='paper-2', receipt_id='e'*64,
                  attempt_id='attempt-2', task_run_id=str(uuid4()))
    with sqlite3.connect(context.db) as db:
        db.execute('INSERT INTO llm_folder_queue VALUES (?,?,?,?,?,?,?,?,?,?)',
                   tuple(second.values()) + ('c'*64, 'waiting_postprocessing', 1))
        db.execute('INSERT INTO llm_request_admissions VALUES (?,?,?,?,?)',
                   (second['task_id'], second['task_run_id'], 'body-2', 'raw_complete', 'body'))
    coordinator = context.coordinator
    coordinator.verify_execution = lambda item, task_context: (
        str(task_context.id) == item['job_id'] and task_context.task_key == item['task_key'])

    with prefect_test_harness():
        # The test harness owns a newly created local test server and database.
        assert '127.0.0.1' in str(PREFECT_API_URL.value())
        other = coordinator.reserve(second, 1)
        other_task = build_task(lambda: coordinator, other['job_id'])
        with get_client(sync_client=True) as client:
            coordinator.submit(other['job_id'], client, other_task)
        result = asyncio.run(asyncio.wait_for(run_selected(coordinator, context.identity, 1,
            client_factory=lambda: get_client(sync_client=True), runtime_factory=lambda: coordinator,
            poll_seconds=.05), timeout=40))
        assert result['state'] == 'finished'
        assert coordinator.job(other['job_id'])['state'] == 'submitted'
        with get_client(sync_client=True) as client:
            assert client.read_task_run(UUID(other['job_id'])).state.is_scheduled()
        batch = []
        with sqlite3.connect(context.db) as db:
            for number in (3, 4):
                item = dict(context.identity, group_id=f'{number:032x}', paper_id=f'paper-{number}',
                            receipt_id=f'{number:064x}', attempt_id=f'attempt-{number}', task_run_id=str(uuid4()))
                db.execute('INSERT INTO llm_folder_queue VALUES (?,?,?,?,?,?,?,?,?,?)',
                           tuple(item.values()) + ('c'*64, 'waiting_postprocessing', 1))
                db.execute('INSERT INTO llm_request_admissions VALUES (?,?,?,?,?)',
                           (item['task_id'], item['task_run_id'], 'body-' + str(number), 'raw_complete', 'body'))
                batch.append((item, 1))
        report = asyncio.run(asyncio.wait_for(run_batch(coordinator, batch,
            client_factory=lambda: get_client(sync_client=True), runtime_factory=lambda: coordinator,
            batch_size=2, poll_seconds=.05), timeout=40))
        assert report['stopped'] is False and len(report['completed']) == 2
        assert coordinator.job(other['job_id'])['state'] == 'submitted'
        with get_client(sync_client=True) as client:
            assert client.read_task_run(UUID(other['job_id'])).state.is_scheduled()
