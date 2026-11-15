import asyncio
from contextlib import nullcontext
from pathlib import Path
import sqlite3
import sys
from uuid import UUID
import pytest

root = Path(__file__).resolve().parents[3]
rag = root / 'candidates/rag-nas-prefect-20261031'
sys.path[:0] = [str(Path(__file__).parent), str(rag / 'tests'), str(rag), str(root / 'prefect-paperflow')]
from paperflow_prefect.legacy import bind
from paperflow_prefect.settings import Settings
bind(Settings.load())
from prefect.client.orchestration import get_client
from prefect.states import Running, Scheduled
from prefect.testing.utilities import prefect_test_harness
from test_group_stage import control
from test_group_prefect import evidence
from group_stage import reserve_group, prefect_task_identity
import group_prefect


@pytest.mark.parametrize(('job_state', 'prefect_state'), [('paused', 'running'), ('waiting', 'scheduled')])
def test_existing_folder_resumes_same_prefect_task_after_restart(control, tmp_path, job_state, prefect_state):
    database, ticket, handoff = control
    job = reserve_group(database, handoff, 5)
    copied, rag_receipt = evidence(tmp_path, ticket, job)
    with sqlite3.connect(database) as db:
        db.execute("UPDATE paperflow_rag_nas_jobs SET state=?,rag_submission='submitted'", (job_state,))
        db.execute("UPDATE tasks SET desired_state='paused'")
    job.update(state=job_state, rag_submission='submitted')
    calls = []
    def operation(stage, current):
        calls.append((stage, current['task_run_id']))
        return {'rag_receipt_path': str(rag_receipt), 'copy_receipt_path': str(copied)}

    async def run():
        task_id = job['rag_task_run_id']
        task = group_prefect.build_task('rag', task_id, lambda *_: None)
        identity = prefect_task_identity('rag', task_id)
        scheduled = Scheduled(); scheduled.state_details.deferred = True
        async with get_client() as client:
            await client.create_task_run(task, flow_run_id=None, id=UUID(task_id),
                dynamic_key=identity['dynamic_key'], name='synthetic old RAG task', state=scheduled)
            if prefect_state == 'running':
                await client.set_task_run_state(UUID(task_id), Running())
        execution = asyncio.create_task(group_prefect.execute_prefect_stage(database, job, 'rag', operation,
                                                                           lambda _: nullcontext()))
        await asyncio.sleep(.5)
        assert calls == []
        with sqlite3.connect(database) as db:
            db.execute("UPDATE tasks SET desired_state='running',start_generation=6")
        await asyncio.wait_for(execution, timeout=45)
        async with get_client() as client:
            actual = await client.read_task_run(UUID(task_id))
            states = await client.read_task_run_states(UUID(task_id))
        return actual, states

    with prefect_test_harness():
        actual, states = asyncio.run(run())
    assert str(actual.id) == job['rag_task_run_id'] and actual.state.is_completed()
    assert calls == [('rag', ticket['task_run_id'])]
    assert any(state.is_running() for state in states)
    with sqlite3.connect(database) as db:
        assert db.execute('SELECT stage,state FROM paperflow_rag_nas_jobs').fetchone() == ('nas', 'waiting')
