import asyncio
from uuid import uuid4

from prefect import task
from prefect.client.orchestration import get_client
from prefect.states import Running, Scheduled
from prefect.task_worker import TaskWorker
from prefect.testing.utilities import prefect_test_harness


@task(retries=0, persist_result=True)
async def existing_stage():
    return 'continued'


def test_existing_running_task_can_be_executed_after_worker_restart():
    async def run():
        existing_stage.task_key = 'synthetic-existing-stage-' + uuid4().hex
        state = Scheduled()
        state.state_details.deferred = True
        async with get_client() as client:
            before = await client.create_task_run(existing_stage, flow_run_id=None,
                dynamic_key='existing', state=state)
            await client.set_task_run_state(before.id, Running())
            running = await client.read_task_run(before.id)
        assert running.state.is_running()
        worker = TaskWorker(existing_stage, limit=1)
        worker.tasks[0].task_key = existing_stage.task_key
        await worker.execute_task_run(running)
        async with get_client() as client:
            after = await client.read_task_run(before.id)
            history = await client.read_task_run_states(before.id)
        return before.id, after, history

    with prefect_test_harness():
        original_id, after, history = asyncio.run(run())
    assert after.id == original_id
    assert after.state.is_completed()
    assert any(state.is_running() for state in history)
