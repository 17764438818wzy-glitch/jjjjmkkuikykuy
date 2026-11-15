import asyncio
from uuid import uuid4

from prefect import flow, get_run_logger, task
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import LogFilter, LogFilterTaskRunId
from prefect.context import TaskRunContext
from prefect.states import Failed, Paused, Pending, Running, Scheduled
from prefect.task_worker import TaskWorker
from prefect.testing.utilities import prefect_test_harness


@flow(name='synthetic-deployed-folder')
def folder():
    pass


@task(name='synthetic-folder-stage', retries=0)
async def stage():
    get_run_logger().info('synthetic stage log follows its folder')
    return {'has_task_context': TaskRunContext.get() is not None}


async def exercise(folder_state):
    stage.task_key = 'synthetic-folder-stage-' + uuid4().hex
    worker = TaskWorker(stage, limit=1)
    worker.tasks[0].task_key = stage.task_key
    async with get_client() as api:
        flow_id = await api.create_flow(folder)
        deployment_id = await api.create_deployment(flow_id, 'synthetic-deployment-' + uuid4().hex)
        parent = await api.create_flow_run_from_deployment(deployment_id, state=folder_state)
        traditional = await api.create_task_run(stage, flow_run_id=parent.id,
                                                dynamic_key='traditional', state=Pending())
        decision = await api.set_task_run_state(traditional.id, Running())
        scheduled = Scheduled()
        scheduled.state_details.deferred = True
        child = await api.create_task_run(stage, flow_run_id=parent.id,
                                          dynamic_key='deferred', state=scheduled)
    async with worker:
        subscription = asyncio.create_task(worker.astart())
        try:
            async with get_client() as api:
                for _ in range(120):
                    await asyncio.sleep(.25)
                    observed = await api.read_task_run(child.id)
                    if observed.state.is_final():
                        break
                observed_parent = await api.read_flow_run(parent.id)
                logs = []
                for _ in range(60):
                    logs = await api.read_logs(log_filter=LogFilter(task_run_id=LogFilterTaskRunId(any_=[child.id])))
                    if any('synthetic stage log follows its folder' in row.message for row in logs):
                        break
                    await asyncio.sleep(.25)
        finally:
            subscription.cancel()
            try:
                await subscription
            except asyncio.CancelledError:
                pass
    return decision, observed, observed_parent, logs


def test_deferred_stage_under_deployed_suspended_folder():
    with prefect_test_harness():
        decision, child, parent, logs = asyncio.run(exercise(
            Paused(name='Suspended', reschedule=True, pause_key='synthetic')))
    assert decision.status.value == 'REJECT'
    assert parent.state.is_paused()
    assert child.state.is_completed()
    assert child.flow_run_id == parent.id
    assert any('synthetic stage log follows its folder' in row.message for row in logs)


def test_deferred_stage_under_deployed_failed_folder():
    with prefect_test_harness():
        _, child, parent, logs = asyncio.run(exercise(Failed(message='synthetic failed folder')))
    assert parent.state.is_failed()
    assert child.state.is_completed()
    assert child.flow_run_id == parent.id
    assert any('synthetic stage log follows its folder' in row.message for row in logs)
