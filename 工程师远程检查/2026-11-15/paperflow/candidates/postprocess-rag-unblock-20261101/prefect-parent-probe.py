import asyncio
from uuid import uuid4
from prefect import flow, task
from prefect.client.orchestration import get_client
from prefect.context import TaskRunContext
from prefect.states import Failed, Paused, Scheduled
from prefect.task_worker import TaskWorker
from prefect.testing.utilities import prefect_test_harness

@flow(name='synthetic-folder-parent')
def parent():
    pass

@task(name='synthetic-attached-stage', retries=0, persist_result=True)
async def stage():
    return {'has_task_context': TaskRunContext.get() is not None}

async def exercise(parent_state):
    stage.task_key = 'synthetic-attached-' + uuid4().hex
    worker = TaskWorker(stage, limit=1)
    worker.tasks[0].task_key = stage.task_key
    def prepare():
        with get_client(sync_client=True) as client:
            flow_run = client.create_flow_run(parent, state=parent_state)
            scheduled = Scheduled()
            scheduled.state_details.deferred = True
            task_run = client.create_task_run(task=stage, flow_run_id=flow_run.id,
                                              dynamic_key=uuid4().hex, state=scheduled)
            return flow_run.id, task_run.id
    async with worker:
        subscription = asyncio.create_task(worker.astart())
        try:
            await asyncio.sleep(0)
            flow_id, task_id = await asyncio.to_thread(prepare)
            for _ in range(90):
                await asyncio.sleep(.25)
                def status():
                    with get_client(sync_client=True) as client:
                        return client.read_task_run(task_id), client.read_flow_run(flow_id)
                actual, parent_run = await asyncio.to_thread(status)
                if actual.state.is_final():
                    print('RESULT', str(actual.state.type), 'FLOW_LINK', actual.flow_run_id == flow_id,
                          'PARENT', str(parent_run.state.type), flush=True)
                    return
            print('RESULT TIMEOUT', 'FLOW_LINK', actual.flow_run_id == flow_id,
                  'STATE', str(actual.state.type), 'PARENT', str(parent_run.state.type), flush=True)
        finally:
            subscription.cancel()
            try:
                await subscription
            except asyncio.CancelledError:
                pass

with prefect_test_harness():
    asyncio.run(exercise(Paused()))
    asyncio.run(exercise(Failed(message="synthetic previous attempt")))
