import asyncio
import importlib.util
from pathlib import Path
import sqlite3
import sys
from uuid import UUID, uuid4

from prefect import flow
from prefect.client.orchestration import get_client
from prefect.states import Paused
from prefect.testing.utilities import prefect_test_harness

here = Path(__file__).resolve().parent
root = here.parents[2]
sys.path.insert(0, str(root / 'candidates/postprocess-prefect-20261031/runtime_next/tests'))
spec = importlib.util.spec_from_file_location('candidate_native_postprocess', here / 'stage_runtime.py')
post = importlib.util.module_from_spec(spec)
spec.loader.exec_module(post)
import test_stage_runtime
test_stage_runtime.Coordinator = post.Coordinator
from test_stage_runtime import context as post_context


@flow(name='synthetic-postprocess-folder')
def folder():
    pass


def test_real_postprocessing_stage_under_deployed_suspended_folder(post_context):
    ctx = post_context
    with prefect_test_harness():
        with get_client(sync_client=True) as api:
            flow_id = api.create_flow(folder)
            deployment_id = api.create_deployment(flow_id, 'synthetic-' + uuid4().hex)
            parent = api.create_flow_run_from_deployment(deployment_id,
                name='资料文件夹 · 等待后处理',
                state=Paused(name='Suspended', reschedule=True, pause_key='synthetic'))
        ctx.identity['task_run_id'] = str(parent.id)
        with sqlite3.connect(ctx.db) as db:
            db.execute('UPDATE llm_folder_queue SET task_run_id=?', (str(parent.id),))
            db.execute('UPDATE llm_request_admissions SET task_run_id=?', (str(parent.id),))
        ctx.switches['open'] = True
        coordinator = ctx.coordinator
        def verify(item, actual):
            with get_client(sync_client=True) as api:
                return post.validate_prefect_stage(item, actual, api, expected_api_url=str(api.api_url))
        coordinator.verify_execution = verify
        job = coordinator.reserve(ctx.identity, 1)
        stage = post.build_task(lambda: coordinator, job['job_id'])

        async def execute():
            worker = post.make_one_worker(stage, job['job_id'])
            async with worker:
                subscription = asyncio.create_task(worker.astart())
                try:
                    await asyncio.sleep(0)
                    def submit():
                        with get_client(sync_client=True) as api:
                            coordinator.submit(job['job_id'], api, stage)
                    await asyncio.to_thread(submit)
                    for _ in range(120):
                        await asyncio.sleep(.25)
                        if worker.finished_task_runs.get(stage.task_key, 0):
                            break
                    with get_client(sync_client=True) as api:
                        return api.read_task_run(UUID(job['job_id'])), api.read_flow_run(parent.id)
                finally:
                    subscription.cancel()
                    try:
                        await subscription
                    except asyncio.CancelledError:
                        pass

        actual, parent_after = asyncio.run(execute())
        assert actual.state.is_completed()
        assert actual.flow_run_id == parent.id
        assert parent_after.state.is_paused()
        assert coordinator.job(job['job_id'])['state'] == 'finished'
