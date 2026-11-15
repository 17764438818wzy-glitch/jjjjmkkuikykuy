import asyncio
import importlib.util
from pathlib import Path
import sqlite3
import sys
from uuid import UUID, uuid4

from prefect import flow
from prefect.client.orchestration import get_client
from prefect.states import Failed, Paused
from prefect.testing.utilities import prefect_test_harness

candidate = Path(__file__).parent
root = candidate.parents[1]
sys.path[:0] = [str(candidate), str(root / 'candidates/postprocess-prefect-20261031/runtime_next/tests'),
                str(root / 'candidates/postprocess-prefect-20261031'), str(root / 'prefect-paperflow')]
spec = importlib.util.spec_from_file_location('stage_runtime', candidate / 'stage_runtime.py')
post = importlib.util.module_from_spec(spec)
sys.modules['stage_runtime'] = post
spec.loader.exec_module(post)
import test_stage_runtime
test_stage_runtime.Coordinator = post.Coordinator
from test_stage_runtime import context as post_context


@flow(name='synthetic-postprocess-folder')
def folder():
    pass


def test_retry_creates_a_native_stage_under_the_original_folder_and_keeps_old_run(post_context):
    ctx = post_context
    with prefect_test_harness():
        with get_client(sync_client=True) as api:
            flow_id = api.create_flow(folder)
            deployment_id = api.create_deployment(flow_id, 'synthetic-' + uuid4().hex)
            parent = api.create_flow_run_from_deployment(deployment_id,
                name='资料文件夹 · 等待后处理', state=Paused(name='Suspended', reschedule=True, pause_key='synthetic'))
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
        old = coordinator.reserve(ctx.identity, 1)
        old_stage = post.build_task(lambda: coordinator, old['job_id'])
        with get_client(sync_client=True) as api:
            api.create_task_run(old_stage, flow_run_id=parent.id, id=UUID(old['job_id']),
                                dynamic_key='paperflow-postprocess-' + old['job_id'], state=Failed())
        with sqlite3.connect(ctx.db) as db:
            db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='execution_unknown' WHERE job_id=?", (old['job_id'],))
            db.execute('UPDATE tasks SET start_generation=2')
        new = coordinator.reserve(ctx.identity, 2, retry_job_id=old['job_id'])
        stage = post.build_task(lambda: coordinator, new['job_id'])

        async def execute():
            worker = post.make_one_worker(stage, new['job_id'])
            async with worker:
                subscription = asyncio.create_task(worker.astart())
                try:
                    await asyncio.sleep(0)
                    def submit():
                        with get_client(sync_client=True) as api:
                            coordinator.submit(new['job_id'], api, stage)
                    await asyncio.to_thread(submit)
                    for _ in range(120):
                        await asyncio.sleep(.25)
                        if worker.finished_task_runs.get(stage.task_key, 0): break
                    with get_client(sync_client=True) as api:
                        return api.read_task_run(UUID(new['job_id'])), api.read_task_run(UUID(old['job_id']))
                finally:
                    subscription.cancel()
                    try: await subscription
                    except asyncio.CancelledError: pass
        actual, previous = asyncio.run(execute())
        assert actual.state.is_completed() and actual.flow_run_id == parent.id
        assert previous.state.is_failed() and previous.flow_run_id == parent.id
        assert new['job_id'] != old['job_id'] and coordinator.job(new['job_id'])['state'] == 'finished'
