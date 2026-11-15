import asyncio
from contextlib import nullcontext
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from uuid import UUID

root = Path(__file__).resolve().parents[3]
rag = root / 'candidates/rag-nas-prefect-20261031'
here = Path(__file__).parent
sys.path[:0] = [str(here), str(rag / 'tests'), str(rag), str(root / 'prefect-paperflow')]
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


def test_restart_a_synthetic_worker_waiting_on_the_same_paused_stage(control, tmp_path):
    database, ticket, handoff = control
    job = reserve_group(database, handoff, 5)
    copied, receipt = evidence(tmp_path, ticket, job)
    with sqlite3.connect(database) as db:
        db.execute("UPDATE paperflow_rag_nas_jobs SET state='paused',rag_submission='submitted'")
        db.execute("UPDATE tasks SET desired_state='paused'")
    job.update(state='paused', rag_submission='submitted')
    marker, config = tmp_path / 'entered', tmp_path / 'child.json'
    config.write_text(json.dumps({'database': str(database), 'job': job, 'marker': str(marker),
                                  'copy': str(copied), 'receipt': str(receipt)}), encoding='utf8')
    child = tmp_path / 'synthetic_worker.py'
    child.write_text('''
import asyncio,json,sys
from contextlib import nullcontext
from pathlib import Path
cfg=json.loads(Path(sys.argv[1]).read_text(encoding='utf8'))
sys.path[:0]=sys.argv[2:5]
from paperflow_prefect.legacy import bind
from paperflow_prefect.settings import Settings
bind(Settings.load())
import group_prefect
original=group_prefect.controlled_executor
def tracked(*args):
    run=original(*args)
    async def execute(*args):
        Path(cfg['marker']).write_text('entered',encoding='utf8')
        return await run(*args)
    return execute
group_prefect.controlled_executor=tracked
def operation(*args):
    return {'rag_receipt_path':cfg['receipt'],'copy_receipt_path':cfg['copy']}
asyncio.run(group_prefect.execute_prefect_stage(cfg['database'],cfg['job'],'rag',operation,lambda _: nullcontext()))
''', encoding='utf8')
    async def run():
        task_id = job['rag_task_run_id']
        task = group_prefect.build_task('rag', task_id, lambda *_: None)
        identity = prefect_task_identity('rag', task_id)
        scheduled = Scheduled(); scheduled.state_details.deferred = True
        async with get_client() as client:
            await client.create_task_run(task, flow_run_id=None, id=UUID(task_id),
                dynamic_key=identity['dynamic_key'], name='synthetic paused RAG task', state=scheduled)
            await client.set_task_run_state(UUID(task_id), Running())
            api = str(client.api_url)
        child_env = dict(os.environ, PREFECT_API_URL=api, PREFECT_SERVER_ALLOW_EPHEMERAL_MODE='false')
        process = subprocess.Popen([sys.executable, str(child), str(config), str(here), str(rag),
                                    str(root / 'prefect-paperflow')], stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL, env=child_env,
                                   creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        try:
            deadline = time.monotonic() + 30
            while not marker.exists() and process.poll() is None and time.monotonic() < deadline:
                await asyncio.sleep(.2)
            assert marker.exists(), 'synthetic worker did not enter the paused stage'
        finally:
            process.terminate()
            process.wait(timeout=10)
        async with get_client() as client:
            previous = await client.read_task_run(UUID(task_id))
        assert previous.state.is_running()
        with sqlite3.connect(database) as db:
            db.execute("UPDATE tasks SET desired_state='running',start_generation=6")
        def operation(stage, current):
            return {'rag_receipt_path': str(receipt), 'copy_receipt_path': str(copied)}
        await asyncio.wait_for(group_prefect.execute_prefect_stage(database, job, 'rag', operation,
                                                                  lambda _: nullcontext()), timeout=45)
        async with get_client() as client:
            return await client.read_task_run(UUID(task_id))
    with prefect_test_harness():
        actual = asyncio.run(run())
    assert str(actual.id) == job['rag_task_run_id'] and actual.state.is_completed()
    with sqlite3.connect(database) as db:
        assert db.execute('SELECT stage,state FROM paperflow_rag_nas_jobs').fetchone() == ('nas', 'waiting')
