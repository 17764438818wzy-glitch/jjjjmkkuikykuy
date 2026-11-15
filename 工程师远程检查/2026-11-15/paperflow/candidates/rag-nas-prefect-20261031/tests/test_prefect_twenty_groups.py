"""Twenty real Prefect task runs with synthetic folder evidence and no business writes."""
import asyncio
from contextlib import closing
import sqlite3
import threading
import time
from uuid import uuid4

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.testing.utilities import prefect_test_harness

from group_prefect import controlled_prefect_slots, execute_prefect_stage, observed_concurrency
from group_stage import FIELDS, reserve_group
from task_effect_admission import task_effect_admission
from test_group_prefect import evidence
from test_group_stage import control


def test_twenty_real_prefect_folder_stages_pause_and_resume(control, tmp_path):
    database, first, first_map = control
    jobs, receipts = [], {}
    for index in range(20):
        if index == 0:
            ticket, verified = first, first_map
        else:
            ticket = {**first, 'group_id': f'{index:032x}', 'paper_id': f'paper-{index}',
                      'receipt_id': f'{index:064x}', 'attempt_id': f'attempt-{index}',
                      'task_run_id': str(uuid4())}
            claim, job_id = f'claim-{index}', f'job-{index}'
            verified = {'identity': ticket, 'job': {'job_id': job_id, 'claim_id': claim,
                        'manifest_sha256': 'c' * 64, 'generation': 4},
                        'receipt_canonical_sha256': 'd' * 64}
            fields = [ticket[key] for key in FIELDS]
            with closing(sqlite3.connect(database)) as db, db:
                db.execute('INSERT INTO postprocessing_claims VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                           (*fields, 5, 'waiting_rag', claim, 'c' * 64))
                db.execute('INSERT INTO paperflow_postprocess_stage_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                           (*fields, job_id, 'finished', 'c' * 64, 4))
                db.execute('INSERT INTO postprocessing_results VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                           (*fields, claim, 'passed', 'passed', 'd' * 64, 'c' * 64, 4))
                db.execute('INSERT INTO postprocessing_receipts VALUES(?,?)', (claim, 'd' * 64))
                db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?)',
                           (*fields, 'waiting_postprocessing', 'c' * 64))
        job = reserve_group(database, verified, 5)
        folder = tmp_path / str(index)
        folder.mkdir()
        receipts[ticket['group_id']] = evidence(folder, ticket, job)
        jobs.append(job)

    entered, attempts = [], []
    release = threading.Event()
    def operation(_stage, row):
        group, generation = row['group_id'], row['run_generation']
        attempts.append((group, generation))
        if generation == 5:
            entered.append(group)
            if not release.wait(90):
                raise TimeoutError('synthetic pause was not released')
            with task_effect_admission(database, row['task_id'], generation) as permit:
                permit.check_current()
        copy, rag = receipts[group]
        return {'rag_receipt_path': str(rag), 'copy_receipt_path': str(copy)}

    async def run():
        def create_limit():
            with get_client(sync_client=True) as api:
                api.create_global_concurrency_limit(GlobalConcurrencyLimitCreate(name='synthetic-rag-20', limit=20))
        await asyncio.to_thread(create_limit)
        tasks = []
        for job in jobs:
            slot = controlled_prefect_slots(database, job['task_id'], job['group_id'], {'rag': ('synthetic-rag-20', 20)})
            tasks.append(asyncio.create_task(execute_prefect_stage(database, job, 'rag', operation, slot)))
        try:
            deadline = time.monotonic() + 80
            while len(entered) < 20 and time.monotonic() < deadline:
                if any(task.done() and task.exception() for task in tasks):
                    await asyncio.gather(*tasks)
                await asyncio.sleep(.2)
            assert len(entered) == 20
            assert observed_concurrency(database, first['task_id'], 'rag')['peak'] == 20
            with closing(sqlite3.connect(database)) as db, db:
                db.execute("UPDATE tasks SET desired_state='paused'")
            release.set()
            while time.monotonic() < deadline:
                with closing(sqlite3.connect(database)) as db:
                    paused = db.execute("SELECT COUNT(*) FROM paperflow_rag_nas_jobs WHERE state='paused'").fetchone()[0]
                if paused == 20:
                    break
                await asyncio.sleep(.2)
            assert paused == 20
            with closing(sqlite3.connect(database)) as db, db:
                db.execute("UPDATE tasks SET desired_state='running',start_generation=6")
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=80)
            with closing(sqlite3.connect(database)) as db:
                finished = db.execute("SELECT COUNT(*) FROM paperflow_rag_nas_jobs WHERE stage='nas' AND state='waiting'").fetchone()[0]
            assert finished == 20
            assert len([row for row in attempts if row[1] == 5]) == 20
            assert len([row for row in attempts if row[1] == 6]) == 20
        finally:
            release.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    with prefect_test_harness():
        asyncio.run(run())
