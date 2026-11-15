"""Exercise waiting for a real isolated Prefect concurrency slot."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import sqlite3
import threading
import time

from test_group_stage import control
from group_stage import reserve_group


def test_parent_pause_cancels_real_prefect_slot_wait(control):
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
    from prefect.testing.utilities import prefect_test_harness
    from group_prefect import controlled_prefect_slots
    from task_effect_admission import TaskNotRunning
    database, ticket, verified = control
    reserve_group(database, verified, 5)
    with prefect_test_harness():
        with get_client(sync_client=True) as api:
            api.create_global_concurrency_limit(GlobalConcurrencyLimitCreate(name='isolated-group-slot', limit=1))
        slots = controlled_prefect_slots(database, ticket['task_id'], ticket['group_id'],
                                          {'nas': ('isolated-group-slot', 1)})
        entered = threading.Event()
        def wait_for_slot():
            entered.set()
            try:
                with slots('nas'):
                    return 'acquired'
            except TaskNotRunning:
                return 'paused'
        with slots('nas'):
            with ThreadPoolExecutor(max_workers=1) as pool:
                waiting = pool.submit(wait_for_slot)
                assert entered.wait(5)
                time.sleep(.3)
                with closing(sqlite3.connect(database)) as db, db:
                    db.execute("UPDATE tasks SET desired_state='paused'")
                assert waiting.result(timeout=10) == 'paused'
