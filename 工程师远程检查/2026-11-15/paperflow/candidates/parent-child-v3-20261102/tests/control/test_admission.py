"""Exercise task isolation and the pause boundary against a temporary Prefect server."""

import asyncio
from uuid import uuid4

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.concurrency.asyncio import concurrency
from prefect.testing.utilities import prefect_test_harness


def test_native_stage_receipts_keep_pause_pending_until_the_folder_is_settled():
    from prefect import flow
    from prefect.context import FlowRunContext
    from paperflow_v3.control.admission import Admission, begin_control_session
    from paperflow_v3.control.stage_admission import FolderStage, pause_progress

    @flow(persist_result=False)
    async def parent():
        task_id = FlowRunContext.get().flow_run.id
        await begin_control_session()
        gate = Admission(task_id)
        await gate.start()
        ids = {'task_id': str(task_id), 'group_id': 'synthetic-folder', 'task_run_id': str(task_id)}
        async with FolderStage(gate, ids, 'llm') as stage:
            await gate.pause()
            assert await stage.running()
            assert (await pause_progress(task_id))['state'] == 'pausing'
        assert (await pause_progress(task_id))['state'] == 'paused'
    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(parent())


def test_tasks_pause_independently_and_restart_requires_a_new_start():
    from paperflow_v3.control.admission import Admission, begin_control_session

    async def exercise():
        await begin_control_session()
        first, second = Admission(uuid4()), Admission(uuid4())
        assert not await first.running()
        await first.start()
        await second.start()
        await first.pause()
        assert not await first.running()
        assert await second.running()
        # Existing classification and RAG clients use synchronous stop callbacks.
        assert not await asyncio.to_thread(first.running_sync)
        assert await asyncio.to_thread(second.running_sync)
        await begin_control_session()
        assert not await first.running()
        assert not await second.running()
        await second.start()
        assert await second.running()
        assert not await first.running()

    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(exercise())


def test_pause_while_waiting_for_a_shared_slot_does_not_start_work():
    from paperflow_v3.control.admission import (
        Admission, admitted_slot, admitted_sync_slot, begin_control_session,
    )

    async def exercise():
        await begin_control_session()
        first, second = Admission(uuid4()), Admission(uuid4())
        await first.start()
        await second.start()
        name = "paperflow-v3-isolated-slot"
        async with get_client() as client:
            await client.create_global_concurrency_limit(
                GlobalConcurrencyLimitCreate(name=name, limit=1, active=True)
            )
        events = []

        async def work(gate, label):
            async with admitted_slot(gate, name):
                events.append(label)

        async with concurrency(name, strict=True):
            waiting = asyncio.create_task(work(first, "first"))
            await asyncio.sleep(0.5)
            await first.pause()
        await asyncio.sleep(2)
        assert events == []
        await asyncio.wait_for(work(second, "second"), timeout=15)
        assert events == ["second"]
        await first.start()
        await asyncio.wait_for(waiting, timeout=15)
        assert events == ["second", "first"]

        # Legacy request callbacks run in a thread and need the same slot gate.
        events.clear()

        def synchronous_work():
            with admitted_sync_slot(first, name):
                events.append("first")

        async with concurrency(name, strict=True):
            waiting = asyncio.create_task(asyncio.to_thread(synchronous_work))
            await asyncio.sleep(0.5)
            await first.pause()
        await asyncio.sleep(2)
        assert events == []
        await asyncio.wait_for(work(second, "second"), timeout=15)
        await first.start()
        await asyncio.wait_for(waiting, timeout=15)
        assert events == ["second", "first"]

    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(exercise())
