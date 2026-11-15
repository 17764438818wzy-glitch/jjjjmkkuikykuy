"""Server receipts remain readable without the worker's local files."""

import asyncio
from uuid import uuid4

from prefect import flow
from prefect.context import FlowRunContext
from prefect.testing.utilities import prefect_test_harness


def test_receipts_are_saved_on_prefect_and_scoped_to_the_child_flow():
    from paperflow_v3.control.receipts import read_receipts, read_receipts_sync, save_receipt, save_receipt_sync

    task_id, folder_id, event_id, sync_id = uuid4(), "folder-a", uuid4(), uuid4()

    @flow(persist_result=False)
    async def child():
        child_id = FlowRunContext.get().flow_run.id
        await save_receipt(
            task_id=task_id,
            folder_id=folder_id,
            child_flow_run_id=child_id,
            event_id=event_id,
            kind="request",
            data={"request_id": "synthetic-1", "state": "unknown"},
        )
        await asyncio.to_thread(
            save_receipt_sync,
            task_id=task_id,
            folder_id=folder_id,
            child_flow_run_id=child_id,
            event_id=sync_id,
            kind="preprocessing",
            data={"state": "passed"},
        )
        return child_id

    @flow(persist_result=False)
    async def parent():
        return await child()

    async def exercise():
        child_id = await parent()
        rows = await read_receipts(child_id)
        assert len(rows) == 2
        assert next(row for row in rows if row["event_id"] == str(event_id)) == {
            "event_id": str(event_id),
            "task_id": str(task_id),
            "folder_id": "folder-a",
            "kind": "request",
            "data": {"request_id": "synthetic-1", "state": "unknown"},
        }
        assert next(row for row in rows if row["event_id"] == str(sync_id))["data"] == {"state": "passed"}
        assert await asyncio.to_thread(read_receipts_sync, child_id) == rows
        assert await read_receipts(uuid4()) == []

    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(exercise())
