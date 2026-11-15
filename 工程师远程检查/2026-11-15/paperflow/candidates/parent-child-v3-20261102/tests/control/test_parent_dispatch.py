"""One qualified folder becomes one real Prefect deployed subflow."""

import asyncio
from uuid import uuid4

from prefect import flow
from prefect.client.orchestration import get_client
from prefect.context import FlowRunContext
from prefect.testing.utilities import prefect_test_harness
import pytest


def test_only_qualified_running_parent_schedules_one_native_child():
    from paperflow_v3.control.admission import Admission, begin_control_session
    from paperflow_v3.control.parent import schedule_registered_paper

    @flow(persist_result=False)
    def paper(task_id: str, group_id: str, registered: dict):
        return group_id

    async def exercise(deployment_id):
        await begin_control_session()

        @flow(persist_result=False)
        async def parent():
            parent_id = FlowRunContext.get().flow_run.id
            gate = Admission(parent_id)
            group = {
                "task_id": str(parent_id), "group_id": "synthetic-a", "paper_id": "paper-a",
                "receipt_id": "receipt-a", "receipt_path": "synthetic.json",
                "receipt_sha256": "synthetic", "paper": {}, "status": "passed",
                "delivery_complete": True, "registration_status": "registered",
            }
            with pytest.raises(InterruptedError):
                await schedule_registered_paper(deployment_id, group, gate)
            await gate.start()
            with pytest.raises(ValueError):
                await schedule_registered_paper(deployment_id, {**group, "delivery_complete": False}, gate)
            first = await schedule_registered_paper(deployment_id, group, gate)
            second = await schedule_registered_paper(deployment_id, group, gate)
            await gate.pause()
            with pytest.raises(InterruptedError):
                await schedule_registered_paper(deployment_id, {**group, "group_id": "synthetic-b"}, gate)
            return parent_id, first, second

        parent_id, first, second = await parent()
        assert first == second
        async with get_client() as client:
            child = await client.read_flow_run(first)
            assert child.parent_task_run_id is not None
            tracking = await client.read_task_run(child.parent_task_run_id)
            assert tracking.flow_run_id == parent_id
            assert child.parameters["group_id"] == "synthetic-a"

    with prefect_test_harness(server_startup_timeout=60):
        with get_client(sync_client=True) as client:
            flow_id = client.create_flow(paper)
            deployment_id = client.create_deployment(flow_id, name="isolated-paper")
        asyncio.run(exercise(deployment_id))
