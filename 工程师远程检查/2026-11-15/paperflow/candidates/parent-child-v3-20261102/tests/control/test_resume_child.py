"""Manual start resumes the original crashed native child without new identity."""

import asyncio
from uuid import uuid4

from fastapi.testclient import TestClient
from prefect import flow
from prefect.client.orchestration import get_client
from prefect.context import FlowRunContext
from prefect.flow_engine import run_flow
from prefect.states import Crashed
from prefect.testing.utilities import prefect_test_harness


def test_manual_start_reschedules_crashed_parent_and_original_child(monkeypatch, tmp_path):
    from paperflow_v3.control import preflight
    from paperflow_v3.control.admission import Admission, begin_control_session
    from paperflow_v3.control.api import create_app
    from paperflow_v3.control.parent import schedule_registered_paper
    from paperflow_v3.control.receipts import read_receipts, save_receipt

    async def ready(_settings):
        return {"ready": True, "checks": []}

    monkeypatch.setattr(preflight, "inspect_dependencies", ready)

    @flow(persist_result=False)
    def paper(task_id: str, group_id: str, registered: dict):
        return "synthetic-resumed"

    with prefect_test_harness(server_startup_timeout=60):
        with get_client(sync_client=True) as client:
            child_deployment = client.create_deployment(client.create_flow(paper), name="paper")

        @flow(persist_result=False)
        async def parent(task_name: str, source_paths: list[str]):
            task_id = str(FlowRunContext.get().flow_run.id)
            registered = {"task_id": task_id, "group_id": "folder", "paper_id": "paper",
                          "receipt_id": "receipt", "status": "passed", "delivery_complete": True,
                          "registration_status": "registered", "paper": {}}
            child_id = await schedule_registered_paper(child_deployment, registered, Admission(task_id))
            await save_receipt(task_id=task_id, folder_id="folder", child_flow_run_id=child_id,
                event_id=uuid4(), kind="preprocessing_complete", data={"status": "preprocessing_complete"})
            return str(child_id)

        with get_client(sync_client=True) as client:
            parent_deployment = client.create_deployment(client.create_flow(parent), name="parent")
        with TestClient(create_app({"deployment_id": parent_deployment})) as client:
            task = client.post("/api/v1/prefect/tasks", json={"request_id": str(uuid4()),
                               "task_name": "synthetic", "source_paths": [str(tmp_path)]}).json()["task_id"]
            assert client.post(f"/api/v1/prefect/tasks/{task}/start",
                               json={"request_id": str(uuid4()), "intent_seq": 1}).status_code == 200
            with get_client(sync_client=True) as prefect:
                parent_run = prefect.read_flow_run(task)
            asyncio.run(run_flow(parent, flow_run=parent_run))
            child_id = client.get("/api/v1/prefect/folder-tickets", params={"task_id": task}).json()["folders"][0]["ticket_id"]
            with get_client(sync_client=True) as prefect:
                child_before = prefect.read_flow_run(child_id)
                prefect.set_flow_run_state(task, Crashed(message="synthetic restart"), force=True)
                prefect.set_flow_run_state(child_id, Crashed(message="synthetic restart"), force=True)
            asyncio.run(begin_control_session())
            assert client.post(f"/api/v1/prefect/tasks/{task}/start",
                               json={"request_id": str(uuid4()), "intent_seq": 2}).status_code == 200
            with get_client(sync_client=True) as prefect:
                parent_after = prefect.read_flow_run(task)
                child_after = prefect.read_flow_run(child_id)
            assert parent_after.state.is_scheduled() and child_after.state.is_scheduled()
            assert child_after.id == child_before.id and child_after.parent_task_run_id == child_before.parent_task_run_id
            assert run_flow(paper, flow_run=child_after) == "synthetic-resumed"
            assert any(row["kind"] == "preprocessing_complete" for row in asyncio.run(read_receipts(child_id)))
            # Prefect may report the child crash after the parent was started.
            with get_client(sync_client=True) as prefect:
                prefect.set_flow_run_state(child_id, Crashed(message="late detection"), force=True)
            task_row = client.get("/api/v1/prefect/folder-tickets", params={"task_id": task}).json()
            assert task_row["desired_state"] == "running" and task_row["actions"]["start"]["allowed"] is True
            assert client.post(f"/api/v1/prefect/tasks/{task}/start",
                               json={"request_id": str(uuid4()), "intent_seq": 3}).status_code == 200
            with get_client(sync_client=True) as prefect:
                assert prefect.read_flow_run(child_id).state.is_scheduled()
