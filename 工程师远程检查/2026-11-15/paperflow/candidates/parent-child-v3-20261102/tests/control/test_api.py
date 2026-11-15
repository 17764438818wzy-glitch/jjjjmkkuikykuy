"""The task API is exercised against an isolated Prefect server."""

import asyncio
from uuid import uuid4

from fastapi.testclient import TestClient
from prefect import flow
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter
from prefect.context import FlowRunContext
from prefect.flow_engine import run_flow
from prefect.states import Crashed, Failed
from prefect.testing.utilities import prefect_test_harness


def capacity_adapters():
    async def test_accounts(*, ensure_current):
        await ensure_current()
        return {'successful_count': 1, 'account_uids': ['synthetic']}

    async def apply_capacity(result, limit, *, ensure_current):
        await ensure_current()

    return dict(test_start_accounts=test_accounts, apply_start_capacity=apply_capacity)


def test_folder_snapshot_is_incomplete_until_each_eligible_group_has_a_child():
    from types import SimpleNamespace
    from paperflow_v3.control.api import _folder_snapshot_complete

    records = [
        {"kind": "classification_complete", "data": {"group_count": 1, "group_ids": ["one"], "ungrouped": []}},
        {"kind": "classification", "folder_id": "one", "data": {"status": "passed", "delivery_complete": True,
                                                                       "registration_status": "registered"}},
        {"kind": "child_dispatch", "folder_id": "one", "data": {"state": "unknown"}},
    ]
    assert not _folder_snapshot_complete(records, [], "task")
    child = SimpleNamespace(parameters={"task_id": "task", "group_id": "one"})
    assert _folder_snapshot_complete(records, [child], "task")


def test_creation_checks_dependencies_again_and_only_creates_one_parent(monkeypatch, tmp_path):
    from paperflow_v3.control import preflight
    from paperflow_v3.control.api import create_app

    @flow(persist_result=False)
    def parent(task_name: str, source_paths: list[str]):
        return task_name, source_paths

    ready = False

    async def isolated_checks(settings):
        return {"ready": ready, "checks": [{"name": "synthetic", "ready": ready, "detail": "isolated"}]}

    monkeypatch.setattr(preflight, "inspect_dependencies", isolated_checks)
    with prefect_test_harness(server_startup_timeout=60):
        with get_client(sync_client=True) as prefect:
            flow_id = prefect.create_flow(parent)
            deployment_id = prefect.create_deployment(flow_id, name="paperflow-v3-isolated")
        app = create_app({"deployment_id": deployment_id}, **capacity_adapters())
        with TestClient(app) as client:
            request = {"request_id": str(uuid4()), "task_name": "A", "source_paths": [str(tmp_path)]}
            missing = client.get(f"/api/v1/prefect/tasks/by-request/{request['request_id']}").json()
            assert missing == {"request_id": request["request_id"], "found": False, "resolved": False}
            assert client.post("/api/v1/prefect/preflight", json={}).json()["ready"] is False
            response = client.post("/api/v1/prefect/tasks", json=request)
            assert response.status_code == 409
            with get_client(sync_client=True) as prefect:
                assert prefect.read_flow_runs(flow_run_filter=FlowRunFilter(deployment_id={"any_": [deployment_id]})) == []

            ready = True
            response = client.post("/api/v1/prefect/tasks", json=request)
            assert response.status_code == 201
            task = response.json()
            assert task["desired_state"] == "paused"
            assert task["actions"]["start"]["allowed"] is True
            assert task["actions"]["pause"]["allowed"] is False
            lookup = client.get(f"/api/v1/prefect/tasks/by-request/{request['request_id']}").json()
            assert lookup["found"] is True and lookup["task"]["task_id"] == task["task_id"]
            assert client.post("/api/v1/prefect/tasks", json=request).json()["task_id"] == task["task_id"]
            with get_client(sync_client=True) as prefect:
                runs = prefect.read_flow_runs(flow_run_filter=FlowRunFilter(deployment_id={"any_": [deployment_id]}))
                assert len(runs) == 1 and runs[0].state.is_paused()

            second = client.post("/api/v1/prefect/tasks", json={**request, "request_id": str(uuid4()), "task_name": "B"}).json()
            started = client.post(f"/api/v1/prefect/tasks/{task['task_id']}/start",
                                  json={"request_id": str(uuid4()), "intent_seq": 1})
            assert started.status_code == 200
            assert started.json()["task_id"] == task["task_id"] and started.json()["accepted"] is True
            assert client.post(f"/api/v1/prefect/tasks/{second['task_id']}/start",
                               json={"request_id": str(uuid4()), "intent_seq": 1}).status_code == 200
            assert client.post(f"/api/v1/prefect/tasks/{task['task_id']}/pause",
                               json={"request_id": str(uuid4()), "intent_seq": 2}).status_code == 200
            listing = client.get("/api/v1/prefect/tasks").json()
            from paperflow_v3.control.admission import Admission
            async def desired_states():
                return {key: 'running' if await Admission(key).running() else 'paused'
                        for key in (task['task_id'], second['task_id'])}
            assert {row['task_id'] for row in listing['tasks']} == {task['task_id'], second['task_id']}
            states = asyncio.run(desired_states())
            assert states == {task["task_id"]: "paused", second["task_id"]: "running"}

            snapshot = client.get("/api/v1/prefect/folder-tickets", params={"task_id": task["task_id"]}).json()
            assert snapshot["task_id"] == task["task_id"]
            assert snapshot["complete"] is False and snapshot["folders"] == []

            with get_client(sync_client=True) as prefect:
                prefect.set_flow_run_state(task["task_id"], Crashed(message="synthetic restart"), force=True)
            assert client.post(f"/api/v1/prefect/tasks/{task['task_id']}/start",
                               json={"request_id": str(uuid4()), "intent_seq": 3}).status_code == 200
            with get_client(sync_client=True) as prefect:
                resumed = prefect.read_flow_run(task["task_id"])
                assert str(resumed.id) == task["task_id"] and resumed.state.is_scheduled()

            ready = False
            assert client.post(f"/api/v1/prefect/tasks/{task['task_id']}/start",
                               json={"request_id": str(uuid4()), "intent_seq": 4}).status_code == 409


def test_folder_list_uses_prefect_native_parent_and_selected_task(monkeypatch, tmp_path):
    from paperflow_v3.control import preflight
    from paperflow_v3.control.admission import Admission
    from paperflow_v3.control.api import create_app
    from paperflow_v3.control.parent import schedule_registered_paper
    from paperflow_v3.control.receipts import save_receipt

    async def isolated_checks(settings):
        return {"ready": True, "checks": []}

    monkeypatch.setattr(preflight, "inspect_dependencies", isolated_checks)

    @flow(persist_result=False)
    def paper(task_id: str, group_id: str, registered: dict):
        return group_id

    with prefect_test_harness(server_startup_timeout=60):
        with get_client(sync_client=True) as prefect:
            child_deployment = prefect.create_deployment(prefect.create_flow(paper), name="paper")

        @flow(persist_result=False)
        async def parent(task_name: str, source_paths: list[str]):
            task_id = str(FlowRunContext.get().flow_run.id)
            registered = {
                "task_id": task_id, "group_id": task_name, "paper_id": "paper-" + task_name,
                "receipt_id": "receipt-" + task_name, "receipt_path": "synthetic.json",
                "receipt_sha256": "synthetic", "paper": {}, "status": "passed",
                "delivery_complete": True, "registration_status": "registered",
            }
            for group_id, status in ((task_name, "passed"), ("bad-" + task_name, "failed")):
                await save_receipt(
                    task_id=task_id, folder_id=group_id, child_flow_run_id=FlowRunContext.get().flow_run.id,
                    event_id=uuid4(), kind="classification",
                    data={"group_id": group_id, "group": group_id, "status": status,
                          "paper_title": None, "reason": "synthetic" if status == "failed" else None},
                )
            child_id = await schedule_registered_paper(child_deployment, registered, Admission(task_id))
            if task_name == "A":
                for kind, data in (
                    ("preprocessing_complete", {"status": "preprocessing_complete"}),
                    ("llm_folder_complete", {"status": "raw_complete", "ranking_complete": True}),
                    ("postprocessing_component_failed", {"component_id": "body", "reason": "synthetic failure"}),
                ):
                    await save_receipt(task_id=task_id, folder_id=task_name, child_flow_run_id=child_id,
                                       event_id=uuid4(), kind=kind, data=data)
            return str(child_id)

        with get_client(sync_client=True) as prefect:
            deployment_id = prefect.create_deployment(prefect.create_flow(parent), name="parent")
        with TestClient(create_app({"deployment_id": deployment_id}, **capacity_adapters())) as client:
            for name in ("A", "B"):
                response = client.post("/api/v1/prefect/tasks", json={
                    "request_id": str(uuid4()), "task_name": name, "source_paths": [str(tmp_path)],
                })
                task_id = response.json()["task_id"]
                assert client.post(f"/api/v1/prefect/tasks/{task_id}/start",
                                   json={"request_id": str(uuid4()), "intent_seq": 1}).status_code == 200
                with get_client(sync_client=True) as prefect:
                    run = prefect.read_flow_run(task_id)
                asyncio.run(run_flow(parent, flow_run=run))
                if name == "A":
                    first = task_id
                else:
                    second = task_id
            first_view = client.get("/api/v1/prefect/folder-tickets", params={"task_id": first}).json()
            second_view = client.get("/api/v1/prefect/folder-tickets", params={"task_id": second}).json()
            assert [row["folder_id"] for row in first_view["folders"]] == ["A"]
            assert [row["folder_id"] for row in second_view["folders"]] == ["B"]
            assert first_view["task_state"] == "unresolved"
            assert first_view["complete"] is False
            assert first_view["folders"][0]["llm_result"] == "passed"
            assert first_view["folders"][0]["postprocessing_result"] == "failed"
            assert first_view["folders"][0]["failed_stage"] == "postprocessing"
            assert second_view["folders"][0]["llm_body_running"] is None
            classification = client.get(f"/api/v1/prefect/tasks/{first}/classifications").json()
            assert {row["group_id"] for row in classification["items"]} == {"A", "bad-A"}
            assert classification["counts"] == {"passed": 1, "failed": 1, "unrecognized": None, "unresolved": None}
            assert classification["complete"] is False
            child_id = first_view["folders"][0]["ticket_id"]
            with get_client(sync_client=True) as prefect:
                prefect.set_flow_run_state(child_id, Failed(message="synthetic postprocessing failure"), force=True)
            retry = client.post(f"/api/v1/prefect/tasks/{first}/folders/A/retry", json={"request_id": str(uuid4())})
            assert retry.status_code == 200
            assert retry.json()["ticket_id"] == child_id
            retry_snapshot = client.get("/api/v1/prefect/folder-tickets", params={"task_id": first}).json()
            last_retry = retry_snapshot["folders"][0]["last_retry"]
            assert last_retry["retry_id"] == retry.json()["retry_id"] and last_retry["status"] == "scheduled"
            with get_client(sync_client=True) as prefect:
                assert prefect.read_flow_run(child_id).state.is_scheduled()
            again = client.post(f"/api/v1/prefect/tasks/{first}/folders/A/retry", json={"request_id": str(uuid4())})
            assert again.status_code == 409 and again.json()["detail"]["code"] == "retry_already_dispatched"
