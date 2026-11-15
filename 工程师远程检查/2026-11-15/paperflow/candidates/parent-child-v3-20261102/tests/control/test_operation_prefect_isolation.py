"""Use Prefect's temporary server to check operation records across API restarts."""

from urllib.parse import urlsplit
from uuid import uuid4

from fastapi.testclient import TestClient
from prefect import flow
from prefect.client.orchestration import get_client
from prefect.settings import PREFECT_API_URL, PREFECT_SERVER_DATABASE_CONNECTION_URL
from prefect.testing.utilities import prefect_test_harness

from paperflow_v3.control import preflight
from paperflow_v3.control.api import create_app
from test_api import capacity_adapters


def test_original_operation_survives_control_api_restart(monkeypatch, tmp_path):
    async def ready(_settings):
        return {"ready": True, "checks": []}

    monkeypatch.setattr(preflight, "inspect_dependencies", ready)

    @flow(persist_result=False)
    def synthetic_parent(task_name: str, source_paths: list[str]):
        return {"task_name": task_name, "source_paths": source_paths}

    with prefect_test_harness(server_startup_timeout=60):
        isolated_api = urlsplit(str(PREFECT_API_URL.value()))
        assert isolated_api.hostname in {"127.0.0.1", "localhost"} and isolated_api.port != 1
        assert "sqlite" in str(PREFECT_SERVER_DATABASE_CONNECTION_URL.value())
        with get_client(sync_client=True) as prefect:
            deployment_id = prefect.create_deployment(prefect.create_flow(synthetic_parent), name="isolated-parent")

        request_id, start_id, pause_id = str(uuid4()), str(uuid4()), str(uuid4())
        with TestClient(create_app({"deployment_id": deployment_id}, **capacity_adapters())) as client:
            created = client.post("/api/v1/prefect/tasks", json={"request_id": request_id,
                "task_name": "synthetic", "source_paths": [str(tmp_path)]})
            assert created.status_code == 201, created.text
            task_id = created.json()["task_id"]
            assert created.json()["request_id"] == request_id and created.json()["accepted"] is True
            start = client.post(f"/api/v1/prefect/tasks/{task_id}/start",
                                json={"request_id": start_id, "intent_seq": 1})
            assert start.status_code == 200 and start.json()["status"] == "confirmed"
            pause = client.post(f"/api/v1/prefect/tasks/{task_id}/pause",
                                json={"request_id": pause_id, "intent_seq": 2})
            assert pause.status_code == 200 and pause.json()["status"] == "confirmed"
            assert client.get(f"/api/v1/prefect/operations/{start_id}").json()["status"] == "superseded"

        gate_name = "paperflow_v3_task_" + task_id.replace("-", "_")
        with get_client(sync_client=True) as prefect:
            gate_before = prefect.read_variable_by_name(gate_name)
        with TestClient(create_app({"deployment_id": deployment_id}, **capacity_adapters())) as restarted:
            result = restarted.get(f"/api/v1/prefect/operations/{pause_id}").json()
            assert result["found"] is True and result["resolved"] is True
            assert result["status"] == "confirmed" and result["intent_seq"] == 2
            original = restarted.get(f"/api/v1/prefect/tasks/by-request/{request_id}").json()
            assert original["resolved"] is True and original["task"]["task_id"] == task_id
            duplicate = restarted.post(f"/api/v1/prefect/tasks/{task_id}/pause",
                                       json={"request_id": pause_id, "intent_seq": 2})
            assert duplicate.json()["request_id"] == pause_id
        with get_client(sync_client=True) as prefect:
            gate_after = prefect.read_variable_by_name(gate_name)
        assert gate_after.updated == gate_before.updated
        assert gate_after.value == {"state": "paused", "session": None}
