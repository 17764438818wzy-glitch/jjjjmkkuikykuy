"""Capture candidate API responses using only a temporary Prefect test server."""

import json
import os
import sys
import tempfile
from pathlib import Path
from uuid import uuid4

sys.dont_write_bytecode = True
output = Path(sys.argv[1])
with tempfile.TemporaryDirectory(prefix="paperflow-frontend-contract-") as temporary:
    os.environ["PREFECT_HOME"] = str(Path(temporary) / "prefect-home")
    os.environ["PREFECT_SERVER_ANALYTICS_ENABLED"] = "false"
    os.environ.pop("PREFECT_API_URL", None)
    os.environ.pop("PREFECT_API_KEY", None)
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "parent-child-v3-20261102"))

    from fastapi.testclient import TestClient
    from prefect import flow
    from prefect.client.orchestration import get_client
    from prefect.settings import PREFECT_API_URL
    from prefect.testing.utilities import prefect_test_harness
    from paperflow_v3.control import preflight
    from paperflow_v3.control.api import create_app

    ready = False

    async def isolated_checks(settings):
        return {"ready": ready, "checks": [{"name": "synthetic", "ready": ready, "detail": "isolated"}]}

    preflight.inspect_dependencies = isolated_checks

    @flow(persist_result=False)
    def synthetic_parent(task_name: str, source_paths: list[str]):
        return task_name, source_paths

    with prefect_test_harness(server_startup_timeout=60):
        assert PREFECT_API_URL.value().startswith("http://127.0.0.1:")
        with get_client(sync_client=True) as client:
            deployment = client.create_deployment(client.create_flow(synthetic_parent), name="frontend-contract")
        with TestClient(create_app({"deployment_id": deployment})) as client:
            request = {"request_id": str(uuid4()), "task_name": "合成任务", "source_paths": [temporary]}
            lookup_missing = client.get(f"/api/v1/prefect/tasks/by-request/{request['request_id']}").json()
            assert lookup_missing["found"] is False and lookup_missing["resolved"] is False
            blocked_check = client.post("/api/v1/prefect/preflight", json={}).json()
            blocked_create = client.post("/api/v1/prefect/tasks", json=request)
            assert blocked_create.status_code == 409
            ready = True
            created_response = client.post("/api/v1/prefect/tasks", json=request)
            assert created_response.status_code == 201
            created = created_response.json()
            lookup_found = client.get(f"/api/v1/prefect/tasks/by-request/{request['request_id']}").json()
            assert lookup_found["found"] is True and lookup_found["task"]["task_id"] == created["task_id"]
            tasks = client.get("/api/v1/prefect/tasks").json()
            folders = client.get("/api/v1/prefect/folder-tickets", params={"task_id": created["task_id"]}).json()
            classifications = client.get(f"/api/v1/prefect/tasks/{created['task_id']}/classifications").json()
    output.write_text(json.dumps({"preflight": blocked_check, "created": created, "tasks": tasks,
                                  "folders": folders, "classifications": classifications,
                                  "lookup_missing": lookup_missing, "lookup_found": lookup_found},
                                 ensure_ascii=False), encoding="utf-8")
    print(f"candidate API fixture saved: {output}")
