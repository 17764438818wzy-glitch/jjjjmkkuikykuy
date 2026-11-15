"""An incomplete dependency check must never admit a new task."""

import asyncio
from pathlib import Path
from types import SimpleNamespace

from prefect import flow
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.settings import PREFECT_API_URL
from prefect.testing.utilities import prefect_test_harness


def test_pipeline_deployment_must_point_to_this_candidate_entrypoint(tmp_path):
    from paperflow_v3.control.preflight import _deployment_matches

    candidate = Path(__file__).resolve().parents[2]
    deployment = SimpleNamespace(path=".", job_variables={"working_dir": str(candidate)},
                                 entrypoint="paperflow_v3/control/flows.py:parent_order")
    assert _deployment_matches(deployment, "paperflow-v3-parent", "parent")
    assert not _deployment_matches(deployment, "other-flow", "parent")
    assert not _deployment_matches(SimpleNamespace(path=".", job_variables={"working_dir": str(tmp_path)},
                                                   entrypoint=deployment.entrypoint),
                                   "paperflow-v3-parent", "parent")


def test_missing_project_and_executor_are_reported_without_starting_them(tmp_path):
    from paperflow_v3.control.preflight import inspect_dependencies

    @flow
    def parent():
        pass

    with prefect_test_harness(server_startup_timeout=60):
        with get_client(sync_client=True) as client:
            deployment_id = client.create_deployment(client.create_flow(parent), name="isolated")
        result = asyncio.run(inspect_dependencies({
            "deployment_id": deployment_id, "project_config": str(tmp_path / "missing.json"),
        }))

    checks = {item["name"]: item for item in result["checks"]}
    assert not result["ready"]
    assert checks["prefect_deployment"]["ready"] is True
    assert checks["prefect_storage"]["ready"] is True
    assert checks["project_config"]["ready"] is False
    assert checks["executor"]["ready"] is False
    assert all(checks[name]["ready"] is False for name in
               ("local_scripts", "nimble", "model_gateway", "rag", "nas", "global_limits", "pipeline"))


def test_pipeline_check_uses_real_candidate_deployment_entries(tmp_path, monkeypatch):
    import json
    import sys
    from paperflow_v3.control import deploy
    from paperflow_v3.control.preflight import inspect_dependencies

    candidate = Path(__file__).resolve().parents[2]
    workspace = candidate.parents[1]
    sys.path.insert(0, str(workspace / "prefect-paperflow"))
    from paperflow_prefect.settings import Settings
    monkeypatch.chdir(candidate)
    data = tmp_path / "data"
    data.mkdir()
    model = tmp_path / "model.json"
    model.write_text("{}", encoding="utf8")
    with prefect_test_harness(server_startup_timeout=60):
        config = {**Settings.load().values, "execution_enabled": False, "data_root": str(data),
                  "library_root": str(tmp_path / "library"), "settings_root": str(tmp_path / "keys"),
                  "model_config": str(model), "nas_destination": str(tmp_path),
                  "nimble_endpoint": "http://127.0.0.1:1/v1/systemone", "rag_url": "http://127.0.0.1:1",
                  "prefect_api_url": PREFECT_API_URL.value()}
        path = tmp_path / "project.json"
        path.write_text(json.dumps(config, ensure_ascii=False), encoding="utf8")
        with get_client(sync_client=True) as client:
            client.create_work_pool(WorkPoolCreate(name="isolated-preflight", type="process"))
        parent, paper = deploy.create_deployments("isolated-preflight")
        result = asyncio.run(inspect_dependencies({"deployment_id": parent, "child_deployment_id": paper,
            "project_config": str(path), "first_step_root": str(workspace / "candidates/first-step-control-20261029")}))
    checks = {item["name"]: item for item in result["checks"]}
    assert checks["pipeline"]["ready"] is True
    assert checks["executor"]["ready"] is False and result["ready"] is False
