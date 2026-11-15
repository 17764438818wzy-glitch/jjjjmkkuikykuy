"""Candidate deployment paths can be loaded by a Prefect process worker."""

from pathlib import Path

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.flows import load_flow_from_entrypoint
from prefect.testing.utilities import prefect_test_harness


def test_process_deployments_point_to_the_actual_candidate(tmp_path, monkeypatch):
    from paperflow_v3.control import deploy
    from paperflow_v3.control.preflight import _deployment_matches

    candidate = Path(__file__).resolve().parents[2]
    monkeypatch.chdir(candidate)
    with prefect_test_harness(server_startup_timeout=60):
        with get_client(sync_client=True) as client:
            client.create_work_pool(WorkPoolCreate(name="isolated-v3", type="process"))
        parent_id, child_id = deploy.create_deployments("isolated-v3")
        with get_client(sync_client=True) as client:
            parent = client.read_deployment(parent_id)
            child = client.read_deployment(child_id)
            assert _deployment_matches(parent, client.read_flow(parent.flow_id).name, "parent")
            assert _deployment_matches(child, client.read_flow(child.flow_id).name, "paper")
            assert load_flow_from_entrypoint(parent.entrypoint).name == "paperflow-v3-parent"
            assert load_flow_from_entrypoint(child.entrypoint).name == "paperflow-v3-paper"
