"""Characterize creating a deployment run without dispatching it."""

from prefect import flow
from prefect.client.orchestration import get_client
from prefect.flow_runs import resume_flow_run
from prefect.states import Paused
from prefect.testing.utilities import prefect_test_harness


def test_deployment_run_can_be_created_paused_then_manually_scheduled():
    @flow(persist_result=False)
    def parent(task_name: str, source_paths: list[str]):
        return task_name, source_paths

    with prefect_test_harness(server_startup_timeout=60):
        with get_client(sync_client=True) as client:
            flow_id = client.create_flow(parent)
            deployment_id = client.create_deployment(flow_id, name="isolated-parent")
            run = client.create_flow_run_from_deployment(
                deployment_id,
                parameters={"task_name": "synthetic", "source_paths": ["synthetic"]},
                state=Paused(reschedule=True),
                idempotency_key="one-user-request",
            )
            assert run.state.is_paused()
            duplicate = client.create_flow_run_from_deployment(
                deployment_id,
                parameters={"task_name": "synthetic", "source_paths": ["synthetic"]},
                state=Paused(reschedule=True),
                idempotency_key="one-user-request",
            )
            assert duplicate.id == run.id
        resume_flow_run(run.id)
        with get_client(sync_client=True) as client:
            assert client.read_flow_run(run.id).state.is_scheduled()
