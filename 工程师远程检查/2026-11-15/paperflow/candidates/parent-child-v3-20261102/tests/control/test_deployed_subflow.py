"""Characterize deployed subflows without starting a worker or business work."""

from prefect import flow
from prefect.client.orchestration import get_client
from prefect.context import FlowRunContext
from prefect.deployments import run_deployment
from prefect.states import Crashed, Scheduled
from prefect.testing.utilities import prefect_test_harness


def test_deployed_child_keeps_native_parent_and_identity_when_rescheduled():
    @flow(persist_result=False)
    def paper(folder_id: str):
        return folder_id

    with prefect_test_harness(server_startup_timeout=60):
        with get_client(sync_client=True) as client:
            flow_id = client.create_flow(paper)
            deployment_id = client.create_deployment(flow_id, name="isolated-paper")

        @flow(persist_result=False)
        def parent():
            parent_id = FlowRunContext.get().flow_run.id
            child = run_deployment(
                deployment_id, parameters={"folder_id": "synthetic"},
                idempotency_key="one-paper", timeout=0, as_subflow=True,
            )
            return parent_id, child.id

        parent_id, child_id = parent()
        with get_client(sync_client=True) as client:
            child = client.read_flow_run(child_id)
            assert child.parent_task_run_id is not None
            assert client.read_task_run(child.parent_task_run_id).flow_run_id == parent_id
            client.set_flow_run_state(child_id, Crashed(message="synthetic process loss"), force=True)
            transition = client.set_flow_run_state(child_id, Scheduled())
            current = client.read_flow_run(child_id)
            assert transition.state.is_scheduled()
            assert current.id == child_id
            assert current.parent_task_run_id == child.parent_task_run_id
