"""Characterize Prefect's native parent/child behavior on a temporary server."""

from prefect import flow
from prefect.client.orchestration import get_client
from prefect.context import FlowRunContext
from prefect.flow_runs import suspend_flow_run
from prefect.testing.utilities import prefect_test_harness
import pytest


def test_native_child_has_parent_task_and_cannot_be_reschedule_suspended():
    @flow(persist_result=False)
    def child():
        child_id = FlowRunContext.get().flow_run.id
        with get_client(sync_client=True) as client:
            child_run = client.read_flow_run(child_id)
            parent_task = client.read_task_run(child_run.parent_task_run_id)
        with pytest.raises(RuntimeError, match="Cannot suspend subflows"):
            suspend_flow_run()
        return str(child_id), str(parent_task.flow_run_id)

    @flow(persist_result=False)
    def parent():
        parent_id = FlowRunContext.get().flow_run.id
        child_id, linked_parent = child()
        return str(parent_id), child_id, linked_parent

    with prefect_test_harness(server_startup_timeout=60):
        parent_id, child_id, linked_parent = parent()

    assert parent_id != child_id
    assert linked_parent == parent_id
