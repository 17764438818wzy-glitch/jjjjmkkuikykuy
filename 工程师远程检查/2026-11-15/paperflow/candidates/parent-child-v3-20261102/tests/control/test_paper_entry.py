"""The child entry keeps one server identity and honors a saved cleanup result."""

import asyncio

from prefect import flow
from prefect.context import FlowRunContext
from prefect.testing.utilities import prefect_test_harness


def test_child_uses_saved_stage_receipts_and_does_not_restart_after_cleanup(monkeypatch):
    from paperflow_v3.control import flows
    from paperflow_v3.control.admission import Admission, begin_control_session
    from paperflow_v3.control.folder_status import folder_status
    from paperflow_v3.control.receipts import read_receipts, save_receipt

    calls = []

    async def fake_process(identity, registered, settings, admission, limits, retry_id, first_step_work_root):
        calls.append("processing")
        assert registered["paper_id"] == identity["paper_id"]
        assert first_step_work_root == "unused"
        for kind, data in (
            ("preprocessing_complete", {"status": "preprocessing_complete"}),
            ("llm_folder_complete", {"status": "raw_complete", "ranking_complete": True}),
            ("postprocessing_complete", {"validation_status": "passed"}),
            ("stage_work_inventory", {"identity": identity, "files": []}),
        ):
            await save_receipt(task_id=identity["task_id"], folder_id=identity["group_id"],
                child_flow_run_id=identity["task_run_id"], event_id=identity["task_run_id"] + "-" + kind,
                kind=kind, data=data)

    async def fake_delivery(identity, registered, settings, admission, limits, first_step_work_root, retry_failed, retry_id=None):
        calls.append("delivery")
        await save_receipt(task_id=identity["task_id"], folder_id=identity["group_id"],
            child_flow_run_id=identity["task_run_id"], event_id=identity["task_run_id"] + "-cleanup",
            kind="cleanup", data={"status": "complete", "identity": identity})
        return {"status": "complete", "cleanup_status": "complete"}

    monkeypatch.setattr(flows, "_process_step", fake_process)
    monkeypatch.setattr(flows, "_delivery_step", fake_delivery)

    @flow(persist_result=False)
    async def parent():
        task_id = str(FlowRunContext.get().flow_run.id)
        gate = Admission(task_id)
        await gate.start()
        registered = {"task_id": task_id, "group_id": "folder", "paper_id": "paper",
                      "receipt_id": "receipt", "status": "passed", "delivery_complete": True,
                      "registration_status": "registered"}

        @flow(persist_result=False)
        async def child():
            child_id = str(FlowRunContext.get().flow_run.id)
            identity = {**{key: registered[key] for key in ("task_id", "group_id", "paper_id", "receipt_id")},
                "attempt_id": child_id, "parent_run_id": task_id, "task_run_id": child_id}
            first = await flows.run_paper(identity, registered, object(), gate, {}, "unused")
            second = await flows.run_paper(identity, registered, object(), gate, {}, "unused")
            records = await read_receipts(child_id)
            assert folder_status(records)["llm_result"] == "passed"
            assert first["cleanup_status"] == second["cleanup_status"] == "complete"

        await child()

    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(begin_control_session())
        asyncio.run(parent())
    assert calls == ["processing", "delivery"]
