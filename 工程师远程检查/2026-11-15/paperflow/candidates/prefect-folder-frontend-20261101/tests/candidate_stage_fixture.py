"""Capture current candidate HTTP snapshots using synthetic Prefect state and receipts."""

import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID

sys.dont_write_bytecode = True
os.environ.pop("PREFECT_API_URL", None)
os.environ.pop("PREFECT_API_KEY", None)
os.environ["PREFECT_SERVER_ANALYTICS_ENABLED"] = "false"
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "parent-child-v3-20261102"))

from fastapi.testclient import TestClient
from prefect.states import Completed, Failed, Running, Scheduled
from paperflow_v3.control import api

deployment_id = UUID("11111111-1111-4111-8111-111111111111")
task_id = UUID("22222222-2222-4222-8222-222222222222")
child_id = UUID("33333333-3333-4333-8333-333333333333")
parent = SimpleNamespace(id=task_id, deployment_id=deployment_id, state=Running(), run_count=1,
                         parameters={"task_name": "合成任务"})
child = SimpleNamespace(id=child_id, state=Running(), parameters={
    "task_id": str(task_id), "group_id": "group-1",
    "registered": {"group": "合成文件夹", "paper_title": "合成论文"},
})
child_list = [child]
parent_records = [
    {"kind": "classification_complete", "task_id": str(task_id), "folder_id": "task",
     "data": {"ungrouped": [], "group_ids": ["group-1"], "group_count": 1}},
    {"kind": "classification", "task_id": str(task_id), "folder_id": "group-1",
     "data": {"group_id": "group-1", "group": "合成文件夹", "status": "passed", "paper_title": "合成论文",
              "delivery_complete": True, "registration_status": "registered"}},
]
records = []
records_by_id = None
running = True
child_present = True


def event(event_kind, **data):
    return {"kind": event_kind, "task_id": str(task_id), "folder_id": "group-1", "data": data}


class FakeClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def read_flow_run(self, run_id):
        assert run_id == task_id
        return parent

    async def read_flow_runs(self, **kwargs):
        return child_list if child_present and kwargs["offset"] == 0 else []


class FakeAdmission:
    def __init__(self, run_id):
        assert run_id == task_id

    async def running(self):
        return running


async def begin_session():
    pass


async def children(run_id):
    assert run_id == task_id
    return child_list if child_present else []


async def read_receipts(run_id):
    assert run_id == task_id
    return parent_records


async def read_folder_receipts(run_ids):
    return {str(run_id): records if records_by_id is None else records_by_id[str(run_id)] for run_id in run_ids}


upstream = [event("llm_folder_complete", status="raw_complete", ranking_complete=True),
            event("preprocessing_complete")]
live = [event("llm_request_started", component_id="attachment", request_id="a1", kind="attachment"),
        event("llm_request_started", component_id="body", request_id="b1", kind="body"),
        event("preprocessing_complete")]
with patch.multiple(api, get_client=FakeClient, Admission=FakeAdmission, begin_control_session=begin_session,
                    _children=children, read_receipts=read_receipts, read_folder_receipts=read_folder_receipts):
    with TestClient(api.create_app({"deployment_id": deployment_id})) as client:
        def snapshot():
            response = client.get("/api/v1/prefect/folder-tickets", params={"task_id": str(task_id)})
            assert response.status_code == 200, response.text
            return response.json()

        records = live
        both_running = snapshot()
        running = False
        paused_draining = snapshot()
        running = True
        records = [live[0], event("llm_request_intent", component_id="body", request_id="b1", kind="body"), live[2]]
        unknown_body = snapshot()
        running = False
        records = [event("llm_request_failed", component_id="attachment", request_id="a1", kind="attachment"),
                   live[1], live[2]]
        failed_parallel = snapshot()
        running = True
        child.state = Failed()
        records = [event("postprocessing_component_failed", component_id="body", reason="合成后处理失败"), *upstream]
        failed = snapshot()
        child.state = Running()
        records = [event("postprocessing_complete", validation_status="passed"),
                   event("postprocessing_component_complete", component_id="body"),
                   event("manual_retry", retry_id="44444444-4444-4444-8444-444444444444",
                         state="running", stage="postprocessing"), *records]
        recovered = snapshot()
        child.state = Completed()
        records = [event("cleanup", status="complete", files={}), *records]
        completed = snapshot()
        child.state = Running()
        records = live
        running = False
        parent_records.insert(0, {"kind": "dependency_failure", "task_id": str(task_id), "folder_id": "task",
                                  "data": {"status": "active", "reason": "合成模型服务不可用"}})
        stopped_by_dependency = snapshot()
        parent_records.pop(0)
        parent_records.insert(0, {"kind": "child_dispatch", "task_id": str(task_id), "folder_id": "group-1",
                                  "data": {"state": "unknown"}})
        child_present = False
        running = True
        unknown_dispatch = snapshot()
        child_present = True
        parent_records = []
        child_list = []
        records_by_id = {}
        for index in range(10):
            group_id = f"queue-group-{index + 1}"
            ticket = SimpleNamespace(id=UUID(int=index + 20), state=Scheduled() if index < 7 else Running(),
                                     parameters={"task_id": str(task_id), "group_id": group_id,
                                                 "registered": {"group": group_id, "paper_title": None}})
            child_list.append(ticket)
            records_by_id[str(ticket.id)] = [] if index < 7 else [{"kind": "preprocessing_started",
                "task_id": str(task_id), "folder_id": group_id, "data": {}}]
        queue_before_classification = snapshot()
        classification_response = client.get(f"/api/v1/prefect/tasks/{task_id}/classifications")
        assert classification_response.status_code == 200, classification_response.text
        queue_classification = classification_response.json()

Path(sys.argv[1]).write_text(json.dumps({"both_running": both_running, "paused_draining": paused_draining,
    "unknown_body": unknown_body, "failed": failed, "recovered": recovered, "completed": completed,
    "stopped_by_dependency": stopped_by_dependency, "unknown_dispatch": unknown_dispatch,
    "failed_parallel": failed_parallel, "queue_before_classification": queue_before_classification,
    "queue_classification": queue_classification},
    ensure_ascii=False), encoding="utf-8")
print("candidate HTTP snapshots saved from synthetic Prefect state")
