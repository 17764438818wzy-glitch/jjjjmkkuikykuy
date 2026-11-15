"""Exercise folder-ticket reads using only synthetic, in-memory Prefect data."""

import asyncio
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest
from prefect.client.orchestration import PrefectClient
from prefect.client.schemas.sorting import ArtifactSort
from prefect.settings import PREFECT_API_URL, temporary_settings
from prefect.states import Failed, Running

from paperflow_v3.control import api


TASK_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
DEPLOYMENT_ID = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
OTHER_TASK = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
REFERENCE = Path(__file__).with_name("folder_tickets_reference.json")
CASES = ("normal", "wrong_receipt", "wrong_child", "duplicate_group", "unknown_dispatch", "unclassified")


def make_child(number, group_id, task_id=str(TASK_ID), *, running=True):
    registered = {"task_id": task_id, "group_id": group_id, "paper_id": "paper-" + group_id,
                  "receipt_id": "receipt-" + group_id, "group": "folder-" + group_id,
                  "paper_title": "title-" + group_id}
    return SimpleNamespace(id=UUID(int=number), state=Running() if running else Failed(),
                           parameters={"task_id": task_id, "group_id": group_id, "registered": registered})


def make_data(case):
    parent = SimpleNamespace(id=TASK_ID, deployment_id=DEPLOYMENT_ID, state=Running(), run_count=1,
                             parameters={"task_name": "synthetic task"})
    if case == "bulk":
        children = [make_child(index + 1, f"group-{index:03}") for index in range(201)]
    elif case == "unknown_dispatch":
        children = []
    else:
        children = [make_child(1, "group-a"), make_child(2, "group-b", running=False)]
    group_ids = [child.parameters["group_id"] for child in children]
    parent_rows = [{"task_id": str(TASK_ID), "folder_id": group_id, "kind": "classification",
                    "data": {"group_id": group_id, "status": "passed", "delivery_complete": True,
                             "registration_status": "registered"}} for group_id in group_ids]
    if case != "unclassified":
        parent_rows.insert(0, {"task_id": str(TASK_ID), "folder_id": "task", "kind": "classification_complete",
                               "data": {"group_count": len(group_ids), "group_ids": group_ids, "ungrouped": []}})
    if case == "unknown_dispatch":
        parent_rows.insert(0, {"task_id": str(TASK_ID), "folder_id": "group-unknown", "kind": "child_dispatch",
                               "data": {"state": "unknown"}})
    if case == "wrong_child":
        children.append(make_child(3, "group-other", OTHER_TASK, running=False))
    elif case == "duplicate_group":
        children.append(make_child(3, "group-a", running=False))

    child_rows = {str(child.id): [{"task_id": child.parameters["task_id"],
                                  "folder_id": child.parameters["group_id"],
                                  "kind": "preprocessing_started", "data": {}}] for child in children}
    if case not in ("bulk", "unknown_dispatch"):
        child_rows[str(children[1].id)] = [
            {"task_id": str(TASK_ID), "folder_id": "group-b", "kind": "manual_retry",
             "data": {"state": "authorized", "retry_id": "retry-1", "stage": "llm"}},
            {"task_id": str(TASK_ID), "folder_id": "group-b", "kind": "llm_request_unknown",
             "data": {"component_id": "body", "request_id": "request-1", "kind": "body"}},
            {"task_id": str(TASK_ID), "folder_id": "group-b", "kind": "preprocessing_complete", "data": {}},
        ]
    if case == "wrong_receipt":
        child_rows[str(children[0].id)].insert(0, {
            "task_id": OTHER_TASK, "folder_id": "group-a", "kind": "preprocessing_failed",
            "data": {"failure_reason": "local_io", "retryable": True, "reason": "other task"},
        })
    if case == "bulk":
        child_rows[str(children[0].id)].insert(0, {
            "task_id": str(TASK_ID), "folder_id": "group-000", "kind": "preprocessing_complete", "data": {},
        })
    return parent, children, parent_rows, child_rows


def setup_fake_prefect(monkeypatch, case):
    parent, children, parent_rows, child_rows = make_data(case)
    counts = Counter()
    child_pages, parent_pages, flow_pages = [], [], []
    artifacts = [SimpleNamespace(flow_run_id=TASK_ID, data=row) for row in parent_rows]
    artifacts.extend(SimpleNamespace(flow_run_id=child.id, data=row)
                     for child in children for row in child_rows[str(child.id)])
    original_parent_read = api.read_receipts
    original_child_read = api.read_folder_receipts

    async def no_version_check(_client):
        pass

    async def read_flow_run(_client, run_id):
        counts["parent_run"] += 1
        assert run_id == TASK_ID
        return parent

    async def read_flow_runs(_client, *, flow_run_filter, limit, offset):
        assert flow_run_filter.parent_flow_run_id.any_ == [TASK_ID]
        assert flow_run_filter.tags.all_ == ["paperflow-v3-paper"]
        assert limit == 200
        flow_pages.append(offset)
        if case == "mutating" and len(flow_pages) > 1:
            return (children + [make_child(3, "group-new")])[offset:offset + limit]
        return children[offset:offset + limit]

    async def read_artifacts(_client, *, artifact_filter, sort, limit, offset):
        assert sort == ArtifactSort.CREATED_DESC and limit == 200
        ids = artifact_filter.flow_run_id.any_
        assert artifact_filter.type.any_ == ["paperflow-v3-receipt"]
        if ids == [TASK_ID]:
            parent_pages.append(offset)
        else:
            child_pages.append((len(ids), offset))
        selected = [item for item in artifacts if item.flow_run_id in ids]
        return selected[offset:offset + limit]

    async def read_parent(run_id):
        counts["parent_receipts"] += 1
        return await original_parent_read(run_id)

    async def read_children(run_ids):
        counts["child_receipts"] += 1
        return await original_child_read(run_ids)

    async def full_folder_records(children):
        return await api.read_folder_receipts([child.id for child in children])

    async def is_running(_admission):
        return True

    class FixedDateTime:
        @staticmethod
        def now(_timezone):
            return datetime(2026, 11, 2, 15, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(PrefectClient, "raise_for_api_version_mismatch_once", no_version_check)
    monkeypatch.setattr(PrefectClient, "read_flow_run", read_flow_run)
    monkeypatch.setattr(PrefectClient, "read_flow_runs", read_flow_runs)
    monkeypatch.setattr(PrefectClient, "read_artifacts", read_artifacts)
    monkeypatch.setattr(api.Admission, "running", is_running)
    monkeypatch.setattr(api, "read_receipts", read_parent)
    monkeypatch.setattr(api, "read_folder_receipts", read_children)
    # This suite isolates reuse within one response; the selective reader has separate tests.
    monkeypatch.setattr(api, "_folder_ticket_records", full_folder_records)
    monkeypatch.setattr(api, "datetime", FixedDateTime)
    def add_child():
        child = make_child(3, "group-new")
        children.append(child)
        artifacts.append(SimpleNamespace(flow_run_id=child.id, data={
            "task_id": str(TASK_ID), "folder_id": "group-new", "kind": "preprocessing_started", "data": {},
        }))

    return counts, flow_pages, parent_pages, child_pages, add_child


def call_endpoint():
    app = api.create_app({"deployment_id": DEPLOYMENT_ID})

    async def request():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/api/v1/prefect/folder-tickets", params={"task_id": str(TASK_ID)})
            assert response.status_code == 200
            return response.json()

    with temporary_settings({PREFECT_API_URL: "http://127.0.0.1:1/api"}):
        return asyncio.run(request())


@pytest.mark.parametrize("case", CASES)
def test_existing_response_and_one_logical_read_per_collection(monkeypatch, case):
    counts, flow_pages, parent_pages, child_pages, _add_child = setup_fake_prefect(monkeypatch, case)
    actual = call_endpoint()
    reference = json.loads(REFERENCE.read_text(encoding="utf-8"))[case]
    assert actual == reference
    assert counts == {"parent_run": 1, "parent_receipts": 1, "child_receipts": 1}
    assert flow_pages == [0] and parent_pages == [0]
    assert child_pages == ([] if case == "unknown_dispatch" else
                           [(3 if case in ("wrong_child", "duplicate_group") else 2, 0)])


def test_pages_are_read_once_and_keep_receipts_in_descending_order(monkeypatch):
    counts, flow_pages, parent_pages, child_pages, _add_child = setup_fake_prefect(monkeypatch, "bulk")
    actual = call_endpoint()
    assert actual["complete"] is True and len(actual["folders"]) == 201
    assert actual["folders"][0]["preprocessing_result"] == "passed"
    assert actual["folders"][0]["stage"] == "waiting_llm"
    assert counts == {"parent_run": 1, "parent_receipts": 1, "child_receipts": 1}
    assert flow_pages == [0, 200]
    assert parent_pages == [0, 200]
    assert child_pages == [(200, 0), (200, 200), (1, 0)]


def test_current_response_reuses_first_child_collection(monkeypatch):
    counts, flow_pages, parent_pages, child_pages, _add_child = setup_fake_prefect(monkeypatch, "mutating")
    actual = call_endpoint()
    reference = json.loads(REFERENCE.read_text(encoding="utf-8"))["normal"]
    assert actual == reference
    assert counts == {"parent_run": 1, "parent_receipts": 1, "child_receipts": 1}
    assert flow_pages == [0] and parent_pages == [0] and child_pages == [(2, 0)]


def test_next_request_reads_new_data(monkeypatch):
    counts, flow_pages, parent_pages, child_pages, add_child = setup_fake_prefect(monkeypatch, "normal")
    app = api.create_app({"deployment_id": DEPLOYMENT_ID})

    async def requests():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            first_response = await client.get("/api/v1/prefect/folder-tickets", params={"task_id": str(TASK_ID)})
            add_child()
            second_response = await client.get("/api/v1/prefect/folder-tickets", params={"task_id": str(TASK_ID)})
        assert first_response.status_code == second_response.status_code == 200
        return first_response.json(), second_response.json()

    with temporary_settings({PREFECT_API_URL: "http://127.0.0.1:1/api"}):
        first, second = asyncio.run(requests())
    assert [row["folder_id"] for row in first["folders"]] == ["group-a", "group-b"]
    assert [row["folder_id"] for row in second["folders"]] == ["group-a", "group-b", "group-new"]
    assert counts == {"parent_run": 2, "parent_receipts": 2, "child_receipts": 2}
    assert flow_pages == [0, 0] and parent_pages == [0, 0]
    assert child_pages == [(2, 0), (3, 0)]


@pytest.mark.parametrize("collection", ["parent", "children", "flow_runs"])
def test_page_failure_is_not_reported_as_empty_data(monkeypatch, collection):
    setup_fake_prefect(monkeypatch, "bulk")
    original_read = PrefectClient.read_artifacts
    original_read_flows = PrefectClient.read_flow_runs

    async def fail_second_page(client, *, artifact_filter, sort, limit, offset):
        is_parent = artifact_filter.flow_run_id.any_ == [TASK_ID]
        if collection != "flow_runs" and offset == 200 and is_parent == (collection == "parent"):
            raise RuntimeError("synthetic receipt page failure")
        return await original_read(client, artifact_filter=artifact_filter, sort=sort, limit=limit, offset=offset)

    async def fail_second_flow_page(client, *, flow_run_filter, limit, offset):
        if collection == "flow_runs" and offset == 200:
            raise RuntimeError("synthetic flow page failure")
        return await original_read_flows(client, flow_run_filter=flow_run_filter, limit=limit, offset=offset)

    monkeypatch.setattr(PrefectClient, "read_artifacts", fail_second_page)
    monkeypatch.setattr(PrefectClient, "read_flow_runs", fail_second_flow_page)
    app = api.create_app({"deployment_id": DEPLOYMENT_ID})

    async def request():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                     base_url="http://test") as client:
            return await client.get("/api/v1/prefect/folder-tickets", params={"task_id": str(TASK_ID)})

    with temporary_settings({PREFECT_API_URL: "http://127.0.0.1:1/api"}):
        response = asyncio.run(request())
    assert response.status_code == 500
