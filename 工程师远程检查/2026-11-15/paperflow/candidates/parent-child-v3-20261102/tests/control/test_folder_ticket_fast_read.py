"""The list omits obsolete progress snapshots without changing projected results."""

import asyncio
import hashlib
import re
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest
from prefect.client.orchestration import PrefectClient
from prefect.settings import PREFECT_API_URL, temporary_settings
from prefect.states import Completed, Failed, Running

from paperflow_v3.control import api
from paperflow_v3.control.receipts import _artifact


TASK_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
DEPLOYMENT_ID = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
START = datetime(2026, 11, 2, 14, 0, tzinfo=timezone.utc)
RETRY_1 = "11111111-1111-4111-8111-111111111111"
RETRY_2 = "22222222-2222-4222-a222-222222222222"


def synthetic_data():
    children, artifacts = [], []
    sequence = 0

    def save(flow_id, folder_id, kind, event_id, data):
        nonlocal sequence
        sequence += 1
        spec = _artifact(task_id=TASK_ID, folder_id=folder_id, child_flow_run_id=flow_id,
                         event_id=event_id, kind=kind, data=data)
        artifacts.append(SimpleNamespace(id=UUID(int=sequence + 100), flow_run_id=flow_id, key=spec.key,
                                         created=START + timedelta(microseconds=sequence), data=spec.data))

    group_ids = [f"group-{index}" for index in range(8)]
    save(TASK_ID, "task", "classification_complete", "parent-complete",
         {"group_count": len(group_ids), "group_ids": group_ids, "ungrouped": []})
    for index, group in enumerate(group_ids):
        save(TASK_ID, group, "classification", "parent-" + group,
             {"group_id": group, "status": "passed", "delivery_complete": True,
              "registration_status": "registered"})
        child_id = UUID(int=index + 1)
        registered = {"task_id": str(TASK_ID), "group_id": group, "paper_id": "paper-" + group,
                      "receipt_id": "receipt-" + group, "group": group, "paper_title": "title"}
        state = Completed() if index < 5 else Failed() if index == 5 else Running()
        child = SimpleNamespace(id=child_id, state=state,
                                parameters={"task_id": str(TASK_ID), "group_id": group, "registered": registered})
        children.append(child)
        identity = api._folder_identity(child)
        save(child_id, group, "preprocessing_complete", f"{child_id}-preprocessing-initial-complete", identity)
        save(child_id, group, "llm_folder_complete", f"{child_id}-llm-complete",
             {"status": "raw_complete", "ranking_complete": True})
        save(child_id, group, "postprocessing_complete", f"{child_id}-postprocessing-complete",
             {"identity": identity, "validation_status": "passed"})
        save(child_id, group, "delivery_stage", f"deliverystage-{index:064x}",
             {"identity": identity, "stage": "rag_in_progress"})
        for number in range(80):
            effect_id = hashlib.sha256(f"effect-{index}-{number}".encode()).hexdigest()
            save(child_id, group, "rag_effect_intent", effect_id + "-intent", {"effect_id": effect_id})
            if index != 6 or number != 79:
                save(child_id, group, "rag_effect_returned", effect_id + "-returned",
                     {"effect_id": effect_id, "result": {"value": "x" * 100}})
            digest = hashlib.sha256(f"snapshot-{index}-{number}".encode()).hexdigest()
            save(child_id, group, "rag-body", "rag-body-" + digest,
                 {"route": {"body_delivery_state": "complete", "payload": "x" * 1500}})
        if index == 4:
            save(child_id, group, "manual_retry", RETRY_1,
                 {"retry_id": RETRY_1, "state": "authorized", "stage": "nas"})
            save(child_id, group, "manual_retry", RETRY_1 + "-running",
                 {"retry_id": RETRY_1, "state": "running", "stage": "nas"})
        if index == 5:
            save(child_id, group, "manual_retry", RETRY_1,
                 {"retry_id": RETRY_1, "state": "authorized", "stage": "nas"})
            save(child_id, group, "manual_retry", RETRY_1 + "-running",
                 {"retry_id": RETRY_1, "state": "running", "stage": "nas"})
            save(child_id, group, "manual_retry", RETRY_2,
                 {"retry_id": RETRY_2, "state": "authorized", "stage": "nas"})
        if index < 5:
            save(child_id, group, "cleanup", "cleanup-" + str(index),
                 {"identity": identity, "status": "complete", "files": {}})
        if index == 7:
            # Parent is paused, but this native child remains RUNNING after a known unsent request.
            effect_id = hashlib.sha256(b"known-unsent").hexdigest()
            save(child_id, group, "rag_effect_intent", effect_id + "-intent", {"effect_id": effect_id})
            save(child_id, group, "rag_effect_not_sent", effect_id + "-not-sent", {"effect_id": effect_id})
            save(child_id, group, "manual_retry", RETRY_1,
                 {"retry_id": RETRY_1, "state": "authorized", "stage": "nas"})
            save(child_id, group, "manual_retry", RETRY_1 + "-running",
                 {"retry_id": RETRY_1, "state": "running", "stage": "nas"})
            save(child_id, group, "manual_retry", RETRY_2,
                 {"retry_id": RETRY_2, "state": "authorized", "stage": "nas"})
    return children, artifacts


def run_view(monkeypatch, *, full_reference=False, mutate_after_snapshots=False):
    children, artifacts = synthetic_data()
    stats = Counter()
    mutated = False
    parent = SimpleNamespace(id=TASK_ID, deployment_id=DEPLOYMENT_ID, state=Running(), run_count=1,
                             parameters={"task_name": "synthetic"})

    async def no_version(_client):
        pass

    async def read_flow_run(_client, run_id):
        assert run_id == TASK_ID
        return parent

    async def read_flow_runs(_client, *, flow_run_filter, limit, offset):
        assert flow_run_filter.parent_flow_run_id.any_ == [TASK_ID]
        return children[offset:offset + limit]

    async def read_artifacts(_client, *, artifact_filter, sort, limit, offset):
        nonlocal mutated
        if (mutate_after_snapshots and not mutated and artifact_filter.key is not None
                and artifact_filter.key.like_ == "intent"):
            mutated = True
            child = children[7]
            effect_id = "e" * 64
            changes = [("rag_effect_intent", effect_id + "-intent", {"effect_id": effect_id})]
            changes.extend(("rag-body", "rag-body-new-after-snapshot-" + str(number),
                            {"route": {"body_delivery_state": "failed"}}) for number in range(30))
            for number, (kind, event_id, data) in enumerate(changes):
                spec = _artifact(task_id=TASK_ID, folder_id=child.parameters["group_id"],
                                 child_flow_run_id=child.id, event_id=event_id, kind=kind, data=data)
                artifacts.append(SimpleNamespace(id=UUID(int=100000 + number), flow_run_id=child.id,
                                                 key=spec.key, created=START + timedelta(days=1, seconds=number),
                                                 data=spec.data))
        ids = set(artifact_filter.flow_run_id.any_)
        matches = [row for row in artifacts if row.flow_run_id in ids]
        if artifact_filter.key is not None:
            if artifact_filter.key.any_ is not None:
                matches = [row for row in matches if row.key in artifact_filter.key.any_]
            if artifact_filter.key.like_ is not None:
                pattern = "".join(".*" if char == "%" else "." if char == "_" else re.escape(char)
                                  for char in artifact_filter.key.like_)
                # Prefect wraps the supplied LIKE pattern in '%' on both sides.
                matches = [row for row in matches if re.search(pattern, row.key, re.IGNORECASE)]
        matches.sort(key=lambda row: row.created, reverse=True)
        result = matches[offset:offset + limit]
        for row in result:
            if row.data["kind"] == "rag-body":
                label = "healthy" if row.flow_run_id in {c.id for c in children[:5] + children[6:]} else "fallback"
                stats[label + "_snapshots"] += 1
        return result

    async def running(_admission):
        return False

    monkeypatch.setattr(PrefectClient, "raise_for_api_version_mismatch_once", no_version)
    monkeypatch.setattr(PrefectClient, "read_flow_run", read_flow_run)
    monkeypatch.setattr(PrefectClient, "read_flow_runs", read_flow_runs)
    monkeypatch.setattr(PrefectClient, "read_artifacts", read_artifacts)
    monkeypatch.setattr(api.Admission, "running", running)

    async def full(children):
        return await api.read_folder_receipts([child.id for child in children])

    app = api.create_app({"deployment_id": DEPLOYMENT_ID})

    async def request():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/api/v1/prefect/folder-tickets", params={"task_id": str(TASK_ID)})
            assert response.status_code == 200
            value = response.json()
            value.pop("as_of")
            return value

    with temporary_settings({PREFECT_API_URL: "http://127.0.0.1:1/api"}):
        if full_reference:
            with monkeypatch.context() as patch:
                patch.setattr(api, "_folder_ticket_records", full)
                return asyncio.run(request()), stats
        return asyncio.run(request()), stats


def test_completed_paused_and_unknown_match_full_history_without_old_snapshots(monkeypatch):
    with monkeypatch.context() as reference_patch:
        reference, reference_stats = run_view(reference_patch, full_reference=True)
    actual, actual_stats = run_view(monkeypatch)
    assert actual == reference
    assert actual["desired_state"] == "paused"
    assert [row["stage"] for row in actual["folders"][:5]] == ["completed"] * 5
    assert actual["folders"][7]["stage"] == "rag_in_progress"
    assert actual["folders"][7]["last_retry"]["retry_id"] == RETRY_2
    assert actual["folders"][7]["last_retry"]["status"] == "running"
    assert actual["folders"][6]["retry_reason"] == "入库请求结果待核对，不能重发"
    assert actual["folders"][5]["last_retry"]["retry_id"] == RETRY_2
    assert actual["folders"][5]["last_retry"]["status"] == "dispatch_unknown"
    assert reference_stats["healthy_snapshots"] == 7 * 80
    # UUID4 key searches can incidentally match a small number of longer snapshot keys.
    assert actual_stats["healthy_snapshots"] < reference_stats["healthy_snapshots"] // 4
    assert actual_stats["fallback_snapshots"] >= 80


def test_request_started_after_snapshot_is_still_read(monkeypatch):
    with monkeypatch.context() as reference_patch:
        reference, _stats = run_view(reference_patch, full_reference=True)
    actual, _stats = run_view(monkeypatch, mutate_after_snapshots=True)
    reference["folders"][7]["retry_reason"] = "入库请求结果待核对，不能重发"
    assert actual == reference


def test_receipt_query_failure_cancels_other_queries_before_client_closes(monkeypatch):
    from paperflow_v3.control import receipts

    events = []

    async def exercise():
        second_started = asyncio.Event()

        class Client:
            async def read_artifacts(self, *, artifact_filter, **_kwargs):
                if artifact_filter.key.like_ == "broken":
                    await second_started.wait()
                    raise RuntimeError("synthetic receipt failure")
                second_started.set()
                try:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    events.append("cancelled")
                    raise

        @asynccontextmanager
        async def get_client():
            try:
                yield Client()
            finally:
                events.append("closed")

        monkeypatch.setattr(receipts, "get_client", get_client)
        with pytest.raises(ExceptionGroup):
            await receipts.read_folder_receipts_by_key([UUID(int=1)], patterns=("broken", "wait"))

    asyncio.run(exercise())
    assert events == ["cancelled", "closed"]


def test_latest_snapshots_and_retry_authorization_on_isolated_prefect():
    from prefect import flow
    from prefect.client.orchestration import get_client
    from prefect.testing.utilities import prefect_test_harness
    from paperflow_v3.control.receipts import read_folder_receipts, save_receipt
    from paperflow_v3.control.retry import retry_decision

    @flow(persist_result=False)
    def isolated_paper():
        pass

    async def exercise():
        async with get_client() as client:
            native_run = await client.create_flow_run(isolated_paper)
        group = "isolated"
        registered = {"task_id": str(TASK_ID), "group_id": group, "paper_id": "paper", "receipt_id": "receipt"}
        child = SimpleNamespace(id=native_run.id, state=Completed(),
                                parameters={"task_id": str(TASK_ID), "group_id": group, "registered": registered})
        identity = api._folder_identity(child)

        async def save(kind, event_id, data):
            await save_receipt(task_id=TASK_ID, folder_id=group, child_flow_run_id=child.id,
                               event_id=event_id, kind=kind, data=data)

        await save("preprocessing_complete", str(child.id) + "-preprocessing-complete", identity)
        await save("llm_folder_complete", str(child.id) + "-llm-complete",
                   {"status": "raw_complete", "ranking_complete": True})
        await save("postprocessing_complete", str(child.id) + "-postprocessing-complete",
                   {"identity": identity, "validation_status": "passed"})
        effect_id = "f" * 64
        await save("rag_effect_intent", effect_id + "-intent", {"effect_id": effect_id})
        await save("rag_effect_returned", effect_id + "-returned", {"effect_id": effect_id, "result": {"ok": True}})
        await save("rag-body", "rag-body-old", {"route": {"body_delivery_state": "in_flight"}})
        await save("rag-body", "rag-body-new", {"route": {"body_delivery_state": "complete"}})
        await save("manual_retry", RETRY_1, {"retry_id": RETRY_1, "state": "authorized", "stage": "nas"})
        await save("manual_retry", RETRY_1 + "-running",
                   {"retry_id": RETRY_1, "state": "running", "stage": "nas"})
        await save("cleanup", "cleanup-latest", {"identity": identity, "status": "complete", "files": {}})
        full = (await read_folder_receipts([child.id]))[str(child.id)]
        selected = (await api._folder_ticket_records([child]))[str(child.id)]
        assert len([row for row in selected if row["kind"] == "rag-body"]) == 1
        assert api.folder_status(selected, identity=identity) == api.folder_status(full, identity=identity)
        assert retry_decision(selected, identity=identity, registered=registered) == retry_decision(
            full, identity=identity, registered=registered)
        assert api._last_retry(selected, child, "completed") == api._last_retry(full, child, "completed")

    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(exercise())
