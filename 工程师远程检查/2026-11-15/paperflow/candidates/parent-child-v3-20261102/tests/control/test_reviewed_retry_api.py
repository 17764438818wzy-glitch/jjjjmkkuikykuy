"""A reviewed failure still uses the original manual-retry endpoint and child."""

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import httpx
from prefect.client.orchestration import PrefectClient
from prefect.settings import PREFECT_API_URL, temporary_settings
from prefect.states import Failed, Scheduled
from prefect.variables import Variable


def test_manual_retry_carries_the_review_and_refuses_changed_evidence(monkeypatch):
    from paperflow_v3.control import api, preflight

    task_id, child_id, deployment_id = uuid4(), uuid4(), uuid4()
    identity = {"task_id": str(task_id), "group_id": "folder", "paper_id": "paper",
                "receipt_id": "receipt", "attempt_id": str(child_id)}
    body_sha, receipt_sha = "1" * 64, "2" * 64
    registered = {**{key: identity[key] for key in ("task_id", "group_id", "paper_id", "receipt_id")},
        "receipt_sha256": receipt_sha,
        "paper": {"body": {"sha256": body_sha}, "attachments": [], "archive_files": [{"sha256": body_sha}]}}
    failure = {"kind": "preprocessing_failed", "event_id": "failure-1", "task_id": str(task_id),
        "folder_id": "folder", "data": {**identity, "processing_attempt_id": "initial",
                                         "failure_reason": "preprocessing_unconfirmed", "retryable": False}}
    review = {"kind": "preprocessing_reviewed", "event_id": "review-1", "task_id": str(task_id),
        "folder_id": "folder", "data": {"identity": identity, "failure_event_id": "failure-1",
            "processing_attempt_id": "initial", "receipt_sha256": receipt_sha, "source_sha256": [body_sha],
            "delivery_sha256": [body_sha], "archive_sha256": [body_sha], "cached_docx_sha256": "3" * 64,
            "observed_at": "2026-11-02T13:30:00+00:00", "conversion_observation": "no_owned_process_observed",
            "local_request_history": "none_observed", "downstream_events": "none_observed"}}
    parent = SimpleNamespace(deployment_id=deployment_id)
    child = SimpleNamespace(id=child_id, state=Failed(),
        parameters={"task_id": str(task_id), "group_id": "folder", "registered": registered})
    saved, transitions = [], []
    variables = {}

    async def no_version_check(_client):
        pass

    async def read_variable(name, *args, **kwargs):
        return variables.get(name)

    async def write_variable(name, value, *args, **kwargs):
        variables[name] = value.copy()

    async def read_flow_run(_client, _run_id):
        return parent

    async def set_flow_run_state(_client, run_id, _state):
        transitions.append(run_id)
        return SimpleNamespace(state=Scheduled())

    async def children(_task_id):
        return [child]

    async def receipts(_child_id):
        return [review, failure]

    async def save(**kwargs):
        saved.append(kwargs)

    async def ready(_settings):
        return {"ready": True, "checks": []}

    monkeypatch.setattr(PrefectClient, "raise_for_api_version_mismatch_once", no_version_check)
    monkeypatch.setattr(Variable, "aget", staticmethod(read_variable))
    monkeypatch.setattr(Variable, "aset", staticmethod(write_variable))
    monkeypatch.setattr(PrefectClient, "read_flow_run", read_flow_run)
    monkeypatch.setattr(PrefectClient, "set_flow_run_state", set_flow_run_state)
    monkeypatch.setattr(api, "_children", children)
    monkeypatch.setattr(api, "read_receipts", receipts)
    monkeypatch.setattr(api, "save_receipt", save)
    monkeypatch.setattr(preflight, "inspect_dependencies", ready)
    app = api.create_app({"deployment_id": deployment_id})

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            url = f"/api/v1/prefect/tasks/{task_id}/folders/folder/retry"
            result = await client.post(url, json={"request_id": str(uuid4())})
            assert result.status_code == 200 and result.json()["ticket_id"] == str(child_id)
            assert len(saved) == 1 and saved[0]["kind"] == "manual_retry"
            assert saved[0]["data"]["review_event_id"] == "review-1"
            assert saved[0]["data"]["failure_event_id"] == "failure-1"
            assert transitions == [child_id]
            review["data"]["receipt_sha256"] = "changed"
            assert (await client.post(url, json={"request_id": str(uuid4())})).status_code == 409
            assert len(saved) == 1 and transitions == [child_id]

    with temporary_settings({PREFECT_API_URL: "http://127.0.0.1:1/api"}):
        asyncio.run(exercise())
