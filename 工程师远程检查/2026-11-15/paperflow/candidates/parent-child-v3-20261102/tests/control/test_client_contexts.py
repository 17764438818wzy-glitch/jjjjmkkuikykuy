"""Prefect clients belong to one API request or one business-operation thread."""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
from fastapi import HTTPException
from prefect.client.orchestration import PrefectClient, SyncPrefectClient, get_client
from prefect.settings import PREFECT_API_URL, temporary_settings
from prefect.variables import Variable


def test_api_reuses_and_closes_one_client_per_request_without_hiding_preflight(monkeypatch, tmp_path):
    from paperflow_v3.control import preflight
    from paperflow_v3.control.api import create_app

    clients = []
    original_init = PrefectClient.__init__

    def init(client, *args, **kwargs):
        original_init(client, *args, **kwargs)
        clients.append(client)

    async def no_version_check(_client):
        pass

    async def read_variable(client, name):
        if name.startswith("paperflow_v3_operation_"):
            return None
        return SimpleNamespace(value=id(client))

    async def failed_preflight(_settings):
        return {"ready": False, "checks": [{"name": "synthetic", "ready": False}]}

    monkeypatch.setattr(PrefectClient, "__init__", init)
    monkeypatch.setattr(PrefectClient, "raise_for_api_version_mismatch_once", no_version_check)
    monkeypatch.setattr(PrefectClient, "read_variable_by_name", read_variable)
    monkeypatch.setattr(preflight, "inspect_dependencies", failed_preflight)
    app = create_app({"deployment_id": uuid4()})

    @app.get("/api/v1/prefect/probe")
    async def probe():
        async with get_client() as first:
            await asyncio.sleep(0)
            async with get_client() as second:
                return {"first": id(first), "second": id(second), "variable": await Variable.aget("test")}

    @app.get("/api/v1/prefect/probe-failure")
    async def probe_failure():
        async with get_client():
            raise HTTPException(409, "synthetic failure")

    @app.get("/unrelated")
    async def unrelated():
        return {"ok": True}

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            preflight_result = await client.post("/api/v1/prefect/preflight")
            creation_result = await client.post("/api/v1/prefect/tasks", json={
                "request_id": str(uuid4()), "task_name": "synthetic", "source_paths": [str(tmp_path)],
            })
            assert preflight_result.status_code == 200 and preflight_result.json()["ready"] is False
            assert creation_result.status_code == 409 and creation_result.json()["detail"]["ready"] is False
            assert (await client.get("/unrelated")).status_code == 200
            # Creation first checks its durable request identity, then runs
            # preflight for new requests; that one client is already closed.
            assert len(clients) == 1 and clients[0]._closed

            responses = await asyncio.gather(*[client.get("/api/v1/prefect/probe") for _ in range(2)])
            assert all(response.status_code == 200 for response in responses)
            results = [response.json() for response in responses]
            assert all(row["first"] == row["second"] == row["variable"] for row in results)
            assert results[0]["first"] != results[1]["first"]
            assert len(clients) == 3 and all(item._closed for item in clients)

            assert (await client.get("/api/v1/prefect/probe-failure")).status_code == 409
            assert len(clients) == 4 and all(item._closed for item in clients)

    with temporary_settings({PREFECT_API_URL: "http://127.0.0.1:1/api"}):
        asyncio.run(exercise())


def test_delivery_reuses_thread_client_and_reads_current_pause_state(monkeypatch, stage_store):
    candidate = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(candidate.parents[1] / "prefect-paperflow"))
    from paperflow_prefect import legacy
    from paperflow_v3.control import flows
    from paperflow_v3.control.admission import Admission
    from paperflow_v3.control.receipts import read_receipts_sync, save_receipt_sync
    from paperflow_v3.delivery import runtime

    clients, calls = [], []
    original_init = SyncPrefectClient.__init__
    state = {"state": "running", "session": "test-session"}

    def init(client, *args, **kwargs):
        original_init(client, *args, **kwargs)
        clients.append(client)

    def read_variable(client, name):
        calls.append(client)
        return SimpleNamespace(value="test-session" if name == "paperflow_v3_control_session" else state.copy())

    def read_artifacts(client, **_kwargs):
        calls.append(client)
        return []

    def create_artifact(client, _artifact):
        calls.append(client)
        return SimpleNamespace(id=uuid4())

    def delivery(**_kwargs):
        assert _kwargs['admission'].admit_sync()
        task_id = uuid4()
        admission = Admission(task_id)
        assert admission.running_sync() is True
        state.update(state="paused", session=None)
        assert admission.running_sync() is False
        assert read_receipts_sync(task_id) == []
        save_receipt_sync(task_id=task_id, folder_id="test", child_flow_run_id=task_id,
                          event_id=uuid4(), kind="test", data={})
        return {"status": "complete"}

    monkeypatch.setattr(SyncPrefectClient, "__init__", init)
    monkeypatch.setattr(SyncPrefectClient, "raise_for_api_version_mismatch_once", lambda _client: None)
    monkeypatch.setattr(SyncPrefectClient, "read_variable_by_name", read_variable)
    monkeypatch.setattr(SyncPrefectClient, "read_artifacts", read_artifacts)
    monkeypatch.setattr(SyncPrefectClient, "create_artifact", create_artifact)
    monkeypatch.setattr(legacy, "bind", lambda *_args: None)
    monkeypatch.setattr(runtime, "deliver_paper", delivery)

    async def records(_run):
        return []

    monkeypatch.setattr(flows, 'read_receipts', records)

    async def exercise():
        ids = {'task_id': str(uuid4()), 'task_run_id': str(uuid4()), 'group_id': 'folder'}
        parent = stage_store.admission.Admission(ids['task_id'])
        await parent.start()
        return await flows._delivery_step(ids, {}, object(), parent, {'rag': ['rag']}, None, False)

    with temporary_settings({PREFECT_API_URL: "http://127.0.0.1:1/api"}):
        result = asyncio.run(exercise())
    assert result == {"status": "complete"}
    assert len(clients) == 1 and len(calls) == 6
    assert all(client is clients[0] for client in calls) and clients[0]._closed
