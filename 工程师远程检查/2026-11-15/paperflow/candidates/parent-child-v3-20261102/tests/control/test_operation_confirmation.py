"""Exercise task operations with isolated Prefect boundaries and synthetic runs."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
from prefect.client.orchestration import PrefectClient
from prefect.states import Completed, Failed, Paused, Running, Scheduled
from prefect.variables import Variable

from paperflow_v3.control import api, preflight
from paperflow_v3.control.admission import Admission


TASK_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
DEPLOYMENT_ID = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")


def control_app(monkeypatch, check, *, variables=None, stage_backend=False, **capacity_adapters):
    if not stage_backend:
        from paperflow_v3.control import admission, stage_admission
        @asynccontextmanager
        async def gate(_task_id):
            yield
        async def no_stages(_task_id, **kwargs):
            return []
        async def no_legacy(_task_id, snapshot=None, **kwargs):
            return set()
        monkeypatch.setattr(admission, 'control_gate', gate)
        monkeypatch.setattr(stage_admission, '_stage_rows', no_stages)
        monkeypatch.setattr(stage_admission, '_legacy_pause_folders', no_legacy)
    if variables is None:
        variables = {"paperflow_v3_control_session": "isolated-session"}
    writes = []
    run = SimpleNamespace(id=TASK_ID, deployment_id=DEPLOYMENT_ID, state=Running(), run_count=1,
                          parameters={"task_name": "synthetic"})

    async def get_variable(name, *args, **kwargs):
        return deepcopy(variables.get(name))

    async def set_variable(name, value, *args, **kwargs):
        writes.append(name)
        variables[name] = deepcopy(value)

    class Client:
        async def read_flow_run(self, task_id):
            assert task_id == TASK_ID
            return run

        async def read_flow_runs(self, **_kwargs):
            return []

        async def count_flow_runs(self, **_kwargs):
            return 0

    @asynccontextmanager
    async def isolated_client():
        yield Client()

    async def children(_task_id):
        return []

    async def receipts(_run_id):
        return []

    async def no_version_check(_client):
        pass

    monkeypatch.setattr(Variable, "aget", staticmethod(get_variable))
    monkeypatch.setattr(Variable, "aset", staticmethod(set_variable))
    monkeypatch.setattr(api, "get_client", isolated_client)
    monkeypatch.setattr(api, "_children", children)
    monkeypatch.setattr(api, "read_receipts", receipts)
    monkeypatch.setattr(preflight, "inspect_dependencies", check)
    monkeypatch.setattr(PrefectClient, "raise_for_api_version_mismatch_once", no_version_check)
    async def tested_accounts(*, ensure_current):
        await ensure_current()
        return {"successful_count": 1, "account_uids": ["synthetic-cn-account"]}

    async def applied_capacity(result, limit, *, ensure_current):
        await ensure_current()
        assert result["successful_count"] == 1 and limit == 2

    capacity_adapters.setdefault("test_start_accounts", tested_accounts)
    capacity_adapters.setdefault("apply_start_capacity", applied_capacity)
    app = api.create_app({"deployment_id": DEPLOYMENT_ID}, **capacity_adapters)
    app.state.isolated_run = run
    app.state.isolated_variables = variables
    app.state.isolated_writes = writes
    return app


def test_pause_while_start_preflight_is_pending_wins_over_old_start(monkeypatch):
    async def exercise():
        preflight_started, finish_preflight = asyncio.Event(), asyncio.Event()

        async def slow_check(_settings):
            preflight_started.set()
            await finish_preflight.wait()
            return {"ready": True, "checks": []}

        app = control_app(monkeypatch, slow_check)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                     base_url="http://isolated") as client:
            start_id, pause_id = str(uuid4()), str(uuid4())
            start = asyncio.create_task(client.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",
                json={"request_id": start_id, "intent_seq": 1}))
            await asyncio.wait_for(preflight_started.wait(), timeout=3)
            pause = await asyncio.wait_for(client.post(f"/api/v1/prefect/tasks/{TASK_ID}/pause",
                json={"request_id": pause_id, "intent_seq": 2}), timeout=3)
            finish_preflight.set()
            started = await start

            assert pause.status_code == 200, pause.text
            assert pause.json()["request_id"] == pause_id
            assert started.json()["status"] == "superseded"
            assert not await Admission(TASK_ID).running()
            old = (await client.get(f"/api/v1/prefect/operations/{start_id}")).json()
            latest = (await client.get(f"/api/v1/prefect/operations/{pause_id}")).json()
            assert old["status"] == "superseded"
            assert latest["status"] == "confirmed" and latest["resolved"] is True

    asyncio.run(exercise())


def test_late_start_request_cannot_overwrite_later_pause(monkeypatch):
    async def exercise():
        async def unexpected_check(_settings):
            raise AssertionError("a superseded start must not run preflight")

        app = control_app(monkeypatch, unexpected_check)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                     base_url="http://isolated") as client:
            pause_id, start_id = str(uuid4()), str(uuid4())
            pause = await client.post(f"/api/v1/prefect/tasks/{TASK_ID}/pause",
                                      json={"request_id": pause_id, "intent_seq": 2})
            start = await client.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",
                                      json={"request_id": start_id, "intent_seq": 1})
            assert pause.status_code == 200 and pause.json()["status"] == "confirmed"
            assert start.status_code == 200 and start.json()["status"] == "superseded"
            assert not await Admission(TASK_ID).running()

    asyncio.run(exercise())


def test_pause_intent_survives_lost_operation_record_and_restart(monkeypatch):
    async def exercise():
        async def ready(_settings):
            return {"ready": True, "checks": []}

        app = control_app(monkeypatch, ready)
        variables = app.state.isolated_variables
        pause_id, start_id = str(uuid4()), str(uuid4())
        operation_key = "paperflow_v3_operation_" + pause_id.replace("-", "_")
        original_set = Variable.aset

        async def lose_operation(name, value, *args, **kwargs):
            if name == operation_key:
                raise ConnectionError("isolated operation record loss")
            await original_set(name, value, *args, **kwargs)

        monkeypatch.setattr(Variable, "aset", staticmethod(lose_operation))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                     base_url="http://isolated") as client:
            pause = await client.post(f"/api/v1/prefect/tasks/{TASK_ID}/pause",
                json={"request_id": pause_id, "intent_seq": 2})
            assert pause.status_code == 500

        restarted = control_app(monkeypatch, ready, variables=variables)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=restarted, raise_app_exceptions=False),
                                     base_url="http://isolated") as client:
            old = await client.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",
                json={"request_id": start_id, "intent_seq": 1})
            assert old.json()["status"] == "superseded"
            assert not await Admission(TASK_ID).running()
            result = (await client.get(f"/api/v1/prefect/operations/{pause_id}")).json()
            assert result["status"] == "unknown" and result["resolved"] is False
            duplicate = await client.post(f"/api/v1/prefect/tasks/{TASK_ID}/pause",
                json={"request_id": pause_id, "intent_seq": 2})
            assert duplicate.json()["status"] == "unknown"
            gate = "paperflow_v3_task_" + str(TASK_ID).replace("-", "_")
            assert gate not in restarted.state.isolated_writes

    asyncio.run(exercise())


def test_start_stays_unknown_when_prefect_does_not_resume_parent(monkeypatch):
    async def exercise():
        async def ready(_settings):
            return {"ready": True, "checks": []}

        async def no_resume(_task_id):
            pass

        app = control_app(monkeypatch, ready)
        app.state.isolated_run.state = Paused()
        monkeypatch.setattr(api, "aresume_flow_run", no_resume)
        request_id = str(uuid4())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                     base_url="http://isolated") as client:
            result = await client.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",
                json={"request_id": request_id, "intent_seq": 1})
            assert result.status_code == 200, result.text
            assert result.json()["status"] == "unknown" and result.json()["resolved"] is False
            lookup = (await client.get(f"/api/v1/prefect/operations/{request_id}")).json()
            assert lookup["status"] == "unknown" and lookup["resolved"] is False

    asyncio.run(exercise())


def test_start_confirms_when_resumed_parent_immediately_completes(monkeypatch):
    async def exercise():
        async def ready(_settings):
            return {"ready": True, "checks": []}

        app = control_app(monkeypatch, ready)
        app.state.isolated_run.state = Paused()

        async def resume_and_finish(_task_id):
            app.state.isolated_run.state = Completed()

        monkeypatch.setattr(api, "aresume_flow_run", resume_and_finish)
        request_id = str(uuid4())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                     base_url="http://isolated") as client:
            result = await client.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",
                json={"request_id": request_id, "intent_seq": 1})
            assert result.status_code == 200, result.text
            assert result.json()["status"] == "confirmed" and result.json()["resolved"] is True
            lookup = (await client.get(f"/api/v1/prefect/operations/{request_id}")).json()
            assert lookup["status"] == "confirmed" and lookup["resolved"] is True

    asyncio.run(exercise())


def test_late_start_response_cannot_undo_confirmed_pause(monkeypatch):
    async def exercise():
        resume_started, finish_resume = asyncio.Event(), asyncio.Event()

        async def ready(_settings):
            return {"ready": True, "checks": []}

        async def slow_resume(_task_id):
            resume_started.set()
            await finish_resume.wait()

        app = control_app(monkeypatch, ready)
        app.state.isolated_run.state = Paused()
        monkeypatch.setattr(api, "aresume_flow_run", slow_resume)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                     base_url="http://isolated") as client:
            start_id, pause_id = str(uuid4()), str(uuid4())
            start = asyncio.create_task(client.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",
                json={"request_id": start_id, "intent_seq": 1}))
            await asyncio.wait_for(resume_started.wait(), timeout=3)
            pause = await asyncio.wait_for(client.post(f"/api/v1/prefect/tasks/{TASK_ID}/pause",
                json={"request_id": pause_id, "intent_seq": 2}), timeout=3)
            finish_resume.set()
            started = await start
            assert pause.json()["status"] == "confirmed"
            assert started.json()["status"] == "superseded"
            assert not await Admission(TASK_ID).running()

    asyncio.run(exercise())


def test_unknown_pause_is_rechecked_after_restart_without_reapplying(monkeypatch):
    async def exercise():
        async def ready(_settings):
            return {"ready": True, "checks": []}

        app = control_app(monkeypatch, ready)
        variables, writes = app.state.isolated_variables, app.state.isolated_writes
        request_id = str(uuid4())
        operation_key = "paperflow_v3_operation_" + request_id.replace("-", "_")
        original_set = Variable.aset

        async def lose_confirmation(name, value, *args, **kwargs):
            if name == operation_key and value.get("status") == "confirmed":
                raise ConnectionError("isolated confirmation loss")
            await original_set(name, value, *args, **kwargs)

        monkeypatch.setattr(Variable, "aset", staticmethod(lose_confirmation))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                     base_url="http://isolated") as client:
            response = await client.post(f"/api/v1/prefect/tasks/{TASK_ID}/pause",
                json={"request_id": request_id, "intent_seq": 1})
            assert response.status_code == 500
        gate_key = "paperflow_v3_task_" + str(TASK_ID).replace("-", "_")
        assert writes.count(gate_key) == 1

        variables["paperflow_v3_control_session"] = "restarted-session"
        restarted = control_app(monkeypatch, ready, variables=variables)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=restarted, raise_app_exceptions=False),
                                     base_url="http://isolated") as client:
            result = (await client.get(f"/api/v1/prefect/operations/{request_id}")).json()
            assert result["found"] is True and result["resolved"] is True
            assert result["status"] == "confirmed"
            duplicate = await client.post(f"/api/v1/prefect/tasks/{TASK_ID}/pause",
                json={"request_id": request_id, "intent_seq": 1})
            assert duplicate.json()["request_id"] == request_id
            assert gate_key not in restarted.state.isolated_writes
            assert not await Admission(TASK_ID).running()

    asyncio.run(exercise())


def test_creation_is_found_by_original_request_when_projection_fails(monkeypatch, tmp_path):
    async def exercise():
        async def ready(_settings):
            return {"ready": True, "checks": []}

        app = control_app(monkeypatch, ready)
        created = []
        run = app.state.isolated_run

        class Client:
            async def create_flow_run_from_deployment(self, deployment_id, *, parameters, idempotency_key, **kwargs):
                assert deployment_id == DEPLOYMENT_ID
                run.parameters = parameters
                run.idempotency_key = idempotency_key
                created.append(run.id)
                return run

            async def read_flow_runs(self, **kwargs):
                return [run] if created else []

        @asynccontextmanager
        async def isolated_client():
            yield Client()

        async def broken_projection(_task_id):
            raise RuntimeError("isolated projection failure")

        monkeypatch.setattr(api, "get_client", isolated_client)
        monkeypatch.setattr(api, "_children", broken_projection)
        request_id = str(uuid4())
        source_path = tmp_path / "source"
        source_path.mkdir()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                     base_url="http://isolated") as client:
            response = await client.post("/api/v1/prefect/tasks", json={
                "request_id": request_id, "task_name": "synthetic", "source_paths": [str(source_path)]})
            assert response.status_code == 500
            result = await client.get(f"/api/v1/prefect/operations/{request_id}")
            assert result.status_code == 200
            assert result.json()["request_id"] == request_id
            assert result.json()["status"] == "confirmed" and result.json()["resolved"] is True
            assert result.json()["target"]["task_id"] == str(TASK_ID)
            original_lookup = await client.get(f"/api/v1/prefect/tasks/by-request/{request_id}")
            assert original_lookup.status_code == 200
            assert original_lookup.json()["task"]["task_id"] == str(TASK_ID)
            assert created == [TASK_ID]

            async def unavailable(_settings):
                return {"ready": False, "checks": []}

            monkeypatch.setattr(preflight, "inspect_dependencies", unavailable)
            source_path.rmdir()
            duplicate = await client.post("/api/v1/prefect/tasks", json={
                "request_id": request_id, "task_name": "synthetic", "source_paths": [str(source_path)]})
            assert duplicate.status_code == 201, duplicate.text
            assert duplicate.json()["status"] == "confirmed"
            assert duplicate.json()["task_id"] == str(TASK_ID)
            assert created == [TASK_ID]

    asyncio.run(exercise())


def test_concurrent_duplicate_creation_returns_original_even_if_one_preflight_fails(monkeypatch, tmp_path):
    async def exercise():
        started, finish = asyncio.Event(), asyncio.Event()
        checks = 0

        async def overlapping_check(_settings):
            nonlocal checks
            checks += 1
            if checks == 1:
                started.set()
                await finish.wait()
                return {"ready": False, "checks": []}
            return {"ready": True, "checks": []}

        app = control_app(monkeypatch, overlapping_check)
        run, created = app.state.isolated_run, []

        class Client:
            async def create_flow_run_from_deployment(self, _deployment_id, *, parameters, **_kwargs):
                run.parameters = parameters
                created.append(run.id)
                return run

        @asynccontextmanager
        async def isolated_client():
            yield Client()

        monkeypatch.setattr(api, "get_client", isolated_client)
        body = {"request_id": str(uuid4()), "task_name": "synthetic", "source_paths": [str(tmp_path)]}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                     base_url="http://isolated") as client:
            slow = asyncio.create_task(client.post("/api/v1/prefect/tasks", json=body))
            await asyncio.wait_for(started.wait(), timeout=3)
            fast = await client.post("/api/v1/prefect/tasks", json=body)
            assert fast.status_code == 201, fast.text
            finish.set()
            late = await slow
            assert late.status_code == 201, late.text
            assert late.json()["status"] == "confirmed" and late.json()["task_id"] == str(TASK_ID)
            assert created == [TASK_ID]

    asyncio.run(exercise())


def test_retry_binds_original_request_and_preserves_child_and_prior_evidence(monkeypatch):
    async def exercise():
        async def ready(_settings):
            return {"ready": True, "checks": []}

        app = control_app(monkeypatch, ready)
        registered = {"task_id": str(TASK_ID), "group_id": "paper-1", "paper_id": "paper",
                      "receipt_id": "registration", "paper": {}}
        child = SimpleNamespace(id=uuid4(), state=Failed(),
            parameters={"task_id": str(TASK_ID), "group_id": "paper-1", "registered": registered})
        failure = {"kind": "llm_request_failed", "data": {"component_id": "body", "request_id": "original",
            "request_sha256": "request", "request_state_sha256": "state", "error_response_sha256": "error",
            "model_dir": "synthetic", "failure_reason": "explicit_transient_response", "retryable": True}}
        preprocessing = {"kind": "preprocessing_complete", "data": {"status": "preprocessing_complete"}}
        records = [failure, preprocessing]
        transitions = []

        class Client:
            async def read_flow_run(self, task_id):
                assert task_id == TASK_ID
                return app.state.isolated_run

            async def set_flow_run_state(self, child_id, state):
                transitions.append(child_id)
                child.state = state
                return SimpleNamespace(state=state)

        @asynccontextmanager
        async def isolated_client():
            yield Client()

        async def children(_task_id):
            return [child]

        async def receipts(_child_id):
            return records

        async def save_receipt(**kwargs):
            records.insert(0, {"kind": kwargs["kind"], "data": kwargs["data"], "event_id": str(kwargs["event_id"])})

        monkeypatch.setattr(api, "get_client", isolated_client)
        monkeypatch.setattr(api, "_children", children)
        monkeypatch.setattr(api, "read_receipts", receipts)
        monkeypatch.setattr(api, "save_receipt", save_receipt)
        request_id = str(uuid4())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                     base_url="http://isolated") as client:
            url = f"/api/v1/prefect/tasks/{TASK_ID}/folders/paper-1/retry"
            first = await client.post(url, json={"request_id": request_id})
            assert first.status_code == 200, first.text
            assert first.json()["request_id"] == request_id and first.json()["accepted"] is True
            retry_id = first.json()["retry_id"]
            duplicate = await client.post(url, json={"request_id": request_id})
            assert duplicate.json()["retry_id"] == retry_id
            lookup = (await client.get(f"/api/v1/prefect/operations/{request_id}")).json()
            assert lookup["status"] == "confirmed" and lookup["retry_id"] == retry_id
            assert lookup["target"]["ticket_id"] == str(child.id)
            assert transitions == [child.id]
            assert len([row for row in records if row["kind"] == "manual_retry"]) == 1
            assert failure in records and preprocessing in records

    asyncio.run(exercise())


@pytest.mark.parametrize("error_type", ["TimeoutError", "ConnectionError", "ConnectTimeout", "ReadTimeout", "ConnectError"])
def test_retry_endpoint_refuses_unconfirmed_postprocessing_network_result(monkeypatch, error_type):
    async def exercise():
        async def ready(_settings):
            return {"ready": True, "checks": []}

        app = control_app(monkeypatch, ready)
        registered = {"task_id": str(TASK_ID), "group_id": "paper-1", "paper_id": "paper",
                      "receipt_id": "registration", "paper": {}}
        child = SimpleNamespace(id=uuid4(), state=Failed(),
            parameters={"task_id": str(TASK_ID), "group_id": "paper-1", "registered": registered})
        records = [
            {"kind": "postprocessing_component_failed", "data": {"component_id": "body",
                "attempt_id": "original", "output_dir": "synthetic", "error_type": error_type, "reason": "lost"}},
            {"kind": "postprocessing_component_failed", "data": {"component_id": "attachment:1",
                "attempt_id": "original", "output_dir": "synthetic", "error_type": "ValueError", "reason": "bad"}},
            {"kind": "preprocessing_complete", "data": {"status": "preprocessing_complete"}},
        ]
        saved, transitions = [], []

        class Client:
            async def read_flow_run(self, _task_id):
                return app.state.isolated_run

            async def set_flow_run_state(self, child_id, state):
                transitions.append(child_id)
                return SimpleNamespace(state=state)

        @asynccontextmanager
        async def isolated_client():
            yield Client()

        async def children(_task_id):
            return [child]

        async def receipts(_child_id):
            return records

        async def save_receipt(**kwargs):
            saved.append(kwargs)

        monkeypatch.setattr(api, "get_client", isolated_client)
        monkeypatch.setattr(api, "_children", children)
        monkeypatch.setattr(api, "read_receipts", receipts)
        monkeypatch.setattr(api, "save_receipt", save_receipt)
        request_id = str(uuid4())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                     base_url="http://isolated") as client:
            result = await client.post(f"/api/v1/prefect/tasks/{TASK_ID}/folders/paper-1/retry",
                                       json={"request_id": request_id})
            assert result.status_code == 409, result.text
            assert result.json()["detail"]["code"] == "retry_not_allowed"
            assert "待核对" in result.json()["detail"]["message"]
            lookup = (await client.get(f"/api/v1/prefect/operations/{request_id}")).json()
            assert lookup["status"] == "rejected" and lookup["resolved"] is True
            assert not saved and not transitions

    asyncio.run(exercise())


def test_retry_unknown_does_not_dispatch_again_and_requires_matching_running_receipt(monkeypatch):
    async def exercise():
        async def ready(_settings):
            return {"ready": True, "checks": []}

        invalidated, transitions = [], []

        async def invalidate(task_id, *, folder_id=None):
            invalidated.append((task_id, folder_id))

        app = control_app(monkeypatch, ready)
        app.state.query_service = SimpleNamespace(invalidate=invalidate)
        registered = {"task_id": str(TASK_ID), "group_id": "paper-1", "paper_id": "paper",
                      "receipt_id": "registration", "paper": {}}
        child = SimpleNamespace(id=uuid4(), state=Failed(),
            parameters={"task_id": str(TASK_ID), "group_id": "paper-1", "registered": registered})
        records = [{"kind": "llm_request_failed", "data": {"component_id": "body", "request_id": "original",
            "request_sha256": "request", "request_state_sha256": "state", "error_response_sha256": "error",
            "model_dir": "synthetic", "failure_reason": "explicit_transient_response", "retryable": True}},
            {"kind": "preprocessing_complete", "data": {"status": "preprocessing_complete"}}]

        class Client:
            async def read_flow_run(self, _task_id):
                return app.state.isolated_run

            async def set_flow_run_state(self, child_id, state):
                transitions.append(child_id)
                child.state = state
                raise ConnectionError("isolated lost dispatch response")

        @asynccontextmanager
        async def isolated_client():
            yield Client()

        async def children(_task_id):
            return [child]

        async def receipts(_child_id):
            return records

        async def save_receipt(**kwargs):
            records.insert(0, {"kind": kwargs["kind"], "data": kwargs["data"]})

        monkeypatch.setattr(api, "get_client", isolated_client)
        monkeypatch.setattr(api, "_children", children)
        monkeypatch.setattr(api, "read_receipts", receipts)
        monkeypatch.setattr(api, "save_receipt", save_receipt)
        request_id = str(uuid4())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                     base_url="http://isolated") as client:
            url = f"/api/v1/prefect/tasks/{TASK_ID}/folders/paper-1/retry"
            first = await client.post(url, json={"request_id": request_id})
            assert first.status_code == 409 and first.json()["detail"]["code"] == "retry_dispatch_unknown"
            retry_id = first.json()["detail"]["retry_id"]
            lookup = (await client.get(f"/api/v1/prefect/operations/{request_id}")).json()
            assert lookup["status"] == "unknown" and lookup["resolved"] is False
            duplicate = await client.post(url, json={"request_id": request_id})
            assert duplicate.json()["status"] == "unknown" and duplicate.json()["retry_id"] == retry_id
            assert transitions == [child.id]
            assert invalidated == [(str(TASK_ID), "paper-1"), (str(TASK_ID), "paper-1")]
            records.insert(0, {"kind": "manual_retry", "data": {"retry_id": retry_id, "state": "running"}})
            confirmed = (await client.get(f"/api/v1/prefect/operations/{request_id}")).json()
            assert confirmed["status"] == "confirmed" and confirmed["resolved"] is True
            assert transitions == [child.id]

    asyncio.run(exercise())


def test_confirmed_pause_invalidates_only_its_task_query(monkeypatch):
    async def exercise():
        async def ready(_settings):
            return {"ready": True, "checks": []}

        invalidated = []

        async def invalidate(task_id, *, folder_id=None):
            invalidated.append((task_id, folder_id))

        app = control_app(monkeypatch, ready)
        app.state.query_service = SimpleNamespace(invalidate=invalidate)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                     base_url="http://isolated") as client:
            response = await client.post(f"/api/v1/prefect/tasks/{TASK_ID}/pause",
                json={"request_id": str(uuid4()), "intent_seq": 1})
            assert response.status_code == 200 and response.json()["status"] == "confirmed"
            assert invalidated == [(str(TASK_ID), None)]

    asyncio.run(exercise())
