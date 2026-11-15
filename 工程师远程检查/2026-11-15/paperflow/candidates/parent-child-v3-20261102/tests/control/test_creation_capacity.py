"""Creation checks use synthetic account transports and never apply capacity."""
import asyncio
from contextlib import asynccontextmanager
import importlib.util
from pathlib import Path
from uuid import uuid4

import pytest

from paperflow_v3.control import api
from paperflow_v3.control.auto_capacity import AutoCapacity, CapacityError
from test_operation_confirmation import TASK_ID, control_app
from test_start_capacity import client_for, ready


def upstream_check(value):
    return next(check for check in value["checks"] if check["name"] == "upstream_accounts")


def test_creation_preflight_counts_only_connected_enabled_domestic_accounts(monkeypatch):
    async def exercise():
        monitor_path = Path(__file__).resolve().parents[3] / "first-step-preview-8795/workbuddy_monitor.py"
        spec = importlib.util.spec_from_file_location("creation_account_monitor", monitor_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        accounts = [{"uid": f"cn-{i}", "realm": "cn", "enabled": True} for i in range(10)]
        accounts += [{"uid": "disabled", "realm": "cn", "enabled": False},
                     {"uid": "international", "realm": "intl", "enabled": True}]
        probes = []

        def transport(path, *, token=None, data=None, **kwargs):
            if path == "/accounts?realm=all":
                return {"accounts": accounts}
            assert path == "/accounts/test", "creation must not change gateway settings"
            probes.append(data.copy())
            if data["uid"] == "cn-9":
                raise TimeoutError("synthetic unknown result")
            return {"ok": data["uid"] != "cn-8"}

        monitor = module.Monitor(transport, prefect_transport=lambda: None, password_loader=lambda: None)
        monitor._token = "synthetic-private-token"
        capacity = AutoCapacity(monitor, lambda: {"per_account_limit": 2, "model": "synthetic-model",
                                                  "private_route": "synthetic-private-route"})

        async def unexpected_apply(*args, **kwargs):
            raise AssertionError("creation must not apply shared capacity")

        app = control_app(monkeypatch, ready, test_start_accounts=capacity.test_start_accounts,
                          apply_start_capacity=unexpected_apply)
        async with client_for(app) as client:
            response = await client.post("/api/v1/prefect/preflight")
        assert response.status_code == 200, response.text
        value = response.json()
        assert value["ready"] is True
        assert value["successful_accounts"] == 8 and value["llm_limit"] == 16
        assert upstream_check(value)["ready"] is True
        assert "8" in upstream_check(value)["detail"] and "16" in upstream_check(value)["detail"]
        assert {probe["uid"] for probe in probes} == {f"cn-{i}" for i in range(10)}
        assert all(probe["model"] == "synthetic-model" and probe["realm"] == "cn" for probe in probes)
        assert "synthetic-private" not in response.text and "account_uids" not in response.text
        assert app.state.isolated_writes == []
    asyncio.run(exercise())


@pytest.mark.parametrize("count", [0, True, -1, 1.5, "8", None])
def test_creation_preflight_blocks_zero_or_invalid_success_count(monkeypatch, count):
    async def exercise():
        async def accounts(*, ensure_current):
            await ensure_current()
            return {"successful_count": count, "account_uids": ["synthetic-private-account"]}
        app = control_app(monkeypatch, ready, test_start_accounts=accounts)
        async with client_for(app) as client:
            response = await client.post("/api/v1/prefect/preflight")
        value = response.json()
        assert response.status_code == 200 and value["ready"] is False
        assert upstream_check(value)["ready"] is False
        assert "synthetic-private" not in response.text
        if type(count) is int and count == 0:
            assert value["successful_accounts"] == value["llm_limit"] == 0
        else:
            assert "successful_accounts" not in value and "llm_limit" not in value
        assert app.state.isolated_writes == []
    asyncio.run(exercise())


@pytest.mark.parametrize("failure", ["busy", "exception", "missing"])
def test_creation_preflight_blocks_unconfirmed_account_tests(monkeypatch, failure):
    async def exercise():
        async def accounts(*, ensure_current):
            if failure == "busy":
                raise CapacityError("account test is busy")
            raise RuntimeError("synthetic-private-secret")
        app = control_app(monkeypatch, ready,
                          test_start_accounts=None if failure == "missing" else accounts)
        async with client_for(app) as client:
            response = await client.post("/api/v1/prefect/preflight")
        value = response.json()
        assert response.status_code == 200 and value["ready"] is False
        assert upstream_check(value)["ready"] is False
        assert "synthetic-private" not in response.text
        assert app.state.isolated_writes == []
    asyncio.run(exercise())


def test_creation_dependency_failure_does_not_test_accounts(monkeypatch):
    async def exercise():
        calls = []
        async def dependencies(_settings):
            return {"ready": False, "checks": [{"name": "rag", "ready": False, "detail": "unavailable"}]}
        async def accounts(*, ensure_current):
            calls.append("unexpected")
            return {"successful_count": 8}
        app = control_app(monkeypatch, dependencies, test_start_accounts=accounts)
        async with client_for(app) as client:
            response = await client.post("/api/v1/prefect/preflight")
        assert response.json()["ready"] is False and calls == []
        assert upstream_check(response.json())["ready"] is False
    asyncio.run(exercise())


def test_direct_creation_cannot_bypass_zero_connected_accounts(monkeypatch, tmp_path):
    async def exercise():
        calls = []
        async def accounts(*, ensure_current):
            calls.append("test")
            return {"successful_count": 0}
        app = control_app(monkeypatch, ready, test_start_accounts=accounts)
        async with client_for(app) as client:
            response = await client.post("/api/v1/prefect/tasks", json={
                "request_id": str(uuid4()), "task_name": "synthetic", "source_paths": [str(tmp_path)]})
        assert response.status_code == 409, response.text
        assert response.json()["detail"]["successful_accounts"] == 0
        assert calls == ["test"] and app.state.isolated_writes == []
    asyncio.run(exercise())


def test_creation_rechecks_accounts_and_repeated_identity_does_not_retest(monkeypatch, tmp_path):
    async def exercise():
        tests, creations = [], []
        async def accounts(*, ensure_current):
            tests.append("test")
            return {"successful_count": 8, "account_uids": ["synthetic-private-account"]}
        app = control_app(monkeypatch, ready, test_start_accounts=accounts)
        run = app.state.isolated_run
        run.run_count = 0

        class Client:
            async def create_flow_run_from_deployment(self, deployment_id, **fields):
                assert fields["state"].is_paused()
                run.parameters, run.state = fields["parameters"], fields["state"]
                creations.append(fields)
                return run

        @asynccontextmanager
        async def isolated_client():
            yield Client()
        monkeypatch.setattr(api, "get_client", isolated_client)
        async def empty_folder_records(_children):
            return {}
        monkeypatch.setattr(api, "_folder_ticket_records", empty_folder_records)
        body = {"request_id": str(uuid4()), "task_name": "synthetic", "source_paths": [str(tmp_path)]}
        async with client_for(app) as client:
            first = await client.post("/api/v1/prefect/tasks", json=body)
            duplicate = await client.post("/api/v1/prefect/tasks", json=body)
        assert first.status_code == duplicate.status_code == 201, first.text
        assert first.json()["task_id"] == duplicate.json()["task_id"] == str(TASK_ID)
        assert first.json()["desired_state"] == "paused"
        assert tests == ["test"] and len(creations) == 1
        assert "synthetic-private" not in first.text
    asyncio.run(exercise())
