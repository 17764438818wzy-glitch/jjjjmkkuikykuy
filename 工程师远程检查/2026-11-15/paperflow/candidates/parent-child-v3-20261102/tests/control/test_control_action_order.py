"""Starting and pausing confirm the original control action without a full projection."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest
from prefect.client.orchestration import PrefectClient
from prefect.settings import PREFECT_API_URL, temporary_settings
from prefect.states import Completed, Crashed, Failed, Paused, Running, Scheduled
from prefect.variables import Variable

from paperflow_v3.control import api, preflight


TASK_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
DEPLOYMENT_ID = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
REQUEST_ID = UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")


def setup_control(monkeypatch, *, running, fail_projection=False, preflight_ready=True, state=None, children=None,
                  reject_schedule=False, resume_error=False, dependency_active=False, save_error=False):
    events = []
    admission = {"running": running}
    variables = {}
    child_runs = children or []
    run = SimpleNamespace(id=TASK_ID, deployment_id=DEPLOYMENT_ID, state=state or Running(), run_count=1,
                          parameters={"task_name": "synthetic"})

    class Client:
        async def read_flow_run(self, task_id):
            assert task_id == TASK_ID
            return run

        async def set_flow_run_state(self, run_id, new_state):
            events.append(("schedule", run_id))
            if reject_schedule:
                return SimpleNamespace(state=None)
            target = run if run_id == TASK_ID else next(child for child in child_runs if child.id == run_id)
            target.state = new_state
            return SimpleNamespace(state=new_state)

    @asynccontextmanager
    async def get_client():
        yield Client()

    async def no_version_check(_client):
        pass

    async def read_variable(name, *args, **kwargs):
        return deepcopy(variables.get(name))

    async def write_variable(name, value, *args, **kwargs):
        variables[name] = deepcopy(value)

    async def read_children(_task_id):
        return child_runs

    async def read_parent(_task_id):
        return [{"kind": "dependency_failure", "data": {"status": "active"}}] if dependency_active else []

    async def resume_parent(run_id):
        events.append(("resume", run_id))
        if resume_error:
            raise RuntimeError("synthetic resume failure")
        run.state = Scheduled()

    async def clear_dependency(*, task_id, kind, data, **_kwargs):
        assert task_id == TASK_ID and kind == "dependency_failure" and data == {"status": "cleared"}
        events.append("clear_dependency")
        if save_error:
            raise RuntimeError("synthetic receipt failure")

    async def read_folder(run_ids):
        events.append("project")
        if fail_projection:
            raise RuntimeError("synthetic receipt failure")
        return {str(run_id): [] for run_id in run_ids}

    async def running_state(_admission):
        return admission["running"]

    async def start(_admission):
        events.append("start")
        admission["running"] = True

    async def pause(_admission):
        events.append("pause")
        admission["running"] = False

    async def check(_settings):
        events.append("preflight")
        return {"ready": preflight_ready, "checks": []}

    async def no_active_folder(_task_id, **kwargs):
        return {"state": "paused", "active_folders": 0, "unresolved_folders": 0}

    monkeypatch.setattr(PrefectClient, "raise_for_api_version_mismatch_once", no_version_check)
    monkeypatch.setattr(Variable, "aget", staticmethod(read_variable))
    monkeypatch.setattr(Variable, "aset", staticmethod(write_variable))
    monkeypatch.setattr(api, "get_client", get_client)
    monkeypatch.setattr(api, "_children", read_children)
    monkeypatch.setattr(api, "read_receipts", read_parent)
    monkeypatch.setattr(api, "read_folder_receipts", read_folder)
    monkeypatch.setattr(api, "aresume_flow_run", resume_parent)
    monkeypatch.setattr(api, "save_receipt", clear_dependency)
    monkeypatch.setattr(api.Admission, "running", running_state)
    monkeypatch.setattr(api.Admission, "start", start)
    monkeypatch.setattr(api.Admission, "pause", pause)
    monkeypatch.setattr(preflight, "inspect_dependencies", check)
    monkeypatch.setattr(api, "pause_progress", no_active_folder)
    return events


def request_action(action):
    async def tested_accounts(*, ensure_current):
        await ensure_current()
        return {"successful_count": 1, "account_uids": ["synthetic-cn-account"]}

    async def applied_capacity(result, limit, *, ensure_current):
        await ensure_current()
        assert result["successful_count"] == 1 and limit == 2

    app = api.create_app({"deployment_id": DEPLOYMENT_ID}, test_start_accounts=tested_accounts,
                         apply_start_capacity=applied_capacity)

    async def request():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                     base_url="http://test") as client:
            return await client.post(f"/api/v1/prefect/tasks/{TASK_ID}/{action}",
                                     json={"request_id": str(REQUEST_ID), "intent_seq": 1})

    with temporary_settings({PREFECT_API_URL: "http://127.0.0.1:1/api"}):
        return asyncio.run(request())


@pytest.mark.parametrize(("action", "running"), [("start", False), ("pause", True)])
def test_action_confirmation_does_not_depend_on_receipt_projection(monkeypatch, action, running):
    events = setup_control(monkeypatch, running=running, fail_projection=True)
    response = request_action(action)
    assert response.status_code == 200
    assert action in events and "project" not in events
    if action == "start":
        assert events.index("preflight") < events.index("start")


@pytest.mark.parametrize(("action", "running"), [("start", False), ("pause", True)])
def test_action_response_confirms_identity_and_acceptance(monkeypatch, action, running):
    events = setup_control(monkeypatch, running=running)
    response = request_action(action)
    assert response.status_code == 200
    result = response.json()
    assert result["task_id"] == str(TASK_ID) and result["action"] == action
    assert result["request_id"] == str(REQUEST_ID) and result["accepted"] is True
    assert result["status"] == "confirmed" and result["resolved"] is True
    assert action in events and "project" not in events


def test_failed_preflight_still_prevents_start(monkeypatch):
    events = setup_control(monkeypatch, running=False, preflight_ready=False)
    response = request_action("start")
    assert response.status_code == 409 and "start" not in events


def test_rejected_start_exposes_failed_preflight_on_later_operation_lookup(monkeypatch):
    events = setup_control(monkeypatch, running=False)

    async def failed_check(_settings):
        return {"ready": False, "checks": [
            {"name": "prefect_storage", "ready": True, "detail": "ready"},
            {"name": "nas", "ready": False, "detail": "NAS unavailable"},
        ]}

    monkeypatch.setattr(preflight, "inspect_dependencies", failed_check)
    app = api.create_app({"deployment_id": DEPLOYMENT_ID})

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",
                                         json={"request_id": str(REQUEST_ID), "intent_seq": 1})
            lookup = await client.get(f"/api/v1/prefect/operations/{REQUEST_ID}")
        return response, lookup

    with temporary_settings({PREFECT_API_URL: "http://127.0.0.1:1/api"}):
        response, lookup = asyncio.run(scenario())
    assert response.status_code == 409 and "start" not in events
    assert lookup.json()["status"] == "rejected"
    assert lookup.json()["failed_checks"] == [{"name": "nas", "ready": False, "detail": "NAS unavailable"}]
    assert lookup.json()["checked_at"]


def test_rejected_finished_task_exposes_reason_on_later_lookup(monkeypatch):
    events = setup_control(monkeypatch, running=False, state=Completed())
    app = api.create_app({"deployment_id": DEPLOYMENT_ID})

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",
                                         json={"request_id": str(REQUEST_ID), "intent_seq": 1})
            lookup = await client.get(f"/api/v1/prefect/operations/{REQUEST_ID}")
        return response, lookup

    with temporary_settings({PREFECT_API_URL: "http://127.0.0.1:1/api"}):
        response, lookup = asyncio.run(scenario())
    assert response.status_code == 409 and "start" not in events
    assert lookup.json()["status"] == "rejected" and lookup.json()["reason"] == "任务已结束"


@pytest.mark.parametrize("action", ["start", "pause"])
def test_completed_task_cannot_be_started_or_paused(monkeypatch, action):
    events = setup_control(monkeypatch, running=action == "pause", state=Completed())
    response = request_action(action)
    assert response.status_code == 409 and response.json()["detail"] == "任务已结束"
    assert action not in events and "preflight" not in events


def test_running_parent_with_crashed_child_can_start_same_child(monkeypatch):
    child = SimpleNamespace(id=UUID(int=1), state=Crashed(), parameters={"group_id": "synthetic"})
    events = setup_control(monkeypatch, running=True, children=[child])
    response = request_action("start")
    assert response.status_code == 200
    assert events.index("preflight") < events.index("start") < events.index(("schedule", child.id))
    assert child.state.is_scheduled()


def test_crashed_parent_can_start_same_parent(monkeypatch):
    events = setup_control(monkeypatch, running=True, state=Crashed())
    response = request_action("start")
    assert response.status_code == 200
    assert events.index("preflight") < events.index("start") < events.index(("schedule", TASK_ID))


def test_start_schedules_only_original_crashed_parent_and_child(monkeypatch):
    crashed = SimpleNamespace(id=UUID(int=1), state=Crashed(), parameters={"group_id": "crashed"})
    running = SimpleNamespace(id=UUID(int=2), state=Running(), parameters={"group_id": "running"})
    failed = SimpleNamespace(id=UUID(int=3), state=Failed(), parameters={"group_id": "failed"})
    events = setup_control(monkeypatch, running=False, state=Crashed(), children=[crashed, running, failed])
    response = request_action("start")
    assert response.status_code == 200
    assert [event for event in events if isinstance(event, tuple)] == [
        ("schedule", TASK_ID), ("schedule", crashed.id)]
    assert crashed.state.is_scheduled() and running.state.is_running() and failed.state.is_failed()


@pytest.mark.parametrize("target", ["parent", "child"])
def test_rejected_original_run_recovery_does_not_confirm_start(monkeypatch, target):
    child = SimpleNamespace(id=UUID(int=1), state=Crashed(), parameters={"group_id": "crashed"})
    events = setup_control(monkeypatch, running=False,
                           state=Crashed() if target == "parent" else Running(),
                           children=[child] if target == "child" else [], reject_schedule=True)
    response = request_action("start")
    assert response.status_code == 409
    assert ("schedule", TASK_ID if target == "parent" else child.id) in events
    assert "project" not in events


@pytest.mark.parametrize("resume_error", [False, True])
def test_paused_parent_resume_is_attempted_before_confirmation(monkeypatch, resume_error):
    events = setup_control(monkeypatch, running=False, state=Paused(), resume_error=resume_error)
    response = request_action("start")
    assert response.status_code == (500 if resume_error else 200)
    assert events.index("start") < events.index(("resume", TASK_ID))
    assert "project" not in events


@pytest.mark.parametrize("save_error", [False, True])
def test_active_dependency_is_cleared_before_start_confirmation(monkeypatch, save_error):
    events = setup_control(monkeypatch, running=False, dependency_active=True, save_error=save_error)
    response = request_action("start")
    assert response.status_code == (500 if save_error else 200)
    assert events.index("start") < events.index("clear_dependency")
    assert "project" not in events
