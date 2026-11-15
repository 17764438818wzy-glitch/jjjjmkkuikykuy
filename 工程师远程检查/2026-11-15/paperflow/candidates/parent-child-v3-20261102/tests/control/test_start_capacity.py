"""Exercise start capacity control using synthetic accounts and Prefect records."""

import asyncio
from uuid import uuid4

import httpx
import pytest
from prefect.states import Completed, Paused, Scheduled

from paperflow_v3.control import api
from paperflow_v3.control.admission import Admission
from test_operation_confirmation import TASK_ID, control_app


async def ready(_settings):
    return {"ready": True, "checks": []}


def client_for(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                             base_url="http://isolated")


async def start(client, request_id, seq=1):
    return await client.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",
                             json={"request_id": request_id, "intent_seq": seq})


def test_start_tests_once_then_applies_read_back_capacity_before_original_resume(monkeypatch):
    async def exercise():
        events = []
        checked = {"successful_count": 3, "account_uids": ["cn-a", "cn-b", "cn-c"],
                   "route": {"private": "synthetic-private-route-value"}}

        async def check(_settings):
            events.append("preflight")
            return await ready(_settings)

        async def test_accounts(*, ensure_current):
            events.append("test")
            assert not await Admission(TASK_ID).running()
            return checked

        async def apply_capacity(result, limit, *, ensure_current):
            await ensure_current()
            assert result is checked and limit == 6
            assert not await Admission(TASK_ID).running()
            events.append("configured-and-read-back")

        app = control_app(monkeypatch, check, test_start_accounts=test_accounts,
                          apply_start_capacity=apply_capacity)
        app.state.isolated_run.state = Paused()

        async def resume(run_id):
            assert run_id == TASK_ID and await Admission(TASK_ID).running()
            events.append("original-resume")
            app.state.isolated_run.state = Scheduled()

        monkeypatch.setattr(api, "aresume_flow_run", resume)
        request_id = str(uuid4())
        async with client_for(app) as client:
            first = await start(client, request_id)
            duplicate = await start(client, request_id)
            result = (await client.get(f"/api/v1/prefect/operations/{request_id}")).json()
        assert first.status_code == 200, first.text
        assert first.json()["status"] == duplicate.json()["status"] == "confirmed"
        assert events == ["preflight", "test", "configured-and-read-back", "original-resume"]
        assert result["successful_accounts"] == 3 and result["llm_limit"] == 6
        assert result["target"] == {"task_id": str(TASK_ID)}
        assert "synthetic-private-route-value" not in first.text + str(result)

    asyncio.run(exercise())


def test_inflight_duplicate_does_not_retest_and_later_pause_discards_old_result(monkeypatch):
    async def exercise():
        entered, finish = asyncio.Event(), asyncio.Event()
        tested, applied = [], []

        async def test_accounts(*, ensure_current):
            tested.append("cn")
            entered.set()
            await finish.wait()
            return {"successful_count": 2, "account_uids": ["cn-a", "cn-b"]}

        async def apply_capacity(result, limit, *, ensure_current):
            await ensure_current()
            applied.append((result, limit))

        app = control_app(monkeypatch, ready, test_start_accounts=test_accounts,
                          apply_start_capacity=apply_capacity)
        request_id, pause_id = str(uuid4()), str(uuid4())
        async with client_for(app) as client:
            original = asyncio.create_task(start(client, request_id))
            await asyncio.wait_for(entered.wait(), timeout=3)
            duplicate = await start(client, request_id)
            assert duplicate.json()["status"] == "accepted"
            assert duplicate.json()["phase"] == "account_test"
            assert "测试" in duplicate.json()["message"]
            pause = await asyncio.wait_for(client.post(f"/api/v1/prefect/tasks/{TASK_ID}/pause",
                json={"request_id": pause_id, "intent_seq": 2}), timeout=3)
            assert pause.json()["status"] == "confirmed"
            finish.set()
            response = await original
            old = (await client.get(f"/api/v1/prefect/operations/{request_id}")).json()
        assert response.json()["status"] == old["status"] == "superseded"
        assert tested == ["cn"] and applied == []
        assert not await Admission(TASK_ID).running()

    asyncio.run(exercise())


def test_pause_during_original_preflight_prevents_even_account_testing(monkeypatch):
    async def exercise():
        entered, finish = asyncio.Event(), asyncio.Event()
        tested = []

        async def check(_settings):
            entered.set()
            await finish.wait()
            return await ready(_settings)

        async def test_accounts(*, ensure_current):
            tested.append("unexpected")
            return {"successful_count": 1, "account_uids": ["cn-a"]}

        app = control_app(monkeypatch, check, test_start_accounts=test_accounts)
        async with client_for(app) as client:
            original = asyncio.create_task(start(client, str(uuid4())))
            await asyncio.wait_for(entered.wait(), timeout=3)
            pause = await client.post(f"/api/v1/prefect/tasks/{TASK_ID}/pause",
                json={"request_id": str(uuid4()), "intent_seq": 2})
            assert pause.json()["status"] == "confirmed"
            finish.set()
            result = await original
        assert result.json()["status"] == "superseded" and tested == []
        assert not await Admission(TASK_ID).running()

    asyncio.run(exercise())


def test_pause_during_account_batch_prevents_starting_the_next_batch(monkeypatch):
    async def exercise():
        entered, finish = asyncio.Event(), asyncio.Event()
        tested, applied = [], []

        async def test_accounts(*, ensure_current):
            await ensure_current()
            tested.append("already-started-batch")
            entered.set()
            await finish.wait()
            await ensure_current()
            tested.append("must-not-start-after-pause")
            return {"successful_count": 2, "account_uids": ["cn-a", "cn-b"]}

        async def apply_capacity(result, limit, *, ensure_current):
            await ensure_current()
            applied.append(limit)

        app = control_app(monkeypatch, ready, test_start_accounts=test_accounts,
                          apply_start_capacity=apply_capacity)
        request_id = str(uuid4())
        async with client_for(app) as client:
            original = asyncio.create_task(start(client, request_id))
            await asyncio.wait_for(entered.wait(), timeout=3)
            pause = await asyncio.wait_for(client.post(f"/api/v1/prefect/tasks/{TASK_ID}/pause",
                json={"request_id": str(uuid4()), "intent_seq": 2}), timeout=3)
            assert pause.json()["status"] == "confirmed"
            finish.set()
            response = await original
            old = (await client.get(f"/api/v1/prefect/operations/{request_id}")).json()
            duplicate = await start(client, request_id)
        assert response.json()["status"] == old["status"] == duplicate.json()["status"] == "superseded"
        assert tested == ["already-started-batch"] and applied == []
        assert not await Admission(TASK_ID).running()

    asyncio.run(exercise())


@pytest.mark.parametrize("count", [0, -1, True, "2"])
def test_zero_or_invalid_success_count_never_configures_or_starts(monkeypatch, count):
    async def exercise():
        tested, applied = [], []

        async def test_accounts(*, ensure_current):
            tested.append("cn")
            return {"successful_count": count, "account_uids": []}

        async def apply_capacity(result, limit, *, ensure_current):
            await ensure_current()
            applied.append(limit)

        app = control_app(monkeypatch, ready, test_start_accounts=test_accounts,
                          apply_start_capacity=apply_capacity)
        request_id = str(uuid4())
        async with client_for(app) as client:
            response = await start(client, request_id)
            result = (await client.get(f"/api/v1/prefect/operations/{request_id}")).json()
            await start(client, request_id)
        assert response.status_code == 409, response.text
        assert result["status"] == "rejected" and result["reason"]
        assert tested == ["cn"] and applied == []
        assert not await Admission(TASK_ID).running()

    asyncio.run(exercise())


@pytest.mark.parametrize("stage", ["test", "apply"])
def test_adapter_timeout_is_saved_without_secret_or_replaying_request(monkeypatch, stage):
    async def exercise():
        calls = []

        async def test_accounts(*, ensure_current):
            calls.append("test")
            if stage == "test":
                raise TimeoutError("secret=synthetic-sensitive-value")
            return {"successful_count": 1, "account_uids": ["cn-a"]}

        async def apply_capacity(result, limit, *, ensure_current):
            await ensure_current()
            calls.append("apply")
            raise TimeoutError("secret=synthetic-sensitive-value")

        app = control_app(monkeypatch, ready, test_start_accounts=test_accounts,
                          apply_start_capacity=apply_capacity)
        request_id = str(uuid4())
        async with client_for(app) as client:
            response = await start(client, request_id)
            result = (await client.get(f"/api/v1/prefect/operations/{request_id}")).json()
            await start(client, request_id)
        assert response.status_code == 409, response.text
        assert result["status"] == "rejected" and result["resolved"] is True
        assert "TimeoutError" in result["reason"] and "synthetic-sensitive-value" not in str(result)
        assert calls == (["test"] if stage == "test" else ["test", "apply"])
        assert not await Admission(TASK_ID).running()

    asyncio.run(exercise())


@pytest.mark.parametrize("stage", ["test", "apply"])
def test_safe_capacity_error_survives_operation_lookup(monkeypatch, stage):
    from paperflow_v3.control.auto_capacity import CapacityError

    async def exercise():
        reason = "当前路由上限 140 低于所需 150"

        async def test_accounts(*, ensure_current):
            if stage == "test":
                raise CapacityError(reason)
            return {"successful_count": 1, "account_uids": ["cn-a"]}

        async def apply_capacity(result, limit, *, ensure_current):
            await ensure_current()
            raise CapacityError(reason)

        app = control_app(monkeypatch, ready, test_start_accounts=test_accounts,
                          apply_start_capacity=apply_capacity)
        request_id = str(uuid4())
        async with client_for(app) as client:
            response = await start(client, request_id)
            result = (await client.get(f"/api/v1/prefect/operations/{request_id}")).json()
        assert response.status_code == 409, response.text
        assert result["status"] == "rejected" and reason in result["reason"]
        assert reason in response.json()["detail"] and not await Admission(TASK_ID).running()

    asyncio.run(exercise())


def test_pause_can_register_during_configuration_and_prevents_remaining_writes(monkeypatch):
    async def exercise():
        entered, finish = asyncio.Event(), asyncio.Event()
        writes = []

        async def test_accounts(*, ensure_current):
            return {"successful_count": 1, "account_uids": ["cn-a"]}

        async def apply_capacity(result, limit, *, ensure_current):
            await ensure_current()
            writes.append("already-sent")
            entered.set()
            await finish.wait()
            await ensure_current()
            writes.append("must-not-be-sent-after-pause")

        app = control_app(monkeypatch, ready, test_start_accounts=test_accounts,
                          apply_start_capacity=apply_capacity)
        request_id = str(uuid4())
        async with client_for(app) as client:
            original = asyncio.create_task(start(client, request_id))
            await asyncio.wait_for(entered.wait(), timeout=3)
            pending = (await client.get(f"/api/v1/prefect/operations/{request_id}")).json()
            assert pending["phase"] == "capacity_apply" and "配置" in pending["message"]
            assert pending["successful_accounts"] == 1 and pending["llm_limit"] == 2
            pause = await asyncio.wait_for(client.post(f"/api/v1/prefect/tasks/{TASK_ID}/pause",
                json={"request_id": str(uuid4()), "intent_seq": 2}), timeout=3)
            assert pause.json()["status"] == "confirmed"
            finish.set()
            response = await original
        assert response.json()["status"] == "superseded"
        assert writes == ["already-sent"] and not await Admission(TASK_ID).running()

    asyncio.run(exercise())


def test_pause_after_last_configuration_write_still_blocks_start(monkeypatch):
    async def exercise():
        configured, finish = asyncio.Event(), asyncio.Event()

        async def apply_capacity(result, limit, *, ensure_current):
            await ensure_current()
            configured.set()
            await finish.wait()

        app = control_app(monkeypatch, ready, apply_start_capacity=apply_capacity)
        async with client_for(app) as client:
            original = asyncio.create_task(start(client, str(uuid4())))
            await asyncio.wait_for(configured.wait(), timeout=3)
            pause = await client.post(f"/api/v1/prefect/tasks/{TASK_ID}/pause",
                json={"request_id": str(uuid4()), "intent_seq": 2})
            assert pause.json()["status"] == "confirmed"
            finish.set()
            response = await original
        assert response.json()["status"] == "superseded"
        assert not await Admission(TASK_ID).running()

    asyncio.run(exercise())


def test_missing_adapters_cannot_bypass_capacity_check(monkeypatch):
    async def exercise():
        app = control_app(monkeypatch, ready, test_start_accounts=None, apply_start_capacity=None)
        async with client_for(app) as client:
            response = await start(client, str(uuid4()))
        assert response.status_code == 409, response.text
        assert not await Admission(TASK_ID).running()

    asyncio.run(exercise())


def test_creation_preflight_only_tests_and_operation_queries_do_not_apply_capacity(monkeypatch):
    async def exercise():
        calls = []
        async def accounts(*, ensure_current):
            await ensure_current()
            calls.append("test")
            return {"successful_count": 8}
        async def unexpected_apply(*args, **kwargs):
            raise AssertionError("creation preflight must not apply capacity")
        app = control_app(monkeypatch, ready, test_start_accounts=accounts,
                          apply_start_capacity=unexpected_apply)
        request_id = str(uuid4())
        async with client_for(app) as client:
            preflight = await client.post("/api/v1/prefect/preflight")
            for _ in range(2):
                operation = await client.get(f"/api/v1/prefect/operations/{request_id}")
        assert preflight.json()["ready"] is True and operation.json()["found"] is False
        assert preflight.json()["llm_limit"] == 16
        assert calls == ["test"] and not await Admission(TASK_ID).running()
    asyncio.run(exercise())


@pytest.mark.parametrize("blocked_by", ["finished", "preflight"])
def test_original_rejections_happen_before_account_testing(monkeypatch, blocked_by):
    async def exercise():
        calls = []

        async def check(_settings):
            return {"ready": blocked_by != "preflight", "checks": []}

        async def unexpected(*, ensure_current):
            calls.append("test")
            raise AssertionError("a rejected start must not test accounts")

        app = control_app(monkeypatch, check, test_start_accounts=unexpected)
        if blocked_by == "finished":
            app.state.isolated_run.state = Completed()
        async with client_for(app) as client:
            response = await start(client, str(uuid4()))
        assert response.status_code == 409, response.text
        assert calls == [] and not await Admission(TASK_ID).running()

    asyncio.run(exercise())
