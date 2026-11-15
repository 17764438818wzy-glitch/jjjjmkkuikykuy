"""A synthetic start through the real control, gateway adapter and capacity wiring."""
import asyncio
from contextlib import asynccontextmanager
import importlib.util
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
from prefect.states import Paused, Scheduled

from paperflow_v3.control import api
from paperflow_v3.control.admission import Admission
from paperflow_v3.control.auto_capacity import AutoCapacity
from test_operation_confirmation import TASK_ID, control_app


def test_synthetic_start_tests_only_domestic_and_applies_two_slots_each(monkeypatch):
    async def exercise():
        path = Path(os.environ.get("PAPERFLOW_TEST_MONITOR", Path(__file__).resolve().parents[3]
                                  / "first-step-preview-8795" / "workbuddy_monitor.py"))
        spec = importlib.util.spec_from_file_location("fake_run_monitor", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        calls, updates = [], []
        key = {"id": "project", "enabled": True, "realm": "", "accounts": []}

        def gateway(path, *, token=None, data=None, headers=None, timeout=3):
            calls.append((path, data, headers))
            if path == "/accounts?realm=all":
                return {"accounts": [{"uid": uid, "realm": realm, "enabled": True}
                                     for uid, realm in (("cn-a", "cn"), ("cn-b", "cn"), ("intl", "intl"))]}
            if path == '/accounts/test':
                return {'uid': data['uid'], 'ok': True}
            if path == "/settings/save":
                key.update(data["api_keys"][0])
                return {"ok": True}
            if path == "/settings":
                return {"api_keys": [key]}
            raise AssertionError(path)

        class Prefect:
            async def update_global_concurrency_limit(self, name, data):
                updates.append((name, data.limit))

            async def read_global_concurrency_limit_by_name(self, name):
                return SimpleNamespace(active=True, limit=updates[-1][1], slot_decay_per_second=0)

        @asynccontextmanager
        async def client_factory():
            yield Prefect()

        monitor = module.Monitor(gateway, prefect_transport=lambda: None, password_loader=lambda: None)
        monitor._token = "fake"
        route = {"gateway_key_id": "project", "model": "fake-model", "route_fingerprint": "fake",
                 "concurrency_caps": {"user": 140, "upstream": 140, "gateway_key": 140}, "per_account_limit": 2}
        capacity = AutoCapacity(monitor, lambda: route, client_factory)

        async def ready(_):
            return {"ready": True, "checks": []}

        app = control_app(monkeypatch, ready, test_start_accounts=capacity.test_start_accounts,
                          apply_start_capacity=capacity.apply_start_capacity)
        app.state.isolated_run.state = Paused()

        async def resume(_):
            assert await Admission(TASK_ID).running()
            app.state.isolated_run.state = Scheduled()

        monkeypatch.setattr(api, "aresume_flow_run", resume)
        request_id = str(uuid4())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://isolated") as client:
            url = f"/api/v1/prefect/tasks/{TASK_ID}/start"
            payload = {"request_id": request_id, "intent_seq": 1}
            first = await client.post(url, json=payload)
            again = await client.post(url, json=payload)
        assert first.status_code == 200 and first.json()["status"] == again.json()["status"] == "confirmed", (first.text, calls)
        assert first.json()["successful_accounts"] == 2 and first.json()["llm_limit"] == 4
        assert {(data['uid'], data['realm'], data['model']) for path, data, _ in calls
                if path == '/accounts/test'} == {('cn-a', 'cn', 'fake-model'), ('cn-b', 'cn', 'fake-model')}
        assert key["realm"] == "cn" and set(key["accounts"]) == {"cn-a", "cn-b"}
        assert len(updates) == 3 and all(limit == 4 for _, limit in updates)
        assert sum(path == '/settings/save' for path, _, _ in calls) == 1

    asyncio.run(exercise())
