import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from urllib.error import URLError

from paperflow_v3.control.auto_capacity import AutoCapacity
import importlib.util
from pathlib import Path

monitor_path = Path(__file__).resolve().parents[3] / "first-step-preview-8795" / "workbuddy_monitor.py"
spec = importlib.util.spec_from_file_location("native_monitor_integration", monitor_path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
Monitor = module.Monitor


def test_native_account_test_drives_existing_allowlist_and_prefect_capacity():
    async def run():
        accounts = [
            {"uid": "good", "realm": "cn", "enabled": True},
            {"uid": "failed", "realm": "cn", "enabled": True},
            {"uid": "unknown", "realm": "cn", "enabled": True},
            {"uid": "international", "realm": "intl", "enabled": True},
        ]
        state = {"allowlist": [], "limits": {}, "tests": []}

        def gateway(path, *, token=None, data=None, headers=None, timeout=3):
            if path == "/accounts?realm=all":
                return {"accounts": accounts}
            if path == "/accounts/test":
                state["tests"].append((data["uid"], data["realm"], data["model"]))
                if data["uid"] == "unknown":
                    raise URLError("simulated timeout")
                return {"uid": data["uid"], "ok": data["uid"] == "good"}
            if path == "/settings/save":
                state["allowlist"] = list(data["api_keys"][0]["accounts"])
                return {"ok": True}
            if path == "/settings":
                return {"api_keys": [{"id": "project", "enabled": True, "realm": "cn",
                                       "accounts": state["allowlist"]}]}
            raise AssertionError(path)

        class Client:
            async def update_global_concurrency_limit(self, name, update):
                state["limits"][name] = update.limit

            async def read_global_concurrency_limit_by_name(self, name):
                return SimpleNamespace(active=True, limit=state["limits"][name], slot_decay_per_second=0)

        @asynccontextmanager
        async def client_factory():
            yield Client()

        monitor = Monitor(gateway, prefect_transport=lambda: None, password_loader=lambda: None)
        monitor._token = "fake"
        route = {"per_account_limit": 2, "route_fingerprint": "same", "model": "paper-model",
                 "gateway_key_id": "project", "concurrency_caps": {"user": 140, "upstream": 140,
                                                                    "gateway_key": 140}}
        capacity = AutoCapacity(monitor, lambda: route.copy(), client_factory)

        async def current():
            pass

        tested = await capacity.test_start_accounts(ensure_current=current)
        await capacity.apply_start_capacity(tested, tested["successful_count"] * 2, ensure_current=current)

        assert set(state["tests"]) == {(uid, "cn", "paper-model") for uid in ("good", "failed", "unknown")}
        assert tested["successful_count"] == 1
        assert state["allowlist"] == ["good"]
        assert len(state["limits"]) == 3 and set(state["limits"].values()) == {2}

    asyncio.run(run())
