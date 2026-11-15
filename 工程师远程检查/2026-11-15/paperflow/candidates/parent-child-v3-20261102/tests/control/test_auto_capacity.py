"""Automatic capacity wiring with a fake route, gateway and Prefect client."""
import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest


from paperflow_v3.control import auto_capacity as module


class FakeGateway:
    def __init__(self, n):
        self.uids = [f"cn-{i}" for i in range(n)]
        self.writes = []

    def test_domestic_accounts(self, model, *, before_batch):
        before_batch()
        assert model == "actual-domestic-model"
        return {"status": "available", "usable_uids": self.uids}

    def allow_domestic_accounts(self, key_id, uids):
        self.writes.append((key_id, list(uids)))
        return {"status": "confirmed"}


class FakePrefect:
    def __init__(self):
        self.updates = []
        self.limits = {}

    async def update_global_concurrency_limit(self, name, data):
        self.updates.append((name, data.limit))
        self.limits[name] = data.limit

    async def read_global_concurrency_limit_by_name(self, name):
        return SimpleNamespace(active=True, limit=self.limits[name], slot_decay_per_second=0)


def create_capacity(n=5):
    route = {"gateway_key_id": "project", "model": "actual-domestic-model", "route_fingerprint": "1/2/3",
             "concurrency_caps": {"user": 140, "upstream": 140, "gateway_key": 140}, "per_account_limit": 2}
    gateway, client = FakeGateway(n), FakePrefect()

    @asynccontextmanager
    async def client_factory():
        yield client

    return module.AutoCapacity(gateway, lambda: dict(route), client_factory), gateway, client, route


def test_fifty_usable_accounts_set_all_three_existing_prefect_limits_to_one_hundred():
    async def check():
        capacity, gateway, client, _ = create_capacity(50)
        result = await capacity.test_start_accounts(ensure_current=lambda: asyncio.sleep(0))
        assert result["successful_count"] == 50
        await capacity.apply_start_capacity(result, 100, ensure_current=lambda: asyncio.sleep(0))
        assert gateway.writes == [("project", gateway.uids)]
        assert client.updates == [("paperflow-connected-" + name, 100)
                                  for name in ("total_llm", "body_llm", "attachment_llm")]
    asyncio.run(check())


def test_existing_route_cap_below_target_stops_before_any_write():
    async def check():
        capacity, gateway, client, route = create_capacity(50)
        result = await capacity.test_start_accounts(ensure_current=lambda: asyncio.sleep(0))
        route["concurrency_caps"] = {"user": 80, "upstream": 140, "gateway_key": 140}
        with pytest.raises(module.CapacityError, match="80"):
            await capacity.apply_start_capacity(result, 100, ensure_current=lambda: asyncio.sleep(0))
        assert not gateway.writes and not client.updates
    asyncio.run(check())


def test_gateway_zero_cap_means_unlimited_in_the_existing_gateway():
    async def check():
        capacity, gateway, client, route = create_capacity(5)
        route["concurrency_caps"]["gateway_key"] = 0
        result = await capacity.test_start_accounts(ensure_current=lambda: asyncio.sleep(0))
        await capacity.apply_start_capacity(result, 10, ensure_current=lambda: asyncio.sleep(0))
        assert len(gateway.writes) == 1 and len(client.updates) == 3
    asyncio.run(check())


def test_pause_during_configuration_prevents_following_writes():
    async def check():
        capacity, gateway, client, _ = create_capacity()
        result = await capacity.test_start_accounts(ensure_current=lambda: asyncio.sleep(0))
        calls = 0

        async def ensure_current():
            nonlocal calls
            calls += 1
            if calls == 3:
                raise RuntimeError("superseded")

        with pytest.raises(RuntimeError, match="superseded"):
            await capacity.apply_start_capacity(result, 10, ensure_current=ensure_current)
        assert len(gateway.writes) == 1
        assert client.updates == [("paperflow-connected-total_llm", 10)]
    asyncio.run(check())


def test_route_changed_during_testing_is_not_overwritten():
    async def check():
        capacity, gateway, client, route = create_capacity()
        result = await capacity.test_start_accounts(ensure_current=lambda: asyncio.sleep(0))
        route["route_fingerprint"] = "different"
        with pytest.raises(module.CapacityError):
            await capacity.apply_start_capacity(result, 10, ensure_current=lambda: asyncio.sleep(0))
        assert not gateway.writes and not client.updates
    asyncio.run(check())
