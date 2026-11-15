import asyncio
import threading
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from paperflow_v3.control.auto_capacity import AutoCapacity


@pytest.mark.parametrize("delayed_write", ["gateway", "prefect"])
def test_previous_start_cannot_overwrite_newer_account_configuration(delayed_write):
    async def run():
        entered, release = threading.Event(), threading.Event()
        state = {"current": "old", "accounts": [], "limits": {}}
        route = {
            "per_account_limit": 2,
            "route_fingerprint": "fake-route",
            "model": "fake-model",
            "gateway_key_id": "fake-key",
            "concurrency_caps": {"user": 140, "upstream": 140, "gateway_key": 140},
        }

        class Gateway:
            def allow_domestic_accounts(self, key_id, uids):
                if delayed_write == "gateway" and uids == ["old"]:
                    entered.set()
                    assert release.wait(5)
                state["accounts"] = list(uids)
                return {"status": "confirmed"}

        class Client:
            async def update_global_concurrency_limit(self, name, update):
                if delayed_write == "prefect" and update.limit == 2:
                    entered.set()
                    assert await asyncio.to_thread(release.wait, 5)
                state["limits"][name] = update.limit

            async def read_global_concurrency_limit_by_name(self, name):
                return SimpleNamespace(active=True, limit=state["limits"][name], slot_decay_per_second=0)

        @asynccontextmanager
        async def client_factory():
            yield Client()

        capacity = AutoCapacity(Gateway(), lambda: route.copy(), client_factory)

        async def apply(owner, accounts, limit):
            async def ensure_current():
                if state["current"] != owner:
                    raise RuntimeError("superseded")

            await capacity.apply_start_capacity(
                {"route": route.copy(), "account_uids": accounts}, limit,
                ensure_current=ensure_current,
            )

        old = asyncio.create_task(apply("old", ["old"], 2))
        assert await asyncio.to_thread(entered.wait, 5)
        state["current"] = "new"
        new = asyncio.create_task(apply("new", ["new-1", "new-2"], 4))
        try:
            try:
                await asyncio.wait_for(asyncio.shield(new), timeout=0.2)
            except TimeoutError:
                pass
        finally:
            release.set()
        with pytest.raises(RuntimeError, match="superseded"):
            await old
        await new
        assert state["accounts"] == ["new-1", "new-2"]
        assert set(state["limits"].values()) == {4}

    asyncio.run(run())
