"""Verify lost release ACK behavior against an isolated native Prefect server."""
import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from prefect import flow
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.states import Running
from prefect.testing.utilities import prefect_test_harness

from paperflow_v3.control import stage_queue as queue


@flow(name='isolated-lease-ack-probe', persist_result=False)
def probe():
    raise AssertionError('Business code must not run during the control test')


def test_native_atomic_capacity_after_release_ack_loss(monkeypatch):
    async def scenario():
        async with get_client() as client:
            @asynccontextmanager
            async def context(): yield client
            monkeypatch.setattr(queue, 'get_client', lambda **kwargs:
                get_client(**kwargs) if kwargs else context())
            release = client.release_concurrency_slots_with_lease
            for committed in (False, True):
                name = 'isolated-ack-' + uuid4().hex
                limit_id = await client.create_global_concurrency_limit(GlobalConcurrencyLimitCreate(name=name, limit=1))
                child = await client.create_flow_run(probe, state=Running())
                monkeypatch.setattr(queue.FlowRunContext, 'get', lambda: SimpleNamespace(flow_run=child, flow=probe))
                grant = await queue.acquire_stage_lease(client, [name], child.id)
                old = queue.PhaseLease({'names': [name], 'lease_id': str(grant.lease_id),
                    'flow_run_id': str(child.id), 'previous_run_count': child.run_count - 1,
                    'limit_ids': {name: str(limit_id)}, 'token': ''})
                posts = []
                async def lose_ack(lease_id):
                    posts.append(lease_id)
                    if lease_id == grant.lease_id:
                        if committed: await release(lease_id)
                        raise httpx.ReadError('lost native release ACK')
                    await release(lease_id)
                monkeypatch.setattr(client, 'release_concurrency_slots_with_lease', lose_ack)
                await old.finish()
                await old.finish()
                assert old.closed.is_set() and not old.released
                with queue.bind_phase(old):
                    if committed:
                        selected = await queue._take_resource(old, [name])
                        assert selected is not old and selected.grant['lease_id'] != old.grant['lease_id']
                        assert (await client.read_global_concurrency_limit_by_name(name)).active_slots == 1
                        selected.operation.release()
                        await selected.finish()
                        assert (await client.read_global_concurrency_limit_by_name(name)).active_slots == 0
                    else:
                        with pytest.raises(queue.StageQueueWait):
                            await queue._take_resource(old, [name])
                        assert (await client.read_global_concurrency_limit_by_name(name)).active_slots == 1
                assert posts.count(grant.lease_id) == 1
    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(scenario())
