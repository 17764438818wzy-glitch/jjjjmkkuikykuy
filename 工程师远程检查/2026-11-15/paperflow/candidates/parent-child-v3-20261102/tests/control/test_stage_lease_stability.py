"""Exercise lost control ACKs without any business or production requests."""
import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest

from paperflow_v3.control import stage_queue as queue
from paperflow_v3.control import stage_worker as worker

RUN_ID = UUID(int=10)
LEASE_ID = UUID(int=20)


def phase():
    return queue.PhaseLease({'names': ['rag'], 'lease_id': str(LEASE_ID),
        'flow_run_id': str(RUN_ID), 'previous_run_count': 0, 'limit_ids': {}, 'token': ''})


def losing_release(monkeypatch):
    calls = []
    class Client:
        async def release_concurrency_slots_with_lease(self, lease_id):
            calls.append(lease_id)
            raise httpx.ReadError('lost release ACK')
    @asynccontextmanager
    async def client():
        yield Client()
    monkeypatch.setattr(queue, 'get_client', client)
    return calls


def test_lost_release_ack_is_not_replayed_or_reported_released(monkeypatch, caplog):
    calls = losing_release(monkeypatch)
    lease = phase()
    async def scenario():
        await asyncio.gather(lease.finish(), lease.finish())
        await lease.finish()
    asyncio.run(scenario())
    assert calls == [LEASE_ID]
    assert lease.closed.is_set() and lease.released is False
    assert str(LEASE_ID) in caplog.text and 'ACK' in caplog.text


@pytest.mark.parametrize('original', [queue.StageQueueWait(['cleanup']), ValueError('original business error')])
def test_lost_release_ack_preserves_original_transition_or_error(monkeypatch, original):
    calls = losing_release(monkeypatch)
    lease = phase()
    async def scenario():
        with queue.bind_phase(lease):
            async with queue.phase_slot(['rag']):
                raise original
    with pytest.raises(type(original)) as caught:
        asyncio.run(scenario())
    assert caught.value is original
    assert calls == [LEASE_ID]


def test_unknown_acquire_is_control_error_and_never_retried(monkeypatch):
    calls = []
    child = SimpleNamespace(id=RUN_ID, run_count=1, labels={})
    monkeypatch.setattr(queue.FlowRunContext, 'get', lambda: SimpleNamespace(flow_run=child))
    class Client:
        async def read_global_concurrency_limit_by_name(self, name):
            return SimpleNamespace(id=UUID(int=30), name=name, active=True, slot_decay_per_second=0)
        async def increment_concurrency_slots_with_lease(self, *args, **kwargs):
            calls.append(kwargs['holder'].id)
            raise httpx.RemoteProtocolError('lost acquire ACK')
    @asynccontextmanager
    async def client():
        yield Client()
    monkeypatch.setattr(queue, 'get_client', client)
    with pytest.raises(RuntimeError, match='acquire.*ACK') as caught:
        asyncio.run(queue._new_resource(['rag']))
    assert not isinstance(caught.value, (httpx.RequestError, queue.StageQueueWait))
    assert isinstance(caught.value.__cause__, httpx.RemoteProtocolError)
    assert calls == [RUN_ID]


@pytest.mark.parametrize('started', [False, True])
def test_worker_transfers_release_ownership_once(monkeypatch, started):
    obj = object.__new__(worker.StageProcessWorker)
    grant = {'lease_id': str(LEASE_ID), 'flow_run_id': str(RUN_ID), 'names': ['rag']}
    obj.phase_grants = {RUN_ID: grant}
    obj.active_phases = {RUN_ID}
    calls = []
    async def native_run(self, flow_run, configuration, task_status):
        if started:
            task_status.started(123)
        return SimpleNamespace(status_code=0)
    async def returned(run_id, selected):
        calls.append(run_id)
        obj.phase_grants.pop(run_id, None)
        obj.active_phases.discard(run_id)
    monkeypatch.setattr(worker.ProcessWorker, 'run', native_run)
    monkeypatch.setattr(obj, '_return_grant', returned)
    class Configuration:
        env = {}
        def model_copy(self, **kwargs): return Configuration()
    asyncio.run(obj.run(SimpleNamespace(id=RUN_ID), Configuration()))
    assert calls == ([] if started else [RUN_ID])
    assert not obj.phase_grants and not obj.active_phases


def test_release_status_error_is_not_hidden(monkeypatch):
    class Client:
        async def release_concurrency_slots_with_lease(self, lease_id):
            request = httpx.Request('POST', 'http://prefect.invalid/release')
            response = httpx.Response(401, request=request)
            response.raise_for_status()
    @asynccontextmanager
    async def client(): yield Client()
    monkeypatch.setattr(queue, 'get_client', client)
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(phase().finish())
