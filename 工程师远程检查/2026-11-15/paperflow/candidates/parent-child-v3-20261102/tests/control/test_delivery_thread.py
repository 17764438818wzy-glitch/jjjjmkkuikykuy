"""An admitted delivery operation drains before its parent reports cancellation."""

import asyncio
from pathlib import Path
from threading import Event
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from prefect.client.orchestration import SyncPrefectClient
from prefect.settings import PREFECT_API_URL, temporary_settings


def test_cancelled_delivery_waits_for_the_admitted_operation(monkeypatch, stage_store):
    candidate = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(candidate.parents[1] / "prefect-paperflow"))
    from paperflow_prefect import legacy
    from paperflow_v3.control import flows
    from paperflow_v3.delivery import runtime

    entered, release, finished = Event(), Event(), Event()
    monkeypatch.setattr(legacy, "bind", lambda *_args: None)
    monkeypatch.setattr(SyncPrefectClient, "raise_for_api_version_mismatch_once", lambda _client: None)

    def delivery(**_kwargs):
        assert _kwargs['admission'].admit_sync()
        entered.set()
        if not release.wait(10):
            raise TimeoutError("synthetic delivery was not released")
        finished.set()
        return {"status": "complete"}

    monkeypatch.setattr(runtime, "deliver_paper", delivery)

    async def records(_run):
        return []

    monkeypatch.setattr(flows, 'read_receipts', records)

    async def exercise():
        ids = {'task_id': str(uuid4()), 'task_run_id': str(uuid4()), 'group_id': 'folder'}
        parent = stage_store.admission.Admission(ids['task_id'])
        await parent.start()
        task = asyncio.create_task(flows._delivery_step(ids, {}, object(), parent, {'rag': ['rag']}, None, False))
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(.1)
        try:
            assert not task.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()

    with temporary_settings({PREFECT_API_URL: "http://127.0.0.1:1/api"}):
        asyncio.run(exercise())


def test_cancelled_delivery_leaves_native_resource_queue_without_starting(monkeypatch, stage_store):
    from paperflow_prefect import resources
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
    from prefect.concurrency.asyncio import concurrency
    from prefect.testing.utilities import prefect_test_harness
    from paperflow_v3.control import flows
    from paperflow_v3.control.admission import admitted_sync_slot
    from paperflow_v3.delivery import runtime

    waiting = Event()
    acquired_queue = Event()
    effects = []
    monkeypatch.setattr(runtime.legacy, 'bind', lambda *_args: None)
    async def records(_run):
        return []
    monkeypatch.setattr(flows, 'read_receipts', records)

    def delivery(**kwargs):
        waiting.set()
        with admitted_sync_slot(kwargs['admission'], kwargs['limits']['rag']):
            effects.append('must-not-start')
        return {'status': 'complete'}
    monkeypatch.setattr(runtime, 'deliver_paper', delivery)
    original_acquire = resources.prefect_sync.aacquire_concurrency_slots_with_lease
    async def observe_queue(*args, **kwargs):
        acquired_queue.set()
        return await original_acquire(*args, **kwargs)
    monkeypatch.setattr(resources.prefect_sync, 'aacquire_concurrency_slots_with_lease', observe_queue)

    async def exercise():
        ids = {'task_id': str(uuid4()), 'task_run_id': str(uuid4()), 'group_id': 'queued-folder'}
        parent = stage_store.admission.Admission(ids['task_id'])
        await parent.start()
        monkeypatch.setattr(parent, 'running_sync', lambda: True)
        name = 'pause-cancel-isolated-' + uuid4().hex
        async with get_client() as client:
            await client.create_global_concurrency_limit(GlobalConcurrencyLimitCreate(name=name, limit=1))
        async with concurrency(name, strict=True):
            task = asyncio.create_task(flows._delivery_step(ids, {}, object(), parent, {'rag': [name]}, None, False))
            assert await asyncio.to_thread(waiting.wait, 2)
            assert await asyncio.to_thread(acquired_queue.wait, 3), 'worker never reached the native resource queue'
            await asyncio.sleep(.2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 5)
            assert effects == [] and stage_store.history == []
            async with get_client() as client:
                assert (await client.read_global_concurrency_limit_by_name(name)).active_slots == 1
    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(exercise())
