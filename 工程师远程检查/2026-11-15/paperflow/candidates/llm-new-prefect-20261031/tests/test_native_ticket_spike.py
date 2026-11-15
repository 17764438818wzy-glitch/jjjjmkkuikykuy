"""Isolated Prefect feasibility checks; no production server or model calls."""

import asyncio
import importlib.util
from pathlib import Path
import threading

from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.client.orchestration import get_client
from prefect.events.worker import EventsWorker
from prefect.states import Paused, Pending, Running, Scheduled
from prefect.task_worker import TaskWorker
from prefect.testing.utilities import prefect_test_harness


def test_existing_small_order_can_run_as_native_task_under_same_parent(tmp_path):
    marker = tmp_path / "ran.txt"

    @flow
    def synthetic_parent():
        pass

    @task(persist_result=False)
    def synthetic_ticket():
        marker.write_text("called", encoding="utf-8")

    async def exercise():
        async with get_client() as client:
            parent = await client.create_flow_run(synthetic_parent, state=Running())
            original = await client.create_task_run(
                synthetic_ticket,
                flow_run_id=parent.id,
                dynamic_key="synthetic-folder",
                state=Pending(name="等待 LLM"),
            )
            scheduled = Scheduled(name="LLM 待执行")
            scheduled.state_details.deferred = True
            result = await client.set_task_run_state(original.id, scheduled)
            assert result.status.value == "ACCEPT"
            updated = await client.read_task_run(original.id)
            assert updated.id == original.id
            assert updated.flow_run_id == parent.id
            worker = TaskWorker(synthetic_ticket, limit=1)
            await worker.execute_task_run(updated)
            # Prefect 3 emits task states through an asynchronous event worker;
            # completion of the local callable is not synchronous API readback.
            await EventsWorker.drain_all()
            observed = []
            for _ in range(40):
                final = await client.read_task_run(original.id)
                observed.append(final.state.type.value)
                if final.state.is_completed():
                    break
                await asyncio.sleep(0.25)
            else:
                raise AssertionError(f"server did not record completion: {observed}")
            assert final.state.is_completed()
            assert final.id == original.id
            assert final.flow_run_id == parent.id
            assert marker.read_text(encoding="utf-8") == "called"

    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(exercise())


def test_paused_parent_does_not_admit_a_native_child():
    """The real parent's suspended state cannot run a native folder task."""
    @flow
    def synthetic_parent():
        pass

    @task
    def folder_ticket():
        raise AssertionError('this test never executes the task')

    async def exercise():
        async with get_client() as client:
            parent = await client.create_flow_run(synthetic_parent, state=Paused(name='Suspended'))
            ticket = await client.create_task_run(
                folder_ticket, flow_run_id=parent.id, dynamic_key='folder', state=Pending())
            decision = await client.set_task_run_state(ticket.id, Running())
            assert decision.status.value == 'REJECT'
            assert decision.state.is_paused() and decision.state.name == 'NotReady'
            observed = await client.read_task_run(ticket.id)
            assert observed.state.is_paused() and observed.flow_run_id == parent.id

    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(exercise())


def test_running_native_child_finishes_after_deployed_parent_suspends(tmp_path):
    """Check whether suspending the parent releases it without killing child work."""
    entered, release = threading.Event(), threading.Event()
    marker = tmp_path / 'after-suspend.txt'

    @flow
    def synthetic_parent():
        pass

    @task(persist_result=False, retries=0)
    def folder_ticket():
        entered.set()
        if not release.wait(10):
            raise RuntimeError('synthetic task was not released')
        marker.write_text('completed', encoding='utf8')

    async def exercise():
        async with get_client() as client:
            flow_id = await client.create_flow(synthetic_parent)
            deployment_id = await client.create_deployment(flow_id, 'synthetic-parent')
            parent = await client.create_flow_run_from_deployment(deployment_id, state=Running())
            ticket = await client.create_task_run(
                folder_ticket, flow_run_id=parent.id, dynamic_key='folder', state=Pending())
            scheduled = Scheduled()
            scheduled.state_details.deferred = True
            assert (await client.set_task_run_state(ticket.id, scheduled)).status.value == 'ACCEPT'
            worker = asyncio.create_task(TaskWorker(folder_ticket, limit=1).execute_task_run(
                await client.read_task_run(ticket.id)))
            try:
                assert await asyncio.to_thread(entered.wait, 5)
                decision = await client.set_flow_run_state(
                    parent.id, Paused(name='Suspended', reschedule=True, pause_key='synthetic'))
                assert decision.status.value == 'ACCEPT'
                assert decision.state.is_paused()
            finally:
                release.set()
                await worker
            await EventsWorker.drain_all()
            assert marker.read_text(encoding='utf8') == 'completed'
            for _ in range(40):
                current = await client.read_task_run(ticket.id)
                if current.state and current.state.is_completed():
                    break
                await asyncio.sleep(0.25)
            assert current.state and current.state.is_completed()
            assert (await client.read_flow_run(parent.id)).state.is_paused()

    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(exercise())


def test_new_folder_task_can_yield_between_preprocessing_and_llm(tmp_path):
    """One real native TaskRun must survive the wait without a second FlowRun."""
    events = tmp_path / "phases.txt"

    @flow
    def synthetic_parent():
        pass

    def folder_fn():
        if not events.exists():
            events.write_text("preprocessing\n", encoding="utf-8")
            return Pending(name="等待 LLM")
        with events.open("a", encoding="utf-8") as stream:
            stream.write("llm\n")
        return None

    spec = importlib.util.spec_from_file_location("native_task", Path(__file__).parent.parent / "native_task.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    folder_ticket = module.FolderTask(folder_fn)

    async def recorded(client, run_id, wanted):
        await EventsWorker.drain_all()
        observed = []
        for _ in range(40):
            value = await client.read_task_run(run_id)
            observed.append((value.state.type.value, value.state.name) if value.state else None)
            if value.state is not None and wanted(value.state):
                return value
            await asyncio.sleep(0.25)
        raise AssertionError(f"server did not record the expected state: {observed}")

    async def exercise():
        async with get_client() as client:
            parent = await client.create_flow_run(synthetic_parent, state=Running())
            first = await client.create_task_run(
                folder_ticket, flow_run_id=parent.id,
                dynamic_key="fresh-folder", state=Pending(name="等待前处理"),
            )
            start = Scheduled(name="前处理待执行")
            start.state_details.deferred = True
            assert (await client.set_task_run_state(first.id, start)).status.value == "ACCEPT"
            first_worker = TaskWorker(folder_ticket, limit=1)
            assert first_worker.tasks[0].cache_policy == NO_CACHE
            await first_worker.execute_task_run(await client.read_task_run(first.id))
            waiting = await recorded(client, first.id, lambda s: s.is_pending() and s.name == "等待 LLM")
            assert waiting.id == first.id and waiting.flow_run_id == parent.id
            assert events.read_text(encoding="utf-8") == "preprocessing\n"

            resume = Scheduled(name="LLM 待执行")
            resume.state_details.deferred = True
            assert (await client.set_task_run_state(first.id, resume)).status.value == "ACCEPT"
            # Each direct execution closes its worker. A fresh worker also
            # simulates process restart while the ticket is waiting.
            await TaskWorker(folder_ticket, limit=1).execute_task_run(await client.read_task_run(first.id))
            finished = await recorded(client, first.id, lambda s: s.is_completed())
            assert finished.id == first.id and finished.flow_run_id == parent.id
            assert events.read_text(encoding="utf-8") == "preprocessing\nllm\n"

    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(exercise())


def test_native_worker_receives_a_scheduled_folder_task(tmp_path):
    marker = tmp_path / 'scheduled.txt'

    @flow
    def synthetic_parent():
        pass

    @task
    def folder_ticket():
        marker.write_text('executed', encoding='utf8')

    async def exercise():
        async with get_client() as client:
            parent = await client.create_flow_run(synthetic_parent, state=Running())
            ticket = await client.create_task_run(
                folder_ticket, flow_run_id=parent.id, dynamic_key='folder-1', state=Pending(),
            )
            worker = TaskWorker(folder_ticket, limit=1)
            async with worker:
                subscriber = asyncio.create_task(worker._subscribe_to_task_scheduling())
                try:
                    # The subscription must be open before the synthetic run is enqueued.
                    await asyncio.sleep(0.5)
                    schedule = Scheduled()
                    schedule.state_details.deferred = True
                    assert (await client.set_task_run_state(ticket.id, schedule)).status.value == 'ACCEPT'
                    for _ in range(40):
                        if marker.is_file():
                            break
                        await asyncio.sleep(0.25)
                    assert marker.read_text(encoding='utf8') == 'executed'
                    await EventsWorker.drain_all()
                    for _ in range(40):
                        final = await client.read_task_run(ticket.id)
                        if final.state and final.state.is_completed():
                            break
                        await asyncio.sleep(0.25)
                    assert final.state and final.state.is_completed()
                    assert final.flow_run_id == parent.id
                finally:
                    subscriber.cancel()
                    try:
                        await subscriber
                    except asyncio.CancelledError:
                        pass

    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(exercise())
