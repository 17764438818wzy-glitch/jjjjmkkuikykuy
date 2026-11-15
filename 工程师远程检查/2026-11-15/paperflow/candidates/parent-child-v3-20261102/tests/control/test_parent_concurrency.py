"""Ten groups can progress independently under one native parent run."""

import asyncio
from pathlib import Path
from threading import Event, Lock
from uuid import uuid4

from prefect import flow
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter
from prefect.context import FlowRunContext, SyncClientContext
from prefect.flow_engine import run_flow
from prefect.testing.utilities import prefect_test_harness
from prefect.variables import get_or_create_client


FIRST_STEP = Path(__file__).resolve().parents[3] / "first-step-control-20261029"


def test_ten_groups_refill_and_paused_classification_finishes_child_registration(tmp_path, monkeypatch):
    from paperflow_v3.control import parent as parent_module
    from paperflow_v3.control.classification import first_step_modules
    from paperflow_v3.control.receipts import read_receipts

    library = tmp_path / "source"
    library.mkdir()
    groups = []
    for index in range(12):
        path = library / f"group-{index:02}"
        path.mkdir()
        groups.append({"group_path": str(path), "library_root": str(library)})
    monkeypatch.setattr(parent_module, "discover_sources", lambda *_args, **_kwargs: {"groups": groups, "ungrouped": []})
    classify = first_step_modules(FIRST_STEP)["classification"]
    release = {Path(item["group_path"]).name: Event() for item in groups}
    guard, started, finished, active = Lock(), [], [], set()
    maximum = [0]
    first_clients = []

    class Gate:
        def __init__(self):
            self.allowed = Event()
            self.allowed.set()

        async def wait(self):
            while not self.allowed.is_set():
                await asyncio.sleep(.02)

        async def running(self):
            return self.allowed.is_set()

        def running_sync(self):
            return self.allowed.is_set()

    def synthetic_classification(**kwargs):
        path = kwargs["group_path"]
        name = path.name
        context = SyncClientContext.get()
        assert context is not None
        with guard:
            started.append(name)
            if len(started) <= 10:
                first_clients.append(context.client)
            active.add(name)
            maximum[0] = max(maximum[0], len(active))
        if not release[name].wait(timeout=45):
            raise TimeoutError("isolated classification was not released")
        with guard:
            active.remove(name)
            finished.append(name)
        return {"task_id": str(kwargs["task_id"]), "group_id": classify._group_id(path, kwargs["library_root"]),
                "group": name, "paper": {}, "status": "passed", "delivery_complete": True,
                "registration_status": "registered"}

    monkeypatch.setattr(parent_module, "classify_and_register", synthetic_classification)
    dispatch_started, dispatch_release = Event(), Event()
    original_dispatch = parent_module.schedule_registered_paper

    async def delayed_dispatch(deployment_id, registered, admission):
        if registered["group"] == "group-01":
            dispatch_started.set()
            while not dispatch_release.is_set():
                await asyncio.sleep(.02)
        return await original_dispatch(deployment_id, registered, admission)

    monkeypatch.setattr(parent_module, "schedule_registered_paper", delayed_dispatch)
    child_runs = []

    async def verify_flow_admission_client():
        from paperflow_v3.control.admission import Admission

        first, inferred = get_or_create_client()
        assert inferred
        admission = Admission(uuid4())
        assert await admission.running() is False
        assert await admission.running() is False
        second, inferred = get_or_create_client()
        assert inferred and second is first

    @flow(name="isolated-concurrent-paper", persist_result=False)
    async def paper(task_id: str, group_id: str, registered: dict):
        await verify_flow_admission_client()
        child_runs.append((task_id, group_id))

    async def exercise(deployment_id):
        from paperflow_v3.control.admission import begin_control_session
        await begin_control_session()
        gate = Gate()
        run_id = asyncio.get_running_loop().create_future()

        @flow(name="isolated-concurrent-parent", persist_result=False)
        async def parent():
            task_id = FlowRunContext.get().flow_run.id
            await verify_flow_admission_client()
            run_id.set_result(task_id)
            return await parent_module.classify_and_dispatch(task_id=task_id, source_paths=[str(library)],
                first_step_root=FIRST_STEP, work_root=tmp_path / "work", history_roots=(),
                legacy_classification_root=tmp_path / "old", nimble_endpoint="http://127.0.0.1:1/v1/systemone",
                nimble_lock_path=tmp_path / "nimble.lock", child_deployment_id=deployment_id, admission=gate)

        async def until(predicate, timeout=12):
            async with asyncio.timeout(timeout):
                while not predicate():
                    await asyncio.sleep(.03)

        async def children(task_id):
            async with get_client() as client:
                return await client.read_flow_runs(flow_run_filter=FlowRunFilter(
                    parent_flow_run_id={"any_": [task_id]}, tags={"all_": ["paperflow-v3-paper"]}))

        task = asyncio.create_task(parent())
        task_id = await run_id
        try:
            await until(lambda: len(started) == 10, timeout=12)
            assert set(started) == {f"group-{index:02}" for index in range(10)}
            assert maximum[0] == 10
            assert len({id(client) for client in first_clients}) == 10
            release["group-01"].set()
            await until(lambda: "group-10" in started)
            await until(dispatch_started.is_set)
            assert not await children(task_id)
            dispatch_release.set()
            async with asyncio.timeout(12):
                while not (first := await children(task_id)):
                    await asyncio.sleep(.05)
            assert len(first) == 1 and first[0].parameters["registered"]["group"] == "group-01"
            assert not any(row["kind"] == "classification_complete" for row in await read_receipts(task_id))
            await run_flow(paper, flow_run=first[0])
            assert len(child_runs) == 1 and not task.done()

            gate.allowed.clear()
            release["group-00"].set()
            await until(lambda: "group-00" in finished)
            async with asyncio.timeout(8):
                while len(await children(task_id)) < 2:
                    await asyncio.sleep(.05)
            assert len(await children(task_id)) == 2
            assert "group-11" not in started
            gate.allowed.set()
            await until(lambda: "group-11" in started)
            async with asyncio.timeout(12):
                while len(await children(task_id)) < 2:
                    await asyncio.sleep(.05)
        finally:
            gate.allowed.set()
            dispatch_release.set()
            for event in release.values():
                event.set()
            await asyncio.wait_for(task, timeout=60)
        assert maximum[0] <= 10
        assert len(await children(task_id)) == 12
        assert all(client._closed for client in first_clients)

    with prefect_test_harness(server_startup_timeout=60):
        with get_client(sync_client=True) as client:
            deployment = client.create_deployment(client.create_flow(paper), name="paper")
        asyncio.run(exercise(deployment))
