"""Restarting the executor invalidates starts even if the control API stays up."""

import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from uuid import uuid4

import psutil
from prefect import flow
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.client.schemas.objects import WorkerStatus
from prefect.deployments.runner import RunnerDeployment
from prefect.settings import PREFECT_API_URL
from prefect.testing.utilities import prefect_test_harness


@flow(name="isolated-admission-probe", persist_result=False)
def worker_probe(task_id: str, marker: str):
    from paperflow_v3.control.admission import Admission
    if Admission(task_id).running_sync():
        Path(marker).write_text("synthetic", encoding="utf8")


def test_executor_start_requires_new_manual_task_start(monkeypatch):
    from paperflow_v3.control import worker
    from paperflow_v3.control.admission import Admission, begin_control_session

    monkeypatch.setattr(worker, "load_runtime", lambda: (object(), object()))

    async def exercise():
        await begin_control_session()
        first, second = Admission(uuid4()), Admission(uuid4())
        await first.start()
        await second.start()
        assert await first.running() and await second.running()
        await worker.prepare_executor()
        assert not await first.running() and not await second.running()
        await first.start()
        assert await first.running() and not await second.running()

    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(exercise())


def test_process_worker_entry_resets_old_starts_before_picking_queued_work(tmp_path, monkeypatch):
    from paperflow_v3.control.admission import Admission, begin_control_session

    candidate = Path(__file__).resolve().parents[2]
    workspace = candidate.parents[1]
    project_root = workspace / "prefect-paperflow"
    first_step = workspace / "candidates/first-step-control-20261029"
    sys.path.insert(0, str(project_root))
    from paperflow_prefect.settings import Settings

    library, data = tmp_path / "library", tmp_path / "data"
    library.mkdir()
    data.mkdir()
    monkeypatch.chdir(candidate)
    with prefect_test_harness(server_startup_timeout=60):
        config = {**Settings.load().values, "execution_enabled": True, "library_root": str(library),
                  "data_root": str(data), "prefect_api_url": PREFECT_API_URL.value()}
        path = tmp_path / "project.json"
        path.write_text(json.dumps(config, ensure_ascii=False), encoding="utf8")
        with get_client(sync_client=True) as client:
            client.create_work_pool(WorkPoolCreate(name="isolated-worker", type="process"))
        deployment = RunnerDeployment.from_flow(worker_probe, name="probe", work_pool_name="isolated-worker",
                                                 job_variables={"working_dir": str(candidate)}).apply(
            work_pool_name="isolated-worker")
        task = str(uuid4())
        gate = Admission(task)
        asyncio.run(begin_control_session())
        asyncio.run(gate.start())
        first_marker, second_marker = tmp_path / "before.txt", tmp_path / "after.txt"
        with get_client(sync_client=True) as client:
            queued = client.create_flow_run_from_deployment(deployment, parameters={"task_id": task, "marker": str(first_marker)})
        env = dict(os.environ, PREFECT_API_URL=PREFECT_API_URL.value(), PAPERFLOW_V3_PROJECT_CONFIG=str(path),
                   PAPERFLOW_V3_PROJECT_ROOT=str(project_root), PAPERFLOW_V3_FIRST_STEP_ROOT=str(first_step))
        process = subprocess.Popen([sys.executable, "-m", "paperflow_v3.control.worker", "--pool", "isolated-worker"],
            cwd=str(candidate), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        try:
            deadline = time.monotonic() + 55
            while time.monotonic() < deadline:
                with get_client(sync_client=True) as client:
                    workers = client.read_workers_for_work_pool("isolated-worker")
                    state = client.read_flow_run(queued.id).state
                if workers and any(worker.status == WorkerStatus.ONLINE for worker in workers) and state.is_final():
                    break
                if process.poll() is not None:
                    raise AssertionError("isolated worker exited before processing")
                time.sleep(.25)
            assert state.is_completed() and not first_marker.exists()
            assert not asyncio.run(gate.running())
            asyncio.run(gate.start())
            with get_client(sync_client=True) as client:
                resumed = client.create_flow_run_from_deployment(deployment, parameters={"task_id": task, "marker": str(second_marker)})
            deadline = time.monotonic() + 55
            while time.monotonic() < deadline:
                with get_client(sync_client=True) as client:
                    state = client.read_flow_run(resumed.id).state
                if state.is_final():
                    break
                time.sleep(.25)
            assert state.is_completed() and second_marker.read_text(encoding="utf8") == "synthetic"
        finally:
            try:
                children = psutil.Process(process.pid).children(recursive=True)
            except psutil.NoSuchProcess:
                children = []
            for child in reversed(children):
                try:
                    child.terminate()
                except psutil.NoSuchProcess:
                    pass
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=10)
