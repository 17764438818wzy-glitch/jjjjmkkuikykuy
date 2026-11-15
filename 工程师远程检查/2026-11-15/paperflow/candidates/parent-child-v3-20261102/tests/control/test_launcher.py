"""The candidate can start from its own entry without borrowing an old worker."""

import asyncio
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from uuid import uuid4

import httpx
import psutil
import pytest
from prefect import flow
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.client.schemas.objects import WorkerStatus
from prefect.deployments.runner import RunnerDeployment
from prefect.settings import PREFECT_API_URL
from prefect.testing.utilities import prefect_test_harness


@flow(name="isolated-launch-environment", persist_result=False)
def launch_probe(output: str):
    Path(output).write_text(json.dumps({
        "parent": os.environ.get("PAPERFLOW_V3_PARENT_DEPLOYMENT_ID"),
        "child": os.environ.get("PAPERFLOW_V3_CHILD_DEPLOYMENT_ID"),
        "config": os.environ.get("PAPERFLOW_V3_PROJECT_CONFIG"),
    }), encoding="utf8")


def test_candidate_pool_rejects_foreign_worker_and_restrictive_pool():
    from paperflow_v3.control.launch import ensure_dedicated_pool

    with prefect_test_harness(server_startup_timeout=60):
        with get_client(sync_client=True) as client:
            client.create_work_pool(WorkPoolCreate(name="occupied", type="process"))
            client.send_worker_heartbeat("occupied", "unverified-worker", heartbeat_interval_seconds=30)
            client.create_work_pool(WorkPoolCreate(name="restricted", type="process", concurrency_limit=1))
        with pytest.raises(RuntimeError, match="已有在线 worker"):
            ensure_dedicated_pool("occupied")
        with pytest.raises(RuntimeError, match="无池级并发上限"):
            ensure_dedicated_pool("restricted")
        with get_client(sync_client=True) as client:
            assert client.read_work_pool("restricted").concurrency_limit == 1


def test_worker_stops_if_launcher_process_disappears():
    from paperflow_v3.control.worker import wait_for_launcher

    owner = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(1)"],
                             creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    worker = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                              creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    try:
        assert wait_for_launcher(worker, owner.pid) != 0
        assert worker.poll() is not None
    finally:
        if worker.poll() is None:
            worker.terminate()
            worker.wait(timeout=5)
        if owner.poll() is None:
            owner.terminate()
        owner.wait(timeout=5)


def test_single_candidate_entry_serves_page_and_starts_its_own_worker(tmp_path, monkeypatch):
    from paperflow_v3.control.admission import Admission, begin_control_session

    candidate = Path(__file__).resolve().parents[2]
    workspace = candidate.parents[1]
    project_root = workspace / "prefect-paperflow"
    first_step = workspace / "candidates/first-step-control-20261029"
    sys.path.insert(0, str(project_root))
    from paperflow_prefect.settings import Settings

    site = workspace / "candidates/prefect-folder-frontend-20261101"
    library, data = (tmp_path / name for name in ("library", "data"))
    for path in (library, data):
        path.mkdir()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    monkeypatch.chdir(candidate)
    with prefect_test_harness(server_startup_timeout=60):
        config = {**Settings.load().values, "execution_enabled": True, "library_root": str(library),
                  "data_root": str(data), "prefect_api_url": PREFECT_API_URL.value()}
        config_file = tmp_path / "project.json"
        config_file.write_text(json.dumps(config, ensure_ascii=False), encoding="utf8")
        gate = Admission(uuid4())
        asyncio.run(begin_control_session())
        asyncio.run(gate.start())
        env = dict(os.environ, PAPERFLOW_V3_PROJECT_CONFIG=str(config_file),
                   PAPERFLOW_V3_PROJECT_ROOT=str(project_root), PAPERFLOW_V3_FIRST_STEP_ROOT=str(first_step),
                   PAPERFLOW_V3_SITE_ROOT=str(site), PAPERFLOW_V3_POOL="isolated-launch",
                   PAPERFLOW_V3_PORT=str(port), PYTHONUTF8="1")
        with (tmp_path / "launcher.log").open("w", encoding="utf8") as log:
            process = subprocess.Popen([sys.executable, "-m", "paperflow_v3.control.launch"],
                cwd=str(candidate), env=env, stdout=log, stderr=log,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            owned = []
            try:
                base = f"http://127.0.0.1:{port}"
                deadline = time.monotonic() + 70
                response = None
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        raise AssertionError((tmp_path / "launcher.log").read_text(encoding="utf8", errors="replace"))
                    try:
                        response = httpx.get(base + "/api-config.json", timeout=1)
                        if response.status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    time.sleep(.25)
                assert response is not None and response.status_code == 200
                assert response.json() == {
                    "tasks_api_url": base + "/api/v1/prefect/tasks",
                    "folder_tickets_api_url": base + "/api/v1/prefect/folder-tickets",
                    "control_api_url": base + "/api/v1/prefect",
                    "automatic_gateway_capacity": True,
                }
                assert "folder-ticket-dashboard.js" in httpx.get(base + "/", timeout=3).text
                assert httpx.get(base + "/folder-ticket-dashboard.js", timeout=3).status_code == 200
                tasks = httpx.get(base + "/api/v1/prefect/tasks", timeout=8)
                assert tasks.status_code == 200 and tasks.json()["tasks"] == []
                resources = httpx.get(base + "/api/host-resources", timeout=5,
                                      headers={"Origin": "http://127.0.0.1:8795"})
                assert resources.status_code == 200
                assert resources.headers["access-control-allow-origin"] == "http://127.0.0.1:8795"
                assert not asyncio.run(gate.running())
                with get_client(sync_client=True) as client:
                    pool = client.read_work_pool("isolated-launch")
                    parent = client.read_deployment_by_name("paperflow-v3-parent/parent")
                    child = client.read_deployment_by_name("paperflow-v3-paper/paper")
                    assert pool.type == "process" and pool.concurrency_limit is None
                    assert parent.work_pool_name == child.work_pool_name == pool.name
                deadline = time.monotonic() + 45
                while time.monotonic() < deadline:
                    with get_client(sync_client=True) as client:
                        workers = client.read_workers_for_work_pool(pool.name)
                    if any(row.status == WorkerStatus.ONLINE for row in workers):
                        break
                    if process.poll() is not None:
                        raise AssertionError("candidate launcher exited")
                    time.sleep(.25)
                assert any(row.status == WorkerStatus.ONLINE for row in workers)
                probe = RunnerDeployment.from_flow(launch_probe, name="probe", work_pool_name=pool.name,
                    job_variables={"working_dir": str(candidate)}).apply(work_pool_name=pool.name)
                output = tmp_path / "worker-environment.json"
                with get_client(sync_client=True) as client:
                    run = client.create_flow_run_from_deployment(probe, parameters={"output": str(output)})
                deadline = time.monotonic() + 50
                while time.monotonic() < deadline:
                    with get_client(sync_client=True) as client:
                        state = client.read_flow_run(run.id).state
                    if state.is_final():
                        break
                    time.sleep(.25)
                assert state.is_completed()
                assert json.loads(output.read_text(encoding="utf8")) == {
                    "parent": str(parent.id), "child": str(child.id), "config": str(config_file),
                }
                owned = psutil.Process(process.pid).children(recursive=True)
                assert any("paperflow_v3.control.worker" in " ".join(item.cmdline()) for item in owned)
                process.terminate()
                process.wait(timeout=10)
                _, alive = psutil.wait_procs(owned, timeout=10)
                assert not alive, "candidate worker survived its launcher"
            finally:
                try:
                    children = psutil.Process(process.pid).children(recursive=True)
                except psutil.NoSuchProcess:
                    children = owned
                for child_process in reversed(children):
                    try:
                        child_process.terminate()
                    except psutil.NoSuchProcess:
                        pass
                if process.poll() is None:
                    process.terminate()
                process.wait(timeout=10)
