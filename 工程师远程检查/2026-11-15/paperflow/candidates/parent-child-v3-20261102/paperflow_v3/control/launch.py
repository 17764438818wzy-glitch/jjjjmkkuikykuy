"""Start the local candidate API and its dedicated Prefect process worker."""

import asyncio
import json
import math
import os
from pathlib import Path
import socket
import subprocess
import sys
import time


def configure_environment():
    candidate = Path(__file__).resolve().parents[2]
    workspace = candidate.parents[1]
    project_root = Path(os.environ.get("PAPERFLOW_V3_PROJECT_ROOT", workspace / "prefect-paperflow")).resolve()
    config = Path(os.environ.get("PAPERFLOW_V3_PROJECT_CONFIG", project_root / "config.json")).resolve()
    first_step = Path(os.environ.get("PAPERFLOW_V3_FIRST_STEP_ROOT",
                      workspace / "candidates/first-step-control-20261029")).resolve()
    site = Path(os.environ.get("PAPERFLOW_V3_SITE_ROOT",
                workspace / "candidates/prefect-folder-frontend-20261101")).resolve()
    settings = json.loads(config.read_text(encoding="utf-8-sig"))
    host_resources = workspace / "candidates/first-step-preview-8795/host_resources_api.py"
    if not (site / "index.html").is_file() or not (first_step / "order_v2_flow.py").is_file() or not host_resources.is_file():
        raise RuntimeError("候选页面或第一步脚本不存在")
    os.environ.update(PREFECT_API_URL=settings["prefect_api_url"],
                      PAPERFLOW_V3_PROJECT_CONFIG=str(config), PAPERFLOW_V3_PROJECT_ROOT=str(project_root),
                      PAPERFLOW_V3_FIRST_STEP_ROOT=str(first_step), PAPERFLOW_V3_SITE_ROOT=str(site),
                      PREFECT_SERVER_ALLOW_EPHEMERAL_MODE="false")
    os.environ.setdefault("PREFECT_HOME", str(project_root / "runtime/prefect-home"))
    os.environ.setdefault("PYTHONUTF8", "1")
    os.chdir(candidate)
    return candidate


def ensure_dedicated_pool(name, *, worker_wait_seconds=120, poll_interval=2):
    """Wait for native worker expiry; API calls retain their own timeouts."""
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import WorkPoolCreate
    from prefect.client.schemas.filters import WorkerFilter
    from prefect.client.schemas.objects import WorkerStatus
    from prefect.exceptions import ObjectNotFound

    if not math.isfinite(worker_wait_seconds) or worker_wait_seconds < 0:
        raise ValueError("worker 等待时限必须是非负有限秒数")
    if not math.isfinite(poll_interval) or poll_interval <= 0:
        raise ValueError("worker 状态查询间隔必须是正有限秒数")
    with get_client(sync_client=True) as client:
        try:
            pool = client.read_work_pool(name)
        except ObjectNotFound:
            pool = client.create_work_pool(WorkPoolCreate(name=name, type="process"))
        deadline = time.monotonic() + worker_wait_seconds
        waiting = False
        while True:
            if pool.type != "process" or pool.is_paused or pool.concurrency_limit is not None:
                raise RuntimeError("候选工作池必须是未暂停、无池级并发上限的 process 池")
            queue = client.read_work_queue_by_name("default", work_pool_name=name)
            if queue.is_paused or queue.concurrency_limit is not None:
                raise RuntimeError("候选工作池默认队列必须未暂停且无队列级并发上限")
            online = client.read_workers_for_work_pool(name,
                worker_filter=WorkerFilter(status={"not_any_": [WorkerStatus.OFFLINE]}), limit=1)
            if not online:
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("候选工作池已有在线 worker，等待原生离线状态超时；请核对旧候选执行器")
            if not waiting:
                print(f"候选工作池 {name} 仍有在线 worker，等待 Prefect 原生离线状态（查询等待时限 {worker_wait_seconds:g} 秒）", flush=True)
                waiting = True
            time.sleep(min(poll_interval, remaining))
            pool = client.read_work_pool(name)
        if waiting:
            print(f"候选工作池 {name} 已无在线 worker，继续启动", flush=True)
    return pool


def main():
    candidate = configure_environment()
    pool = os.environ.get("PAPERFLOW_V3_POOL", "paperflow-v3-parent-child")
    port = int(os.environ.get("PAPERFLOW_V3_PORT", "8796"))
    with socket.socket() as sock:
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        sock.bind(("127.0.0.1", port))

    from .flows import load_runtime
    load_runtime()
    from .admission import begin_control_session
    ensure_dedicated_pool(pool)
    asyncio.run(begin_control_session())
    from .deploy import create_deployments
    parent, child = create_deployments(pool)
    os.environ.update(PAPERFLOW_V3_PARENT_DEPLOYMENT_ID=str(parent),
                      PAPERFLOW_V3_CHILD_DEPLOYMENT_ID=str(child))

    worker_env = dict(os.environ, PAPERFLOW_V3_LAUNCHER_PID=str(os.getpid()))
    process = subprocess.Popen([sys.executable, "-m", "paperflow_v3.control.worker", "--pool", pool],
        cwd=str(candidate), env=worker_env, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    try:
        import uvicorn
        from .server import create_configured_app
        print(f"候选页面：http://127.0.0.1:{port}/  工作池：{pool}", flush=True)
        uvicorn.run(create_configured_app(), host="127.0.0.1", port=port)
    finally:
        from .worker import stop_process
        stop_process(process)


if __name__ == "__main__":
    main()
