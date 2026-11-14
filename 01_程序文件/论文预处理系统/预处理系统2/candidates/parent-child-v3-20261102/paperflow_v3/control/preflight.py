"""Check dependencies and start the managed Nimble service if it is stopped."""

import asyncio
import importlib.util
import json
import os
from pathlib import Path
import subprocess
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from prefect.client.orchestration import get_client
from prefect.client.schemas.objects import WorkerStatus
from prefect.settings import PREFECT_API_URL


NIMBLE_LOCAL_PORT = 11434
NIMBLE_START_SCRIPT = Path(r"F:\论文处理端迁移包\02_运行环境\nimble\Start-Nimble.ps1")
NIMBLE_START_LOG_DIR = Path(__file__).resolve().parents[2] / "runtime" / "nimble-start-logs"


class NimbleStartError(RuntimeError):
    pass


def _start_nimble():
    """Reuse the installed local service launcher; never send inference."""
    try:
        NIMBLE_START_LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_path = NIMBLE_START_LOG_DIR / ("nimble-preflight-" + uuid4().hex + ".log")
        with log_path.open("x+", encoding="utf-8", errors="replace") as output:
            try:
                subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
                                "-File", str(NIMBLE_START_SCRIPT), "-Quiet"], check=True, timeout=25,
                               stdin=subprocess.DEVNULL, stdout=output, stderr=output,
                               creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            except subprocess.TimeoutExpired as exc:
                raise NimbleStartError(f"等待启动超过 25 秒，服务状态待核对；日志：{log_path}") from exc
            except subprocess.CalledProcessError as exc:
                output.seek(0)
                detail = output.readline().strip()[:300] or f"启动脚本退出代码 {exc.returncode}"
                raise NimbleStartError(f"{detail}；日志：{log_path}") from exc
    except OSError as exc:
        raise NimbleStartError("无法创建启动日志或运行 Nimble 启动脚本：" + str(exc)) from exc


def _deployment_matches(deployment, flow_name, expected):
    entrypoint = str(deployment.entrypoint or "")
    if ":" not in entrypoint:
        return False
    file_name, function = entrypoint.rsplit(":", 1)
    expected_function = "parent_order" if expected == "parent" else "paper_order"
    work_dir = (deployment.job_variables or {}).get("working_dir")
    if not work_dir or Path(work_dir).resolve() != Path(__file__).resolve().parents[2]:
        return False
    entry_path = Path(file_name)
    if not entry_path.is_absolute():
        entry_path = Path(work_dir) / (deployment.path or ".") / entry_path
    return (flow_name == "paperflow-v3-" + expected and function == expected_function
            and entry_path.resolve() == Path(__file__).with_name("flows.py").resolve())


async def inspect_dependencies(settings):
    checks = []

    def add(name, ready, detail):
        checks.append({"name": name, "ready": ready, "detail": detail})

    config_path = settings.get("project_config")
    try:
        config = json.loads(Path(config_path).read_text(encoding="utf-8-sig"))
        add("project_config", True, "项目配置可读取")
    except (OSError, TypeError, ValueError) as exc:
        config = None
        add("project_config", False, "项目配置不可读取：" + type(exc).__name__)
    enabled = bool(config and config.get("execution_enabled") is True)
    add("execution_enabled", enabled, "项目允许业务执行" if enabled else "项目业务执行尚未启用")

    async with get_client() as client:
        try:
            deployment = await asyncio.wait_for(client.read_deployment(settings["deployment_id"]), timeout=10)
            add("prefect_deployment", True, "父工单部署可读取")
        except Exception as exc:
            deployment = None
            add("prefect_deployment", False, "Prefect 部署不可用：" + type(exc).__name__)
        try:
            child_deployment = await asyncio.wait_for(client.read_deployment(settings["child_deployment_id"]), timeout=10)
            add("paper_deployment", True, "论文子工单部署可读取")
        except Exception as exc:
            child_deployment = None
            add("paper_deployment", False, "论文子工单部署不可用：" + type(exc).__name__)
        try:
            await asyncio.wait_for(client.read_artifacts(limit=1), timeout=10)
            add("prefect_storage", True, "Prefect 回执接口可读取，未写入测试数据")
        except Exception as exc:
            add("prefect_storage", False, "Prefect 回执接口不可读取：" + type(exc).__name__)
        try:
            pools = {row.work_pool_name for row in (deployment, child_deployment) if row and row.work_pool_name}
            if not deployment or not child_deployment or any(not row.work_pool_name for row in (deployment, child_deployment)):
                raise ValueError("deployment has no worker pool")
            workers = await asyncio.wait_for(asyncio.gather(
                *(client.read_workers_for_work_pool(pool) for pool in pools)), timeout=10)
            online = all(any(worker.status == WorkerStatus.ONLINE for worker in rows) for rows in workers)
            add("executor", online, "父子工单的执行进程在线" if online else "未核实到父子部署的在线执行进程")
        except Exception as exc:
            add("executor", False, "执行进程不可核实：" + type(exc).__name__)
        try:
            required = {"local", "package", "body_llm", "attachment_llm", "total_llm", "rag", "nas", "cleanup"}
            if not config or not required.issubset(config.get("limits") or {}):
                raise ValueError("missing configured limits")
            limits = await asyncio.wait_for(asyncio.gather(*(client.read_global_concurrency_limit_by_name(
                "paperflow-connected-" + key) for key in config["limits"])), timeout=10)
            for (key, expected), limit in zip(config["limits"].items(), limits):
                dynamic = key in {"body_llm", "attachment_llm", "total_llm"}
                if (not limit.active or (limit.limit <= 0 if dynamic else limit.limit != expected)
                        or limit.slot_decay_per_second):
                    raise ValueError("configured Prefect limit is unavailable")
            add("global_limits", True, "已有 Prefect 全局名额可用，模型并发在开始时核定")
        except Exception as exc:
            add("global_limits", False, "共享名额未确认：" + type(exc).__name__)
        try:
            if not deployment or not child_deployment:
                raise ValueError("missing deployment")
            parent_flow, paper_flow = await asyncio.wait_for(asyncio.gather(
                client.read_flow(deployment.flow_id), client.read_flow(child_deployment.flow_id)), timeout=10)
            deployment_matches = (_deployment_matches(deployment, parent_flow.name, "parent") and
                                  _deployment_matches(child_deployment, paper_flow.name, "paper"))
        except Exception:
            deployment_matches = False

    project = None
    try:
        if not config:
            raise ValueError("project config unavailable")
        from paperflow_prefect import legacy
        from paperflow_prefect.settings import Settings
        project = Settings(config)
        observed = await asyncio.wait_for(asyncio.to_thread(legacy.preflight, project), timeout=8)
        if not (observed["template_exists"] and observed["catalog_exists"]
                and all(observed["counter_paths"].values()) and all(observed["modules"].values())):
            raise ValueError("local script dependency unavailable")
        add("local_scripts", True, "现有处理模块、模板、目录及计数器文件可读取")
    except Exception as exc:
        add("local_scripts", False, "处理脚本或本机依赖未确认：" + type(exc).__name__)

    probe = None
    repaired = False
    try:
        if not config:
            raise ValueError("project config unavailable")
        source = Path(settings["first_step_root"]) / "paperflow_prefect" / "nimble_dependency.py"
        spec = importlib.util.spec_from_file_location("paperflow_v3_nimble_probe", source)
        loaded_probe = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(loaded_probe)
        probe = loaded_probe
        endpoint = urlsplit(config["nimble_endpoint"])
        try:
            await asyncio.wait_for(asyncio.to_thread(probe.probe_service, endpoint.hostname, endpoint.port, probe.MODEL), timeout=7)
        except probe.NimbleUnavailable as exc:
            managed = (endpoint.scheme == "http" and endpoint.hostname in {"127.0.0.1", "localhost"}
                       and endpoint.port == NIMBLE_LOCAL_PORT and endpoint.path == "/v1/systemone"
                       and not (endpoint.username or endpoint.password or endpoint.query or endpoint.fragment))
            if not managed or not isinstance(exc.__cause__, ConnectionRefusedError):
                raise
            await asyncio.to_thread(_start_nimble)
            await asyncio.wait_for(asyncio.to_thread(probe.probe_service, endpoint.hostname, endpoint.port, probe.MODEL), timeout=7)
            repaired = True
        detail = ("第一步服务已自动启动并复检通过，所需模型已注册，未发送推理" if repaired
                  else "第一步服务及所需模型已注册，未发送推理")
        add("nimble", True, detail)
    except Exception as exc:
        if isinstance(exc, NimbleStartError):
            detail = "第一步服务自动启动失败：" + str(exc)
        elif probe is not None and isinstance(exc, probe.NimbleUnavailable):
            if isinstance(exc.__cause__, ConnectionRefusedError):
                detail = "第一步服务连接被拒绝，请检查服务是否启动"
            elif isinstance(exc.__cause__, TimeoutError):
                detail = "第一步服务连接超时，请检查服务是否正常响应"
            else:
                detail = str(exc)
        elif isinstance(exc, TimeoutError):
            detail = "第一步服务检查超时，请检查服务是否正常响应"
        else:
            detail = "第一步服务配置或检查脚本不可读取"
        add("nimble", False, detail)

    try:
        if project is None:
            raise ValueError("local bindings unavailable")
        from auto_config import load_key
        model = json.loads(project.path("model_config").read_text(encoding="utf-8-sig"))
        key = load_key(project.path("settings_root"))
        if not key:
            raise ValueError("model key unavailable")
        base = model["base_url"].rstrip("/").removesuffix("/chat/completions")
        async with httpx.AsyncClient(timeout=5, follow_redirects=False, trust_env=False) as client:
            response = await client.get(base + "/models", headers={"Authorization": "Bearer " + key})
            response.raise_for_status()
            models = response.json()["data"]
        if not any(row.get("id") == model["text_model"] for row in models):
            raise ValueError("configured model unavailable")
        add("model_gateway", True, "模型路由已认证且配置模型在列表中；未发送推理")
    except Exception as exc:
        add("model_gateway", False, "模型路由或凭据未确认：" + type(exc).__name__)

    try:
        if project is None:
            raise ValueError("local bindings unavailable")
        if config.get('rag_backend','ragflow')=='qdrant':
            from paperflow_v3.delivery.backend import probe_qdrant
            await probe_qdrant(project)
            add('rag',True,'Qdrant 已认证且存储可读取；未写入')
        else:
            from auto_config import load_key
            from ragflow_client import RagflowClient
            rag = RagflowClient(config["rag_url"], load_key(project.path("settings_root"), "ragflow"))
            rows = await asyncio.wait_for(asyncio.to_thread(rag.request, "GET", "/datasets?page=1&page_size=1"), timeout=6)
            if not isinstance(rows, list):
                raise ValueError("RAG dataset list unavailable")
            add("rag", True, "RAG 已认证且专业库列表可读取；未写入")
    except Exception as exc:
        add("rag", False, "RAG 连接或凭据未确认：" + type(exc).__name__)

    try:
        destination = Path(config["nas_destination"])
        ready = await asyncio.wait_for(asyncio.to_thread(
            lambda: destination.is_dir() and os.access(destination, os.W_OK)), timeout=15)
        add("nas", ready, "NAS 目录可访问且报告可写，未写入测试文件" if ready else "NAS 目录不可访问或不可写")
    except TimeoutError:
        add("nas", False, "NAS目录检查超过15秒，状态未确认")
    except (OSError, TypeError, KeyError) as exc:
        add("nas", False, "NAS 目标未配置或不可访问：" + type(exc).__name__)
    try:
        if not project or not deployment_matches:
            raise ValueError("parent and child deployment entries not verified")
        if not PREFECT_API_URL.value() or PREFECT_API_URL.value().rstrip("/") != config["prefect_api_url"].rstrip("/"):
            raise ValueError("Prefect server does not match project")
        from paperflow_v3.control.classification import first_step_modules
        from paperflow_v3.control.flows import parent_order, paper_order
        from paperflow_v3.stages.pipeline import process_paper
        from paperflow_v3.delivery.runtime import deliver_paper
        first = first_step_modules(settings["first_step_root"])
        if not all(callable(item) for item in (first["classification"].process_one_group,
                first["intake"].receive_handoff, parent_order, paper_order, process_paper, deliver_paper)):
            raise ValueError("pipeline entry unavailable")
        add("pipeline", True, "第一步、父子工单、单篇处理及收尾入口可读取")
    except Exception as exc:
        add("pipeline", False, "处理流程或父子部署未确认：" + type(exc).__name__)
    return {"ready": all(row["ready"] for row in checks), "checks": checks}
