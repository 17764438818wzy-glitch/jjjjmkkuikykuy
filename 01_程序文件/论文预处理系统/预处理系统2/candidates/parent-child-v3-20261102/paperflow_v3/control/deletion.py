"""Delete one owned task without reaching into source files or external deliveries."""
import asyncio
import json
from pathlib import Path
import shutil
import time
import httpx
from urllib.parse import urlsplit
import psutil
from types import SimpleNamespace
from uuid import UUID

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import ArtifactCreate, ArtifactUpdate
from prefect.client.schemas.filters import ArtifactFilter, FlowRunFilter, LogFilter
from prefect.exceptions import ObjectNotFound
from prefect.states import Cancelled, Cancelling

from .admission import Admission
from ..delivery.owned_work import owned_work_files
from ..delivery.progress import progress_path

PLAN_TYPE = "paperflow-v3-deletion-plan"


def plan_key(task_id):
    return "paperflow-v3-deletion-" + UUID(str(task_id)).hex


async def read_plan(task_id, deployment_id):
    async with get_client() as client:
        rows = await client.read_artifacts(artifact_filter=ArtifactFilter(
            key={"any_": [plan_key(task_id)]}, type={"any_": [PLAN_TYPE]}), limit=1)
    if not rows:
        return None
    row = rows[0]
    if (row.data["task_id"] != str(task_id)
            or row.data["deployment_id"] != str(deployment_id)):
        raise ValueError("deletion plan belongs to another task or deployment")
    return row


def dashboard_query_url(settings):
    filename = settings.get("dashboard_query_config")
    if filename is None:
        return None
    record = json.loads(Path(filename).read_text(encoding="utf-8-sig"))
    value = record.get("read_api_url") if isinstance(record, dict) else None
    if not isinstance(value, str):
        raise ValueError("Dashboard query endpoint is unavailable")
    parsed = urlsplit(value)
    try:
        parsed.port
    except ValueError:
        raise ValueError("Dashboard query endpoint has an invalid port") from None
    if (parsed.scheme not in {"http", "https"} or parsed.hostname not in {"127.0.0.1", "localhost"}
            or parsed.username or parsed.password or parsed.query or parsed.fragment):
        raise ValueError("Dashboard query endpoint must identify the local read service")
    return value.rstrip("/")


async def forget_dashboard_task(task_id, saved_plan):
    url = saved_plan.data.get("dashboard_query_url")
    if url is None:
        return
    async with httpx.AsyncClient(timeout=130) as client:
        response = await client.delete(url + "/cache/tasks/" + str(task_id),
            params={"deletion_plan_id": str(saved_plan.id)})
        response.raise_for_status()
        result = response.json()
    if (not isinstance(result, dict) or result.get("task_id") != str(task_id)
            or result.get("deletion_plan_id") != str(saved_plan.id) or result.get("forgotten") is not True):
        raise RuntimeError("Task display cache invalidation was not confirmed")


def local_cleanup_plan(config, task_id, sources, run_ids):
    data = Path(config["data_root"]).resolve()
    task_id = str(UUID(str(task_id)))
    parent = data / "paperflow-v3"
    root = parent / task_id
    for path in (parent, root):
        if path.is_symlink() or path.is_junction():
            raise ValueError("task work root is a linked directory")
    if root.resolve() != root or not root.is_relative_to(parent):
        raise ValueError("task work root is outside its owner")
    if not sources or not config.get("nas_destination"):
        raise ValueError("source originals and archive destination must be identified")
    protected = [Path(value).resolve() for value in [*sources, config["nas_destination"]]]
    if any(root == path or root.is_relative_to(path) or path.is_relative_to(root)
           for path in protected):
        raise ValueError("task work root overlaps source originals or archive")
    if root.exists():
        owned_work_files([root], sources[0], config["nas_destination"])
    progress = []
    for run_id in run_ids:
        path = progress_path(data, run_id)
        if path.parent.is_symlink() or path.parent.is_junction():
            raise ValueError("delivery state directory is linked")
        for item in [path, *path.parent.glob(path.name + ".*.tmp")]:
            if any(item.resolve() == protected_root or item.resolve().is_relative_to(protected_root)
                   for protected_root in protected):
                raise ValueError("delivery state overlaps protected source originals or archive")
            if item.is_symlink() or item.is_junction() or item.resolve().parent != path.parent:
                raise ValueError("delivery state is outside its owner")
            if item.is_file() and item == path:
                identity = json.loads(item.read_text(encoding="utf-8-sig"))["identity"]
                if identity["task_id"] != task_id or identity["task_run_id"] != str(run_id):
                    raise ValueError("delivery state belongs to another task")
            progress.append(item)
    return root, progress


def remove_local_work(config, task_id, sources, run_ids):
    # Revalidate the resolved absolute paths immediately before recursive removal.
    root, progress = local_cleanup_plan(config, task_id, sources, run_ids)
    if root.exists():
        shutil.rmtree(root)
    for path in progress:
        path.unlink(missing_ok=True)


async def descendants(client, task_id):
    result, pending, seen = [], [UUID(str(task_id))], {str(task_id)}
    while pending:
        parents, pending = pending[:200], pending[200:]
        offset = 0
        while True:
            rows = await client.read_flow_runs(flow_run_filter=FlowRunFilter(
                parent_flow_run_id={"any_": parents}), limit=200, offset=offset)
            for row in rows:
                if str(row.id) in seen:
                    raise ValueError("duplicate native child identity")
                seen.add(str(row.id))
                result.append(row)
                pending.append(row.id)
            offset += len(rows)
            if len(rows) < 200:
                break
    return result


def execution_active(run, *, known_unstarted=False):
    raw = getattr(run, "infrastructure_pid", None)
    if raw is None:
        if known_unstarted and run.state.is_cancelled() and not getattr(run, "run_count", 0):
            return False
        if (getattr(run, "run_count", 0) or getattr(run, "start_time", None)
                or run.state.is_running()):
            raise RuntimeError("started execution has no verifiable process identity")
        return False
    if not str(raw).isdecimal():
        raise RuntimeError("execution process identity cannot be verified")
    try:
        process = psutil.Process(int(raw))
        if not process.is_running():
            return False
        owner = process.environ().get("PREFECT__FLOW_RUN_ID")
        if not owner:
            raise RuntimeError("execution process identity is missing")
        return owner == str(run.id)
    except psutil.NoSuchProcess:
        return False
    except psutil.Error as exc:
        raise RuntimeError("execution process identity is unavailable") from exc


async def stop_runs(client, run_ids, timeout=120, known_unstarted=()):
    deadline = time.monotonic() + timeout
    for run_id in run_ids:
        try:
            run = await client.read_flow_run(run_id)
        except ObjectNotFound:
            continue
        live = execution_active(run, known_unstarted=str(run.id) in known_unstarted)
        if not run.state.is_final() or live:
            active = run.state.is_running() or run.state.is_pending() or live
            await client.set_flow_run_state(run.id,
                Cancelling() if active else Cancelled(), force=True)
    while True:
        active = False
        for run_id in run_ids:
            try:
                run = await client.read_flow_run(run_id)
            except ObjectNotFound:
                continue
            if not run.state.is_final() or execution_active(run, known_unstarted=str(run.id) in known_unstarted):
                active = True
        if not active:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("task execution has not stopped; local files were retained")
        await asyncio.sleep(.2)


async def remove_artifacts(client, run_ids, keep):
    for start in range(0, len(run_ids), 200):
        while True:
            rows = await client.read_artifacts(artifact_filter=ArtifactFilter(
                flow_run_id={"any_": run_ids[start:start + 200]}), limit=200)
            selected = [row for row in rows if row.id != keep]
            if not selected:
                break
            for row in selected:
                await client.delete_artifact(row.id)


async def task_variable_names(client, task_id):
    names, offset = [], 0
    while True:
        response = await client.request("POST", "/variables/filter",
            json={"variables": {"name": {"like_": "paperflow_v3_operation_"}},
                  "sort": "NAME_ASC", "limit": 200, "offset": offset})
        rows = response.json()
        for row in rows:
            value = row.get("value") or {}
            target = value.get("target") if isinstance(value, dict) else None
            if (row["name"].startswith("paperflow_v3_operation_")
                    and isinstance(target, dict) and target.get("task_id") == str(task_id)):
                names.append(row["name"])
        offset += len(rows)
        if len(rows) < 200:
            break
    suffix = str(task_id).replace("-", "_")
    for name in ["paperflow_v3_recovery_" + suffix, "paperflow_v3_intent_" + suffix,
                 "paperflow_v3_task_" + suffix, "paperflow_v3_classification_summary_" + suffix,
                 "paperflow_v3_model_failures_" + suffix]:
        if await client.read_variable_by_name(name) is not None:
            names.append(name)
    return names


async def remove_variables(client, task_id, extra_names=()):
    names = set(await task_variable_names(client, task_id))
    names.update(extra_names)
    for name in names:
        try:
            await client.delete_variable_by_name(name)
        except ObjectNotFound:
            pass


async def remove_control_limit(client, task_id):
    """Remove only this deleted task's admission gate, preserving shared capacity."""
    try:
        await client.delete_global_concurrency_limit_by_name(
            'paperflow-v3-control-' + str(task_id).replace('-', ''))
    except ObjectNotFound:
        pass


async def delete_task_data(task_id, settings, operations, query_service, run=None):
    task_id = UUID(str(task_id))
    config = json.loads(Path(settings["project_config"]).read_text(encoding="utf-8-sig"))
    saved = await read_plan(task_id, settings["deployment_id"])
    async with get_client() as client:
        if saved is None:
            if run is None:
                raise ValueError("task and deletion plan are both unavailable")
            children = await descendants(client, task_id)
            plan = {"task_id": str(task_id), "deployment_id": str(settings["deployment_id"]),
                    "source_paths": run.parameters["source_paths"],
                    "nas_destination": config["nas_destination"],
                    "dashboard_query_url": dashboard_query_url(settings),
                    "run_ids": [str(child.id) for child in children],
                    "unstarted": [str(row.id) for row in [run, *children]
                        if row.infrastructure_pid is None and not row.run_count and row.start_time is None
                        and (row.state.is_paused() or row.state.is_scheduled())]}
            create_key = getattr(run, "idempotency_key", None) or ""
            plan["create_operations"] = []
            if create_key.startswith("paperflow-v3-task-"):
                request_id = UUID(create_key[len("paperflow-v3-task-"):])
                name = "paperflow_v3_operation_" + str(request_id).replace("-", "_")
                value = await client.read_variable_by_name(name)
                if value is not None:
                    target = value.value.get("target")
                    if value.value.get("action") != "create" or target not in (None, {"task_id": str(task_id)}):
                        raise ValueError("creation operation belongs to another task")
                    plan["create_operations"].append(name)
            local_cleanup_plan(config, task_id, plan["source_paths"], plan["run_ids"])
            saved_id = await client.create_artifact(ArtifactCreate(key=plan_key(task_id),
                type=PLAN_TYPE, flow_run_id=task_id, data=plan))
            saved = SimpleNamespace(id=saved_id.id, data=plan)
        plan = saved.data
        updated = False
        if not plan.get("dashboard_query_url") and settings.get("dashboard_query_config") is not None:
            plan["dashboard_query_url"] = dashboard_query_url(settings)
            updated = True
        if not plan.get("nas_destination"):
            plan["nas_destination"] = config["nas_destination"]
            updated = True
        if updated:
            await client.update_artifact(saved.id, ArtifactUpdate(data=plan))
        run_ids = [task_id, *[UUID(value) for value in plan["run_ids"]]]
        local_cleanup_plan(config, task_id, plan["source_paths"], plan["run_ids"])
        await Admission(task_id).delete()
        await query_service.forget_task(task_id)
        await forget_dashboard_task(task_id, saved)
        # Let older start/retry handlers observe the higher delete intent and finish.
        deadline = time.monotonic() + 120
        while operations.active.get(str(task_id)):
            if time.monotonic() >= deadline:
                raise TimeoutError("earlier task controls have not settled")
            await asyncio.sleep(.1)
        await stop_runs(client, run_ids, known_unstarted=plan.get('unstarted', ()))
        # An already admitted parent may have created one more native child while stopping.
        if run is not None:
            current = await descendants(client, task_id)
            extra = [child for child in current if str(child.id) not in plan["run_ids"]]
            if extra:
                plan["run_ids"].extend(str(child.id) for child in extra)
                plan.setdefault("unstarted", []).extend(str(child.id) for child in extra
                    if child.infrastructure_pid is None and not child.run_count and child.start_time is None
                    and (child.state.is_paused() or child.state.is_scheduled()))
                await client.update_artifact(saved.id,
                    ArtifactUpdate(data=plan))
                run_ids = [task_id, *[UUID(value) for value in plan["run_ids"]]]
                await stop_runs(client, run_ids, known_unstarted=plan.get('unstarted', ()))
        await asyncio.to_thread(remove_local_work, config, task_id, plan["source_paths"], plan["run_ids"])
        await remove_artifacts(client, run_ids, saved.id)
        for run_id in [*reversed(run_ids[1:]), task_id]:
            try:
                await client.delete_flow_run(run_id)
            except ObjectNotFound:
                pass
        # Prefect deletes logs asynchronously after deleting a run.
        deadline = time.monotonic() + 120
        while True:
            has_logs = False
            for start in range(0, len(run_ids), 200):
                if await client.read_logs(log_filter=LogFilter(
                        flow_run_id={"any_": run_ids[start:start + 200]}), limit=1):
                    has_logs = True
                    break
            if not has_logs:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("Prefect is still clearing task logs")
            await asyncio.sleep(.2)
        await remove_artifacts(client, run_ids, saved.id)
        await remove_variables(client, task_id, plan.get("create_operations", ()))
        await remove_control_limit(client, task_id)
        await forget_dashboard_task(task_id, saved)
        await client.delete_artifact(saved.id)
    return {"task_id": str(task_id), "deleted": True}
