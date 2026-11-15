"""Candidate task API; no service is started by importing this module."""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from hashlib import sha256
import json
import asyncio
import httpx
import logging
from pathlib import Path
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter
from prefect.context import AsyncClientContext
from prefect.exceptions import ObjectNotFound
from prefect.flow_runs import aresume_flow_run
from prefect.states import Paused, Scheduled

from . import preflight
from . import recovery
from .admission import Admission, begin_control_session
from .stage_admission import pause_execution_progress as pause_progress
from .auto_capacity import (CapacityError, SeatCapacityError, tested_llm_limit,
                            capacity_fields, capacity_description)
from .folder_status import folder_status
from .operations import Operations
from .query_service import QueryService
from .parent import _children
from .receipts import (read_folder_receipts, read_folder_receipts_by_key, read_latest_folder_snapshots,
                       read_receipts, save_receipt)
from .retry import read_rag_retry_settings, retry_decision
from .task_state import task_state


class CreateTask(BaseModel):
    request_id: UUID
    task_name: str = Field(min_length=1)
    source_paths: list[str] = Field(min_length=1, max_length=30000)


class ControlAction(BaseModel):
    request_id: UUID
    intent_seq: int = Field(ge=1)


class RetryAction(BaseModel):
    request_id: UUID


class MaintenanceResume(BaseModel):
    request_id: UUID
    expected_request_id: UUID
    intent_seq: int = Field(ge=1)
    session: str = Field(min_length=1)


def _folder_snapshot_complete(records, children, task_id):
    actual = [child.parameters.get("group_id") for child in children]
    if (not all(child.parameters.get("task_id") == str(task_id) and child.parameters.get("group_id")
                for child in children) or len(actual) != len(set(actual))):
        return False
    dispatches = {}
    for row in records:
        if row["kind"] == "child_dispatch":
            dispatches.setdefault(row["folder_id"], row["data"])
    return not any(row.get("state") in ("intent", "unknown") and group not in actual
                   for group, row in dispatches.items())


def _folder_identity(child):
    parameters = child.parameters
    registered = parameters.get("registered") or {}
    fields = ("task_id", "group_id", "paper_id", "receipt_id")
    if (not all(registered.get(key) for key in fields)
            or any(registered[key] != parameters.get(key) for key in ("task_id", "group_id"))):
        return None
    child_id = str(child.id)
    return {**{key: registered[key] for key in fields}, "attempt_id": child_id,
            "parent_run_id": parameters["task_id"], "task_run_id": child_id}


def _last_retry(records, child, stage, decision=None):
    retry = next((row["data"] for row in records if row["kind"] == "manual_retry"), None)
    if not retry:
        return None
    if child.state.is_scheduled() or child.state.is_pending():
        status = "scheduled"
    elif child.state.is_running():
        status = "running"
    elif retry.get("state") == "authorized":
        status = "dispatch_unknown"
    elif child.state.is_completed() and stage == "completed":
        status = "completed"
    elif (child.state.is_failed() or child.state.is_crashed()) and (decision if decision is not None else retry_decision(
            records, identity=_folder_identity(child), registered=child.parameters.get("registered")))["allowed"]:
        status = "failed"
    else:
        status = "unresolved"
    return {"retry_id": retry["retry_id"], "stage": retry.get("stage"), "status": status}


def _task_actions(run, running, children):
    ended = run.state.is_final() and not run.state.is_crashed()
    start_allowed = not ended and (not running or run.state.is_crashed()
                                   or any(child.state.is_crashed() for child in children))
    pause_allowed = not ended and running
    return {
        "delete": {"allowed": True, "reason": None},
        "start": {"allowed": start_allowed, "reason": None if start_allowed else
                  "任务已结束" if ended else "任务已开始"},
        "pause": {"allowed": pause_allowed, "reason": None if pause_allowed else
                  "任务已结束" if ended else "任务尚未开始或已暂停"},
    }


async def _folder_ticket_records(children):
    """Keep request events and only the latest of each progress snapshot."""
    candidates = [child for child in children if (child.state.is_running() or child.state.is_completed())
                  and _folder_identity(child)]
    from paperflow_v3.delivery.progress import local_records, merge_records
    from .receipts import receipt_data
    if not candidates:
        records = await read_folder_receipts([child.id for child in children])
        return {key: merge_records(local_records(key), rows) for key, rows in records.items()}
    ids = [child.id for child in candidates]
    snapshots = await read_latest_folder_snapshots(ids)
    patterns = ("intent", "start", "outcome", "response", "failed", "complete", "running", "error",
                "notsent", "returned", "deliverystage")
    # A retry authorization uses a UUID4 as its key, without a suffix.
    patterns += tuple("paperflow-v3-" + "_" * 12 + "4" + "_" * 3 + variant for variant in "89ab")
    selected = await read_folder_receipts_by_key(ids, patterns=patterns)
    snapshot_kinds = {"rag-body", "rag-attachments", "nas", "cleanup"}
    records = {}
    for child in candidates:
        child_id = str(child.id)
        items = [item for item in selected[child_id] if item.data["kind"] not in snapshot_kinds]
        items.extend(snapshots[child_id])
        by_id = {item.id: item for item in items}
        rows = [receipt_data(item) for item in sorted(by_id.values(), key=lambda item: item.created, reverse=True)]
        rows = merge_records(local_records(child_id), rows)
        identity = _folder_identity(child)
        if any(row["task_id"] != identity["task_id"] or row["folder_id"] != identity["group_id"] for row in rows):
            continue
        projected = folder_status(rows, is_running=child.state.is_running(),
                                  is_active=not child.state.is_final(), identity=identity)
        if (projected["stage"] in {"failed", "unresolved"}
                or any(row["kind"] == "child_error" for row in rows)):
            continue
        records[child_id] = rows
    remaining = [child.id for child in children if str(child.id) not in records]
    if remaining:
        records.update({key: merge_records(local_records(key), rows)
                        for key, rows in (await read_folder_receipts(remaining)).items()})
    return records


def create_app(settings, *, test_start_accounts=None, apply_start_capacity=None):
    deployment_id = UUID(str(settings["deployment_id"]))
    rag_retry_settings = read_rag_retry_settings(settings.get("project_config"))
    operations = Operations()
    query_service = QueryService(settings)

    @asynccontextmanager
    async def lifespan(app):
        await begin_control_session()
        try:
            yield
        finally:
            await query_service.close()

    app = FastAPI(lifespan=lifespan)
    app.state.query_service = query_service
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.get("cors_origins", ["http://127.0.0.1:8795", "http://localhost:8795"]),
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type"],
    )

    async def invalidate_query(task_id, *, folder_id=None):
        service = getattr(app.state, "query_service", None)
        if service is not None:
            try:
                await service.invalidate(str(task_id), folder_id=folder_id)
            except Exception:
                logging.getLogger(__name__).exception("Task query invalidation failed")

    @app.middleware("http")
    async def track_task_controls(request, call_next):
        parts = request.url.path.strip("/").split("/")
        tracked = (request.method == "POST" and len(parts) >= 6
                   and parts[:4] == ["api", "v1", "prefect", "tasks"]
                   and parts[-1] in {"start", "pause", "retry"})
        key = parts[4] if tracked else None
        task = asyncio.current_task()
        if tracked:
            operations.active.setdefault(key, set()).add(task)
        try:
            return await call_next(request)
        finally:
            if tracked:
                operations.active.get(key, set()).discard(task)

    @app.middleware("http")
    async def prefect_request_context(request, call_next):
        path = request.url.path
        before_checks = request.method == "POST" and path in (
            "/api/v1/prefect/preflight", "/api/v1/prefect/tasks")
        query_read = request.method == "GET" and (
            path in ("/api/v1/prefect/tasks", "/api/v1/prefect/folder-tickets")
            or path.startswith("/api/v1/prefect/tasks/") and path.endswith("/classifications"))
        if request.method in ("GET", "POST") and path.startswith("/api/v1/prefect/") and not before_checks and not query_read:
            async with AsyncClientContext.get_or_create():
                return await call_next(request)
        return await call_next(request)

    async def read_task(task_id):
        async with get_client() as client:
            try:
                run = await client.read_flow_run(task_id)
            except ObjectNotFound:
                raise HTTPException(404, "任务不存在") from None
        if run.deployment_id != deployment_id:
            raise HTTPException(404, "任务不存在")
        return run

    async def describe(run, *, include_snapshot=False):
        running = await Admission(run.id).running()
        records = await read_receipts(run.id)
        dependency = next((row["data"] for row in records
                           if row["kind"] == "dependency_failure"), None)
        classification = next((row["data"] for row in records if row["kind"] == "classification_complete"), None)
        groups = {}
        dispatches = {}
        for row in records:
            if row["kind"] == "classification":
                groups.setdefault(row["folder_id"], row["data"])
            elif row["kind"] == "child_dispatch":
                dispatches.setdefault(row["folder_id"], row["data"])
        children = await _children(run.id)
        child_records = await _folder_ticket_records(children)
        folders = [{"folder_id": child.parameters.get("group_id"),
                    **folder_status(child_records[str(child.id)], is_running=child.state.is_running(),
                                    is_active=not child.state.is_final(), identity=_folder_identity(child))}
                   for child in children]
        progress = (await pause_progress(run.id, snapshot={"folders": folders,
            "items": [{**value, "group_id": key} for key, value in groups.items()]})
            if not running else None)
        done = bool(classification and not classification["ungrouped"] and
                    set(classification["group_ids"]) == set(groups))
        business_state = task_state(done, list(groups.values()), folders,
                                    "running" if running else "paused",
                                    _folder_snapshot_complete(records, children, run.id))
        if classification and classification["ungrouped"]:
            business_state = "unresolved"
        if any(row.get("state") in ("intent", "unknown") and group not in
               {child.parameters.get("group_id") for child in children} for group, row in dispatches.items()):
            business_state = "unresolved"
        actions = _task_actions(run, running, children)
        if run.state.is_completed():
            state = "completed" if business_state == "completed" else "unresolved"
        elif run.state.is_failed() or run.state.is_crashed():
            state = "failed"
        elif run.state.is_paused():
            state = "unresolved" if running else "created" if run.run_count == 0 else progress["state"]
        elif not running:
            state = progress["state"]
        elif run.state.is_scheduled() or run.state.is_pending():
            state = "waiting"
        else:
            state = business_state
        description = {
            "task_id": str(run.id),
            "task_name": run.parameters["task_name"],
            "parent_flow_run_id": str(run.id),
            "desired_state": "running" if running else "paused",
            "task_state": state,
            "pause_progress": progress,
            "stop_reason": dependency.get("reason") if dependency and dependency.get("status") == "active" else None,
            "actions": actions,
        }
        if include_snapshot:
            return description, records, children, child_records
        return description

    async def check_dependencies():
        result = await preflight.inspect_dependencies(settings)
        return {**result, "checked_at": datetime.now(timezone.utc).isoformat()}

    @app.post("/api/v1/prefect/preflight")
    async def check_creation_dependencies():
        result = await check_dependencies()
        account_check = {"name": "upstream_accounts", "ready": False,
                         "detail": "依赖自检未通过，尚未测试上游账号"}
        result = {**result, "checks": [*result["checks"], account_check]}
        if not result["ready"]:
            return result

        async def before_batch():
            pass  # No task has been created, so there is no task admission to change.

        try:
            if test_start_accounts is None:
                raise CapacityError("上游账号连通测试尚未接入")
            tested = await test_start_accounts(ensure_current=before_batch)
            count = tested.get("successful_count")
            if type(count) is not int or count < 0:
                raise CapacityError("上游账号测试未返回有效的联通数量")
            limit = tested_llm_limit(tested)
            result.update(successful_accounts=count, llm_limit=limit)
            if tested.get('capacity_policy') == 'domestic-credit-pool-v1':
                result.update(capacity_fields(tested))
                result['checks'].insert(-1, {'name': 'gateway_seats', 'ready': True,
                    'detail': f"配置50、启用{tested['seat_capacity']}；本次未单独测试出口连通性"})
                account_check.update(ready=count > 0,
                    detail=capacity_description(tested) + '；未确认账号暂不计入' + ('；不能创建任务' if not count else ''))
            elif count:
                account_check.update(ready=True,
                    detail=f"本次实际联通 {count} 个国内账号，预计 LLM 并发 {limit}；未联通账号不计入")
            else:
                account_check["detail"] = "本次实际联通 0 个国内账号，预计 LLM 并发 0，不能创建任务"
        except Exception as exc:
            detail = str(exc) if isinstance(exc, CapacityError) else type(exc).__name__
            account_check["detail"] = "上游账号测试结果未确认：" + detail
            if isinstance(exc, SeatCapacityError):
                result['checks'].insert(-1, {'name': 'gateway_seats', 'ready': False, 'detail': detail})
        return {**result, "ready": account_check["ready"],
                "checked_at": datetime.now(timezone.utc).isoformat()}

    async def created_run(request_id):
        async with get_client() as client:
            runs = await client.read_flow_runs(
                flow_run_filter=FlowRunFilter(
                    deployment_id={"any_": [deployment_id]},
                    idempotency_key={"any_": ["paperflow-v3-task-" + str(request_id)]},
                ), limit=2,
            )
        if len(runs) > 1:
            raise HTTPException(409, "同一创建身份对应多个工单，需核对")
        return runs[0] if runs else None

    @app.post("/api/v1/prefect/tasks", status_code=201)
    async def create_task(request: CreateTask):
        if not request.task_name.strip() or any(not Path(path).is_absolute() for path in request.source_paths):
            raise HTTPException(422, "任务名称或本机资料路径无效")
        parameters = {"task_name": request.task_name, "source_paths": request.source_paths}
        payload_hash = sha256(json.dumps(parameters, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()

        async def existing_result(operation):
            if operation["action"] != "create" or operation["payload_hash"] != payload_hash:
                raise HTTPException(409, "创建请求身份已用于其他资料，需核对")
            result = await operations.view(operation)
            if operation["target"]:
                result.update(task_id=operation["target"]["task_id"], task_name=request.task_name)
            return result

        async with AsyncClientContext.get_or_create():
            async with operations.lock:
                operation = await operations.read(request.request_id)
                if operation:
                    return await existing_result(operation)
            paths_ready = all(Path(path).is_dir() for path in request.source_paths)
            checks = await check_creation_dependencies() if paths_ready else None
            async with operations.lock:
                operation = await operations.read(request.request_id)
                if operation:
                    return await existing_result(operation)
                if not paths_ready:
                    raise HTTPException(422, "任务名称或本机资料路径无效")
                if not checks["ready"]:
                    raise HTTPException(409, detail=checks)
                operation = {"request_id": str(request.request_id), "action": "create", "target": None,
                             "payload_hash": payload_hash, "status": "accepted"}
                await operations.save(operation)
            async with get_client() as client:
                run = await client.create_flow_run_from_deployment(
                    deployment_id,
                    parameters=parameters,
                    state=Paused(reschedule=True),
                    name=request.task_name,
                    tags=["paperflow-v3-task"],
                    idempotency_key="paperflow-v3-task-" + str(request.request_id),
                )
            if run.parameters != parameters:
                operation["status"] = "rejected"
                async with operations.lock:
                    await operations.save(operation)
                raise HTTPException(409, "创建请求身份已用于其他资料，需核对")
            operation.update(target={"task_id": str(run.id)}, status="confirmed")
            async with operations.lock:
                await operations.save(operation)
            await invalidate_query(run.id)
            return {**await describe(run), "request_id": str(request.request_id),
                    "action": "create", "accepted": True, "status": "confirmed"}

    @app.get("/api/v1/prefect/tasks")
    async def list_tasks():
        result = await query_service.list_tasks()
        return JSONResponse(result, status_code=503 if result["query_status"] == "unavailable" else 200)

    @app.get("/api/v1/prefect/tasks/by-request/{request_id}")
    async def find_task_by_request(request_id: UUID):
        run = await created_run(request_id)
        if not run:
            return {"request_id": str(request_id), "found": False, "resolved": False}
        return {"request_id": str(request_id), "found": True, "resolved": True,
                "task": {"task_id": str(run.id), "task_name": run.parameters["task_name"],
                         "parent_flow_run_id": str(run.id)}}

    @app.post("/api/v1/prefect/tasks/{task_id}/maintenance-resume", include_in_schema=False)
    async def maintenance_resume(task_id: UUID, request: MaintenanceResume):
        """Continue a drained reload, serialized with normal user controls.

        The operator has already handed off only the frozen idle original
        tickets. This opens admission only; it never retries jobs or runs the
        account/router setup performed by an ordinary user Start.
        """
        from prefect.variables import Variable
        from .admission import control_gate, _SESSION
        await read_task(task_id)
        async with operations.lock:
            previous = await operations.read(request.request_id)
            if previous:
                if previous.get('action') != 'maintenance_resume' or previous.get('target') != {'task_id': str(task_id)}:
                    raise HTTPException(409, '维护操作身份不匹配')
                return await operations.view(previous)
            current = await operations.latest(task_id)
            if (not current or current.get('request_id') != str(request.expected_request_id)
                    or current.get('intent_seq') != request.intent_seq or current.get('action') != 'start'):
                raise HTTPException(409, '用户操作已经改变，保留当前状态')
            original = await operations.read(request.expected_request_id)
            if not original or original.get('status') != 'confirmed':
                raise HTTPException(409, '原开始请求尚未确认，未续跑')
            async with control_gate(task_id):
                admission = Admission(task_id)
                state = await Variable.aget(admission.name)
                expected = {'state': 'paused', 'session': None, 'maintenance_token': str(request.request_id)}
                session = await Variable.aget(_SESSION)
                if state != expected or session != request.session:
                    raise HTTPException(409, '维护暂停身份或执行会话已改变，未续跑')
                operation = {'request_id': str(request.request_id), 'action': 'maintenance_resume',
                    'target': {'task_id': str(task_id)}, 'status': 'accepted', 'session': session,
                    'original_request_id': str(request.expected_request_id)}
                await operations.save(operation)
                if await Variable.aget(_SESSION) != session:
                    operation['status'] = 'unknown'
                    await operations.save(operation)
                    return await operations.view(operation)
                await Variable.aset(admission.name, {'state': 'running', 'session': session}, overwrite=True)
                operation['status'] = ('confirmed' if await Variable.aget(_SESSION) == session
                    and await Variable.aget(admission.name) == {'state': 'running', 'session': session} else 'unknown')
                await operations.save(operation)
        return await operations.view(operation)

    @app.get("/api/v1/prefect/operations/{request_id}")
    async def operation_result(request_id: UUID):
        operation = await operations.read(request_id)
        if not operation or (operation["action"] == "create" and operation["status"] == "accepted"):
            run = await created_run(request_id)
            if run:
                return {"request_id": str(request_id), "action": "create", "target": {"task_id": str(run.id)},
                        "found": True, "resolved": True, "status": "confirmed", "accepted": True}
        if not operation:
            return {"request_id": str(request_id), "action": None, "target": None,
                    "found": False, "resolved": False, "status": "unknown"}
        if operation["action"] == "pause" and operation["status"] in {"accepted", "unknown"}:
            async with operations.lock:
                current = await operations.latest(operation["target"]["task_id"])
                if (current and current["request_id"] == operation["request_id"]
                        and not await Admission(operation["target"]["task_id"]).running()):
                    progress = await pause_progress(operation["target"]["task_id"])
                    operation = {**operation, "pause_state": progress["state"],
                                 "status": "confirmed" if progress["state"] == "paused" else
                                           "unknown" if progress["state"] == "unresolved" else "accepted"}
                    await operations.save(operation)
        if (operation["action"] == "retry" and operation["status"] in {"accepted", "unknown"}
                and operation["target"].get("ticket_id")):
            records = await read_receipts(UUID(operation["target"]["ticket_id"]))
            if any(row["kind"] == "manual_retry" and row["data"].get("retry_id") == operation["retry_id"]
                   and row["data"].get("state") == "running" for row in records):
                operation = {**operation, "status": "confirmed"}
        return await operations.view(operation)

    @app.post("/api/v1/prefect/tasks/{task_id}/start")
    async def start_task(task_id: UUID, request: ControlAction):
        run = await read_task(task_id)
        async with operations.lock:
            operation, fresh = await operations.begin_control("start", request.request_id,
                                                               request.intent_seq, task_id)
            if fresh and str(task_id) in operations.recovery_dispatches:
                operation.update(status="rejected", reason="原工单派发正在核对，未重复发送")
                await operations.save(operation)
                raise HTTPException(409, operation["reason"])
        if not fresh:
            return await operations.view(operation)
        try:
            scope = await recovery.read_scope(task_id)
            if scope is not None:
                scope_children = await _children(task_id)
                if not recovery.can_start(scope, scope_children):
                    raise RuntimeError("没有待恢复的原工单；未确认请求仍保留待核对")
        except Exception as exc:
            reason = str(exc) if isinstance(exc, RuntimeError) else "原工单恢复范围读取未确认，需核对"
            operation.update(status="rejected", reason=reason)
            async with operations.lock:
                await operations.save(operation)
            raise HTTPException(409, reason) from exc
        await invalidate_query(task_id)
        running = await Admission(task_id).running()
        children = scope_children if scope is not None else await _children(task_id)
        action = (recovery.actions(scope, children, running) if scope is not None
                  else _task_actions(run, running, children))["start"]
        if not action["allowed"]:
            operation.update(status="rejected", reason=action["reason"])
            async with operations.lock:
                await operations.save(operation)
            raise HTTPException(409, action["reason"])
        checks = await check_dependencies()
        if not checks["ready"]:
            operation.update(status="rejected", checked_at=checks["checked_at"],
                             failed_checks=[check for check in checks["checks"] if not check["ready"]])
            async with operations.lock:
                await operations.save(operation)
            raise HTTPException(409, detail=checks)

        async def save_start_progress(**fields):
            async with operations.lock:
                current = await operations.latest(task_id)
                if current["request_id"] != operation["request_id"]:
                    operation.update(status="superseded", message="开始已被后续操作取代")
                else:
                    operation.update(fields)
                await operations.save(operation)
            return operation["status"] != "superseded"

        async def reject_start(reason):
            if not await save_start_progress(status="rejected", reason=reason, message=reason):
                return await operations.view(operation)
            raise HTTPException(409, reason)

        async def ensure_current():
            # Adapters check before each account-test batch or configuration
            # write; a pause can register during a preceding network request.
            async with operations.lock:
                current = await operations.latest(task_id)
                if current["request_id"] != operation["request_id"]:
                    raise HTTPException(409, "开始已被后续操作取代")

        if test_start_accounts is None or apply_start_capacity is None:
            return await reject_start("国内账号测试与并发配置尚未接入，本次未启动处理")
        if not await save_start_progress(phase="account_test", message="正在测试国内账号，本次尚未开始处理"):
            return await operations.view(operation)
        try:
            tested = await test_start_accounts(ensure_current=ensure_current)
            successful_count = tested.get("successful_count")
        except Exception as exc:
            reason = str(exc) if isinstance(exc, CapacityError) else type(exc).__name__
            return await reject_start("国内账号测试结果未确认（" + reason + "），本次未启动处理")
        if type(successful_count) is not int or successful_count < 0:
            return await reject_start("国内账号测试未返回有效成功数量，本次未启动处理")
        if successful_count == 0:
            return await reject_start("没有测试成功的国内账号，本次未启动处理")
        try:
            limit = tested_llm_limit(tested)
        except CapacityError as exc:
            return await reject_start(str(exc))
        summary = capacity_fields(tested)
        applying = (capacity_description(tested) + '；正在核对账号池和共享名额'
                    if summary else f"已确认 {successful_count} 个国内账号，正在配置账号范围和并发上限 {limit}")
        if not await save_start_progress(phase="capacity_apply", successful_accounts=successful_count, llm_limit=limit,
                **summary, message=applying):
            return await operations.view(operation)
        try:
            # Apply and read back the existing gateway whitelist and Prefect
            # limits without holding the lock that registers a later pause.
            await apply_start_capacity(tested, limit, ensure_current=ensure_current)
        except Exception as exc:
            reason = str(exc) if isinstance(exc, CapacityError) else type(exc).__name__
            return await reject_start("账号范围或并发配置未确认（" + reason + "），本次未启动处理")
        async def perform_start():
            async with operations.lock:
                current = await operations.latest(task_id)
                if current["request_id"] != operation["request_id"]:
                    operation.update(status="superseded", message="开始已被后续操作取代")
                    await operations.save(operation)
                    return await operations.view(operation)
                operation.update(phase="task_start", message="账号范围和并发配置已核对，正在开始任务")
                if scope is not None:
                    operations.recovery_dispatches[str(task_id)] = operation["request_id"]
                else:
                    await Admission(task_id).start()
                    crashed_children = None
                await operations.save(operation)
            if scope is not None:
                if await recovery.read_scope(task_id) != scope:
                    raise RuntimeError("原工单恢复范围已变化，需核对")
                children = await _children(task_id)
                if not recovery.can_start(scope, children):
                    raise RuntimeError("没有待恢复的原工单，未放行处理")
                crashed_children = recovery.select_targets(scope, children)
            resume_confirmed = True
            if scope is None and run.state.is_paused():
                await aresume_flow_run(task_id)
                resumed = await read_task(task_id)
                resume_confirmed = (resumed.state.is_scheduled() or resumed.state.is_pending()
                                    or resumed.state.is_running() or resumed.state.is_completed())
            elif scope is None and run.state.is_crashed():
                async with get_client() as client:
                    transition = await client.set_flow_run_state(task_id, Scheduled())
                if transition.state is None or not transition.state.is_scheduled():
                    raise HTTPException(409, "Prefect 未接受原工单恢复，需核对")
            if crashed_children is None:
                crashed_children = [child for child in await _children(task_id) if child.state.is_crashed()]
            async with get_client() as client:
                for child in crashed_children:
                    if scope is None:
                        transition = await client.set_flow_run_state(child.id, Scheduled())
                        if transition.state is None or not transition.state.is_scheduled():
                            raise HTTPException(409, "原论文工单恢复结果待核对")
                        continue
                    async with operations.lock:
                        current = await operations.latest(task_id)
                        if current["request_id"] != operation["request_id"]:
                            operation["status"] = "superseded"
                            await operations.save(operation)
                            return await operations.view(operation)
                    if await recovery.read_scope(task_id) != scope:
                        raise RuntimeError("原工单恢复范围已变化，需核对")
                    actual = await client.read_flow_run(child.id)
                    recovery.check_child(scope, actual)
                    if not actual.state.is_crashed() or actual.state.id != child.state.id:
                        raise RuntimeError("原工单恢复状态已变化，需核对")
                    scope["dispatch_in_flight"] = {"request_id": operation["request_id"], "ticket_id": str(child.id)}
                    # Durable uncertainty precedes the native effect. This guard
                    # blocks a new Start without blocking the user's Pause.
                    await recovery.save_scope(scope)
                    async with operations.lock:
                        current = await operations.latest(task_id)
                        superseded = current["request_id"] != operation["request_id"]
                    if superseded:
                        # No native proposal was sent. Settle that known fact
                        # while our submission guard still excludes other writers.
                        if await recovery.read_scope(task_id) != scope:
                            raise RuntimeError("原工单恢复范围已变化，需核对")
                        scope.pop("dispatch_in_flight")
                        await recovery.save_scope(scope)
                        async with operations.lock:
                            operation.update(status="superseded", message="开始已被后续操作取代，本次尚未派发",
                                last_dispatch={"ticket_id":str(child.id),"status":"not_sent"})
                            await operations.save(operation)
                            return await operations.view(operation)
                    transition = await client.set_flow_run_state(child.id, Scheduled())
                    if transition.state is None or not transition.state.is_scheduled():
                        raise RuntimeError("原论文工单恢复结果待核对")
                    observed = await client.read_flow_run(child.id)
                    if not (observed.state.is_scheduled() or observed.state.is_pending()
                            or observed.state.is_running() or observed.state.is_final()):
                        raise RuntimeError("原论文工单恢复状态读回待核对")
                    if (str(observed.state.id) == scope["eligible"][str(child.id)]["prepared_state_id"]
                            or observed.state.is_scheduled() and observed.state.id != transition.state.id):
                        raise RuntimeError("原论文工单派发状态未读回，保留待核对")
                    if await recovery.read_scope(task_id) != scope:
                        raise RuntimeError("原工单恢复范围已变化，需核对")
                    scope["eligible"][str(child.id)]["dispatch"] = {
                        "request_id": operation["request_id"], "state_id": str(transition.state.id)}
                    scope.pop("dispatch_in_flight")
                    # A confirmed native result is evidence, even if Pause has
                    # since superseded this request. Admission remains closed.
                    await recovery.save_scope(scope)
                    async with operations.lock:
                        current = await operations.latest(task_id)
                        if current["request_id"] != operation["request_id"]:
                            operation.update(status="superseded", last_dispatch={"ticket_id": str(child.id),
                                "proposal_state_id": str(transition.state.id), "observed_state_id": str(observed.state.id)})
                            await operations.save(operation)
                            return await operations.view(operation)
            if scope is not None:
                if await recovery.read_scope(task_id) != scope:
                    raise RuntimeError("原工单恢复范围已变化，需核对")
                async with operations.lock:
                    current = await operations.latest(task_id)
                    if current["request_id"] != operation["request_id"]:
                        operation["status"] = "superseded"
                        await operations.save(operation)
                        return await operations.view(operation)
                    # Native scheduling and its evidence are durable before opening admission.
                    await Admission(task_id).start()
                    operations.recovery_dispatches.pop(str(task_id), None)
            dependency = next((row["data"] for row in await read_receipts(task_id)
                               if row["kind"] == "dependency_failure"), None)
            if resume_confirmed and dependency and dependency.get("status") == "active":
                await save_receipt(task_id=task_id, folder_id="task", child_flow_run_id=task_id,
                                   event_id=uuid4(), kind="dependency_failure", data={"status": "cleared"})
            async with operations.lock:
                current = await operations.latest(task_id)
                operation["status"] = ("superseded" if current["request_id"] != operation["request_id"]
                                       else "confirmed" if resume_confirmed and await Admission(task_id).running()
                                       else "unknown")
                operation["message"] = {"superseded": "开始已被后续操作取代", "confirmed": "开始已确认",
                                        "unknown": "开始结果待核对"}[operation["status"]]
                if operation['status'] == 'confirmed' and summary:
                    operation['message'] += '；' + capacity_description(tested)
                await operations.save(operation)
            await invalidate_query(task_id)
            return {"task_id": str(task_id), **await operations.view(operation)}

        try:
            return await perform_start()
        except Exception as exc:
            if scope is None:
                raise
            async with operations.lock:
                current = await operations.latest(task_id)
                if current["request_id"] != operation["request_id"]:
                    operation.update(status="superseded", dispatch_status="unknown",
                        reason="旧开始结果待核对，已被后续操作取代，未自动重发")
                    await operations.save(operation)
                    return await operations.view(operation)
                operations.recovery_dispatches[str(task_id)] = operation["request_id"]
                await Admission(task_id).pause()
            # This request owns the short submission guard; later Starts can
            # register, but cannot write scope or open admission before it settles.
            current_scope = await recovery.read_scope(task_id)
            if current_scope == scope:
                scope.update(prepared_at=None, unconfirmed_dispatch={"request_id": operation["request_id"]})
                await recovery.save_scope(scope)
            async with operations.lock:
                current = await operations.latest(task_id)
                operation.update(status="unknown" if current["request_id"] == operation["request_id"] else "superseded",
                    reason="原论文工单恢复结果待核对，已暂停，未自动重发")
                await operations.save(operation)
            await invalidate_query(task_id)
            raise HTTPException(409, operation["reason"]) from exc
        finally:
            if operations.recovery_dispatches.get(str(task_id)) == operation["request_id"]:
                operations.recovery_dispatches.pop(str(task_id))

    @app.post("/api/v1/prefect/tasks/{task_id}/pause")
    async def pause_task(task_id: UUID, request: ControlAction):
        run = await read_task(task_id)
        async with operations.lock:
            operation, fresh = await operations.begin_control("pause", request.request_id,
                                                               request.intent_seq, task_id)
            if fresh:
                if run.state.is_final() and not run.state.is_crashed():
                    operation["status"] = "rejected"
                else:
                    await Admission(task_id).pause()
                    progress = await pause_progress(task_id)
                    operation["pause_state"] = progress["state"]
                    operation["status"] = ("confirmed" if progress["state"] == "paused" else
                                           "unknown" if progress["state"] == "unresolved" else "accepted")
                await operations.save(operation)
        if operation["status"] == "rejected":
            raise HTTPException(409, "任务已结束" if run.state.is_final() and not run.state.is_crashed()
                                else "操作顺序重复，需核对")
        if fresh and operation["status"] != "rejected":
            await invalidate_query(task_id)
        return {"task_id": str(task_id), **await operations.view(operation)}

    async def read_delete_run(task_id):
        async with get_client() as client:
            try:
                run = await client.read_flow_run(task_id)
            except ObjectNotFound:
                return None
        if run.deployment_id != deployment_id:
            raise HTTPException(404, "Task does not belong to this application")
        return run

    @app.get("/api/v1/prefect/tasks/{task_id}/deletion")
    async def task_deletion_status(task_id: UUID):
        from .deletion import read_plan, task_variable_names
        run = await read_delete_run(task_id)
        current = await operations.latest(task_id)
        plan = await read_plan(task_id, deployment_id)
        if plan is not None or current and current.get("action") == "delete":
            operation = await operations.read(current["request_id"]) if current else None
            active = str(task_id) in operations.delete_requests
            return {"task_id": str(task_id), "deleted": False,
                    "status": "deleting" if active else "failed",
                    "reason": operation.get("reason") if operation else None}
        if run is None:
            async with get_client() as client:
                remaining = await task_variable_names(client, task_id)
            if remaining:
                return {"task_id": str(task_id), "deleted": False, "status": "failed",
                        "reason": "Task control cleanup needs to be resumed"}
            return {"task_id": str(task_id), "deleted": True}
        return {"task_id": str(task_id), "deleted": False, "status": "present"}

    @app.post("/api/v1/prefect/tasks/{task_id}/delete")
    async def delete_task(task_id: UUID, request: ControlAction):
        from .deletion import delete_task_data, read_plan, remove_variables, remove_control_limit
        run = await read_delete_run(task_id)
        if run is None and await read_plan(task_id, deployment_id) is None:
            async with get_client() as client:
                await remove_variables(client, task_id)
                await remove_control_limit(client, task_id)
            return {"task_id": str(task_id), "request_id": str(request.request_id), "deleted": True}
        async with operations.lock:
            if str(task_id) in operations.delete_requests:
                raise HTTPException(409, "Task deletion is already running")
            operation, fresh = await operations.begin_control("delete", request.request_id,
                                                               request.intent_seq, task_id)
            if operation["status"] in {"rejected", "superseded"}:
                raise HTTPException(409, "Delete intent was superseded")
            operations.deleting.add(str(task_id))
            operations.delete_requests.add(str(task_id))
            operation["status"] = "accepted"
        try:
            async with operations.lock:
                await operations.save(operation)
                await Admission(task_id).delete()
            result = await delete_task_data(task_id, settings, operations, query_service, run)
            return {**result, "request_id": str(request.request_id)}
        except (OSError, ValueError, TimeoutError, RuntimeError, httpx.HTTPError) as exc:
            operation.update(status="unknown", reason=str(exc))
            await operations.save(operation)
            raise HTTPException(409, detail={"message": str(exc), "deleted": False}) from exc
        finally:
            operations.delete_requests.discard(str(task_id))

    @app.get("/api/v1/prefect/folder-tickets")
    async def folder_tickets(task_id: UUID):
        result = await query_service.task_details(task_id)
        return JSONResponse(result, status_code=503 if result["query_status"] == "unavailable" else 200)

    @app.post("/api/v1/prefect/tasks/{task_id}/folders/{folder_id}/retry")
    async def retry_folder(task_id: UUID, folder_id: str, request: RetryAction):
        if str(task_id) in operations.deleting or (await operations.latest(task_id) or {}).get("action") == "delete":
            raise HTTPException(409, "Task is being deleted")
        await read_task(task_id)
        scope = await recovery.read_scope(task_id)
        if scope is not None:
            permitted = {row["group_id"] for row in scope["eligible"].values()}
            if folder_id not in permitted:
                raise HTTPException(409, "当前仅恢复已核对的失联工单；此工单仍需核对或不在范围内")
        async with operations.lock:
            operation = await operations.read(request.request_id)
            if operation:
                target = operation.get("target") or {}
                if (operation["action"] != "retry" or target.get("task_id") != str(task_id)
                        or target.get("folder_id") != folder_id):
                    raise HTTPException(409, "操作请求身份已用于其他操作，需核对")
                result = await operations.view(operation)
                return {"task_id": str(task_id), "folder_id": folder_id,
                        "ticket_id": target.get("ticket_id"), **result,
                        "status": "scheduled" if result["status"] == "confirmed" else result["status"]}
            operation = {"request_id": str(request.request_id), "action": "retry",
                         "target": {"task_id": str(task_id), "folder_id": folder_id},
                         "retry_id": str(uuid4()), "status": "accepted"}
            await operations.save(operation)
        await invalidate_query(task_id, folder_id=folder_id)

        async def reject(detail, status_code=409):
            operation["status"] = "rejected"
            async with operations.lock:
                await operations.save(operation)
            raise HTTPException(status_code, detail)

        children = [child for child in await _children(task_id) if child.parameters.get("group_id") == folder_id]
        if not children:
            await reject("论文工单不存在", 404)
        if len(children) != 1:
            await reject("论文工单身份不唯一，需核对")
        child = children[0]
        operation["target"]["ticket_id"] = str(child.id)
        async with operations.lock:
            await operations.save(operation)
        records = await read_receipts(child.id)
        from paperflow_v3.delivery.progress import local_records, merge_records
        records = merge_records(local_records(child.id), records)
        identity = _folder_identity(child)
        registered = child.parameters.get("registered")
        decision = retry_decision(records, identity=identity, registered=registered, **rag_retry_settings)
        latest_retry = _last_retry(records, child, folder_status(records)["stage"], decision)
        if not (child.state.is_failed() or child.state.is_crashed()):
            await reject({"code": "retry_already_dispatched" if latest_retry else "paper_not_stopped",
                          "message": "论文工单尚未停止，不能重复重试", "last_retry": latest_retry})
        if not decision["allowed"]:
            await reject({"code": "retry_dispatch_unknown" if latest_retry and
                          latest_retry["status"] == "dispatch_unknown" else "retry_not_allowed",
                          "message": decision["reason"], "last_retry": latest_retry})
        checks = await check_dependencies()
        if not checks["ready"]:
            await reject(checks)
        retry_id = operation["retry_id"]
        operation["affected_stages"] = [decision["stage"]]
        async with operations.lock:
            await operations.save(operation)
        evidence = {key: decision[key] for key in ("review_event_id", "failure_event_id") if key in decision}
        await save_receipt(task_id=task_id, folder_id=folder_id, child_flow_run_id=child.id,
                           event_id=retry_id, kind="manual_retry",
                           data={"request_id": str(request.request_id), "retry_id": retry_id,
                                 "state": "authorized", "stage": decision["stage"], **evidence})
        try:
            async with get_client() as client:
                from .stage_queue import TOKEN_LABEL
                if getattr(child, 'labels', {}).get(TOKEN_LABEL):
                    # This endpoint has checked and saved explicit manual retry
                    # authorization. Its source is this reviewed final state.
                    await client.update_flow_run_labels(child.id, labels={TOKEN_LABEL: str(child.state_id)})
                transition = await client.set_flow_run_state(child.id, Scheduled())
        except Exception as exc:
            operation["status"] = "unknown"
            async with operations.lock:
                await operations.save(operation)
            await invalidate_query(task_id, folder_id=folder_id)
            raise HTTPException(409, {"code": "retry_dispatch_unknown", "message": "原论文工单重试派发结果待核对",
                                      "request_id": str(request.request_id), "retry_id": retry_id}) from exc
        if transition.state is None or not transition.state.is_scheduled():
            operation["status"] = "unknown"
            async with operations.lock:
                await operations.save(operation)
            await invalidate_query(task_id, folder_id=folder_id)
            raise HTTPException(409, {"code": "retry_dispatch_unknown", "message": "原论文工单重试派发结果待核对",
                                      "request_id": str(request.request_id), "retry_id": retry_id})
        operation["status"] = "confirmed"
        async with operations.lock:
            await operations.save(operation)
        await invalidate_query(task_id, folder_id=folder_id)
        return {"task_id": str(task_id), "folder_id": folder_id, "ticket_id": str(child.id),
                "request_id": str(request.request_id), "accepted": True,
                "retry_id": retry_id, "status": "scheduled", "affected_stages": operation["affected_stages"]}

    @app.get("/api/v1/prefect/tasks/{task_id}/classifications")
    async def classifications(task_id: UUID):
        result = await query_service.classifications(task_id)
        return JSONResponse(result, status_code=503 if result["query_status"] == "unavailable" else 200)

    return app
