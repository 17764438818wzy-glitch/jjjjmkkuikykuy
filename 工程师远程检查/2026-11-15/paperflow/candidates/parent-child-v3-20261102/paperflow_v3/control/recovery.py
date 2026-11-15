"""A fixed recovery range; Prefect FlowRun states remain the execution lifecycle."""
from datetime import datetime,timezone
import hashlib
import json
import re
from uuid import UUID

import psutil
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import ArtifactCreate
from prefect.client.schemas.filters import ArtifactFilter
from prefect.states import Crashed
from prefect.variables import Variable
from .admission import Admission

def key(task_id):
    return "paperflow_v3_recovery_"+str(UUID(str(task_id))).replace("-","_")

def binding(child):
    params=child.parameters
    return {"parent_task_run_id":str(child.parent_task_run_id),"group_id":params["group_id"],
            "registration_sha256":hashlib.sha256(json.dumps(params["registered"],sort_keys=True,ensure_ascii=False,separators=(",",":")).encode()).hexdigest()}

async def read_scope(task_id):
    pointer=await Variable.aget(key(task_id))
    if pointer is None:return None
    try:
        artifact_id=UUID(pointer["artifact_id"])
        async with get_client() as client:
            rows=await client.read_artifacts(artifact_filter=ArtifactFilter(id={"any_":[artifact_id]}),limit=1)
        if (len(rows)!=1 or rows[0].type!="paperflow-v3-recovery" or str(rows[0].flow_run_id)!=str(task_id)
                or pointer["sha256"]!=digest(rows[0].data)):
            raise ValueError("manifest mismatch")
        scope=rows[0].data
        validate(scope,task_id)
    except (ValueError,TypeError,KeyError,AttributeError) as exc:
        raise RuntimeError("恢复范围记录无效，需核对") from exc
    return scope

def digest(scope):
    return hashlib.sha256(json.dumps(scope,sort_keys=True,ensure_ascii=False,separators=(",",":")).encode()).hexdigest()

async def save_scope(scope):
    """Immutable native artifact, with a small pointer below Prefect's 5000-char limit."""
    validate(scope)
    async with get_client() as client:
        artifact=await client.create_artifact(ArtifactCreate(type="paperflow-v3-recovery",
            flow_run_id=UUID(scope["task_id"]),data=scope))
    await Variable.aset(key(scope["task_id"]),{"artifact_id":str(artifact.id),"sha256":digest(scope)},overwrite=True)

def validate(scope,task_id=None):
    try:
        if (scope.get("version")!=1 or scope.get("mode")!="existing_children"
                or task_id is not None and scope.get("task_id")!=str(task_id)
                or not isinstance(scope.get("eligible"),dict) or not isinstance(scope.get("blocked"),dict)
                or set(scope["eligible"]) & set(scope["blocked"])):raise ValueError("range")
        UUID(scope["task_id"])
        if scope.get("prepared_at") is not None:
            if datetime.fromisoformat(scope["prepared_at"].replace("Z","+00:00")).tzinfo is None:raise ValueError("time")
        if scope.get("dispatch_in_flight") is not None:
            UUID(scope["dispatch_in_flight"]["request_id"]);UUID(scope["dispatch_in_flight"]["ticket_id"])
            if scope["dispatch_in_flight"]["ticket_id"] not in scope["eligible"]:raise ValueError("dispatch range")
        for child_id,row in {**scope["eligible"],**scope["blocked"]}.items():
            UUID(child_id);UUID(row["parent_task_run_id"])
            if not isinstance(row["group_id"],str) or not row["group_id"]:raise ValueError("group")
            if not re.fullmatch("[a-f0-9]{64}",row["registration_sha256"]):raise ValueError("registration")
            if scope.get("prepared_at") or row.get("prepared_state_id") is not None:UUID(row["prepared_state_id"])
            if child_id in scope["blocked"] and (not isinstance(row["reason"],str) or not row["reason"]):raise ValueError("reason")
            if row.get("dispatch") is not None:
                UUID(row["dispatch"]["request_id"]);UUID(row["dispatch"]["state_id"])
    except (ValueError,TypeError,KeyError,AttributeError) as exc:
        raise RuntimeError("恢复范围记录无效，需核对") from exc

def check_child(scope,child):
    if scope is None:return
    validate(scope)
    if not scope.get("prepared_at"):raise RuntimeError("恢复准备尚未完成，需核对")
    row=scope["eligible"].get(str(child.id))
    if row is None:
        reason=scope["blocked"].get(str(child.id),{}).get("reason","不在已确认的恢复范围")
        raise RuntimeError("原工单恢复范围阻止执行："+reason)
    if child.parameters.get("task_id")!=scope["task_id"] or binding(child)!={k:row[k] for k in binding(child)}:
        raise RuntimeError("恢复原工单身份已变化，需核对")

def execution_exists(child):
    """A reused Windows PID is not proof of the original flow execution."""
    try:
        pid=str(child.infrastructure_pid)
        if not pid.isdecimal():return False
        process=psutil.Process(int(pid))
        return process.is_running() and process.environ().get("PREFECT__FLOW_RUN_ID")==str(child.id)
    except (psutil.Error,AttributeError,ValueError):return False

def _targets(scope,children):
    validate(scope)
    if not scope.get("prepared_at"):raise RuntimeError("恢复准备尚未完成，需核对")
    by_id={str(c.id):c for c in children}
    selected=[];active=[]
    for child_id,row in {**scope["eligible"],**scope["blocked"]}.items():
        child=by_id.get(child_id)
        if (child is None or child.parameters.get("task_id")!=scope["task_id"]
                or binding(child)!={k:row[k] for k in binding(child)}):
            raise RuntimeError("恢复原工单身份已变化，需核对")
        if child.state.is_crashed():
            if str(child.state.id)!=row["prepared_state_id"]:
                raise RuntimeError("原工单中断状态已变化，需重新核对")
            if child_id in scope["eligible"]:selected.append(child)
        elif child_id in scope["blocked"]:
            raise RuntimeError("待核对原工单状态已变化，禁止开始")
        else:
            dispatch=row.get("dispatch")
            if not dispatch:raise RuntimeError("原工单状态已变化且没有已确认派发证据，需核对")
            if child.state.is_scheduled():
                if str(child.state.id)!=dispatch["state_id"]:raise RuntimeError("原工单排队身份已变化，需核对")
                active.append(child)
            elif child.state.is_running() or child.state.is_pending():
                if not execution_exists(child):raise RuntimeError("原工单执行进程待核对，未放行处理")
                active.append(child)
            elif not child.state.is_final():raise RuntimeError("原工单状态待核对，未放行处理")
    return selected,active

def select_targets(scope,children):return _targets(scope,children)[0]

def can_start(scope,children):
    if scope.get("dispatch_in_flight") is not None:raise RuntimeError("原工单派发结果待核对，未自动重发")
    selected,active=_targets(scope,children)
    return bool(selected or active)

def actions(scope,children,running):
    try:
        allowed=can_start(scope,children) and not running
        reason=None if allowed else "任务已开始" if running else "没有待恢复的原工单；未确认请求仍保留待核对"
    except RuntimeError as exc:allowed=False;reason=str(exc)
    return {"start":{"allowed":allowed,"reason":reason},
            "pause":{"allowed":running,"reason":None if running else "任务尚未开始或已暂停"}}

async def prepare_scope(task_id,children,decisions):
    """Record reviewed original IDs, then native crashes only; never enqueue work."""
    task_id=str(task_id)
    if await read_scope(task_id) is not None:raise RuntimeError("已有恢复范围，保留现场，不重复准备")
    if {str(c.id) for c in children}!=set(decisions):raise RuntimeError("恢复范围不完整")
    if any(not(c.state.is_running() or c.state.is_pending()) for c in children):
        raise RuntimeError("仅能准备已核对的运行中或等待工单")
    scope={"version":1,"mode":"existing_children","task_id":task_id,"prepared_at":None,"eligible":{},"blocked":{}}
    for child in children:
        if child.parameters.get("task_id")!=task_id or child.parent_task_run_id is None:raise RuntimeError("原工单身份不匹配")
        row={**binding(child),"observed_state_id":str(child.state.id),"prepared_state_id":None}
        reason=decisions[str(child.id)]
        if reason is not None:row["reason"]=reason
        scope["eligible" if reason is None else "blocked"][str(child.id)]=row
    await Admission(task_id).pause()
    await save_scope(scope)
    async with get_client() as client:
        for child in children:
            row=scope["eligible"].get(str(child.id),scope["blocked"].get(str(child.id)))
            current=await client.read_flow_run(child.id)
            if str(current.state.id)!=row["observed_state_id"] or binding(current)!=binding(child):
                raise RuntimeError("原工单身份或状态已变化，停止准备")
            result=await client.set_flow_run_state(child.id,Crashed(message="本机执行因关机中断；已暂停，原请求及工单身份保留"))
            if result.state is None or not result.state.is_crashed():raise RuntimeError("原工单中断状态提案待核对")
            current=await client.read_flow_run(child.id)
            if not current.state.is_crashed() or current.state.id!=result.state.id:raise RuntimeError("原工单中断状态读回待核对")
            row["prepared_state_id"]=str(current.state.id)
    scope["prepared_at"]=datetime.now(timezone.utc).isoformat()
    await save_scope(scope)
    return scope
