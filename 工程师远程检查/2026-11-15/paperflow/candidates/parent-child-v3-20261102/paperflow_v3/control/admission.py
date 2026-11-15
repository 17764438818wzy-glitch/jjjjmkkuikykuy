"""Per-task admission backed by Prefect variables and global concurrency limits."""

import asyncio
from contextlib import asynccontextmanager, contextmanager
import time
from uuid import uuid4

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.client.schemas.filters import VariableFilter
from prefect.client.schemas.objects import Variable as VariableRecord
from prefect.concurrency.asyncio import concurrency
from prefect.concurrency.sync import concurrency as sync_concurrency
from prefect.variables import Variable
from prefect.exceptions import ObjectAlreadyExists, ObjectNotFound


_SESSION = "paperflow_v3_control_session"


def read_variables_sync(client, names):
    """One fresh native read of the requested control values; never cache permission."""
    names = list(dict.fromkeys(names))
    selected = VariableFilter(name={'any_': names})
    response = client.request('POST', '/variables/filter', json={
        'variables': selected.model_dump(mode='json', exclude_none=True), 'limit': len(names)})
    return {row.name: row for row in VariableRecord.model_validate_list(response.json())}


@asynccontextmanager
async def control_gate(task_id):
    """Coordinate stage entry and task control across API/worker processes."""
    name = "paperflow-v3-control-" + str(task_id).replace("-", "")
    async with get_client() as client:
        try:
            limit = await client.read_global_concurrency_limit_by_name(name)
        except ObjectNotFound:
            try:
                await client.create_global_concurrency_limit(
                    GlobalConcurrencyLimitCreate(name=name, limit=1, active=True))
            except ObjectAlreadyExists:
                pass
            limit = await client.read_global_concurrency_limit_by_name(name)
        if limit.limit != 1 or not limit.active or limit.slot_decay_per_second != 0:
            raise RuntimeError("任务准入控制额度无效，未开始新阶段")
    async with concurrency(name, occupy=1, strict=True):
        yield


async def begin_control_session():
    """Invalidate prior starts when the control service starts or restarts."""
    session = str(uuid4())
    await Variable.aset(_SESSION, session, overwrite=True)
    return session


async def pause_for_dependency(task_id, folder_id, dependency):
    """Pause this task for a confirmed shared-service authentication failure."""
    from .receipts import save_receipt

    reasons = {"model": "模型服务认证或连接失败", "rag": "RAG 服务认证或连接失败",
               "nimble": "分类服务认证或连接失败", "nas": "NAS 目标不可访问"}
    await Admission(task_id).pause()
    await save_receipt(task_id=task_id, folder_id="task", child_flow_run_id=task_id,
                       event_id=uuid4(), kind="dependency_failure",
                       data={"status": "active", "dependency": dependency, "reason": reasons[dependency],
                             "folder_id": folder_id})


class Admission:
    def __init__(self, task_id):
        self.task_id = str(task_id)
        self.name = "paperflow_v3_task_" + str(task_id).replace("-", "_")

    async def delete(self):
        async with control_gate(self.task_id):
            await Variable.aset(self.name, {"state": "deleting", "session": None}, overwrite=True)

    async def start(self):
        async with control_gate(self.task_id):
            current = await Variable.aget(self.name)
            if isinstance(current, dict) and current.get("state") == "deleting":
                raise RuntimeError("Task is being deleted")
            session = await Variable.aget(_SESSION)
            if session is None:
                raise RuntimeError("Control service has not started a session")
            await Variable.aset(self.name, {"state": "running", "session": session}, overwrite=True)

    async def pause(self):
        async with control_gate(self.task_id):
            current = await Variable.aget(self.name)
            if isinstance(current, dict) and current.get("state") == "deleting":
                return
            await Variable.aset(self.name, {"state": "paused", "session": None}, overwrite=True)

    async def running(self):
        from .stage_queue import phase_healthy
        if not phase_healthy():
            return False
        session = await Variable.aget(_SESSION)
        state = await Variable.aget(self.name)
        return session is not None and state == {"state": "running", "session": session}

    def running_sync(self):
        """Read admission from a synchronous business-operation thread."""
        # Variable.get is sync-compatible and can return a coroutine when the
        # thread inherited an async Prefect flow context. Use the sync client
        # explicitly so this boundary always returns an actual boolean.
        from .stage_queue import phase_healthy
        if not phase_healthy():
            return False
        with get_client(sync_client=True) as client:
            values = read_variables_sync(client, [_SESSION, self.name])
        session, state = values.get(_SESSION), values.get(self.name)
        return (session is not None and state is not None
                and state.value == {"state": "running", "session": session.value})

    async def wait(self):
        while not await self.running():
            from .stage_queue import current_phase, StageQueueWait
            if phase := current_phase():
                phase.require(phase.names)
                raise StageQueueWait(phase.names)
            await asyncio.sleep(0.2)


async def wait_for_resources(admission, limits):
    from .stage_queue import StageQueueWait
    try:
        await admission.wait()
    except StageQueueWait as wait:
        raise StageQueueWait(limits, wait.seconds) from None


@asynccontextmanager
async def admitted_slot(admission, limits):
    """Do not begin work if the task paused while awaiting a shared slot."""
    from .stage_queue import current_phase, StageQueueWait, holds_resource, phase_slot
    if phase := current_phase():
        async def admitted():
            await admission.wait()
            if hasattr(admission, 'admit') and not await admission.admit():
                raise StageQueueWait(limits)
            if not await admission.running():
                raise StageQueueWait(limits)
        if holds_resource(limits):
            phase.require(limits)
            await admitted()
            yield
            return
        async with phase_slot(limits):
            await admitted()
            yield
        return
    while True:
        await admission.wait()
        async with concurrency(limits, occupy=1, strict=True):
            if hasattr(admission, 'admit') and not await admission.admit():
                continue
            if not await admission.running():
                continue
            yield
            return


@contextmanager
def admitted_sync_slot(admission, limits):
    """The same shared-slot boundary for synchronous business-operation threads."""
    from .stage_queue import current_phase, StageQueueWait, holds_resource, sync_phase_slot
    if phase := current_phase():
        def admitted():
            if hasattr(admission, 'wait_sync'):
                admission.wait_sync()
            if hasattr(admission, 'admit_sync') and not admission.admit_sync():
                raise StageQueueWait(limits)
            if not admission.running_sync():
                raise StageQueueWait(limits)
        if holds_resource(limits):
            phase.require(limits)
            admitted()
            yield
            return
        with sync_phase_slot(limits):
            admitted()
            yield
        return
    while True:
        while True:
            if hasattr(admission, 'wait_sync'):
                admission.wait_sync()
            if admission.running_sync():
                break
            time.sleep(0.2)
        stop = getattr(admission, 'stop', None)
        if stop is None:
            resource = sync_concurrency(limits, occupy=1, strict=True)
        else:
            from paperflow_prefect.resources import cancellable_concurrency
            names = [limits] if isinstance(limits, str) else limits
            with get_client(sync_client=True) as client:
                expected = {name: client.read_global_concurrency_limit_by_name(name).limit for name in names}
            resource = cancellable_concurrency(names, stop, expected=expected)
        with resource:
            if hasattr(admission, 'admit_sync') and not admission.admit_sync():
                continue
            if not admission.running_sync():
                continue
            yield
            return


@contextmanager
def continue_sync_slot(limits):
    """An already admitted folder still acquires each downstream capacity slot."""
    with sync_concurrency(limits, occupy=1, strict=True):
        yield
