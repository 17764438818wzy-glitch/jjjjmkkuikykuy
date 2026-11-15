"""Use native Scheduled tickets between resource phases of the same FlowRun."""
import asyncio
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
import json
import logging
import os
import time
from threading import Event, Lock, Thread
from uuid import UUID

import httpx
from prefect.client.orchestration import get_client
from prefect.client.schemas.objects import ConcurrencyLeaseHolder
from prefect.client.schemas.responses import ConcurrencyLimitWithLeaseResponse
from prefect.concurrency._asyncio import _discard_cleanup_lease
from prefect.context import FlowRunContext
from prefect.exceptions import Abort
from prefect.settings import PREFECT_CLIENT_MAX_RETRIES, temporary_settings
from prefect.states import AwaitingRetry
from prefect.utilities.engine import propose_state

GRANT_ENV = 'PAPERFLOW_V3_PHASE_GRANT'
RESOURCE_LABEL = 'paperflow.stage_resources'
TOKEN_LABEL = 'paperflow.stage_token'
LEASE_SECONDS = 300
_phase = ContextVar('paperflow_native_phase', default=None)
_held = ContextVar('paperflow_held_stage_resource', default=None)


class StageQueueWait(Exception):
    def __init__(self, names, seconds=0):
        self.names = tuple(sorted(set(names)))
        self.seconds = seconds
        super().__init__('等待阶段通道：' + ','.join(self.names))


class LeaseControlUncertain(RuntimeError):
    """An unconfirmed control operation, never a business transport result."""
    def __init__(self, operation, names, run_id):
        self.operation, self.names, self.run_id = operation, tuple(names), str(run_id)
        super().__init__(f'native stage lease {operation} ACK unknown; '
                         f'flow_run={run_id}, resources={",".join(names)}; reconciliation required')


def current_phase():
    return _phase.get()


def phase_healthy():
    phase = current_phase()
    return phase is None or not phase.failed.is_set()


def holds_resource(names):
    return _held.get() == tuple(sorted(set(names)))


@contextmanager
def held_resource(names):
    token = _held.set(tuple(sorted(set(names))))
    try:
        yield
    finally:
        _held.reset(token)


@contextmanager
def bind_phase(phase):
    token = _phase.set(phase)
    try:
        yield phase
    finally:
        _phase.reset(token)


def validate_grant(expected, actual):
    if {row.name: str(row.id) for row in actual} != expected:
        raise RuntimeError('native stage lease grant is incomplete or changed')


def capacity_busy(exc):
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code == 423
        exc = exc.__cause__ or exc.__context__
    return False


async def acquire_stage_lease(client, names, run_id):
    # Use the public native atomic operation in THIS coroutine. The background
    # acquisition service retains its first caller's retry context; a warm
    # service must not replay a lease POST after an unconfirmed response.
    try:
        with temporary_settings({PREFECT_CLIENT_MAX_RETRIES: 0}):
            response = await client.increment_concurrency_slots_with_lease(list(names), slots=1,
                mode='concurrency', lease_duration=LEASE_SECONDS,
                holder=ConcurrencyLeaseHolder(type='flow_run', id=run_id))
    except httpx.TransportError as exc:
        raise LeaseControlUncertain('acquire', names, run_id) from exc
    return ConcurrencyLimitWithLeaseResponse.model_validate(response.json())


async def release_stage_lease(client, lease_id):
    """Release once; a lost ACK leaves capacity to native expiry, never replay."""
    try:
        with temporary_settings({PREFECT_CLIENT_MAX_RETRIES: 0}):
            await client.release_concurrency_slots_with_lease(lease_id)
    except httpx.TransportError:
        logging.getLogger(__name__).warning(
            'Native stage lease release ACK unknown; lease_id=%s; no replay, native expiry owns reclamation',
            lease_id, exc_info=True)
        return False
    _discard_cleanup_lease(lease_id)
    return True


class PhaseLease:
    """A worker-acquired native lease, renewed inside its isolated child process."""
    def __init__(self, grant):
        self.grant = grant
        self.names = tuple(sorted(grant['names']))
        self.failed, self.hard_failed, self.closed = Event(), Event(), Event()
        self.renewed_at = time.monotonic()
        self.operation = Lock()
        self.released = False
        self._finish_task = None
        self.loop, self.thread = None, None

    def require(self, names):
        if self.failed.is_set():
            raise RuntimeError('native stage lease renewal failed; no new operation')
        if tuple(sorted(set(names))) != self.names:
            raise StageQueueWait(names)

    def _renew(self):
        try:
            with temporary_settings({PREFECT_CLIENT_MAX_RETRIES: 0}):
                with get_client(sync_client=True, httpx_settings={'timeout': 10}) as client:
                    delay = 60
                    while not self.closed.wait(delay):
                        try:
                            client.renew_concurrency_lease(UUID(self.grant['lease_id']), LEASE_SECONDS)
                        except Exception as exc:
                            self.failed.set()
                            gone = isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 410
                            if gone or time.monotonic() - self.renewed_at >= LEASE_SECONDS - 30:
                                self.hard_failed.set()
                            delay = 5
                        else:
                            self.renewed_at = time.monotonic()
                            if not self.hard_failed.is_set():
                                self.failed.clear()
                            delay = 60
        except Exception:
            self.failed.set()
            self.hard_failed.set()

    def fenced_stop(self, original):
        phase = self
        class FencedStop:
            def is_set(self):
                return (phase.hard_failed.is_set() or time.monotonic() - phase.renewed_at >= LEASE_SECONDS - 30
                        or original.is_set())
            def wait(self, seconds=None):
                deadline = None if seconds is None else time.monotonic() + seconds
                while not self.is_set():
                    if deadline is not None and time.monotonic() >= deadline:
                        return False
                    time.sleep(.05)
                return True
        return FencedStop()

    async def start(self, child):
        self.loop = asyncio.get_running_loop()
        if (self.grant.get('flow_run_id') != str(child.id)
                or self.grant.get('previous_run_count') + 1 != child.run_count):
            raise RuntimeError('native stage lease belongs to another run or attempt')
        async with get_client() as client:
            current = await client.read_flow_run(child.id)
            if (current.run_count != child.run_count or not current.state.is_running()
                    or current.labels.get(TOKEN_LABEL, '') != self.grant.get('token', '')):
                raise RuntimeError('native stage attempt or queue checkpoint changed')
            rows = await asyncio.gather(*(client.read_global_concurrency_limit_by_name(name) for name in self.names))
            validate_grant(self.grant['limit_ids'], rows)
            if any(not row.active or row.slot_decay_per_second for row in rows):
                raise RuntimeError('native stage capacity is inactive or decaying')
            await client.renew_concurrency_lease(UUID(self.grant['lease_id']), LEASE_SECONDS)
            self.renewed_at = time.monotonic()
        thread = Thread(target=self._renew, daemon=True, name='paperflow-stage-lease')
        thread.start()
        self.thread = thread
        return thread

    async def finish(self):
        # Body and attachment branches may leave the same unused pregrant.
        # Share one release, including an unknown ACK; never decrement twice.
        self.closed.set()
        if self._finish_task is None:
            self._finish_task = asyncio.create_task(self._finish())
        await asyncio.shield(self._finish_task)

    async def _finish(self):
        self.closed.set()
        if self.thread is not None:
            await asyncio.to_thread(self.thread.join, 12)
        async with get_client() as client:
            lease_id = UUID(self.grant['lease_id'])
            self.released = await release_stage_lease(client, lease_id)


async def _new_resource(names):
    child = FlowRunContext.get().flow_run
    async with get_client() as client:
        rows = await asyncio.gather(*(client.read_global_concurrency_limit_by_name(name) for name in names))
    if any(not row.active or row.slot_decay_per_second for row in rows):
        raise RuntimeError('native stage capacity is inactive or decaying')
    expected = {row.name: str(row.id) for row in rows}
    try:
        async with get_client() as client:
            lease = await acquire_stage_lease(client, names, child.id)
    except Exception as exc:
        if capacity_busy(exc):
            raise StageQueueWait(names) from None
        raise
    try:
        validate_grant(expected, lease.limits)
        phase = PhaseLease({'flow_run_id': str(child.id), 'previous_run_count': child.run_count - 1,
            'names': list(names), 'lease_id': str(lease.lease_id), 'limit_ids': expected,
            'token': child.labels.get(TOKEN_LABEL, '')})
        await phase.start(child)
        return phase
    except BaseException:
        async with get_client() as client:
            await release_stage_lease(client, lease.lease_id)
        raise


async def _take_resource(phase, names):
    phase.require(phase.names)
    names = tuple(sorted(set(names)))
    if (not phase.released and not phase.closed.is_set()
            and names == phase.names and phase.operation.acquire(blocking=False)):
        return phase
    # A saved/cached stage may no longer need the worker's pregrant. Never
    # retain it while trying another resource, and never free an active branch.
    if not phase.released and not phase.operation.locked():
        await phase.finish()
    selected = await _new_resource(names)
    assert selected.operation.acquire(blocking=False)
    return selected


@asynccontextmanager
async def phase_slot(names):
    selected = await _take_resource(current_phase(), names)
    try:
        with bind_phase(selected), held_resource(names):
            yield
    finally:
        selected.closed.set()
        selected.operation.release()
        await selected.finish()


@contextmanager
def sync_phase_slot(names):
    owner = current_phase()
    selected = asyncio.run_coroutine_threadsafe(_take_resource(owner, names), owner.loop).result()
    try:
        with bind_phase(selected), held_resource(names):
            yield
    finally:
        selected.closed.set()
        selected.operation.release()
        asyncio.run_coroutine_threadsafe(selected.finish(), owner.loop).result()


@asynccontextmanager
async def worker_phase(child):
    raw = os.environ.get(GRANT_ENV)
    if not raw:
        raise RuntimeError('工单执行器尚未载入阶段队列版本')
    phase = PhaseLease(json.loads(raw))
    thread = None
    try:
        thread = await phase.start(child)
        with bind_phase(phase):
            yield phase
    finally:
        # The existing drain_thread boundaries have already drained business
        # operations. Only this child returns its lease; native expiry covers
        # abrupt process loss without a second release POST from the worker.
        if thread is not None:
            await phase.finish()


async def requeue_phase(identity, wait):
    """Call only after FolderStage, business threads and resource scopes exit."""
    from .receipts import read_receipts
    from .stage_admission import unsettled_requests
    from .admission import control_gate, Admission
    records = await read_receipts(identity['task_run_id'])
    if any(unsettled_requests(records, stage) for stage in ('preprocessing', 'llm', 'postprocessing', 'delivery')):
        raise RuntimeError('原工单存在待核对请求，未重新排队')
    child = FlowRunContext.get().flow_run
    async with control_gate(identity['task_id']), get_client() as client:
        task = await client.read_variable_by_name(Admission(identity['task_id']).name)
        if task is not None and task.value.get('state') == 'deleting':
            raise Abort('Task deletion owns this ticket; no queue update')
        current = await client.read_flow_run(child.id)
        if current.run_count != child.run_count or not current.state.is_running():
            raise Abort('Native attempt ownership changed; no queue update')
        labels = dict(current.labels)
        labels.update({RESOURCE_LABEL: json.dumps(wait.names),
                       TOKEN_LABEL: str(child.state_id)})
        # Once queue writes start, any missing ACK exits without a new terminal
        # proposal: a successor may already own the same original FlowRun.
        try:
            await client.update_flow_run_labels(child.id, labels=labels)
            state = await propose_state(client, AwaitingRetry(
                scheduled_time=datetime.now(timezone.utc) + timedelta(seconds=wait.seconds)), flow_run_id=child.id)
        except Abort:
            raise
        except Exception as exc:
            logging.getLogger(__name__).exception('Native stage handoff ACK could not be confirmed')
            raise Abort('Native stage handoff ACK unknown; retain original state for reconciliation') from exc
    signal = Abort('原工单已保存，等待下一阶段通道')
    signal.state = state
    raise signal


async def return_unstarted_to_queue(flow_run, client):
    """Only the worker owning Native Pending may defer a confirmed423."""
    from .admission import control_gate, Admission
    task_id = flow_run.parameters['task_id']
    async with control_gate(task_id):
        task = await client.read_variable_by_name(Admission(task_id).name)
        current = await client.read_flow_run(flow_run.id)
        if (task is not None and task.value.get('state') == 'deleting') or not current.state.is_pending():
            return
        await client.update_flow_run_labels(flow_run.id, labels={TOKEN_LABEL: str(current.state_id)})
        await propose_state(client, AwaitingRetry(), flow_run_id=flow_run.id)
