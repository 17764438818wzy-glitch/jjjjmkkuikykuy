"""Release local capacity during HTTP waits; drain threads before releasing owners."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import contextvars
import time
from prefect.concurrency import _sync as prefect_sync
from prefect.client.orchestration import get_client
from prefect.settings import PREFECT_CLIENT_MAX_RETRIES, temporary_settings

_business_threads = ThreadPoolExecutor(max_workers=256, thread_name_prefix='paperflow-business')
def _validate_limit_states(expected, stop=None):
    if stop is not None and stop.is_set():
        raise InterruptedError('检查并发名额前已停止')
    identities = {}
    with temporary_settings({PREFECT_CLIENT_MAX_RETRIES: 0}):
        with get_client(httpx_settings={'timeout': 3.0}, sync_client=True) as api:
            for name, limit in expected.items():
                if stop is not None and stop.is_set():
                    raise InterruptedError('检查并发名额时已停止')
                row = api.read_global_concurrency_limit_by_name(name)
                if not row.active or row.limit != limit or row.slot_decay_per_second:
                    raise RuntimeError('Prefect 并发名额配置不符，未发送请求：' + name)
                identities[name] = str(row.id)
            if stop is not None and stop.is_set():
                raise InterruptedError('检查并发名额后已停止')
    return identities


@contextmanager
def cancellable_concurrency(names, stop, *, expected):
    """Cancel native Prefect acquisition, then hold its lease on the send thread."""
    names = names if isinstance(names, list) else [names]
    if set(names) != set(expected):
        raise ValueError('并发名额验证范围不符')
    duration = 300
    identities = _validate_limit_states(expected, stop)

    async def release_cancelled(lease):
        if lease.limits:
            await prefect_sync.arelease_concurrency_slots_with_lease(lease.lease_id)
            prefect_sync._discard_cleanup_lease(lease.lease_id)

    async def acquire():
        if stop is not None and stop.is_set():
            raise InterruptedError('等待并发名额时已停止')
        task = asyncio.create_task(prefect_sync.aacquire_concurrency_slots_with_lease(
            names, 1, strict=True, lease_duration=duration))
        try:
            while True:
                done, _ = await asyncio.wait({task}, timeout=0.05)
                if done:
                    lease = task.result()
                    if stop is not None and stop.is_set():
                        await release_cancelled(lease)
                        raise InterruptedError('取得并发名额后已停止')
                    return lease
                if stop is not None and stop.is_set():
                    task.cancel()
                    try:
                        lease = await task
                    except asyncio.CancelledError:
                        raise InterruptedError('等待并发名额时已停止') from None
                    await release_cancelled(lease)
                    raise InterruptedError('取得并发名额后已停止')
        finally:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    lease = asyncio.run(acquire())
    events = {}
    try:
        if {limit.name: limit.limit for limit in lease.limits} != expected:
            raise RuntimeError('Prefect 并发名额不完整，未发送请求')
        current_identities = _validate_limit_states(expected, stop)
        granted_identities = {limit.name: str(limit.id) for limit in lease.limits}
        if identities != current_identities or granted_identities != current_identities:
            raise RuntimeError('Prefect 并发名额身份已改变，未发送请求')
        events = prefect_sync.emit_concurrency_acquisition_events(lease.limits, 1)
        with prefect_sync.maintain_concurrency_lease(
                lease.lease_id, duration, raise_on_lease_renewal_failure=True):
            if stop is not None and stop.is_set():
                raise InterruptedError('取得并发名额后已停止')
            yield
    finally:
        # A cancelled/failed release remains unknown and must reach the caller.
        prefect_sync.release_concurrency_slots_with_lease(lease.lease_id)
        prefect_sync._discard_cleanup_lease(lease.lease_id)
        if events:
            prefect_sync.emit_concurrency_release_events(lease.limits, 1, events)


class DeadlineStop:
    """Honor cancellation while queuing and time optional work after admission."""
    def __init__(self, parent, seconds):
        self.parent = parent
        self.seconds = seconds
        self.deadline = None

    def start(self):
        if self.deadline is None:
            self.deadline = time.monotonic() + self.seconds

    def is_set(self):
        return self.parent.is_set() or (self.deadline is not None and time.monotonic() >= self.deadline)

    def wait(self, seconds=None):
        if self.deadline is None:
            return self.parent.wait(seconds)
        remaining = max(0, self.deadline - time.monotonic())
        self.parent.wait(remaining if seconds is None else min(seconds, remaining))
        return self.is_set()


async def drain_thread(function, *args, stop=None):
    # The concurrent future tracks the actual thread. Cancelling an asyncio
    # wrapper (including during loop shutdown) must not masquerade as completion.
    worker = _business_threads.submit(contextvars.copy_context().run, function, *args)
    try:
        return await asyncio.shield(asyncio.wrap_future(worker))
    except asyncio.CancelledError:
        if stop is not None: stop.set()
        while not worker.done():
            try: await asyncio.shield(asyncio.wrap_future(worker))
            except asyncio.CancelledError: continue
            except BaseException: break
        # Retrieve a failure without replacing the caller's cancellation.
        try: worker.result()
        except BaseException: pass
        raise


class LocalPhase:
    """Synchronous Prefect resource scope that pauses around a model invocation."""
    def __init__(self, factory): self.factory, self.current = factory, None
    def __enter__(self):
        self.current = self.factory(); self.current.__enter__()
        return self
    def __exit__(self, *exc):
        if self.current is not None:
            current, self.current = self.current, None
            return current.__exit__(*exc)
    @contextmanager
    def suspended(self):
        self.__exit__(None, None, None)
        try: yield
        finally: self.__enter__()
