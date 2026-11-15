"""Isolated candidate for bounded synchronous work in an async RAG API.

This uses the Python executor already used by Quart, with explicit admission,
per-request context, and a separate lifetime for running work after cancellation.
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import contextvars
import threading


class BoundedExecutor:
    def __init__(self, *, workers, max_waiting, max_admission_waiting=20):
        if type(workers) is not int or workers not in (2, 4):
            raise ValueError("isolated candidate workers must be 2 or 4")
        if type(max_waiting) is not int or not 0 <= max_waiting <= 20:
            raise ValueError("invalid executor waiting bound")
        if type(max_admission_waiting) is not int or not 0 <= max_admission_waiting <= 20:
            raise ValueError("invalid admission waiting bound")
        self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="paperflow-ingest")
        self._semaphore = asyncio.Semaphore(workers + max_waiting)
        self._lock = threading.Lock()
        self._executing = 0
        self._queued = 0
        self._waiting = 0
        self._max_admission_waiting = max_admission_waiting
        self._closed = False

    def snapshot(self):
        with self._lock:
            return {"executing": self._executing, "queued": self._queued,
                    "waiting_to_admit": self._waiting}

    async def run(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        with self._lock:
            if self._closed:
                raise RuntimeError("executor is closed")
            if self._semaphore.locked() and self._waiting >= self._max_admission_waiting:
                raise RuntimeError("executor admission backlog is full")
            self._waiting += 1
        try:
            await self._semaphore.acquire()
        finally:
            with self._lock:
                self._waiting -= 1
        with self._lock:
            if self._closed:
                self._semaphore.release()
                raise RuntimeError("executor is closed")
            self._queued += 1
        state = {"started": False}
        ctx = contextvars.copy_context()

        def invoke():
            with self._lock:
                state["started"] = True
                self._queued -= 1
                self._executing += 1
            try:
                return ctx.run(func, *args, **kwargs)
            finally:
                with self._lock:
                    self._executing -= 1

        try:
            future = self._executor.submit(invoke)
        except BaseException:
            with self._lock:
                self._queued -= 1
            self._semaphore.release()
            raise

        def finished(_):
            with self._lock:
                if not state["started"]:
                    self._queued -= 1
            try:
                loop.call_soon_threadsafe(self._semaphore.release)
            except RuntimeError:
                # No request can use a closed event loop. Running work must
                # nevertheless finish and remain accounted for in its ledger.
                pass

        future.add_done_callback(finished)
        return await asyncio.wrap_future(future)

    async def aclose(self):
        with self._lock:
            self._closed = True
        await asyncio.to_thread(self._executor.shutdown, wait=True)
