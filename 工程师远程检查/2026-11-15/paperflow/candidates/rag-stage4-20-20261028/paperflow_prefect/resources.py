"""Release local capacity during HTTP waits; drain threads before releasing owners."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import contextvars

_business_threads = ThreadPoolExecutor(max_workers=256, thread_name_prefix='paperflow-business')


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
