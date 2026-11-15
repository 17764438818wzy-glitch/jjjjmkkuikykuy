import asyncio
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch


class SafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancelling_inner_waiter_cannot_hide_a_running_thread(self):
        from filelock import FileLock, Timeout
        from paperflow_prefect.resources import drain_thread
        started, release = threading.Event(), threading.Event()
        waiters = []
        original = asyncio.wrap_future
        def capture(*args, **kwargs):
            result = original(*args, **kwargs); waiters.append(result); return result
        def operation(): started.set(); release.wait(3)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'paper.lock'
            async def run():
                with FileLock(path, timeout=0, thread_local=False): await drain_thread(operation)
            with patch('asyncio.wrap_future', side_effect=capture), patch('asyncio.futures.wrap_future', side_effect=capture):
                pending = asyncio.create_task(run())
                while not started.is_set(): await asyncio.sleep(0.005)
                try:
                    pending.cancel(); waiters[0].cancel()
                    await asyncio.sleep(0.03)
                    self.assertFalse(pending.done())
                    with self.assertRaises(Timeout):
                        with FileLock(path, timeout=0): pass
                finally:
                    release.set()
                    with self.assertRaises(asyncio.CancelledError): await pending
            with FileLock(path, timeout=0): pass

    async def test_process_mode_cannot_bypass_local_disabled_switch(self):
        from paperflow_prefect import flows
        from paperflow_prefect.settings import ExecutionDisabled
        with patch.object(flows, 'Backend', side_effect=AssertionError('business reached')):
            with patch.object(flows, 'discover', side_effect=AssertionError('source scanned')):
                with self.assertRaises(ExecutionDisabled):
                    await flows.paperflow_batch.fn(run_mode='process')

    async def test_cancellation_keeps_paper_lock_until_thread_has_finished(self):
        from filelock import FileLock, Timeout
        from paperflow_prefect.resources import drain_thread
        started, release, stop = threading.Event(), threading.Event(), threading.Event()
        def operation(): started.set(); release.wait(3)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'paper.lock'
            async def run():
                with FileLock(path, timeout=0, thread_local=False):
                    await drain_thread(operation, stop=stop)
            pending = asyncio.create_task(run())
            await asyncio.to_thread(started.wait, 2)
            pending.cancel()
            await asyncio.sleep(0.03)
            try:
                self.assertTrue(stop.is_set())
                self.assertFalse(pending.done())
                with self.assertRaises(Timeout):
                    with FileLock(path, timeout=0): pass
            finally: release.set()
            with self.assertRaises(asyncio.CancelledError): await pending
            with FileLock(path, timeout=0): pass


if __name__ == '__main__': unittest.main()
