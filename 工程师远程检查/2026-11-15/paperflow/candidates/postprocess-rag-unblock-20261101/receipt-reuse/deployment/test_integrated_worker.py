import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


path = Path(__file__).with_name('run_remote_postprocess_worker.py')
spec = importlib.util.spec_from_file_location('integrated_worker_candidate', path)
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)


class Done(BaseException):
    pass


def test_bridge_tasks_share_the_existing_wait(monkeypatch):
    monkeypatch.setattr(worker, 'running_tasks', lambda _: ([], []))
    runtime = SimpleNamespace(original_runtime={'database': 'unused'})

    async def exercise():
        item = asyncio.create_task(asyncio.Event().wait())
        class Bridge:
            async def tick(self):
                self.called = True
            def pending(self):
                return {item}
        bridge = Bridge()
        async def wait(items, **kwargs):
            assert bridge.called and set(items) == {item}
            raise Done()
        monkeypatch.setattr(worker.asyncio, 'wait', wait)
        try:
            with pytest.raises(Done):
                await worker.serve(runtime, None, None, bridge)
        finally:
            item.cancel()
            await asyncio.gather(item, return_exceptions=True)
    asyncio.run(exercise())


def test_bridge_failure_does_not_stop_postprocessing_loop(monkeypatch):
    scans, events = [], []
    monkeypatch.setattr(worker, 'running_tasks', lambda _: (scans.append(True) or ([], [])))
    monkeypatch.setattr(worker, 'log', lambda event, **fields: events.append((event, fields)))
    async def no_wait(_):
        return None
    monkeypatch.setattr(worker.asyncio, 'sleep', no_wait)
    class Bridge:
        async def tick(self):
            if len(scans) == 1:
                raise ValueError('temporary downstream failure')
            raise Done()
        def pending(self):
            return set()
    runtime = SimpleNamespace(original_runtime={'database': 'unused'})
    with pytest.raises(Done):
        asyncio.run(worker.serve(runtime, None, None, Bridge()))
    assert len(scans) == 2
    assert any(name == 'rag_nas_coordinator_unavailable' for name, _ in events)
