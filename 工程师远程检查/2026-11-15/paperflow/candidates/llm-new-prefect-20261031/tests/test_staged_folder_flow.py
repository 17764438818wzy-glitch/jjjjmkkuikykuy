"""Exercise the staged folder flow on a temporary Prefect server only."""
import asyncio
from contextlib import asynccontextmanager
import importlib.util
import os
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from uuid import uuid4

from prefect import flow
from prefect.client.orchestration import get_client
from prefect.runtime import flow_run
from prefect.testing.utilities import prefect_test_harness


HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parents[1]
STAGE = ROOT / 'candidates' / os.environ.get('PAPERFLOW_TEST_STAGE', 'llm-flow-staged-20261031')
sys.path.insert(0, str(ROOT / 'prefect-paperflow'))


def test_one_folder_continues_after_releasing_preprocessing_and_suspends_before_post(tmp_path, monkeypatch):
    import paperflow_prefect
    import prefect.flow_runs
    events = []
    fake_flows = ModuleType('paperflow_prefect.flows')
    fake_flows.LIMIT_PREFIX = 'isolated-'
    @asynccontextmanager
    async def lease(*_args, **_kwargs):
        events.append('preprocessing_enter')
        yield
        events.append('preprocessing_exit')
    fake_flows.async_concurrency = lease
    monkeypatch.setitem(sys.modules, 'paperflow_prefect.flows', fake_flows)
    monkeypatch.setattr(paperflow_prefect, 'flows', fake_flows, raising=False)
    package = ModuleType('paperflow_prefect.llm_queue')
    package.__path__ = [str(STAGE / 'llm_queue')]
    monkeypatch.setitem(sys.modules, 'paperflow_prefect.llm_queue', package)
    fake_runtime = ModuleType('paperflow_prefect.llm_queue.flow_runtime')
    def llm(_settings, task_id, group_id, receipt_id, attempt_id, folder_run_id):
        assert events[-1] == 'preprocessing_exit'
        events.append(('llm',task_id,group_id,receipt_id,attempt_id,folder_run_id))
        return {'status':'waiting_postprocessing'}
    fake_runtime.run_live_llm_folder = llm
    monkeypatch.setitem(sys.modules, 'paperflow_prefect.llm_queue.flow_runtime', fake_runtime)
    unified = ModuleType('paperflow_prefect.unified_flows')
    unified.runtime_identity = lambda _: None
    monkeypatch.setitem(sys.modules, 'paperflow_prefect.unified_flows', unified)
    spec = importlib.util.spec_from_file_location('paperflow_prefect._staged_local_preprocessing',
                                                  STAGE / 'local_preprocessing.py')
    entry = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(entry)
    class Settings:
        def __init__(self, values): self.values = values
        @classmethod
        def load(cls): return cls({})
        def require_execution(self): pass
        def path(self, key):
            assert key == 'data_root'
            return tmp_path
    monkeypatch.setattr(entry, 'Settings', Settings)
    monkeypatch.setattr(entry, '_queue', lambda: object())
    monkeypatch.setattr(entry, '_bound_folder_order', lambda *_: True)
    monkeypatch.setattr(entry, 'prepare_folder', lambda *_: {'status':'waiting_for_llm'})
    async def drain(fn, *args, **_kwargs): return fn(*args)
    monkeypatch.setattr(entry, 'drain_thread', drain)
    class Suspended(Exception): pass
    async def suspend(**_kwargs):
        async with get_client() as client:
            current = await client.read_flow_run(flow_run.id)
        events.append(('suspend',current.name))
        raise Suspended()
    monkeypatch.setattr(prefect.flow_runs, 'asuspend_flow_run', suspend)
    task_id, group_id, receipt_id, attempt_id = str(uuid4()), uuid4().hex, 'a' * 64, 'attempt-1'

    @flow(retries=0)
    async def synthetic_folder():
        try:
            await entry.local_preprocessing_handoff.fn(task_id=task_id, group_id=group_id,
                receipt_id=receipt_id, attempt_id=attempt_id, folder_label='合成文件夹')
        except Suspended:
            return 'waiting_postprocessing'
        raise AssertionError('folder did not suspend')

    with prefect_test_harness(server_startup_timeout=60):
        result = asyncio.run(synthetic_folder())
    assert result == 'waiting_postprocessing'
    assert events[:2] == ['preprocessing_enter','preprocessing_exit']
    assert events[2][0] == 'llm' and events[2][1:5] == (task_id,group_id,receipt_id,attempt_id)
    assert events[3][0] == 'suspend' and events[3][1].endswith('等待后处理')
