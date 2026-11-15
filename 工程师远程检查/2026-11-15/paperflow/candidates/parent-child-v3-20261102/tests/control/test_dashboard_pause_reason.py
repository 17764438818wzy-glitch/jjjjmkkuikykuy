"""Current native pause summaries take precedence over old Start receipts."""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest
from prefect.states import Running
from paperflow_v3.control import admission, api, dashboard_source as source, recovery, stage_admission

TASK = UUID(int=10)
REQUEST = str(UUID(int=20))


@pytest.mark.parametrize('running, reason, expected', [
    (False, '连续20组模型请求失败，已保护暂停', '连续20组模型请求失败，已保护暂停'),
    (False, None, None),
    (True, '连续20组模型请求失败，已保护暂停', None),
])
def test_current_pause_summary_overrides_stale_duplicate_start(monkeypatch, running, reason, expected):
    intent = {'target': {'task_id': str(TASK)}, 'action': 'start', 'intent_seq': 2, 'request_id': REQUEST}
    run = SimpleNamespace(tags=['paperflow-v3-task'], parameters={'task_name': 'synthetic'},
        name='synthetic', state=Running(), created=datetime(2026, 11, 10, tzinfo=timezone.utc))
    class Client:
        async def read_flow_run(self, task): return run
        async def count_flow_runs(self, **kwargs): return 0
        async def read_variable_by_name(self, name):
            if 'classification_summary' in name:
                return SimpleNamespace(value={'task_id': str(TASK), 'phase': 'closed',
                    'counts': dict(passed=0, failed=0, unrecognized=0, unresolved=0)})
            if 'intent' in name: return SimpleNamespace(value=intent)
            if 'operation' in name:
                return SimpleNamespace(value={**intent, 'status': 'rejected', 'reason': '任务已开始'},
                    updated=datetime(2026, 11, 10, 17, tzinfo=timezone.utc))
            raise AssertionError(name)
    @asynccontextmanager
    async def context(*args): yield Client()
    async def is_running(self): return running
    async def summaries(ids):
        assert ids == [TASK]
        return {str(TASK): {'stop_reason': reason, 'as_of': '2026-11-11T01:52:13+00:00'}}
    async def children(*args, **kwargs): return []
    async def projection(*args, **kwargs):
        return {'folders': [], 'items': [], 'complete': True, 'as_of': '2026-11-11T01:52:13+00:00'}
    async def pause(*args, **kwargs): return {'state': 'paused', 'active_folders': 0}
    async def scope(*args): return None
    monkeypatch.setattr(source, 'native_context', context)
    monkeypatch.setattr(source, 'changed_children', children)
    monkeypatch.setattr(source, 'load_task_snapshot', projection)
    monkeypatch.setattr(source, 'read_task_summaries', summaries, raising=False)
    monkeypatch.setattr(admission.Admission, 'running', is_running)
    monkeypatch.setattr(stage_admission, 'pause_execution_progress', pause)
    monkeypatch.setattr(recovery, 'read_scope', scope)
    monkeypatch.setattr(api, '_task_actions', lambda *args: {})
    result = asyncio.run(source.NativeDashboardSource('http://prefect.invalid/api').observe(TASK, None, prime=True))
    assert result['header']['stop_reason'] == expected
    assert result['header']['control_intent_seq'] == 2
