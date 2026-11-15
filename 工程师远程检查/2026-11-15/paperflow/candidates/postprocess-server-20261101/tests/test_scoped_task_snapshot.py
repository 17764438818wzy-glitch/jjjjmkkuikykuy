import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest


def setup():
    task, parent = str(uuid4()), str(uuid4())
    def identity(index):
        return dict(task_id=task, group_id=f'{index:032x}', paper_id='paper', receipt_id='a' * 64,
                    attempt_id='attempt', parent_run_id=parent, task_run_id=str(uuid4()))
    folders = [(identity(1), 1), (identity(2), 1)]
    class Runtime:
        api = 'http://127.0.0.1:4200/api'
        execution_enabled = True
        original_runtime = {'database': 'synthetic'}
        batch = SimpleNamespace(snapshot_waiting=lambda *_: folders, _identity=lambda row: row)
        def check_configuration(self): pass
        def _batch_task_authority(self, actual):
            assert actual == task
            return 1, parent, 'running'
    return task, parent, folders, Runtime()


def test_original_task_snapshot_runs_each_folder_with_its_own_policy():
    from scoped_task_snapshot import run_explicit_task
    task, _, folders, runtime = setup()
    calls = []
    class Launcher:
        def prepare(self, identity, generation):
            return {'identity': identity, 'generation': generation}
        async def launch(self, identity, generation, prepared):
            assert prepared == {'identity': identity, 'generation': generation}
            calls.append(identity['task_run_id'])
            return 'finished'
    result = asyncio.run(run_explicit_task(runtime, Launcher(), task, max_folders=2, batch_size=2))
    assert not result['stopped'] and result['confirmed_finished'] == 2
    assert set(calls) == {item[0]['task_run_id'] for item in folders}


def test_foreign_parent_or_generation_is_rejected_before_any_folder_launch():
    from scoped_task_snapshot import run_explicit_task
    task, _, folders, runtime = setup()
    folders[0][0]['parent_run_id'] = str(uuid4())
    calls = []
    launcher = SimpleNamespace(prepare=lambda *x: calls.append(x), launch=lambda *x: None)
    with pytest.raises(ValueError):
        asyncio.run(run_explicit_task(runtime, launcher, task, max_folders=2, batch_size=2))
    assert calls == []
