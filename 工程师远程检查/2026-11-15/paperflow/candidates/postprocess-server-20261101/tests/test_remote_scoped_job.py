import asyncio
from types import SimpleNamespace

import pytest

from remote_scoped_job import run_exact_folder


def runtime(selected, selected_generation=1):
    calls = []
    class Entry:
        async def run_one(self, identity, generation):
            calls.append((identity, generation))
            return {'state': 'finished'}
    result = SimpleNamespace(execution_enabled=True, api='http://127.0.0.1:4200/api',
        original_runtime={'database': 'synthetic-db'}, remote=SimpleNamespace(entry=Entry()),
        live=SimpleNamespace(select_folder=lambda *_: (selected, selected_generation)),
        check_configuration=lambda: None)
    return result, calls


def identity():
    return dict(task_id='t', group_id='g', paper_id='p', receipt_id='r', attempt_id='a',
                parent_run_id='parent', task_run_id='folder')


def test_exact_prefect_folder_identity_and_generation_are_passed_to_original_entry():
    selected = identity(); rt, calls = runtime(selected)
    assert asyncio.run(run_exact_folder(rt, selected, 1))['state'] == 'finished'
    assert calls == [(selected, 1)]


@pytest.mark.parametrize('change', ['identity', 'generation', 'disabled'])
def test_changed_original_folder_never_reaches_original_entry(change):
    selected = identity(); current = dict(selected)
    if change == 'identity': current['paper_id'] = 'changed'
    rt, calls = runtime(current, 2 if change == 'generation' else 1)
    if change == 'disabled': rt.execution_enabled = False
    with pytest.raises(ValueError):
        asyncio.run(run_exact_folder(rt, selected, 1))
    assert calls == []
