import asyncio
import copy
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from test_remote_folder_config import fixture


@dataclass
class Runtime:
    original_runtime: dict
    remote: object = None
    execution_enabled: bool = True

    def check_configuration(self):
        return None


def test_each_original_ticket_gets_its_own_verified_remote_source_scope():
    from scoped_task_launcher import ScopedTaskLauncher
    template, first, identity = fixture()
    second_identity = dict(identity, group_id='g2', task_run_id='folder2')
    second = copy.deepcopy(first)
    second['identity'] = second_identity
    second['configuration']['allowed_roots'] = [first['configuration']['allowed_roots'][1]]
    records = {identity['group_id']: first, second_identity['group_id']: second}
    calls = []
    def make_remote(config):
        calls.append(copy.deepcopy(config))
        return SimpleNamespace(scope=config['snapshot_options']['allowed_roots'])
    async def run_exact(runtime, actual_identity, generation):
        assert runtime.remote.scope == records[actual_identity['group_id']]['configuration']['allowed_roots']
        return {'state': 'finished'}
    launcher = ScopedTaskLauncher(Runtime({}), template, {'counter': 'approved'},
        read_policy=lambda identity, generation: records[identity['group_id']],
        make_remote=make_remote, run_exact=run_exact)
    async def exercise():
        for item in (identity, second_identity):
            prepared = launcher.prepare(item, 2)
            assert await launcher.launch(item, 2, prepared) == 'finished'
    asyncio.run(exercise())
    assert len(calls) == 2
    assert calls[0]['transport_config']['allowed_roots'] != calls[1]['transport_config']['allowed_roots']
    assert template['snapshot_options']['allowed_roots'] == ['old']


def test_policy_or_folder_identity_change_never_launches_a_remote_task():
    from scoped_task_launcher import ScopedTaskLauncher
    template, record, identity = fixture()
    sent = []
    launcher = ScopedTaskLauncher(Runtime({}), template, {'counter': 'approved'},
        read_policy=lambda *_: record, make_remote=lambda value: sent.append(value),
        run_exact=lambda *_: None)
    prepared = launcher.prepare(identity, 2)
    with pytest.raises(ValueError):
        asyncio.run(launcher.launch(dict(identity, group_id='other'), 2, prepared))
    record['configuration']['trusted_files'][0]['sha256'] = '9' * 64
    with pytest.raises(ValueError):
        launcher.prepare(identity, 2)
    assert sent == []
