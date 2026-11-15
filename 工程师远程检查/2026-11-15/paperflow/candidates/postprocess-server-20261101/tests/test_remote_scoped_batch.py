import asyncio

from remote_scoped_batch import run_scoped_batch


def test_one_folder_policy_per_prefect_ticket_and_no_extra_batch_after_unknown():
    current = {'running': 0, 'peak': 0}
    prepared, started = [], []
    selections = [({'task_id': 'task', 'group_id': str(i)}, 1) for i in range(5)]

    def authority(): return ('running', 1)
    def prepare(identity, generation):
        prepared.append(identity['group_id'])
        return {'only_folder': identity['group_id']}
    async def launch(identity, config):
        assert config == {'only_folder': identity['group_id']}
        started.append(identity['group_id'])
        current['running'] += 1
        current['peak'] = max(current['peak'], current['running'])
        await asyncio.sleep(0)
        current['running'] -= 1
        return 'unknown' if identity['group_id'] == '1' else 'finished'

    result = asyncio.run(run_scoped_batch(task_id='task', selections=selections, batch_size=2,
        check_configuration=lambda: None, authority=authority, prepare=prepare, launch=launch))
    assert result['stopped'] and result['reason'] == 'folder_not_confirmed'
    assert result['confirmed_finished'] == 1 and result['attempted'] == 2
    assert prepared == started == ['0', '1'] and current['peak'] == 2


def test_parent_pause_during_source_check_prevents_any_batch_dispatch():
    authority_calls, started = [], []
    def authority():
        authority_calls.append(None)
        return ('running', 1) if len(authority_calls) <= 2 else ('paused', 1)
    async def launch(identity, config):
        started.append(identity)
        return 'finished'
    result = asyncio.run(run_scoped_batch(task_id='task', selections=[({'task_id': 'task', 'group_id': 'a'}, 1)],
        batch_size=1, check_configuration=lambda: None, authority=authority,
        prepare=lambda identity, generation: {}, launch=launch))
    assert result['stopped'] and result['reason'] == 'task_or_configuration_changed'
    assert started == [] and result['attempted'] == 0


def test_folder_policy_failure_stops_without_launching_a_partial_group():
    started = []
    def prepare(identity, generation):
        if identity['group_id'] == 'b': raise ValueError('synthetic source mismatch')
        return {}
    async def launch(identity, config):
        started.append(identity)
        return 'finished'
    result = asyncio.run(run_scoped_batch(task_id='task', selections=[({'task_id': 'task', 'group_id': 'a'}, 1),
        ({'task_id': 'task', 'group_id': 'b'}, 1)], batch_size=2, check_configuration=lambda: None,
        authority=lambda: ('running', 1), prepare=prepare, launch=launch))
    assert result['stopped'] and result['reason'] == 'folder_policy_unavailable'
    assert started == [] and result['attempted'] == 0


def test_folder_identity_mutation_during_preparation_is_never_launched():
    identity = {'task_id': 'task', 'group_id': 'original'}
    started = []
    def prepare(actual, generation):
        actual['group_id'] = 'changed'
        return {}
    async def launch(actual, config):
        started.append(actual)
        return 'finished'
    result = asyncio.run(run_scoped_batch(task_id='task', selections=[(identity, 1)], batch_size=1,
        check_configuration=lambda: None, authority=lambda: ('running', 1), prepare=prepare, launch=launch))
    assert result['stopped'] and result['reason'] == 'folder_policy_unavailable'
    assert identity['group_id'] == 'original' and started == []
