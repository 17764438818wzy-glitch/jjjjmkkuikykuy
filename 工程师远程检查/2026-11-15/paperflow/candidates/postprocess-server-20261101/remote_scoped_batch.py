"""Bounded explicit batch with independently verified per-folder policies.

This module never runs work on import, scans no tasks and adds no process-wide
lock. The caller supplies one explicit task snapshot, original task authority,
per-folder source preparation and a launcher that uses the original Prefect
stage/claim and shared postprocessing capacity.
"""
import asyncio
import copy


async def run_scoped_batch(*, task_id, selections, batch_size, check_configuration, authority, prepare, launch):
    if (not isinstance(task_id, str) or not task_id or not isinstance(selections, list)
            or type(batch_size) is not int or not 1 <= batch_size <= 8
            or not all(callable(fn) for fn in (check_configuration, authority, prepare, launch))):
        raise ValueError('one explicit task, bounded selections and original controls are required')
    seen = set()
    for entry in selections:
        if (not isinstance(entry, (tuple, list)) or len(entry) != 2 or not isinstance(entry[0], dict)
                or entry[0].get('task_id') != task_id or not isinstance(entry[0].get('group_id'), str)
                or not entry[0]['group_id'] or entry[0]['group_id'] in seen
                or type(entry[1]) is not int or entry[1] < 1):
            raise ValueError('batch identity or generation is invalid')
        seen.add(entry[0]['group_id'])
    selections = copy.deepcopy(selections)
    try:
        await asyncio.to_thread(check_configuration)
        original_authority = await asyncio.to_thread(authority)
    except Exception:
        return dict(stopped=True, reason='task_or_configuration_changed', attempted=0, confirmed_finished=0,
                    snapshot_selected=len(selections))
    confirmed = attempted = 0
    for offset in range(0, len(selections), batch_size):
        try:
            await asyncio.to_thread(check_configuration)
            if await asyncio.to_thread(authority) != original_authority:
                raise ValueError('task authority changed')
        except Exception:
            return dict(stopped=True, reason='task_or_configuration_changed', attempted=attempted,
                        confirmed_finished=confirmed, snapshot_selected=len(selections))
        group = selections[offset:offset + batch_size]
        prepared = []
        try:
            for identity, generation in group:
                supplied = copy.deepcopy(identity)
                item = await asyncio.to_thread(prepare, supplied, generation)
                if supplied != identity:
                    raise ValueError('folder policy preparation changed the original identity')
                prepared.append(item)
        except Exception:
            return dict(stopped=True, reason='folder_policy_unavailable', attempted=attempted,
                        confirmed_finished=confirmed, snapshot_selected=len(selections))
        try:
            await asyncio.to_thread(check_configuration)
            if await asyncio.to_thread(authority) != original_authority:
                raise ValueError('task authority changed')
        except Exception:
            return dict(stopped=True, reason='task_or_configuration_changed', attempted=attempted,
                        confirmed_finished=confirmed, snapshot_selected=len(selections))
        async def checked_launch(identity, config):
            actual, supplied = copy.deepcopy(identity), copy.deepcopy(config)
            outcome = await launch(actual, supplied)
            if actual != identity:
                raise ValueError('folder launch changed the original identity')
            return outcome
        outcomes = await asyncio.gather(*(checked_launch(identity, config)
                                          for (identity, _), config in zip(group, prepared)),
                                        return_exceptions=True)
        attempted += len(group)
        confirmed += sum(outcome == 'finished' for outcome in outcomes)
        if any(outcome != 'finished' for outcome in outcomes):
            return dict(stopped=True, reason='folder_not_confirmed', attempted=attempted,
                        confirmed_finished=confirmed, snapshot_selected=len(selections))
    return dict(stopped=False, reason=None, attempted=attempted, confirmed_finished=confirmed,
                snapshot_selected=len(selections))


