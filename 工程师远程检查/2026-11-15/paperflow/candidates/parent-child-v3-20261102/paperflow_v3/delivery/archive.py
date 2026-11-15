"""Copy the received originals once to a caller-selected paper directory."""

import copy
import os
from pathlib import Path
import shutil

def save_archive(paper, identity, handoff, destination, state, save_state, check_running,
                 *, retry_failed=False):
    rows = paper['archive_files']
    expected = [paper['body']['file_id'], *(row['file_id'] for row in paper['attachments'])]
    if len(rows) != len(expected) or {row['file_id'] for row in rows} != set(expected):
        raise ValueError('archive inventory does not cover the body and attachments')
    root = Path(destination).absolute()
    if state and state.get('identity') != identity:
        raise ValueError('archive state belongs to another child ticket')
    state.setdefault('identity', copy.deepcopy(identity))
    files = state.setdefault('files', {})
    for row in rows:
        relative = Path(row['relative'])
        if relative.is_absolute() or '..' in relative.parts or not relative.parts:
            raise ValueError('archive member must have a relative destination')
        existing = files.get(row['file_id'], {})
        if existing and Path(existing['destination']) != root / relative:
            raise ValueError('archive destination changed')
        target = root / relative
        if (existing.get('partial_owned')
                and Path(existing['partial_path']) != target.with_name(target.name + '.paperflow-part')):
            raise ValueError('partial archive path changed')
        if existing.get('status') in ('in_flight', 'unknown'):
            raise ValueError('archive outcome unknown; reconciliation required')
        if existing.get('status') == 'failed' and not retry_failed:
            raise ValueError('archive file requires a user-selected retry')

    state.setdefault('status', 'incomplete')
    for row in rows:
        entry = files.get(row['file_id'])
        if entry and entry['status'] == 'complete':
            continue
        if check_running() is not True:
            raise InterruptedError('parent task is paused')
        source = Path(row['path'])
        target = root / row['relative']
        partial = target.with_name(target.name + '.paperflow-part')
        if entry and entry['status'] == 'failed' and entry.get('partial_owned'):
            if check_running() is not True:
                raise InterruptedError('parent task is paused')
            Path(entry['partial_path']).unlink(missing_ok=True)
        entry = files[row['file_id']] = {'status': 'in_flight', 'destination': str(target),
                                         'partial_path': str(partial)}
        if not save_state(state):
            raise ValueError('server archive intent was not saved')
        if check_running() is not True:
            value = copy.deepcopy(state)
            value['files'][row['file_id']]['status'] = 'pending'
            if not save_state(value):
                raise ValueError('server archive state was not saved')
            state.clear()
            state.update(value)
            raise InterruptedError('parent task is paused')
        partial_owned = False
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            with source.open('rb') as source_stream, partial.open('xb') as target_stream:
                partial_owned = True
                shutil.copyfileobj(source_stream, target_stream)
                target_stream.flush()
                os.fsync(target_stream.fileno())
        except OSError as exc:
            value = copy.deepcopy(state)
            value['files'][row['file_id']].update(status='failed', partial_owned=partial_owned,
                                                    error_type=type(exc).__name__)
            if not save_state(value):
                raise ValueError('server archive failure was not saved') from exc
            state.clear()
            state.update(value)
            raise
        # A failed network rename can have an unknown outcome, so leave the
        # original in-flight identity untouched if this operation raises.
        partial.rename(target)
        value = copy.deepcopy(state)
        value['files'][row['file_id']]['status'] = 'complete'
        if not save_state(value):
            raise ValueError('server archive result was not saved')
        state.clear()
        state.update(value)
        files = state['files']
    value = copy.deepcopy(state)
    value['status'] = 'complete'
    if not save_state(value):
        raise ValueError('server archive result was not saved')
    state.clear()
    state.update(value)
    return state
