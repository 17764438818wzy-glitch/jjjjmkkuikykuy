import copy
from types import SimpleNamespace

import pytest

from paperflow_v3.delivery.progress import FolderProgress
from paperflow_v3.delivery.transport import ServerEffects


@pytest.fixture
def setup(tmp_path):
    identity = dict(task_id='task', group_id='group', paper_id='paper', receipt_id='receipt',
                    task_run_id='child', attempt_id='child', parent_run_id='task')
    settings = SimpleNamespace(path=lambda key: tmp_path / 'data')
    sent = []
    def publish(**row):
        sent.append(copy.deepcopy(row))
        return 'saved'
    return settings, identity, sent, publish


def test_many_libraries_and_files_keep_progress_local_and_publish_once(setup):
    settings, identity, sent, publish = setup
    progress = FolderProgress(settings, identity, [], publish)
    for index in range(50):
        progress.save('rag-body', {str(index): {'body_delivery_state': 'complete'}})
        progress.save('nas', {'identity': identity, 'status': 'incomplete', 'files': {}})
        progress.save('cleanup', {'identity': identity, 'status': 'incomplete', 'files': {}})
    assert sent == []
    for kind in ('rag_complete', 'nas', 'cleanup'):
        progress.complete(kind, {'identity': identity, 'status': 'complete'})
        progress.complete(kind, {'identity': identity, 'status': 'complete'})
    assert [row['kind'] for row in sent] == ['rag_complete', 'nas', 'cleanup']
    assert all(set(row['data']) == {'identity', 'status'} for row in sent)
    assert len(list(progress.path.parent.iterdir())) == 1
    assert not progress.path.is_relative_to(settings.path('data'))


def test_unknown_request_stays_blocked_after_local_reopen(setup):
    settings, identity, sent, publish = setup
    progress = FolderProgress(settings, identity, [], publish)
    effects = ServerEffects(identity, [], lambda event, kind, data: progress.save(kind, data, event))
    calls = []
    def interrupted():
        calls.append('sent')
        raise TimeoutError()
    with pytest.raises(TimeoutError):
        effects.send({'endpoint': '/chunks'}, lambda: True, interrupted)
    restored = FolderProgress(settings, identity, [], publish)
    effects = ServerEffects(identity, restored.records, lambda *args: True)
    with pytest.raises(ValueError, match='unknown'):
        effects.send({'endpoint': '/chunks'}, lambda: True, interrupted)
    assert calls == ['sent'] and sent == []


def test_returned_request_is_reused_after_completion_write_fails(setup):
    settings, identity, sent, publish = setup
    progress = FolderProgress(settings, identity, [], lambda **row: False)
    calls = []
    effects = ServerEffects(identity, [], lambda event, kind, data: progress.save(kind, data, event))
    assert effects.send({'endpoint': '/chunks'}, lambda: True, lambda: calls.append(1) or {'id': 'chunk'}) == {'id': 'chunk'}
    result = {'identity': identity, 'status': 'complete'}
    with pytest.raises(ValueError, match='not saved'):
        progress.complete('rag_complete', result)
    restored = FolderProgress(settings, identity, [], publish)
    from paperflow_v3.control.retry import retry_decision
    assert retry_decision(restored.records, identity=identity)['stage'] == 'rag'
    effects = ServerEffects(identity, restored.records, lambda *args: True)
    assert effects.send({'endpoint': '/chunks'}, lambda: True, lambda: calls.append(2)) == {'id': 'chunk'}
    restored.complete('rag_complete', result)
    assert calls == [1] and len(sent) == 1


def test_identity_mismatch_and_corruption_do_not_publish(setup):
    settings, identity, sent, publish = setup
    progress = FolderProgress(settings, identity, [], publish)
    progress.save('nas', {'status': 'incomplete'})
    with pytest.raises(ValueError, match='another paper'):
        FolderProgress(settings, {**identity, 'group_id': 'other'}, [], publish)
    progress.path.write_text('{', encoding='utf8')
    with pytest.raises(ValueError):
        FolderProgress(settings, identity, [], publish)
    assert sent == []


def test_newer_remote_retry_is_after_old_local_failure():
    from paperflow_v3.delivery.progress import merge_records
    from paperflow_v3.control.retry import retry_decision
    old = {'kind': 'nas', 'data': {'files': {'file': {'status': 'failed'}}},
           'recorded_at': '2026-11-03T00:00:00+00:00'}
    retry = {'kind': 'manual_retry', 'data': {'state': 'running'},
             'recorded_at': '2026-11-03T00:01:00+00:00'}
    assert not retry_decision(merge_records([old], [retry]))['allowed']
    new = {**old, 'recorded_at': '2026-11-03T00:02:00+00:00'}
    assert retry_decision(merge_records([new], [retry]))['allowed']


def test_lost_reply_reuses_remote_completion_and_clears_pending(setup):
    settings, identity, sent, _ = setup
    def lost_reply(**row):
        sent.append({**row, 'folder_completion': True})
        raise TimeoutError('reply lost after create')
    state = FolderProgress(settings, identity, [], lost_reply)
    with pytest.raises(TimeoutError):
        state.complete('nas', {'identity': identity, 'status': 'complete'})
    assert 'completion:nas' in state.value['records']
    restored = FolderProgress(settings, identity, sent, lost_reply)
    restored.complete('nas', {'identity': identity, 'status': 'complete'})
    assert len(sent) == 1 and 'completion:nas' not in restored.value['records']
