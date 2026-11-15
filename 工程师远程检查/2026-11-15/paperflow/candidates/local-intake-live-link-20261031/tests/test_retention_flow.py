import sqlite3
import uuid
import json
from urllib.error import HTTPError

import pytest

from test_registered_input import registered
from test_upstream_discovery import TwoPageSource


class RetentionService:
    def __init__(self):
        self.ids = []
        self.fail_reserve_once = False
        self.fail_confirm_once = False
        self.retention_id = str(uuid.uuid4())
        self.state = 'reserved'
        self.fail_list = False
        self.hide_reference = False

    def reserve_retention(self, task_id, item, *, consumer_id, request_id):
        self.ids.append(request_id)
        self.last = {'task_id': task_id, 'group_id': item['group_id'], 'paper_id': item['paper_id'],
                     'receipt_id': item['receipt_id'], 'consumer_id': consumer_id,
                     'request_id': request_id}
        if self.fail_reserve_once:
            self.fail_reserve_once = False
            raise TimeoutError('synthetic unknown response')
        return {**self.last, 'retention_id': self.retention_id,
                'state': 'reserved', 'created': True}

    def confirm_retention(self, task_id, retention_id, *, consumer_id, receipt_id):
        if self.fail_confirm_once:
            self.fail_confirm_once = False
            raise TimeoutError('synthetic unknown confirmation')
        self.state = 'accepted'
        return {**self.last, 'retention_id': retention_id, 'state': 'accepted'}

    def list_retentions(self, task_id, *, stop_event=None):
        if self.fail_list:
            raise OSError('synthetic protection endpoint unavailable')
        refs = [] if self.hide_reference or not hasattr(self, 'last') else [
            {**self.last, 'retention_id': self.retention_id, 'state': self.state}]
        return {'task_id': task_id, 'items': refs, 'pending_receipts': []}


def test_no_protection_means_no_local_admission_or_count(registered, tmp_path):
    from local_queue import LocalQueue

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'queue.sqlite3', require_retention=True)
    try:
        queue.receive('task-one', item, receipt_root=receipt_root)
    except ValueError:
        pass
    else:
        raise AssertionError('unprotected delivery was accepted')
    assert queue.list_waiting() == []
    assert queue.snapshot()['waiting_count'] is None


def test_lost_reservation_reply_reuses_persisted_request_id(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'queue.sqlite3', require_retention=True)
    source, retention = TwoPageSource(item), RetentionService()
    retention.fail_reserve_once = True
    assert discover_once(source, queue, receipt_root=receipt_root, retention_client=retention)['complete'] is False
    assert queue.list_waiting() == []
    with sqlite3.connect(queue.path) as db:
        saved_id = db.execute('SELECT request_id FROM retention_requests').fetchone()[0]
    assert discover_once(source, queue, receipt_root=receipt_root, retention_client=retention)['complete'] is True
    assert retention.ids == [saved_id, saved_id]
    assert queue.snapshot()['waiting_count'] == 1
    with sqlite3.connect(queue.path) as db:
        assert db.execute('SELECT state FROM retention_requests').fetchone()[0] == 'accepted'


def test_lost_confirmation_does_not_duplicate_or_release_a_received_group(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'queue.sqlite3', require_retention=True)
    source, retention = TwoPageSource(item), RetentionService()
    retention.fail_confirm_once = True
    assert discover_once(source, queue, receipt_root=receipt_root, retention_client=retention)['complete'] is False
    assert queue.snapshot()['waiting_count'] is None
    assert discover_once(source, queue, receipt_root=receipt_root, retention_client=retention)['complete'] is True
    assert len(queue.list_waiting()) == 1
    with sqlite3.connect(queue.path) as db:
        assert db.execute('SELECT state FROM retention_requests').fetchone()[0] == 'accepted'


def test_mismatched_reservation_response_never_admits_group(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    class WrongGroup(RetentionService):
        def reserve_retention(self, *args, **kwargs):
            return {**super().reserve_retention(*args, **kwargs), 'group_id': 'wrong'}

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'queue.sqlite3', require_retention=True)
    result = discover_once(TwoPageSource(item), queue, receipt_root=receipt_root,
                           retention_client=WrongGroup())
    assert result['complete'] is False
    assert queue.list_waiting() == []
    assert queue.snapshot()['waiting_count'] is None


def test_unprotected_old_record_is_not_reported_or_given_to_a_consumer(registered, tmp_path):
    from local_queue import LocalQueue

    _, receipt_root, _, item = registered
    path = tmp_path / 'queue.sqlite3'
    LocalQueue(path).receive('task-one', item, receipt_root=receipt_root)
    protected_queue = LocalQueue(path, require_retention=True)
    assert protected_queue.snapshot()['waiting_count'] is None
    assert protected_queue.claim_next(consumer_id='preprocessor', attempt_id='attempt-1') is None


def test_unavailable_retention_endpoint_keeps_same_request_for_later_retry(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    class NotReady(RetentionService):
        def __init__(self):
            super().__init__()
            self.first = True

        def reserve_retention(self, task_id, item, *, consumer_id, request_id):
            if self.first:
                self.first = False
                self.ids.append(request_id)
                raise HTTPError('http://127.0.0.1', 404, 'not available', {}, None)
            return super().reserve_retention(task_id, item, consumer_id=consumer_id, request_id=request_id)

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'queue.sqlite3', require_retention=True)
    source, retention = TwoPageSource(item), NotReady()
    assert discover_once(source, queue, receipt_root=receipt_root, retention_client=retention)['complete'] is False
    assert discover_once(source, queue, receipt_root=receipt_root, retention_client=retention)['complete'] is True
    assert retention.ids[0] == retention.ids[1]
    assert queue.snapshot()['waiting_count'] == 1


def test_damaged_reservation_response_keeps_same_request_for_retry(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    class DamagedOnce(RetentionService):
        def reserve_retention(self, task_id, item, *, consumer_id, request_id):
            if not self.ids:
                self.ids.append(request_id)
                raise json.JSONDecodeError('synthetic truncated response', '', 0)
            return super().reserve_retention(task_id, item, consumer_id=consumer_id, request_id=request_id)

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'queue.sqlite3', require_retention=True)
    source, retention = TwoPageSource(item), DamagedOnce()
    assert discover_once(source, queue, receipt_root=receipt_root, retention_client=retention)['complete'] is False
    assert discover_once(source, queue, receipt_root=receipt_root, retention_client=retention)['complete'] is True
    assert retention.ids[0] == retention.ids[1]


def test_changed_upstream_paper_identity_freezes_prior_waiting_group(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'queue.sqlite3', require_retention=True)
    retention = RetentionService()
    assert discover_once(TwoPageSource(item), queue, receipt_root=receipt_root,
                         retention_client=retention)['complete'] is True
    changed = {**item, 'paper_id': 'PF_conflicting'}
    assert discover_once(TwoPageSource(changed), queue, receipt_root=receipt_root,
                         retention_client=retention)['complete'] is False
    assert queue.list_waiting() == []
    assert len(queue.list_review()) == 1
    assert queue.claim_next(consumer_id='preprocessor', attempt_id='wrong') is None


def test_unavailable_protection_endpoint_holds_and_rechecks_existing_reception(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, root, _, item = registered
    queue = LocalQueue(tmp_path / 'queue.sqlite3', require_retention=True)
    source, retention = TwoPageSource(item), RetentionService()
    assert discover_once(source, queue, receipt_root=root, retention_client=retention)['complete']
    retention.fail_list = True
    assert not discover_once(source, queue, receipt_root=root, retention_client=retention)['complete']
    assert queue.snapshot()['waiting_count'] is None
    assert len(queue.list_review()) == 1
    retention.fail_list = False
    assert discover_once(source, queue, receipt_root=root, retention_client=retention)['complete']
    assert queue.snapshot()['waiting_count'] == 1


def test_missing_upstream_protection_reference_requires_review(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, root, _, item = registered
    queue = LocalQueue(tmp_path / 'queue.sqlite3', require_retention=True)
    source, retention = TwoPageSource(item), RetentionService()
    assert discover_once(source, queue, receipt_root=root, retention_client=retention)['complete']
    retention.hide_reference = True
    assert not discover_once(source, queue, receipt_root=root, retention_client=retention)['complete']
    assert queue.list_waiting() == []
    assert len(queue.list_review()) == 1
    with sqlite3.connect(queue.path) as db:
        assert db.execute('SELECT state FROM retention_requests').fetchone()[0] == 'invalid'


def test_file_recheck_cannot_restore_a_group_while_protection_is_unavailable(registered, tmp_path):
    import pytest
    from pathlib import Path
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, root, receipt, item = registered
    queue = LocalQueue(tmp_path / 'queue.sqlite3', require_retention=True)
    source, retention = TwoPageSource(item), RetentionService()
    assert discover_once(source, queue, receipt_root=root, retention_client=retention)['complete']
    body = Path(receipt['inventory']['body']['path'])
    original = body.read_bytes()
    body.write_bytes(b'broken')
    with pytest.raises(ValueError):
        queue.receive('task-one', item, receipt_root=root)
    queue.retention_unavailable('task-one')
    body.write_bytes(original)
    with pytest.raises(ValueError):
        queue.receive('task-one', item, receipt_root=root)
    assert queue.list_waiting() == []


def test_reserved_reference_is_rechecked_even_before_group_was_persisted(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, root, _, item = registered
    queue = LocalQueue(tmp_path / 'queue.sqlite3', require_retention=True)
    source, retention = TwoPageSource(item), RetentionService()
    intent = queue.prepare_retention('task-one', item)
    response = retention.reserve_retention('task-one', item, consumer_id=intent['consumer_id'],
                                            request_id=intent['request_id'])
    queue.record_retention(intent, response)
    retention.fail_list = True
    assert not discover_once(source, queue, receipt_root=root, retention_client=retention)['complete']
    assert queue.list_waiting() == []


def test_live_summary_waits_for_current_receiver_protection_check(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, root, _, item = registered
    path = tmp_path / 'queue.sqlite3'
    queue = LocalQueue(path, require_retention=True)
    source, retention = TwoPageSource(item), RetentionService()
    assert discover_once(source, queue, receipt_root=root, retention_client=retention)['complete']
    api_queue = LocalQueue(path, require_retention=True, require_live_watcher=True)
    assert api_queue.snapshot()['waiting_count'] is None
    queue.start_receiver()
    assert api_queue.snapshot()['waiting_count'] is None
    assert discover_once(source, queue, receipt_root=root, retention_client=retention)['complete']
    assert api_queue.snapshot()['waiting_count'] == 1
    queue.record_receiver('stopped')
    assert api_queue.snapshot()['waiting_count'] is None


def test_one_task_protection_failure_does_not_hide_another_verified_waiting_group(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, root, _, item = registered
    path = tmp_path / 'queue.sqlite3'
    queue = LocalQueue(path, require_retention=True)
    source, retention = TwoPageSource(item), RetentionService()
    queue.start_receiver()
    assert discover_once(source, queue, receipt_root=root, retention_client=retention)['complete']
    with sqlite3.connect(path) as db:
        db.execute("INSERT INTO groups(task_id,group_id,paper_id,receipt_id,state,record,observed_at) "
                   "SELECT 'other-task','other-group','other-paper','other-receipt','waiting',record,observed_at "
                   "FROM groups WHERE task_id='task-one'")
        db.execute("INSERT INTO retention_requests(task_id,group_id,paper_id,receipt_id,consumer_id,request_id,"
                   "retention_id,state,created_at,updated_at,checked_session) SELECT 'other-task','other-group',"
                   "'other-paper','other-receipt',consumer_id,'other-request','other-retention','accepted',"
                   "created_at,updated_at,checked_session FROM retention_requests WHERE task_id='task-one'")
    queue.retention_unavailable('other-task')
    api_queue = LocalQueue(path, require_retention=True, require_live_watcher=True)
    assert api_queue.snapshot()['waiting_count'] == 1
    assert api_queue.snapshot()['needs_review_count'] == 1


def test_valid_new_reservation_and_reception_are_visible_without_waiting_for_other_groups(registered, tmp_path):
    from local_queue import LocalQueue

    _, root, _, item = registered
    path = tmp_path / 'queue.sqlite3'
    queue = LocalQueue(path, require_retention=True)
    queue.start_receiver()
    retention = RetentionService()
    intent = queue.prepare_retention('task-one', item)
    response = retention.reserve_retention('task-one', item, consumer_id=intent['consumer_id'],
                                            request_id=intent['request_id'])
    intent = queue.record_retention(intent, response)
    queue.receive('task-one', item, receipt_root=root)
    api_queue = LocalQueue(path, require_retention=True, require_live_watcher=True)
    assert api_queue.snapshot()['waiting_count'] is None
    confirmed = retention.confirm_retention('task-one', intent['retention_id'],
                                            consumer_id=intent['consumer_id'], receipt_id=intent['receipt_id'])
    queue.record_retention(intent, confirmed, confirmed=True)
    assert api_queue.snapshot()['waiting_count'] == 1


@pytest.mark.parametrize('status', [409, 503])
def test_unconfirmed_reservation_never_counts_and_records_http_status(registered, tmp_path, status):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    class RejectedConfirmation(RetentionService):
        def __init__(self):
            super().__init__()
            self.reject = True

        def confirm_retention(self, *args, **kwargs):
            if self.reject:
                raise HTTPError('http://127.0.0.1', status, 'synthetic error', {}, None)
            return super().confirm_retention(*args, **kwargs)

    _, root, _, item = registered
    queue = LocalQueue(tmp_path / 'queue.sqlite3', require_retention=True)
    source, retention = TwoPageSource(item), RejectedConfirmation()
    assert not discover_once(source, queue, receipt_root=root, retention_client=retention)['complete']
    assert queue.snapshot()['waiting_count'] is None
    with sqlite3.connect(queue.path) as db:
        assert db.execute('SELECT state,confirm_error FROM retention_requests').fetchone() == ('reserved', f'HTTP_{status}')
    retention.reject = False
    assert discover_once(source, queue, receipt_root=root, retention_client=retention)['complete']
    assert queue.snapshot()['waiting_count'] == 1
    with sqlite3.connect(queue.path) as db:
        assert db.execute('SELECT state,confirm_error FROM retention_requests').fetchone() == ('accepted', None)


def test_rechecking_only_protection_does_not_make_old_unchecked_files_visible(registered, tmp_path):
    from pathlib import Path
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, root, receipt, item = registered
    path = tmp_path / 'queue.sqlite3'
    old_queue = LocalQueue(path, require_retention=True)
    source, retention = TwoPageSource(item), RetentionService()
    assert discover_once(source, old_queue, receipt_root=root, retention_client=retention)['complete']
    Path(receipt['inventory']['body']['path']).write_bytes(b'changed while stopped')
    new_queue = LocalQueue(path, require_retention=True)
    new_queue.start_receiver()
    assert new_queue.reconcile_retention('task-one', retention.list_retentions('task-one')) == 0
    api_queue = LocalQueue(path, require_retention=True, require_live_watcher=True)
    assert api_queue.snapshot()['waiting_count'] is None
    assert not discover_once(source, new_queue, receipt_root=root, retention_client=retention)['complete']
    assert api_queue.snapshot()['waiting_count'] is None


def test_one_shot_scan_cannot_mark_another_receiver_session_verified(registered, tmp_path):
    from local_queue import LocalQueue

    _, root, _, item = registered
    path = tmp_path / 'queue.sqlite3'
    one_shot = LocalQueue(path, require_retention=True)
    intent = one_shot.prepare_retention('task-one', item)
    retention = RetentionService()
    response = retention.reserve_retention('task-one', item, consumer_id=intent['consumer_id'],
                                            request_id=intent['request_id'])
    receiver = LocalQueue(path, require_retention=True)
    receiver.start_receiver()
    one_shot.record_retention(intent, response)
    one_shot.receive('task-one', item, receipt_root=root)
    api_queue = LocalQueue(path, require_retention=True, require_live_watcher=True)
    assert api_queue.snapshot()['waiting_count'] is None


def test_old_or_late_heartbeat_cannot_revive_or_stop_another_receiver(tmp_path):
    from local_queue import LocalQueue

    path = tmp_path / 'queue.sqlite3'
    old = LocalQueue(path, require_retention=True)
    old.start_receiver()
    old_session = old.receiver_session_id
    old.record_receiver('stopped')
    old.receiver_session_id = None
    old.record_receiver('running', expected_session=old_session)
    assert old.snapshot()['receiver_status'] == 'stopped'
    new = LocalQueue(path, require_retention=True)
    new.start_receiver()
    old.record_receiver('stopped', expected_session=old_session)
    assert new.snapshot()['receiver_status'] == 'running'
