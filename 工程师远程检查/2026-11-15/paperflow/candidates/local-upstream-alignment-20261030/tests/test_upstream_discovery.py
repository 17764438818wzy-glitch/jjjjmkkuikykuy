from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from test_registered_input import registered


class TwoPageSource:
    def __init__(self, item):
        self.item = item
        self.fail = False
        self.empty = False
        self.change_on_second_pass = False
        self.task_scans = 0
        self.revision = 1

    def list_tasks(self, page):
        if page == 1:
            self.task_scans += 1
            if self.empty:
                return {'items': [], 'has_next': False, 'page': 1, 'page_size': 1, 'total': 0}
            return {'items': [{'task_id': 'task-one', 'control_revision': self.revision}],
                    'has_next': False, 'page': 1, 'page_size': 1, 'total': 1}
        raise AssertionError('unexpected task page')

    def list_groups(self, task_id, page):
        assert task_id == 'task-one'
        if self.fail:
            raise OSError('synthetic source unavailable')
        good = dict(self.item)
        pending = {'group_id': 'unregistered-group', 'status': 'passed',
                   'delivery_complete': True, 'registration_status': 'failed'}
        rows = [good, pending]
        if self.change_on_second_pass and self.task_scans % 2 == 0:
            rows = [pending, good]
            rows[0] = {**rows[0], 'registration_status': 'registered'}
        return {'task_id': task_id, 'control_revision': 1, 'page': page,
                'page_size': 1, 'list_total': 2, 'items': [rows[page - 1]]}


def test_stable_paginated_scan_accepts_only_registered_groups_once(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    source = TwoPageSource(item)
    assert queue.snapshot()['waiting_count'] is None
    first = discover_once(source, queue, receipt_root=receipt_root)
    assert first['complete'] is True
    assert queue.snapshot()['waiting_count'] == 1
    assert discover_once(source, queue, receipt_root=receipt_root)['complete'] is True
    assert len(queue.list_waiting()) == 1


def test_partial_scan_keeps_old_records_but_does_not_report_known_count(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    source = TwoPageSource(item)
    assert discover_once(source, queue, receipt_root=receipt_root)['complete'] is True
    source.fail = True
    assert discover_once(source, queue, receipt_root=receipt_root)['complete'] is False
    assert len(queue.list_waiting()) == 1
    snapshot = queue.snapshot()
    assert snapshot['waiting_available'] is False
    assert snapshot['waiting_count'] is None


def test_concurrent_page_changes_are_not_claimed_as_a_complete_scan(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    source = TwoPageSource(item)
    source.change_on_second_pass = True
    assert discover_once(source, queue, receipt_root=receipt_root)['complete'] is False
    assert queue.snapshot()['waiting_count'] is None
    assert len(queue.list_waiting()) == 1


def test_deleted_delivery_is_held_for_review_instead_of_waiting(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, receipt_root, receipt, item = registered
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    source = TwoPageSource(item)
    assert discover_once(source, queue, receipt_root=receipt_root)['complete'] is True
    attachment = next(row for row in receipt['items'] if row['kind'] == 'attachment')
    Path(attachment['path']).unlink()
    assert discover_once(source, queue, receipt_root=receipt_root)['complete'] is False
    assert queue.list_waiting() == []
    assert len(queue.list_review()) == 1
    assert queue.snapshot()['waiting_count'] is None


def test_disappeared_upstream_task_does_not_leave_a_false_waiting_group(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    source = TwoPageSource(item)
    assert discover_once(source, queue, receipt_root=receipt_root)['complete'] is True
    source.empty = True
    assert discover_once(source, queue, receipt_root=receipt_root)['complete'] is False
    assert queue.list_waiting() == []
    assert len(queue.list_review()) == 1
    assert queue.snapshot()['waiting_count'] is None
    discover_once(source, queue, receipt_root=receipt_root)
    assert queue.snapshot()['waiting_available'] is False
    assert queue.snapshot()['waiting_count'] is None


def test_inconsistent_task_page_cannot_be_reported_as_an_empty_complete_scan(tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    class InconsistentSource:
        def list_tasks(self, page):
            return {'items': [], 'total': 1, 'page': 1, 'page_size': 1, 'has_next': False}

    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    assert discover_once(InconsistentSource(), queue, receipt_root=tmp_path)['complete'] is False
    assert queue.snapshot()['waiting_count'] is None


def test_missing_receipt_root_is_not_a_known_empty_queue(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, _, _, item = registered
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    source = TwoPageSource(item)
    source.empty = True
    assert discover_once(source, queue, receipt_root=tmp_path / 'missing')['complete'] is False
    assert queue.snapshot()['waiting_count'] is None


def test_unbound_empty_upstream_is_not_claimed_as_a_known_zero(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    source = TwoPageSource(item)
    source.empty = True
    assert discover_once(source, queue, receipt_root=receipt_root)['complete'] is False
    assert queue.snapshot()['waiting_count'] is None


def test_later_upstream_failure_does_not_prevent_receiving_an_earlier_group(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')

    class PartiallyAvailableSource:
        def list_tasks(self, page):
            return {'items': [{'task_id': 'task-one', 'control_revision': 1},
                              {'task_id': 'task-two', 'control_revision': 1}],
                    'has_next': False, 'page': 1, 'page_size': 2, 'total': 2}

        def list_groups(self, task_id, page):
            if task_id == 'task-two':
                raise OSError('synthetic second task unavailable')
            return {'task_id': task_id, 'control_revision': 1, 'page': 1,
                    'page_size': 1, 'list_total': 1, 'items': [item]}

    result = discover_once(PartiallyAvailableSource(), queue, receipt_root=receipt_root)
    assert result['complete'] is False
    assert len(queue.list_waiting()) == 1
    assert queue.snapshot()['waiting_count'] is None


def test_source_change_during_file_validation_prevents_complete_snapshot(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    source = TwoPageSource(item)
    real_receive = queue.receive

    def receive_and_change(*args, **kwargs):
        result = real_receive(*args, **kwargs)
        source.revision = 2
        return result

    queue.receive = receive_and_change
    assert discover_once(source, queue, receipt_root=receipt_root)['complete'] is False
    assert queue.snapshot()['waiting_count'] is None
    assert len(queue.list_waiting()) == 1


def test_only_one_discovery_can_reconcile_the_same_queue_at_a_time(registered, tmp_path):
    from local_queue import LocalQueue
    from upstream_discovery import discover_once

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    entered, release = Event(), Event()

    class SlowSource(TwoPageSource):
        def list_tasks(self, page):
            if not entered.is_set():
                entered.set()
                assert release.wait(5)
            return super().list_tasks(page)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(discover_once, SlowSource(item), queue, receipt_root=receipt_root)
        assert entered.wait(5)
        try:
            second = pool.submit(discover_once, TwoPageSource(item), queue, receipt_root=receipt_root)
            assert second.result(timeout=3)['reason'] == 'scan_in_progress'
        finally:
            release.set()
        assert first.result(timeout=5)['complete'] is True
    assert len(queue.list_waiting()) == 1


def test_expensive_verification_does_not_prevent_discovery_of_later_pages(tmp_path):
    from upstream_discovery import discover_once

    class Source:
        revision = 1

        def list_tasks(self, page):
            return {'items': [{'task_id': 'task-one', 'control_revision': self.revision}],
                    'has_next': False, 'page': 1, 'page_size': 1, 'total': 1}

        def list_groups(self, task_id, page):
            row = {'group_id': f'group-{page}', 'status': 'passed',
                   'registration_status': 'registered', 'delivery_complete': True,
                   'receipt_id': f'receipt-{page}'}
            return {'task_id': task_id, 'control_revision': self.revision, 'page': page,
                    'page_size': 1, 'list_total': 2, 'items': [row]}

    class Queue:
        path = tmp_path / 'queue.sqlite3'
        received = []

        def list_waiting(self):
            return []

        def record_scan(self, **kwargs):
            pass

        def receive(self, task_id, item, *, receipt_root):
            self.received.append(item['group_id'])
            source.revision += 1

        def reconcile_visible(self, visible, *, candidates):
            return []

    source, queue = Source(), Queue()
    result = discover_once(source, queue, receipt_root=tmp_path)
    assert queue.received == ['group-1', 'group-2']
    assert result['complete'] is False


def test_changed_page_revision_still_offers_later_groups_for_individual_verification(tmp_path):
    from upstream_discovery import discover_once

    class Source:
        def list_tasks(self, page):
            return {'items': [{'task_id': 'task-one', 'control_revision': 1}],
                    'has_next': False, 'page': 1, 'page_size': 1, 'total': 1}

        def list_groups(self, task_id, page):
            row = {'group_id': f'group-{page}', 'status': 'passed',
                   'registration_status': 'registered', 'delivery_complete': True,
                   'receipt_id': f'receipt-{page}'}
            return {'task_id': task_id, 'control_revision': page, 'page': page,
                    'page_size': 1, 'list_total': 2, 'items': [row]}

    class Queue:
        path = tmp_path / 'queue.sqlite3'
        received = []

        def list_waiting(self):
            return []

        def record_scan(self, **kwargs):
            pass

        def receive(self, task_id, item, *, receipt_root):
            self.received.append(item['group_id'])

    queue = Queue()
    result = discover_once(Source(), queue, receipt_root=tmp_path)
    assert queue.received == ['group-1', 'group-2']
    assert result['complete'] is False


def test_cancellation_does_not_start_verifying_already_enumerated_groups(tmp_path):
    from upstream_discovery import discover_once

    class Source:
        def list_tasks(self, page):
            return {'items': [{'task_id': 'task-one', 'control_revision': 1}],
                    'has_next': False, 'page': 1, 'page_size': 1, 'total': 1}

        def list_groups(self, task_id, page):
            if page == 2:
                raise KeyboardInterrupt()
            row = {'group_id': 'group-1', 'status': 'passed',
                   'registration_status': 'registered', 'delivery_complete': True}
            return {'task_id': task_id, 'control_revision': 1, 'page': page,
                    'page_size': 1, 'list_total': 2, 'items': [row]}

    class Queue:
        path = tmp_path / 'queue.sqlite3'
        received = []

        def list_waiting(self):
            return []

        def record_scan(self, **kwargs):
            pass

        def receive(self, task_id, item, *, receipt_root):
            self.received.append(item['group_id'])

    queue = Queue()
    with pytest.raises(KeyboardInterrupt):
        discover_once(Source(), queue, receipt_root=tmp_path)
    assert queue.received == []


def test_failed_later_task_list_page_still_receives_previously_found_tasks(tmp_path):
    from upstream_discovery import discover_once

    class Source:
        def list_tasks(self, page):
            if page == 2:
                raise OSError('synthetic task list page unavailable')
            return {'items': [{'task_id': 'task-one', 'control_revision': 1}],
                    'has_next': True, 'page': 1, 'page_size': 1, 'total': 2}

        def list_groups(self, task_id, page):
            row = {'group_id': 'group-1', 'status': 'passed',
                   'registration_status': 'registered', 'delivery_complete': True}
            return {'task_id': task_id, 'control_revision': 1, 'page': page,
                    'page_size': 1, 'list_total': 1, 'items': [row]}

    class Queue:
        path = tmp_path / 'queue.sqlite3'
        received = []

        def list_waiting(self):
            return []

        def record_scan(self, **kwargs):
            pass

        def receive(self, task_id, item, *, receipt_root):
            self.received.append((task_id, item['group_id']))

    queue = Queue()
    result = discover_once(Source(), queue, receipt_root=tmp_path)
    assert queue.received == [('task-one', 'group-1')]
    assert result['complete'] is False


def test_polling_is_explicit_and_stops_without_starting_another_scan(monkeypatch, tmp_path):
    import upstream_discovery

    stop = Event()
    calls = []

    def scan(source, queue, *, receipt_root, stop_event=None):
        calls.append(receipt_root)
        if len(calls) == 2:
            stop.set()

    monkeypatch.setattr(upstream_discovery, 'discover_once', scan)
    upstream_discovery.poll_until_stopped(object(), object(), receipt_root=tmp_path,
                                         interval_seconds=0.01, stop_event=stop)
    assert calls == [tmp_path, tmp_path]


def test_polling_stop_during_reception_does_not_start_later_groups(tmp_path):
    from upstream_discovery import poll_until_stopped

    stop = Event()

    class Source:
        def list_tasks(self, page):
            return {'items': [{'task_id': 'task-one', 'control_revision': 1}],
                    'has_next': False, 'page': 1, 'page_size': 1, 'total': 1}

        def list_groups(self, task_id, page):
            row = {'group_id': f'group-{page}', 'status': 'passed',
                   'registration_status': 'registered', 'delivery_complete': True,
                   'receipt_id': f'receipt-{page}'}
            return {'task_id': task_id, 'control_revision': 1, 'page': page,
                    'page_size': 1, 'list_total': 2, 'items': [row]}

    class Queue:
        path = tmp_path / 'queue.sqlite3'
        received = []

        def list_waiting(self):
            return []

        def record_scan(self, **kwargs):
            pass

        def receive(self, task_id, item, *, receipt_root):
            self.received.append(item['group_id'])
            stop.set()

        def reconcile_visible(self, visible, *, candidates):
            return []

    queue = Queue()
    poll_until_stopped(Source(), queue, receipt_root=tmp_path,
                       interval_seconds=0.01, stop_event=stop)
    assert queue.received == ['group-1']
