import time

import pytest

from ingest_queue import QueueStore
from ingest_scheduler import WorkPool
from test_ingest_contracts import spec
from test_ingest_scheduler import FakeGateway


@pytest.mark.parametrize('operation', ['parse', 'post'])
def test_completion_cannot_overwrite_a_concurrent_queue_change(tmp_path, operation):
    class ConcurrentQueue(QueueStore):
        raced = False

        def transition(self, task_id, expected, target, **fields):
            is_completion = (target == 'submitted' if operation == 'parse' else
                             target == 'accepted' and fields.get('post_status') == 'done')
            if not self.raced and is_completion:
                self.raced = True
                # Model a second writer acting after the worker's snapshot check
                # but before its state update commits.
                if operation == 'parse':
                    super().transition(task_id, expected, 'technical_review')
                    self.request_review(task_id, 'concurrent retry')
                    self.claim('parse', 'next-worker', 1, time.time(), 120)
                else:
                    super().transition(task_id, 'accepted', 'accepted',
                                       post_status='needs_review', error='newer source verification')
            return super().transition(task_id, expected, target, **fields)

    queue = ConcurrentQueue(tmp_path / 'queue.db')
    initial = {'status': 'parse_waiting', 'remote_id': 'doc'} if operation == 'parse' else {
        'status': 'accepted', 'remote_id': 'doc', 'post_status': 'pending'}
    key = queue.enqueue(spec(), initial)
    gateway = FakeGateway()
    gateway.docs['doc'] = {'id': 'doc', 'name': spec().remote_name,
                           'pipeline_id': 'pipe-1',
                           'run': 'UNSTART' if operation == 'parse' else 'DONE'}
    pool = WorkPool(queue, lambda: gateway, lambda *_: 'accepted',
                    on_accepted_for=lambda *_: None)
    try:
        for _ in range(40):
            pool.tick()
            if queue.raced:
                break
            time.sleep(.01)
        assert queue.raced
        task = queue.get(key)
        if operation == 'parse':
            assert task['execution_attempt'] == 2
            assert task['status'] == 'parse_intent'
            assert len(gateway.parse_calls) == 1
        else:
            assert task['post_status'] == 'needs_review'
            assert task['error'] == 'newer source verification'
        assert not [row for row in queue.attempts(key)
                    if row['operation'] == operation and row['result'] in ('submitted', 'done')]
    finally:
        pool.close()
