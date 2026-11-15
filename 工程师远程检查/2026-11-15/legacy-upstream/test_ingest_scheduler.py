import tempfile
import time
import unittest
from pathlib import Path
from threading import Event

from ingest_queue import QueueStore
from ingest_scheduler import WorkPool
from test_ingest_contracts import spec


class FakeGateway:
    def __init__(self):
        self.docs = {}
        self.upload_calls = []
        self.parse_calls = []
        self.block_image = None

    def upload(self, task):
        if task['lane'] == 'image' and self.block_image:
            self.block_image.wait(2)
        remote_id = 'remote-' + task['source_id']
        self.upload_calls.append(task['source_id'])
        self.docs[remote_id] = {'id': remote_id, 'name': task['remote_name'],
                                'pipeline_id': task['pipeline_id'], 'run': 'UNSTART'}
        return remote_id

    def parse(self, task):
        self.parse_calls.append(task['source_id'])
        self.docs[task['remote_id']]['run'] = 'RUNNING'

    def lookup(self, task):
        row = self.docs.get(task.get('remote_id'))
        return [row.copy()] if row else []


class WorkPoolTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.queue = QueueStore(Path(self.temp.name) / 'tasks.sqlite3')
        self.gateway = FakeGateway()
        self.pool = None

    def tearDown(self):
        if self.pool:
            self.pool.close()

    def enqueue(self, source, **changes):
        return self.queue.enqueue(spec(source_id=source, remote_name=f'{source}_' + 'a'*16 + '.md',
                                       **changes))

    def drive(self, condition, limit=60):
        for _ in range(limit):
            self.pool.tick()
            if condition():
                return
            time.sleep(0.01)
        self.fail('scheduler did not reach expected state')

    def test_quality_terminals_release_parse_slots_and_refill(self):
        ids = [self.enqueue(f'p{i}') for i in range(5)]
        self.pool = WorkPool(self.queue, lambda: self.gateway,
                             lambda task, remote: {'p0': 'quality_hold',
                                                   'p1': 'quality_reject'}.get(task['source_id'], 'accepted'),
                             upload_limit=5, parse_limit=3)
        self.drive(lambda: len(self.gateway.parse_calls) == 3)
        self.assertEqual(len(self.gateway.parse_calls), 3)
        self.gateway.docs['remote-p0']['run'] = 'FAIL'
        self.gateway.docs['remote-p1']['run'] = 'FAIL'
        self.drive(lambda: len(self.gateway.parse_calls) == 5)
        self.assertEqual(self.queue.get(ids[0])['status'], 'quality_hold')
        self.assertEqual(self.queue.get(ids[1])['status'], 'quality_reject')

    def test_http_rate_limit_pauses_only_the_affected_service_writes(self):
        gateway=self.gateway
        original=gateway.upload
        class RateLimited(Exception):
            status_code=429
        def upload(task):
            if task['source_id']=='limited':
                gateway.upload_calls.append('limited')
                raise RateLimited()
            return original(task)
        gateway.upload=upload
        first=self.enqueue('limited')
        waiting=self.enqueue('same-service')
        self.enqueue('other-service',service_url='http://other:9380')
        self.pool=WorkPool(self.queue,lambda:gateway,lambda *_:'accepted',upload_limit=1,parse_limit=1)
        self.drive(lambda:'other-service' in gateway.parse_calls)
        self.assertEqual(self.queue.get(first)['status'],'upload_unknown')
        self.assertEqual(self.queue.get(waiting)['status'],'upload_waiting')
        self.assertNotIn('same-service',gateway.upload_calls)

    def test_blocked_image_upload_does_not_block_paper_parse(self):
        self.gateway.block_image = Event()
        self.enqueue('image-1', lane='image', role='original_image',
                     pipeline_id=None, parse_required=False)
        self.enqueue('paper-1')
        self.pool = WorkPool(self.queue, lambda: self.gateway,
                             lambda task, remote: 'accepted', image_limit=1)
        try:
            self.drive(lambda: 'paper-1' in self.gateway.parse_calls)
        finally:
            self.gateway.block_image.set()

    def test_channel_limit_skips_blocked_channel(self):
        self.enqueue('blocked', channel_key='limited')
        self.enqueue('open', channel_key='open')
        self.pool = WorkPool(self.queue, lambda: self.gateway,
                             lambda task, remote: 'accepted',
                             channel_limits={'limited': 0, 'open': 2})
        self.drive(lambda: 'open' in self.gateway.parse_calls)
        self.assertNotIn('blocked', self.gateway.parse_calls)

    def test_arrearage_pauses_only_affected_model_channel(self):
        self.enqueue('first-bad', channel_key='bad')
        self.enqueue('second-bad', channel_key='bad')
        self.enqueue('good', channel_key='good')
        self.pool = WorkPool(self.queue, lambda: self.gateway,
                             lambda task, remote: 'technical_review', parse_limit=1)
        self.drive(lambda: 'first-bad' in self.gateway.parse_calls)
        self.gateway.docs['remote-first-bad']['run'] = 'FAIL'
        self.gateway.docs['remote-first-bad']['progress_msg'] = 'Arrearage'
        self.drive(lambda: 'good' in self.gateway.parse_calls)
        self.assertNotIn('second-bad', self.gateway.parse_calls)

    def test_expired_parse_intent_is_reconciled_before_retry(self):
        task_id = self.enqueue('old')
        self.assertTrue(self.queue.transition(task_id, 'upload_waiting', 'parse_intent',
                                               remote_id='remote-old', lease_until=0,
                                               execution_begin_at='2026-09-27T01:00:00'))
        self.gateway.docs['remote-old'] = {'id': 'remote-old',
            'name': self.queue.get(task_id)['remote_name'], 'pipeline_id': 'pipe-1',
            'run': 'RUNNING', 'process_begin_at':'2026-09-27T01:00:00'}
        self.pool = WorkPool(self.queue, lambda: self.gateway,
                             lambda task, remote: 'accepted')
        self.drive(lambda: self.queue.get(task_id)['status'] in ('submitted', 'running'))
        self.assertEqual(self.gateway.parse_calls, [])

    def test_failed_reconciliation_stays_uncertain_without_repeat(self):
        task_id = self.enqueue('lost')
        self.queue.transition(task_id, 'upload_waiting', 'upload_intent', lease_until=0)
        calls = []

        def unavailable(task):
            calls.append(task['id'])
            raise TimeoutError('remote status unavailable')

        self.gateway.lookup = unavailable
        self.pool = WorkPool(self.queue, lambda: self.gateway,
                             lambda task, remote: 'accepted')
        self.drive(lambda: self.queue.get(task_id)['status'] == 'upload_unknown')
        for _ in range(3):
            self.pool.tick()
        self.assertEqual(self.gateway.upload_calls, [])
        self.assertEqual(len(calls), 1)

    def test_reconciliation_result_query_error_does_not_spin(self):
        task_id = self.enqueue('lost-parse')
        self.queue.transition(task_id, 'upload_waiting', 'parse_intent',
                              remote_id='remote-lost-parse', lease_until=0,
                              execution_begin_at='2026-09-27T01:00:00')
        self.gateway.docs['remote-lost-parse'] = {'id': 'remote-lost-parse',
            'name': self.queue.get(task_id)['remote_name'],
            'pipeline_id': 'pipe-1', 'run': 'RUNNING', 'process_begin_at':'2026-09-27T01:00:00'}
        original_lookup = self.gateway.lookup
        calls = []

        def flaky_lookup(task):
            calls.append(task['id'])
            if len(calls) > 1:
                raise TimeoutError('second identity query failed')
            return original_lookup(task)

        self.gateway.lookup = flaky_lookup
        self.pool = WorkPool(self.queue, lambda: self.gateway,
                             lambda task, remote: 'accepted')
        self.drive(lambda: self.queue.get(task_id)['status'] == 'parse_unknown')
        self.assertEqual(self.gateway.parse_calls, [])
        self.assertEqual(len(calls), 2)

    def test_blocked_postprocessing_does_not_block_next_parse(self):
        first = self.enqueue('first')
        self.enqueue('second')
        blocked = Event()
        entered = Event()

        def postprocess(task, remote):
            entered.set()
            blocked.wait(2)

        self.pool = WorkPool(self.queue, lambda: self.gateway,
                             lambda task, remote: 'accepted', on_accepted_for=postprocess,
                             parse_limit=1)
        try:
            self.drive(lambda: 'first' in self.gateway.parse_calls)
            self.gateway.docs['remote-first']['run'] = 'DONE'
            self.drive(lambda: entered.is_set())
            self.drive(lambda: 'second' in self.gateway.parse_calls)
            self.assertEqual(self.queue.get(first)['status'], 'accepted')
        finally:
            blocked.set()


if __name__ == '__main__':
    unittest.main()
