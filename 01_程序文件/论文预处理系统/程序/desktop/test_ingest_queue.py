import tempfile
import unittest
from pathlib import Path

from ingest_queue import QueueStore, VersionConflict
from test_ingest_contracts import HASH, spec


class QueueStoreTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'queue.sqlite3'
        self.queue = QueueStore(self.path)

    def test_duplicate_enqueue_and_upload_only_parse_upgrade(self):
        original = spec(parse_required=False, pipeline_id=None)
        task_id = self.queue.enqueue(original)
        self.assertEqual(self.queue.enqueue(original), task_id)
        self.assertTrue(self.queue.transition(task_id, 'upload_waiting', 'uploaded',
                                               remote_id='remote-1'))
        self.assertEqual(self.queue.enqueue(spec(parse_required=True, priority=10)), task_id)
        row = self.queue.get(task_id)
        self.assertEqual(row['status'], 'parse_waiting')
        self.assertEqual(row['remote_id'], 'remote-1')
        self.assertTrue(row['parse_required'])
        self.assertEqual(row['pipeline_id'], 'pipe-1')
        self.queue.enqueue(original)
        self.assertEqual(self.queue.get(task_id)['status'], 'parse_waiting')

    def test_conflicting_version_policy_and_pipeline_are_rejected(self):
        self.queue.enqueue(spec())
        for change in ({'content_sha256': 'b' * 64}, {'policy_key': 'finance'},
                       {'pipeline_id': 'another-pipeline'}):
            with self.subTest(change=change), self.assertRaises(VersionConflict):
                self.queue.enqueue(spec(**change))

    def test_claim_is_exclusive_and_expired_intent_requires_reconciliation(self):
        task_id = self.queue.enqueue(spec())
        second = QueueStore(self.path)
        first_claim = self.queue.claim('upload', 'worker-a', 1, now=100, lease_seconds=10)
        self.assertEqual([row['id'] for row in first_claim], [task_id])
        self.assertEqual(first_claim[0]['status'], 'upload_intent')
        self.assertEqual(second.claim('upload', 'worker-b', 1, now=101, lease_seconds=10), [])
        self.assertEqual(second.claim('upload', 'worker-b', 1, now=200, lease_seconds=10), [])
        self.assertEqual(second.get(task_id)['status'], 'upload_intent')

    def test_claim_fairness_preserves_auto_progress(self):
        for i in range(8):
            self.queue.enqueue(spec(source_id=f'manual-{i}', priority=10))
            self.queue.enqueue(spec(source_id=f'auto-{i}', priority=0))
        priorities = [self.queue.claim('upload', 'worker', 1, now=100+i,
                                       lease_seconds=30)[0]['priority'] for i in range(8)]
        self.assertEqual(priorities, [10, 10, 10, 0, 10, 10, 10, 0])

    def test_manual_request_upgrades_unstarted_priority(self):
        task_id = self.queue.enqueue(spec(priority=0))
        self.assertEqual(self.queue.enqueue(spec(priority=10)), task_id)
        self.assertEqual(self.queue.get(task_id)['priority'], 10)

    def test_education_image_waits_for_admitted_primary(self):
        primary = self.queue.enqueue(spec())
        self.queue.enqueue(spec(dataset_id='images', role='image:assets/picture.png',
                                lane='image', pipeline_id=None, parse_required=False,
                                remote_name='picture.png'))
        self.assertEqual(self.queue.claim('image', 'worker', 1, now=100,
                                          lease_seconds=10), [])
        self.queue.transition(primary, 'upload_waiting', 'accepted', remote_id='remote-1')
        claimed = self.queue.claim('image', 'worker', 1, now=101, lease_seconds=10)
        self.assertEqual(len(claimed), 1)

    def test_review_request_preserves_terminal_attempt(self):
        task_id = self.queue.enqueue(spec())
        self.assertTrue(self.queue.transition(task_id, 'upload_waiting', 'quality_hold',
                                               remote_id='remote-1'))
        with self.assertRaises(ValueError):
            self.queue.request_review(task_id, '')
        self.queue.request_review(task_id, '证据字段已修复')
        self.assertEqual(self.queue.get(task_id)['status'], 'parse_waiting')
        self.assertEqual(self.queue.get(task_id)['remote_id'], 'remote-1')
        self.assertTrue(any(row['operation'] == 'review_requested'
                            for row in self.queue.attempts(task_id)))


if __name__ == '__main__':
    unittest.main()
