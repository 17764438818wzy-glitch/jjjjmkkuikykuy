import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from auto_ragflow import handoff
from auto_pipeline import AutoProcessor
from ingest_queue import QueueStore
from ingest_workbench import enqueue_workbench_items
from test_ingest_workbench import prepared


class EntryPointTest(unittest.TestCase):
    def test_legacy_lost_parse_ack_is_not_submitted_again_on_unstart(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            store,item,config=prepared(root)
            item['uploads']={'http://localhost:9380|finance-md':{
                'status':'uploaded','route':'paper','document_id':'legacy-1',
                'content_hash':item['reviewed_hash'],'remote_name':'legacy.md',
                'parse_status':'unknown'}}
            queue=QueueStore(root/'queue.sqlite3')
            config['_receipt_lookup']=lambda *_:[{'id':'legacy-1','name':'legacy.md',
                                                  'run':'UNSTART','pipeline_id':'finance-pipe'}]
            ids=enqueue_workbench_items(store,'batch-1',[item['id']],None,
                'upload_and_parse',queue,config,'http://localhost:9380')
            primary=queue.find_identity('http://localhost:9380','finance-md',
                                        item['paper_id'],'paper_primary')
            self.assertIn(primary['id'],ids)
            self.assertEqual(primary['status'],'parse_unknown')
            self.assertEqual(primary['remote_id'],'legacy-1')

    def test_saved_approved_item_is_enqueued_before_batch_end(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, item, config = prepared(root)
            store.root = root
            processor = AutoProcessor.__new__(AutoProcessor)
            processor.store = store
            processor.config = dict(config, work_pool_enabled=True,
                                    auto_upload=True, auto_parse=True)
            class Client:
                base_url = 'http://localhost:9380'
                def datasets(self):
                    return config['datasets']
            processor.rag_client = Client()
            with patch('auto_ragflow.handoff') as queued:
                processor._enqueue_approved_after_save('batch-1', [item],
                                                       lambda *_: None)
                self.assertEqual(queued.call_count, 1)
                self.assertEqual(queued.call_args.kwargs['item_ids'], [item['id']])
                self.assertTrue(queued.call_args.kwargs['upload_config']['work_pool_enabled'])

    def test_upload_only_then_parse_reuses_one_task_and_remote_id(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, item, config = prepared(root)
            queue = QueueStore(root / 'queue.sqlite3')
            first = enqueue_workbench_items(store, 'batch-1', [item['id']], None,
                'upload_only', queue, dict(config, manual_request=True),
                'http://localhost:9380')
            primary = queue.find_identity('http://localhost:9380', 'finance-md',
                                          item['paper_id'], 'paper_primary')
            self.assertIn(primary['id'], first)
            self.assertEqual(len(first), len(set(first)))
            self.assertFalse(primary['parse_required'])
            queue.transition(primary['id'], 'upload_waiting', 'uploaded', remote_id='remote-1')
            second = enqueue_workbench_items(store, 'batch-1', [item['id']], None,
                'upload_and_parse', queue, config, 'http://localhost:9380')
            self.assertIn(primary['id'], second)
            after = queue.get(primary['id'])
            self.assertEqual(after['remote_id'], 'remote-1')
            self.assertEqual(after['status'], 'parse_waiting')

    def test_queue_enabled_handoff_does_not_write_directly_to_ragflow(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, item, config = prepared(root)
            store.root = root

            class Client:
                base_url = 'http://localhost:9380'
                def datasets(self):
                    return config['datasets']
                def upload(self, *args, **kwargs):
                    raise AssertionError('queue handoff must not upload directly')
                def parse(self, *args, **kwargs):
                    raise AssertionError('queue handoff must not parse directly')

            handoff(store, 'batch-1', Client(), config['dataset_map'],
                    auto_parse=True, upload_config=dict(config,
                    work_pool_enabled=True, work_pool_auto_start=False,
                    work_pool_queue=str(root / 'queue.sqlite3')))
            queue = QueueStore(root / 'queue.sqlite3')
            primary = queue.find_identity('http://localhost:9380', 'finance-md',
                                          item['paper_id'], 'paper_primary')
            self.assertTrue(primary['parse_required'])
            self.assertEqual(primary['status'], 'upload_waiting')


if __name__ == '__main__':
    unittest.main()
