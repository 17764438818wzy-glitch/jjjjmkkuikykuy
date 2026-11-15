import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch
import education_stream_ingest as esi

from education_stream_ingest import (
    ArrearageRecoveryHalted, Stream,
    classify_parse_failure, education_remote_name, exact_document, ready_items,
    recovery_poll_delay, write_json,
)


class ReadyItemsTest(unittest.TestCase):
    def test_recovery_poll_delay_does_not_back_off_for_ambiguous_failures(self):
        self.assertEqual(recovery_poll_delay(0), 15)
        self.assertEqual(recovery_poll_delay(2), 15)
        self.assertEqual(recovery_poll_delay(3), 15)
        self.assertEqual(recovery_poll_delay(10), 15)

    def test_receipt_write_retries_transient_windows_replace_denial(self):
        import os
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'receipts.json'
            path.write_text('{"old": true}', encoding='utf-8')
            replace = os.replace
            attempts = []

            def temporarily_blocked(source, destination):
                attempts.append((source, destination))
                if len(attempts) == 1:
                    raise PermissionError('file temporarily open')
                replace(source, destination)

            with patch('education_stream_ingest.os.replace', side_effect=temporarily_blocked), \
                    patch('education_stream_ingest.time.sleep'):
                write_json(path, {'new': True})
            self.assertEqual(len(attempts), 2)
            self.assertEqual(json.loads(path.read_text(encoding='utf-8')), {'new': True})

    def test_parse_failure_classification_keeps_unresolved_separate_from_quality(self):
        self.assertEqual(classify_parse_failure('0 tasks are ahead in the queue...'), 'outcome_unresolved')
        self.assertEqual(classify_parse_failure('1 tasks are ahead in the queue...'), 'outcome_unresolved')
        self.assertEqual(classify_parse_failure('Task has been received.\nArrearage'), 'embedding_arrearage')
        self.assertEqual(classify_parse_failure('EDUCATION_HOLD|{"education_processing_issue":"annotation_invalid"}'),
                         'annotation_invalid')
        self.assertEqual(classify_parse_failure('EDUCATION_HOLD|{"education_processing_issue":"input_invalid"}'),
                         'input_invalid')

    def test_three_consecutive_ambiguous_failures_are_recorded_without_backoff(self):
        class Client:
            def documents(self, dataset_id, document_id):
                return [{'id': document_id, 'name': document_id + '.md',
                         'run': 'FAIL', 'chunk_count': 0,
                         'progress_msg': '0 tasks are ahead in the queue...'}]
        stream = Stream.__new__(Stream)
        stream.client = Client()
        stream.paper_dataset = 'education-md'
        stream.state = {'primary': {str(i): {'status': 'submitted',
            'document_id': str(i), 'ragflow_document_id': str(i),
            'upload_name': str(i) + '.md'} for i in range(3)}, 'errors': {}}
        stream.save = lambda: None
        stream.poll_primary()
        self.assertEqual([row['failure_kind'] for row in stream.state['primary'].values()],
                         ['outcome_unresolved'] * 3)
        self.assertEqual(stream.state['queue_failure_streak'], 0)

    def test_embedding_arrearage_halts_immediately_after_recording_failure(self):
        class Client:
            def documents(self, dataset_id, document_id):
                return [{'id': document_id, 'name': 'paper.md', 'run': 'FAIL',
                         'chunk_count': 0, 'progress_msg': 'Arrearage'}]
        stream = Stream.__new__(Stream)
        stream.client = Client()
        stream.paper_dataset = 'education-md'
        stream.state = {'primary': {'paper-1': {'status': 'submitted',
            'document_id': 'source-1', 'ragflow_document_id': 'remote-1',
            'upload_name': 'paper.md'}}, 'errors': {}}
        stream.save = lambda: None
        with self.assertRaises(ArrearageRecoveryHalted):
            stream.poll_primary()
        self.assertEqual(stream.state['primary']['paper-1']['failure_kind'],
                         'embedding_arrearage')

    def test_done_document_writes_idempotent_method_graph_with_source_chunk(self):
        paper_id = 'paper_' + 'a' * 24
        class Client:
            calls = 0
            def documents(self, dataset_id, document_id):
                self.calls += 1
                return [{'id': document_id, 'name': 'paper.md', 'run': 'DONE',
                         'meta_fields': {'education_paper_id': paper_id,
                             'education_admission': '入库',
                             'education_method_details': json.dumps([{'name': '问卷调查法'}]),
                             'education_research_steps': json.dumps([{'name': '问卷调查',
                                 'status': '实际执行', 'quote': '发放问卷并分析学生学习情况'}])}}]
            def request(self, method, endpoint):
                return {'chunks': [{'id': 'chunk-1', 'available': True,
                    'document_id': 'remote-1', 'important_keywords': ['role=body'],
                    'content': '本研究发放问卷并分析学生学习情况。'}], 'total': 1}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / 'canonical_packages' / 'source-1'
            package.mkdir(parents=True)
            (package / 'ingest.md').write_text('正文', encoding='utf-8')
            stream = Stream.__new__(Stream)
            stream.client = Client()
            stream.paper_dataset = 'education-md'
            stream.private_dir = root
            stream.state = {'primary': {paper_id: {'status': 'done',
                'document_id': 'source-1', 'ragflow_document_id': 'remote-1',
                'upload_name': 'paper.md'}}, 'errors': {}}
            stream.save = lambda: None
            item = {'id': 'source-1', 'paper_id': paper_id}
            stream.method_graph(item, package)
            result = json.loads((root / 'method_graphs' / (paper_id + '.json')).read_text(encoding='utf-8'))
            self.assertEqual(result['edges'][0]['source_chunk_ids'], ['chunk-1'])
            self.assertEqual(stream.state['primary'][paper_id]['method_graph_status'], 'done')
            stream.method_graph(item, package)
            self.assertEqual(stream.client.calls, 1)

    def test_done_document_writes_direct_tree_with_real_chunk_source(self):
        paper_id = 'paper_' + 'a' * 24
        class Client:
            def documents(self, dataset_id, document_id):
                return [{'id': document_id, 'name': 'paper.md', 'run': 'DONE',
                         'meta_fields': {'education_paper_id': paper_id,
                                         'education_admission': '入库'}}]
            def request(self, method, endpoint):
                return {'chunks': [{'id': 'chunk-1', 'available': True,
                    'document_id': 'remote-1',
                    'important_keywords': ['role=body', 'section_id=section_1111aaaa'],
                    'content': '# 第1章 绪论\n研究背景。'}], 'total': 1}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / 'canonical_packages' / 'source-1'
            package.mkdir(parents=True)
            (package / 'ingest.md').write_text('# 第1章 绪论\n资料关联（程序生成）：section_1111aaaa\n研究背景。', encoding='utf-8')
            stream = Stream.__new__(Stream)
            stream.client = Client()
            stream.paper_dataset = 'education-md'
            stream.private_dir = root
            stream.state = {'primary': {paper_id: {'status': 'done', 'document_id': 'source-1',
                'ragflow_document_id': 'remote-1', 'upload_name': 'paper.md'}}, 'errors': {}}
            stream.save = lambda: None
            stream.direct_tree({'id': 'source-1', 'paper_id': paper_id}, package)
            tree = json.loads((root / 'direct_trees' / (paper_id + '.json')).read_text(encoding='utf-8'))
            self.assertEqual(tree['nodes'][0]['source_chunk_ids'], ['chunk-1'])
            self.assertEqual(tree['unlinked_headings'], 0)
            self.assertEqual(stream.state['primary'][paper_id]['tree_status'], 'done')

    def test_tree_backfill_limits_remote_work_per_tick(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            items = [{'id': str(i), 'paper_id': 'paper_' + f'{i:024x}',
                      'status': 'pass', 'category': '教育教学类', 'package_path': str(i)}
                     for i in range(7)]
            (root / 'manifest.json').write_text(json.dumps({'run_id': root.name,
                'status': 'running', 'items': items}), encoding='utf-8')
            stream = Stream.__new__(Stream)
            stream.batch_path = root / 'manifest.json'
            stream.batch_dir = root
            stream.copies = root / 'copies'
            stream.pilot_ids = set()
            stream.state = {'primary': {item['paper_id']: {'status': 'done'} for item in items},
                            'images_complete': {item['paper_id']: True for item in items},
                            'images': {}, 'errors': {}}
            stream.poll_primary = lambda: None
            stream.poll_images = lambda: None
            stream.retry_arrearage = lambda: None
            stream.save = lambda: None
            visited = []
            def direct_tree(item, package):
                visited.append(item['paper_id'])
                stream.state['primary'][item['paper_id']]['tree_status'] = 'done'
            stream.direct_tree = direct_tree
            stream.method_graph = lambda item, package: stream.state['primary'][item['paper_id']].update(
                method_graph_status='done') or True
            stream.rebuild_method_index = lambda: None
            with patch('education_stream_ingest.checked_copy', return_value=(root, {'files': []})) as copy:
                stream.tick()
            self.assertEqual(len(visited), 4)
            self.assertEqual(copy.call_count, 4)

    def recovery_stream(self, remote_run='FAIL', progress_msg='Arrearage', attempts=0):
        class Client:
            def __init__(self):
                self.parse_calls = []
                self.remote_run = remote_run
                self.progress_msg = progress_msg
                self.pipeline_id = 'deepseek-first-pass'
                self.patch_calls = []

            def documents(self, dataset_id, document_id):
                return [{'id': 'remote-1', 'name': 'paper.md', 'run': self.remote_run,
                         'pipeline_id': self.pipeline_id,
                         'progress_msg': self.progress_msg}]

            def request(self, method, endpoint, payload):
                self.patch_calls.append((method, endpoint, payload))
                self.pipeline_id = payload['pipeline_id']

            def parse(self, dataset_id, document_id):
                self.parse_calls.append((dataset_id, document_id))
                self.remote_run = 'RUNNING'

        stream = Stream.__new__(Stream)
        stream.client = Client()
        stream.paper_dataset = 'education-md'
        stream.pipeline = 'deepseek-first-pass'
        stream.max_active = 2
        stream.arrearage_recovery_plan = [{'paper_id': 'paper-1',
            'source_document_id': 'source-1', 'ragflow_document_id': 'remote-1'}]
        stream.state = {'primary': {'paper-1': {'document_id': 'source-1',
            'upload_name': 'paper.md', 'ragflow_document_id': 'remote-1',
            'status': 'fail', 'retry_attempts': attempts}}, 'images': {}, 'errors': {}}
        stream.save = lambda: None
        return stream

    def test_arrearage_retry_reuses_document_and_records_intent(self):
        stream = self.recovery_stream()
        stream.retry_arrearage()
        receipt = stream.state['primary']['paper-1']
        self.assertEqual(stream.client.parse_calls, [('education-md', 'remote-1')])
        self.assertEqual(receipt['status'], 'submitted')
        self.assertEqual(receipt['retry_attempts'], 1)
        stream.retry_arrearage()
        self.assertEqual(len(stream.client.parse_calls), 1)

    def test_new_arrearage_retry_switches_to_fast_pipeline(self):
        stream = self.recovery_stream()
        stream.pipeline = 'fast-v5'
        stream.arrearage_pipeline = 'deepseek-first-pass'
        stream.retry_arrearage()
        self.assertEqual(stream.client.parse_calls, [('education-md', 'remote-1')])
        self.assertEqual(stream.client.pipeline_id, 'fast-v5')
        self.assertEqual(stream.state['primary']['paper-1']['retry_pipeline_id'], 'fast-v5')

    def test_in_flight_old_retry_keeps_existing_pipeline(self):
        stream = self.recovery_stream(remote_run='RUNNING', attempts=1)
        stream.pipeline = 'fast-v5'
        stream.arrearage_pipeline = 'deepseek-first-pass'
        stream.state['primary']['paper-1']['status'] = 'submitted'
        stream.retry_arrearage()
        self.assertEqual(stream.client.pipeline_id, 'deepseek-first-pass')
        self.assertEqual(stream.client.patch_calls, [])

    def test_arrearage_pipeline_patch_ack_loss_reconciles_before_parse(self):
        stream = self.recovery_stream()
        stream.pipeline = 'fast-v5'
        stream.arrearage_pipeline = 'deepseek-first-pass'
        original = stream.client.request

        def uncertain_patch(*args):
            original(*args)
            raise TimeoutError('patch ack lost')

        stream.client.request = uncertain_patch
        with self.assertRaises(TimeoutError):
            stream.retry_arrearage()
        receipt = stream.state['primary']['paper-1']
        self.assertEqual(receipt['status'], 'fail')
        self.assertEqual(receipt['retry_attempts'], 0)
        self.assertEqual(receipt['retry_pipeline_id'], 'fast-v5')
        stream.client.request = original
        stream.retry_arrearage()
        self.assertEqual(stream.client.parse_calls, [('education-md', 'remote-1')])
        self.assertEqual(len(stream.client.patch_calls), 1)

    def test_arrearage_pipeline_patch_can_reset_failed_document_to_unstart(self):
        stream = self.recovery_stream()
        stream.pipeline = 'fast-v5'
        stream.arrearage_pipeline = 'deepseek-first-pass'
        original = stream.client.request

        def reset_on_patch(*args):
            original(*args)
            stream.client.remote_run = 'UNSTART'

        stream.client.request = reset_on_patch
        stream.retry_arrearage()
        self.assertEqual(stream.client.parse_calls, [('education-md', 'remote-1')])
        self.assertEqual(stream.state['primary']['paper-1']['status'], 'submitted')

    def test_arrearage_patch_ack_loss_reconciles_unstart_without_second_patch(self):
        stream = self.recovery_stream()
        stream.pipeline = 'fast-v5'
        stream.arrearage_pipeline = 'deepseek-first-pass'
        original = stream.client.request

        def reset_and_lose_ack(*args):
            original(*args)
            stream.client.remote_run = 'UNSTART'
            raise TimeoutError('patch ack lost')

        stream.client.request = reset_and_lose_ack
        with self.assertRaises(TimeoutError):
            stream.retry_arrearage()
        self.assertEqual(stream.state['primary']['paper-1']['retry_attempts'], 0)
        stream.client.request = original
        stream.retry_arrearage()
        self.assertEqual(stream.client.parse_calls, [('education-md', 'remote-1')])
        self.assertEqual(len(stream.client.patch_calls), 1)

    def test_arrearage_recovery_tick_only_selects_one_untried_charged_failure(self):
        stream = Stream.__new__(Stream)
        stream.state = {'primary': {
            'charged-1': {'status': 'fail', 'failure_kind': 'embedding_arrearage'},
            'charged-2': {'status': 'fail', 'failure_kind': 'embedding_arrearage'},
            'annotation': {'status': 'fail', 'failure_kind': 'annotation_invalid'},
        }}
        stream.arrearage_recovery_plan = [
            {'paper_id': 'charged-1'}, {'paper_id': 'annotation'}, {'paper_id': 'charged-2'}]
        stream.poll_primary = lambda: None
        selected = []

        def retry():
            selected.append([row['paper_id'] for row in stream.arrearage_recovery_plan])
            stream.state['primary'][selected[-1][0]]['status'] = 'submitted'

        stream.retry_arrearage = retry
        finished, summary = stream.recover_arrearage_tick()
        self.assertFalse(finished)
        self.assertEqual(selected, [['charged-1']])
        self.assertEqual(summary['remaining'], 1)
        stream.recover_arrearage_tick()
        self.assertEqual(selected, [['charged-1']])

    def test_arrearage_retry_rejects_unrelated_failure(self):
        stream = self.recovery_stream(progress_msg='论文引文校验失败')
        with self.assertRaisesRegex(ValueError, '欠费'):
            stream.retry_arrearage()
        self.assertEqual(stream.client.parse_calls, [])

    def test_arrearage_retry_halts_if_provider_still_rejects(self):
        stream = self.recovery_stream(attempts=1)
        with self.assertRaises(ArrearageRecoveryHalted):
            stream.retry_arrearage()
        self.assertEqual(stream.client.parse_calls, [])

    def test_arrearage_retry_obeys_active_limit(self):
        stream = self.recovery_stream()
        stream.state['primary']['other'] = {'status': 'submitted'}
        stream.state['primary']['third'] = {'status': 'parse_intent'}
        stream.retry_arrearage()
        self.assertEqual(stream.client.parse_calls, [])

    def test_arrearage_retry_keeps_intent_when_parse_ack_is_lost(self):
        stream = self.recovery_stream()

        def uncertain_parse(dataset_id, document_id):
            raise TimeoutError('ack lost')

        stream.client.parse = uncertain_parse
        with self.assertRaises(TimeoutError):
            stream.retry_arrearage()
        receipt = stream.state['primary']['paper-1']
        self.assertEqual(receipt['status'], 'parse_intent')
        self.assertEqual(receipt['retry_attempts'], 1)
        stream.retry_arrearage()
        self.assertEqual(receipt['status'], 'parse_intent')

    def test_stream_uses_first_pass_pipeline_without_changing_pilot_pipeline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            deployment = {'paper_dataset_id': 'education-md', 'image_dataset_id': 'education-images',
                          'pipeline_id': 'openai-review', 'full_ingest_pipeline_id': 'deepseek-first-pass'}
            stream = Stream(root / 'manifest.json', root, object(), deployment, set())
            self.assertEqual(stream.pipeline, 'deepseek-first-pass')
            self.assertEqual(stream.max_active, 2)
            self.assertEqual(deployment['pipeline_id'], 'openai-review')

    def test_transition_can_allow_one_fast_parse_beside_two_old_compilers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            deployment = {'paper_dataset_id': 'education-md', 'image_dataset_id': 'education-images',
                          'pipeline_id': 'openai-review', 'full_ingest_pipeline_id': 'fast-v5',
                          'full_ingest_max_active': 3}
            stream = Stream(root / 'manifest.json', root, object(), deployment, set())
            self.assertEqual(stream.max_active, 3)

    def test_long_chinese_title_is_shortened_with_identity_suffix_intact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'metadata.json').write_text(json.dumps({
                'id': 'AUTO_12345678901234567890', 'title': '课程教学研究' * 25,
                'automatic_approval': True}, ensure_ascii=False), encoding='utf-8')
            (root / 'ingest.md').write_text('论文正文', encoding='utf-8')
            name = education_remote_name(root / 'ingest.md')
            self.assertLessEqual(len(name.encode('utf-8')), 255)
            self.assertIn('_AUTO_12345678901234567890_', name)
            self.assertEqual(name, education_remote_name(root / 'ingest.md'))

    def test_backlog_does_not_block_new_upload(self):
        paper_id = 'paper_' + 'a' * 24
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'manifest.json').write_text(json.dumps({'run_id': root.name,
                'status': 'running', 'items': [{'id': 'new', 'paper_id': paper_id,
                'status': 'pass', 'category': '教育教学类', 'package_path': 'source'}]}), encoding='utf-8')
            stream = Stream.__new__(Stream)
            stream.batch_path = root / 'manifest.json'
            stream.batch_dir = root
            stream.copies = root / 'copies'
            stream.pilot_ids = set()
            stream.state = {'primary': {str(i): {'status': 'uploaded'} for i in range(12)},
                            'images': {}, 'errors': {}}
            stream.poll_primary = lambda: None
            stream.poll_images = lambda: None
            stream.save = lambda: None
            stream.primary = lambda item, package, manifest: stream.state['primary'].update(
                {item['paper_id']: {'status': 'uploaded'}})
            with patch('education_stream_ingest.checked_copy', return_value=(None, {})):
                stream.tick()
            self.assertIn(paper_id, stream.state['primary'])

    def test_primary_before_auxiliary_for_older_done_paper(self):
        old_id = 'paper_' + 'a' * 24
        new_id = 'paper_' + 'b' * 24
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            items = [{'id': label, 'paper_id': paper_id, 'status': 'pass',
                      'category': '教育教学类', 'package_path': label}
                     for label, paper_id in [('old', old_id), ('new', new_id)]]
            (root / 'manifest.json').write_text(json.dumps({
                'run_id': root.name, 'status': 'running', 'items': items}), encoding='utf-8')
            stream = Stream.__new__(Stream)
            stream.batch_path = root / 'manifest.json'
            stream.batch_dir = root
            stream.copies = root / 'copies'
            stream.pilot_ids = set()
            stream.max_active = 1
            stream.state = {'primary': {old_id: {'status': 'done',
                'tree_status': 'done', 'method_graph_status': 'done'}},
                'images': {}, 'errors': {}, 'images_complete': {}}
            stream.poll_primary = lambda: None
            stream.poll_images = lambda: None
            stream.retry_arrearage = lambda: None
            stream.native_graph_cleanup_pending = lambda paper_id, receipt: False
            stream.cleanup_native_graph = lambda item: None
            stream.save = lambda: None
            calls = []
            stream.primary = lambda item, package, manifest: (
                calls.append('primary'),
                stream.state['primary'].update({new_id: {'status': 'submitted'}}))
            stream.images = lambda item, package, manifest: calls.append('images') or True
            with patch('education_stream_ingest.checked_copy', return_value=(root, {})):
                stream.tick()
            self.assertEqual(calls, ['primary', 'images'])

    def test_parse_intent_counts_against_active_limit(self):
        stream = Stream.__new__(Stream)
        stream.max_active = 1
        stream.paper_dataset = 'education-md'
        stream.state = {'primary': {'old': {'status': 'parse_intent'},
            'new': {'status': 'uploaded', 'document_id': 'new',
                    'upload_name': 'new.md', 'ragflow_document_id': 'remote-new'}},
            'errors': {}}
        stream.client = type('Client', (), {'documents': lambda *args, **kwargs:
            self.fail('parse slot should remain occupied')})()
        with patch('education_stream_ingest.education_remote_name', return_value='new.md'):
            stream.primary({'id': 'new', 'paper_id': 'new'}, Path('ingest.md'), {})

    def test_poll_delay_tracks_waiting_parse_work(self):
        self.assertEqual(esi.poll_delay(1), 5)
        self.assertEqual(esi.poll_delay(0), 15)

    def test_new_document_gets_parse_slot_before_old_failed_retry(self):
        paper_id = 'paper_' + 'c' * 24
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'manifest.json').write_text(json.dumps({
                'run_id': root.name, 'status': 'running',
                'items': [{'id': 'source', 'paper_id': paper_id, 'status': 'pass',
                           'category': '教育教学类', 'package_path': 'source'}]}),
                encoding='utf-8')
            stream = Stream.__new__(Stream)
            stream.batch_path = root / 'manifest.json'
            stream.batch_dir = root
            stream.copies = root / 'copies'
            stream.pilot_ids = set()
            stream.max_active = 1
            stream.state = {'primary': {'old': {'status': 'fail'}},
                            'images': {}, 'errors': {}}
            stream.poll_primary = lambda: None
            stream.poll_images = lambda: None
            stream.save = lambda: None
            order = []

            def primary(item, package, manifest):
                order.append('new')
                stream.state['primary'][paper_id] = {'status': 'submitted'}

            stream.primary = primary
            stream.retry_arrearage = lambda: order.append('retry')
            with patch('education_stream_ingest.checked_copy', return_value=(root, {})):
                stream.tick()
            self.assertEqual(order, ['new', 'retry'])

    def test_completed_old_graph_is_cleaned_after_method_artifact(self):
        paper_id = 'paper_' + 'd' * 24
        class Client:
            pipeline = 'old'
            def documents(self, dataset_id, document_id):
                return [{'id': document_id, 'name': 'paper.md', 'run': 'DONE',
                         'pipeline_id': self.pipeline,
                         'meta_fields': {'education_paper_id': paper_id,
                                         'education_admission': '入库'}}]
        with tempfile.TemporaryDirectory() as directory:
            stream = Stream.__new__(Stream)
            stream.client = Client()
            stream.paper_dataset = 'ecf88dc4b82b11f1a8846d29005ec004'
            stream.pipeline = 'fast-v5'
            stream.private_dir = Path(directory)
            stream.native_graph_cleanup_dir = stream.private_dir / 'native_graph_cleanup'
            stream.native_graph_cleanup_ledger = {'dataset_id': stream.paper_dataset,
                                                  'items': {}}
            stream.state = {'primary': {paper_id: {
                'status': 'done', 'admission': '入库',
                'method_graph_status': 'done', 'ragflow_document_id': 'remote-1',
                'upload_name': 'paper.md'}}, 'errors': {}}
            stream.save = lambda: None
            receipt = stream.state['primary'][paper_id]
            self.assertTrue(stream.native_graph_cleanup_pending(paper_id, receipt))
            with patch('education_native_graph_cleanup.cleanup_one', return_value='deleted') as clean:
                stream.cleanup_native_graph({'paper_id': paper_id})
            self.assertEqual(receipt['native_graph_cleanup_status'], 'deleted')
            self.assertEqual(clean.call_count, 1)
            stream.client.pipeline = 'fast-v5'
            receipt.pop('native_graph_cleanup_status')
            stream.cleanup_native_graph({'paper_id': paper_id})
            self.assertEqual(receipt['native_graph_cleanup_status'], 'not_required')

    def test_image_parse_receipt_reconciles_to_done(self):
        class Client:
            def documents(self, dataset_id, document_id):
                return [{'id': document_id, 'name': 'image.md', 'run': 'DONE'}]

        stream = Stream.__new__(Stream)
        stream.client = Client()
        stream.image_dataset = 'images'
        stream.state = {'images': {'paper_x:image.md': {
            'name': 'image.md', 'ragflow_document_id': 'remote-image',
            'role': 'paper_assets', 'status': 'submitted'}}, 'errors': {}}
        stream.save = lambda: None
        stream.poll_images()
        self.assertEqual(stream.state['images']['paper_x:image.md']['status'], 'done')

    def test_unknown_image_receipt_keeps_images_incomplete(self):
        paper_id = 'paper_' + 'a' * 24
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'manifest.json').write_text(json.dumps({'run_id': root.name,
                'status': 'running', 'items': [{'id': 'source', 'paper_id': paper_id,
                'status': 'pass', 'category': '教育教学类', 'package_path': 'source'}]}),
                encoding='utf-8')
            stream = Stream.__new__(Stream)
            stream.batch_path = root / 'manifest.json'
            stream.batch_dir = root
            stream.copies = root / 'copies'
            stream.pilot_ids = set()
            stream.state = {'primary': {paper_id: {'status': 'done'}},
                            'images': {}, 'errors': {}, 'images_complete': {}}
            stream.poll_primary = lambda: None
            stream.poll_images = lambda: None
            stream.retry_arrearage = lambda: None
            stream.save = lambda: None
            stream.images = lambda item, package, manifest: False
            with patch('education_stream_ingest.checked_copy', return_value=(root, {'files': []})):
                stream.tick()
            self.assertNotIn(paper_id, stream.state['images_complete'])

    def test_unknown_image_does_not_upload_again_without_reconciliation(self):
        paper_id = 'paper_' + 'b' * 24
        image_key = paper_id + ':ragflow_assets.md'

        class Client:
            def documents(self, dataset_id, document_id):
                return [{'id': document_id}]

            def upload(self, *args, **kwargs):
                raise AssertionError('unknown receipt must not be uploaded again')

        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory)
            (package / 'metadata.json').write_text('{"resources": []}', encoding='utf-8')
            stream = Stream.__new__(Stream)
            stream.client = Client()
            stream.paper_dataset = 'education-md'
            stream.image_dataset = 'education-images'
            stream.state = {'primary': {paper_id: {'status': 'done',
                'ragflow_document_id': 'remote-paper'}},
                'images': {image_key: {'name': paper_id + '_ragflow_assets.md',
                'sha256': 'source-hash', 'role': 'paper_assets', 'status': 'unknown'}},
                'errors': {}}
            stream.save = lambda: None
            with patch('education_stream_ingest.eligible_for_auxiliary_index', return_value=True):
                result = stream.images({'paper_id': paper_id}, package,
                    {'files': [{'path': 'ragflow_assets.md', 'role': 'paper_assets',
                                'sha256': 'source-hash'}]})
            self.assertIs(result, False)
            self.assertIn('image:' + image_key, stream.state['errors'])

    def test_remote_lookup_uses_paged_inventory_not_name_filter(self):
        class Client:
            def __init__(self):
                self.calls = []

            def request(self, method, endpoint):
                self.calls.append(endpoint)
                return {'docs': [{'name': 'example.md', 'id': 'remote-1'}]}

            def documents(self, *args, **kwargs):
                raise AssertionError('name filter must not be used')

        client = Client()
        self.assertEqual(exact_document(client, 'education', 'example.md')['id'], 'remote-1')
        self.assertEqual(exact_document(client, 'education', 'missing.md'), None)
        self.assertEqual(len(client.calls), 1)

    def test_only_new_passed_education_packages_are_selected(self):
        batch = {'items': [
            {'id': 'a', 'paper_id': 'paper_' + 'a' * 24, 'status': 'pass',
             'category': '教育教学类', 'package_path': 'a'},
            {'id': 'b', 'paper_id': 'paper_' + 'b' * 24, 'status': 'pass',
             'category': '教育教学类', 'package_path': 'b'},
            {'id': 'c', 'paper_id': 'paper_' + 'c' * 24, 'status': 'reject',
             'category': '教育教学类', 'package_path': 'c'},
            {'id': 'd', 'paper_id': 'paper_' + 'd' * 24, 'status': 'pass',
             'category': '财会类', 'package_path': 'd'},
            {'id': 'e', 'paper_id': 'paper_' + 'a' * 24, 'status': 'pass',
             'category': '教育教学类', 'package_path': 'e'},
        ]}
        result = ready_items(batch, {'paper_' + 'b' * 24}, {})
        self.assertEqual([item['id'] for item in result], ['a'])

    def test_existing_receipt_is_still_selected_for_reconciliation(self):
        paper_id = 'paper_' + 'a' * 24
        batch = {'items': [{'id': 'a', 'paper_id': paper_id, 'status': 'pass',
                            'category': '教育教学类', 'package_path': 'a'}]}
        self.assertEqual([item['id'] for item in ready_items(batch, set(),
                                                              {paper_id: {'status': 'uploaded'}})], ['a'])


if __name__ == '__main__':
    unittest.main()
