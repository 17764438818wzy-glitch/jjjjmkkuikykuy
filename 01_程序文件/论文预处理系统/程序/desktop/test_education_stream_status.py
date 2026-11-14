import unittest

from education_stream_status import monitor_snapshot, overlay_status


class StreamStatusTest(unittest.TestCase):
    def test_workbench_row_uses_external_receipt_when_present(self):
        from rag_results_ui import RAGResultsView
        from upload_tracking import UploadIndex

        paper_id = 'paper_' + 'a' * 24

        class Variable:
            def get(self):
                return 'http://192.168.1.120:8088'

        class Store:
            def history(self):
                return []

        view = RAGResultsView.__new__(RAGResultsView)
        view.current = {'run_id': 'batch', 'task_settings': {}}
        view.store = Store()
        view.url = Variable()
        view._selected_target_dataset = lambda: None
        view._stream_context = ({'primary': {paper_id: {
            'document_id': 'AUTO_123', 'ragflow_document_id': 'remote',
            'status': 'done', 'admission': '入库', 'chunk_count': 53}}, 'images': {}},
            'md-id', 'image-id')
        row = view._remote_item_status({'id': 'AUTO_123', 'paper_id': paper_id,
                                        'uploads': {}}, UploadIndex([]))
        self.assertEqual(row['upload'], '已上传')
        self.assertEqual(row['process'], '已完成')
        self.assertEqual(row['target_id'], 'md-id')

    def test_monitor_distinguishes_uploaded_from_parsed(self):
        a = 'paper_' + 'a' * 24
        b = 'paper_' + 'b' * 24
        batch = {'status': 'running', 'processed_sources': 10, 'source_count': 100,
                 'items': [
                     {'id': 'one', 'paper_id': a, 'status': 'pass',
                      'category': '教育教学类', 'title': '甲', 'major': '小学教育'},
                     {'id': 'two', 'paper_id': b, 'status': 'pass',
                      'category': '教育教学类', 'title': '乙', 'major': '学前教育'}]}
        ledger = {'primary': {a: {'document_id': 'one', 'status': 'uploaded',
                                 'ragflow_document_id': 'remote'}}, 'images': {}}
        rows = monitor_snapshot(batch, ledger, set())
        self.assertEqual(rows[0]['upload'], '已上传')
        self.assertEqual(rows[0]['process'], '等待解析')
        self.assertEqual(rows[1]['upload'], '待上传')

    def test_external_receipt_shows_actual_destination_and_admission(self):
        paper_id = 'paper_' + 'a' * 24
        item = {'id': 'AUTO_123', 'paper_id': paper_id}
        base = {'target': '未指定知识库', 'image_target': '未指定',
                'upload': '待上传', 'process': '尚未提交', 'reason': '',
                'assets': '尚无资料上传记录', 'records': [], 'can_retry': True}
        ledger = {'primary': {paper_id: {'document_id': 'AUTO_123',
            'ragflow_document_id': 'remote-paper', 'status': 'done',
            'admission': '入库', 'chunk_count': 53}},
            'images': {paper_id + ':ragflow_assets.md': {
                'ragflow_document_id': 'remote-image', 'status': 'done',
                'role': 'paper_assets'}}}
        result = overlay_status(item, base, ledger, 'md-id', '图片-id')
        self.assertEqual(result['upload'], '已上传')
        self.assertEqual(result['process'], '已完成')
        self.assertEqual(result['target'], '教育论文研究范例·八篇试验 md')
        self.assertEqual(result['image_target'], '教育论文研究范例·八篇试验图片')
        self.assertIn('入库', result['reason'])
        self.assertIn('1/1', result['assets'])
        self.assertFalse(result['can_retry'])

    def test_other_paper_is_not_overlaid(self):
        base = {'upload': '待上传'}
        self.assertEqual(overlay_status({'paper_id': 'paper_' + 'b' * 24}, base,
                                        {'primary': {}}, 'md', 'image'), base)

    def test_ambiguous_failure_is_shown_as_unresolved(self):
        paper_id = 'paper_' + 'a' * 24
        receipt = {'document_id': 'AUTO_123', 'ragflow_document_id': 'remote-paper',
                   'status': 'fail', 'failure_kind': 'outcome_unresolved', 'chunk_count': 0}
        batch = {'items': [{'id': 'AUTO_123', 'paper_id': paper_id,
                           'status': 'pass', 'category': '教育教学类'}]}
        ledger = {'primary': {paper_id: receipt}, 'images': {}}
        self.assertIn('解析结果待核查', monitor_snapshot(batch, ledger, set())[0]['process'])
        result = overlay_status({'id': 'AUTO_123', 'paper_id': paper_id},
                                {'reason': ''}, ledger, 'md', 'image')
        self.assertIn('解析结果待核查', result['reason'])
        self.assertNotIn('论文质量', result['reason'])


if __name__ == '__main__':
    unittest.main()
