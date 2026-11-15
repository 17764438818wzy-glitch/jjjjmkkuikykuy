import unittest
from paperflow_prefect.workflow import execute_stage3


class Stage3WorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def test_confirmed_attachment_advances_without_body_or_other_items(self):
        paper = {'paper_id': 'p', 'body': None, 'attachments': [{'file_id': 'a'}, {'file_id': 'b'}]}
        calls = []
        async def call(stage, paper, row=None):
            calls.append((stage, row and row['file_id']))
            if row and row['file_id'] == 'b': raise ValueError('broken attachment')
        result = await execute_stage3(paper, call)
        self.assertEqual(set(calls), {('attachment', 'a'), ('attachment', 'b')})
        self.assertEqual(result['attachments']['a']['status'], 'complete')
        self.assertEqual(result['attachments']['b']['status'], 'failed')
        self.assertEqual(result['status'], 'incomplete')

    async def test_body_and_attachments_stop_at_stage3_and_errors_remain_specific(self):
        from paperflow_prefect.attachments import AttachmentError
        paper = {'paper_id': 'p', 'body': {'path': 'body'}, 'attachments': [{'file_id': 'a'}]}
        calls = []
        async def call(stage, paper, row=None):
            calls.append(stage)
            if stage == 'attachment': raise AttachmentError('timeout_unknown')
        result = await execute_stage3(paper, call)
        self.assertEqual(result['body']['status'], 'complete')
        self.assertEqual(result['attachments']['a']['error']['code'], 'timeout_unknown')
        self.assertEqual(set(calls), {'prepare', 'profile', 'attachment'})

    async def test_all_trash_receipt_has_no_paper_task(self):
        async def forbidden(*args): self.fail('no task expected')
        result = await execute_stage3({'paper_id': None, 'body': None, 'attachments': []}, forbidden)
        self.assertEqual(result['status'], 'no_material')
