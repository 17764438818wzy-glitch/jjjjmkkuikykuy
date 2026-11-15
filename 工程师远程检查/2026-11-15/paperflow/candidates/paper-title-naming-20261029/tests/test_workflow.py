import asyncio
import unittest


class WorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def test_attachment_failure_preserves_completed_body(self):
        from paperflow_prefect.workflow import execute_paper
        calls = []
        async def call(stage, paper, row=None):
            await asyncio.sleep(0)
            calls.append(stage)
            if stage == 'attachment': raise ValueError('fixture')
            return {'status': 'verified'}
        result = await execute_paper({'paper_id': 'fixture', 'attachments': [{'file_id':'a'}]}, call)
        self.assertEqual(result['status'], 'incomplete')
        self.assertIn('publish_body', calls)
        self.assertNotIn('archive', calls)

    async def test_nas_failure_cannot_mark_complete(self):
        from paperflow_prefect.workflow import execute_paper
        calls = []
        async def call(stage, paper, row=None):
            calls.append(stage)
            if stage == 'archive': raise OSError('fixture')
            return {'status':'verified'}
        result = await execute_paper({'paper_id':'fixture', 'attachments':[]}, call)
        self.assertEqual(result['status'], 'incomplete')
        self.assertIn('publish_attachments', calls)
        self.assertNotIn('finish', calls)

    async def test_body_and_attachment_are_independent(self):
        from paperflow_prefect.workflow import execute_paper
        entered = asyncio.Event(); released = asyncio.Event(); calls = []
        async def call(stage, paper, row=None):
            calls.append(stage)
            if stage == 'profile':
                entered.set()
                await asyncio.wait_for(released.wait(), timeout=2)
            if stage == 'attachment':
                await asyncio.wait_for(entered.wait(), timeout=2)
                released.set()
            return {'status': 'complete' if stage == 'finish' else 'verified'}
        result = await execute_paper({'paper_id':'fixture', 'attachments':[{'file_id':'a'}]}, call)
        self.assertEqual(result['status'], 'complete')
        self.assertLess(calls.index('publish_body'), calls.index('archive'))
        self.assertLess(calls.index('publish_attachments'), calls.index('archive'))


if __name__ == '__main__': unittest.main()
