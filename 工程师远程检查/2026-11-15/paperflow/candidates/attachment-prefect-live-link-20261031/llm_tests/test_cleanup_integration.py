import asyncio
from pathlib import Path
import unittest
from unittest.mock import patch
from cleanup_fixture import Crash
from unified_fixture import Fixture
from paperflow_prefect.settings import Settings
from paperflow_prefect.records import read_json


class CleanupIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.f = Fixture()
        self.addCleanup(self.f.close)

    def backend(self, enabled=True):
        from paperflow_prefect.unified import UnifiedBackend as Backend
        values = dict(self.f.values, local_cleanup_enabled=enabled)
        return Backend(Settings(values), invoke=lambda *a, **k: self.fail('model must not run'))

    def prepare(self):
        self.f.manager().freeze(self.f.paper, self.f.work)

    def run_rescan(self, backend):
        from paperflow_prefect.unified import execute_unified
        paper = backend.receive(self.f.receipt_path)
        calls = []
        async def call(stage, current, row=None):
            calls.append(stage)
            self.assertIn(stage, {'archive','cleanup', 'finish'})
            return getattr(backend, stage)(current)
        result = asyncio.run(execute_unified(paper,backend.receipt_for(paper),call,resume_stage5=True))
        return paper, calls, result

    def test_complete_rescan_uses_receipts_without_originals_or_model(self):
        self.prepare()
        self.f.manager().run(self.f.work)
        paper, calls, result = self.run_rescan(self.backend())
        self.assertEqual(paper['revision'], self.f.paper['revision'])
        self.assertEqual(calls, ['archive','cleanup', 'finish'])
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(result['local_originals'], 'cleaned')
        self.assertTrue(Path(result['record']).is_file())
        self.f.assert_protected(self)

    def test_partial_delete_rescan_uses_original_revision_only(self):
        self.prepare()
        def interrupt(stage, path):
            if stage == 'after_complete':
                raise Crash()
        with self.assertRaises(Crash):
            self.f.manager().run(self.f.work, checkpoint=interrupt)
        paper, calls, result = self.run_rescan(self.backend())
        self.assertEqual(paper['revision'], self.f.paper['revision'])
        self.assertEqual(calls, ['archive','cleanup', 'finish'])
        self.assertEqual(result['status'], 'complete')

    def test_model_signature_change_does_not_redirect_cleanup_records(self):
        self.prepare()
        self.f.manager().run(self.f.work)
        backend = self.backend()
        backend.body_signature = 'new-model-config'
        paper, calls, result = self.run_rescan(backend)
        self.assertEqual(backend.stage5_work(paper), self.f.work)
        self.assertEqual(result['status'], 'complete')

    def test_new_material_after_cleanup_is_reported_without_reingesting_old_paper(self):
        self.prepare()
        self.f.manager().run(self.f.work)
        new_file = self.f.attachment.parent / 'later.txt'
        new_file.write_text('separate future intake', encoding='utf8')
        paper, calls, result = self.run_rescan(self.backend())
        self.assertEqual(result['status'], 'incomplete')
        self.assertEqual(result['cleanup']['new_files'], [str(new_file)])
        self.assertTrue(new_file.exists())
        self.assertFalse(self.f.body.exists())

    def test_disabled_setting_does_not_resume_pending_deletion(self):
        self.prepare()
        backend = self.backend(False)
        paper = backend.receive(self.f.receipt_path)
        result = backend.cleanup(paper)
        self.assertEqual(result['status'], 'cleanup_pending')
        self.assertTrue(self.f.body.exists())
        self.assertTrue(self.f.attachment.exists())

    def test_recreated_completed_path_is_not_deleted_again(self):
        self.prepare()
        self.f.manager().run(self.f.work)
        self.f.body.write_text('A genuinely new paper', encoding='utf8')
        paper, calls, result = self.run_rescan(self.backend())
        self.assertEqual(result['status'], 'incomplete')
        self.assertIn(str(self.f.body), result['cleanup']['new_files'])
        self.assertTrue(self.f.body.exists())

    def test_completed_rescan_returns_saved_cleanup_without_reopening_nas(self):
        self.prepare()
        self.f.manager().run(self.f.work)
        backend = self.backend()
        with patch('paperflow_prefect.cleanup.verified_nas', side_effect=AssertionError('NAS reopened on completed rescan')):
            paper, calls, result = self.run_rescan(backend)
        self.assertEqual(result['status'], 'complete')

    def test_intake_without_cleanup_index_does_not_probe_nas_path(self):
        backend = self.backend(False)
        original_resolve = Path.resolve
        probes = []
        nas = self.f.nas
        def checked(path, *args, **kwargs):
            if path == nas:
                probes.append(str(path))
            return original_resolve(path, *args, **kwargs)
        with patch.object(Path, 'resolve', checked):
            backend.receive(self.f.receipt_path)
        self.assertEqual(probes, [])


class WorkflowCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_normal_flow_orders_archive_then_cleanup_then_finish(self):
        from paperflow_prefect.unified import execute_unified
        calls = []
        async def call(stage, paper, row=None):
            calls.append(stage)
            return {'status': 'complete' if stage == 'finish' else 'verified'}
        await execute_unified({'paper_id': 'fixture','body':{}, 'attachments': []}, {'state':'已接收'},call)
        self.assertEqual(calls[-3:], ['archive', 'cleanup', 'finish'])

    async def test_cleanup_failure_never_reenters_upstream_stages(self):
        from paperflow_prefect.unified import execute_unified
        calls = []
        async def call(stage, paper, row=None):
            calls.append(stage)
            if stage == 'cleanup':
                raise ValueError('NAS evidence changed')
            if stage == 'archive':return {'status':'verified'}
            self.fail('another stage must not run')
        result = await execute_unified({'paper_id': 'fixture', 'attachments': []},{},call,resume_stage5=True)
        self.assertEqual(calls, ['archive','cleanup'])
        self.assertEqual(result['status'], 'incomplete')
        self.assertIn('cleanup', result['failures'])
