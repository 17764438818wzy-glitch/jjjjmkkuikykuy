"""Use stage4's real validator and fixture; only its RAG evidence is synthetic."""
import asyncio
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from cleanup_fixture import Crash
from nas_handoff_fixture import fixture, build
from paperflow_prefect.records import Records, read_json, sha_file, write_json
from paperflow_prefect.settings import Settings
from paperflow_prefect.legacy import bind


class Stage5HandoffTests(unittest.TestCase):
    def setUp(self):
        try:
            from paperflow_prefect.stage5_handoff import Stage5Receiver
        except ImportError as exc:
            self.fail('stage4 to stage5 receiver is not implemented: ' + str(exc))
        bind(Settings.load())
        self.temp = tempfile.TemporaryDirectory(prefix='stage45-handoff-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.f = fixture(self.root)
        self.handoff = build(self.f)
        self.data = self.root / 'stage5-records'
        self.nas = self.root / 'nas'
        self.receiver = Stage5Receiver(self.data, self.root / 'library', self.nas,
                                      enabled=True, isolation_root=self.root, reserve_bytes=0)
        self.originals = [Path(r['path']) for r in self.handoff['files']]

    def receive_archive(self):
        accepted = self.receiver.receive(self.handoff)
        self.assertEqual(accepted['status'], 'ready')
        work = Path(accepted['work'])
        result = self.receiver.archive(work)
        self.assertEqual(result['status'], 'verified')
        self.assertEqual(len(result['files']), 2)
        return work

    def test_complete_handoff_archives_both_files_and_cleanup_reuses_ids(self):
        work = self.receive_archive()
        result = self.receiver.cleanup(work)
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(result['paper_id'], self.handoff['paper']['paper_id'])
        self.assertEqual(result['revision'], self.handoff['paper']['revision'])
        self.assertFalse(any(p.exists() for p in self.originals))
        self.assertTrue(Path(self.f['attachment_receipt_paths'][0]).exists())
        self.assertTrue(Path(read_json(self.f['attachment_receipt_paths'][0])['saved_md_path']).exists())

    def test_not_ready_has_zero_nas_calls_and_zero_cleanup(self):
        bad = build(self.f, body_receipt_path=None)
        with patch('paperflow_prefect.stage5_handoff.archive_originals', side_effect=AssertionError('NAS called')):
            result = self.receiver.receive(bad)
        self.assertEqual(result['status'], 'not_ready')
        self.assertFalse(self.nas.exists())
        self.assertTrue(all(p.exists() for p in self.originals))

    def test_missing_or_changed_nas_evidence_blocks_all_cleanup(self):
        accepted = self.receiver.receive(self.handoff)
        work = Path(accepted['work'])
        with self.assertRaises(ValueError):
            self.receiver.cleanup(work)
        self.assertTrue(all(p.exists() for p in self.originals))
        result = self.receiver.archive(work)
        row = result['files'][0]
        path = self.nas / 'objects' / row['sha256'][:2] / row['sha256']
        path.write_bytes(b'corrupt synthetic NAS object')
        with self.assertRaises(ValueError):
            self.receiver.cleanup(work)
        self.assertTrue(all(p.exists() for p in self.originals))

    def test_interruption_and_two_rescans_only_execute_cleanup_finish(self):
        work = self.receive_archive()
        def crash(stage, path):
            if stage == 'after_unlink':
                raise Crash()
        with self.assertRaises(Crash):
            self.receiver.cleanup(work, checkpoint=crash)
        values = dict(Settings.load().values, execution_enabled=True, local_cleanup_enabled=True,
                      library_root=str(self.root / 'library'), data_root=str(self.data), nas_destination=str(self.nas))
        from paperflow_prefect.unified import UnifiedBackend, execute_unified
        backend = UnifiedBackend(Settings(values), invoke=lambda *a, **k: self.fail('model must not run'))
        paper = backend.receive(self.f['intake_receipt_path'])
        binding_path = backend.paper_root(paper)/'stage5-binding.json'
        accepted = dict(status='ready',work=str(work))
        write_json(binding_path,accepted)
        Records(backend.paper_root(paper)).complete('stage5-binding',accepted,[binding_path])
        calls = []
        async def call(stage, paper, row=None):
            calls.append(stage)
            self.assertIn(stage, {'archive','cleanup', 'finish'})
            return getattr(backend, stage)(paper)
        for _ in range(2):
            paper = backend.receive(self.f['intake_receipt_path'])
            self.assertEqual(paper['paper_id'], self.handoff['paper']['paper_id'])
            result = asyncio.run(execute_unified(paper,backend.receipt_for(paper),call,resume_stage5=True))
            self.assertEqual(result['status'], 'complete')
        self.assertEqual(calls, ['archive','cleanup','finish','archive','cleanup','finish'])

    def test_completed_resume_does_not_reopen_nas_or_revalidate_deleted_sources(self):
        work = self.receive_archive()
        self.receiver.cleanup(work)
        with patch('paperflow_prefect.cleanup.verified_nas', side_effect=AssertionError('NAS reopened')), \
                patch('paperflow_prefect.nas_handoff.validate_nas_handoff', side_effect=AssertionError('stage4 replayed')):
            result = self.receiver.resume(self.root / 'library/person')
        self.assertEqual(result['status'], 'complete')

    def test_new_or_reappeared_source_is_preserved_on_rescan(self):
        work = self.receive_archive()
        self.receiver.cleanup(work)
        self.originals[0].write_bytes(b'new object at old path')
        extra = self.root / 'library/person/later.bin'
        extra.write_bytes(b'new unknown material')
        result = self.receiver.resume(self.root / 'library/person')
        self.assertEqual(result['status'], 'incomplete')
        self.assertEqual(set(result['cleanup']['new_files']), {str(self.originals[0]), str(extra)})
        self.assertTrue(self.originals[0].exists())
        self.assertTrue(extra.exists())

    def test_upstream_mutable_receipt_update_does_not_break_recovery(self):
        work = self.receive_archive()
        # This is a subsequent revision of a generated fixture receipt only.
        p = self.f['attachment_receipt_paths'][0]
        r = read_json(p); r['reconciled_at'] = 'later'
        write_json(p, r)
        self.assertEqual(self.receiver.cleanup(work)['status'], 'complete')
        self.assertEqual(self.receiver.resume(self.root / 'library/person')['status'], 'complete')

    def test_fixture_needs_explicit_isolation_and_cannot_escape_bound_library(self):
        from paperflow_prefect.stage5_handoff import Stage5Receiver
        no_fixture = Stage5Receiver(self.data, self.root / 'library', self.nas, enabled=True)
        with self.assertRaises(PermissionError):
            no_fixture.receive(self.handoff)
        wrong = self.root / 'another-library'; wrong.mkdir()
        receiver = Stage5Receiver(self.data, wrong, self.nas, enabled=True, isolation_root=self.root)
        with self.assertRaises(ValueError):
            receiver.receive(self.handoff)
        self.assertFalse(self.nas.exists())
        self.assertTrue(all(p.exists() for p in self.originals))

    def test_admitted_owned_cache_is_cleaned_with_originals_and_unlisted_file_is_reported(self):
        from types import SimpleNamespace
        from test_stage5_scope import cache_item
        row = cache_item(SimpleNamespace(paper=self.handoff['paper'], work=self.data / 'cache-job'))
        self.handoff = build(self.f, intermediates=[row])
        unknown = self.root / 'library/person/unclassified.bin'
        unknown.write_bytes(b'unknown stays local')
        work = self.receive_archive()
        result = self.receiver.cleanup(work)
        self.assertEqual(result['cleanup']['removed_files'], 3)
        self.assertFalse(Path(row['path']).exists())
        self.assertTrue(unknown.exists())
        self.assertIn(str(unknown), result['cleanup'].get('unlisted_source_files', []))
        self.assertEqual(len(Records(work).cached('nas')['files']), 2)
