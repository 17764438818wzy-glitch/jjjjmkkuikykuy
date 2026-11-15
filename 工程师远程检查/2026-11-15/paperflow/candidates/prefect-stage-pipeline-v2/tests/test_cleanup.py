import json
import os
from pathlib import Path
import unittest
from cleanup_fixture import Fixture, Crash
from paperflow_prefect.records import read_json, sha_file, write_json


class CleanupTests(unittest.TestCase):
    def setUp(self):
        self.f = Fixture()
        self.addCleanup(self.f.close)

    def freeze(self):
        manager = self.f.manager()
        manager.freeze(self.f.paper, self.f.work)
        return manager

    def test_missing_or_unverified_rag_or_nas_causes_zero_delete(self):
        for stage in ('rag-body', 'rag-attachments', 'nas'):
            path = self.f.records.path(stage)
            original = path.read_bytes()
            for state in ('missing', 'unverified'):
                with self.subTest(stage=stage, state=state):
                    if state == 'missing':
                        path.unlink()
                    else:
                        self.f.records.complete(stage, {'status': 'pending'})
                    with self.assertRaises(ValueError):
                        self.f.manager().freeze(self.f.paper, self.f.work)
                    self.assertTrue(self.f.body.exists())
                    self.assertTrue(self.f.attachment.exists())
                    path.write_bytes(original)

    def test_disabled_cleanup_never_deletes(self):
        manager = self.f.manager(False)
        with self.assertRaises(PermissionError):
            manager.freeze(self.f.paper, self.f.work)
        self.assertTrue(self.f.body.exists())
        self.assertTrue(self.f.attachment.exists())

    def test_exact_files_removed_garbage_records_and_folders_preserved(self):
        manager = self.freeze()
        result = manager.run(self.f.work)
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(result['removed_files'], 2)
        self.assertFalse(self.f.body.exists())
        self.assertFalse(self.f.attachment.exists())
        self.assertTrue(self.f.body.parent.is_dir())
        self.assertTrue(self.f.person.is_dir())
        self.f.assert_protected(self)
        self.assertEqual(manager.inspect(self.f.work)['status'], 'complete')
        again = manager.run(self.f.work)
        self.assertEqual(again['journal_head_sha256'], result['journal_head_sha256'])

    def test_changed_file_retained_and_other_frozen_file_can_finish(self):
        manager = self.freeze()
        self.f.body.write_bytes(b'new manuscript must remain')
        result = manager.run(self.f.work)
        self.assertEqual(result['status'], 'retained')
        self.assertIn(str(self.f.body), [r['path'] for r in result['retained']])
        self.assertEqual(self.f.body.read_bytes(), b'new manuscript must remain')
        self.assertFalse(self.f.attachment.exists())
        self.f.assert_protected(self)

    def test_same_content_replacement_retained(self):
        manager = self.freeze()
        replacement = self.f.body.with_suffix('.replacement')
        replacement.write_bytes(self.f.body.read_bytes())
        os.replace(replacement, self.f.body)
        result = manager.run(self.f.work)
        self.assertEqual(result['status'], 'retained')
        self.assertTrue(self.f.body.is_file())

    def test_replacement_between_intake_and_freeze_rejected(self):
        replacement = self.f.body.with_suffix('.replacement')
        replacement.write_bytes(self.f.body.read_bytes())
        os.replace(replacement, self.f.body)
        with self.assertRaises(ValueError):
            self.f.manager().freeze(self.f.paper, self.f.work)
        self.assertTrue(self.f.attachment.exists())

    def test_added_file_not_enrolled_in_frozen_plan(self):
        manager = self.freeze()
        added = self.f.attachment.parent / 'new.txt'
        added.write_bytes(b'new material')
        result = manager.run(self.f.work)
        self.assertEqual(result['status'], 'retained')
        self.assertEqual(result['new_files'], [str(added)])
        self.assertTrue(added.exists())
        self.assertFalse(self.f.attachment.exists())

    def crash_and_resume(self, checkpoint):
        manager = self.freeze()
        def interrupt(stage, path):
            if stage == checkpoint:
                raise Crash(checkpoint)
        with self.assertRaises(Crash):
            manager.run(self.f.work, checkpoint=interrupt)
        saved = self.f.manager().lookup(self.f.person)
        self.assertEqual(saved['paper']['revision'], self.f.paper['revision'])
        self.assertEqual(saved['action'], 'resume_cleanup')
        result = self.f.manager().run(self.f.work)
        self.assertEqual(result['status'], 'complete')
        self.f.assert_protected(self)
        return result

    def test_crash_after_intent_before_unlink_reconciles_present_original(self):
        result = self.crash_and_resume('after_intent')
        self.assertEqual(result['removed_files'], 2)
        self.assertEqual(result['reconciled_absent_files'], 0)

    def test_crash_after_unlink_before_result_reconciles_missing_name(self):
        result = self.crash_and_resume('after_unlink')
        self.assertEqual(result['reconciled_absent_files'], 1)
        self.assertEqual(result['removed_files'], 1)

    def test_crash_after_first_complete_only_handles_remaining_file(self):
        result = self.crash_and_resume('after_complete')
        self.assertEqual(result['removed_files'], 2)

    def test_missing_without_intent_is_unknown_not_deleted(self):
        manager = self.freeze()
        self.f.body.unlink()
        result = manager.run(self.f.work)
        self.assertEqual(result['status'], 'unknown')
        self.assertIn('missing_without_intent', [r['reason'] for r in result['unknown_files']])

    def test_external_hardlink_preserved_without_scope_expansion(self):
        manager = self.freeze()
        external = self.f.root / 'old-library-original.txt'
        os.link(self.f.body, external)
        result = manager.run(self.f.work)
        self.assertEqual(result['status'], 'retained')
        self.assertTrue(self.f.body.exists())
        self.assertTrue(external.exists())
        self.assertEqual(sha_file(external), self.f.paper['body']['sha256'])

    def test_nas_tampering_before_delete_causes_zero_delete(self):
        manager = self.freeze()
        nas = read_json(self.f.work / 'archive/nas-receipt.json')
        manifest = read_json(self.f.nas / nas['manifest_path'])
        digest = manifest['files'][0]['sha256']
        (self.f.nas / 'objects' / digest[:2] / digest).write_bytes(b'corrupt')
        with self.assertRaises(ValueError):
            manager.run(self.f.work)
        self.assertTrue(self.f.body.exists())
        self.assertTrue(self.f.attachment.exists())

    def test_changed_rag_receipt_after_freeze_causes_zero_delete(self):
        manager = self.freeze()
        write_json(self.f.work / 'body-delivery.json', {'status': 'uncertain'})
        with self.assertRaises(ValueError):
            manager.run(self.f.work)
        self.assertTrue(self.f.body.exists())
        self.assertTrue(self.f.attachment.exists())

    def test_truncated_event_chain_stops_remaining_delete(self):
        manager = self.freeze()
        def interrupt(stage, path):
            if stage == 'after_complete':
                raise Crash()
        with self.assertRaises(Crash):
            manager.run(self.f.work, checkpoint=interrupt)
        events = list((self.f.work / 'cleanup/journal').glob('event-*.json'))
        events[0].write_text('{}')
        with self.assertRaises((ValueError, KeyError)):
            manager.run(self.f.work)
        self.assertTrue(self.f.attachment.exists())

    def test_new_library_boundary_rejects_garbage_and_other_roots(self):
        from copy import deepcopy
        for path, kind in ((self.f.garbage, 'body'), (self.f.saved, 'attachment')):
            paper = deepcopy(self.f.paper)
            paper['archive_files'][0].update(path=str(path), relative='垃圾材料/report.txt', kind=kind)
            with self.assertRaises(ValueError):
                self.f.manager().freeze(paper, self.f.work)
        self.f.assert_protected(self)


if __name__ == '__main__':
    unittest.main()
