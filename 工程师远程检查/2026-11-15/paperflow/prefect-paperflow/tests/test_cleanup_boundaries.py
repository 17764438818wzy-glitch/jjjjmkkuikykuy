import os
from pathlib import Path
import unittest
from cleanup_fixture import Fixture, Crash


class CleanupBoundaries(unittest.TestCase):
    def setUp(self):
        self.f = Fixture()
        self.addCleanup(self.f.close)
        self.manager = self.f.manager()
        self.manager.freeze(self.f.paper, self.f.work)

    def test_named_stream_is_preserved_because_archive_covers_only_default_stream(self):
        stream = Path(str(self.f.body) + ':unarchived')
        stream.write_bytes(b'keep this unarchived named stream')
        result = self.manager.run(self.f.work)
        self.assertEqual(result['status'], 'retained')
        self.assertTrue(self.f.body.exists())
        self.assertEqual(stream.read_bytes(), b'keep this unarchived named stream')

    def test_busy_file_is_deferred_then_recovered_without_reprocessing(self):
        from paperflow.original_archive.lifecycle import locked_file
        with locked_file(self.f.body):
            first = self.manager.run(self.f.work)
            self.assertEqual(first['status'], 'incomplete')
            self.assertTrue(first['deferred'])
            self.assertTrue(self.f.body.exists())
        self.assertEqual(self.manager.lookup(self.f.person)['action'], 'resume_cleanup')
        second = self.manager.run(self.f.work)
        self.assertEqual(second['status'], 'complete')

    def test_deleted_journal_tail_is_not_treated_as_unattempted_work(self):
        def crash(stage, path):
            if stage == 'after_complete':
                raise Crash()
        with self.assertRaises(Crash):
            self.manager.run(self.f.work, checkpoint=crash)
        events = sorted((self.f.work / 'cleanup/journal').glob('event-*.json'))
        events[-1].unlink()
        with self.assertRaisesRegex(ValueError, 'journal tail'):
            self.manager.run(self.f.work)
        self.assertTrue(self.f.attachment.exists())

    def test_directory_link_swap_blocks_deletion(self):
        original = self.f.attachment.parent
        outside = self.f.root / 'old-attachments'
        os.rename(original, outside)
        os.symlink(outside, original, target_is_directory=True)
        with self.assertRaises(ValueError):
            self.manager.run(self.f.work)
        self.assertTrue(self.f.body.exists())
        self.assertTrue((outside / self.f.attachment.name).exists())
