import os
from pathlib import Path
import unittest
from cleanup_fixture import Fixture
from paperflow_prefect.records import read_json


class ReviewRegressions(unittest.TestCase):
    def setUp(self):
        self.f = Fixture()
        self.addCleanup(self.f.close)
        self.manager = self.f.manager()
        self.manager.freeze(self.f.paper, self.f.work)

    def test_recovery_bindings_are_held_through_every_delete(self):
        bindings = [self.f.work / 'cleanup/association.json',
                    self.f.records.path('cleanup-association'),
                    self.manager._index(self.f.paper)]
        attempts = []
        def replace_at_checkpoint(stage, path):
            if stage not in {'after_intent', 'after_complete'}:
                return
            for binding in bindings:
                replacement = binding.with_name(binding.name + '.replace-probe')
                replacement.write_bytes(binding.read_bytes())
                with self.assertRaises(PermissionError, msg=str(binding)):
                    os.replace(replacement, binding)
                replacement.unlink()
                attempts.append(str(binding))
        result = self.manager.run(self.f.work, checkpoint=replace_at_checkpoint)
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(len(attempts), 12)
        self.assertEqual(self.manager.lookup(self.f.person)['action'], 'complete')

    def test_missing_without_intent_remains_unknown_and_unresolved(self):
        self.f.body.unlink()
        result = self.manager.run(self.f.work)
        self.assertEqual(result['status'], 'unknown')
        self.assertEqual(result['pending_files'], 1)
        self.assertEqual(result['unknown_files'], [{'path': str(self.f.body), 'reason': 'missing_without_intent'}])
        self.assertEqual(result['retained'], [])
        self.assertEqual(result['removed_files'], 1)
        again = self.manager.run(self.f.work)
        self.assertEqual(again['status'], 'unknown')
        self.assertEqual(again['pending_files'], 1)
