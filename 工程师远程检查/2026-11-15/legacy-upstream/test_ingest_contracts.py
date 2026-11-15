import dataclasses
import unittest
from pathlib import Path

from ingest_contracts import ArtifactSpec, Outcome, canonical_remote_name, task_identity


HASH = 'a' * 64


def spec(**changes):
    values = dict(service_url='http://LOCALHOST:9380/', dataset_id='md-1',
                  source_id='paper_123', role='paper_primary', lane='paper',
                  content_sha256=HASH, path=Path('ingest.md'),
                  remote_name='paper_123_aaaaaaaaaaaaaaaa.md',
                  pipeline_id='pipe-1', parse_required=True, priority=0,
                  batch_id='batch-1', policy_key='education', channel_key='model-a')
    values.update(changes)
    return ArtifactSpec(**values)


class ArtifactSpecTest(unittest.TestCase):
    def test_identity_uses_normalized_service_dataset_source_and_role(self):
        original = spec()
        self.assertEqual(task_identity(original), task_identity(spec(service_url='http://127.0.0.1:9380')))
        self.assertNotEqual(task_identity(original), task_identity(spec(dataset_id='image-1')))
        self.assertNotEqual(task_identity(original), task_identity(spec(role='original_image')))

    def test_required_identity_fields_and_hash_are_validated(self):
        for name, value in [('source_id', ''), ('dataset_id', ' '), ('role', ''),
                            ('remote_name', ''), ('policy_key', ''), ('channel_key', ''),
                            ('lane', 'other'), ('content_sha256', 'a' * 63)]:
            with self.subTest(name=name), self.assertRaises(ValueError):
                spec(**{name: value})

    def test_spec_is_frozen_and_outcomes_are_explicit(self):
        with self.assertRaises(dataclasses.FrozenInstanceError):
            spec().source_id = 'other'
        self.assertEqual({outcome.value for outcome in Outcome}, {
            'accepted', 'quality_hold', 'quality_reject',
            'technical_review', 'outcome_unresolved'})

    def test_canonical_name_keeps_identity_within_utf8_limit(self):
        name = canonical_remote_name('教育研究' * 100, 'paper_123', HASH, '.md',
                                     legacy_id='AUTO_12345678901234567890')
        self.assertLessEqual(len(name.encode('utf-8')), 255)
        self.assertIn('paper_123', name)
        self.assertIn(HASH[:16], name)
        self.assertIn('AUTO_12345678901234567890', name)
        self.assertTrue(name.endswith('.md'))


if __name__ == '__main__':
    unittest.main()
