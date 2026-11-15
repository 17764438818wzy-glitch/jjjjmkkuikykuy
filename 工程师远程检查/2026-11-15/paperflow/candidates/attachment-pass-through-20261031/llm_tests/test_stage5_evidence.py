import copy
from pathlib import Path
import unittest
from cleanup_fixture import Fixture
from test_stage5_h21_scope import h21_paper
from paperflow_prefect.records import identity, read_json, sha_file, write_json


class FrozenHandoffEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.f = Fixture()
        self.addCleanup(self.f.close)
        enhanced = h21_paper(self.f)
        self.paper = copy.deepcopy(enhanced)
        for name in ('archive_files', 'body_copies', 'attachments'):
            for row in self.paper[name]:
                row.pop('object_fingerprint', None)
        self.paper['body'].pop('object_fingerprint', None)
        ref_path = self.f.data / 'source-intake-receipt.json'
        write_json(ref_path, dict(inventory=self.paper, status='fixture'))
        self.ref = dict(path=str(ref_path), sha256=sha_file(ref_path))
        self.handoff = dict(schema='paperflow-nas-handoff/1', handoff_id='fixture-frozen-handoff',
            status='ready', reasons=[], validation_scope='isolated_fixture', paper=self.paper,
            files=copy.deepcopy(enhanced['archive_files']), intake_receipt=self.ref,
            evidence=[self.ref], retained_evidence=[], intermediates=[])
        self.work = self.f.data / 'stage5-evidence'

    def api(self):
        try:
            from paperflow_prefect.stage5_evidence import snapshot_handoff, frozen_handoff
            return snapshot_handoff, frozen_handoff
        except ImportError as exc:
            self.fail('immutable handoff evidence is missing: ' + str(exc))

    def test_source_receipt_rewrite_does_not_change_frozen_evidence(self):
        snapshot, load = self.api()
        snapshot(self.handoff, self.work)
        Path(self.ref['path']).write_bytes(b'subsequent upstream reconciliation')
        saved = load(self.work)
        self.assertEqual(saved['paper']['paper_id'], 'PF_formal_received_identity')
        self.assertEqual(saved['handoff']['paper'], self.paper)
        copied = saved['snapshots'][0]
        self.assertEqual(sha_file(copied['snapshot_path']), self.ref['sha256'])

    def test_frozen_evidence_tampering_is_rejected(self):
        snapshot, load = self.api()
        snapshot(self.handoff, self.work)
        copied = load(self.work)['snapshots'][0]
        Path(copied['snapshot_path']).write_bytes(b'changed snapshot')
        with self.assertRaises(ValueError):
            load(self.work)

    def test_original_payload_is_not_retained_as_an_evidence_copy(self):
        snapshot, load = self.api()
        body = self.handoff['files'][0]
        self.handoff['evidence'].append(dict(path=body['path'], sha256=body['sha256']))
        snapshot(self.handoff, self.work)
        saved = load(self.work)
        self.assertEqual(len(saved['snapshots']), 1)
        self.assertEqual(saved['nas_source_evidence'][0]['path'], body['path'])

    def test_augmented_files_cannot_change_ids_or_paths(self):
        snapshot, _ = self.api()
        self.handoff['files'][0]['file_id'] = 'BODY_another_identity'
        with self.assertRaises(ValueError):
            snapshot(self.handoff, self.work)
        self.assertFalse((self.work / 'stages/stage4-handoff.json').exists())

    def test_missing_or_conflicting_source_evidence_prevents_commit(self):
        snapshot, _ = self.api()
        self.handoff['evidence'].append(dict(path=self.ref['path'], sha256='0' * 64))
        with self.assertRaises(ValueError):
            snapshot(self.handoff, self.work)
        self.assertFalse((self.work / 'stages/stage4-handoff.json').exists())

    def test_not_ready_handoff_is_not_persisted_as_accepted(self):
        snapshot, _ = self.api()
        self.handoff['status'] = 'not_ready'
        with self.assertRaises(ValueError):
            snapshot(self.handoff, self.work)
        self.assertFalse((self.work / 'stages/stage4-handoff.json').exists())
