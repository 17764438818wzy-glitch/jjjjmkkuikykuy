"""H2.1 keeps formal IDs and a finite inventory even in a flat source folder."""
import copy
from pathlib import Path
import unittest
from cleanup_fixture import Fixture
from paperflow_prefect.intake import object_fingerprint, verify_inventory
from paperflow_prefect.records import identity, read_json
from paperflow_prefect.cleanup_evidence import validate_scope
from paperflow_prefect.archive import archive_originals


def h21_paper(f):
    rows = []
    for old in f.paper['archive_files']:
        target = f.person / Path(old['path']).name
        Path(old['path']).rename(target)
        row = dict(old, path=str(target), relative=target.name,
                   file_id='BODY_frozen' if old['kind'] == 'body' else 'ATT_frozen',
                   state='已接收', source={'source_sha256':old['sha256']},
                   upstream_file_id=old['file_id'], group_root=str(f.person),
                   object_fingerprint=object_fingerprint(target.stat()))
        rows.append(row)
    paper = dict(f.paper, receipt_schema='paperflow-receipt/1', paper_id='PF_formal_received_identity',
                 archive_files=rows, body=rows[0], body_copies=[rows[0]], attachments=[rows[1]],
                 revision=identity([(r['kind'], r['file_id'], r['sha256']) for r in rows])[:24])
    return paper


class H21ScopeTests(unittest.TestCase):
    def setUp(self):
        self.f = Fixture()
        self.addCleanup(self.f.close)
        self.paper = h21_paper(self.f)

    def test_formal_identity_and_flat_paths_are_accepted_without_reassignment(self):
        try:
            person = validate_scope(self.paper, self.f.library)
        except ValueError as exc:
            self.fail('H2.1 identity or finite flat paths are not supported: ' + str(exc))
        self.assertEqual(person, self.f.person)
        self.assertEqual(self.paper['paper_id'], 'PF_formal_received_identity')

    def test_unknown_flat_file_does_not_expand_frozen_archive_inventory(self):
        extra = self.f.person / 'unknown.bin'
        extra.write_bytes(b'unclassified and retained')
        try:
            self.assertTrue(verify_inventory(self.paper))
        except ValueError as exc:
            self.fail('H2.1 inventory incorrectly rescanned the old three-folder layout: ' + str(exc))
        self.assertTrue(extra.exists())
        self.assertEqual(len(self.paper['archive_files']), 2)

    def test_native_archive_preserves_both_frozen_files_and_formal_identity(self):
        try:
            result = archive_originals(self.paper, self.f.work / 'h21-archive', self.f.root / 'h21-nas', reserve_bytes=0)
        except ValueError as exc:
            self.fail('native archive cannot receive H2.1 inventory: ' + str(exc))
        self.assertEqual(result['paper_id'], 'PF_formal_received_identity')
        self.assertEqual(result['revision'], self.paper['revision'])
        self.assertEqual({r['name'] for r in result['files']}, {'paper.txt', 'source.zip'})
        self.assertEqual(result['status'], 'verified')

    def test_h21_still_rejects_other_library_and_rebound_file_membership(self):
        bad = copy.deepcopy(self.paper)
        bad['archive_files'][0]['path'] = str(self.f.root / 'outside.txt')
        with self.assertRaises(ValueError):
            validate_scope(bad, self.f.library)

    def test_receipt_content_verification_precedes_stage4_object_freeze(self):
        paper = copy.deepcopy(self.paper)
        for key in ('archive_files', 'body_copies', 'attachments'):
            for row in paper[key]:
                row.pop('object_fingerprint', None)
        paper['body'].pop('object_fingerprint', None)
        try:
            self.assertTrue(verify_inventory(paper))
        except ValueError as exc:
            self.fail('read-only H2.1 verification incorrectly requires stage4 augmentation: ' + str(exc))
        self.assertTrue(all(Path(r['path']).exists() for r in paper['archive_files']))
        bad = copy.deepcopy(self.paper)
        bad['attachments'] = []
        with self.assertRaises(ValueError):
            validate_scope(bad, self.f.library)
