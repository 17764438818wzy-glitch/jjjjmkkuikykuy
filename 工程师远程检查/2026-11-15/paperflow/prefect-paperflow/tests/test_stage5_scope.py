"""Real local-file checks for explicit disposable caches, never a broad sweep."""
import os
from pathlib import Path
import unittest
from cleanup_fixture import Fixture, Crash
from paperflow_prefect.records import identity, read_json, sha_file, write_json
from paperflow_prefect.intake import object_fingerprint


def cache_item(f, name='extract.txt', **updates):
    path = f.work / 'reproducible-cache' / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'isolated reproducible cache\n')
    row = dict(path=str(path), root=str(path.parent), kind='extracted_text',
               paper_id=f.paper['paper_id'], revision=f.paper['revision'],
               bytes=path.stat().st_size, sha256=sha_file(path),
               object_fingerprint=object_fingerprint(path.stat()))
    row.update(updates)
    owner = dict(schema='paperflow-intermediate-ownership/1',
                 paper_id=f.paper['paper_id'], revision=f.paper['revision'],
                 producer_state='complete', finalized=True, reproducible=True,
                 owners=[dict(paper_id=f.paper['paper_id'], revision=f.paper['revision'])],
                 references=[], active_consumers=[], files=[dict(row)])
    owner_path = f.work / 'ownership' / (name + '.json')
    write_json(owner_path, owner)
    row['producer_record'] = dict(path=str(owner_path), sha256=sha_file(owner_path))
    return row


def change_owner(row, **values):
    path = row['producer_record']['path']
    value = read_json(path)
    value.update(values)
    write_json(path, value)
    row['producer_record']['sha256'] = sha_file(path)


class Stage5ScopeTests(unittest.TestCase):
    def setUp(self):
        self.f = Fixture()
        self.addCleanup(self.f.close)

    def freeze(self, rows):
        try:
            return self.f.manager().freeze(self.f.paper, self.f.work, intermediates=rows)
        except TypeError as exc:
            self.fail('explicit intermediate scope is not supported: ' + str(exc))

    def test_only_frozen_owned_cache_is_removed_and_nas_contains_originals_only(self):
        row = cache_item(self.f)
        extra = Path(row['root']) / 'unknown.bin'
        extra.write_bytes(b'not in frozen list')
        self.freeze([row])
        result = self.f.manager().run(self.f.work)
        self.assertEqual(result['removed_files'], 3)
        self.assertFalse(Path(row['path']).exists())
        self.assertTrue(extra.exists())
        self.assertEqual(len(read_json(self.f.work / 'archive/nas-receipt.json')['files']), 2)
        self.assertTrue(Path(row['producer_record']['path']).exists())
        self.f.assert_protected(self)

    def test_shared_unknown_and_referenced_caches_are_preserved(self):
        rows = [cache_item(self.f, name) for name in ('shared.txt', 'unknown.txt', 'referenced.txt')]
        change_owner(rows[0], owners=[dict(paper_id='another-paper', revision='other')])
        rows[1].pop('producer_record')
        change_owner(rows[2], references=['another-consumer'])
        self.freeze(rows)
        result = self.f.manager().run(self.f.work)
        self.assertEqual(result['removed_files'], 2)
        self.assertEqual(len(result['retained']), 3)
        self.assertTrue(all(Path(r['path']).exists() for r in rows))
        self.f.assert_protected(self)

    def test_final_markdown_and_recovery_evidence_cannot_be_classified_as_cache(self):
        row = cache_item(self.f, 'attachment.md')
        evidence = cache_item(self.f, 'evidence.txt')
        evidence['path'] = str(self.f.work / 'inventory.json')
        evidence.update(bytes=Path(evidence['path']).stat().st_size,
                        sha256=sha_file(evidence['path']),
                        object_fingerprint=object_fingerprint(Path(evidence['path']).stat()))
        self.freeze([row, evidence])
        result = self.f.manager().run(self.f.work)
        self.assertEqual(result['removed_files'], 2)
        self.assertEqual(len(result['retained']), 2)
        self.assertTrue(Path(row['path']).exists())
        self.assertTrue(Path(evidence['path']).exists())
        self.assertIsNotNone(self.f.manager().lookup(self.f.person))

    def test_changed_cache_is_retained_and_not_added_to_archive(self):
        row = cache_item(self.f)
        self.freeze([row])
        Path(row['path']).write_bytes(b'changed after freeze')
        result = self.f.manager().run(self.f.work)
        self.assertEqual(result['removed_files'], 2)
        self.assertEqual(len(result['retained']), 1)
        self.assertTrue(Path(row['path']).exists())

    def test_cache_interruption_reconciles_without_reprocessing_originals(self):
        row = cache_item(self.f)
        self.freeze([row])
        def crash(stage, path):
            if stage == 'after_unlink' and path == row['path']:
                raise Crash()
        with self.assertRaises(Crash):
            self.f.manager().run(self.f.work, checkpoint=crash)
        result = self.f.manager().run(self.f.work)
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(result['removed_files'], 2)
        self.assertEqual(result['reconciled_absent_files'], 1)
        self.assertEqual(self.f.manager().lookup(self.f.person)['action'], 'complete')

    def test_changed_producer_record_blocks_all_deletion(self):
        row = cache_item(self.f)
        self.freeze([row])
        change_owner(row, active_consumers=['new-reader'])
        with self.assertRaises(ValueError):
            self.f.manager().run(self.f.work)
        self.assertTrue(self.f.body.exists())
        self.assertTrue(self.f.attachment.exists())
        self.assertTrue(Path(row['path']).exists())

    def test_external_hardlink_and_busy_cache_stay_local(self):
        from paperflow.original_archive.lifecycle import locked_file
        shared = cache_item(self.f, 'hardlink.txt')
        busy = cache_item(self.f, 'busy.txt')
        other = self.f.root / 'external-link.txt'
        os.link(shared['path'], other)
        self.freeze([shared, busy])
        with locked_file(Path(busy['path'])):
            result = self.f.manager().run(self.f.work)
        self.assertEqual(result['removed_files'], 2)
        self.assertEqual(len(result['retained']), 1)
        self.assertEqual(len(result['deferred']), 1)
        self.assertTrue(other.exists())
        result = self.f.manager().run(self.f.work)
        self.assertEqual(result['removed_files'], 3)
        self.assertTrue(Path(shared['path']).exists())
        self.assertFalse(Path(busy['path']).exists())

    def test_unverified_nas_or_rag_never_removes_caches(self):
        row = cache_item(self.f)
        self.f.records.failed('rag-body', ValueError('fixture incomplete'))
        try:
            with self.assertRaises(ValueError):
                self.f.manager().freeze(self.f.paper, self.f.work, intermediates=[row])
        except TypeError as exc:
            self.fail('explicit intermediate scope is not supported: ' + str(exc))
        self.assertTrue(self.f.body.exists())
        self.assertTrue(self.f.attachment.exists())
        self.assertTrue(Path(row['path']).exists())
