import hashlib
import json
from pathlib import Path
import tempfile
import unittest


class IntakeTests(unittest.TestCase):
    def test_confirmed_body_attachments_and_excluded_preserved(self):
        from paperflow_prefect.intake import inspect_person
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            person = root / '学校' / '001_甲'
            for name in ('正文', '附件', '垃圾材料'):
                (person / name).mkdir(parents=True)
            (person / '正文/a.docx').write_bytes(b'one manuscript')
            (person / '正文/duplicate.docx').write_bytes(b'one manuscript')
            (person / '附件/code.py').write_bytes(b'print(1)')
            (person / '垃圾材料/report.pdf').write_bytes(b'excluded')
            result = inspect_person(person, root)
            self.assertEqual(len(result['body_copies']), 2)
            self.assertEqual(len(result['attachments']), 1)
            self.assertEqual(result['excluded'], ['垃圾材料/report.pdf'])
            self.assertEqual(len(result['archive_files']), 3)
            self.assertEqual(len(list(person.rglob('*.*'))), 4)

    def test_distinct_bodies_do_not_silently_choose(self):
        from paperflow_prefect.intake import inspect_person, IntakeConflict
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); person = root / 'school/person'
            (person / '正文').mkdir(parents=True)
            (person / '正文/a.pdf').write_bytes(b'a')
            (person / '正文/b.docx').write_bytes(b'b')
            with self.assertRaises(IntakeConflict):
                inspect_person(person, root)

    def test_identity_distinguishes_people_and_revision_changes(self):
        from paperflow_prefect.intake import inspect_person
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); results = []
            for name in ('a', 'b'):
                person = root / 'school' / name
                (person / '正文').mkdir(parents=True)
                (person / '正文/a.pdf').write_bytes(b'same')
                results.append(inspect_person(person, root))
            self.assertNotEqual(results[0]['paper_id'], results[1]['paper_id'])
            (root / 'school/a/正文/a.pdf').write_bytes(b'changed')
            changed = inspect_person(root / 'school/a', root)
            self.assertEqual(results[0]['paper_id'], changed['paper_id'])
            self.assertNotEqual(results[0]['revision'], changed['revision'])

    def test_source_change_is_detected_before_effect(self):
        from paperflow_prefect.intake import inspect_person, verify_inventory
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); person = root / 'school/person'
            (person / '正文').mkdir(parents=True)
            file = person / '正文/a.pdf'; file.write_bytes(b'old')
            result = inspect_person(person, root)
            file.write_bytes(b'new')
            with self.assertRaises(ValueError): verify_inventory(result)


class RecordTests(unittest.TestCase):
    def test_cached_result_is_bound_to_artifact_hash(self):
        from paperflow_prefect.records import Records
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); records = Records(root)
            artifact = root / 'answer.txt'; artifact.write_text('saved', encoding='utf8')
            records.complete('profile', {'answer': str(artifact)}, [artifact])
            self.assertEqual(records.cached('profile')['answer'], str(artifact))
            artifact.write_text('changed', encoding='utf8')
            with self.assertRaises(ValueError): records.cached('profile')

    def test_pending_record_never_looks_complete(self):
        from paperflow_prefect.records import Records
        with tempfile.TemporaryDirectory() as directory:
            records = Records(Path(directory))
            records.started('rag')
            self.assertIsNone(records.cached('rag'))
            self.assertEqual(records.status('rag')['state'], 'running')


if __name__ == '__main__': unittest.main()
