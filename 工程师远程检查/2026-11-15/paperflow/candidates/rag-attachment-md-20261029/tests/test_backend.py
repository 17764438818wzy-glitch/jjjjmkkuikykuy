import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from paperflow_prefect.settings import Settings, ExecutionDisabled
from paperflow_prefect.records import read_json, write_json


class BackendTests(unittest.TestCase):
    def test_disabled_execution_does_not_load_credentials_or_scan(self):
        from paperflow_prefect.backend import Backend
        settings = Settings({'execution_enabled': False})
        with self.assertRaises(ExecutionDisabled): Backend(settings)

    def test_package_without_profile_cannot_call_model(self):
        from paperflow_prefect.body import package_body
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FileNotFoundError):
                package_body(Path(directory), {}, {}, None)

    def test_unknown_model_response_is_not_resent(self):
        from paperflow_prefect.legacy import bind
        from paperflow_prefect.body import extract_profile
        bind(Settings.load())
        import preextract_request
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_json(root / 'material.json', {'source': {}})
            write_json(root / 'model/request.json', {'model': 'fixture', 'messages': []})
            write_json(root / 'model/request-state.json', {'status': 'request_failed_or_uncertain'})
            with patch.object(preextract_request, 'request_once', side_effect=AssertionError('must not send')):
                with self.assertRaises(RuntimeError):
                    extract_profile(root, Path('unused'), {}, {'model': 'fixture'}, lambda *a, **k: self.fail('must not send'))

    def test_attachment_evidence_is_bounded_and_does_not_execute_code(self):
        from paperflow_prefect.attachments import evidence_for
        from paperflow_prefect.records import sha_file
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); source = root / 'source.py'
            source.write_text('raise RuntimeError("MUST NOT EXECUTE")\n' * 5000)
            result = evidence_for({'path': str(source), 'sha256': sha_file(source)}, root / 'reading')
            self.assertLessEqual(len(result['text']), 24000)
            self.assertTrue(result['bounded'])
            self.assertEqual(result['format'], '.py')

    def test_nas_copy_verifies_all_originals_and_keeps_local(self):
        from paperflow_prefect.legacy import bind
        from paperflow_prefect.archive import archive_originals
        from paperflow_prefect.intake import inspect_person
        from paperflow_prefect.records import sha_file
        bind(Settings.load())
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); library = root / 'library'; person = library / 'school/person'
            (person / '正文').mkdir(parents=True); (person / '附件').mkdir()
            (person / '正文/a.docx').write_bytes(b'original manuscript')
            (person / '附件/code.zip').write_bytes(b'original archive')
            paper = inspect_person(person, library)
            result = archive_originals(paper, root / 'records', root / 'nas', reserve_bytes=0)
            self.assertEqual(result['status'], 'verified')
            self.assertEqual(len(result['files']), 2)
            self.assertFalse(result['local_release_enabled'])
            for row in paper['archive_files']:
                self.assertEqual(sha_file(row['path']), row['sha256'])
            second = archive_originals(paper, root / 'records', root / 'nas', reserve_bytes=0)
            self.assertEqual(second['manifest_sha256'], result['manifest_sha256'])


if __name__ == '__main__': unittest.main()
