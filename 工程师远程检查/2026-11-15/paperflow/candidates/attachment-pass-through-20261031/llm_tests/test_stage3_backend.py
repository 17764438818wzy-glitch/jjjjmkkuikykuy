from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from paperflow_prefect.records import Records, identity, read_json, sha_file, write_json
from paperflow_prefect.settings import Settings


class Stage3BackendTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        from paperflow_prefect.backend import Backend
        self.backend = object.__new__(Backend)
        self.backend.root = self.root / 'data'; self.backend.body_signature = 'new-signature'
        self.backend.model_signature = 'new-model'; self.backend.stops = {'p': threading.Event()}
        self.backend.settings = Settings(dict(execution_enabled=True, library_root=str(self.root / 'library'),
            data_root=str(self.backend.root), source_manifest=str(self.root / 'manifest.json')))
        write_json(self.root / 'manifest.json', {})
        self.work = self.root / 'old-work'; self.work.mkdir()
        source = self.root / 'library/body.docx'; source.parent.mkdir(); source.write_bytes(b'original')
        self.row = dict(path=str(source), file_id='a', name='body.docx', relative='body.docx',
            sha256=sha_file(source), bytes=8, history=dict(body_work=str(self.work), attachment_work=str(self.work),
            work_dirs=[str(self.work)], policy='reuse_complete', allow_new_request=False, limitations=[]))
        self.paper = dict(paper_id='p', body=self.row, attachments=[self.row])

    def test_original_work_directory_wins_over_new_model_signature(self):
        self.assertEqual(self.backend.body_work(self.paper), self.work)
        self.assertEqual(self.backend.attachment_work(self.paper, self.row), self.work)

    def test_unknown_or_ambiguous_history_cannot_fall_through_to_empty_directory(self):
        self.row['history']['body_work'] = None
        self.row['history']['work_dirs'].append(str(self.root / 'other-work'))
        with self.assertRaises(ValueError): self.backend.body_work(self.paper)

    def test_cached_profile_skips_preparation_and_connection(self):
        write_json(self.work / 'material.json', {'retained': True})
        Records(self.work).complete('profile', {'candidate': 'saved'})
        with patch('paperflow_prefect.body.prepare_body', side_effect=AssertionError('already successful')):
            result = self.backend.prepare(self.paper)
        self.assertEqual(result['material'], str(self.work / 'material.json'))
        self.assertEqual(self.backend.profile(self.paper), {'candidate': 'saved'})

    def test_cached_attachment_produces_md_without_model_or_description_rewrite(self):
        description = dict(schema='attachment-description/1', paper_id='p', attachment_id='a',
            source_sha256=self.row['sha256'], kind='文本', summary='已有说明。', format='.docx',
            evidence_refs=[], limitations=[])
        write_json(self.work / 'description.json', description)
        Records(self.work).complete('identify', description, [self.work / 'description.json'])
        before = sha_file(self.work / 'description.json')
        with patch('paperflow_prefect.legacy.connection', side_effect=AssertionError('must not load')):
            self.assertEqual(self.backend.attachment(self.paper, self.row), description)
        card = read_json(self.work / 'cards/current.json')
        self.assertTrue(Path(card['path']).is_file())
        self.assertEqual(sha_file(self.work / 'description.json'), before)

    def test_no_history_does_not_authorize_model(self):
        from paperflow_prefect.request_admission import require_model_history
        self.row['history'] = dict(policy='history_lookup_required', work_dirs=[], allow_new_request=False)
        with self.assertRaises(ValueError): require_model_history(self.paper, self.row, self.work)

    def test_recorded_work_survives_empty_later_receipt_and_model_change(self):
        from paperflow_prefect.request_admission import bind_work
        bind_work(self.backend.root, self.paper, self.row, 'attachment', self.work)
        self.row['history'] = dict(work_dirs=[], attachment_work=None, policy='history_lookup_required')
        self.backend.model_signature = 'changed-model'
        self.assertEqual(self.backend.attachment_work(self.paper, self.row), self.work)

    def test_first_request_confirmation_cannot_cover_known_unknown_request(self):
        from paperflow_prefect.request_admission import confirm_first_request
        write_json(self.work / 'model/request.json', {'model': 'old'})
        with self.assertRaises(ValueError):
            confirm_first_request(self.paper, self.row, self.work, history_roots=[self.root])
