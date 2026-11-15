import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from paperflow_prefect.legacy import bind
from paperflow_prefect.records import read_json, write_json
from paperflow_prefect.settings import PROJECT, Settings
from local_rag import LocalRag
from rag_fixture import native_fixture


class RecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        bind(Settings.load())
        fixture = native_fixture()
        cls.prepared = read_json(Path(fixture['root']) / 'body/prepared-publication.json')

    def initial(self, client, receipt):
        from preextract_publish import deliver_publication
        return deliver_publication(self.prepared, config=None, receipt_path=receipt, client=client)

    def recover(self, client, receipt):
        from paperflow_prefect.recovery import recover_publication
        with patch('paperflow_prefect.legacy.connection', side_effect=AssertionError('model credentials requested')):
            with patch('preextract_request.request_once', side_effect=AssertionError('model invoked')):
                return recover_publication(self.prepared, receipt_path=receipt, client=client)

    def test_lost_chunk_response_adopts_original_and_only_sends_never_submitted_chunks(self):
        client = LocalRag(); client.lose_chunk = True
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / 'receipt.json'
            with self.assertRaises(TimeoutError): self.initial(client, receipt)
            original = read_json(receipt); original_id = original['targets']['080901']['body']['document_id']
            result = self.recover(client, receipt)
            self.assertEqual(result['status'], 'complete')
            self.assertEqual(result['targets']['080901']['body']['document_id'], original_id)
            self.assertEqual(client.uploads, 1)
            contents = [c['content'] for rows in client.chunks.values() for c in rows]
            self.assertEqual(len(contents), len(set(contents)))
            self.assertEqual(client.posts, len(contents))
            self.assertTrue(list(Path(directory).glob('receipt.before-resume.*.json')))
            uploads, posts = client.uploads, client.posts
            self.recover(client, receipt)
            self.assertEqual((client.uploads, client.posts), (uploads, posts))

    def test_lost_upload_response_adopts_exact_existing_document(self):
        client = LocalRag(); client.lose_upload = True
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / 'receipt.json'
            with self.assertRaises(TimeoutError): self.initial(client, receipt)
            result = self.recover(client, receipt)
            self.assertEqual(result['status'], 'complete')
            self.assertEqual(client.uploads, 1)
            self.assertEqual(result['targets']['080901']['body']['document_id'], 'doc-1')

    def test_unknown_missing_chunk_is_not_resent(self):
        client = LocalRag(); client.drop_chunk = True
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / 'receipt.json'
            with self.assertRaises(TimeoutError): self.initial(client, receipt)
            before = receipt.read_bytes(); count = len(client.calls)
            with self.assertRaises(RuntimeError): self.recover(client, receipt)
            self.assertEqual(receipt.read_bytes(), before)
            self.assertEqual((client.uploads, client.posts), (1, 1))
            self.assertTrue(all(c[0] == 'GET' for c in client.calls[count:]))

    def test_missing_original_document_is_never_recreated(self):
        client = LocalRag()
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / 'receipt.json'; self.initial(client, receipt)
            key = next(iter(client.docs)); client.docs.pop(key)
            before = receipt.read_bytes(); uploads = client.uploads; count = len(client.calls)
            with self.assertRaises(ConnectionError): self.recover(client, receipt)
            self.assertEqual(client.uploads, uploads)
            self.assertEqual(receipt.read_bytes(), before)
            self.assertTrue(all(c[0] == 'GET' for c in client.calls[count:]))

    def test_conflicting_manifest_is_rejected_before_remote_access(self):
        client = LocalRag()
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / 'receipt.json'; self.initial(client, receipt)
            changed = read_json(receipt); changed['manifest_sha256'] = '0' * 64; write_json(receipt, changed)
            count = len(client.calls)
            with self.assertRaises(ValueError): self.recover(client, receipt)
            self.assertEqual(len(client.calls), count)

    def test_extra_remote_chunk_cannot_be_overwritten_or_marked_complete(self):
        client = LocalRag()
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / 'receipt.json'; self.initial(client, receipt)
            key = next(iter(client.docs)); client.chunks[key].append(
                dict(id='foreign', document_id=key[1], content='wrong paper', available=True))
            count = len(client.calls)
            with self.assertRaises(ConnectionError): self.recover(client, receipt)
            self.assertTrue(all(c[0] == 'GET' for c in client.calls[count:]))


if __name__ == '__main__': unittest.main()
