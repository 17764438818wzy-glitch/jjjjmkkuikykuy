import hashlib
import tempfile
import unittest
import io
from pathlib import Path

from ingest_gateway import RagflowGateway, SourceChanged, reconcile_write


class FakeClient:
    def __init__(self, docs=None, error=None):
        self.docs = docs or []
        self.error = error
        self.upload_calls = 0
        self.parse_calls = 0
        self.patch_calls = 0

    def documents(self, dataset_id, name=None, document_id=None):
        if self.error:
            raise self.error
        return [row for row in self.docs
                if (name is None or row['name'] == name)
                and (document_id is None or row['id'] == document_id)]

    def upload(self, dataset_id, path, remote_name=None):
        self.upload_calls += 1
        return 'remote-1'

    def parse(self, dataset_id, document_id):
        self.parse_calls += 1

    def request(self, method, endpoint, payload):
        self.patch_calls += 1
        self.docs[0]['pipeline_id'] = payload['pipeline_id']


class GatewayTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'ingest.md'
        self.path.write_bytes(b'paper text')
        self.hash = hashlib.sha256(b'paper text').hexdigest()
        self.task = {'dataset_id': 'md', 'source_id': 'paper_123',
                     'remote_name': f'paper_123_{self.hash[:16]}.md',
                     'content_sha256': self.hash, 'path': str(self.path),
                     'pipeline_id': 'pipe-1', 'remote_id': None}

    def test_known_remote_id_can_be_reconciled_by_identity(self):
        task = dict(self.task, remote_id='remote-1',
                    parse_baseline={'remote_id':'remote-1','process_begin_at':None})
        client = FakeClient([{'id': 'remote-1', 'name': task['remote_name'],
                              'run': 'UNSTART', 'pipeline_id': 'pipe-1'}])
        gateway = RagflowGateway(lambda: client)
        self.assertEqual(reconcile_write(task, 'upload', gateway), 'confirmed')
        self.assertEqual(reconcile_write(task, 'parse', gateway), 'uncertain')
        client.docs[0]['run'] = 'RUNNING'
        client.docs[0]['process_begin_at'] = '2026-09-27T01:00:00'
        self.assertEqual(reconcile_write(task, 'parse', gateway), 'confirmed')

    def test_lost_upload_ack_name_alone_is_insufficient(self):
        client = FakeClient([{'id': 'remote-1', 'name': self.task['remote_name'],
                              'run': 'UNSTART'}])
        gateway = RagflowGateway(lambda: client)
        self.assertEqual(reconcile_write(self.task, 'upload', gateway), 'uncertain')
        self.assertEqual(client.upload_calls, 0)

    def test_verified_remote_bytes_allow_adoption_after_lost_ack(self):
        client = FakeClient([{'id': 'remote-1', 'name': self.task['remote_name'],
                              'run': 'UNSTART'}])
        gateway = RagflowGateway(lambda: client,
                                 remote_content_reader=lambda client, dataset, doc: b'paper text')
        self.assertEqual(reconcile_write(self.task, 'upload', gateway), 'confirmed')

    def test_real_client_default_reconciles_lost_ack_only_after_download_hash_matches(self):
        from ragflow_client import RagflowClient
        from types import SimpleNamespace
        client=RagflowClient('http://127.0.0.1:8088','test-key')
        client.documents=lambda *a,**kw:[{'id':'remote-1','name':self.task['remote_name']}]
        requests=[]
        payload=[b'paper text']
        def download(request,timeout):
            requests.append(request)
            return io.BytesIO(payload[0])
        client.opener=SimpleNamespace(open=download)
        gateway=RagflowGateway(lambda:client)
        self.assertEqual(reconcile_write(self.task,'upload',gateway),'confirmed')
        self.assertEqual(requests[0].full_url,'http://127.0.0.1:8088/api/v1/datasets/md/documents/remote-1')
        self.assertEqual(requests[0].get_method(),'GET')
        payload[0]=b'different source'
        self.assertEqual(reconcile_write(self.task,'upload',gateway),'uncertain')

    def test_network_lookup_failure_remains_uncertain(self):
        gateway = RagflowGateway(lambda: FakeClient(error=TimeoutError('network')))
        self.assertEqual(reconcile_write(self.task, 'upload', gateway), 'uncertain')

    def test_changed_or_missing_queued_file_never_uploads(self):
        client = FakeClient()
        gateway = RagflowGateway(lambda: client)
        self.path.write_bytes(b'changed text')
        with self.assertRaises(SourceChanged):
            gateway.upload(self.task)
        self.path.unlink()
        with self.assertRaises(SourceChanged):
            gateway.upload(self.task)
        self.assertEqual(client.upload_calls, 0)

    def test_parse_binds_expected_pipeline_on_exact_remote_id(self):
        task = dict(self.task, remote_id='remote-1')
        client = FakeClient([{'id': 'remote-1', 'name': task['remote_name'],
                              'run': 'UNSTART', 'pipeline_id': 'old'}])
        gateway = RagflowGateway(lambda: client)
        gateway.parse(task)
        self.assertEqual(client.patch_calls, 1)
        self.assertEqual(client.parse_calls, 1)

    def test_routed_parse_cannot_rebind_running_or_unexpected_pipeline(self):
        from ingest_gateway import IdentityMismatch
        task = dict(self.task, remote_id='remote-1', pipeline_id='official-pipe',
                    requested_pipeline_id='pipe-1', execution_attempt=1)
        client = FakeClient([{'id':'remote-1','name':task['remote_name'],
                              'run':'RUNNING','pipeline_id':'pipe-1'}])
        with self.assertRaises(IdentityMismatch):
            RagflowGateway(lambda:client).parse(task)
        client.docs[0].update(run='FAIL',pipeline_id='unrelated-pipe')
        with self.assertRaises(IdentityMismatch):
            RagflowGateway(lambda:client).parse(task)
        self.assertEqual(client.patch_calls,0)
        self.assertEqual(client.parse_calls,0)

    def test_changed_package_privacy_receipt_blocks_upload_even_when_primary_is_unchanged(self):
        from evidence import package_fingerprint
        package=Path(self.temp.name)
        metadata=package/'metadata.json'
        (package/'document.md').write_text('paper text',encoding='utf-8')
        metadata.write_text('{"privacy_status":"approved"}',encoding='utf-8')
        task=dict(self.task,package_path=str(package),
                  package_sha256=package_fingerprint(package))
        metadata.write_text('{"privacy_status":"blocked"}',encoding='utf-8')
        client=FakeClient()
        with self.assertRaises(SourceChanged):
            RagflowGateway(lambda:client).upload(task)
        self.assertEqual(client.upload_calls,0)


if __name__ == '__main__':
    unittest.main()
