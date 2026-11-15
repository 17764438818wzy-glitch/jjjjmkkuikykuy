from pathlib import Path
import tempfile
import unittest
from paperflow_prefect.legacy import bind
from paperflow_prefect.settings import Settings

PARENT = {'paperflow_task': 'PRE_fixture', 'paperflow_source_sha256': 'b'*64,
          'paperflow_manifest_sha256': 'c'*64, 'paperflow_revision':'1',
          'paperflow_specialty':'080901', 'paperflow_role':'body'}


class FakeRag:
    """An in-memory server with lost-response faults, never a network client."""
    def __init__(self):
        self.docs = {}; self.rows = {}; self.uploads = 0; self.posts = 0; self.lose_chunk_response = False
        self.docs['body-doc'] = {'id':'body-doc', 'name':'fixture-body.md', 'status':'1', 'meta_fields':dict(PARENT)}
        self.rows['body-doc'] = []
    def upload_bytes(self, ds, name, data):
        self.uploads += 1; did = f'doc-{self.uploads}'
        self.docs[did] = {'id': did, 'name': name, 'meta_fields': {}, 'status': '0', '_bytes': data}
        self.rows[did] = []; return did
    def request(self, method, endpoint, body=None):
        from urllib.parse import urlsplit, parse_qs
        url = urlsplit(endpoint); parts = url.path.strip('/').split('/'); query = parse_qs(url.query)
        if endpoint == '/retrieval':
            return {'chunks': list(self.rows[body['document_ids'][0]])}
        if len(parts) == 3 and parts[2] == 'documents':
            docs = list(self.docs.values())
            for key in ('id', 'name'):
                if key in query: docs = [r for r in docs if r[key] == query[key][0]]
            return {'docs': docs}
        did = parts[3]
        if len(parts) == 4 and method == 'PATCH':
            self.docs[did].update(body)
            if 'enabled' in body: self.docs[did]['status'] = str(body['enabled'])
            return self.docs[did]
        if parts[-1] == 'chunks' and method == 'GET':
            return {'chunks': list(self.rows[did]), 'total': len(self.rows[did])}
        if parts[-1] == 'chunks' and method == 'POST':
            self.posts += 1
            row = {'id': f'chunk-{self.posts}', 'document_id': did, 'content': body['content']}
            self.rows[did].append(row)
            if self.lose_chunk_response:
                self.lose_chunk_response = False
                raise TimeoutError('response lost after server write')
            return {'chunk': row}
        raise AssertionError((method, endpoint))


class DescriptionDeliveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): bind(Settings.load())

    def test_lost_chunk_response_reuses_existing_content(self):
        from paperflow_prefect.delivery import publish_description
        client = FakeRag(); client.lose_chunk_response = True
        description = {'schema':'attachment-description/1', 'paper_id':'PF_fixture',
            'attachment_id':'att_fixture', 'source_sha256':'a'*64, 'format':'.py',
            'kind':'Python源码', 'summary':'实现作业排序。', 'limitations':[], 'evidence_refs':['text-preview']}
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / 'receipt.json'
            with self.assertRaises(TimeoutError):
                publish_description(description, 'fixture-dataset', 'body-doc', client, receipt, parent_identity=PARENT)
            result = publish_description(description, 'fixture-dataset', 'body-doc', client, receipt, parent_identity=PARENT)
            self.assertEqual(result['status'], 'verified')
            self.assertEqual(client.uploads, 1)
            self.assertEqual(client.posts, 1)
            self.assertEqual(client.docs[result['document_id']]['meta_fields']['paper_id'], 'PF_fixture')

    def test_unknown_upload_with_no_readback_does_not_resend(self):
        from paperflow_prefect.delivery import publish_description
        from paperflow_prefect.records import write_json
        client = FakeRag()
        description = {'schema':'attachment-description/1', 'paper_id':'PF_fixture',
            'attachment_id':'att_fixture', 'source_sha256':'a'*64, 'format':'.py',
            'kind':'Python源码', 'summary':'排序。', 'limitations':[], 'evidence_refs':[]}
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / 'receipt.json'
            def unknown(*args): raise TimeoutError('unknown upload outcome')
            client.upload_bytes = unknown
            with self.assertRaises(TimeoutError): publish_description(description, 'ds', 'body-doc', client, receipt, parent_identity=PARENT)
            client.upload_bytes = lambda *a: self.fail('must not reupload')
            with self.assertRaises(RuntimeError): publish_description(description, 'ds', 'body-doc', client, receipt, parent_identity=PARENT)

    def test_missing_parent_blocks_before_any_upload(self):
        from paperflow_prefect.delivery import publish_description
        client = FakeRag(); client.docs.clear()
        description = {'paper_id':'PF_fixture', 'attachment_id':'att_fixture', 'source_sha256':'a'*64,
            'format':'.py', 'kind':'Python源码', 'summary':'排序', 'limitations':[]}
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ConnectionError):
                publish_description(description, 'ds', 'body-doc', client, Path(directory)/'r.json', parent_identity=PARENT)
        self.assertEqual(client.uploads, 0)


if __name__ == '__main__': unittest.main()
