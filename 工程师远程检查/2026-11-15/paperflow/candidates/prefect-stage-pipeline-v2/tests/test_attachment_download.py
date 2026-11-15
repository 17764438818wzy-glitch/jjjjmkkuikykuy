import importlib.util
from pathlib import Path
import tempfile
import unittest

from paperflow_prefect.records import read_json, write_json
from test_attachment_ingest import handoff
import test_attachment_publish as publishing


class AttachmentDownloadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): publishing.AttachmentPublishTests.setUpClass()
    def ready(self, root):
        client=publishing.FakeStore(); item=handoff(root)
        result=publishing.AttachmentPublishTests().ingest(item,client,root)
        return client,result
    def download(self, receipt, client, output):
        self.assertIsNotNone(importlib.util.find_spec('paperflow_prefect.attachment_download'), 'download receiver missing')
        from paperflow_prefect.attachment_download import download_attachment
        return download_attachment(receipt,client,output)

    def test_download_uses_bound_file_and_verifies_bytes(self):
        with tempfile.TemporaryDirectory() as root:
            client,receipt=self.ready(root); sent=list(client.sent); output=Path(root)/'downloads'/'code.txt'
            result=self.download(receipt['receipt_path'],client,output)
            self.assertEqual(output.read_bytes(),b'print(1)')
            self.assertEqual(result['sha256'],receipt['binding']['source_sha256'])
            self.assertEqual(client.sent,sent)

    def test_wrong_remote_bytes_are_not_saved(self):
        with tempfile.TemporaryDirectory() as root:
            client,receipt=self.ready(root); client.files[receipt['raw']['file_id']]['_bytes']=b'corrupt'
            output=Path(root)/'download.txt'
            with self.assertRaises(ValueError): self.download(receipt['receipt_path'],client,output)
            self.assertFalse(output.exists())

    def test_existing_different_output_and_source_are_preserved(self):
        with tempfile.TemporaryDirectory() as root:
            client,receipt=self.ready(root); output=Path(root)/'download.txt'; output.write_bytes(b'my existing file')
            with self.assertRaises(FileExistsError): self.download(receipt['receipt_path'],client,output)
            self.assertEqual(output.read_bytes(),b'my existing file')
            with self.assertRaises(ValueError):
                self.download(receipt['receipt_path'],client,receipt['raw']['local_original_path'])
            self.assertEqual(Path(receipt['raw']['local_original_path']).read_bytes(),b'print(1)')

    def test_wrong_service_or_unverified_receipt_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            client,receipt=self.ready(root); client.base_url='http://127.0.0.1:10'
            with self.assertRaises(ValueError): self.download(receipt['receipt_path'],client,Path(root)/'out')
            client.base_url='http://127.0.0.1:9'
            state=read_json(receipt['receipt_path']); state['raw']['status']='pending'
            write_json(receipt['receipt_path'],state)
            with self.assertRaises(ValueError): self.download(receipt['receipt_path'],client,Path(root)/'out')


if __name__=='__main__': unittest.main()
