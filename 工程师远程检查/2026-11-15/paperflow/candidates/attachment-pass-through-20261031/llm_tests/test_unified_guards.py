from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
from paperflow_prefect.records import write_json
from paperflow_prefect.unified_guards import audit_legacy_uploads,StopAwareClient
from paperflow_prefect.settings import Settings


class GuardTests(unittest.TestCase):
    def test_unavailable_explicit_receipt_history_is_not_a_new_upload(self):
        from paperflow_prefect.unified_attachments import receipt_root_for
        with tempfile.TemporaryDirectory() as folder:
            backend=SimpleNamespace(root=Path(folder),settings=Settings({'attachment_receipt_roots':[str(Path(folder)/'missing')]}))
            with self.assertRaises(ValueError):receipt_root_for(backend,{'paper_id':'p'},{'file_id':'a'})
    def test_pending_native_manifest_and_supplement_plan_prevent_new_upload(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);backend=SimpleNamespace(body_work=lambda p:root/'body',attachment_work=lambda p,r:root/'attachment',
                settings=Settings({'rag_history_roots':[str(root)]}))
            paper={'body':{'sha256':'b'*64}};row={'sha256':'a'*64}
            for name in ('private/associated_pending/job/manifest.json','private/associated_pending/job/attachment-plan.json',
                         'private/associated-supplements/job/plan.json','private/associated-supplements/job/receipt.json'):
                path=root/name;write_json(path,{'identity':{'source_sha256':'b'*64},'files':[{'sha256':'a'*64}]})
                with self.assertRaises(ValueError):audit_legacy_uploads(backend,paper,row)
                path.unlink()
    def test_cancel_blocks_new_writes_but_allows_readback(self):
        native=Mock(base_url='http://127.0.0.1:9');stop=threading.Event();client=StopAwareClient(native,stop);stop.set()
        with self.assertRaises(InterruptedError):client.upload_bytes('ds','n',b'data')
        with self.assertRaises(InterruptedError):client.request('POST','/files',{})
        native.upload_bytes.assert_not_called();native.request.assert_not_called()
        client.request('GET','/files');native.request.assert_called_once_with('GET','/files',None)
