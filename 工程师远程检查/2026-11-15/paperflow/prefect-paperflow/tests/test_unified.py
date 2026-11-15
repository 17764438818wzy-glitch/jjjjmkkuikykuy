import asyncio
from contextlib import nullcontext
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from paperflow_prefect.records import Records, read_json, write_json
from paperflow_prefect.settings import Settings
from test_receipt_h21 import handoff


class UnifiedTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.library = self.root / 'library'; self.library.mkdir()
        h = handoff(self.library, self.library / 'person', attachments=[('附件/code.txt', b'print(1)')])
        write_json(self.root / 'handoff.json', h)
        from paperflow_prefect.receipts import ReceiptStore
        from paperflow_prefect.intake_handoff import receive_handoff
        self.receipt = receive_handoff(self.root / 'handoff.json', ReceiptStore(self.root / 'receipts'))
        from paperflow_prefect.unified import UnifiedBackend
        self.backend = object.__new__(UnifiedBackend)
        self.backend.root = self.root / 'data'; self.backend.root.mkdir()
        self.backend.body_signature = self.backend.model_signature = 'fixture'
        self.backend.stops = {}; self.backend.local_scope = nullcontext
        write_json(self.root / 'manifest.json', {})
        self.backend.settings = Settings(dict(execution_enabled=True, library_root=str(self.library),
            data_root=str(self.backend.root), source_manifest=str(self.root / 'manifest.json'),
            nas_destination=str(self.root / 'nas'), nas_reserve_bytes=0, local_cleanup_enabled=False,
            history_roots=[str(self.backend.root)]))

    def test_receipt_intake_preserves_identity_and_is_repeatable(self):
        paper = self.backend.receive(self.receipt['record_path'])
        self.assertEqual(paper, self.backend.receive(self.receipt['record_path']))
        self.assertEqual(paper['paper_id'], self.receipt['paper_id'])
        self.assertEqual(paper['revision'], self.receipt['inventory']['revision'])
        saved = read_json(self.backend.paper_root(paper) / 'received-inventory.json')
        self.assertEqual(saved, self.receipt['inventory'])
        self.assertEqual(self.backend.receipt_for(paper)['receipt_id'], self.receipt['receipt_id'])

    def test_receipt_outside_configured_input_root_is_rejected(self):
        wrong = self.root / 'wrong'; wrong.mkdir()
        self.backend.settings = Settings(dict(self.backend.settings.values, library_root=str(wrong)))
        with self.assertRaises(ValueError): self.backend.receive(self.receipt['record_path'])
        self.assertFalse(list(self.backend.root.glob('papers/*')))

    def test_partial_received_items_can_run_but_never_finish_whole_paper(self):
        from paperflow_prefect.unified import execute_unified
        paper = self.backend.receive(self.receipt['record_path']); calls=[]
        async def call(stage, paper, row=None):
            calls.append(stage)
            return {'status': 'complete'}
        receipt = dict(self.receipt, state='未接收', unresolved=[{'reason':'unknown'}])
        result = asyncio.run(execute_unified(paper, receipt, call))
        self.assertIn('profile',calls); self.assertIn('attachment',calls)
        self.assertNotIn('archive',calls); self.assertNotIn('cleanup',calls)
        self.assertEqual(result['status'],'incomplete')

    def test_cleanup_incomplete_is_not_replaced_by_complete(self):
        from paperflow_prefect.unified import execute_unified
        paper = self.backend.receive(self.receipt['record_path']); calls=[]
        async def call(stage, paper, row=None):
            calls.append(stage)
            return {'status':'incomplete','failures':{'cleanup':'retained'}} if stage=='finish' else {'status':'verified'}
        result = asyncio.run(execute_unified(paper,self.receipt,call))
        self.assertEqual(calls[-3:],['archive','cleanup','finish'])
        self.assertEqual(result['status'],'incomplete')

    def test_stage5_resume_skips_models_packages_and_uploads(self):
        from paperflow_prefect.unified import execute_unified
        paper = self.backend.receive(self.receipt['record_path']); calls=[]
        async def call(stage, paper, row=None): calls.append(stage); return {'status':'complete'}
        result = asyncio.run(execute_unified(paper,self.receipt,call,resume_stage5=True))
        self.assertEqual(calls,['archive','cleanup','finish'])
        self.assertEqual(result['status'],'complete')

    def test_stage4_resume_skips_prepare_profile_and_identification(self):
        from paperflow_prefect.unified import execute_unified
        paper = self.backend.receive(self.receipt['record_path']); calls=[]
        async def call(stage, paper, row=None): calls.append(stage); return {'status':'complete'}
        asyncio.run(execute_unified(paper,self.receipt,call,resume_rag=True))
        self.assertEqual(calls,['recover_body','publish_attachments','archive','cleanup','finish'])

    def test_cleanup_off_does_not_claim_new_contract_complete(self):
        paper = self.backend.receive(self.receipt['record_path'])
        with patch.object(self.backend,'stage5_work',return_value=self.root/'work'):
            result=self.backend.cleanup(paper)
        self.assertEqual(result['status'],'cleanup_pending')
        self.assertEqual(result['removed_files'],0)

    def test_empty_attachments_needs_no_rag_client(self):
        paper=self.backend.receive(self.receipt['record_path']);paper=dict(paper,attachments=[])
        with patch('paperflow_prefect.legacy.rag_client',side_effect=AssertionError('no network')):
            result=self.backend.publish_attachments(paper)
        self.assertEqual(result['attachments'],0)
        self.assertEqual(result['status'],'verified')
