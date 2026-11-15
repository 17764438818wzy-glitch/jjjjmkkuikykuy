import asyncio
import copy
import html
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import unquote
from contextlib import nullcontext
from paperflow_prefect.records import Records, read_json, sha_file, write_json
from paperflow_prefect.settings import Settings, ExecutionDisabled
from test_receipt_h21 import handoff


class HandoffIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from paperflow_prefect.legacy import bind
        bind(Settings.load())

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve(); self.library = self.root / 'library'; self.library.mkdir()
        self.handoff = handoff(self.library, self.library / 'person', attachments=[('附件/code.txt', b'print(1)')])
        self.hpath = self.root / 'handoff.json'; write_json(self.hpath, self.handoff)
        from paperflow_prefect.receipts import ReceiptStore
        from paperflow_prefect.intake_handoff import receive_handoff
        self.receipt = receive_handoff(self.hpath, ReceiptStore(self.root / 'receipts'))
        from paperflow_prefect.stage3_handoff import read_receipt
        _, self.paper = read_receipt(self.receipt['record_path'])
        self.row = self.paper['attachments'][0]
        from paperflow_prefect.backend import Backend
        self.backend = object.__new__(Backend); self.backend.root = self.root / 'data'; self.backend.root.mkdir()
        self.backend.model_signature='fixture'; self.backend.body_signature='fixture'; self.backend.stops={}
        self.backend.local_scope=nullcontext
        write_json(self.root / 'manifest.json', {})
        self.backend.settings=Settings(dict(execution_enabled=True, library_root=str(self.library),
            data_root=str(self.backend.root), source_manifest=str(self.root / 'manifest.json')))
        self.sent = []
        self.conn=dict(model='fixture', api_key='fixture-only', base_url='http://127.0.0.1:9/v1', max_tokens=2048, timeout=1)
        def invoke(kind, request, connection, work, **kwargs):
            from preextract_request import request_once
            self.sent.append((kind, request))
            class Reply(io.BytesIO): headers={'Content-Type':'application/json'}
            class Opener:
                def open(inner, *args, **kw):
                    raw=dict(kind='源码文件', summary='包含一段程序。', evidence_refs=['text-preview'], limitations=[])
                    return Reply(json.dumps({'choices':[{'message':{'content':json.dumps(raw,ensure_ascii=False)},
                                          'finish_reason':'stop'}]}).encode())
            return request_once(request, connection, work, opener=Opener(), **kwargs)
        self.backend.invoke=invoke
        from paperflow_prefect.stage3_handoff import register_received
        register_received(self.backend, self.receipt['record_path'])

    def test_fresh_llm_result_and_md_are_one_delivery_then_reused_after_model_change(self):
        from paperflow_prefect.request_admission import confirm_first_request
        from paperflow_prefect.stage3_handoff import attachment_deliveries
        work = self.backend.attachment_work(self.paper,self.row)
        confirm_first_request(self.paper,self.row,work,history_roots=[self.backend.root])
        with patch('paperflow_prefect.legacy.connection', return_value=self.conn):
            description=self.backend.attachment(self.paper,self.row)
        self.assertEqual(len(self.sent),1)
        saved = {p: sha_file(p) for p in (work / 'model').iterdir() if p.is_file()}
        self.backend.model_signature='another-config'
        with patch('paperflow_prefect.legacy.connection',side_effect=AssertionError('reuse must not connect')):
            self.assertEqual(description,self.backend.attachment(self.paper,self.row))
        self.assertEqual(len(self.sent),1)
        self.assertEqual(saved,{p:sha_file(p) for p in saved})
        rows=attachment_deliveries(self.backend,self.paper,{'attachments':{self.row['file_id']:{'status':'complete'}}})
        self.assertEqual(len(rows),1)
        md=Path(rows[0]['md_path']).read_text(encoding='utf8')
        self.assertIn('源码文件',md);self.assertIn('包含一段程序。',md)
        self.assertEqual(Path(rows[0]['source_path']).name,'示例论文_附件01.txt')
        self.assertEqual(rows[0]['original_source_path'],self.row['path'])
        self.assertEqual(sha_file(rows[0]['source_path']),self.row['sha256'])
        from paperflow_prefect.attachment_ingest import read_attachment_delivery
        checked=read_attachment_delivery(rows[0])
        self.assertEqual(checked['metadata']['source_name'],Path(self.row['path']).name)
        self.assertNotIn('description',rows[0])

    def test_received_identity_drift_is_rejected_before_any_work(self):
        from paperflow_prefect.stage3_handoff import read_receipt
        changed=copy.deepcopy(self.receipt);changed['inventory']['attachments'][0]['file_id']='wrong'
        target=self.root/'tampered.json';write_json(target,changed)
        with self.assertRaises(ValueError):read_receipt(target)
        self.assertEqual(self.sent,[])

    def test_body_journal_does_not_block_a_new_attachment_in_the_same_paper(self):
        from paperflow_prefect.request_admission import confirm_first_request, require_model_history
        work=self.backend.attachment_work(self.paper,self.row)
        confirm_first_request(self.paper,self.row,work,history_roots=[self.backend.root])
        old=self.backend.root/'papers'/self.paper['paper_id']/'body'/'old-signature'
        write_json(old/'material.json',dict(source=dict(source_sha256=self.paper['body']['sha256'])))
        write_json(old/'model/request.json',dict(model='existing-body-request'))
        require_model_history(self.paper,self.row,work)
        self.assertEqual(self.sent,[])

    def test_attachment_journal_does_not_block_a_new_body_in_the_same_paper(self):
        from paperflow_prefect.request_admission import confirm_first_request, require_model_history
        row=self.paper['body'];work=self.backend.body_work(self.paper)
        confirm_first_request(self.paper,row,work,history_roots=[self.backend.root])
        old=self.backend.root/'papers'/self.paper['paper_id']/'attachments'/self.row['file_id']/'old-signature'
        write_json(old/'evidence.json',dict(source_sha256=self.row['sha256']))
        write_json(old/'model/request.json',dict(model='existing-attachment-request'))
        require_model_history(self.paper,row,work)
        self.assertEqual(self.sent,[])

    def test_unattributed_or_same_item_journal_still_blocks_a_first_request(self):
        from paperflow_prefect.request_admission import confirm_first_request, require_model_history
        from paperflow_prefect.attachments import AttachmentError
        work=self.backend.attachment_work(self.paper,self.row)
        confirm_first_request(self.paper,self.row,work,history_roots=[self.backend.root])
        old=self.backend.root/'papers'/self.paper['paper_id']/'attachments'/self.row['file_id']/'old-signature'
        write_json(old/'model/request.json',dict(model='unknown-request'))
        with self.assertRaises(AttachmentError):require_model_history(self.paper,self.row,work)
        write_json(old/'evidence.json',dict(source_sha256=self.row['sha256']))
        with self.assertRaises(AttachmentError):require_model_history(self.paper,self.row,work)
        self.assertEqual(self.sent,[])

    def test_markdown_path_escape_roundtrip_distinguishes_literal_percent_sequence(self):
        from paperflow_prefect.attachment_card import _inline
        original='src/%0A|x\n[a]_b`c&d'
        escaped=_inline(original)
        self.assertIn('%250A',escaped)
        self.assertEqual(unquote(html.unescape(escaped)),original)

    def test_stage3_flow_disabled_switch_precedes_receipt_or_backend(self):
        from paperflow_prefect.flows import received_handoff
        with patch('paperflow_prefect.flows.Settings.load',return_value=Settings({'execution_enabled':False})), \
             patch('paperflow_prefect.flows.Backend',side_effect=AssertionError('must not construct')):
            with self.assertRaises(ExecutionDisabled):asyncio.run(received_handoff.fn('missing.json'))
