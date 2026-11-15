import copy
from pathlib import Path
import tempfile
import unittest
from paperflow_prefect.records import sha_file, write_json
from test_receipt_h21 import handoff


class AttachmentInputTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();library=self.root/'library';library.mkdir()
        self.handoff=handoff(library,library/'person',attachments=[('附件/code.zip',b'archive')])
        source=self.handoff['attachments'][0]
        self.origin=dict(kind='current_read',record_ref=None,run_ref=None,entry_ref=None,record_sha256=None)
        def state(status,reason=None):return dict(status=status,reason=reason)
        self.record=dict(schema='attachment-evidence-handoff/1',group_id=self.handoff['group_id'],
            source=dict(handoff_id=self.handoff['handoff_id'],result_version=self.handoff['result_version'],
                        file_id=source['file_id'],source_sha256=source['sha256']),evidence_version=1,
            previous_evidence_version=None,
            read_scope=dict(directory=state('partial','只有部分目录'),text=state('available'),images=state('not_read'),
                            request_records=state('unknown','上游没有任务台账'),errors=state('available')),
            archive_listing=dict(evidence_id='directory',status='partial',total_entries=None,listed_entries=1,
                entries=[dict(path='main.py',type=None,bytes=10)],reason='类型未核实',provenance=self.origin),
            text_samples=[dict(evidence_id='readme',text='main.py is the entry point',
                location=dict(member_path='README.txt',page=None,line=None,region=None),
                extraction_method='bounded_text',truncated=None,provenance=self.origin)],
            images=[],decision_basis=dict(classification='attachment',provenance=self.origin),
            limitations=['未运行项目'],errors=[],existing_identification=None,request_records=[])

    def test_bounded_input_preserves_unknown_truncation_and_avoids_classification_summary(self):
        from paperflow_prefect.attachment_input import model_evidence
        from paperflow_prefect.attachment_input import checked_handoff_evidence
        path=self.root/'evidence.json';write_json(path,self.record)
        report=checked_handoff_evidence(self.record,self.handoff,trusted_sidecar=dict(path=str(path),sha256=sha_file(path)))
        result=model_evidence(self.record,self.handoff,report=report)
        self.assertIn('readme',result['refs']);self.assertIn('"truncated": null',result['text'])
        self.assertNotIn('decision_basis',result)
        self.assertEqual(result['images'],[])
        self.assertIn('包内目录不表示已读取成员内容',result['limitations'])

    def test_unlocatable_history_is_retained_but_not_forwarded_as_verified_input(self):
        from paperflow_prefect.attachment_input import model_evidence, card_evidence, checked_handoff_evidence
        origin=dict(self.origin,kind='historical_reuse',record_ref=str(self.root/'lost.json'),record_sha256='a'*64)
        self.record['archive_listing']['provenance']=origin
        self.record['text_samples'][0]['provenance']=origin
        before=copy.deepcopy(self.record)
        result=model_evidence(self.record,self.handoff)
        self.assertEqual(result['refs'],['file-facts']);self.assertEqual(result['text'],'')
        projected=card_evidence(self.record,checked_handoff_evidence(self.record,self.handoff))
        self.assertEqual(projected['archive_listing']['status'],'unavailable')
        self.assertEqual(self.record,before)

    def test_ref_hash_and_same_group_binding_are_required_on_receipt_consumption(self):
        from paperflow_prefect.attachment_input import load_received_evidence
        hpath=self.root/'handoff.json';epath=self.root/'evidence.json'
        write_json(hpath,self.handoff);write_json(epath,self.record)
        row=dict(self.handoff['attachments'][0],source_binding=dict(self.record['source'],group_id=self.record['group_id']),
            handoff_ref=dict(path=str(hpath),sha256=sha_file(hpath)),
            evidence_records=[dict(path=str(epath),sha256=sha_file(epath),evidence_version=1)])
        self.assertEqual(load_received_evidence(row)[0],self.record)
        row['source_binding']['group_id']='other'
        with self.assertRaises(ValueError):load_received_evidence(row)

    def test_workbook_is_not_treated_as_an_attachment_archive(self):
        import zipfile
        from paperflow_prefect.attachments import evidence_for
        p=self.root/'book.xlsx'
        with zipfile.ZipFile(p,'w') as z:z.writestr('xl/workbook.xml','<workbook/>')
        result=evidence_for(dict(path=str(p),sha256=sha_file(p)),self.root/'reading')
        self.assertEqual(result['archive_listing']['status'],'not_archive')
        self.assertNotIn('xl/workbook.xml',result['text'])

    def test_sparse_handoff_can_reuse_local_preview_before_first_identification(self):
        from unittest.mock import patch
        from paperflow_prefect.attachments import evidence_for
        projected=dict(schema='attachment-evidence/1',format='.pdf',text='',images=[],refs=['file-facts'],
            limitations=[],read_scope={'images':{'status':'not_read','reason':None}})
        local=dict(text='local caption',images=[dict(path='retained.png',ref='page-1',sha256='x')],
            refs=['page-1'],limitations=['仅一页预览'])
        with patch('paperflow_prefect.attachment_input.load_received_evidence',return_value=({}, {}, {})), \
             patch('paperflow_prefect.attachment_input.model_evidence',return_value=projected), \
             patch('paperflow_prefect.attachments._local_evidence',return_value=local):
            result=evidence_for(dict(path='drawing.pdf'),self.root/'preview')
        self.assertEqual(result['images'],local['images'])
        self.assertIn('page-1',result['refs'])
        self.assertEqual(result['read_scope']['images']['status'],'partial')
