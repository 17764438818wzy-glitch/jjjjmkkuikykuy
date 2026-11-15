import copy
import importlib
from pathlib import Path
import sys
import unittest

import paperflow_prefect
from test_receipt_h21 import ReceiptFixture
from paperflow_prefect.records import read_json, sha_file
from paperflow_prefect.intake_handoff import receive_handoff
from paperflow_prefect.receipts import ReceiptStore


UPSTREAM=Path(__file__).resolve().parents[2]/'group-classification-20261028'/'paperflow_prefect'


def evidence(h):
    row=h['attachments'][0]
    return dict(schema='attachment-evidence-handoff/1',group_id=h['group_id'],
        source=dict(handoff_id=h['handoff_id'],result_version=h['result_version'],
                    file_id=row['file_id'],source_sha256=row['sha256']),evidence_version=1,
        read_scope={k:dict(status='unknown' if k=='request_records' else 'not_read',
                           reason='历史待核对' if k=='request_records' else None)
                    for k in ('directory','text','images','request_records','errors')},
        archive_listing=dict(evidence_id=None,status='not_read',total_entries=None,
                             listed_entries=0,entries=[],reason='未尝试读取'),
        text_samples=[],images=[],decision_basis=None,limitations=[],errors=[],
        existing_identification=None,request_records=[])


class SharedEvidenceTests(ReceiptFixture):
    @classmethod
    def setUpClass(cls):
        # Reuse window 1's actual module and pure helpers; never copy a validator.
        try:
            cls.shared=importlib.import_module('paperflow_prefect.attachment_evidence_handoff')
        except ModuleNotFoundError as exc:
            if exc.name!='paperflow_prefect.attachment_evidence_handoff':raise
            if not (UPSTREAM/'attachment_evidence_handoff.py').is_file():
                raise unittest.SkipTest('window 1 shared module not available')
            if str(UPSTREAM) not in paperflow_prefect.__path__:paperflow_prefect.__path__.append(str(UPSTREAM))
            cls.shared=importlib.import_module('paperflow_prefect.attachment_evidence_handoff')

    def make_evidence(self,h,value=None,directory='evidence'):
        return self.shared.write_evidence_handoff(value or evidence(h),self.base/directory,handoff=h)

    def chain(self,h):
        first=evidence(h)
        first['request_records']=[dict(status='unknown',purpose='attachment_identification',
            provenance=dict(kind='historical_reuse',record_ref='saved-request-state',run_ref=None,
                            entry_ref=h['attachments'][0]['file_id'],record_sha256=None))]
        first['read_scope']['request_records']=dict(status='partial',reason='历史请求结果未知')
        path1=self.make_evidence(h,first)
        second=copy.deepcopy(first)
        second.update(evidence_version=2,previous_evidence_version=1,request_records=[],limitations=['补证据'])
        second['read_scope']['request_records']=dict(status='unknown',reason='本版未补交请求记录')
        return path1,self.make_evidence(h,second)

    def test_latest_evidence_expands_and_pins_old_unknown_request_version(self):
        h=self.make(attachments=[('附件/a.zip',b'a')])
        first,latest=self.chain(h)
        r=receive_handoff(h,self.store,evidence_records=[latest])
        refs=r['items'][1]['evidence_records']
        self.assertEqual([x['evidence_version'] for x in refs],[1,2])
        self.assertEqual([x['path'] for x in refs],[str(first),str(latest)])
        self.assertEqual([x['sha256'] for x in refs],[sha_file(first),sha_file(latest)])
        self.assertFalse(r['items'][1]['history']['allow_new_request'])

    def test_changed_old_evidence_cannot_hide_behind_unchanged_latest_reference(self):
        from test_receipt_h21 import save
        h=self.make(attachments=[('附件/a.zip',b'a')])
        first,latest=self.chain(h)
        before=receive_handoff(h,self.store,evidence_records=[latest])
        changed=read_json(first);changed['request_records']=[];save(first,changed)
        after=receive_handoff(h,self.store,evidence_records=[latest])
        self.assertNotEqual(before['receipt_id'],after['receipt_id'])
        self.assertEqual(after['items'][1]['evidence_records'],[])
        self.assertTrue(after['items'][1]['unverified_evidence'])

    def test_incomplete_evidence_chain_is_not_forwarded_as_latest_only(self):
        h=self.make(attachments=[('附件/a.zip',b'a')])
        first,latest=self.chain(h);first.unlink()
        r=receive_handoff(h,self.store,evidence_records=[latest])
        self.assertEqual(r['state'],'已接收')
        self.assertEqual(r['items'][1]['evidence_records'],[])
        self.assertTrue(r['items'][1]['unverified_evidence'])

    def test_actual_shared_validator_binds_evidence_without_changing_official_identity(self):
        h=self.make(attachments=[('附件/a.zip',b'archive')])
        e=self.make_evidence(h)
        store=ReceiptStore(self.base/'shared-ledger',evidence_validator=self.shared.validate_evidence_handoff)
        first=receive_handoff(h,store,evidence_records=[e])
        ref=first['inventory']['attachments'][0]['evidence_records'][0]
        self.assertEqual(ref['path'],str(e))
        self.assertEqual(ref['sha256'],sha_file(e))
        self.assertEqual(ref['evidence_version'],1)
        self.assertEqual(first['items'][1]['source_binding']['group_id'],h['group_id'])
        e2=read_json(e);e2.update(evidence_version=2,previous_evidence_version=1,limitations=['新补充限制'])
        path2=self.make_evidence(h,e2)
        second=receive_handoff(h,store,evidence_records=[path2])
        self.assertEqual(first['paper_id'],second['paper_id'])
        self.assertEqual(first['items'][1]['attachment_id'],second['items'][1]['attachment_id'])
        self.assertEqual(second['new_items'],[])
        self.assertTrue(e.is_file())
        self.assertFalse(second['items'][1]['history']['allow_new_request'])

    def test_foreign_evidence_is_not_forwarded_but_does_not_block_verified_original(self):
        a=self.make(attachments=[('附件/a.zip',b'a')])
        b=self.make('学校/乙',attachments=[('附件/a.zip',b'a')])
        path=self.make_evidence(b)
        r=receive_handoff(a,self.store,evidence_records=[path])
        self.assertEqual(r['state'],'已接收')
        self.assertEqual(r['items'][1]['evidence_records'],[])

    def test_same_evidence_version_with_different_bytes_is_not_silently_accepted(self):
        from test_receipt_h21 import save
        h=self.make(attachments=[('附件/a.zip',b'a')])
        path=self.make_evidence(h)
        first=receive_handoff(h,self.store,evidence_records=[path])
        changed=read_json(path);changed['limitations']=['changed without new version']
        alternate=self.base/'alternate.json';save(alternate,changed)
        second=receive_handoff(h,self.store,evidence_records=[alternate])
        self.assertEqual(second['state'],'已接收')
        self.assertEqual(second['items'][1]['evidence_records'],[])
        self.assertTrue(second['items'][1]['unverified_evidence'])
        self.assertEqual(first['items'][1]['attachment_id'],second['items'][1]['attachment_id'])

    def test_invalid_schema_is_not_accepted_as_an_empty_evidence_record(self):
        from test_receipt_h21 import save
        h=self.make(attachments=[('附件/a.zip',b'a')])
        value=evidence(h);value['read_scope']=None
        path=self.base/'invalid.json';save(path,value)
        r=receive_handoff(h,self.store,evidence_records=[path])
        self.assertEqual(r['state'],'已接收')
        self.assertEqual(r['items'][1]['evidence_records'],[])
        self.assertTrue(r['items'][1]['unverified_evidence'])

    def test_non_object_evidence_is_a_limitation_and_cannot_block_valid_originals(self):
        from test_receipt_h21 import save
        h=self.make(attachments=[('附件/a.zip',b'a')])
        path=self.base/'invalid-list.json';save(path,[])
        r=receive_handoff(h,self.store,evidence_records=[path])
        self.assertEqual(r['state'],'已接收')
        self.assertEqual(len(r['inventory']['archive_files']),2)
        self.assertTrue(r['items'][1]['unverified_evidence'])

    def test_non_object_evidence_source_is_a_limitation(self):
        from test_receipt_h21 import save
        h=self.make(attachments=[('附件/a.zip',b'a')])
        path=self.base/'invalid-source.json';save(path,dict(source=[]))
        r=receive_handoff(h,self.store,evidence_records=[path])
        self.assertEqual(r['state'],'已接收')
        self.assertTrue(r['items'][1]['unverified_evidence'])


if __name__=='__main__':unittest.main()
