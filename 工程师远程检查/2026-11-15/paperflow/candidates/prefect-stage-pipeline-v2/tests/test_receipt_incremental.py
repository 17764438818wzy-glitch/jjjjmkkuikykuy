import copy
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import test_receipt_h21 as support
from test_receipt_h21 import ReceiptFixture, handoff, save, version
from paperflow_prefect.records import sha_file
from paperflow_prefect.receipts import ReceiptStore, ReceiptError
from paperflow_prefect.intake_handoff import receive_handoff, received_items, batch_summary


class IncrementalTests(ReceiptFixture):
    def test_failed_item_does_not_consume_first_accepted_version_event(self):
        from paperflow_prefect.receipt_history import observe_history
        h=self.make(attachments=[('附件/a.zip',b'a')])
        def temporarily_unreadable(kind,*args,**kwargs):
            if kind=='attachment':raise OSError('temporary history read failure')
            return observe_history(kind,*args,**kwargs)
        with patch('paperflow_prefect.intake_handoff.observe_history',side_effect=temporarily_unreadable):
            first=receive_handoff(h,self.store)
        self.assertEqual(first['state'],'未接收')
        self.assertEqual([r['kind'] for r in received_items(first)],['body'])
        second=receive_handoff(h,self.store)
        self.assertEqual(second['state'],'已接收')
        self.assertEqual([r['kind'] for r in second['new_items']],['attachment'])

    def test_reused_group_id_cannot_silently_claim_another_source_directory(self):
        a=self.make('学校/甲')
        first=receive_handoff(a,self.store)
        b=self.make('学校/乙',body=b'foreign body')
        b['group_id']=a['group_id']
        rows=([b['body']] if b['body'] else [])+b['attachments']+b['trash']+b['unresolved']
        from paperflow_prefect.records import identity
        b['handoff_id']=identity(['group-handoff/1',b['group_id'],
            [(r['relative'],r['bytes'],r['sha256']) for r in sorted(rows,key=lambda x:x['relative'].casefold())]])[:32]
        version(b)
        with self.assertRaises(ReceiptError):receive_handoff(b,self.store)
        again=receive_handoff(a,self.store)
        self.assertEqual(first,again)

    def test_pending_attachment_can_resume_without_new_body_identity(self):
        h=self.make(attachments=[('附件/a.zip',b'archive')])
        path=Path(h['attachments'][0]['path']);path.unlink()
        first=receive_handoff(h,self.store)
        path.write_bytes(b'archive')
        second=receive_handoff(h,self.store)
        self.assertEqual(first['state'],'未接收')
        self.assertEqual(second['state'],'已接收')
        self.assertEqual(first['items'][0]['file_id'],second['items'][0]['file_id'])
        self.assertEqual(second['new_items'],[dict(kind='attachment',file_id=second['items'][1]['file_id'],
                                                 source_sha256=h['attachments'][0]['sha256'])])
        self.assertEqual(batch_summary([first,second]),dict(total=1,received=1,remaining=0))
        self.assertEqual(len(list((self.base/'ledger'/'receipts').glob('*.json'))),2)

    def test_attachment_only_supplement_uses_previously_received_parent(self):
        first=receive_handoff(self.make(),self.store)
        h=self.make(body=None,attachments=[('附件/supplement.zip',b'new')])
        r=receive_handoff(h,self.store)
        self.assertEqual(r['state'],'已接收')
        self.assertEqual(r['paper_id'],first['paper_id'])
        self.assertIsNone(r['inventory']['body'])
        self.assertEqual(len(r['inventory']['attachments']),1)

    def test_attachment_can_be_received_before_body_and_later_body_keeps_parent_id(self):
        h=self.make(body=None,attachments=[('附件/a.zip',b'a')])
        r=receive_handoff(h,self.store)
        self.assertEqual(r['state'],'已接收')
        self.assertTrue(r['paper_id'])
        self.assertEqual([x['kind'] for x in received_items(r)],['attachment'])
        self.assertIsNone(r['inventory']['body'])
        later=receive_handoff(self.make(attachments=[('附件/a.zip',b'a')]),self.store)
        self.assertEqual(later['paper_id'],r['paper_id'])
        self.assertEqual(later['items'][1]['attachment_id'],r['items'][0]['attachment_id'])

    def test_two_simultaneous_receipts_get_one_identity_and_one_snapshot(self):
        h=self.make(attachments=[('附件/a.zip',b'a')])
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:receive_handoff(h,ReceiptStore(self.base/'concurrent')),range(2)))
        self.assertEqual(results[0],results[1])
        self.assertEqual(len(list((self.base/'concurrent'/'receipts').glob('*.json'))),1)

    def test_committed_receipt_can_recover_missing_output_without_renumbering(self):
        h=self.make()
        first=receive_handoff(h,self.store)
        path=Path(first['record_path']);saved=path.read_bytes();path.unlink()
        second=receive_handoff(h,self.store)
        self.assertEqual(first,second)
        self.assertEqual(path.read_bytes(),saved)

    def test_source_change_is_not_hidden_by_a_new_evidence_note(self):
        h=self.make(attachments=[('附件/a.zip',b'a')])
        first=receive_handoff(h,self.store)
        Path(h['attachments'][0]['path']).write_bytes(b'changed')
        changed=version(dict(h,title_note='supplement'))
        second=receive_handoff(changed,self.store)
        self.assertEqual(second['state'],'未接收')
        self.assertEqual(first['paper_id'],second['paper_id'])
        self.assertEqual([x['kind'] for x in received_items(second)],['body'])


class HistoryPersistenceTests(ReceiptFixture):
    legacy=support.LegacyHistoryTests.legacy

    def test_replay_rechecks_new_historical_attachment_id_conflict_without_blocking_body(self):
        h=self.make(attachments=[('附件/a.zip',b'a')])
        first=receive_handoff(h,self.store)
        root,_,_=self.legacy(h,pid=first['paper_id'])
        inventory=next(root.rglob('inventory.json'))
        saved=json.loads(inventory.read_text(encoding='utf8'))
        saved['body']['file_id']=first['items'][0]['file_id']
        save(inventory,saved)
        second=receive_handoff(h,ReceiptStore(self.base/'ledger',history_roots=[root]))
        self.assertEqual(second['state'],'未接收')
        self.assertEqual([r['kind'] for r in received_items(second)],['body'])
        self.assertTrue(Path(first['record_path']).is_file())

    def test_malformed_historical_inventory_cannot_block_verified_original(self):
        h=self.make()
        root=self.base/'bad-history';save(root/'inventory.json',[])
        r=receive_handoff(h,ReceiptStore(self.base/'bad-history-ledger',history_roots=[root]))
        self.assertEqual(r['state'],'已接收')
        self.assertFalse(r['items'][0]['history']['allow_new_request'])
        self.assertTrue(r['items'][0]['history']['limitations'])

    def test_malformed_historical_request_stays_unknown_in_original_workspace(self):
        h=self.make(attachments=[('附件/a.zip',b'a')])
        root,_,work=self.legacy(h,unknown=True)
        save(work/'model'/'request-state.json',[])
        r=receive_handoff(h,ReceiptStore(self.base/'bad-request-ledger',history_roots=[root]))
        self.assertEqual(r['state'],'已接收')
        history=r['items'][1]['history']
        self.assertEqual(history['policy'],'inspect_saved_request')
        self.assertFalse(history['allow_new_request'])

    def test_output_root_cannot_be_an_ancestor_of_original_library(self):
        h=self.make()
        with self.assertRaises(ReceiptError):receive_handoff(h,ReceiptStore(self.base))
        self.assertFalse((self.base/'receipts.sqlite3').exists())

    def test_explicit_group_migration_preserves_pending_attachments_legacy_identity(self):
        h=self.make(attachments=[('附件/a.zip',b'a')])
        root,_,att_work=self.legacy(h,unknown=True)
        Path(h['attachments'][0]['path']).unlink()
        store=ReceiptStore(self.base/'migrate-pending',history_roots=[root])
        first=receive_handoff(h,store)
        self.assertEqual(first['state'],'未接收')
        moved=self.make('搬家后/甲',attachments=[('附件/a.zip',b'a')])
        second=receive_handoff(moved,store,origin_group_id=first['origin_group_id'])
        self.assertEqual(second['state'],'已接收')
        self.assertEqual(second['paper_id'],first['paper_id'])
        self.assertEqual(second['items'][1]['attachment_id'],'historic-att-01')
        self.assertEqual(second['items'][1]['history']['attachment_work'],str(att_work))
        self.assertEqual(second['items'][1]['history']['policy'],'inspect_saved_request')
        self.assertFalse(second['items'][1]['history']['allow_new_request'])
        self.assertEqual(second['new_items'],[])

    def test_replay_does_not_hide_newly_discovered_historical_id_conflict(self):
        h=self.make()
        first=receive_handoff(h,self.store)
        root,_,_=self.legacy(h)
        second=receive_handoff(h,ReceiptStore(self.base/'ledger',history_roots=[root]))
        self.assertEqual(first['state'],'已接收')
        self.assertEqual(second['state'],'未接收')
        self.assertEqual(received_items(second),[])
        self.assertTrue(Path(first['record_path']).is_file())

    def test_ambiguous_legacy_owner_is_rejected_even_without_incoming_paper_id(self):
        h=self.make('学校/甲')
        root,_,_=self.legacy(h)
        original=next(root.rglob('inventory.json'))
        foreign=json.loads(original.read_text(encoding='utf8'))
        other=self.make('学校/乙',body=b'other body')
        foreign.update(person_dir=other['group_root'],body=other['body'],
                       body_copies=[other['body']],archive_files=[other['body']])
        save(root/'papers'/'PF_HISTORIC'/'revisions'/'foreign'/'signature'/'inventory.json',foreign)
        before={p:p.read_bytes() for p in root.rglob('*') if p.is_file()}
        r=receive_handoff(h,ReceiptStore(self.base/'ambiguous-ledger',history_roots=[root]))
        self.assertEqual(r['state'],'未接收')
        self.assertIsNone(r['paper_id'])
        self.assertEqual(received_items(r),[])
        self.assertEqual({p:p.read_bytes() for p in before},before)

    def test_historical_attachment_sequence_is_reserved_before_new_attachments(self):
        h=self.make(attachments=[('附件/z.zip',b'z')])
        root,_,_=self.legacy(h)
        inv=next(root.rglob('inventory.json'))
        value=json.loads(inv.read_text(encoding='utf8'))
        value['attachments'][0]['sequence']=7
        save(inv,value)
        newer=self.make(attachments=[('附件/a.zip',b'a'),('附件/z.zip',b'z')])
        r=receive_handoff(newer,ReceiptStore(self.base/'sequence-ledger',history_roots=[root]))
        self.assertEqual(r['state'],'已接收')
        old=next(x for x in r['items'] if x['relative']=='附件/z.zip')
        added=next(x for x in r['items'] if x['relative']=='附件/a.zip')
        self.assertEqual(old['sequence'],7)
        self.assertEqual(added['sequence'],8)
        self.assertEqual(old['attachment_id'],'historic-att-01')
        self.assertEqual(r['new_items'],[dict(kind='attachment',file_id=added['file_id'],source_sha256=added['sha256'])])

    def test_history_locations_survive_new_evidence_without_rescanning_old_roots(self):
        h=self.make(attachments=[('附件/a.zip',b'a')])
        root,body,att=self.legacy(h)
        store_root=self.base/'history-ledger'
        first=receive_handoff(h,ReceiptStore(store_root,history_roots=[root]))
        newer=version(dict(h,title_note='additional evidence'))
        second=receive_handoff(newer,ReceiptStore(store_root))
        self.assertEqual(second['paper_id'],first['paper_id'])
        self.assertEqual(second['items'][0]['history']['body_work'],str(body))
        self.assertEqual(second['items'][1]['history']['attachment_work'],str(att))
        self.assertEqual(second['items'][1]['history']['policy'],'reuse_complete')

    def test_tampered_saved_response_is_unknown_and_does_not_authorize_resend(self):
        h=self.make(attachments=[('附件/a.zip',b'a')])
        root,body,att=self.legacy(h)
        (att/'model'/'response.txt').write_text('tampered',encoding='utf8')
        r=receive_handoff(h,ReceiptStore(self.base/'tamper-ledger',history_roots=[root]))
        self.assertEqual(r['state'],'已接收')
        self.assertEqual(r['items'][1]['history']['policy'],'inspect_saved_request')
        self.assertIsNone(r['items'][1]['history']['existing_identification'])
        self.assertFalse(r['items'][1]['history']['allow_new_request'])

    def test_95_successful_body_workspaces_keep_identity_and_history_bytes(self):
        roots=[];handoffs=[];expected=[]
        # Synthetic saved work, not a real-paper/model rerun.
        for i in range(95):
            h=self.make('学校/人'+str(i),body=('body-'+str(i)).encode())
            root,body,_=self.legacy(h,pid='PF_OLD_'+str(i))
            handoffs.append(h);expected.append((h,'PF_OLD_'+str(i),str(body)))
        before={p:p.read_bytes() for p in root.rglob('*') if p.is_file()}
        store=ReceiptStore(self.base/'95-ledger',history_roots=[root])
        for h,pid,work in expected:
            r=receive_handoff(h,store)
            self.assertEqual(r['paper_id'],pid)
            self.assertEqual(r['items'][0]['history']['body_work'],work)
            self.assertEqual(r['items'][0]['history']['policy'],'reuse_complete')
            self.assertFalse(r['items'][0]['history']['allow_new_request'])
        self.assertEqual({p:p.read_bytes() for p in before},before)


if __name__=='__main__':unittest.main()
