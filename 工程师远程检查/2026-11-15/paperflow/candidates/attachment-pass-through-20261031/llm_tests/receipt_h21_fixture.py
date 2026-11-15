import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from paperflow_prefect.records import identity, sha_file
from paperflow_prefect.receipts import ReceiptStore, ReceiptError
from paperflow_prefect.intake_handoff import (
    receive_handoff, received_items, accepted_paper, batch_summary, verify_received_file,
)


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf8')


def handoff(library, person, body=b'body', attachments=(), trash=(), unresolved=()):
    person.mkdir(parents=True, exist_ok=True)
    gid = identity(['source-group/1', str(library).casefold(),
                    person.relative_to(library).as_posix().casefold()])[:32]
    def row(relative, content):
        path = person / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return dict(file_id=identity([gid, relative.casefold()])[:24],
                    path=str(path), relative=relative, bytes=len(content),
                    sha256=hashlib.sha256(content).hexdigest())
    b = row('正文/original.docx', body) if body is not None else None
    a = [row(name, data) for name, data in attachments]
    t = [dict(row(name, data), reason='confirmed excluded') for name, data in trash]
    u = [dict(row(name, data), reason='not classified') for name, data in unresolved]
    rows = sorted(([b] if b else []) + a + t + u, key=lambda r:r['relative'].casefold())
    result = dict(schema='paperflow-group-handoff/2', group_id=gid,
                  library_root=str(library), group_root=str(person),
                  group_relative=person.relative_to(library).as_posix(),
                  handoff_id=identity(['group-handoff/1',gid,
                      [(r['relative'],r['bytes'],r['sha256']) for r in rows]])[:32],
                  paper_id=None, existing_paper_id=None,
                  body=b, attachments=a, trash=t, unresolved=u,
                  paper_title='示例论文', paper_title_candidate=None,
                  classification_complete=not u,
                  ready_for_receipt=not u and (b is not None or not a),
                  attachment_status='有附件' if a else '尚未确认' if u else '空白')
    result['result_version'] = identity(result)[:32]
    return result


def version(value):
    value['result_version'] = identity({k:v for k,v in value.items() if k!='result_version'})[:32]
    return value


class ReceiptFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name).resolve()
        self.library = self.base / 'library'
        self.library.mkdir()
        self.store = ReceiptStore(self.base/'ledger')

    def tearDown(self):
        self.temporary.cleanup()

    def make(self, name='学校/甲', **kwargs):
        return handoff(self.library, self.library/name, **kwargs)


class ReceiptTests(ReceiptFixture):

    def test_body_and_attachment_are_received_with_official_ids_and_source_binding(self):
        h = self.make(attachments=[('附件/source.zip',b'archive')])
        before = {p:sha_file(p) for p in (Path(h['body']['path']),Path(h['attachments'][0]['path']))}
        r = receive_handoff(h,self.store)
        self.assertEqual(r['state'],'已接收')
        self.assertEqual(len(received_items(r)),2)
        att = next(x for x in r['items'] if x['kind']=='attachment')
        self.assertTrue(att['attachment_id'])
        self.assertEqual(att['paper_id'],r['paper_id'])
        self.assertEqual(att['source'],dict(handoff_id=h['handoff_id'],result_version=h['result_version'],
            file_id=h['attachments'][0]['file_id'],source_sha256=h['attachments'][0]['sha256']))
        self.assertEqual(att['display_name'],'示例论文_附件01.zip')
        self.assertEqual({p:sha_file(p) for p in before},before)
        self.assertFalse(att['history']['allow_new_request'])

    def test_missing_attachment_does_not_block_body_and_partial_group_is_not_complete(self):
        h = self.make(attachments=[('附件/missing.rar',b'archive')])
        Path(h['attachments'][0]['path']).unlink()
        r = receive_handoff(h,self.store)
        self.assertEqual(r['state'],'未接收')
        self.assertEqual([x['kind'] for x in received_items(r)],['body'])
        self.assertEqual(r['progress'],dict(body_received=1,body_total=1,
            attachments_received=0,attachments_total=1,unresolved=0))
        p = accepted_paper(r)
        self.assertIsNotNone(p['body'])
        self.assertEqual(p['attachments'],[])
        self.assertEqual(batch_summary([r]),dict(total=1,received=0,remaining=1))

    def test_unresolved_keeps_group_unreceived_but_verified_body_can_advance(self):
        h=self.make(unresolved=[('other.bin',b'unknown')])
        r=receive_handoff(h,self.store)
        self.assertEqual(r['state'],'未接收')
        self.assertEqual(len(received_items(r)),1)
        self.assertFalse(h['ready_for_receipt'])
        self.assertEqual(r['upstream']['ready_for_receipt'],False)
        self.assertEqual(len(r['unresolved']),1)

    def test_trash_cannot_be_opened_or_hashed_during_receipt(self):
        h=self.make(trash=[('垃圾材料/gone.pdf',b'excluded')])
        Path(h['trash'][0]['path']).unlink()
        r=receive_handoff(h,self.store)
        self.assertEqual(r['state'],'已接收')
        self.assertEqual([x['kind'] for x in received_items(r)],['body'])
        self.assertEqual(len(r['retained_trash']),1)

    def test_trash_only_and_empty_groups_do_not_create_paper_tasks(self):
        for name,kwargs in [('学校/垃圾',dict(body=None,trash=[('垃圾/a.txt',b'a')])),
                            ('学校/空',dict(body=None))]:
            with self.subTest(name=name):
                r=receive_handoff(self.make(name,**kwargs),self.store)
                self.assertEqual(r['state'],'已接收')
                self.assertIsNone(r['paper_id'])
                self.assertEqual(received_items(r),[])
                self.assertIsNone(accepted_paper(r))

    def test_same_content_and_names_in_other_groups_never_merge(self):
        a=receive_handoff(self.make('学校/甲',attachments=[('附件/a.zip',b'same')]),self.store)
        b=receive_handoff(self.make('学校/乙',attachments=[('附件/a.zip',b'same')]),self.store)
        self.assertNotEqual(a['paper_id'],b['paper_id'])
        self.assertNotEqual(a['items'][1]['attachment_id'],b['items'][1]['attachment_id'])

    def test_replayed_receipt_reuses_ids_receipt_and_immutable_files(self):
        h=self.make(attachments=[('附件/a.zip',b'a')])
        a=receive_handoff(h,self.store)
        snapshots={p:p.read_bytes() for p in (self.base/'ledger'/'receipts').glob('*.json')}
        b=receive_handoff(h,self.store)
        self.assertEqual(a,b)
        self.assertEqual({p:p.read_bytes() for p in snapshots},snapshots)
        self.assertEqual(len(list((self.base/'ledger'/'receipts').glob('*.json'))),1)

    def test_extra_evidence_version_does_not_renumber_or_allow_model(self):
        h=self.make(attachments=[('附件/a.zip',b'a')])
        a=receive_handoff(h,self.store)
        h=version(dict(h,title_note='additional cover evidence'))
        b=receive_handoff(h,self.store)
        self.assertEqual(a['paper_id'],b['paper_id'])
        self.assertEqual(a['items'][1]['attachment_id'],b['items'][1]['attachment_id'])
        self.assertEqual(b['items'][1]['history']['policy'],'history_lookup_required')
        self.assertFalse(b['items'][1]['history']['allow_new_request'])
        self.assertEqual(len(list((self.base/'ledger'/'receipts').glob('*.json'))),2)

    def test_added_attachment_is_incremental_and_keeps_existing_sequence(self):
        h=self.make(attachments=[('附件/z.zip',b'z')])
        a=receive_handoff(h,self.store)
        h=self.make(attachments=[('附件/a.zip',b'a'),('附件/z.zip',b'z')])
        b=receive_handoff(h,self.store)
        z=next(x for x in b['items'] if x['source']['file_id']==h['attachments'][1]['file_id'])
        self.assertEqual(a['items'][1]['attachment_id'],z['attachment_id'])
        self.assertEqual(z['sequence'],1)
        self.assertEqual(next(x for x in b['items'] if x.get('sequence')==2)['display_name'],'示例论文_附件02.zip')
        self.assertEqual(len(b['new_items']),1)

    def test_explicit_group_alias_preserves_ids_after_folder_migration(self):
        old=self.make(attachments=[('附件/a.zip',b'a')])
        a=receive_handoff(old,self.store)
        new=self.make('新学校/新目录',attachments=[('附件/a.zip',b'a')])
        b=receive_handoff(new,self.store,origin_group_id=old['group_id'])
        self.assertEqual(a['paper_id'],b['paper_id'])
        self.assertEqual(a['items'][1]['attachment_id'],b['items'][1]['attachment_id'])
        self.assertEqual(batch_summary([a,b]),dict(total=1,received=1,remaining=0))

    def test_source_hash_mismatch_stops_only_that_item(self):
        h=self.make(attachments=[('附件/a.zip',b'good'),('附件/b.zip',b'other')])
        Path(h['attachments'][0]['path']).write_bytes(b'bad!')
        r=receive_handoff(h,self.store)
        self.assertEqual(r['state'],'未接收')
        self.assertEqual(len(received_items(r)),2)
        self.assertEqual(sum(x['state']=='未接收' for x in r['items']),1)

    def test_downstream_recheck_detects_mutation_after_receipt(self):
        h=self.make()
        r=receive_handoff(h,self.store)
        item=received_items(r)[0]
        self.assertTrue(verify_received_file(item))
        Path(h['body']['path']).write_bytes(b'mutated')
        with self.assertRaises(ReceiptError):verify_received_file(item)

    def test_foreign_group_path_is_never_received(self):
        h=self.make(attachments=[('附件/a.zip',b'a')])
        outside=self.base/'foreign.zip'; outside.write_bytes(b'a')
        h['attachments'][0]['path']=str(outside)
        version(h)
        r=receive_handoff(h,self.store)
        self.assertEqual(r['state'],'未接收')
        self.assertEqual([x['kind'] for x in received_items(r)],['body'])

    def test_ledger_cannot_be_created_inside_original_library(self):
        h=self.make()
        with self.assertRaises(ReceiptError):
            receive_handoff(h,ReceiptStore(self.library/'wrong-output'))


class LegacyHistoryTests(ReceiptFixture):
    def legacy(self,h,pid='PF_HISTORIC',attid='historic-att-01',unknown=False):
        root=self.base/'old-data'
        inventory=copy.deepcopy(dict(schema='prefect-paper/1',paper_id=pid,revision='old-revision',
            person_dir=h['group_root'],library_root=h['library_root'],body=h['body'],
            body_copies=[h['body']],attachments=h['attachments'],archive_files=[h['body']]+h['attachments'],excluded=[]))
        for row in inventory['attachments']:row['file_id']=attid
        save(root/'papers'/pid/'revisions'/'old-revision'/'signature'/'inventory.json',inventory)
        body_key=identity([h['body']['sha256'],h['body']['relative'],'signature'])[:24]
        body_work=root/'papers'/pid/'body'/body_key
        save(body_work/'model'/'request.json',dict(model='retained',messages=[]))
        request_sha=hashlib.sha256(json.dumps(dict(model='retained',messages=[]),ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
        response=body_work/'model'/'response.txt';response.write_text('saved body',encoding='utf8')
        save(body_work/'model'/'request-state.json',dict(status='response_complete',request_sha256=request_sha,response_sha256=sha_file(response)))
        att_work=None
        if h['attachments']:
            att_work=root/'papers'/pid/'attachments'/attid/'adapter-key'
            req=dict(model='retained',messages=[])
            save(att_work/'model'/'request.json',req)
            state=dict(status='request_in_flight' if unknown else 'response_complete',request_sha256=request_sha)
            if not unknown:
                response=att_work/'model'/'response.txt';response.write_text('saved attachment',encoding='utf8')
                state['response_sha256']=sha_file(response)
                save(att_work/'description.json',dict(schema='attachment-description/1',paper_id=pid,
                    attachment_id=attid,source_sha256=h['attachments'][0]['sha256'],format='.zip',
                    kind='already known',summary='retained summary',evidence_refs=[],limitations=[]))
            save(att_work/'model'/'request-state.json',state)
        return root,body_work,att_work

    def test_existing_ids_and_original_work_directories_are_reused(self):
        h=self.make(attachments=[('附件/a.zip',b'a')])
        root,body_work,att_work=self.legacy(h)
        before={p:p.read_bytes() for p in root.rglob('*') if p.is_file()}
        r=receive_handoff(h,ReceiptStore(self.base/'legacy-ledger',history_roots=[root]))
        self.assertEqual(r['paper_id'],'PF_HISTORIC')
        body,att=r['items']
        self.assertEqual(att['attachment_id'],'historic-att-01')
        self.assertEqual(body['history']['body_work'],str(body_work))
        self.assertEqual(att['history']['attachment_work'],str(att_work))
        self.assertEqual(att['history']['policy'],'reuse_complete')
        self.assertEqual(att['history']['existing_identification']['path'],str(att_work/'description.json'))
        self.assertEqual({p:p.read_bytes() for p in before},before)

    def test_unknown_historical_request_is_retained_and_never_authorizes_resend(self):
        h=self.make(attachments=[('附件/a.zip',b'a')])
        root,_,att_work=self.legacy(h,unknown=True)
        r=receive_handoff(h,ReceiptStore(self.base/'unknown-ledger',history_roots=[root]))
        att=r['items'][1]
        self.assertEqual(att['state'],'已接收')
        self.assertEqual(att['history']['attachment_work'],str(att_work))
        self.assertEqual(att['history']['policy'],'inspect_saved_request')
        self.assertFalse(att['history']['allow_new_request'])

    def test_upstream_existing_id_cannot_claim_other_persons_history(self):
        h=self.make('学校/甲')
        root,_,_=self.legacy(h)
        other=self.make('学校/乙')
        other['existing_paper_id']='PF_HISTORIC';version(other)
        r=receive_handoff(other,ReceiptStore(self.base/'collision-ledger',history_roots=[root]))
        self.assertEqual(r['state'],'未接收')
        self.assertEqual(received_items(r),[])

    def test_explicit_previous_group_root_reuses_history_after_move(self):
        h=self.make(attachments=[('附件/a.zip',b'a')])
        root,body_work,att_work=self.legacy(h)
        new=self.make('搬家后/甲',attachments=[('附件/a.zip',b'a')])
        r=receive_handoff(new,ReceiptStore(self.base/'move-ledger',history_roots=[root]),previous_group_root=h['group_root'])
        self.assertEqual(r['paper_id'],'PF_HISTORIC')
        self.assertEqual(r['items'][1]['attachment_id'],'historic-att-01')
        self.assertEqual(r['items'][0]['history']['body_work'],str(body_work))
        self.assertEqual(r['items'][1]['history']['attachment_work'],str(att_work))


if __name__=='__main__':unittest.main()
