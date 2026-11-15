import json
import multiprocessing
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from paperflow_prefect.legacy import bind
from paperflow_prefect.settings import Settings
from paperflow_prefect.records import read_json, sha_file, write_json
from test_attachment_ingest import handoff
from test_attachment_publish import FakeStore


def hold_folder_lock(lock_path, ready, release):
    from filelock import FileLock
    with FileLock(lock_path, timeout=0):
        ready.set()
        release.wait(10)


def fixture(root):
    root=Path(root); delivery=handoff(root)
    def ref(name, data):
        p=root/(name+'.json'); write_json(p,data)
        return {'path':str(p),'sha256':sha_file(p)}
    routing=[{'code':'081301','name':'化学工程与工艺'}, {'code':'080206','name':'过程装备与控制工程'}]
    body=root/'body.pdf'; body.write_bytes(b'body'); body_sha=sha_file(body)
    body_ref={'path':str(body),'sha256':body_sha}
    original={'path':delivery['source_path'],'sha256':delivery['source_sha256']}
    candidate=ref('candidate',{'profile':{'routing_candidates':routing}})
    associated=ref('associated',{'sha256':delivery['source_sha256'],'remote_original_uploaded':False})
    history=ref('history',{'schema':'single-attachment-history-audit/1',
        'source_sha256':delivery['source_sha256'],'body_source_sha256':body_sha,
        'remote_original_uploaded':False,'body_candidate':candidate,'associated_record':associated,
        'routing_candidates':routing,'independent_associated_request_records':[]})
    receipt=ref('receipt',{'paper_id':delivery['paper_id'],'receipt_id':'receipt'})
    paper={'paper_id':delivery['paper_id'],'body':{'sha256':body_sha},
           'attachments':[{'file_id':delivery['attachment_id'],'path':delivery['source_path'],
               'sha256':delivery['source_sha256'],'bytes':8}]}
    run=ref('run',{'paper_id':delivery['paper_id'],'receipt_id':'receipt','deliveries':[delivery],
        'body':{'status':'not_selected'},'attachments':{delivery['attachment_id']:{'status':'complete'}}})
    manifest={'schema':'single-attachment-live-handoff/1','delivery':delivery,'intake_receipt':receipt,
        'stage3_run':run,'history_audit':history,
        'body_context':{'body_candidate':candidate,'body_source_sha256':body_sha,
            'routing_json_pointer':'/profile/routing_candidates','routing_candidates':routing,
            'original_body':body_ref,'copied_body':body_ref},
        'attachment_context':{'associated_record':associated,'remote_original_uploaded':False,
            'original_attachment':original,'copied_attachment':original},
        'constraints':{'original_items':1,'original_stored_once':True,'no_body_publication':True,
            'no_nas_operations':True,'parent_link_status':'paper_identity_only',
            'body_remote_document_id':None,'unknown_requests_resend':False}}
    p=root/'handoff.json'; write_json(p,manifest)
    return p,manifest,read_json(receipt['path']),paper


class FrozenHandoffTests(unittest.TestCase):
    def read(self,p,r,paper):
        from paperflow_prefect.attachment_ingest import read_frozen_handoff
        with patch('paperflow_prefect.stage3_handoff.read_receipt',return_value=(r,paper)):
            return read_frozen_handoff(p,sha_file(p))

    def test_frozen_profile_order_and_received_identity_are_reused(self):
        with tempfile.TemporaryDirectory() as root:
            p,m,r,paper=fixture(root); result=self.read(p,r,paper)
            self.assertEqual(result['routing_codes'],['081301','080206'])
            self.assertEqual(result['profile_sha256'],m['body_context']['body_candidate']['sha256'])
            for key,value in m['delivery'].items():
                self.assertEqual(result['card']['delivery'][key],str(Path(value).resolve()) if key.endswith('_path') else value)

    def test_changed_evidence_or_wrong_receipt_identity_is_rejected(self):
        for change in ['evidence','paper','body','attachment']:
            with self.subTest(change=change), tempfile.TemporaryDirectory() as root:
                p,m,r,paper=fixture(root)
                if change=='evidence': Path(m['body_context']['body_candidate']['path']).write_bytes(b'changed')
                elif change=='paper': paper['paper_id']='wrong'
                elif change=='body': paper['body']['sha256']='c'*64
                else: paper['attachments'][0]['file_id']='ATT_wrong'
                with self.assertRaises(ValueError): self.read(p,r,paper)

    def test_reordered_profile_and_unresolved_history_are_rejected(self):
        for change in ['order','history']:
            with self.subTest(change=change), tempfile.TemporaryDirectory() as root:
                p,m,r,paper=fixture(root)
                if change=='order': m['body_context']['routing_candidates'].reverse()
                else: m['attachment_context']['remote_original_uploaded']=None
                write_json(p,m)
                with self.assertRaises(ValueError): self.read(p,r,paper)


class FolderStore(FakeStore):
    def request(self,method,endpoint,body=None):
        if endpoint=='/files' and method=='POST':
            self.sent.append(('folder',body)); self._fault('folder_unknown')
            self.files['folder-id']=dict(body,id='folder-id')
            self._fault('folder_lost'); return self.files['folder-id']
        return super().request(method,endpoint,body)


class FolderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): bind(Settings.load())
    def folder(self,c,root,**kw):
        from paperflow_prefect.attachment_publish import ensure_original_folder
        return ensure_original_folder(c,'root','081301','【081301】附件原件',root,**kw)

    def test_verified_folder_can_be_read_while_another_owner_holds_creation_lock(self):
        from filelock import FileLock, Timeout
        with tempfile.TemporaryDirectory() as root:
            client = FolderStore()
            self.assertEqual(self.folder(client, root), 'folder-id')
            receipt = next((Path(root) / 'folders').glob('*.json'))
            before, sent = receipt.read_bytes(), list(client.sent)
            with FileLock(str(receipt) + '.lock', timeout=0):
                try:
                    result = self.folder(client, root)
                except Timeout:
                    self.fail('An already verified folder was blocked by its creation lock')
                self.assertEqual(result, 'folder-id')
            self.assertEqual(receipt.read_bytes(), before)
            self.assertEqual(client.sent, sent)

    def test_verified_folder_read_succeeds_while_another_process_holds_creation_lock(self):
        with tempfile.TemporaryDirectory() as root:
            client = FolderStore()
            self.assertEqual(self.folder(client, root), 'folder-id')
            receipt = next((Path(root) / 'folders').glob('*.json'))
            context = multiprocessing.get_context('spawn')
            ready, release = context.Event(), context.Event()
            owner = context.Process(target=hold_folder_lock, args=(str(receipt) + '.lock', ready, release))
            owner.start()
            try:
                self.assertTrue(ready.wait(8), 'test process did not acquire the creation lock')
                self.assertEqual(self.folder(client, root), 'folder-id')
                self.assertEqual(len(client.sent), 1)
            finally:
                release.set()
                owner.join(8)
                if owner.is_alive():
                    owner.terminate()
                    owner.join(5)
            self.assertEqual(owner.exitcode, 0)

    def test_verified_folder_never_reuses_replaced_remote_id(self):
        with tempfile.TemporaryDirectory() as root:
            client = FolderStore()
            self.assertEqual(self.folder(client, root), 'folder-id')
            sent = list(client.sent)
            client.files['folder-id']['id'] = 'replacement-id'
            with self.assertRaisesRegex(ValueError, 'ID'):
                self.folder(client, root)
            self.assertEqual(client.sent, sent)

    def test_verified_folder_missing_or_duplicated_is_never_recreated(self):
        for change in ('missing', 'duplicate'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as root:
                client = FolderStore()
                self.assertEqual(self.folder(client, root), 'folder-id')
                sent = list(client.sent)
                if change == 'missing':
                    client.files.clear()
                else:
                    client.files['other'] = dict(client.files['folder-id'], id='other')
                with self.assertRaises((RuntimeError, ValueError)):
                    self.folder(client, root)
                self.assertEqual(client.sent, sent)

    def test_verified_folder_binding_change_is_rejected_without_writes(self):
        from paperflow_prefect.attachment_publish import ensure_original_folder
        with tempfile.TemporaryDirectory() as root:
            client = FolderStore()
            self.assertEqual(self.folder(client, root), 'folder-id')
            sent = list(client.sent)
            with self.assertRaisesRegex(ValueError, '归属'):
                ensure_original_folder(client, 'other-root', '081301', '【081301】附件原件', root)
            self.assertEqual(client.sent, sent)

    def test_unverified_or_unknown_creation_never_bypasses_creation_lock(self):
        from filelock import FileLock, Timeout
        from paperflow_prefect.records import identity
        for prior_unknown in (False, True):
            with self.subTest(prior_unknown=prior_unknown), tempfile.TemporaryDirectory() as root:
                client = FolderStore()
                if prior_unknown:
                    client.fault = 'folder_unknown'
                    with self.assertRaises(TimeoutError):
                        self.folder(client, root)
                folder_dir = Path(root) / 'folders'
                folder_dir.mkdir(exist_ok=True)
                receipt = folder_dir / (identity([client.base_url, '081301']) + '.json')
                sent = list(client.sent)
                with FileLock(str(receipt) + '.lock', timeout=0):
                    with self.assertRaises(Timeout):
                        self.folder(client, root)
                self.assertEqual(client.sent, sent)
                if prior_unknown:
                    with self.assertRaises(RuntimeError):
                        self.folder(client, root)
                    self.assertEqual(client.sent, sent)

    def test_known_folder_reused_after_lost_response(self):
        with tempfile.TemporaryDirectory() as root:
            c=FolderStore(); c.fault='folder_lost'
            with self.assertRaises(TimeoutError): self.folder(c,root)
            self.assertEqual(self.folder(c,root),'folder-id'); self.folder(c,root)
            self.assertEqual(len(c.sent),1)

    def test_unknown_missing_folder_never_recreated(self):
        with tempfile.TemporaryDirectory() as root:
            c=FolderStore(); c.fault='folder_unknown'
            with self.assertRaises(TimeoutError): self.folder(c,root)
            with self.assertRaises(RuntimeError): self.folder(c,root)
            self.assertEqual(len(c.sent),1)

    def test_readonly_and_wrong_parent_do_not_create(self):
        with tempfile.TemporaryDirectory() as root:
            c=FolderStore(); c.files['elsewhere']={'id':'elsewhere','name':'【081301】附件原件',
                                                  'type':'folder','parent_id':'other'}
            with self.assertRaises(RuntimeError): self.folder(c,root,read_only=True)
            self.assertEqual(c.sent,[])


class EntryTests(unittest.TestCase):
    def test_existing_shared_limit_is_checked_and_acquired_without_update(self):
        from attachment_ingest_cli import rag_slot
        with tempfile.TemporaryDirectory() as root, patch.dict('os.environ'), \
                patch('prefect.client.orchestration.get_client') as get_client, \
                patch('prefect.concurrency.sync.concurrency') as concurrency:
            api=get_client.return_value.__enter__.return_value
            api.read_global_concurrency_limit_by_name.return_value=SimpleNamespace(limit=4,active=True,slot_decay_per_second=0)
            report={}
            with rag_slot({'prefect_api_url':'http://127.0.0.1:9/api','limits':{'rag':4}},root,report): pass
            self.assertEqual(concurrency.call_args.args,('paperflow-connected-rag',))
            self.assertEqual(concurrency.call_args.kwargs['occupy'],1)
            self.assertTrue(concurrency.call_args.kwargs['strict'])
            api.update_global_concurrency_limit.assert_not_called()
            self.assertFalse(report['rag_limit']['changed'])
            concurrency.reset_mock()
            with self.assertRaises(ValueError):
                with rag_slot({'prefect_api_url':'http://127.0.0.1:9/api','limits':{'rag':20}},root,{}): pass
            concurrency.assert_not_called()

    def test_target_mapping_must_match_frozen_order_before_network(self):
        from attachment_ingest_cli import validate_targets
        with tempfile.TemporaryDirectory() as root:
            p,m,r,paper=fixture(root)
            with patch('paperflow_prefect.stage3_handoff.read_receipt',return_value=(r,paper)):
                from paperflow_prefect.attachment_ingest import read_frozen_handoff
                frozen=read_frozen_handoff(p,sha_file(p))
            targets={'service':'http://127.0.0.1:9','root_id':'root',
                     'folder_name':'【081301】附件原件','libraries':[
                         {'code':'081301','name':'化学工程与工艺','dataset_id':'ds-main','dataset_name':'【081301】化学工程与工艺｜资料库','embedding_model':'model'},
                         {'code':'080206','name':'过程装备与控制工程','dataset_id':'ds-ref','dataset_name':'【080206】过程装备与控制工程｜资料库','embedding_model':'model'}]}
            validate_targets(frozen,targets,'http://127.0.0.1:9')
            targets['libraries'].reverse()
            with self.assertRaises(ValueError): validate_targets(frozen,targets,'http://127.0.0.1:9')

    def test_audit_counts_reads_and_mutations_without_credentials(self):
        from attachment_ingest_cli import AuditedClient
        with tempfile.TemporaryDirectory() as root:
            c=AuditedClient(FakeStore(),Path(root)/'audit.json')
            fid=c.upload_file('folder','file.zip',b'123'); c.download_file(fid)
            c.request('GET','/files'); c.request('POST','/retrieval',{'document_ids':[]})
            summary=c.summary()
            self.assertEqual(summary['mutations'],1); self.assertEqual(summary['queries'],3)
            self.assertNotIn('api_key',Path(root,'audit.json').read_text())


if __name__=='__main__': unittest.main()
