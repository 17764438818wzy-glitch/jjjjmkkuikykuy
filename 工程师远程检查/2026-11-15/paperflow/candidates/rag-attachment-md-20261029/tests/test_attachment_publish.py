import importlib.util
from pathlib import Path
import tempfile
import unittest
from urllib.parse import parse_qs, unquote, urlsplit

from paperflow_prefect.legacy import bind
from paperflow_prefect.settings import Settings
from test_attachment_ingest import handoff, edit_card


class Counter:
    model_budget = 1500
    cl_limit = 1500
    def count_many(self, values):
        return [{'model_tokens': len(value), 'cl100k_tokens': len(value)} for value in values]


class FakeStore:
    """Stateful in-memory implementation of the native file/document API."""
    base_url = 'http://127.0.0.1:9'
    def __init__(self):
        self.files = {}; self.docs = {}; self.chunks = {}; self.sent = []
        self.fault = None; self.retrievals = 0
    def _fault(self, name):
        if self.fault == name:
            self.fault = None; raise TimeoutError(name)
    def upload_file(self, parent, name, data):
        self.sent.append(('file', parent, name)); self._fault('file_unknown')
        fid = 'file-' + str(len(self.files)+1)
        self.files[fid] = {'id':fid, 'name':name, 'parent_id':parent, 'size':len(data),
                           'type':'other', 'kbs_info':[], '_bytes':data}
        self._fault('file_lost'); return fid
    def download_file(self, fid): return self.files[fid]['_bytes']
    def upload_bytes(self, ds, name, data):
        self.sent.append(('md', ds, name)); did = 'doc-' + str(len(self.docs)+1)
        self.docs[did] = {'id':did, 'name':name, 'dataset_id':ds, 'meta_fields':{},
                          'status':'0', 'run':'UNSTART', 'chunk_count':0, '_bytes':data}
        self.chunks[did] = []; self._fault('md_lost'); return did
    def download_bytes(self, ds, did):
        if self.docs[did]['dataset_id'] != ds: raise ValueError('wrong dataset')
        return self.docs[did]['_bytes']
    def request(self, method, endpoint, body=None):
        url=urlsplit(endpoint); query=parse_qs(url.query); parts=[unquote(p) for p in url.path.strip('/').split('/')]
        if parts == ['files'] and method == 'GET':
            rows=list(self.files.values())
            if 'parent_id' in query: rows=[r for r in rows if r['parent_id']==query['parent_id'][0]]
            if 'keywords' in query: rows=[r for r in rows if query['keywords'][0] in r['name']]
            return {'files':rows, 'total':len(rows)}
        if parts == ['files','link-to-datasets'] and method == 'POST':
            self.sent.append(('link',body)); self._fault('link_unknown')
            for fid in body['file_ids']:
                for ds in body['kb_ids']:
                    did='raw-doc-'+str(len(self.docs)+1); original=self.files[fid]
                    self.docs[did]={'id':did, 'name':original['name'], 'dataset_id':ds,
                                    'meta_fields':{}, 'status':'1', 'run':'UNSTART',
                                    'chunk_count':0, '_bytes':original['_bytes']}
                    self.chunks[did]=[]; original['kbs_info'].append({'kb_id':ds,'document_id':did})
            self._fault('link_lost'); return True
        if parts == ['retrieval']:
            self.retrievals+=1
            return {'chunks':[c for did in body['document_ids'] for c in self.chunks[did] if self.docs[did]['status']=='1']}
        ds=parts[1]
        if len(parts)==3 and method=='GET':
            rows=[r for r in self.docs.values() if r['dataset_id']==ds]
            for key in ['id','name']:
                if key in query: rows=[r for r in rows if r[key]==query[key][0]]
            return {'docs':rows}
        did=parts[3]
        if len(parts)==4 and method=='PATCH':
            self.sent.append(('patch',did,body)); self._fault('patch_unknown')
            self.docs[did].update(body)
            if 'enabled' in body: self.docs[did]['status']=str(body['enabled'])
            self._fault('patch_lost'); return self.docs[did]
        if parts[-1]=='chunks' and method=='GET':
            return {'chunks':list(self.chunks[did]),'total':len(self.chunks[did])}
        if parts[-1]=='chunks' and method=='POST':
            self.sent.append(('chunk',did,body)); self._fault('chunk_unknown')
            row={'id':'chunk-'+str(len(self.sent)), 'document_id':did, 'content':body['content'], 'available':True}
            self.chunks[did].append(row); self.docs[did]['chunk_count']=len(self.chunks[did])
            self._fault('chunk_lost'); return {'chunk':row}
        raise AssertionError((method,endpoint))


def route():
    return {'paper_id':'PF_sample','body_source_sha256':'b'*64,'profile_sha256':'c'*64,
            'routing_codes':['081301','080206'],
            'primary':{'code':'081301','name':'化学工程与工艺','parent_id':'folder-main','dataset_id':'ds-main'},
            'references':[{'code':'080206','name':'过程装备与控制工程','dataset_id':'ds-ref'}],
            'history_reviewed':True, 'parent_targets':[]}


class AttachmentPublishTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): bind(Settings.load())
    def ingest(self, item, client, root, **kw):
        self.assertIsNotNone(importlib.util.find_spec('paperflow_prefect.attachment_publish'), 'attachment publisher is missing')
        from paperflow_prefect.attachment_publish import ingest_attachment
        from preextract_publish import _Budget
        return ingest_attachment(item, route(), client, Path(root)/'receipts', _Budget(Counter()), **kw)

    def test_one_original_linked_only_to_primary_and_references_share_id(self):
        with tempfile.TemporaryDirectory() as root:
            item=handoff(root); client=FakeStore(); result=self.ingest(item,client,root)
            self.assertEqual(result['status'],'verified')
            self.assertEqual(len(client.files),1)
            original=next(iter(client.files.values()))
            self.assertEqual([r['kb_id'] for r in original['kbs_info']],['ds-main'])
            self.assertEqual(original['_bytes'],b'print(1)')
            notes=[r for r in client.docs.values() if r['meta_fields'].get('paperflow_role') in {'attachment-card','attachment-reference'}]
            self.assertEqual(len(notes),2)
            self.assertEqual({r['meta_fields']['original_file_id'] for r in notes},{original['id']})
            self.assertTrue(all(r['status']=='1' for r in notes))
            raw_doc=client.docs[original['kbs_info'][0]['document_id']]
            self.assertEqual(raw_doc['status'],'0'); self.assertEqual(raw_doc['chunk_count'],0)

    def test_verified_rerun_makes_no_additional_writes(self):
        with tempfile.TemporaryDirectory() as root:
            item=handoff(root); client=FakeStore(); first=self.ingest(item,client,root)
            sent=list(client.sent); second=self.ingest(item,client,root)
            self.assertEqual(client.sent,sent)
            self.assertEqual(first['raw']['file_id'],second['raw']['file_id'])

    def test_lost_file_link_md_or_chunk_reply_reconciles_original_objects(self):
        for fault in ['file_lost','link_lost','md_lost','chunk_lost','patch_lost']:
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as root:
                item=handoff(root); client=FakeStore(); client.fault=fault
                with self.assertRaises(TimeoutError): self.ingest(item,client,root)
                result=self.ingest(item,client,root)
                self.assertEqual(result['status'],'verified')
                self.assertEqual(sum(r[0]=='file' for r in client.sent),1)
                self.assertEqual(sum(r[0]=='link' for r in client.sent),1)
                self.assertEqual(sum(r[0]=='md' for r in client.sent),2)

    def test_unknown_missing_remote_write_never_replays(self):
        for fault in ['file_unknown','link_unknown','chunk_unknown','patch_unknown']:
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as root:
                item=handoff(root); client=FakeStore(); client.fault=fault
                with self.assertRaises(TimeoutError): self.ingest(item,client,root)
                sent=list(client.sent)
                with self.assertRaises(RuntimeError): self.ingest(item,client,root)
                self.assertEqual(client.sent,sent)

    def test_updated_md_reuses_raw_file_and_keeps_previous_document(self):
        with tempfile.TemporaryDirectory() as root:
            item=handoff(root); client=FakeStore(); self.ingest(item,client,root)
            old_ids=set(client.docs); edit_card(item,'包含一段程序。','仅查看文本，没有运行。')
            self.ingest(item,client,root)
            self.assertEqual(sum(row[0]=='file' for row in client.sent),1)
            self.assertTrue(old_ids.issubset(client.docs))

    def test_new_md_cannot_bypass_previous_unknown_publication(self):
        with tempfile.TemporaryDirectory() as root:
            item=handoff(root); client=FakeStore(); client.fault='md_lost'
            with self.assertRaises(TimeoutError): self.ingest(item,client,root)
            sent=list(client.sent); edit_card(item,'包含一段程序。','新版说明。')
            with self.assertRaises(RuntimeError): self.ingest(item,client,root)
            self.assertEqual(client.sent,sent)

    def test_disabled_chunk_is_not_currently_verified(self):
        with tempfile.TemporaryDirectory() as root:
            item=handoff(root); client=FakeStore(); result=self.ingest(item,client,root)
            did=result['publications'][item['md_sha256']]['081301']['document_id']
            for chunk in client.chunks[did]: chunk['available']=False
            sent=list(client.sent)
            with self.assertRaises(ValueError): self.ingest(item,client,root)
            self.assertEqual(client.sent,sent)

    def test_native_new_chunks_follow_document_enabled_state(self):
        class NativeStateStore(FakeStore):
            def request(self,method,endpoint,body=None):
                result=super().request(method,endpoint,body)
                parts=urlsplit(endpoint).path.strip('/').split('/')
                if method=='POST' and parts[-1]=='chunks':
                    result['chunk']['available']=self.docs[parts[3]]['status']=='1'
                if method=='PATCH' and 'enabled' in body:
                    for row in self.chunks[parts[3]]: row['available']=bool(body['enabled'])
                return result
        with tempfile.TemporaryDirectory() as root:
            item=handoff(root); client=NativeStateStore(); result=self.ingest(item,client,root)
            self.assertEqual(result['status'],'verified')
            self.assertTrue(all(row['available'] for rows in client.chunks.values() for row in rows))

    def test_successful_reuse_refreshes_retrieval_without_writes(self):
        with tempfile.TemporaryDirectory() as root:
            item=handoff(root); client=FakeStore(); self.ingest(item,client,root)
            queries=client.retrievals; sent=list(client.sent)
            self.ingest(item,client,root,read_only=True)
            self.assertEqual(client.retrievals,queries+2); self.assertEqual(client.sent,sent)

    def test_retrieval_respects_native_candidate_and_page_bounds(self):
        class NativeRetrievalStore(FakeStore):
            def request(self,method,endpoint,body=None):
                if endpoint=='/retrieval':
                    if body['rerank_candidates_count'] < body.get('page',1)*body['page_size']:
                        raise ValueError('rerank_candidates_count smaller than page boundary')
                return super().request(method,endpoint,body)
        with tempfile.TemporaryDirectory() as root:
            result=self.ingest(handoff(root),NativeRetrievalStore(),root)
            self.assertEqual(result['status'],'verified')

    def test_long_md_is_not_truncated_or_forced_into_one_chunk(self):
        with tempfile.TemporaryDirectory() as root:
            item=handoff(root,archive=True,total=80,rows=[('code/file'+str(i)+'.py','file','8') for i in range(80)])
            client=FakeStore(); self.ingest(item,client,root)
            doc=next(r for r in client.docs.values() if r['meta_fields'].get('paperflow_role')=='attachment-card')
            parts=client.chunks[doc['id']]
            self.assertGreater(len(parts),1)
            assembled=''.join(part['content'].split('\n',1)[1] for part in parts)
            self.assertTrue(assembled.startswith(Path(item['md_path']).read_bytes().decode('utf8')))
            self.assertEqual(client.download_bytes('ds-main',doc['id']),Path(item['md_path']).read_bytes())

    def test_wrong_routing_order_fails_before_any_write(self):
        with tempfile.TemporaryDirectory() as root:
            item=handoff(root); client=FakeStore(); changed=route(); changed['routing_codes'].reverse()
            self.assertIsNotNone(importlib.util.find_spec('paperflow_prefect.attachment_publish'))
            from paperflow_prefect.attachment_publish import ingest_attachment
            from preextract_publish import _Budget
            with self.assertRaises(ValueError): ingest_attachment(item,changed,client,Path(root)/'receipts',_Budget(Counter()))
            self.assertEqual(client.sent,[])

    def test_read_only_never_creates_missing_remote_objects(self):
        with tempfile.TemporaryDirectory() as root:
            client=FakeStore()
            with self.assertRaises(RuntimeError): self.ingest(handoff(root),client,root,read_only=True)
            self.assertEqual(client.sent,[])


if __name__=='__main__': unittest.main()
