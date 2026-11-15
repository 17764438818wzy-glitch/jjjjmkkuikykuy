"""Upload identity survives reprocessing; upload acknowledgements stay distinct from parsing."""
import json
from pathlib import Path

import pytest

from engine import Store,digest
from evidence import build_package,export_package,package_fingerprint
from ragflow_client import upload_items,RagflowHTTPError,upload_name

DATASET={'id':'accounting','name':'会计学'}


def processed(store,run_id,body='研究正文。企业2023年营业收入100万元。'):
    source=store.root/(run_id+'.txt');source.write_text(body,encoding='utf-8')
    package=store.runs/run_id/'package'
    meta=build_package(source,package,{'id':run_id,'title':'企业财务研究'},redact_content=False)
    meta['preprocessing_complete']=True
    meta['text_privacy_complete']=True
    if run_id.startswith('asset'):
        from PIL import Image
        image_path=package/'assets'/'figure.png'
        Image.new('RGB',(32,24),'navy').save(image_path)
        import hashlib
        meta['resources']=[{'id':'fixture-figure','local_path':'assets/figure.png','kind':'image',
            'privacy_status':'approved','sha256':hashlib.sha256(image_path.read_bytes()).hexdigest(),
            'position':{'line':1}}]
        meta['blocks'].append({'id':'fixture-figure-block','type':'image','resource_id':'fixture-figure',
            'position':{'line':1},'caption':'图1 测试趋势','ocr':{'verified':True,'text':'2023 100','source':'fixture'}})
    meta['automatic_approval']=True;export_package(package,meta)
    path=package/'ingest.md'
    item={'id':run_id,'title':'企业财务研究','major':'会计学','status':'pass','reason':'自动处理通过',
        'automatic_approval':True,'source_path':str(source),'source_hash':digest(source),
        'path':str(path),'package_path':str(package),'reviewed_hash':digest(path),
        'reviewed_package_hash':package_fingerprint(package),'uploads':{},
        'paper_id':json.loads((package/'manifest.json').read_text(encoding='utf-8'))['paper_id']}
    store.save({'run_id':run_id,'status':'completed','pipeline':'automatic','started_at':'2026-09-15',
        'message':'通过 1','source_count':1,'items':[item],'events':[]})
    return item


class Remote:
    base_url='http://localhost';api_key='fixture-key'
    def __init__(self):self.docs=[];self.posts=0;self.parse_calls=[]
    def datasets(self):return [DATASET]
    def upload(self,dataset_id,path):
        self.posts+=1;doc={'id':f'doc-{self.posts}','name':upload_name(path),'run':'UNSTART','progress':0}
        doc['dataset_id']=dataset_id
        self.docs.append(doc);return doc['id']
    def documents(self,dataset_id,name=None,document_id=None):
        return [d for d in self.docs if (not name or d['name']==name) and (not document_id or d['id']==document_id)]
    def parse(self,dataset_id,document_id):self.parse_calls.append(document_id)


class AssetRemote(Remote):
    def upload(self, dataset_id, path, remote_name=None):
        self.posts += 1
        doc={'id':f'doc-{self.posts}','name':remote_name or upload_name(path),'dataset_id':dataset_id,
             'run':'UNSTART','progress':0}
        self.docs.append(doc)
        return doc['id']


def test_reprocessing_same_source_adopts_confirmed_record_after_restart(tmp_path):
    store=Store(tmp_path/'data');first=processed(store,'first');client=Remote()
    assert upload_items(store,'first',['first'],client,DATASET)==1
    store=Store(store.root);second=processed(store,'renamed')
    assert second['reviewed_hash']!=first['reviewed_hash']
    assert upload_items(store,'renamed',['renamed'],client,DATASET)==0
    record=store.load('renamed')['items'][0]['uploads']['http://localhost|accounting']
    assert record['status']=='uploaded' and record['document_id']=='doc-1'
    assert record['uploaded_at'] and record['reused_from']['run_id']=='first'
    assert record['content_hash']==first['reviewed_hash'], 'Retain the hash of the actual remote body'
    assert client.posts==1


@pytest.mark.parametrize('skip_derived', [True, False])
def test_asset_handoff_applies_derived_policy_and_uploads_approved_images(tmp_path, skip_derived):
    store=Store(tmp_path/'data');item=processed(store,'assets');client=AssetRemote()
    messages=[]
    assert upload_items(store,'assets',['assets'],client,DATASET,include_assets=True,
                        callback=messages.append,
                        upload_config={'skip_derived_upload':skip_derived})==1
    record=store.load('assets')['items'][0]['uploads']['http://localhost|accounting']
    assert record['asset_status']=='uploaded'
    assert len(record['asset_uploads']) >= 1
    names=[d['name'] for d in client.docs]
    assert any(name.endswith('ragflow_assets.md') for name in names) is (not skip_derived)
    assert any(name.endswith('.png') for name in names)
    assert any('子集率' in message and '阈值' in message for message in messages) is skip_derived


def test_routed_upload_separates_paper_and_image_documents(tmp_path):
    from ragflow_client import upload_routed_items

    store=Store(tmp_path/'data');processed(store,'assets-routed');client=AssetRemote()
    routes={'paper':{'id':'paper-kb','name':'论文库'},'image':{'id':'image-kb','name':'图片库'}}
    assert upload_routed_items(store,'assets-routed',['assets-routed'],client,routes)==1
    paper_names=[d['name'] for d in client.docs if d['dataset_id']=='paper-kb']
    image_names=[d['name'] for d in client.docs if d['dataset_id']=='image-kb']
    assert len(paper_names)==1
    assert not any(name.endswith('ragflow_assets.md') for name in paper_names)
    assert all(not name.endswith('.png') for name in paper_names)
    assert any(name.endswith('ragflow_images.md') for name in image_names)
    assert any(name.endswith('.png') for name in image_names)
    uploads=store.load('assets-routed')['items'][0]['uploads']
    assert 'http://localhost|paper-kb' in uploads
    assert 'http://localhost|image-kb' in uploads
    assert uploads['http://localhost|paper-kb']['dataset_id']=='paper-kb'
    assert uploads['http://localhost|image-kb']['dataset_id']=='image-kb'
    image_record=uploads['http://localhost|image-kb']
    assert image_record['status']=='uploaded'
    assert image_record['document_id']
    assert image_record['asset_status']=='uploaded'


def test_education_upload_rejects_legacy_paper_id_before_network(tmp_path):
    store=Store(tmp_path/'data');item=processed(store,'education-old-id');client=Remote()
    package=Path(item['package_path'])
    meta=json.loads((package/'metadata.json').read_text(encoding='utf-8'))
    meta['category']='教育教学类';meta['major']='小学教育'
    meta['classification']={'major':'小学教育','method':'original_rules','source':'front_matter','confidence':'high'}
    export_package(package,meta,canonical_paper_id=False)
    manifest=store.load('education-old-id')
    row=manifest['items'][0]
    row['reviewed_hash']=digest(package/'ingest.md')
    row['reviewed_package_hash']=package_fingerprint(package)
    store.save(manifest)
    with pytest.raises(ValueError,match='稳定论文编号'):
        upload_items(store,'education-old-id',['education-old-id'],client,DATASET)
    assert client.posts==0


def test_routed_upload_skips_images_when_image_dataset_is_missing(tmp_path):
    from ragflow_client import upload_routed_items

    store=Store(tmp_path/'data');processed(store,'assets-paper-only');client=AssetRemote();messages=[]
    routes={'paper':{'id':'paper-kb','name':'论文库'},'image':None}
    assert upload_routed_items(store,'assets-paper-only',['assets-paper-only'],client,routes,messages.append)==1
    assert any('图片知识库' in message for message in messages)
    assert all(d['dataset_id']=='paper-kb' for d in client.docs)


def test_routed_upload_deduplicates_same_paper_across_new_auto_ids(tmp_path):
    from ragflow_client import upload_routed_items

    store=Store(tmp_path/'data');processed(store,'asset-first');client=AssetRemote()
    routes={'paper':{'id':'paper-kb','name':'论文库'},'image':{'id':'image-kb','name':'图片库'}}
    assert upload_routed_items(store,'asset-first',['asset-first'],client,routes)==1
    processed(store,'asset-second')
    assert upload_routed_items(store,'asset-second',['asset-second'],client,routes)==0
    assert len([d for d in client.docs if d['dataset_id']=='paper-kb' and not d['name'].endswith('ragflow_assets.md')]) == 1
    assert len([d for d in client.docs if d['dataset_id']=='image-kb' and d['name'].endswith('ragflow_images.md')]) == 1


def test_identical_title_different_original_content_remains_a_new_paper(tmp_path):
    store=Store(tmp_path);processed(store,'first');processed(store,'revised','研究正文。营业收入已更新为200万元。')
    client=Remote()
    assert upload_items(store,'first',['first'],client,DATASET)==1
    assert upload_items(store,'revised',['revised'],client,DATASET)==1
    assert client.posts==2


def test_uploaded_record_is_scoped_to_destination(tmp_path):
    store=Store(tmp_path);processed(store,'first');client=Remote()
    upload_items(store,'first',['first'],client,DATASET)
    assert upload_items(store,'first',['first'],client,{'id':'another-kb','name':'测试库'})==1
    client.base_url='https://different.example'
    assert upload_items(store,'first',['first'],client,DATASET)==1


def test_local_alias_and_default_port_do_not_create_duplicate_upload(tmp_path):
    store=Store(tmp_path);processed(store,'first');client=Remote()
    upload_items(store,'first',['first'],client,DATASET)
    client.base_url='http://127.0.0.1:80/';processed(store,'second')
    assert upload_items(store,'second',['second'],client,DATASET)==0
    assert client.posts==1


def test_old_success_record_is_reused_without_timestamp_fabrication(tmp_path):
    store=Store(tmp_path);old=processed(store,'old');manifest=store.load('old')
    manifest['items'][0]['uploads']['http://localhost|accounting']={
        'status':'uploaded','document_id':'historical-doc','dataset_name':'会计学','content_hash':old['reviewed_hash']}
    store.save(manifest);processed(store,'new');client=Remote()
    assert upload_items(store,'new',['new'],client,DATASET)==0
    record=store.load('new')['items'][0]['uploads']['http://localhost|accounting']
    assert record['document_id']=='historical-doc' and not record.get('uploaded_at')


def test_timeout_reconciles_original_name_from_another_batch_without_second_post(tmp_path):
    store=Store(tmp_path);processed(store,'first')
    class LostReply(Remote):
        def upload(self,dataset_id,path):
            super().upload(dataset_id,path);raise TimeoutError('lost acknowledgement')
    client=LostReply()
    with pytest.raises(TimeoutError):upload_items(store,'first',['first'],client,DATASET)
    processed(store,'second')
    assert upload_items(store,'second',['second'],client,DATASET)==0
    assert client.posts==1
    assert store.load('first')['items'][0]['uploads']['http://localhost|accounting']['status']=='uploaded'
    assert store.load('second')['items'][0]['uploads']['http://localhost|accounting']['document_id']=='doc-1'


def test_timeout_with_no_remote_match_stays_unconfirmed_and_never_reposts(tmp_path):
    store=Store(tmp_path);processed(store,'first')
    class Lost(Remote):
        def upload(self,*a):self.posts+=1;raise TimeoutError('connection lost')
    client=Lost()
    with pytest.raises(TimeoutError):upload_items(store,'first',['first'],client,DATASET)
    processed(store,'second')
    with pytest.raises(RuntimeError,match='确认|不确定'):upload_items(store,'second',['second'],client,DATASET)
    assert client.posts==1
    assert store.load('second')['items'][0]['uploads']['http://localhost|accounting']['status']=='unknown'


def test_explicit_http_rejection_can_be_retried(tmp_path):
    store=Store(tmp_path);processed(store,'first')
    class RejectedOnce(Remote):
        rejected=False
        def upload(self,*a):
            if not self.rejected:self.rejected=True;raise RagflowHTTPError(403)
            return super().upload(*a)
    client=RejectedOnce()
    with pytest.raises(RagflowHTTPError):upload_items(store,'first',['first'],client,DATASET)
    assert store.load('first')['items'][0]['uploads']['http://localhost|accounting']['status']=='failed'
    assert upload_items(store,'first',['first'],client,DATASET)==1


def test_automatic_handoff_reuses_doc_id_without_pending_key_error(tmp_path):
    from auto_ragflow import handoff
    store=Store(tmp_path);processed(store,'first');client=Remote()
    upload_items(store,'first',['first'],client,DATASET)
    processed(store,'second');handoff(store,'second',client,{},auto_parse=True)
    item=store.load('second')['items'][0]
    assert item['handoff_status']=='uploaded' and not item.get('handoff_reason')
    assert item['uploads']['http://localhost|accounting']['document_id']=='doc-1'
    # Automatic handoff reuses the primary and skips the redundant sidecar.
    assert client.posts==1 and client.parse_calls==['doc-1']


def test_visible_result_status_reads_historical_upload_before_another_handoff(tmp_path,monkeypatch):
    import tkinter as tk
    from app import Workbench
    monkeypatch.delenv('RAGFLOW_API_KEY',raising=False)
    store=Store(tmp_path);processed(store,'first');client=Remote();upload_items(store,'first',['first'],client,DATASET)
    processed(store,'second');root=tk.Tk();root.withdraw()
    try:
        app=Workbench(root,tmp_path);app.url.set(Remote.base_url)
        app.connected([DATASET,{'id':'another-kb','name':'测试库'}])
        app.current=app.store.load('second');app.render_results()
        assert '已上传' in app.rag_result_tree.item('second','values')[5]
        assert '交接提示' not in app.result_tree['columns']
        source=Path(app.current['items'][0]['source_path'])
        app.scanned([{'path':str(source),'name':source.name,'format':'.txt','size':source.stat().st_size}])
        assert app.sources[0]['upload_mark']=='已上传'
        assert 'RAGFlow' not in app.source_tree['columns']
        app.connected([DATASET,{'id':'another-kb','name':'测试库'}])
        app.task_dataset_combo.current(1)
        root.update()
        assert app.rag_result_tree.item('second','values')[5]=='已上传', 'Historical receipt stays tied to its actual destination'
    finally:
        for callback in root.tk.call('after','info'):root.after_cancel(callback)
        root.destroy()


def test_concurrent_handoffs_share_one_reservation_and_one_remote_document(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    store=Store(tmp_path);processed(store,'first');processed(store,'second')
    entered=Event();release=Event()
    class Slow(Remote):
        def upload(self,*a):
            entered.set();assert release.wait(3)
            return super().upload(*a)
    client=Slow()
    with ThreadPoolExecutor(max_workers=2) as pool:
        first=pool.submit(upload_items,store,'first',['first'],client,DATASET)
        assert entered.wait(3)
        second=pool.submit(upload_items,store,'second',['second'],client,DATASET)
        release.set()
        assert sorted([first.result(5),second.result(5)])==[0,1]
    assert client.posts==1
    assert all(store.load(r)['items'][0]['uploads']['http://localhost|accounting']['document_id']=='doc-1' for r in ['first','second'])


def test_upload_mark_distinguishes_unattempted_from_failed_record(tmp_path):
    from upload_tracking import UploadIndex, upload_mark
    store=Store(tmp_path/'data'); item=processed(store,'mark-state')
    assert upload_mark(item,UploadIndex(store.history()),'http://localhost','accounting') == '未尝试'
    manifest=store.load('mark-state')
    manifest['items'][0]['uploads']['http://localhost|accounting']={'status':'failed','error':'403'}
    store.save(manifest)
    assert upload_mark(store.load('mark-state')['items'][0],UploadIndex(store.history()),'http://localhost','accounting') == '上传失败，可重试'


def test_adopted_receipt_does_not_claim_new_local_export_exists_remotely(tmp_path):
    store=Store(tmp_path)
    for run,source,body in [('a','source-A','export-original'),('b','source-A','export-new'),('c','source-C','export-new')]:
        path=tmp_path/(run+'.md');path.write_text(body,encoding='utf-8')
        store.save({'run_id':run,'status':'completed','items':[{'id':run,'major':'会计学','status':'pass',
            'path':str(path),'source_hash':source,'reviewed_hash':digest(path),'uploads':{}}]})
    class Plain(Remote):
        def upload(self,ds,path):self.posts+=1;return f'doc-{self.posts}'
    client=Plain()
    assert upload_items(store,'a',['a'],client,DATASET)==1
    assert upload_items(store,'b',['b'],client,DATASET)==0
    assert upload_items(store,'c',['c'],client,DATASET)==1
    assert client.posts==2


def test_same_batch_identical_exports_share_newly_written_receipt(tmp_path):
    store=Store(tmp_path);items=[]
    for name in ['a','b']:
        path=tmp_path/(name+'.md');path.write_text('同一份已检查的入库正文',encoding='utf-8')
        items.append({'id':name,'major':'会计学','status':'pass','path':str(path),
            'source_hash':'different-source-'+name,'reviewed_hash':digest(path),'uploads':{}})
    store.save({'run_id':'batch','status':'completed','items':items})
    class Plain(Remote):
        def upload(self,ds,path):self.posts+=1;return f'doc-{self.posts}'
    client=Plain()
    assert upload_items(store,'batch',['a','b'],client,DATASET)==1
    records=[i['uploads']['http://localhost|accounting'] for i in store.load('batch')['items']]
    assert all(r['document_id']=='doc-1' for r in records) and client.posts==1


@pytest.mark.parametrize('completion',['manual','automatic'])
def test_successful_upload_refreshes_the_existing_input_list(tmp_path,monkeypatch,completion):
    import tkinter as tk
    from app import Workbench
    monkeypatch.delenv('RAGFLOW_API_KEY',raising=False)
    store=Store(tmp_path);item=processed(store,'first');source=Path(item['source_path'])
    root=tk.Tk();root.withdraw()
    try:
        app=Workbench(root,tmp_path);app.url.set(Remote.base_url);app.current=app.store.load('first')
        app.scanned([{'path':str(source),'name':source.name,'format':'.txt','size':source.stat().st_size}])
        assert '已上传' not in app.source_tree.item('0','values')
        upload_items(app.store,'first',['first'],Remote(),DATASET)
        if completion=='manual':app.uploaded('first',1)
        else:app.finished(app.store.load('first'))
        root.update()
        assert app.sources[0]['upload_mark']=='已上传'
        assert 'RAGFlow' not in app.source_tree['columns']
        app.source_tree.selection_remove('0')
        app.finished(app.store.load('first'))
        assert not app.source_tree.selection(), 'Refreshing markers must retain the user selection'
        app.url.set('https://different.example')
        assert '已上传' not in app.source_tree.item('0','values')
    finally:
        for callback in root.tk.call('after','info'):root.after_cancel(callback)
        root.destroy()
