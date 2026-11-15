"""Completed-page facts come from the real isolated ledger, never a RAG call."""
import json
import time
from pathlib import Path

import pytest

from paperflow.handoff import PackageHandoff
from paperflow.package import load, save, sha
from test_managed_entry import entry_case
from test_workbench import web_case, login


def test_indexed_attempts_keep_immutable_version_and_attachment_ownership():
    from paperflow.run_timing import job_belongs
    from paperflow.workbench_view import _jobs_by_task
    tasks={key:dict(task_id=key,source_id='shared-source') for key in ('old','new','attachment')}
    tasks['attachment']['attachment_id']='original-file'
    packages={'old-manifest':'old','new-manifest':'new'}
    rows=[
        dict(kind='graph',unit_key='old'),
        dict(kind='repair',unit_key='new'),
        dict(kind='delivery',unit_key='delivery-old',payload=json.dumps({'manifest_sha256':'old-manifest'})),
        dict(kind='delivery',unit_key='delivery-new',data=json.dumps({'manifest_sha256':'new-manifest'})),
        dict(kind='delivery',unit_key='attachment',data=json.dumps({'attachment_id':'original-file'})),
        dict(kind='delivery',unit_key='foreign',data=json.dumps({'attachment_id':'other-file'})),
        dict(kind='delivery',unit_key='foreign',data=json.dumps({'manifest_sha256':'other-manifest'})),
        dict(kind='cleanup',unit_key='new'),
    ]
    assert _jobs_by_task(rows,tasks,packages)=={
        key:[row for row in rows if job_belongs(row,task,packages)] for key,task in tasks.items()}


def completed_paper(w, package_factory, name='paper', *, two_targets=False, images=True):
    source=w.root/(name+'.docx');source.write_bytes(name.encode())
    task=w.store.register_source(source,sha(source),display_name=name+'.docx')
    material,_,catalog=package_factory(name,task_id=task['task_id'],source_sha=task['source_sha256'])
    if two_targets:
        catalog['majors'].append(dict(code='120202',name='市场营销',paper_dataset_id='second-paper',image_dataset_id='second-images'))
    codes=[r['code'] for r in catalog['majors']]
    for filename in ('profile.json','tree.json','assets.json','chunks.json','review.json','corrections.json'):
        value=load(material/filename);value['source_id']=task['source_id']
        if filename=='profile.json':value['routing_candidates']=[{'code':c} for c in codes]
        if filename=='corrections.json':value['final_routing_candidates']=[{'code':c} for c in codes]
        if filename=='assets.json':
            value['items']=[]
            if images:
                from PIL import Image
                image=material/'fig.png';Image.new('RGB',(2,2),'green').save(image)
                value['items']=[dict(id='fig-current',kind='image',storage='image',file='fig.png',sha256=sha(image),
                    caption='当前版本的布局图',description='工位与物流路径。',section_id='s1',source_location={'page':3}),
                    dict(id='table-text',kind='table',storage='description',description='指标对比表。',section_id='s1')]
        save(material/filename,value)
    (material/'manifest.json').unlink();(material/'READY.json').unlink()
    save(w.config.catalog_path,catalog)
    h=PackageHandoff(w.store,w.config.handoff_root)
    # Seed an already accepted historical package for read-only UI tests.
    # Current embedding admission is tested separately; this fixture never delivers.
    h.check_embedding=lambda task,packet:None
    intent=h.prepare(task['task_id'],task,material,catalog,mapping_version=sha(w.config.catalog_path))
    result=h.resume(task['task_id'],intent['input_fingerprint'])
    digest=result['manifest_sha256']
    w.store.update(task['task_id'],stage='handed_off',package_dir=result['package_dir'])
    with w.store.db() as db:
        for route in catalog['majors']:
            code=route['code'];identity=dict(task_id=task['task_id'],source_id=task['source_id'],source_sha256=task['source_sha256'],manifest_sha256=digest,specialty_code=code)
            receipts={code:dict(body=dict(dataset_id=route['paper_dataset_id'],document_id='body-'+code,chunks={}),
                images=dict(dataset_id=route['image_dataset_id'],document_id='image-'+code,
                    chunks={'asset_fig-current':dict(chunk_id='chunk-'+code,image_id='stored-'+code)}),verified=True)}
            active=dict(identity=identity,receipts=receipts,activated_at=1234.0)
            db.execute('INSERT INTO specialty_active VALUES(?,?,?,?)',(task['source_id'],code,digest,json.dumps(active)))
        db.execute('INSERT INTO web_batches VALUES(?,?,?,?,?)',(name,name,'[]',json.dumps([task['task_id']]),time.time()))
    return task,Path(result['package_dir']),digest


def test_completed_crosses_batch_limit_deduplicates_and_never_starts_work(web_case,package_factory):
    w,client,_=web_case
    assert client.get('/api/completed').status_code==401
    login(client)
    task,_,_=completed_paper(w,package_factory)
    with w.store.db() as db:
        for n in range(105):
            db.execute('INSERT INTO web_batches VALUES(?,?,?,?,?)',(str(n),str(n),'[]','[]',time.time()+n+1))
        db.execute('INSERT INTO web_batches VALUES(?,?,?,?,?)',('duplicate','duplicate','[]',json.dumps([task['task_id']]),time.time()+200))
        before={t:db.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in ('tasks','scheduler_attempts','control_actions')}
    for _ in range(2):
        response=client.get('/api/completed');assert response.status_code==200,response.text
        rows=response.json()['items']
        assert [x['task_id'] for x in rows]==[task['task_id']]
        assert rows[0]['image_summary']=={'state':'checked','retained':1,'described':1,'omitted':0}
        assert rows[0]['attachment_summary']=={'state':'not_checked','count':None}
        assert client.get('/api/completed/'+task['task_id']).status_code==200
    with w.store.db() as db:
        after={t:db.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in before}
    assert after==before
    assert w.manager.stopped.is_set()


def test_completed_reads_do_not_recompute_unrelated_dashboard_summaries(web_case,package_factory,monkeypatch):
    w,client,_=web_case;login(client)
    task,_,_=completed_paper(w,package_factory)
    def dashboard_only(*args,**kwargs):
        raise AssertionError('completed reads must not scan batch intake or system pressure')
    monkeypatch.setattr(w.intake,'summary',dashboard_only)
    monkeypatch.setattr(w.scheduler,'pressure',dashboard_only)
    response=client.get('/api/completed?limit=20')
    assert response.status_code==200,response.text
    assert [row['task_id'] for row in response.json()['items']]==[task['task_id']]
    detail=client.get('/api/completed/'+task['task_id'])
    assert detail.status_code==200,detail.text
    assert detail.json()['complete'] is True
    assert detail.json()['image_summary']['retained']==1


@pytest.mark.parametrize('block',['partial','frozen'])
def test_partial_or_frozen_delivery_is_not_a_completed_article(web_case,package_factory,block):
    w,client,_=web_case;login(client)
    task,_,digest=completed_paper(w,package_factory,two_targets=True)
    with w.store.db() as db:
        if block=='partial':db.execute("DELETE FROM specialty_active WHERE specialty_code='120202'")
        else:db.execute('INSERT INTO package_freezes VALUES(?,?,?)',(digest,'120202','test freeze'))
    response=client.get('/api/completed');assert response.status_code==200,response.text
    assert response.json()['items']==[]
    assert client.get('/api/completed/'+task['task_id']).status_code==404


def test_detail_uses_current_manifest_registered_routes_and_original_identity(web_case,package_factory):
    w,client,_=web_case;login(client)
    task,root,digest=completed_paper(w,package_factory,two_targets=True)
    old=w.root/'obsolete';old.mkdir();save(old/'assets.json',{'items':[{'id':'old-image','storage':'image'}]})
    w.store.update(task['task_id'],package_dir=str(old))
    # A later catalog edit must not change where this version was actually sent.
    save(w.config.catalog_path,{'majors':[dict(code='120201',name='工商管理',paper_dataset_id='wrong-new-paper',image_dataset_id='wrong-new-images')]})
    response=client.get('/api/completed/'+task['task_id']);assert response.status_code==200,response.text
    row=response.json()
    assert [(r['code'],r['paper_dataset_id'],r['materials_dataset_id']) for r in row['routes']]==[
        ('120201','paper-ds','image-ds'),('120202','second-paper','second-images')]
    details=row['completed_details'];images=details['image_versions']
    assert images[0]['manifest']==digest
    assert images[0]['state']=='checked' and images[0]['codes']==['120201','120202']
    assert [i['id'] for i in images[0]['items']]==['fig-current','table-text']
    assert images[0]['items'][0]['filename']=='fig.png'
    assert images[0]['items'][0]['source_location']=={'page':3}
    assert {r['dataset_id'] for r in images[0]['items'][0]['destinations']}=={'image-ds','second-images'}
    assert details['attachments']=={'state':'not_checked','count':None,'items':[]}
    assert row['completed_at']==1234.0


@pytest.mark.parametrize('failure',['missing','invalid','changed','array','null','bad_items'])
def test_unreadable_asset_inventory_is_unknown_not_zero(web_case,package_factory,failure):
    w,client,_=web_case;login(client)
    task,root,_=completed_paper(w,package_factory)
    if failure=='missing':(root/'assets.json').unlink()
    elif failure=='invalid':(root/'assets.json').write_text('{broken',encoding='utf-8')
    elif failure=='array':save(root/'assets.json',[])
    elif failure=='null':save(root/'assets.json',None)
    elif failure=='bad_items':save(root/'assets.json',{'items':['invalid item']})
    else:save(root/'assets.json',{'items':[]})
    response=client.get('/api/completed/'+task['task_id']);assert response.status_code==200,response.text
    row=response.json()
    assert row['image_summary']['retained'] is None
    assert row['image_summary']['state']=='unavailable'
    assert row['completed_details']['image_versions'][0]['state']=='unavailable'


def test_explicit_empty_final_asset_inventory_can_report_no_images(web_case,package_factory):
    w,client,_=web_case;login(client)
    task,_,_=completed_paper(w,package_factory,images=False)
    response=client.get('/api/completed/'+task['task_id']);assert response.status_code==200,response.text
    assert response.json()['image_summary']=={'state':'checked','retained':0,'described':0,'omitted':0}


def test_completed_attachment_preserves_pending_paper_link_and_exact_remote_facts(web_case):
    from paperflow.attachments import AttachmentLedger,prepare_package
    w,client,_=web_case;login(client)
    source=w.root/'装配图.dwg';source.write_bytes(b'original CAD bytes')
    task=w.store.register_source(source,sha(source),display_name=source.name)
    draft=w.config.root/'work'/task['task_id'];draft.mkdir(parents=True)
    save(draft/'profile.json',dict(attachment=dict(kind='drawing',description='装配图原件，未解析CAD。',related_paper_title='布局优化研究')))
    task['draft_dir']=str(draft);root=prepare_package(w.config,task)
    manifest=load(root/'manifest.json');ledger=AttachmentLedger(w.store)
    targets={'drawing':dict(parent_id='archive-folder',dataset_id='material-ds')}
    ledger._write(lambda db:ledger.register_sql(db,task,root,manifest,targets,sha(root/'manifest.json'),w.config.rag_url))
    with w.store.db() as db:
        row=db.execute('SELECT data FROM attachment_tasks WHERE task_id=?',(task['task_id'],)).fetchone();data=json.loads(row[0])
        data['remote']={'raw':{'file_id':'raw-original'},'note':{'document_id':'note-doc','dataset_id':'material-ds'}}
        db.execute("UPDATE attachment_tasks SET state='complete',data=? WHERE task_id=?",(json.dumps(data),task['task_id']))
        db.execute('INSERT INTO web_batches VALUES(?,?,?,?,?)',('attachment','attachment','[]',json.dumps([task['task_id']]),time.time()))
    response=client.get('/api/completed/'+task['task_id']);assert response.status_code==200,response.text
    row=response.json();item=row['completed_details']['attachments']['items'][0]
    assert item['filename']=='装配图.dwg' and item['bytes']==18
    assert item['sha256']==sha(source) and item['verification']=='original_and_note_readback'
    assert item['association']['status']=='pending' and item['association']['body_document_id'] is None
    assert item['remote']['raw']['file_id']=='raw-original' and item['target']['dataset_id']=='material-ds'
    assert row['attachment_summary']=={'state':'registered','count':1}
    assert row['image_summary']['state']=='not_applicable'


def test_completed_pagination_search_and_excluded_results_are_read_only(web_case,package_factory):
    w,client,_=web_case;login(client)
    one,_,_=completed_paper(w,package_factory,'first',images=False)
    two,_,_=completed_paper(w,package_factory,'second',images=False)
    excluded=w.store.register_source(w.root/'开题报告.docx','excluded',display_name='开题报告.docx')
    w.store.update(excluded['task_id'],stage='complete',disposition='exclude')
    with w.store.db() as db:
        db.execute('INSERT INTO web_batches VALUES(?,?,?,?,?)',('excluded','excluded','[]',json.dumps([excluded['task_id']]),time.time()))
    ids=[];cursor=None
    for _ in range(4):
        response=client.get('/api/completed',params={'limit':1,**({'cursor':cursor} if cursor else {})})
        assert response.status_code==200,response.text
        value=response.json();ids.extend(x['task_id'] for x in value['items']);cursor=value['next_cursor']
        if not cursor:break
    assert len(ids)==3 and set(ids)=={one['task_id'],two['task_id'],excluded['task_id']}
    filtered=client.get('/api/completed?query=second').json()['items']
    assert [x['task_id'] for x in filtered]==[two['task_id']]
    row=client.get('/api/completed/'+excluded['task_id']).json()
    assert row['state']=='excluded' and row['complete'] is False
    assert client.get('/api/completed?limit=0').status_code==400
    assert client.get('/api/completed?cursor=garbage').status_code==400


@pytest.mark.parametrize('storage',[{'type':'image','file':'fig.png','content':'模型保留原图说明'},'future-storage-mode','image'])
def test_compatible_metadata_keeps_details_without_claiming_image_upload(web_case,package_factory,storage):
    w,client,_=web_case;login(client)
    task,root,digest=completed_paper(w,package_factory)
    assets=load(root/'assets.json');assets['items'][0]['storage']=storage
    save(root/'assets.json',assets)
    # Model an accepted immutable historical package using that actual field shape.
    with w.store.db() as db:
        raw=bytes(db.execute('SELECT manifest_bytes FROM delivery_packages WHERE manifest_sha256=?',(digest,)).fetchone()[0])
        manifest=json.loads(raw)
        for entry in manifest['files']:
            if entry['path']=='assets.json':entry.update(bytes=(root/'assets.json').stat().st_size,sha256=sha(root/'assets.json'))
        encoded=json.dumps(manifest,ensure_ascii=False,indent=2).encode();new_digest=__import__('hashlib').sha256(encoded).hexdigest()
        db.execute('UPDATE delivery_packages SET manifest_sha256=?,manifest_bytes=? WHERE manifest_sha256=?',(new_digest,encoded,digest))
        for table in ('target_versions','specialty_tasks'):db.execute('UPDATE '+table+' SET manifest_sha256=? WHERE manifest_sha256=?',(new_digest,digest))
        active=json.loads(db.execute('SELECT data FROM specialty_active WHERE source_id=?',(task['source_id'],)).fetchone()[0])
        active['identity']['manifest_sha256']=new_digest;active['receipts']['120201'].pop('images')
        db.execute('UPDATE specialty_active SET manifest_sha256=?,data=? WHERE source_id=?',(new_digest,json.dumps(active),task['source_id']))
    response=client.get('/api/completed/'+task['task_id']);assert response.status_code==200,response.text
    row=response.json()
    assert row['image_summary']['retained'] is None
    assert row['image_summary']['state']=='unconfirmed'
    version=row['completed_details']['image_versions'][0]
    assert version['state']=='checked' and len(version['items'])==2
    item=version['items'][0]
    assert item['storage']=='unknown' and item['filename']=='fig.png'
    assert item['declared_storage']==('image' if isinstance(storage,dict) else storage)
