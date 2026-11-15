"""生产接口契约探针：仅创建/修改自己登记的独立测试库与文档。"""
from __future__ import annotations
import base64
import hashlib
import json
from pathlib import Path
import time
from urllib.parse import quote
from urllib.request import Request
import uuid

from .package import load,save
from .rag import RagPublisher,native_client

PIXEL='iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j2ioAAAAASUVORK5CYII='


def image_bytes(client,image_id):
    return RagPublisher(client).download_image(image_id)


def run_probe(config,client=None):
    client=client or native_client(config);publisher=RagPublisher(client)
    folder=config.root/'probe';folder.mkdir(parents=True,exist_ok=True)
    identity_file=folder/'identity.json'
    if identity_file.is_file():identity=load(identity_file)
    else:
        identity=dict(name='PaperFlow API acceptance '+uuid.uuid4().hex,server=config.rag_url)
        save(identity_file,identity)
    if identity['server']!=config.rag_url:raise ValueError('probe server identity changed')
    datasets=client.datasets()
    if not identity.get('dataset_id'):
        matches=[d for d in datasets if d.get('name')==identity['name']]
        if len(matches)>1:raise ValueError('ambiguous probe dataset')
        if matches:dataset=matches[0]
        else:
            source=next((d for d in datasets if d.get('embedding_model')),None)
            body=dict(name=identity['name'],permission='me',chunk_method='naive')
            if source:body['embedding_model']=source['embedding_model']
            dataset=client.request('POST','/datasets',body)
        identity['dataset_id']=dataset['id'];save(identity_file,identity)
    ds=identity['dataset_id']
    assert any(d['id']==ds and d['name']==identity['name'] for d in client.datasets())
    report=dict(server=config.rag_url,dataset_id=ds,started_at=time.time(),checks={},metadata={},owned_documents=[])
    save(folder/'report.json',report)
    def note(key,value):
        report['checks'][key]=value;save(folder/'report.json',report)
    token=uuid.uuid4().hex
    try:
        names=['PF_API_'+token+'_a.md','PF_API_'+token+'_b.md']
        ids=[]
        for name in names:
            did=client.upload_bytes(ds,name,b'Isolated workflow API acceptance.');ids.append(did)
            report['owned_documents'].append(dict(dataset_id=ds,document_id=did,name=name))
            save(folder/'report.json',report)
            client.request('PATCH',f'/datasets/{ds}/documents/{did}',dict(enabled=0))
        did=ids[0]
        note('filter_name',[(d['id'],d['name']) for d in publisher.documents(ds,name=names[0])]==[(did,names[0])])
        note('filter_id',[(d['id'],d['name']) for d in publisher.documents(ds,id=did)]==[(did,names[0])])
        for size in (10*1024,100*1024,1024*1024):
            payload=dict(probe=token,value='x'*size)
            try:
                client.request('PATCH',f'/datasets/{ds}/documents/{did}',dict(meta_fields=payload))
                read=publisher.documents(ds,id=did)
                ok=len(read)==1 and read[0].get('meta_fields')==payload
                report['metadata'][str(size)]=dict(exact_readback=ok,json_bytes=len(json.dumps(payload).encode()))
            except Exception as exc:report['metadata'][str(size)]=dict(exact_readback=False,error_type=type(exc).__name__)
            save(folder/'report.json',report)
        note('metadata_10kb',report['metadata'][str(10*1024)]['exact_readback'])
        note('metadata_100kb',report['metadata'][str(100*1024)]['exact_readback'])
        content='PaperFlow isolated acceptance '+token+' 指标分解与研究步骤关系'
        result=client.request('POST',f'/datasets/{ds}/documents/{did}/chunks',dict(content=content,image_base64=PIXEL))
        chunk=result.get('chunk',result);cid=chunk.get('id');image_id=chunk.get('image_id')
        report['image_chunk_id']=cid;report['image_id']=image_id;save(folder/'report.json',report)
        rows=publisher.chunks(ds,did)
        note('image_base64_readback',bool(cid and image_id and any(r['id']==cid and r.get('image_id')==image_id for r in rows)))
        if image_id:
            raw=image_bytes(client,image_id)
            report['image_sha256']=hashlib.sha256(raw).hexdigest()
            note('image_download',bool(raw))
        else:note('image_download',False)
        client.request('PATCH',f'/datasets/{ds}/documents/{did}',dict(enabled=1))
        query=dict(question=content,dataset_ids=[ds],similarity_threshold=0.0,vector_similarity_weight=1.0,top_k=32,page_size=10)
        hits=client.request('POST','/retrieval',query).get('chunks',[])
        note('image_in_retrieval',any(r.get('id',r.get('chunk_id'))==cid and r.get('image_id')==image_id for r in hits))
        client.request('PATCH',f'/datasets/{ds}/documents/{did}',dict(enabled=0))
        hits=client.request('POST','/retrieval',query).get('chunks',[])
        note('disabled_excluded',not any(r.get('document_id',r.get('doc_id'))==did for r in hits))
    except Exception as exc:
        report['error']=dict(type=type(exc).__name__,message=str(exc)[:250])
    finally:
        # 只清理本次已持久登记的探针文档，不删测试库、不碰既有文章。
        cleaned=True
        for obj in report['owned_documents']:
            try:
                before=publisher.documents(ds,name=obj['name'])
                if any(d['id']!=obj['document_id'] for d in before):raise ValueError('probe cleanup identity conflict')
                if before:client.request('DELETE',f'/datasets/{ds}/documents',dict(ids=[obj['document_id']]))
                if any(d['id']==obj['document_id'] for d in publisher.documents(ds)):cleaned=False
            except Exception:cleaned=False
        note('owned_document_cleanup',cleaned)
        required={'filter_name','filter_id','metadata_10kb','metadata_100kb','image_base64_readback',
                  'image_download','image_in_retrieval','disabled_excluded','owned_document_cleanup'}
        report.update(completed_at=time.time(),passed=not report.get('error') and all(report['checks'].get(k) for k in required))
        save(folder/'report.json',report)
    return report
