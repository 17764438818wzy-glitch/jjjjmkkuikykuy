"""Read remote observations without rewriting local processing history."""
import copy
from datetime import datetime
from rag_status import normalize_remote_doc
from upload_tracking import service_key


def same_service(receipt_key,url):
    return '|' in receipt_key and service_key(receipt_key.rsplit('|',1)[0])==service_key(url)


def refresh_documents(store,refs,client,progress=lambda message:None):
    checked=0;observations={}
    for run_id,item_id in dict.fromkeys(refs):
        manifest=store.load(run_id)
        item=next((i for i in manifest.get('items',[]) if i.get('id')==item_id),None)
        if not item:continue
        for key,record in (item.get('uploads') or {}).items():
            if not same_service(key,client.base_url):continue
            dataset=record.get('dataset_id')
            for relative,receipt in [(None,record)]+[(a.get('path'),a) for a in record.get('asset_uploads',[])]:
                doc_id=receipt.get('document_id')
                if not dataset or not doc_id:continue
                identity=(dataset,doc_id)
                if identity not in observations:
                    try:
                        docs=[d for d in client.documents(dataset,document_id=doc_id) if d.get('id')==doc_id]
                        if len(docs)!=1:raise ValueError('远端未返回唯一对应文档，请核对知识库和文档是否仍存在')
                        status,message=normalize_remote_doc(docs[0])
                        observations[identity]={'ragflow_status':status,'ragflow_reason':message,
                            'progress':docs[0].get('progress'),'chunk_count':docs[0].get('chunk_count'),
                            'query_error':'','last_checked_at':datetime.now().isoformat(timespec='seconds')}
                        if status in {'running','queued','done'}:observations[identity]['parse_error']=''
                    except Exception as exc:
                        observations[identity]={'query_error':str(exc).replace(getattr(client,'api_key','') or '\0','[密钥]')[:500],
                            'last_checked_at':datetime.now().isoformat(timespec='seconds')}
                    checked+=1
                with store.lock:
                    latest=store.load(run_id)
                    current=next((i for i in latest.get('items',[]) if i.get('id')==item_id),None)
                    parent=(current or {}).get('uploads',{}).get(key,{})
                    target=parent if relative is None else next((a for a in parent.get('asset_uploads',[]) if a.get('path')==relative),{})
                    if target.get('document_id')==doc_id:
                        target.update(copy.deepcopy(observations[identity]));store.save(latest)
            progress('已查询：'+item.get('title',item_id))
    return checked


def recover_failed_assets(store,run_id,item_id,client,dataset_ids=None):
    """Resume only remotely failed/cancelled assets; never initiate untouched assets."""
    manifest=store.load(run_id);item=next(i for i in manifest['items'] if i['id']==item_id)
    observed={}
    for key,record in item.get('uploads',{}).items():
        if not same_service(key,client.base_url):continue
        dataset=record.get('dataset_id')
        if dataset_ids is not None and dataset not in dataset_ids:continue
        for receipt in record.get('asset_uploads',[]):
            doc_id=receipt.get('document_id')
            if receipt.get('status')!='uploaded' or not dataset or not doc_id:continue
            identity=(dataset,doc_id)
            if identity not in observed:
                fields={}
                try:
                    docs=[d for d in client.documents(dataset,document_id=doc_id) if d.get('id')==doc_id]
                    if len(docs)!=1:raise ValueError('附属资料远端文档未唯一确认')
                    status,message=normalize_remote_doc(docs[0])
                    fields.update(ragflow_status=status,ragflow_reason=message,query_error='')
                    if status in {'failed','cancelled'}:
                        try:client.parse(dataset,doc_id)
                        except Exception as exc:
                            fields.update(parse_status='unknown',parse_error=str(exc).replace(getattr(client,'api_key','') or '\0','[密钥]')[:500])
                        else:fields.update(ragflow_status='submitted',parse_status='submitted',parse_error='')
                    elif status in {'running','queued','done'}:fields['parse_error']=''
                except Exception as exc:
                    fields['query_error']=str(exc).replace(getattr(client,'api_key','') or '\0','[密钥]')[:500]
                observed[identity]=fields
            with store.lock:
                latest=store.load(run_id);current=next(i for i in latest['items'] if i['id']==item_id)
                rows=current.get('uploads',{}).get(key,{}).get('asset_uploads',[])
                target=next((r for r in rows if r.get('path')==receipt.get('path') and r.get('document_id')==doc_id),None)
                if target is not None:target.update(observed[identity]);store.save(latest)
