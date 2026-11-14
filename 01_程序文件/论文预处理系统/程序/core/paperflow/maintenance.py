"""只回收曾由本工作流激活、已被稳定新版替换的远端文档。"""
import time,hashlib,json
from .rag import RemoteConflict
from .graph import ACTIVATION_LOCK


def collect_version(store,publisher,retired,limit=4):
    task=store.get(retired['task_id']);revision=retired['revision']
    receipt=store.receipt(task['task_id'],revision,'gc');count=0
    for code,target in retired['targets'].items():
        for role,obj in [(r,target.get(r)) for r in ('body','images')]+[('legacy',x) for x in target.get('legacy_images',[])]:
            if not obj:continue
            ds,did=obj['dataset_id'],obj['document_id'];key=ds+'/'+did
            if receipt.get(key,{}).get('status')=='deleted':continue
            if count>=limit:return count
            # inventory 查询也可可靠确认已删除；服务端按缺失 id 查询会抛归属错误。
            docs=[d for d in publisher.documents(ds) if d['id']==did]
            prior=receipt.get(key,{})
            if not docs:
                if prior.get('status')!='delete_intent':raise RemoteConflict('retired document missing before owned delete intent')
            else:
                meta=verify_retired_object(publisher,task,revision,code,role,obj,docs)
                receipt[key]=dict(status='delete_intent',dataset_id=ds,document_id=did,
                                  meta_fields=meta,read_before_at=time.time())
                store.receipt(task['task_id'],revision,'gc',receipt)
                publisher.client.request('DELETE',f'/datasets/{ds}/documents',{'ids':[did]})
            if any(d['id']==did for d in publisher.documents(ds)):raise ConnectionError('retired deletion not yet confirmed')
            receipt[key]=dict(receipt[key],status='deleted',read_after_at=time.time())
            store.receipt(task['task_id'],revision,'gc',receipt);count+=1
    return count


def maintain(config,store,publisher):
    count=0
    with ACTIVATION_LOCK:
        for old in store.retired_versions(stable_seconds=config.old_version_retention_seconds):
            count+=collect_version(store,publisher,old,config.maintenance_documents_per_pass-count)
            if count>=config.maintenance_documents_per_pass:break
    return count


def verify_retired_object(publisher,task,revision,code,role,obj,docs,*,strict_available=False):
    ds,did=obj["dataset_id"],obj["document_id"]
    doc=docs[0];meta=doc.get('meta_fields') or {}
    expected=dict(paperflow_task=task['task_id'],paperflow_revision=str(revision),
        paperflow_source_sha256=task['source_sha256'],paperflow_specialty=code,paperflow_role=role)
    expected_chunks=obj.get('chunks',{})
    chunks=publisher.chunks(ds,did)
    by_id={x.get('chunk_id'):x for x in expected_chunks.values()}
    if (len(chunks)!=len(expected_chunks) or {x['id'] for x in chunks}!=set(by_id) or
        any(hashlib.sha256(x.get('content','').encode()).hexdigest()!=by_id[x['id']].get('content_sha256') for x in chunks)):
        raise RemoteConflict('retired chunk inventory/content changed')
    manifest=obj.get('identity',{}).get('paperflow_manifest_sha256')
    if not manifest and chunks:
        # 旧回执没有 identity 时，只从已与持久散列一致的本程序标记取完整身份。
        try:
            markers=[json.loads(x['content'].split('[PAPERFLOW]',1)[1].split('[/PAPERFLOW]',1)[0]) for x in chunks]
            hashes={m['manifest_sha256'] for m in markers}
            if len(hashes)==1 and all(m['task']==task['task_id'] and m['source_sha256']==task['source_sha256'] for m in markers):
                manifest=hashes.pop()
        except (KeyError,IndexError,ValueError):pass
    if not manifest or len(manifest)!=64:raise RemoteConflict('retired immutable manifest proof unavailable')
    expected['paperflow_manifest_sha256']=manifest
    if (str(doc.get('status'))!='0' or doc.get('name')!=obj.get('name') or
        any(meta.get(k)!=v for k,v in expected.items())):
        raise RemoteConflict('retired document ownership/disabled readback mismatch')
    if strict_available and any(x.get("available") is not False for x in chunks):
        raise RemoteConflict("retired chunk availability not disabled")
    return meta
