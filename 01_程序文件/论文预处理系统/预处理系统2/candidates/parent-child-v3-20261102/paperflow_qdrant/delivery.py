"""Connect sealed Paperflow records to folder-level batch delivery."""
import copy
import hashlib
import json
import logging
from pathlib import Path
import time
import httpx

from .identity import stable_id


def compact(value): return json.dumps(value,ensure_ascii=False,separators=(',',':'))


def _read_with_retry(read, require_running):
    """Bounded transport recovery for reads only; every attempt honors Pause."""
    for attempt in range(3):
        require_running()
        try:
            return read()
        except httpx.TransportError:
            if attempt == 2:
                raise
            time.sleep(.25 * (attempt + 1))


def _verify_object(client, digest):
    with client.stream('GET','/paperflow/objects/'+digest) as response:
        response.raise_for_status()
        observed=hashlib.sha256()
        for part in response.iter_bytes(): observed.update(part)
        if observed.hexdigest()!=digest: raise ValueError('remote object readback mismatch')


def build_folder(paper, identity, task, packet, handoff, budget, base_url):
    from paperflow.rag import RagPublisher
    from paperflow.package import inside
    native=RagPublisher(None)
    routes=packet['routes']
    folder_id=stable_id(identity['task_id'],identity['group_id'],task['source_sha256'],packet['manifest_sha256'])
    documents=[]; objects={}; targets={r['code']:{'backend':'qdrant','formal_route':copy.deepcopy(r),
        'ticket_identity':copy.deepcopy(identity)} for r in routes}
    attachments={}
    chunk_metadata={r['id']:r for r in packet['chunks']}
    for role, records in native.records(packet).items():
        if not records: continue
        did=stable_id(folder_id,role)
        dataset_key='paper_dataset_id' if role=='body' else 'image_dataset_id'
        converted=[]
        for rid, text, asset in records:
            record={'id':rid,'content':native.render(task,packet,(rid,text,asset)),
                    'section_ids':chunk_metadata.get(rid,{}).get('section_ids',[])}
            if asset:
                path=inside(packet['root'],asset['file'])
                objects[asset['sha256']]=path
                record.update(image_id=asset['sha256'],section_ids=[asset['section_id']] if asset.get('section_id') else [])
            converted.append(record)
        identities={}
        for route in routes:
            owner=dict(paperflow_task=task['task_id'],paperflow_source_sha256=task['source_sha256'],
                paperflow_manifest_sha256=packet['manifest_sha256'],paperflow_revision=str(task['revision']),
                paperflow_specialty=route['code'],paperflow_role=role)
            identities[route[dataset_key]]=copy.deepcopy(owner)
            targets[route['code']][role]={'dataset_id':route[dataset_key],'document_id':did,'identity':owner,
                'chunks':{r['id']:{'status':'written','chunk_id':stable_id(did,r['id']),
                    'image_id':r.get('image_id'),'content_sha256':hashlib.sha256(r['content'].encode()).hexdigest()}
                    for r in converted}}
        metadata={}
        if role=='body':
            source=(Path(packet['root'])/'article.redacted.md').read_text(encoding='utf-8-sig')
            retained=packet['profile'].get('retained_content',[])
            for item in retained:
                path=inside(packet['root'],item['file'])
                with path.open('rb') as stream:
                    if hashlib.file_digest(stream,'sha256').hexdigest()!=item['sha256']:
                        raise ValueError('retained source object changed')
                objects[item['sha256']]=path
            metadata.update(two_pass_profile_json=compact(packet['profile']),two_pass_tree_json=compact(packet['tree']),
                rag_mid_assets_json=compact(packet['assets']),paperflow_contract='paperflow/1.0',
                paperflow_record_map={r['id']:stable_id(did,r['id']) for r in converted},
                paperflow_section_chunks={section:[stable_id(did,r['id']) for r in converted if section in r['section_ids']]
                    for section in {section for r in converted for section in r['section_ids']}},
                paperflow_original_sha256=hashlib.sha256(source.encode('utf8')).hexdigest(),
                paperflow_backend='qdrant',paperflow_object_base_url=base_url.rstrip('/')+'/paperflow/objects/')
            if retained:
                metadata.update(paperflow_format_status=packet['profile'].get('format_status'),
                    paperflow_retained_objects=retained)
        name=native.document_names(task,packet,routes[0]['code'],role)[1]
        documents.append({'id':did,'name':name,'dataset_ids':[r[dataset_key] for r in routes],
            'meta_fields':metadata,'dataset_meta_fields':identities,'records':converted,
            'original':(Path(packet['root'])/'article.redacted.md').read_text(encoding='utf-8-sig') if role=='body' else compact(packet['assets'])})
    body_doc=documents[0]
    for route in routes:
        body_doc['dataset_meta_fields'][route['paper_dataset_id']]['paperflow_image_associations_json']=compact(targets[route['code']].get('images',{}))
    for item in handoff['attachments']:
        from paperflow_prefect.attachment_ingest import read_attachment_delivery
        card=read_attachment_delivery(item)
        meta=card['metadata']; aid=meta['attachment_id']; digest=meta['source_sha256']
        objects[digest]=Path(card['delivery']['source_path'])
        did=stable_id(folder_id,'attachment',aid,item['md_sha256'])
        pointer='\n\n原附件下载接口（需认证）：'+base_url.rstrip('/')+'/paperflow/objects/'+digest+'\n原附件SHA256：'+digest+'\n'
        projection=card['md_text']+pointer
        retained=item.get('retained_response')
        retained_metadata=[]
        if retained:
            path=Path(retained['path']);raw=path.read_bytes()
            if hashlib.sha256(raw).hexdigest()!=retained['sha256']:
                raise ValueError('retained attachment response changed')
            objects[retained['sha256']]=path
            projection+='\n\n模型完整原回答（未解析）：\n'+raw.decode('utf8')
            retained_metadata=[{'role':'llm_response','sha256':retained['sha256'],'bytes':len(raw)}]
        def render(index,part): return '<!-- PFA '+did+' '+str(index).zfill(6)+' -->\n'+part
        parts=budget.split(projection,render)
        records=[{'id':str(i),'content':render(i,part),'section_ids':[]} for i,part in enumerate(parts)]
        fields={'paperflow_role':'attachment-card','paper_id':paper['paper_id'],'attachment_id':aid,
            'attachment_source_sha256':digest,'body_source_sha256':paper['body']['sha256'],
            'original_file_id':digest,'original_download_url':base_url.rstrip('/')+'/paperflow/objects/'+digest,
            'parent_link_status':'verified_remote_body','parent_body_document_id':body_doc['id'],
            'md_sha256':item['md_sha256'],'paperflow_backend':'qdrant'}
        if retained:
            fields.update(paperflow_format_status='unstructured_retained',paperflow_retained_objects=retained_metadata)
        documents.append({'id':did,'name':Path(item['md_path']).name,'dataset_ids':[r['image_dataset_id'] for r in routes],
            'meta_fields':fields,'dataset_meta_fields':{r['image_dataset_id']:{'paperflow_specialty':r['code']} for r in routes},
            'original':card['md_text'],'records':records})
        attachments[aid]={'backend':'qdrant','status':'verified','delivery_state':'complete','ticket_identity':copy.deepcopy(identity),
            'binding':{'paper_id':paper['paper_id'],'attachment_id':aid,'source_sha256':digest},
            'raw':{'file_id':digest,'source_sha256':digest},'current_md_sha256':item['md_sha256'],
            'publications':{item['md_sha256']:{r['code']:{'status':'verified','dataset_id':r['image_dataset_id'],
                'document_id':did,'retrieval_verified':True} for r in routes}}}
    return {'folder_id':folder_id,'documents':documents},targets,attachments,objects


def deliver_folder(*, paper, identity, task, packet, handoff, budget, base_url, api_key, check_running=None):
    def require_running():
        if check_running is not None and not check_running():
            raise InterruptedError('控制会话已改变，未开始后续交付请求')

    require_running()
    batch,targets,attachments,objects=build_folder(paper,identity,task,packet,handoff,budget,base_url)
    headers={'Authorization':'Bearer '+api_key}
    with httpx.Client(base_url=base_url,headers=headers,timeout=httpx.Timeout(1200,connect=10),trust_env=False) as client:
        for digest,path in objects.items():
            upload_error=None
            with path.open('rb') as stream:
                if hashlib.file_digest(stream,'sha256').hexdigest()!=digest:
                    raise ValueError('object changed before upload')
                stream.seek(0)
                require_running()
                try:
                    response=client.put('/paperflow/objects/'+digest,content=stream)
                    response.raise_for_status()
                except httpx.TransportError as exc:
                    upload_error=exc
            try:
                _read_with_retry(lambda: _verify_object(client,digest), require_running)
            except Exception as exc:
                if upload_error is not None:
                    raise upload_error from exc
                raise
            if upload_error is not None:
                logging.getLogger(__name__).warning('Object upload ACK lost; exact SHA256 readback confirmed %s; no upload replay',digest)
        require_running()
        response=client.post('/paperflow/folders',json=batch)
        response.raise_for_status(); result=response.json()
        if result.get('status')!='complete' or result.get('folder_id')!=batch['folder_id']:
            raise ValueError('folder completion not confirmed')
        for doc in batch['documents']:
            for ds in doc['dataset_ids']:
                require_running()
                response=_read_with_retry(lambda: client.get(f'/api/v1/datasets/{ds}/documents',
                    params={'id':doc['id']}), require_running)
                response.raise_for_status(); rows=response.json()['data']['docs']
                expected={**doc['meta_fields'],**doc.get('dataset_meta_fields',{}).get(ds,{})}
                if (len(rows)!=1 or rows[0]['id']!=doc['id'] or rows[0]['dataset_id']!=ds or
                        rows[0]['meta_fields']!=expected or rows[0]['chunk_count']!=len(doc['records'])):
                    raise ValueError('full metadata or library membership readback mismatch')
                require_running()
                response=client.post('/api/v1/retrieval',json={'question':doc['records'][0]['content'][:800],
                    'dataset_ids':[ds],'document_ids':[doc['id']],'page_size':1})
                response.raise_for_status()
                if not any(hit.get('dataset_id')==ds and hit.get('document_id')==doc['id']
                           for hit in response.json()['data']['chunks']):
                    raise ValueError('document not searchable in expected library')
    for target in targets.values():
        target.update(body_delivery_state='complete',verified=True,retrieval_verified=True)
    return targets,attachments
