"""Convert stored legacy records into the existing immutable folder contract."""
import collections
import copy
import hashlib
import json
import re

from paperflow_qdrant.identity import stable_id


def compact(value):return json.dumps(value,ensure_ascii=False,separators=(',',':'))
def digest(text):return hashlib.sha256(text.encode()).hexdigest()


def record_identity(text,legacy_id,identity):
    if text.startswith('[PAPERFLOW]'):
        stop=text.find('[/PAPERFLOW]')
        if stop<0:raise ValueError('legacy record identity marker is incomplete')
        marker=json.loads(text[len('[PAPERFLOW]'):stop])
        expected=dict(zip(('task','source_sha256','manifest_sha256'),identity))
        if any(marker.get(k)!=v for k,v in expected.items() if v):raise ValueError('legacy record identity mismatch')
        return marker['record']
    return str(legacy_id)


def role_key(doc):
    meta=doc.get('meta') or {}
    role=meta.get('paperflow_role') or ('images' if str(doc.get('name','')).endswith('_images.md') else 'body')
    if role.startswith('attachment'):
        return role+':'+str(meta.get('attachment_id') or meta.get('attachment_source_sha256') or meta.get('original_file_id') or doc['id'])
    return role


def convert(folder,article_id,payload,documents,rows,*,base_url,object_ids=None,document_map=None):
    object_ids=object_ids or {}
    document_map=document_map or {}
    identity=payload.get('identity') or [None,None,None]
    sections={row['id']:row.get('section_ids',[]) for row in payload.get('chunks',[])}
    assets={str(a['id']):a for a in payload.get('assets',[]) if a.get('id')}
    groups=collections.defaultdict(list)
    for doc in documents:
        if doc['id'] not in rows:raise ValueError('legacy document records were not read')
        signature=[]
        for row in rows[doc['id']]:
            content=row.get('content_with_weight') or row.get('md_with_weight') or ''
            rid=record_identity(content,row['id'],identity)
            image=object_ids.get(row.get('img_id')) or (assets.get(rid.removeprefix('asset_')) or {}).get('sha256')
            signature.append((rid,digest(content),row.get('available_int',1),row.get('chunk_order_int',0),image))
        groups[role_key(doc)+':'+digest(compact(sorted(signature,key=compact)))].append(doc)
    id_map={d['id']:stable_id(folder,key) for key,group in groups.items() for d in group}
    output=[];vectors={};record_maps={};final_records={};old_chunk_map={}
    variable={'paperflow_specialty','paperflow_task','paperflow_source_sha256','paperflow_manifest_sha256',
              'paperflow_revision','paperflow_role','paperflow_image_associations_json','parent_body_document_id'}
    for key,group in groups.items():
        group=sorted(group,key=lambda d:(-len(rows[d['id']]),d['id']))
        role=key.split(':',1)[0];did=id_map[group[0]['id']]
        metas=[d.get('meta') or {} for d in group]
        common={k:copy.deepcopy(v) for k,v in metas[0].items() if k not in variable and all(m.get(k)==v for m in metas[1:])}
        owners={}
        for doc in group:
            fields=doc.get('meta') or {}
            owner=owners.setdefault(doc['kb_id'],{})
            owner.update({k:copy.deepcopy(v) for k,v in fields.items() if k not in common})
            owner.setdefault('legacy_document_ids',[]).append(doc['id'])
        replacements={}
        if role.startswith('attachment'):
            for fields in [common,*owners.values()]:
                old=fields.get('original_file_id')
                sha=object_ids.get(old) or object_ids.get(fields.get('attachment_source_sha256'))
                if old or fields.get('original_download_url'):
                    if not sha:raise ValueError('attachment original object is not verified')
                    url=base_url.rstrip('/')+'/paperflow/objects/'+sha
                    if old:fields['legacy_original_file_id']=old
                    legacy_urls=[]
                    if fields.get('original_download_url'):
                        replacements[fields['original_download_url']]=url;legacy_urls.append(fields['original_download_url'])
                    for doc in group:
                        for row in rows[doc['id']]:
                            text=row.get('content_with_weight') or row.get('md_with_weight') or ''
                            for old_url in re.findall(r'https?://[^\s<>()]+/api/v1/files/'+re.escape(str(old))+r'(?![a-zA-Z0-9])',text):
                                replacements[old_url]=url;legacy_urls.append(old_url)
                    if legacy_urls:fields['legacy_original_download_urls']=sorted(set(legacy_urls))
                    for label in ('原件文件ID：','原件编号：'):replacements[label+str(old)]=label+sha
                    fields.update(original_file_id=sha,original_download_url=url)
                original_id=fields.pop('original_document_id',None)
                if original_id:fields['legacy_original_document_id']=original_id
                primary_id=fields.get('primary_document_id')
                if primary_id:
                    target=id_map.get(primary_id) or document_map.get(str(fields.get('primary_dataset_id') or common.get('primary_dataset_id'))+'/'+primary_id)
                    fields['legacy_primary_document_id']=primary_id
                    if target:
                        fields['primary_document_id']=target
                        replacements['完整MD文档ID：'+primary_id]='完整MD文档ID：'+target
                    else:
                        fields.pop('primary_document_id',None)
                        fields['primary_document_status']='unresolved_legacy_reference'
                        replacements['完整MD文档ID：'+primary_id]='旧库完整MD文档ID（尚未定位）：'+primary_id
        records=[];by_rid={};mapping={}
        for doc in group:
            for row in sorted(rows[doc['id']],key=lambda r:(r.get('chunk_order_int',0),str(r.get('id','')))):
                content=row.get('content_with_weight') or row.get('md_with_weight')
                if not isinstance(content,str) or not content.strip():raise ValueError('empty legacy record content')
                rid=record_identity(content,row['id'],identity)
                prior_content=content
                for old,url in replacements.items():content=content.replace(old,url)
                if rid in by_rid and by_rid[rid]['content']!=content:rid+='__legacy_'+digest(content)[:12]
                existing=by_rid.get(rid)
                if existing:
                    if row.get('available_int',1)==1:existing['available']=True
                    old_chunk_map[row['id']]=stable_id(did,rid)
                    if content==prior_content and row.get('q_1024_vec') and stable_id(did,rid) not in vectors:
                        vectors[stable_id(did,rid)]={'content_sha256':digest(content),'vector':row['q_1024_vec']}
                    continue
                base_rid=rid.split('.p',1)[0]
                section_ids=sections.get(rid,sections.get(base_rid,[]))
                record={'id':rid,'content':content,'section_ids':section_ids,'available':row.get('available_int',1)==1}
                if row.get('position_int'):record['positions']=row['position_int']
                if row.get('img_id'):
                    asset=assets.get(rid.removeprefix('asset_'))
                    image_id=object_ids.get(row['img_id']) or (asset or {}).get('sha256')
                    if not image_id or not re.fullmatch('[a-f0-9]{64}',image_id):raise ValueError('legacy image object is not verified')
                    record['image_id']=image_id
                    if asset and asset.get('section_id'):record['section_ids']=[asset['section_id']]
                records.append(record);by_rid[rid]=record
                mapping[rid]=stable_id(did,rid);old_chunk_map[row['id']]=stable_id(did,rid)
                if content==prior_content and row.get('q_1024_vec'):vectors[stable_id(did,rid)]={'content_sha256':digest(content),'vector':row['q_1024_vec']}
        original=(payload.get('original') if role=='body' else compact(payload.get('assets',[])) if role=='images'
                  else (payload.get('originals') or {}).get(group[0]['id']))
        if original is None:
            original='\n\n'.join(re.sub(r'^\[PAPERFLOW\].*?\[/PAPERFLOW\]\n?','',r['content'],count=1) for r in records)
            common['legacy_original_reconstructed']=True
        for old,url in replacements.items():original=original.replace(old,url)
        common.update(paperflow_role=role,paperflow_backend='qdrant',repository_article_id=article_id,
                      legacy_migration=True,legacy_document_ids=[d['id'] for d in group],
                      paperflow_record_map=mapping,paperflow_object_base_url=base_url.rstrip('/')+'/paperflow/objects/')
        if not records:common['legacy_unindexed']=True
        if role=='body':
            common.setdefault('two_pass_profile_json',compact(payload.get('profile') or {}))
            common.setdefault('two_pass_tree_json',compact(payload.get('tree') or {}))
            common.setdefault('rag_mid_assets_json',compact(payload.get('assets',[])))
            common['paperflow_original_sha256']=digest(original)
            common['paperflow_section_chunks']={s:[stable_id(did,r['id']) for r in records if s in r['section_ids']]
                for s in {s for r in records for s in r['section_ids']}}
        name=((payload.get('profile') or {}).get('article') or {}).get('title') if role=='body' else None
        output.append({'id':did,'name':name or group[0]['name'],'dataset_ids':list(owners),'meta_fields':common,
                       'dataset_meta_fields':owners,'records':records,'original':original})
        record_maps[did]=mapping
        final_records[did]={r['id']:r for r in records}
    for doc in output:
        meta=doc['meta_fields']
        for owner in doc['dataset_meta_fields'].values():
            for field in ('paperflow_record_map','paperflow_section_chunks','paperflow_original_sha256','paperflow_backend','paperflow_object_base_url'):
                if field in owner:owner[field]=copy.deepcopy(meta.get(field))
            if owner.get('parent_body_document_id'):
                old=owner['parent_body_document_id']
                if old not in id_map:raise ValueError('attachment parent body is absent')
                owner['parent_body_document_id']=id_map[old]
            association=owner.get('paperflow_image_associations_json')
            if association:
                value=json.loads(association) if isinstance(association,str) else copy.deepcopy(association)
                old=value.get('document_id')
                if old:
                    if old not in id_map:raise ValueError('image association document is absent')
                    value['document_id']=id_map[old]
                    for rid,item in value.get('chunks',{}).items():
                        target=record_maps[id_map[old]].get(rid)
                        if target is None:raise ValueError('image association record is absent')
                        item['chunk_id']=target
                        target_record=final_records[id_map[old]][rid]
                        if target_record.get('image_id'):item['image_id']=target_record['image_id']
                    owner['paperflow_image_associations_json']=compact(value)
    if not output:raise ValueError('legacy folder has no stored records')
    return {'folder_id':folder,'documents':output},vectors
