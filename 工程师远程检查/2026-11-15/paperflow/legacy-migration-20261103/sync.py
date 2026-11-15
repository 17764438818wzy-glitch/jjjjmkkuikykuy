"""Reuse the existing immutable batch writer for explicit article revisions."""
import copy
import json
from types import SimpleNamespace

from qdrant_client import models
from paperflow_qdrant.library import condition,stable_id
from .profiles import FIELDS


def revised_profile(profile,overrides):
    value=copy.deepcopy(profile)
    fields=overrides.get('fields',{})
    article=value.setdefault('article',{})
    if 'title' in overrides: article['title']=overrides['title']
    for name,key in [('summary','summary'),('keywords','keywords')]:
        if name in fields: article[key]=fields[name] if name=='summary' else [fields[name]]
    applicable=value.setdefault('applicable_level',{})
    if 'audience' in fields: applicable['applicable_audience']=fields['audience']
    if 'level' in fields: applicable['labels']=[fields['level']] if fields['level'] else []
    research=value.setdefault('research_profile',{})
    for name,key in [('object','object'),('path','overall_path')]:
        if name in fields: research[key]=fields[name]
    for field,target in [('methods','method'),('learn','what_to_learn')]:
        if field in fields:
            units=value.setdefault('learning_units',[])
            if not units: units.append({'id':'repository-correction'})
            for index,unit in enumerate(units): unit[target]=fields[field] if index==0 else ''
    return value


class RevisionSync:
    def __init__(self,library): self.library=library

    def manifests(self,folders=None,*,active=True):
        filters=[condition('kind','manifest')]
        if active: filters.append(condition('active',True))
        if folders: filters.append(condition('folder_id',folders,'in'))
        rows=self.library.store.filter_documents({'operator':'AND','conditions':filters})
        complete=self.library._existing(list({stable_id(r.meta['folder_id'],'complete') for r in rows}))
        return [r for r in rows if stable_id(r.meta['folder_id'],'complete') in complete]

    def catalog_manifests(self):
        """Catalog discovery does not load every article's chunks and original."""
        client=self.library.store._client
        selector=models.Filter(must=[models.FieldCondition(key='meta.kind',match=models.MatchValue(value='manifest')),
                                    models.FieldCondition(key='meta.active',match=models.MatchValue(value=True))])
        offset=None; rows=[]
        while True:
            page,offset=client.scroll(collection_name=self.library.store.index,scroll_filter=selector,
                limit=128,offset=offset,with_vectors=False,
                with_payload=models.PayloadSelectorInclude(include=['meta.folder_id','meta.document.id','meta.document.name',
                    'meta.document.dataset_ids','meta.document.meta_fields','meta.document.dataset_meta_fields']))
            rows.extend(SimpleNamespace(meta=p.payload['meta']) for p in page)
            if offset is None: break
        complete=self.library._existing(list({stable_id(r.meta['folder_id'],'complete') for r in rows}))
        return [r for r in rows if stable_id(r.meta['folder_id'],'complete') in complete]

    def apply(self,item,majors):
        folders=item.get('qdrant',{}).get('folder_ids') or []
        if not folders: return {'status':'saved_for_migration'}
        target_folder=stable_id('repository-revision',item['id'],str(item['revision']))
        # Read inactive source manifests too: a repeated confirmed revision is idempotent.
        source=self.manifests(item.get('qdrant',{}).get('source_folder_ids') or folders,active=False)
        if not source: raise ValueError('新库来源版本未核实，未进行同步')
        # One source version, not one copy per previously assigned specialty.
        latest=max(source,key=lambda r:(r.meta.get('document',{}).get('meta_fields',{}).get('paperflow_revision',''),r.meta['folder_id']))
        chosen=[r for r in source if r.meta['folder_id']==latest.meta['folder_id']]
        id_map={r.meta['document']['id']:stable_id(target_folder,r.meta['document']['id']) for r in chosen}
        docs=[]
        overrides=item.get('overrides',{})
        for row in chosen:
            doc=copy.deepcopy(row.meta['document'])
            role=doc.get('meta_fields',{}).get('paperflow_role') or next(iter(doc.get('dataset_meta_fields',{}).values()),{}).get('paperflow_role','body')
            key='paper_dataset_id' if role=='body' else 'image_dataset_id'
            doc['id']=id_map[doc['id']]
            doc['dataset_ids']=[m[key] for m in majors]
            previous=next(iter(doc.get('dataset_meta_fields',{}).values()),{})
            doc['dataset_meta_fields']={m[key]:{**previous,'paperflow_specialty':m['code']} for m in majors}
            for owner in doc['dataset_meta_fields'].values():
                if owner.get('parent_body_document_id') in id_map:
                    owner['parent_body_document_id']=id_map[owner['parent_body_document_id']]
            meta=doc['meta_fields']
            if role=='body':
                doc['name']=item['title']
                profile=json.loads(meta.get('two_pass_profile_json','{}'))
                meta['two_pass_profile_json']=json.dumps(revised_profile(profile,overrides),ensure_ascii=False,separators=(',',':'))
                # Generated profile projections must reflect corrected fields too.
                for record in doc['records']:
                    if record['id'].lstrip('_').startswith(('profile','learning','unit','applicability','migration')):
                        record['content']='当前画像修订：'+meta['two_pass_profile_json']
                doc['records'].append({'id':'repository-profile','content':item['title']+'\n'+
                    '\n'.join(FIELDS[k]+'：'+v for k,v in item['fields'].items() if v),'section_ids':[]})
                meta['paperflow_record_map']={r['id']:stable_id(doc['id'],r['id']) for r in doc['records']}
                meta['paperflow_section_chunks']={s:[stable_id(doc['id'],r['id']) for r in doc['records'] if s in r.get('section_ids',[])]
                    for s in {s for r in doc['records'] for s in r.get('section_ids',[])}}
            if meta.get('parent_body_document_id') in id_map: meta['parent_body_document_id']=id_map[meta['parent_body_document_id']]
            meta['repository_article_id']=item['id']; meta['repository_revision']=item['revision']
            docs.append(doc)
        for doc in docs:
            if not doc['meta_fields'].get('two_pass_profile_json'): continue
            for major in majors:
                owner=doc['dataset_meta_fields'][major['paper_dataset_id']]
                association=json.loads(owner.get('paperflow_image_associations_json') or '{}')
                if association:
                    old_id=association.get('document_id')
                    if old_id not in id_map: raise ValueError('配图版本关联未核实')
                    association.update(dataset_id=major['image_dataset_id'],document_id=id_map[old_id])
                    for rid,chunk in association.get('chunks',{}).items(): chunk['chunk_id']=stable_id(id_map[old_id],rid)
                    if association.get('identity'): association['identity']['paperflow_specialty']=major['code']
                    owner['paperflow_image_associations_json']=json.dumps(association,ensure_ascii=False,separators=(',',':'))
        result=self.library.ingest({'folder_id':target_folder,'documents':docs})
        for doc in docs:
            for dataset in doc['dataset_ids']:
                observed=self.library.document(dataset,doc['id'])
                if not observed or observed['chunk_count']!=len(doc['records']): raise ValueError('新归属读回未完成')
                parent=observed['meta_fields'].get('parent_body_document_id')
                if parent:
                    body=next((d for d in docs if d['id']==parent),None)
                    if not body or not all(self.library.document(ds,parent) for ds in body['dataset_ids']):
                        raise ValueError('附件正文关联读回未完成')
        # All target memberships are readable before old versions become inactive.
        old=[folder for folder in folders if folder!=target_folder]
        if old:
            selector=models.Filter(must=[models.FieldCondition(key='meta.folder_id',match=models.MatchAny(any=old))])
            self.library.store._client.set_payload(collection_name=self.library.store.index,payload={'active':False},key='meta',points=selector,wait=True)
        return {'status':'complete','qdrant':{'folder_ids':[target_folder]},'documents':result['document_ids']}
