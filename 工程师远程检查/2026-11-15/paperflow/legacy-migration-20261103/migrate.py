"""Bounded, resumable legacy storage migration; one final result per folder."""
import argparse
import collections
from concurrent.futures import ThreadPoolExecutor,as_completed
import copy
from datetime import datetime,timezone
import hashlib
import io
import json
import os
import re
from pathlib import Path,PureWindowsPath
import sqlite3
import tempfile
import threading
import time
import uuid

import httpx
from minio import Minio
from minio.error import S3Error
import psycopg
from psycopg import sql
from haystack.utils import Secret
from haystack_integrations.document_stores.qdrant import QdrantDocumentStore
from qdrant_client import models
from paperflow_qdrant.embedding import OllamaEmbedder
from paperflow_qdrant.library import Library,stable_id
from article_repository.catalog import Catalog
from article_repository.files import Files
from article_repository.sync import RevisionSync
from legacy_migration import convert,compact,digest,role_key
from nas_transfer import publish_file

INPUT=Path('/input');STATE=Path('/state')
SCHEMA='legacy_migration_acceptance_20261103';TEST_COLLECTION='legacy_migration_acceptance_20261103'


class Objects:
    def __init__(self,root):self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True)
    def path(self,sha):return self.root/sha[:2]/sha
    def put(self,stream,expected=None):
        fd,name=tempfile.mkstemp(prefix='.legacy-migration-',dir=self.root)
        temporary=Path(name);hashed=hashlib.sha256()
        try:
            with os.fdopen(fd,'wb') as output:
                while chunk:=stream.read(256*1024):hashed.update(chunk);output.write(chunk)
                output.flush();os.fsync(output.fileno())
            sha=hashed.hexdigest()
            if expected and sha!=expected:raise ValueError('legacy object hash mismatch')
            target=self.path(sha);target.parent.mkdir(parents=True,exist_ok=True)
            if target.exists():
                with target.open('rb') as old:
                    if hashlib.file_digest(old,'sha256').hexdigest()!=sha:raise ValueError('existing object hash mismatch')
            else:os.replace(temporary,target)
            return sha
        finally:temporary.unlink(missing_ok=True)


class Source:
    def __init__(self,snapshot,storage):
        self.tenant=snapshot['tenant'];self.index='ragflow_'+self.tenant
        self.http=httpx.Client(base_url=os.environ['LEGACY_ES_URL'],auth=('elastic',os.environ['LEGACY_ES_PASSWORD']),
                               timeout=120,trust_env=False,limits=httpx.Limits(max_connections=8))
        self.minio=Minio(os.environ['LEGACY_MINIO_ENDPOINT'],access_key=os.environ['LEGACY_MINIO_USER'],
                         secret_key=os.environ['LEGACY_MINIO_PASSWORD'],secure=False)
        self.files={f['id']:f for f in storage['files']}
        self.file_by_doc={r['document_id']:r['file_id'] for r in storage['relations']}

    def query(self,path,body):
        response=self.http.post(path,json=body);response.raise_for_status();return response.json()

    def metadata(self,docs):
        response=self.query('ragflow_doc_meta_'+self.tenant+'/_search',{'size':1000,'query':{'terms':{'id':[d['id'] for d in docs]}}})
        fields={r['_source']['id']:r['_source'].get('meta_fields',{}) for r in response['hits']['hits']}
        return [{**d,'meta':{**d.get('meta',{}),**fields.get(d['id'],{})}} for d in docs]

    def chunks(self,doc,*,vectors=True):
        body={'size':512,'sort':['_doc'],'track_total_hits':True,'query':{'bool':{'filter':[
            {'term':{'doc_id':doc['id']}},{'term':{'kb_id':doc['kb_id']}}]}}}
        if not vectors:body['_source']={'excludes':['q_1024_vec']}
        result=self.query(self.index+'/_search?scroll=2m',body);rows=[];scroll=None
        try:
            while True:
                scroll=result.get('_scroll_id');hits=result['hits']['hits']
                if not hits:break
                rows.extend({**h['_source'],'id':h['_source'].get('id',h['_id']),'_es_id':h['_id']} for h in hits)
                if len(hits)<512:break
                result=self.query('_search/scroll',{'scroll':'2m','scroll_id':scroll})
        finally:
            if scroll:self.http.request('DELETE','_search/scroll',json={'scroll_id':[scroll]}).raise_for_status()
        return rows

    def original_stream(self,doc=None,file_id=None):
        key=file_id or (doc['id'] if doc else None)
        file=self.files.get(key) or self.files.get(self.file_by_doc.get(key))
        if file and (not file.get('source_type') or file['source_type']=='local'):
            bucket,key=file['bucket'],file['location']
        elif doc:bucket,key=doc['kb_id'],doc['location']
        elif file:bucket,key=file['bucket'],file['location']
        else:raise FileNotFoundError('legacy original file location is unknown')
        try:return self.minio.get_object(bucket,key)
        except S3Error as exc:
            if exc.code in ('NoSuchKey','NoSuchBucket','NoSuchObject'):raise FileNotFoundError('legacy original object is missing') from exc
            raise


class Migration:
    def __init__(self,acceptance=False):
        self.acceptance=acceptance
        self.snapshot=json.loads((INPUT/'source-inventory.json').read_text())
        self.majors=json.loads((INPUT/'majors.json').read_text())['majors']
        self.major_by_dataset={m[k]:m for m in self.majors for k in ('paper_dataset_id','image_dataset_id')}
        self.documents=[d for d in self.snapshot['documents'] if d['kb_id'] in self.major_by_dataset]
        self.source=Source(self.snapshot,json.loads((INPUT/'storage-inventory.json').read_text()))
        index=json.loads((INPUT/'package-index.json').read_text())
        self.plan_by_identity={tuple(p['identity']):p for p in index['plans']}
        overrides=INPUT/'context-overrides.json'
        self.context_overrides=json.loads(overrides.read_text()) if overrides.exists() else {}
        supplements=INPUT/'native-supplements.json'
        self.supplements=json.loads(supplements.read_text()).get('payloads',{}) if supplements.exists() else {}
        contexts=INPUT/'fallback-contexts.json'
        self.fallback_contexts=json.loads(contexts.read_text()) if contexts.exists() else {}
        collection=TEST_COLLECTION if acceptance else 'paperflow_v1'
        schema=SCHEMA if acceptance else 'public'
        if acceptance:
            with psycopg.connect(os.environ['ARTICLE_DATABASE_URL']) as connection:
                connection.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(sql.Identifier(SCHEMA)))
        store=QdrantDocumentStore(url=os.environ['QDRANT_URL'],api_key=Secret.from_env_var('QDRANT_API_KEY'),
            index=collection,embedding_dim=1024,progress_bar=False,wait_result_from_api=True,timeout=180,on_disk_payload=True)
        self.embedder=OllamaEmbedder(os.getenv('EMBEDDING_URL','http://172.17.0.1:11436'),concurrency=1)
        self.library=Library(store,self.embedder)
        self.catalog=Catalog(os.environ['ARTICLE_DATABASE_URL'],schema=schema);self.catalog.initialize();self.catalog.set_majors(self.majors)
        self.sync=RevisionSync(self.library)
        self.files=Files(os.environ['ARTICLE_NAS_ROOT'],username=os.environ['ARTICLE_NAS_USER'],password=os.environ['ARTICLE_NAS_PASSWORD'])
        self.files.stat(self.files.root)
        self.nas_verified=set()
        self.objects=Objects(STATE/'acceptance-objects' if acceptance else '/objects')
        self.lock=threading.Lock()
        self.ledger=STATE/('acceptance.sqlite' if acceptance else 'migration.sqlite')
        with sqlite3.connect(self.ledger) as connection:
            connection.execute('PRAGMA journal_mode=WAL')
            connection.execute('CREATE TABLE IF NOT EXISTS result(job_id TEXT PRIMARY KEY,status TEXT,detail TEXT,finished_at TEXT)')
        self.base_url='http://testserver' if acceptance else 'http://192.168.1.120:8097'

    def payload(self,identity):
        if compact(list(identity)) in self.supplements:return copy.deepcopy(self.supplements[compact(list(identity))])
        plan=self.plan_by_identity.get(identity)
        if not plan:return None
        with (INPUT/'packages.jsonl').open('rb') as stream:
            stream.seek(plan['offset']);payload=json.loads(stream.read(plan['length']))
        if compact(list(identity)) in self.context_overrides:payload['catalog_records']=self.context_overrides[compact(list(identity))]
        return payload

    def save(self,job,status,detail):
        with sqlite3.connect(self.ledger,timeout=30) as connection:
            connection.execute('INSERT INTO result VALUES(?,?,?,?) ON CONFLICT(job_id) DO UPDATE SET status=excluded.status,detail=excluded.detail,finished_at=excluded.finished_at',
                               (job,status,compact(detail),datetime.now(timezone.utc).isoformat()))

    def completed(self):
        with sqlite3.connect(self.ledger) as connection:return {r[0] for r in connection.execute("SELECT job_id FROM result WHERE status IN ('complete','superseded','recovered')")}

    def status(self):
        with sqlite3.connect(self.ledger) as connection:
            rows=connection.execute('SELECT status,detail FROM result').fetchall()
        counts=collections.Counter();totals=collections.Counter();errors=[]
        for state,raw in rows:
            if state in ('superseded','recovered'):continue
            value=json.loads(raw);counts[state]+=1
            if state=='complete':
                for key in ('source_documents','source_chunks','target_chunks','reused_vectors','new_vectors'):totals[key]+=value.get(key,0)
            elif len(errors)<12:errors.append(value)
        return {'folders':dict(counts),'totals':dict(totals),'errors':errors}

    def raw_object(self,doc=None,file_id=None,expected=None):
        source=self.source.original_stream(doc,file_id)
        try:return self.objects.put(source,expected)
        finally:source.close();source.release_conn()

    def load(self,identity,docs,payload):
        docs=self.source.metadata(docs);rows={};cache={};objects={};originals={};raw_files=[]
        for doc in docs:
            if all(identity):
                fields=doc['meta']
                actual=tuple(fields.get(k) for k in ('paperflow_task','paperflow_source_sha256','paperflow_manifest_sha256'))
                if all(actual) and actual!=identity:raise ValueError('source metadata identity changed')
        chosen={}
        for doc in sorted(docs,key=lambda d:(-d.get('chunk_count',0),d['id'])):chosen.setdefault(role_key(doc),doc['id'])
        for doc in docs:
            canonical=chosen[role_key(doc)]==doc['id']
            rows[doc['id']]=self.source.chunks(doc,vectors=canonical)
            for row in rows[doc['id']]:
                text=row.get('content_with_weight') or row.get('md_with_weight') or ''
                if row.get('q_1024_vec'):cache.setdefault(digest(text),row['q_1024_vec'])
        for doc in docs:
            for row in rows[doc['id']]:
                text=row.get('content_with_weight') or row.get('md_with_weight') or ''
                if not row.get('q_1024_vec') and digest(text) in cache:row['q_1024_vec']=cache[digest(text)]
                if row.get('img_id'):
                    image=row['img_id'];asset=None
                    if payload:
                        from legacy_migration import record_identity
                        rid=record_identity(text,row['id'],identity)
                        asset=next((a for a in payload.get('assets',[]) if 'asset_'+str(a.get('id'))==rid),None)
                    if asset and (INPUT/'images'/asset['sha256'][:2]/asset['sha256']).is_file():
                        sha=asset['sha256']
                        with (INPUT/'images'/sha[:2]/sha).open('rb') as stream:self.objects.put(stream,sha)
                    else:
                        bucket,key=image.split('-',1)
                        source=self.source.minio.get_object(bucket,key)
                        try:sha=self.objects.put(source)
                        finally:source.close();source.release_conn()
                    objects[image]=sha
            meta=doc['meta'];role=role_key(doc)
            if role.startswith('attachment'):
                old_file=meta.get('original_file_id')
                expected=meta.get('attachment_source_sha256') or meta.get('source_sha256')
                if old_file:
                    original_doc=next((d for d in self.snapshot['documents'] if d['id']==old_file),None)
                    sha=self.raw_object(original_doc,file_id=None if original_doc else old_file,expected=expected)
                    objects[old_file]=sha
                    if expected:objects[expected]=sha
                    saved=self.source.files.get(old_file) or original_doc or {}
                    raw_files.append({'id':'legacy-file-'+sha[:20],'name':meta.get('original_source_name') or saved.get('name') or '原始附件','sha256':sha,
                        'bytes':self.objects.path(sha).stat().st_size,'kind':'附件','relative':'objects/'+sha[:2]+'/'+sha,'availability':'unknown'})
                try:
                    sha=self.raw_object(doc)
                    text=self.objects.path(sha).read_text(encoding='utf-8-sig')
                    if not (text.lstrip().startswith('{') and 'paperflow_source_sha256' in text):originals[doc['id']]=text
                except (FileNotFoundError,UnicodeDecodeError):pass
            elif payload is None:
                try:
                    source=self.source.original_stream(doc)
                    try:
                        if doc.get('bytes',0)<65536:
                            raw=source.read()
                            try:stub=json.loads(raw) if raw.lstrip().startswith(b'{') else None
                            except ValueError:stub=None
                            if isinstance(stub,dict) and stub.get('paperflow_source_sha256'):sha=None
                            else:sha=self.objects.put(io.BytesIO(raw))
                        else:sha=self.objects.put(source)
                    finally:source.close();source.release_conn()
                    if sha:
                        raw_files.append({'id':'legacy-file-'+sha[:20],'name':doc['name'],'sha256':sha,
                            'bytes':self.objects.path(sha).stat().st_size,'kind':'原文','relative':'objects/'+sha[:2]+'/'+sha,'availability':'unknown'})
                        try:originals[doc['id']]=self.objects.path(sha).read_text(encoding='utf-8-sig')
                        except UnicodeDecodeError:pass
                except (FileNotFoundError,json.JSONDecodeError):pass
        if payload is None:
            first=next((d for d in docs if d['meta'].get('two_pass_profile_json')),docs[0])
            profile=json.loads(first['meta'].get('two_pass_profile_json') or '{}')
            tree=json.loads(first['meta'].get('two_pass_tree_json') or '{}')
            assets=json.loads(first['meta'].get('rag_mid_assets_json') or '[]')
            source=identity[1] if all(identity) else None
            payload={'identity':list(identity) if all(identity) else [None,None,None],'original':originals.get(first['id']),
                     'profile':profile,'tree':tree,'assets':assets,'chunks':[]}
            key='legacy-unresolved:'+self.snapshot['tenant']+':'+digest(compact(list(identity)))
            payload['catalog_records']=[{'source_key':key,'folder_name':'原始文件夹未记录','source':'legacy_ragflow_migration',
                'title':(profile.get('article') or {}).get('title') or first['name'],'profile':profile,'files':[],
                'inventory_complete':False,'source_sha256':source}]
            contexts=self.fallback_contexts.get('identities',{}).get(compact(list(identity)))
            if not contexts:
                matches={row['source_key']:row for file in raw_files for row in self.fallback_contexts.get('unique_file_hashes',{}).get(file['sha256'],[])}
                if len(matches)==1:contexts=list(matches.values())
            if contexts:
                base=payload['catalog_records'][0]
                payload['catalog_records']=[{**copy.deepcopy(base),**copy.deepcopy(context)} for context in contexts]
        payload=copy.deepcopy(payload);payload['originals']=originals
        return docs,rows,objects,payload,raw_files

    def nas_object(self,sha):
        if sha not in self.nas_verified:
            publish_file(self.objects.path(sha),sha,self.files)
            self.nas_verified.add(sha)

    def migrate_group(self,identity,docs,*,validate=False):
        payload=self.payload(identity)
        if payload and all(stable_id('legacy-migration',self.snapshot['tenant'],r['source_key'],*identity) in self.completed() for r in payload['catalog_records']):return []
        docs,rows,objects,payload,raw_files=self.load(identity,docs,payload)
        results=[]
        source_chunks=sum(len(page) for page in rows.values())
        for record in payload['catalog_records']:
            job=stable_id('legacy-migration',self.snapshot['tenant'],record['source_key'],*identity)
            if job in self.completed():continue
            aid=hashlib.sha256(record['source_key'].encode()).hexdigest()[:32]
            try:
                batch,vectors=convert(job,aid,payload,docs,rows,base_url=self.base_url,object_ids=objects)
                before=self.library.store.count_documents() if validate else None
                self.library.ingest(batch,embeddings=vectors)
                for document in batch['documents']:
                    for dataset in document['dataset_ids']:
                        manifest=self.library._existing([stable_id(document['id'],'manifest')]).get(stable_id(document['id'],'manifest'))
                        assert manifest and dataset in manifest.meta['dataset_ids'] and manifest.meta['document']==document
                record=copy.deepcopy(record)
                known={f.get('sha256') for f in record.get('files',[])}
                record.setdefault('files',[])
                for file in raw_files:
                    if not self.acceptance:self.nas_object(file['sha256'])
                    if file['sha256'] not in known:record['files'].append(file);known.add(file['sha256'])
                record.update(qdrant={'folder_ids':[job],'source_folder_id':job},migration='complete',source='legacy_ragflow_migration',
                    majors=sorted({self.major_by_dataset[d['kb_id']]['code'] for d in docs}),
                    source_updated_at=datetime.fromtimestamp(max(d.get('updated_at') or 0 for d in docs)/1000,timezone.utc).isoformat())
                record['files']=self.files.observe(record.get('files',[]))
                result=self.catalog.register(record)
                if result['manually_edited']:
                    result=self.catalog.synchronize(result['id'],self.sync)
                    if result['sync'].get('status')!='complete':raise RuntimeError('manual correction synchronization pending')
                else:
                    current=self.catalog.raw(result['id'])['base']['qdrant'].get('source_folder_id')
                    retire=[f for f in result['qdrant']['folder_ids'] if f!=current]
                    if retire:
                        self.library.store._client.set_payload(collection_name=self.library.store.index,payload={'active':False},key='meta',
                            points=models.Filter(must=[models.FieldCondition(key='meta.folder_id',match=models.MatchAny(any=retire))]),wait=True)
                detail={'article_id':aid,'folder_id':job,'source_documents':len(docs),'source_chunks':source_chunks,
                    'source_document_ids':[d['id'] for d in docs],'unindexed_source_documents':sum(not page for page in rows.values()),
                    'target_chunks':sum(len(d['records']) for d in batch['documents']),'reused_vectors':len(vectors),
                    'new_vectors':sum(len(d['records']) for d in batch['documents'])-len(vectors)}
                if validate:
                    self.validate(batch,payload,vectors,result)
                    count=self.library.store.count_documents()
                    self.library.ingest(batch,embeddings=vectors)
                    assert self.library.store.count_documents()==count
                    detail.update(validation='passed',new_points=count-before,repeat_adds_zero=True)
                self.save(job,'complete',detail);results.append(detail)
            except Exception as exc:
                self.save(job,'failed',{'identity':list(identity),'article_id':aid,'error':type(exc).__name__,'message':str(exc)[:500]})
                if validate:raise
        return results

    def validate(self,batch,payload,vectors,item):
        from fastapi.testclient import TestClient
        from paperflow_qdrant.api import create_app
        app=create_app(self.library,self.objects.root,token=os.environ['PAPERFLOW_RAG_API_KEY'])
        headers={'Authorization':'Bearer '+os.environ['PAPERFLOW_RAG_API_KEY']}
        with TestClient(app) as client:
            for doc in batch['documents']:
                for dataset in doc['dataset_ids']:
                    response=client.get('/api/v1/datasets/'+dataset+'/documents',params={'id':doc['id']},headers=headers)
                    response.raise_for_status();assert len(response.json()['data']['docs'])==1
                    chunks=client.get(f'/api/v1/datasets/{dataset}/documents/{doc["id"]}/chunks',headers=headers,params={'page_size':1000})
                    chunks.raise_for_status();assert chunks.json()['data']['total']==len(doc['records'])
                    if doc['meta_fields']['paperflow_role']=='body':
                        assert self.library.original(dataset,doc['id'])==payload['original']
                        full=self.library.document(dataset,doc['id'])['meta_fields']
                        assert json.loads(full['two_pass_profile_json'])==payload['profile']
                        units=payload['profile'].get('learning_units') or []
                        located=[s for u in units for s in u.get('section_ids',[]) if full['paperflow_section_chunks'].get(s)]
                        assert located,'No learning unit section could be resolved'
                    for record in doc['records']:
                        if record.get('image_id'):
                            image=client.get('/api/v1/documents/images/'+record['image_id'],headers=headers);image.raise_for_status()
                            assert hashlib.sha256(image.content).hexdigest()==record['image_id']
            first=next(d for d in batch['documents'] if d['meta_fields']['paperflow_role']=='body')
            enabled=next((r for r in first['records'] if r.get('available',True)),None)
            if enabled:
                for dataset in first['dataset_ids']:
                    answer=self.library.search(enabled['content'][:600],[dataset],document_ids=[first['id']],limit=1)
                    assert answer and answer[0]['document_id']==first['id']
        available=next((f for f in item['files'] if f['availability']=='available'),None)
        if available:
            file=self.files.download(available)
            try:
                with file.open('rb') as stream:assert hashlib.file_digest(stream,'sha256').hexdigest()==available['sha256']
            finally:file.unlink(missing_ok=True)

    def groups(self):
        keys=('paperflow_task','paperflow_source_sha256','paperflow_manifest_sha256')
        unresolved=[d for d in self.documents if not all(d['meta'].get(k) for k in keys)]
        enriched={d['id']:d for d in self.source.metadata(unresolved)} if unresolved else {}
        documents=[enriched.get(d['id'],d) for d in self.documents]
        by_id={d['id']:d for d in documents}
        for doc in documents:
            if all(doc['meta'].get(k) for k in keys):continue
            parent=by_id.get(doc['meta'].get('parent_body_document_id'))
            if parent and all(parent['meta'].get(k) for k in keys):
                doc['meta'].update({k:parent['meta'][k] for k in keys});continue
            response=self.source.query(self.source.index+'/_search',{'size':1,'_source':['content_with_weight'],
                'query':{'bool':{'filter':[{'term':{'doc_id':doc['id']}},{'term':{'kb_id':doc['kb_id']}}]}}})
            hits=response['hits']['hits'];text=hits[0]['_source'].get('content_with_weight','') if hits else ''
            if text.startswith('[PAPERFLOW]') and '[/PAPERFLOW]' in text:
                marker=json.loads(text[len('[PAPERFLOW]'):text.index('[/PAPERFLOW]')])
                identity=[marker.get(k) for k in ('task','source_sha256','manifest_sha256')]
                if all(identity):doc['meta'].update(dict(zip(keys,identity)))
        groups=collections.defaultdict(list)
        for doc in documents:
            meta=doc['meta'];identity=tuple(meta.get(k) for k in ('paperflow_task','paperflow_source_sha256','paperflow_manifest_sha256'))
            if not all(identity):identity=('legacy-document',doc['id'],None)
            groups[identity].append(doc)
        print(compact({'official_documents':len(documents),'source_groups':len(groups),
            'unresolved_document_identity':sum(1 for d in documents if not all(d['meta'].get(k) for k in keys))}),flush=True)
        return groups

    def run(self,workers=3,limit=None,skip_attachment_cards=False):
        groups=self.groups()
        selected=list(groups.items())
        if skip_attachment_cards:
            selected=[(identity,docs) for identity,docs in selected if not any(d.get('name','').startswith('PFA_MD_') or re.fullmatch(r'att_[A-Za-z0-9_-]+_note\.md',d.get('name','')) for d in docs)]
        if self.acceptance:
            selected=[]
            for identity,docs in groups.items():
                payload=self.payload(identity)
                if payload and 0<len(payload.get('assets',[]))<=8 and len(docs)>3 and payload['catalog_records'][0]['folder_name']!='原始文件夹未记录':
                    selected=[(identity,docs)];break
            assert selected,'No representative historical source was found'
        if limit:selected=selected[:limit]
        started=time.monotonic()
        with ThreadPoolExecutor(max_workers=1 if self.acceptance else workers) as executor:
            futures={executor.submit(self.migrate_group,identity,docs,validate=self.acceptance):identity for identity,docs in selected}
            for pos,future in enumerate(as_completed(futures)):
                identity=futures[future]
                try:
                    future.result()
                    with sqlite3.connect(self.ledger,timeout=30) as connection:
                        connection.execute('DELETE FROM result WHERE job_id=?',(stable_id('group-error',*identity),))
                except Exception as exc:
                    self.save(stable_id('group-error',*identity),'failed',{'identity':list(identity),'error':type(exc).__name__,'message':str(exc)[:500]})
                    if self.acceptance:raise
                if (pos+1)%20==0 or self.acceptance:
                    status=self.status();status.update(processed_groups=pos+1,total_groups=len(selected),elapsed_seconds=round(time.monotonic()-started,1))
                    (STATE/'progress.json').write_text(compact(status));print(compact(status),flush=True)
        status=self.status();status.update(total_groups=len(selected),elapsed_seconds=round(time.monotonic()-started,1))
        (STATE/('acceptance-result.json' if self.acceptance else 'migration-result.json')).write_text(compact(status))
        if not self.acceptance:self.catalog.source_status('旧库迁移',status)
        print(compact(status),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('phase',choices=['acceptance','migrate','status']);parser.add_argument('--workers',type=int,default=3);parser.add_argument('--limit',type=int);parser.add_argument('--skip-attachment-cards',action='store_true')
    args=parser.parse_args();STATE.mkdir(parents=True,exist_ok=True)
    migration=Migration(acceptance=args.phase=='acceptance')
    if args.phase=='status':print(compact(migration.status()))
    else:migration.run(workers=args.workers,limit=args.limit,skip_attachment_cards=args.skip_attachment_cards)
