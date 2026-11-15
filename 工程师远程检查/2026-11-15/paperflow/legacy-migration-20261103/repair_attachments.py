"""Publish one bounded correction for historical attachment cards, retaining source history."""
import argparse,collections,copy,hashlib,json,os,re,sqlite3,time
from datetime import datetime,timezone
from pathlib import Path
from qdrant_client import models
from migrate import Migration,STATE
from attachment_notes import enrich
from legacy_migration import compact,convert
from paperflow_qdrant.library import stable_id
from psycopg.types.json import Jsonb

def card(doc):
    return doc.get('name','').startswith('PFA_MD_') or bool(re.fullmatch(r'att_[A-Za-z0-9_-]+_note\.md',doc.get('name','')))

def current_map(migration):
    candidates=collections.defaultdict(set);offset=None
    while True:
        page,offset=migration.library.store._client.scroll('paperflow_v1',limit=256,offset=offset,with_vectors=False,
            scroll_filter=models.Filter(must=[models.FieldCondition(key='meta.kind',match=models.MatchValue(value='manifest')),
                models.FieldCondition(key='meta.active',match=models.MatchValue(value=True))]),
            with_payload=models.PayloadSelectorInclude(include=['meta.folder_id','meta.document.id','meta.document.dataset_ids','meta.document.dataset_meta_fields']))
        complete=migration.library._existing([stable_id(p.payload['meta']['folder_id'],'complete') for p in page])
        for point in page:
            meta=point.payload['meta'];doc=meta['document']
            if stable_id(meta['folder_id'],'complete') not in complete:continue
            for dataset,owner in doc.get('dataset_meta_fields',{}).items():
                if dataset not in doc['dataset_ids']:raise ValueError('Alias membership mismatch')
                for old in owner.get('legacy_document_ids',[]):candidates[dataset+'/'+old].add(doc['id'])
        if offset is None:break
    return {key:next(iter(values)) for key,values in candidates.items() if len(values)==1}

def retire(migration,folders):
    if folders:migration.library.store._client.set_payload('paperflow_v1',payload={'active':False},key='meta',wait=True,
        points=models.Filter(must=[models.FieldCondition(key='meta.folder_id',match=models.MatchAny(any=sorted(set(folders))))]))

def run(limit=None,*,reference_refresh=False):
    migration=Migration();started=time.monotonic()
    raw_metadata=migration.source.metadata;note_cache={}
    def metadata(docs):
        result=raw_metadata(docs)
        for d in result:
            if card(d) and d['id'] not in note_cache:note_cache[d['id']]=migration.source.chunks(d,vectors=False)
        return [enrich(d,note_cache[d['id']]) if card(d) else d for d in result]
    migration.source.metadata=metadata
    migration.documents=[*metadata([d for d in migration.documents if card(d)]),*[d for d in migration.documents if not card(d)]]
    groups=[(identity,docs) for identity,docs in migration.groups().items() if any(card(d) for d in docs)]
    if reference_refresh:
        audit=json.loads((STATE/'reference-alias-audit.json').read_text())
        selected={old for row in audit['rows'] if row.get('now_unique_target') for old in row['legacy_source_documents']}
        groups=[g for g in groups if any(d['id'] in selected for d in g[1])]
    waves=([g for g in groups if not any(d['meta'].get('primary_document_id') for d in g[1])],
           [g for g in groups if any(d['meta'].get('primary_document_id') for d in g[1])])
    processed=0;failures=[];unresolved=0
    for wave in waves:
        aliases=current_map(migration)
        for identity,docs in wave:
            if limit is not None and processed>=limit:break
            payload=migration.payload(identity)
            try:
                docs,rows,objects,payload,raw_files=migration.load(identity,docs,payload)
                for record in payload['catalog_records']:
                    aid=hashlib.sha256(record['source_key'].encode()).hexdigest()[:32]
                    namespace='legacy-attachment-reference-fix-20261103-v2' if reference_refresh else 'legacy-attachment-fix-20261103'
                    job=stable_id(namespace,migration.snapshot['tenant'],record['source_key'],*identity)
                    if job in migration.completed():continue
                    batch,vectors=convert(job,aid,payload,docs,rows,base_url=migration.base_url,object_ids=objects,document_map=aliases)
                    has_body=any(d['meta_fields']['paperflow_role']=='body' for d in batch['documents'])
                    for doc in batch['documents']:
                        for dataset in doc['dataset_ids']:
                            fields={**doc['meta_fields'],**doc['dataset_meta_fields'][dataset]}
                            if fields.get('primary_document_id'):
                                local=next((d for d in batch['documents'] if d['id']==fields['primary_document_id'] and fields['primary_dataset_id'] in d['dataset_ids']),None)
                                if not local and not migration.library.document(fields['primary_dataset_id'],fields['primary_document_id']):raise ValueError('Primary attachment reference is not currently readable')
                            elif fields.get('legacy_primary_document_id'):
                                if reference_refresh:raise ValueError('Reference target is no longer unique; refresh not published')
                                unresolved+=1
                    migration.library.ingest(batch,embeddings=vectors)
                    for doc in batch['documents']:
                        for dataset in doc['dataset_ids']:
                            observed=migration.library.document(dataset,doc['id'])
                            assert observed and observed['chunk_count']==len(doc['records'])
                            fields=observed['meta_fields']
                            if fields.get('primary_document_id'):assert migration.library.document(fields['primary_dataset_id'],fields['primary_document_id'])
                    record=copy.deepcopy(record)
                    for file in raw_files:migration.nas_object(file['sha256'])
                    record['files']=migration.catalog._files(record.get('files',[]),raw_files,False)
                    record['files']=migration.files.observe(record['files'])
                    record.update(source='legacy_ragflow_migration',migration='complete',
                        source_updated_at=datetime.fromtimestamp(max(d.get('updated_at') or 0 for d in docs)/1000,timezone.utc).isoformat(),
                        majors=sorted({migration.major_by_dataset[d['kb_id']]['code'] for d in docs}),
                        qdrant={'folder_ids':[job],'source_folder_id':job})
                    try:prior=migration.catalog.raw(aid)
                    except LookupError:prior=None
                    if not has_body:
                        if prior and prior['base'].get('entry_kind')!='attachment' and prior['source_key'].startswith('folder:'):
                            # File recovery never replaces a known paper's body/profile publication.
                            with migration.catalog.connect() as conn:
                                current=conn.execute('SELECT base FROM article WHERE id=%s FOR UPDATE',(aid,)).fetchone()['base']
                                current['files']=migration.catalog._files(current.get('files',[]),record['files'],False)
                                conn.execute('UPDATE article SET base=%s WHERE id=%s',(Jsonb(current),aid))
                            result=migration.catalog.detail(aid)
                        else:
                            record['entry_kind']='attachment';record['parent_link_status']='unresolved'
                            result=migration.catalog.register(record)
                    else:
                        result=migration.catalog.register(record)
                        if result['manually_edited']:
                            result=migration.catalog.synchronize(aid,migration.sync)
                            if result['sync'].get('status')!='complete':raise RuntimeError('Manual correction sync pending')
                    with sqlite3.connect(migration.ledger,timeout=30) as conn:
                        previous=conn.execute('SELECT job_id,status,detail FROM result WHERE job_id!=?',(job,)).fetchall()
                    old=[];old_jobs=[];ids={d['id'] for d in docs}
                    for old_job,status,raw in previous:
                        detail=json.loads(raw)
                        same=detail.get('article_id')==aid and (set(detail.get('source_document_ids',[]))==ids or detail.get('identity')==list(identity))
                        if same and status in ('complete','failed'):
                            old_jobs.append(old_job)
                            old.append(detail.get('folder_id') or old_job)
                    if has_body and not result['manually_edited']:
                        current=migration.catalog.raw(aid)['base'].get('qdrant',{}).get('source_folder_id')
                        old.extend(f for f in result['qdrant']['folder_ids'] if f!=current)
                    retire(migration,old)
                    detail={'article_id':aid,'folder_id':job,'entry_kind':'article' if has_body else 'attachment',
                        'source_documents':len(docs),'source_document_ids':sorted(ids),'source_chunks':sum(len(p) for p in rows.values()),
                        'target_chunks':sum(len(d['records']) for d in batch['documents']),'reused_vectors':len(vectors),
                        'new_vectors':sum(len(d['records']) for d in batch['documents'])-len(vectors)}
                    with sqlite3.connect(migration.ledger,timeout=30) as conn:
                        conn.execute('INSERT INTO result VALUES(?,?,?,?) ON CONFLICT(job_id) DO UPDATE SET status=excluded.status,detail=excluded.detail,finished_at=excluded.finished_at',
                            (job,'complete',compact(detail),datetime.now(timezone.utc).isoformat()))
                        conn.executemany("UPDATE result SET status='superseded' WHERE job_id=?",[(j,) for j in old_jobs])
                        conn.execute('DELETE FROM result WHERE job_id=?',(stable_id('group-error',*identity),))
                processed+=1
                if processed%10==0:print(compact({'repaired_groups':processed,'total_groups':len(groups),'elapsed_seconds':round(time.monotonic()-started,1)}),flush=True)
            except Exception as exc:
                failures.append({'identity':list(identity),'error':type(exc).__name__,'message':str(exc)[:400]})
                print(compact(failures[-1]),flush=True)
        if limit is not None and processed>=limit:break
    value={'processed_groups':processed,'total_groups':len(groups),'failures':failures,'unresolved_reference_memberships':unresolved,
        'elapsed_seconds':round(time.monotonic()-started,1)}
    name='reference-repair-result.json' if reference_refresh else 'attachment-repair-result.json'
    (STATE/name).write_text(compact(value));print(compact(value),flush=True)
    return not failures

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--limit',type=int);parser.add_argument('--reference-refresh',action='store_true');args=parser.parse_args()
    raise SystemExit(0 if run(args.limit,reference_refresh=args.reference_refresh) else 1)
