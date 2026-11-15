"""Bounded final coverage proof; no source writes or processing dispatch."""
import subprocess
program=r'''
import collections,json,os,sqlite3
from pathlib import Path
import httpx
from qdrant_client import QdrantClient,models
from haystack_integrations.document_stores.qdrant.converters import convert_id
from paperflow_qdrant.identity import stable_id
from article_repository.catalog import Catalog
state=Path('/state');source=json.loads(Path('/input/source-inventory.json').read_text())
majors=json.loads(Path('/input/majors.json').read_text())['majors']
datasets={m[k] for m in majors for k in ('paper_dataset_id','image_dataset_id')}
expected={d['id']:d for d in source['documents'] if d['kb_id'] in datasets}
with sqlite3.connect(state/'migration.sqlite') as conn:
 ledger=[{'job_id':j,'status':s,**json.loads(d)} for j,s,d in conn.execute('SELECT job_id,status,detail FROM result')]
complete=[r for r in ledger if r['status']=='complete'];failures=[r for r in ledger if r['status']=='failed']
covered={d for r in complete for d in r.get('source_document_ids',[])}
folders=sorted({r['folder_id'] for r in complete});counts=collections.Counter();actual_members=collections.defaultdict(set)
client=QdrantClient(url=os.environ['QDRANT_URL'],api_key=os.environ['QDRANT_API_KEY'],timeout=180)
for start in range(0,len(folders),128):
 part=folders[start:start+128]
 commits=client.retrieve('paperflow_v1',[convert_id(stable_id(f,'complete')) for f in part],with_vectors=False,
  with_payload=models.PayloadSelectorInclude(include=['meta.folder_id','meta.kind']))
 assert {p.payload['meta']['folder_id'] for p in commits}==set(part),'Completion missing'
 offset=None
 while True:
  points,offset=client.scroll('paperflow_v1',limit=256,offset=offset,with_vectors=False,
   scroll_filter=models.Filter(must=[models.FieldCondition(key='meta.kind',match=models.MatchValue(value='manifest')),
    models.FieldCondition(key='meta.folder_id',match=models.MatchAny(any=part))]),
   with_payload=models.PayloadSelectorInclude(include=['meta.folder_id','meta.active','meta.document.id','meta.document.dataset_ids',
    'meta.document.meta_fields.legacy_document_ids','meta.document.meta_fields.legacy_unindexed','meta.document.meta_fields.legacy_original_reconstructed','meta.document.meta_fields.paperflow_role']))
  for point in points:
   doc=point.payload['meta']['document'];fields=doc['meta_fields'];old=fields['legacy_document_ids']
   assert all(d in expected for d in old),'Non-official source leaked'
   assert set(doc['dataset_ids'])=={expected[d]['kb_id'] for d in old},'Source specialty membership changed'
   for d in old:actual_members[d].add(point.payload['meta']['folder_id'])
   counts['target_documents']+=1
   counts['active_target_documents']+=bool(point.payload['meta']['active'])
   counts['unindexed_target_documents']+=bool(fields.get('legacy_unindexed'))
   counts['reconstructed_full_text_documents']+=bool(fields.get('legacy_original_reconstructed'))
  if offset is None:break
 counts['target_chunks']+=client.count('paperflow_v1',exact=True,count_filter=models.Filter(must=[
  models.FieldCondition(key='meta.kind',match=models.MatchValue(value='chunk')),
  models.FieldCondition(key='meta.folder_id',match=models.MatchAny(any=part))])).count
group_chunks={}
for r in complete:
 key=tuple(sorted(r['source_document_ids']))
 if key in group_chunks:assert group_chunks[key]==r['source_chunks']
 group_chunks[key]=r['source_chunks']
http=httpx.Client(base_url=os.environ['LEGACY_ES_URL'],auth=('elastic',os.environ['LEGACY_ES_PASSWORD']),trust_env=False,timeout=120)
response=http.post('ragflow_'+source['tenant']+'/_count',json={'query':{'bool':{'filter':[{'terms':{'doc_id':list(expected)}},{'terms':{'kb_id':list(datasets)}}]}}})
response.raise_for_status();source_actual=response.json()['count']
cat=Catalog(os.environ['ARTICLE_DATABASE_URL'])
with cat.connect() as conn:
 entries=conn.execute("SELECT coalesce(base->>'entry_kind','article') kind,coalesce(base->>'source','') source,count(*) n FROM article GROUP BY kind,source ORDER BY kind,source").fetchall()
 files=conn.execute("SELECT coalesce(a.base->>'entry_kind','article') kind,coalesce(f->>'availability','unknown') availability,count(*) n FROM article a CROSS JOIN LATERAL jsonb_array_elements(a.base->'files') f GROUP BY kind,availability ORDER BY kind,availability").fetchall()
 unknown=conn.execute("SELECT count(*) n FROM article WHERE source_key LIKE 'legacy-unresolved:%%' AND coalesce(base->>'entry_kind','article')='article'").fetchone()['n']
 empty=conn.execute("SELECT count(*) n FROM article WHERE jsonb_array_length(base->'files')=0 AND coalesce(base->>'entry_kind','article')='article'").fetchone()['n']
result={'official_libraries':len({d['kb_id'] for d in expected.values()}),'official_source_documents':len(expected),
 'excluded_test_or_other_documents':len(source['documents'])-len(expected),'completed_folder_publications':len(folders),
 'source_document_coverage':len(covered),'target_source_document_coverage':len(actual_members),
 'missing_source_documents':sorted(set(expected)-covered),'extra_source_documents':sorted(covered-set(expected)),
 'source_actual_chunks':source_actual,'source_chunks_from_unique_groups':sum(group_chunks.values()),
 'target_chunks_expected':sum(r['target_chunks'] for r in complete),'counts':dict(counts),
 'failures':failures,'catalog_entries':entries,'catalog_files':files,'article_source_folder_unknown':unknown,
 'article_no_linked_files':empty,'state_counts':dict(collections.Counter(r['status'] for r in ledger))}
assert not result['missing_source_documents'] and not result['extra_source_documents'],'Source coverage incomplete'
assert set(actual_members)==set(expected),'Target manifest coverage incomplete'
assert source_actual==result['source_chunks_from_unique_groups'],'Source chunk coverage differs'
assert counts['target_chunks']==result['target_chunks_expected'],'Target chunks differ'
assert not failures,'Migration still has failures'
(state/'final-coverage.json').write_text(json.dumps(result,ensure_ascii=False,indent=2));print(json.dumps(result,ensure_ascii=False))
cat.source_status('旧库迁移',{'status':'complete','official_documents':len(expected),'source_chunks':source_actual,
 'completed_publications':len(folders),'target_chunks_retained':counts['target_chunks'],'failed':0,
 'scope':'仅正式资料；覆盖包含留存历史版本；原件与文件夹未知信息单独保留'})
'''
result=subprocess.run(['docker','run','--rm','-i','--network','host','--env-file','/opt/paperflow-legacy-migration-20261103/.migration.env',
 '-v','/opt/paperflow-legacy-migration-20261103/input:/input:ro','-v','/opt/paperflow-legacy-migration-20261103/state:/state',
 '--entrypoint','python','paperflow-legacy-migration:20261103','-'],input=program,capture_output=True,text=True,timeout=600)
if result.returncode:print(result.stdout);raise RuntimeError(result.stderr[-3500:])
print(result.stdout)
