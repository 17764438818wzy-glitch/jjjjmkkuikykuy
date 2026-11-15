"""Retire only generated unresolved duplicates covered by an exact completed source set."""
import subprocess
program=r'''
import collections,json,os,sqlite3
from pathlib import Path
from article_repository.catalog import Catalog
from psycopg.types.json import Jsonb
from qdrant_client import QdrantClient,models
from haystack_integrations.document_stores.qdrant.converters import convert_id
from paperflow_qdrant.identity import stable_id
ledger=Path('/state/migration.sqlite');cat=Catalog(os.environ['ARTICLE_DATABASE_URL'])
client=QdrantClient(url=os.environ['QDRANT_URL'],api_key=os.environ['QDRANT_API_KEY'],timeout=120)
with sqlite3.connect(ledger) as conn:
 rows=[{'job_id':j,'status':s,**json.loads(d)} for j,s,d in conn.execute('SELECT job_id,status,detail FROM result')]
by_set=collections.defaultdict(list)
for r in rows:
 if r['status']=='complete' and r.get('source_document_ids'):by_set[tuple(sorted(r['source_document_ids']))].append(r)
with cat.connect() as conn:
 unknown=conn.execute("SELECT * FROM article WHERE source_key LIKE 'legacy-unresolved:%%'").fetchall()
changed=[];manual=[]
for old in unknown:
 owned=[r for r in rows if r.get('article_id')==old['id'] and r.get('folder_id')==old['base'].get('qdrant',{}).get('source_folder_id')]
 if len(owned)!=1:continue
 previous=owned[0];ids=tuple(sorted(previous.get('source_document_ids',[])))
 targets=[]
 for r in by_set.get(ids,[]):
  if r['article_id']==old['id']:continue
  target=cat.raw(r['article_id'])
  if target['source_key'].startswith('folder:') and target['base'].get('entry_kind')!='superseded':
   commit=client.retrieve('paperflow_v1',[convert_id(stable_id(r['folder_id'],'complete'))],with_vectors=False,with_payload=False)
   assert commit,'Replacement is not committed'
   targets.append(target)
 if not targets:continue
 if old['overrides']:manual.append(old['id']);continue
 targets={r['id']:r for r in targets}
 with cat.connect() as conn:
  current=conn.execute('SELECT * FROM article WHERE id=%s FOR UPDATE',(old['id'],)).fetchone()
  assert current['base']==old['base'] and not current['overrides'],'Source changed during reconciliation'
  for aid in sorted(targets):
   base=conn.execute('SELECT base FROM article WHERE id=%s FOR UPDATE',(aid,)).fetchone()['base']
   base['files']=cat._files(base.get('files',[]),old['base'].get('files',[]),True)
   conn.execute('UPDATE article SET base=%s WHERE id=%s',(Jsonb(base),aid))
  base=current['base'];base.update(entry_kind='superseded',superseded_by=sorted(targets))
  conn.execute('UPDATE article SET base=%s WHERE id=%s',(Jsonb(base),old['id']))
 folder=previous['folder_id']
 client.set_payload('paperflow_v1',payload={'active':False},key='meta',wait=True,points=models.Filter(must=[
  models.FieldCondition(key='meta.folder_id',match=models.MatchValue(value=folder))]))
 with sqlite3.connect(ledger,timeout=30) as conn:
  conn.execute("UPDATE result SET status='superseded' WHERE job_id=?",(previous['job_id'],))
 changed.append({'old_article_id':old['id'],'target_article_ids':sorted(targets),'source_documents':len(ids)})
recovered=[]
for old in rows:
 identity=old.get('identity',[])
 if old['status']!='failed' or len(identity)!=3 or identity[0]!='legacy-document':continue
 targets=by_set.get((identity[1],),[])
 if not targets:continue
 for target in targets:
  assert client.retrieve('paperflow_v1',[convert_id(stable_id(target['folder_id'],'complete'))],with_vectors=False,with_payload=False)
 client.set_payload('paperflow_v1',payload={'active':False},key='meta',wait=True,points=models.Filter(must=[
  models.FieldCondition(key='meta.folder_id',match=models.MatchValue(value=old['job_id']))]))
 with sqlite3.connect(ledger,timeout=30) as conn:conn.execute("UPDATE result SET status='recovered' WHERE job_id=?",(old['job_id'],))
 recovered.append({'old_job_id':old['job_id'],'completed_replacement_jobs':[t['job_id'] for t in targets]})
value={'reconciled_unknown_entries':len(changed),'manual_entries_preserved':manual,'changes':changed,'recovered_failures':recovered}
Path('/state/source-reconciliation.json').write_text(json.dumps(value,ensure_ascii=False,indent=2));print(json.dumps(value,ensure_ascii=False))
'''
result=subprocess.run(['docker','run','--rm','-i','--network','host','--env-file','/opt/paperflow-legacy-migration-20261103/.migration.env',
 '-v','/opt/paperflow-legacy-migration-20261103/state:/state','--entrypoint','python','paperflow-legacy-migration:20261103','-'],
 input=program,capture_output=True,text=True,timeout=300)
assert result.returncode==0,result.stderr[-3000:]
print(result.stdout)
