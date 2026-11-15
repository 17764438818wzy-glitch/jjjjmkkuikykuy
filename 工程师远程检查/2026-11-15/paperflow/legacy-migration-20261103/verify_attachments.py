"""Read back corrected attachment identities through the deployed native API."""
import subprocess
program=r'''
import hashlib,json,os,sqlite3
from pathlib import Path
import httpx
from qdrant_client import QdrantClient,models
state=Path('/state')
with sqlite3.connect(state/'migration.sqlite') as conn:
 rows=[json.loads(r[0]) for r in conn.execute("SELECT detail FROM result WHERE status='complete'")]
folders=[r['folder_id'] for r in rows if r.get('entry_kind') in ('article','attachment')]
client=QdrantClient(url=os.environ['QDRANT_URL'],api_key=os.environ['QDRANT_API_KEY'],timeout=120)
points=client.scroll('paperflow_v1',limit=256,with_vectors=False,scroll_filter=models.Filter(must=[
 models.FieldCondition(key='meta.kind',match=models.MatchValue(value='manifest')),
 models.FieldCondition(key='meta.active',match=models.MatchValue(value=True)),
 models.FieldCondition(key='meta.folder_id',match=models.MatchAny(any=folders))]))[0]
checks=[];linked=0;downloaded={}
with httpx.Client(trust_env=False,timeout=120,headers={'Authorization':'Bearer '+os.environ['PAPERFLOW_RAG_API_KEY']}) as http:
 for point in points:
  doc=point.payload['meta']['document'];ds=doc['dataset_ids'][0]
  if not str(doc['meta_fields'].get('paperflow_role','')).startswith('attachment'):continue
  root='http://192.168.1.120:8097/api/v1/datasets/'+ds+'/documents'
  response=http.get(root,params={'id':doc['id']});response.raise_for_status()
  observed=response.json()['data']['docs'];assert len(observed)==1
  meta=observed[0]['meta_fields'];sha=meta['original_file_id']
  assert len(sha)==64 and ':8088' not in meta['original_download_url']
  if sha not in downloaded:
   raw=http.get(meta['original_download_url']);raw.raise_for_status()
   assert hashlib.sha256(raw.content).hexdigest()==sha
   downloaded[sha]=len(raw.content)
  original=http.get(root+'/'+doc['id']);original.raise_for_status();assert original.content==doc['original'].encode()
  assert '/api/v1/files/' not in original.text
  for dataset in doc['dataset_ids']:
   fields={**doc['meta_fields'],**doc['dataset_meta_fields'][dataset]}
   assert not fields.get('primary_document_status')=='unresolved_legacy_reference'
   if fields.get('primary_document_id'):
    primary=http.get('http://192.168.1.120:8097/api/v1/datasets/'+fields['primary_dataset_id']+'/documents',params={'id':fields['primary_document_id']})
    primary.raise_for_status();assert len(primary.json()['data']['docs'])==1,{'attachment':doc['id'],'dataset':dataset,'primary_dataset':fields['primary_dataset_id'],'primary':fields['primary_document_id'],'source':fields['legacy_document_ids']};linked+=1
  checks.append({'id':doc['id'],'original_sha256':sha,'bytes':downloaded[sha],'primary_readable':bool(meta.get('primary_document_id'))})
assert checks,'No completed corrected attachment to verify'
result={'attachments_checked':len(checks),'unique_originals_downloaded':len(downloaded),'primary_references_checked':linked,'checks':checks}
(state/'attachment-endpoint-validation.json').write_text(json.dumps(result));print(json.dumps({k:v for k,v in result.items() if k!='checks'}))
'''
result=subprocess.run(['docker','run','--rm','-i','--network','host','--env-file','/opt/paperflow-legacy-migration-20261103/.migration.env',
 '-v','/opt/paperflow-legacy-migration-20261103/state:/state','--entrypoint','python','paperflow-legacy-migration:20261103','-'],
 input=program,capture_output=True,text=True,timeout=600)
assert result.returncode==0,result.stderr[-3000:]
print(result.stdout)
