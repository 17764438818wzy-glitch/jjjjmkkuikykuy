import subprocess
program='''
import hashlib,json,os,sqlite3,time
from pathlib import Path
import httpx
from qdrant_client import QdrantClient,models
from article_repository.catalog import Catalog
state=Path('/state');cat=Catalog(os.environ['ARTICLE_DATABASE_URL'])
with sqlite3.connect(state/'migration.sqlite') as conn:
 results=[json.loads(r[0]) for r in conn.execute("SELECT detail FROM result WHERE status='complete' LIMIT 60")]
folders=[]
for value in results:
 row=cat.raw(value['article_id'])
 if row['base'].get('qdrant',{}).get('source_folder_id')==value['folder_id']:
  folders.append(value['folder_id'])
 if len(folders)==8:break
client=QdrantClient(url=os.environ['QDRANT_URL'],api_key=os.environ['QDRANT_API_KEY'],timeout=120)
selector=models.Filter(must=[models.FieldCondition(key='meta.kind',match=models.MatchValue(value='manifest')),
 models.FieldCondition(key='meta.active',match=models.MatchValue(value=True)),models.FieldCondition(key='meta.folder_id',match=models.MatchAny(any=folders))])
points=client.scroll('paperflow_v1',scroll_filter=selector,limit=100,with_vectors=False)[0]
checks=[];images=0;attachments=0
with httpx.Client(base_url='http://192.168.1.120:8097',trust_env=False,timeout=120,
 headers={'Authorization':'Bearer '+os.environ['PAPERFLOW_RAG_API_KEY']}) as http:
 for point in points:
  doc=point.payload['meta']['document'];ds=doc['dataset_ids'][0];start=time.monotonic()
  response=http.get('/api/v1/datasets/'+ds+'/documents',params={'id':doc['id']});response.raise_for_status()
  observed=response.json()['data']['docs'];assert len(observed)==1
  chunks=http.get('/api/v1/datasets/'+ds+'/documents/'+doc['id']+'/chunks',params={'page_size':1000});chunks.raise_for_status()
  assert chunks.json()['data']['total']==len(doc['records'])
  original=http.get('/api/v1/datasets/'+ds+'/documents/'+doc['id']);original.raise_for_status()
  assert original.content==doc['original'].encode()
  if doc['meta_fields'].get('two_pass_profile_json'):
   owner=doc.get('dataset_meta_fields',{}).get(ds,{})
   assert observed[0]['meta_fields']['two_pass_profile_json']==owner.get('two_pass_profile_json',doc['meta_fields']['two_pass_profile_json'])
  record=next((r for r in doc['records'] if r.get('image_id')),None)
  if record:
   response=http.get('/api/v1/documents/images/'+record['image_id']);response.raise_for_status()
   assert hashlib.sha256(response.content).hexdigest()==record['image_id'];images+=1
  meta=observed[0]['meta_fields']
  if str(meta.get('paperflow_role','')).startswith('attachment') and meta.get('original_file_id'):
   response=http.get(meta['original_download_url']);response.raise_for_status()
   assert hashlib.sha256(response.content).hexdigest()==meta['original_file_id'];attachments+=1
  checks.append({'document_id':doc['id'],'dataset':ds,'chunks':len(doc['records']),'seconds':round(time.monotonic()-start,3)})
result={'sample_folders':len(folders),'documents_checked':len(checks),'images_downloaded':images,'attachments_downloaded':attachments,'checks':checks}
assert result['documents_checked']>=8
(state/'endpoint-validation.json').write_text(json.dumps(result,ensure_ascii=False))
print(json.dumps(result))
'''
command=['docker','run','--rm','-i','--network','host','--env-file','/opt/paperflow-legacy-migration-20261103/.migration.env',
 '-v','/opt/paperflow-legacy-migration-20261103/state:/state','--entrypoint','python','paperflow-legacy-migration:20261103','-']
result=subprocess.run(command,input=program,capture_output=True,text=True,timeout=240)
assert result.returncode==0,result.stderr[-2000:]
print(result.stdout)
