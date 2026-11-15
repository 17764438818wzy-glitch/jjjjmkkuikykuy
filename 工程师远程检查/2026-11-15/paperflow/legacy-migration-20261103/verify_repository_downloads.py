"""Read-only warehouse user path: search, original, attachment, and single-paper ZIP."""
import subprocess
program=r'''
import hashlib,io,json,os,zipfile
from pathlib import Path
import httpx
from article_repository.catalog import Catalog
cat=Catalog(os.environ['ARTICLE_DATABASE_URL'])
with cat.connect() as conn:
 sample=conn.execute("SELECT id FROM article WHERE coalesce(base->>'entry_kind','article')='article' AND jsonb_array_length(base->'files')>1 AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(base->'files') f WHERE f->>'availability'!='available' OR coalesce((f->>'bytes')::bigint,999999999)>10000000) ORDER BY id LIMIT 1").fetchone()
assert sample,'No suitable multi-file article to test'
checks=[]
with httpx.Client(base_url='http://192.168.1.120:8098',trust_env=False,timeout=120) as client:
 response=client.get('/api/library/articles',params={'keyword':'数字金融对企业创新','page_size':10});response.raise_for_status()
 assert response.json()['total']==1 and response.json()['rows'][0]['migration']=='complete'
 for aid in ['0f490d65e5ea96fe2b6d77e6f45507de',sample['id']]:
  response=client.get('/api/library/articles/'+aid);response.raise_for_status();article=response.json()
  available=[f for f in article['files'] if f['availability']=='available'];assert available
  expected=set()
  for file in available:
   raw=client.get(file['url']);raw.raise_for_status()
   assert hashlib.sha256(raw.content).hexdigest()==file['sha256'];expected.add(file['sha256'])
  zipped=client.get('/api/library/articles/'+aid+'/bundle');zipped.raise_for_status()
  with zipfile.ZipFile(io.BytesIO(zipped.content)) as archive:
   assert archive.testzip() is None
   actual={hashlib.sha256(archive.read(name)).hexdigest() for name in archive.namelist() if name!='资料完整性.txt'}
   assert actual==expected
  checks.append({'article_id':aid,'files_checked':len(available),'zip_files_match':True})
value={'search_verified':True,'checks':checks}
Path('/tmp/article-repository-download-validation.json').write_text(json.dumps(value));print(json.dumps(value))
'''
result=subprocess.run(['docker','exec','-i','paperflow-article-repository','python','-'],input=program,capture_output=True,text=True,timeout=240)
assert result.returncode==0,result.stderr[-2500:]
print(result.stdout)
