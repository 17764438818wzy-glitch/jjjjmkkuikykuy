"""Link only exact original hashes already recorded on available NAS files."""
import subprocess
program=r'''
import hashlib,json,os
from article_repository.catalog import Catalog
from article_repository.files import Files
from psycopg.types.json import Jsonb
cat=Catalog(os.environ['ARTICLE_DATABASE_URL'])
files=Files(os.environ['ARTICLE_NAS_ROOT'],username=os.environ['ARTICLE_NAS_USER'],password=os.environ['ARTICLE_NAS_PASSWORD'])
with cat.connect() as conn:
 rows=conn.execute("SELECT id,base FROM article WHERE jsonb_array_length(base->'files')=0 AND coalesce(base->>'entry_kind','article')='article'").fetchall()
 known=conn.execute("SELECT f FROM article a CROSS JOIN LATERAL jsonb_array_elements(a.base->'files') f WHERE f->>'availability'='available'").fetchall()
by_hash={r['f'].get('sha256'):r['f'] for r in known if r['f'].get('sha256')}
linked=[];unresolved=[]
for row in rows:
 sha=row['base'].get('source_sha256');candidate=by_hash.get(sha)
 if not candidate:unresolved.append({'id':row['id'],'source_sha256':sha,'title':row['base'].get('title')});continue
 with files.open(files.path(candidate),'rb') as source:assert hashlib.file_digest(source,'sha256').hexdigest()==sha
 with cat.connect() as conn:
  current=conn.execute('SELECT base FROM article WHERE id=%s FOR UPDATE',(row['id'],)).fetchone()['base']
  assert current.get('source_sha256')==sha
  current['files']=cat._files(current.get('files',[]),[candidate],True)
  conn.execute('UPDATE article SET base=%s WHERE id=%s',(Jsonb(current),row['id']))
 linked.append(row['id'])
print(json.dumps({'linked':linked,'unresolved':unresolved},ensure_ascii=False))
'''
result=subprocess.run(['docker','exec','-i','paperflow-article-repository','python','-'],input=program,capture_output=True,text=True,timeout=180)
assert result.returncode==0,result.stderr[-2000:]
print(result.stdout)
