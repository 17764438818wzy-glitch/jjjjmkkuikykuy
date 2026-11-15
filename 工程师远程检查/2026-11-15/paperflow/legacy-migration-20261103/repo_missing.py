import subprocess
program=r'''
import json,os
from article_repository.catalog import Catalog
cat=Catalog(os.environ['ARTICLE_DATABASE_URL'])
with cat.connect() as conn:
 rows=conn.execute("SELECT id,source_key,base FROM article WHERE jsonb_array_length(base->'files')=0 OR EXISTS(SELECT 1 FROM jsonb_array_elements(base->'files') f WHERE coalesce(f->>'availability','unknown')!='available')").fetchall()
 print(json.dumps({'articles':rows},ensure_ascii=False))
'''
result=subprocess.run(['docker','exec','-i','paperflow-article-repository','python','-'],input=program,capture_output=True,text=True,timeout=60)
if result.returncode:raise RuntimeError(result.stderr[-500:])
print(result.stdout)
