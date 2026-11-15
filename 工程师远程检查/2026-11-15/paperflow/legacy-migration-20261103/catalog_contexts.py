import subprocess
program='''
import json,os
from article_repository.catalog import Catalog
cat=Catalog(os.environ['ARTICLE_DATABASE_URL'])
with cat.connect() as conn:
 rows=conn.execute("SELECT source_key,base->>'source_sha256' sha,base->>'folder_name' folder_name,base->'files' files FROM article").fetchall()
print(json.dumps(rows,ensure_ascii=False))
'''
result=subprocess.run(['docker','exec','-i','paperflow-article-repository','python','-'],input=program,capture_output=True,text=True,timeout=60)
if result.returncode:raise RuntimeError(result.stderr[-500:])
print(result.stdout)
