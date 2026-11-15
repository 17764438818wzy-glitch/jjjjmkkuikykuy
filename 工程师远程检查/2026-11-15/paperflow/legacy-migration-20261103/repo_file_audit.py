import subprocess
program=r'''
import json,os
from article_repository.catalog import Catalog
from article_repository.files import Files
cat=Catalog(os.environ['ARTICLE_DATABASE_URL'])
with cat.connect() as conn:
 rows=conn.execute("SELECT coalesce(f->>'availability','unknown') state,count(*) FROM article a CROSS JOIN LATERAL jsonb_array_elements(a.base->'files') f GROUP BY state").fetchall()
 empty=conn.execute("SELECT count(*) n FROM article WHERE jsonb_array_length(base->'files')=0").fetchone()
 selected=conn.execute("SELECT id,source_key,base FROM article WHERE strpos(title,'数字金融对企业创新')>0").fetchall()
 print(json.dumps({'file_states':rows,'empty_articles':empty},ensure_ascii=False))
 files=Files(os.environ['ARTICLE_NAS_ROOT'],username=os.environ['ARTICLE_NAS_USER'],password=os.environ['ARTICLE_NAS_PASSWORD'])
 for row in selected:
  base=row['base']
  print(json.dumps({'id':row['id'],'source_key':row['source_key'],'asset_ids':base.get('legacy_asset_ids'),
    'source_sha256':base.get('source_sha256'),'files':files.observe(base.get('files',[]))},ensure_ascii=False))
'''
result=subprocess.run(['docker','exec','-i','paperflow-article-repository','python','-'],input=program,capture_output=True,text=True,timeout=120)
print(result.stdout)
if result.returncode:print(result.stderr[-2000:]);raise RuntimeError('Repository file audit failed')
