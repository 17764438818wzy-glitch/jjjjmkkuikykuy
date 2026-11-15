import subprocess
program=r'''
import json,os,sqlite3
from pathlib import Path
from article_repository.catalog import Catalog
cat=Catalog(os.environ['ARTICLE_DATABASE_URL'])
with cat.connect() as conn:
 rows=conn.execute("SELECT id,source_key,base->'profile' profile,base->'qdrant' qdrant FROM article WHERE source_key LIKE 'folder:%%' AND base->>'entry_kind'='attachment'").fetchall()
with sqlite3.connect('/state/migration.sqlite') as conn:
 fixes=[json.loads(r[0]) for r in conn.execute("SELECT detail FROM result WHERE status='complete'") if json.loads(r[0]).get('entry_kind')=='attachment']
print(json.dumps({'physical_folder_attachment_entries':rows,'completed_attachment_corrections':len(fixes)},ensure_ascii=False))
'''
result=subprocess.run(['docker','run','--rm','-i','--network','host','--env-file','/opt/paperflow-legacy-migration-20261103/.migration.env',
 '-v','/opt/paperflow-legacy-migration-20261103/state:/state','--entrypoint','python','paperflow-legacy-migration:20261103','-'],input=program,capture_output=True,text=True,timeout=120)
assert result.returncode==0,result.stderr[-2000:]
print(result.stdout)
