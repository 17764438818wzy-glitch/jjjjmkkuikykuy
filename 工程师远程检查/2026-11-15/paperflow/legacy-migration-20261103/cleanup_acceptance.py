"""Remove only this migration's isolated acceptance resources and redundant temporary input archive."""
import json,shutil,subprocess
from pathlib import Path
root=Path('/opt/paperflow-legacy-migration-20261103').resolve()
assert str(root)=='/opt/paperflow-legacy-migration-20261103'
for name in ('paperflow-legacy-migration-acceptance','paperflow-legacy-migration-20261103','paperflow-legacy-attachment-repair-20261103','paperflow-legacy-reference-repair-20261103'):
 result=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
 if not result.returncode:assert not json.loads(result.stdout)[0]['State']['Running'],'Migration still running'
program=r'''
import json,os
import psycopg
from psycopg import sql
from qdrant_client import QdrantClient
name='legacy_migration_acceptance_20261103'
client=QdrantClient(url=os.environ['QDRANT_URL'],api_key=os.environ['QDRANT_API_KEY'],timeout=120)
if client.collection_exists(name):client.delete_collection(name)
with psycopg.connect(os.environ['ARTICLE_DATABASE_URL']) as conn:
 conn.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(name)))
print(json.dumps({'isolated_collection_removed':name,'isolated_schema_removed':name}))
'''
result=subprocess.run(['docker','run','--rm','-i','--network','host','--env-file',str(root/'.migration.env'),
 '--entrypoint','python','paperflow-legacy-migration:20261103','-'],input=program,capture_output=True,text=True,timeout=180)
assert result.returncode==0,result.stderr[-2000:]
print(result.stdout)
subprocess.run(['docker','rm','paperflow-legacy-migration-acceptance'],capture_output=True,check=True)
temporary=root/'state/acceptance-objects'
assert temporary.resolve().parent==root/'state' and not temporary.is_symlink()
if temporary.exists():shutil.rmtree(temporary)
archive=root/'inputs.tar.gz'
assert archive.resolve().parent==root
archive.unlink(missing_ok=True)
objects=Path('/opt/paperflow-qdrant-20261103/objects').resolve();removed=0
for path in objects.glob('.legacy-migration-*'):
 if path.is_file() and not path.is_symlink():path.unlink();removed+=1
print(json.dumps({'redundant_upload_archive_removed':True,'owned_partial_object_files_removed':removed,
 'retained':'source storage, originals, production objects, verified inputs, ledger and final reports'}))
