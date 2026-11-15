import collections,json,sqlite3,subprocess
from pathlib import Path
root=Path('/opt/paperflow-legacy-migration-20261103')
name='paperflow-legacy-migration-20261103'
value=json.loads(subprocess.check_output(['docker','inspect',name]))[0]
print(json.dumps({'runner':value['State']['Status'],'exit':value['State']['ExitCode']}))
latest=root/'state'/('progress.json' if value['State']['Running'] else 'migration-result.json')
if latest.exists():
 value=json.loads(latest.read_text());value.pop('errors',None);print(json.dumps(value))
with sqlite3.connect(root/'state/migration.sqlite') as conn:
 failures=[json.loads(r[0]) for r in conn.execute("SELECT detail FROM result WHERE status='failed'")]
print(json.dumps({'failures_by_type':dict(collections.Counter(x.get('error') for x in failures)),
 'other_failures':[v for v in failures if v.get('error')!='SMBOSError'][:4]}))
name='paperflow-legacy-attachment-repair-20261103'
value=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
if not value.returncode:
 info=json.loads(value.stdout)[0];logs=subprocess.run(['docker','logs','--tail','3',name],capture_output=True,text=True)
 print(json.dumps({'attachment_runner':info['State']['Status'],'exit':info['State']['ExitCode'],'logs':logs.stdout[-2000:]}))
result=subprocess.run(['docker','stats','--no-stream','--format','{{.Name}} {{.CPUPerc}} {{.MemUsage}}','paperflow-legacy-migration-20261103','paperflow-qdrant'],capture_output=True,text=True,timeout=15)
print(result.stdout)
