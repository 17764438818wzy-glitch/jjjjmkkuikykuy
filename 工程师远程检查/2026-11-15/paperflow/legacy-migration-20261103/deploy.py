"""Deploy only the owned migration runner. Credentials are passed by a private env file."""
import json,os
from pathlib import Path
import sys
import paramiko

ROOT=Path(__file__).parent;REMOTE='/opt/paperflow-legacy-migration-20261103'
phase=sys.argv[1]
client=paramiko.SSHClient();client.load_system_host_keys()
client.connect('192.168.1.120',username='wzy',password=os.environ['RAG_DIAG_SSH_PASSWORD'],look_for_keys=False,allow_agent=False,timeout=15)
def run(program,timeout=900):
    stdin,stdout,stderr=client.exec_command("sudo -S -p '' python3 -u -",timeout=timeout)
    stdin.write(os.environ['RAG_DIAG_SSH_PASSWORD']+'\n'+program);stdin.flush();stdin.channel.shutdown_write()
    for line in stdout:print(line,end='',flush=True)
    error=stderr.read().decode('utf-8','replace');code=stdout.channel.recv_exit_status()
    if error:print(error[-2500:])
    if code:raise RuntimeError('Migration deployment stage failed')

if phase in ('build','upload'):
    run("import subprocess\nsubprocess.run(['install','-d','-m','750','-o','wzy','-g','wzy',"+repr(REMOTE)+"],check=True)")
    sftp=client.open_sftp()
    if phase=='build':
        for name in ('Dockerfile','.dockerignore','legacy_migration.py','migrate.py','nas_transfer.py','attachment_notes.py','repair_attachments.py'):
            sftp.put(str(ROOT/name),REMOTE+'/'+name)
        library=ROOT.parent/'qdrant-migration/paperflow_qdrant/library.py'
        if not library.exists():library=ROOT/'library.py'
        sftp.put(str(library),REMOTE+'/library.py')
        for name in ('catalog.py','sync.py'):
            source=ROOT.parent/'article-repository/article_repository'/name
            if not source.exists():source=ROOT/name
            sftp.put(str(source),REMOTE+'/'+name)
        for name in ('context-overrides.json','native-supplements.json','fallback-contexts.json'):
            if (ROOT/name).exists():sftp.put(str(ROOT/name),REMOTE+'/'+name)
    else:
        archive=ROOT/'inputs.tar.gz';last=[-1]
        def progress(done,total):
            percent=done*10//total
            if percent!=last[0]:last[0]=percent;print(json.dumps({'upload_percent':percent*10}),flush=True)
        sftp.put(str(archive),REMOTE+'/inputs.tar.gz',callback=progress)
    sftp.close()
    if phase=='build':
        run('import json,subprocess,os\nfrom pathlib import Path\nroot=Path(\'/opt/paperflow-legacy-migration-20261103\')\nimport shutil\nfor name in (\'context-overrides.json\',\'native-supplements.json\',\'fallback-contexts.json\'):\n if (root/name).exists():shutil.copyfile(root/name,root/\'input\'/name)\ndef inspect(name):return json.loads(subprocess.check_output([\'docker\',\'inspect\',name]))[0]\ndef env(name):return dict(v.split(\'=\',1) for v in inspect(name)[\'Config\'][\'Env\'] if \'=\' in v)\narticle=env(\'paperflow-article-repository\');es=env(\'ragflow-recovery-es-20260923\');minio=env(\'ragflow-server-minio-1\')\npg=next(v[\'IPAddress\'] for v in inspect(\'paperflow-article-postgres\')[\'NetworkSettings\'][\'Networks\'].values() if v.get(\'IPAddress\'))\nvalues={key:article[key] for key in (\'ARTICLE_DATABASE_URL\',\'QDRANT_API_KEY\',\'PAPERFLOW_RAG_API_KEY\',\'ARTICLE_NAS_ROOT\',\'ARTICLE_NAS_USER\',\'ARTICLE_NAS_PASSWORD\')}\nvalues[\'ARTICLE_DATABASE_URL\']=values[\'ARTICLE_DATABASE_URL\'].replace(\'@article-postgres/\',\'@\'+pg+\'/\')\nvalues.update(QDRANT_URL=\'http://127.0.0.1:6333\',LEGACY_ES_URL=\'http://172.23.0.2:9200\',LEGACY_ES_PASSWORD=es[\'ELASTIC_PASSWORD\'],\n LEGACY_MINIO_ENDPOINT=\'172.30.88.3:9000\',LEGACY_MINIO_USER=minio.get(\'MINIO_ROOT_USER\') or minio[\'MINIO_ACCESS_KEY\'],\n LEGACY_MINIO_PASSWORD="<REDACTED_SECRET>"\npath=root/\'.migration.env\';path.write_text(\'\'.join(k+\'=\'+v+\'\\n\' for k,v in values.items()));path.chmod(0o600)\n(root/\'state\').mkdir(exist_ok=True)\nresult=subprocess.run([\'docker\',\'build\',\'-t\',\'paperflow-legacy-migration:20261103\',str(root)],capture_output=True,text=True,timeout=600)\nprint(result.stdout[-5000:]);print(result.stderr[-3000:])\nassert result.returncode==0,\'Migration image build failed\'\nprint(\'Owned migration image and private environment ready\')\n')
    else:
        run("""import json,tarfile,shutil
from pathlib import Path
root=Path('/opt/paperflow-legacy-migration-20261103');destination=root/'input';destination.mkdir(exist_ok=True)
with tarfile.open(root/'inputs.tar.gz','r:gz') as archive:
 for item in archive:
  path=(destination/item.name).resolve()
  assert path.is_relative_to(destination.resolve()) and not item.issym() and not item.islnk()
  if item.isdir():path.mkdir(parents=True,exist_ok=True)
  elif item.isfile():
   path.parent.mkdir(parents=True,exist_ok=True)
   with archive.extractfile(item) as source,path.open('wb') as target:shutil.copyfileobj(source,target,1024*1024)
  else:raise RuntimeError('Unexpected migration archive member')
plans=json.loads((destination/'package-index.json').read_text())
print(json.dumps({'input_packages':len(plans['plans']),'source_stats':plans['stats']}))
""",timeout=900)
elif phase in ('acceptance','first','migrate','attachment-first','attachments','references'):
    run("""import json,subprocess
from pathlib import Path
root=Path('/opt/paperflow-legacy-migration-20261103')
phase="""+repr(phase)+"""
name='paperflow-legacy-migration-acceptance' if phase=='acceptance' else 'paperflow-legacy-reference-repair-20261103' if phase=='references' else 'paperflow-legacy-attachment-repair-20261103' if phase.startswith('attachment') else 'paperflow-legacy-migration-20261103'
existing=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
if existing.returncode==0:
 info=json.loads(existing.stdout)[0]
 assert not info['State']['Running'],'Owned migration runner is already running'
 subprocess.run(['docker','rm',name],check=True,capture_output=True)
command=['docker','run','-d','--name',name,'--network','host','--env-file',str(root/'.migration.env'),
 '-v',str(root/'input')+':/input:ro','-v',str(root/'state')+':/state']
if phase!='acceptance':command+=['-v','/opt/paperflow-qdrant-20261103/objects:/objects']
if phase.startswith('attachment') or phase=='references':
 command+=['--entrypoint','python','paperflow-legacy-migration:20261103','/app/migration/repair_attachments.py']
else:command+=['paperflow-legacy-migration:20261103','acceptance' if phase=='acceptance' else 'migrate']
if phase=='first':command+=['--limit','1']
if phase=='attachment-first':command+=['--limit','1']
if phase=='references':command+=['--reference-refresh']
if phase=='migrate':command+=['--workers','8','--skip-attachment-cards']
result=subprocess.run(command,capture_output=True,text=True,timeout=60)
assert result.returncode==0,result.stderr
print(json.dumps({'container':name,'id':result.stdout.strip(),'phase':phase}))
""")
elif phase=='status':
    run("""import json,subprocess
from pathlib import Path
root=Path('/opt/paperflow-legacy-migration-20261103')
for name in ('paperflow-legacy-migration-acceptance','paperflow-legacy-migration-20261103','paperflow-legacy-attachment-repair-20261103','paperflow-legacy-reference-repair-20261103'):
 value=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
 if value.returncode:continue
 info=json.loads(value.stdout)[0]
 log=subprocess.run(['docker','logs','--tail','12',name],capture_output=True,text=True)
 print(json.dumps({'container':name,'state':info['State']['Status'],'exit':info['State']['ExitCode'],
  'stdout':log.stdout[-10000:],'stderr':log.stderr[-3500:]}))
for name in ('acceptance-result.json','progress.json','migration-result.json','attachment-repair-result.json','reference-repair-result.json'):
 path=root/'state'/name
 if path.exists():print(json.dumps({'file':name,'value':json.loads(path.read_text())}))
""")
else:raise ValueError('Unknown deployment phase')
client.close()
