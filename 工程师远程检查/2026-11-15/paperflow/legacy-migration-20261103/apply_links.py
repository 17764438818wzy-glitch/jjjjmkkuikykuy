import json,os,sys
from pathlib import Path
import paramiko
ROOT=Path(__file__).parent
plan=(ROOT/(sys.argv[1] if len(sys.argv)>1 else 'file-link-plan.json')).resolve()
assert plan.parent==ROOT.resolve()
patches=json.loads(plan.read_text())['patches']
program='''
import json,os
from psycopg.types.json import Jsonb
from article_repository.catalog import Catalog
cat=Catalog(os.environ['ARTICLE_DATABASE_URL']);patches=PATCHES
changed=0
for patch in patches:
 with cat.connect() as conn:
  row=conn.execute('SELECT source_key,base FROM article WHERE id=%s FOR UPDATE',(patch['article_id'],)).fetchone()
  assert row and row['source_key']==patch['source_key']
  base=row['base'];base['files']=cat._files(base.get('files',[]),patch['files'],False)
  if patch.get('inventory_complete'):base['inventory_complete']=True
  conn.execute('UPDATE article SET base=%s WHERE id=%s',(Jsonb(base),patch['article_id']))
  changed+=1
print(json.dumps({'linked_articles':changed,'files':sum(len(p['files']) for p in patches)}))
'''.replace('PATCHES',repr(patches))
outer="import subprocess\nr=subprocess.run(['docker','exec','-i','paperflow-article-repository','python','-'],input="+repr(program)+",capture_output=True,text=True,timeout=120)\nprint(r.stdout)\nassert r.returncode==0,r.stderr[-1000:]\n"
ssh=paramiko.SSHClient();ssh.load_system_host_keys();ssh.connect('192.168.1.120',username='wzy',password=os.environ['RAG_DIAG_SSH_PASSWORD'],look_for_keys=False,allow_agent=False,timeout=15)
i,o,e=ssh.exec_command("sudo -S -p '' python3 -",timeout=180);i.write(os.environ['RAG_DIAG_SSH_PASSWORD']+'\n'+outer);i.flush();i.channel.shutdown_write()
print(o.read().decode());error=e.read().decode();code=o.channel.recv_exit_status();ssh.close()
if code:raise RuntimeError(error[-1500:])
