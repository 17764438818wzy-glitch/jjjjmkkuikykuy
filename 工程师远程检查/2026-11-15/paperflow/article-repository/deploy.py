"""Install only the independent article catalog service; secrets are not logged."""
import os
from pathlib import Path
import paramiko

ROOT=Path(__file__).parent
REMOTE='/opt/paperflow-article-repository'
ssh=paramiko.SSHClient(); ssh.load_system_host_keys()
ssh.connect('192.168.1.120',username='wzy',password=os.environ['RAG_DIAG_SSH_PASSWORD'],look_for_keys=False,allow_agent=False,timeout=12)
def run(command,timeout=60,stdin_text=''):
    i,o,e=ssh.exec_command("sudo -S -p '' "+command,timeout=timeout)
    i.write(os.environ['RAG_DIAG_SSH_PASSWORD']+'\n'+stdin_text);i.flush();i.channel.shutdown_write()
    for line in o: print(line,end='',flush=True)
    error=e.read().decode('utf8','replace')
    code=o.channel.recv_exit_status()
    if error: print(error[-2500:])
    if code: raise RuntimeError('Server operation failed with exit '+str(code))
run("install -d -o wzy -g wzy "+REMOTE)
sftp=ssh.open_sftp()
for file in [*ROOT.joinpath('article_repository').glob('*.py'),ROOT/'article_repository/schema.sql',
             *ROOT.joinpath('frontend/dist').rglob('*'),ROOT/'majors.json',ROOT/'legacy-records.json',
             ROOT/'Dockerfile',ROOT/'requirements.txt',ROOT/'compose.yaml']:
    if not file.is_file(): continue
    target=REMOTE+'/'+file.relative_to(ROOT).as_posix()
    parts=target.split('/')[:-1]
    for n in range(2,len(parts)+1):
        directory='/'.join(parts[:n])
        try:sftp.stat(directory)
        except FileNotFoundError:sftp.mkdir(directory)
    sftp.put(str(file),target)
library=ROOT.parent/'qdrant-migration/paperflow_qdrant/library.py'
if not library.exists():library=ROOT/'library.py'
sftp.put(str(library),REMOTE+'/library.py')
sftp.close()
secret_program="""import os,secrets
from pathlib import Path
root=Path('/opt/paperflow-article-repository')
old=dict(line.split('=',1) for line in Path('/opt/paperflow-qdrant-20261103/.env').read_text().splitlines() if '=' in line and not line.startswith('#'))
path=root/'.env'
if not path.exists():
    values={k:old[k] for k in ('QDRANT_API_KEY','PAPERFLOW_RAG_API_KEY') if k in old}
    values.update(ARTICLE_POSTGRES_PASSWORD=secrets.token_hex(24),ARTICLE_NAS_USER=os.environ['ARTICLE_SETUP_NAS_USER'],ARTICLE_NAS_PASSWORD=os.environ['ARTICLE_SETUP_NAS_PASSWORD'])
    path.write_text(''.join(k+'='+v+'\\n' for k,v in values.items()))
    path.chmod(0o600)
print('Repository service credentials configured without disclosure')
"""
program="import os\nos.environ['ARTICLE_SETUP_NAS_USER']="+repr(os.environ['ARTICLE_SETUP_NAS_USER'])+"\nos.environ['ARTICLE_SETUP_NAS_PASSWORD']="+repr(os.environ['ARTICLE_SETUP_NAS_PASSWORD'])+"\n"+secret_program
run('python3 -',stdin_text=program)
run('docker compose -f '+REMOTE+'/compose.yaml build repository',timeout=900)
run('docker compose -f '+REMOTE+'/compose.yaml up -d',timeout=120)
ssh.close()
