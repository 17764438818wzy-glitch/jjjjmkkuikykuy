import getpass
import importlib.util
import json
from pathlib import Path
import shlex


root = Path(__file__).resolve().parents[3]
config = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/remote-config.json').read_text(encoding='utf8'))
path = config['transport_config']['service_config']
expected = config['transport_config']['service_config_sha256']
code = '''
import hashlib,json,sqlite3,sys
from pathlib import Path
p=Path(sys.argv[1]);raw=p.read_bytes();assert hashlib.sha256(raw).hexdigest()==sys.argv[2]
c=json.loads(raw);manifest=Path(c['release']['code_manifest']);data=manifest.read_bytes();rows=json.loads(data)
matches=[r for r in rows if r['relative'].endswith('/postprocess-prefect-20261031/body_adapter.py')]
assert len(matches)==1
body=Path(c['release']['code_root'])/matches[0]['relative']
assert hashlib.sha256(body.read_bytes()).hexdigest()==matches[0]['sha256']
shared=p.parent.parent/'shared-runtime'
with sqlite3.connect(Path(c['ledger']['database']).resolve().as_uri()+'?mode=ro',uri=True) as db:
 states=db.execute('SELECT state,count(*) FROM executor_jobs GROUP BY state').fetchall()
print(json.dumps({'base':c['base'],'code_root':c['release']['code_root'],'code_manifest':str(manifest),
 'code_manifest_sha256':hashlib.sha256(data).hexdigest(),'business_files':len(rows),
 'body_relative':matches[0]['relative'],'body_sha256':matches[0]['sha256'],
 'shared_exists':shared.is_dir(),'executor_states':states}))
'''
monitor_path = Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py')
spec = importlib.util.spec_from_file_location('existing_server_monitor', monitor_path)
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
password = getpass.getpass('')
client = monitor.connect('192.168.1.120', 'wzy', password=password)
try:
    command = 'python3 -c ' + shlex.quote(code) + ' ' + shlex.quote(path) + ' ' + shlex.quote(expected)
    stdin, stdout, stderr = client.exec_command(command, timeout=20)
    stdin.close()
    raw = stdout.read(65536)
    if stdout.channel.recv_exit_status() != 0:
        raise RuntimeError('server revision inventory failed')
    print(raw.decode('utf8'))
finally:
    client.close()
