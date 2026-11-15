"""Verify the inactive revision and prepare its read-only access inventory."""
import getpass
import importlib.util
import json
from pathlib import Path
import shlex

here = Path(__file__).resolve().parent
root = here.parents[1]
bindings = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/bindings.json').read_text(encoding='utf8'))
config = json.loads(Path(bindings['remote_config']).read_text(encoding='utf8'))
prepared = json.loads((here / 'prepared-server.json').read_text(encoding='utf8'))
code = '''
import hashlib,json,sqlite3,sys
from pathlib import Path
old_path,new_path,old_sha,new_sha,body_sha,validator_sha,jobs_sha,query_sha,unread_path,unread_sha=sys.argv[1:]
old=json.loads(Path(old_path).read_bytes()); new_raw=Path(new_path).read_bytes()
assert hashlib.sha256(new_raw).hexdigest()==new_sha
new=json.loads(new_raw)
old_raw=Path(old['release']['code_manifest']).read_bytes(); new_manifest=Path(new['release']['code_manifest']).read_bytes()
assert hashlib.sha256(old_raw).hexdigest()==old_sha
a={r['relative']:r for r in json.loads(old_raw)}; b={r['relative']:r for r in json.loads(new_manifest)}
assert set(a)<=set(b) and len(set(b)-set(a))==1
overlay=next(iter(set(b)-set(a)))
assert overlay.endswith('/clean_overlay/preextract_contract.py') and b[overlay]['sha256']==validator_sha
changed=[name for name in a if a[name]!=b[name]]
assert len(changed)==1 and changed[0].endswith('/postprocess-prefect-20261031/body_adapter.py') and b[changed[0]]['sha256']==body_sha
root=Path(new['release']['code_root']); app=Path(new_path).parent
for name in (overlay,changed[0]):
 raw=(root/name).read_bytes(); assert hashlib.sha256(raw).hexdigest()==b[name]['sha256']; compile(raw,name,'exec')
raw=(app/'remote_jobs.py').read_bytes(); assert hashlib.sha256(raw).hexdigest()==jobs_sha; compile(raw,str(app/'remote_jobs.py'),'exec')
raw=(app/'server_receipt_query.py').read_bytes(); assert hashlib.sha256(raw).hexdigest()==query_sha; compile(raw,str(app/'server_receipt_query.py'),'exec')
raw=Path(unread_path).read_bytes(); assert hashlib.sha256(raw).hexdigest()==unread_sha; compile(raw,unread_path,'exec')
sys.path.insert(0,str(app))
from remote_jobs import RemoteJobs
ledger=RemoteJobs(new['ledger']['database'],capacity=new['ledger']['capacity'],ledger_id=new['server_identity']['ledger_id'])
occupied=ledger.occupied()
files={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(app.glob('*.py'))}
guard=dict(schema='paperflow-page-release/1',python=None,packages={},files=files)
raw=json.dumps(guard,ensure_ascii=False,sort_keys=True,separators=(',', ':'),allow_nan=False).encode('utf8')
guardpath=app.parent/'page-app-manifest.json'
with guardpath.open('xb') as f:f.write(raw)
print(json.dumps({'old_files_retained':len(a),'changed_files':len(changed),'added_validator':True,
 'capacity':new['ledger']['capacity'],'occupied_from_actual_ledger':occupied,
 'guard_manifest':str(guardpath),'guard_sha256':hashlib.sha256(raw).hexdigest(),
 'business_work_started':False}))
'''
spec = importlib.util.spec_from_file_location('existing_server_monitor', Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py'))
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
client = monitor.connect('192.168.1.120', 'wzy', password=getpass.getpass(''))
try:
    transport = config['transport_config']
    argv = [transport['server_python'], '-c', code, transport['service_config'], prepared['config_path'],
            config['snapshot_options']['code_sha256'], prepared['config_sha256'], prepared['body_sha256'],
            prepared['validator_sha256'], prepared['server_jobs_sha256'], prepared['server_query_sha256'],
            prepared['unread_path'], prepared['unread_sha256']]
    stdin, stdout, stderr = client.exec_command(' '.join(shlex.quote(value) for value in argv), timeout=60)
    stdin.close()
    out, err = stdout.read(65536), stderr.read(65536)
    if stdout.channel.recv_exit_status() != 0:
        raise RuntimeError('prepared server verification failed: ' + err.decode('utf8', 'replace')[-1500:])
    result = json.loads(out)
    (here / 'verified-server.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf8')
    print(json.dumps(result, ensure_ascii=False))
finally:
    client.close()
