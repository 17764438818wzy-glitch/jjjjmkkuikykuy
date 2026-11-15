import getpass
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex


here = Path(__file__).resolve().parent
root = here.parents[2]
current = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/remote-config.json').read_text(encoding='utf8'))
prepared = json.loads((here / 'prepared-server.json').read_text(encoding='utf8'))
source = '''
import hashlib,json,os,stat,sys,uuid
from pathlib import Path
old_config,new_config,expected_old,expected_new=sys.argv[1:]
old=json.loads(Path(old_config).read_bytes());new=json.loads(Path(new_config).read_bytes())
old_raw=Path(old['release']['code_manifest']).read_bytes();new_raw=Path(new['release']['code_manifest']).read_bytes()
assert hashlib.sha256(old_raw).hexdigest()==expected_old
assert hashlib.sha256(new_raw).hexdigest()==expected_new
a={r['relative']:r for r in json.loads(old_raw)};b={r['relative']:r for r in json.loads(new_raw)}
assert set(a)==set(b)
changed=[name for name in a if a[name]!=b[name]]
assert len(changed)==1 and changed[0].endswith('/postprocess-prefect-20261031/body_adapter.py')
revision=Path(new_config).parent.parent;shared=revision/'shared-runtime'
assert shared.resolve()==(Path(old_config).parent.parent/'shared-runtime').resolve()
sample=shared/'drive_c/Python/python.exe';info=sample.stat();assert stat.S_ISREG(info.st_mode) and info.st_mode & 0o222==0
jobroot=Path(new['jobs_root']);assert info.st_dev==jobroot.stat().st_dev
test=jobroot/('.link-probe-'+uuid.uuid4().hex)
try:
 os.link(sample,test)
 assert test.stat().st_ino==info.st_ino and test.stat().st_dev==info.st_dev
finally:
 if test.exists():test.unlink()
app=Path(new_config).parent
files={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(app.glob('*.py'))}
guard=dict(schema='paperflow-page-release/1',python=None,packages={},files=files)
raw=json.dumps(guard,ensure_ascii=False,sort_keys=True,separators=(',', ':'),allow_nan=False).encode('utf8')
guardpath=revision/'page-app-manifest.json'
with guardpath.open('xb') as f:f.write(raw)
print(json.dumps({'old_manifest_matches_mainline':True,'business_file_count':len(a),'changed_file_count':len(changed),
 'shared_readonly_sample':True,'shared_hardlink_probe':True,'shared_same_filesystem':True,
 'app_files':len(files),'guard_manifest':str(guardpath),'guard_sha256':hashlib.sha256(raw).hexdigest(),
 'business_work_started':False}))
'''
monitor_path = Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py')
spec = importlib.util.spec_from_file_location('existing_server_monitor', monitor_path)
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
client = monitor.connect('192.168.1.120', 'wzy', password=getpass.getpass(''))
try:
    args = [current['transport_config']['server_python'], '-c', source,
            current['transport_config']['service_config'], prepared['config_path'],
            current['snapshot_options']['code_sha256'], prepared['code_sha256']]
    stdin, stdout, stderr = client.exec_command(' '.join(shlex.quote(a) for a in args), timeout=30)
    stdin.close()
    out, err = stdout.read(65536), stderr.read(65536)
    if stdout.channel.recv_exit_status() != 0:
        raise RuntimeError(err.decode('utf8', 'replace')[-2000:])
    result = json.loads(out)
    (here / 'verified-server.json').write_text(json.dumps(result, sort_keys=True, indent=2), encoding='utf8')
    print(json.dumps(result, sort_keys=True))
finally:
    client.close()
