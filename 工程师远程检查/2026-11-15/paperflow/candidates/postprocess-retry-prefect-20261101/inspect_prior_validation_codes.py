"""Summarize saved validation diagnostics without processing papers."""
import getpass
import importlib.util
import json
from pathlib import Path
import shlex

here = Path(__file__).resolve().parent
root = here.parents[1]
bindings = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/bindings.json').read_bytes())
config = json.loads(Path(bindings['remote_config']).read_bytes())
jobs = next(iter(json.loads((here / 'retry-targets.json').read_bytes()).values()))
code = '''
from collections import Counter
import json,sys
from pathlib import Path
c=json.loads(Path(sys.argv[1]).read_bytes());jobs=json.load(sys.stdin); result=[]
for job in jobs:
 root=Path(c['staging_root'])/job/'drives/f/pfwire'
 if not root.is_dir():continue
 for path in root.rglob('*validation.json'):
  v=json.loads(path.read_bytes()); errors=v.get('errors',[])
  if errors:result.append({'codes':sorted(set(row.get('code','unknown') for row in errors)),
                           'locations':sorted(set(row.get('code','unknown')+':'+row.get('path','') for row in errors))})
print(json.dumps({'diagnostic_files':len(result),'error_combinations':dict(Counter(','.join(row['codes']) for row in result)),
                  'locations':dict(Counter(location for row in result for location in row['locations']))}))
'''
spec = importlib.util.spec_from_file_location('existing_server_monitor', Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py'))
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
client = monitor.connect('192.168.1.120', 'wzy', password=getpass.getpass(''))
try:
    argv = [config['transport_config']['server_python'], '-c', code, config['transport_config']['service_config']]
    stdin, stdout, stderr = client.exec_command(' '.join(shlex.quote(value) for value in argv), timeout=30)
    stdin.write(json.dumps(jobs)); stdin.close()
    out, err = stdout.read(65536), stderr.read(65536)
    if stdout.channel.recv_exit_status() != 0:
        raise RuntimeError('saved diagnostics could not be read: ' + err.decode('utf8', 'replace')[-500:])
    print(out.decode('utf8'))
finally:
    client.close()
