"""Read the current server ledger without starting or changing jobs."""
import getpass
import importlib.util
import json
from pathlib import Path
import shlex

root = Path(__file__).resolve().parents[2]
config = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/remote-config.json').read_text(encoding='utf8'))
code = '''
import json,sqlite3,sys
from pathlib import Path
c=json.loads(Path(sys.argv[1]).read_bytes())
inventory=json.loads(Path(c['release']['code_manifest']).read_bytes())
files=[row['relative'] for row in inventory if Path(row['relative']).name in ('body_adapter.py','preextract_contract.py','folder_processor.py')]
with sqlite3.connect(Path(c['ledger']['database']).resolve().as_uri()+'?mode=ro',uri=True) as db:
 states=dict(db.execute('SELECT state,count(*) FROM executor_jobs GROUP BY state'))
 present=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='terminated_capacity_releases'").fetchone()
 released=db.execute('SELECT count(*) FROM terminated_capacity_releases').fetchone()[0] if present else 0
 missing=db.execute("SELECT count(*) FROM executor_jobs j LEFT JOIN terminated_capacity_releases r ON r.job_id=j.job_id WHERE j.state='unknown' AND r.job_id IS NULL").fetchone()[0] if present else states.get('unknown',0)
print(json.dumps({'states':states,'release_records':released,'unknown_without_release_record':missing,'capacity':c['ledger']['capacity'],'business_files':files}))
'''
monitor_path = Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py')
spec = importlib.util.spec_from_file_location('existing_server_monitor', monitor_path)
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
client = monitor.connect('192.168.1.120', 'wzy', password=getpass.getpass(''))
try:
    args = [config['transport_config']['server_python'], '-c', code, config['transport_config']['service_config']]
    stdin, stdout, stderr = client.exec_command(' '.join(shlex.quote(a) for a in args), timeout=20)
    stdin.close()
    result = stdout.read(65536)
    if stdout.channel.recv_exit_status() != 0:
        raise RuntimeError('server ledger observation unavailable')
    print(result.decode('utf8'))
finally:
    client.close()
