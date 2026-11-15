"""Back up the paused server ledger and permit another attempt per folder."""
import getpass
import importlib.util
import json
from pathlib import Path, PurePosixPath
import shlex

import requests

here = Path(__file__).resolve().parent
root = here.parents[1]
task = requests.get('http://127.0.0.1:14203/api/v1/first-step/tasks/6cb5a766-94f9-4613-aa5e-17a883be9016', timeout=5).json()
if task['desired_state'] != 'paused' or task['inflight_requests'] != 0:
    raise ValueError('the original task is not paused and idle')
bindings = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/bindings.json').read_bytes())
current = json.loads(Path(bindings['remote_config']).read_bytes())
prepared = json.loads((here / 'prepared-server.json').read_bytes())
code = '''
import json,sqlite3,sys
from pathlib import Path
from uuid import uuid4
app, config_path, config_sha = sys.argv[1:]
sys.path.insert(0, app)
from server_job_service import load_config
from remote_jobs import RemoteJobs
config = load_config(config_path, config_sha)
path = Path(config['ledger']['database'])
ledger = RemoteJobs(path, capacity=config['ledger']['capacity'], ledger_id=config['server_identity']['ledger_id'])
if ledger.occupied() != 0:
    raise ValueError('server postprocessing is still occupied')
with sqlite3.connect(path) as db:
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name='executor_jobs'").fetchone():
        raise ValueError('server ledger has unexpected triggers')
    for (table,) in db.execute("SELECT name FROM sqlite_master WHERE type='table'"):
        if any(row[2] == 'executor_jobs' for row in db.execute('PRAGMA foreign_key_list("' + table + '")')):
            raise ValueError('server ledger has a foreign key to executor jobs')
    before = db.execute('SELECT * FROM executor_jobs ORDER BY job_id').fetchall()
    backup = path.with_name(path.name + '.before-retry-' + uuid4().hex + '.bak')
    with sqlite3.connect(backup) as saved:
        db.backup(saved)
ledger.allow_multiple_attempts()
with sqlite3.connect(path) as db:
    after = db.execute('SELECT * FROM executor_jobs ORDER BY job_id').fetchall()
    assert after == before and db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
print(json.dumps({'previous_jobs_preserved':len(before), 'occupied':ledger.occupied(), 'backup':str(backup)}))
'''
spec = importlib.util.spec_from_file_location('existing_server_monitor', Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py'))
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
client = monitor.connect('192.168.1.120', 'wzy', password=getpass.getpass(''))
try:
    argv = [current['transport_config']['server_python'], '-B', '-c', code,
            str(PurePosixPath(prepared['config_path']).parent), prepared['config_path'], prepared['config_sha256']]
    stdin, stdout, stderr = client.exec_command(' '.join(shlex.quote(x) for x in argv), timeout=60)
    stdin.close()
    output, error = stdout.read(65536), stderr.read(65536)
    if stdout.channel.recv_exit_status() != 0:
        raise RuntimeError('server ledger migration failed: ' + error.decode('utf8', 'replace')[-1200:])
    print(output.decode('utf8'))
finally:
    client.close()
