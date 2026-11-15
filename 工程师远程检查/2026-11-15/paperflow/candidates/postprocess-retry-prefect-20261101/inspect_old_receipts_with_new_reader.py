"""Read representative existing receipts with the inactive new server reader."""
import getpass
import importlib.util
import json
from pathlib import Path, PurePosixPath
import shlex
import sqlite3

here = Path(__file__).resolve().parent
root = here.parents[1]
bindings = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/bindings.json').read_bytes())
current = json.loads(Path(bindings['remote_config']).read_bytes())
prepared = json.loads((here / 'prepared-server.json').read_bytes())
with sqlite3.connect(Path(current['journal_database']).resolve().as_uri() + '?mode=ro', uri=True) as db:
    control = dict(db.execute('SELECT name,value FROM journal_settings'))['control_database']
    intents = {row[0]: json.loads(row[1]) for row in db.execute('SELECT claim_id,intent_json FROM remote_start_intents')}
with sqlite3.connect(Path(control).resolve().as_uri() + '?mode=ro', uri=True) as db:
    rows = db.execute("SELECT p.claim_id,p.receipt_json,p.receipt_sha256 FROM postprocessing_claims c "
        "JOIN postprocessing_receipts p ON p.claim_id=c.claim_id "
        "LEFT JOIN paperflow_rag_nas_jobs r ON r.task_id=c.task_id AND r.group_id=c.group_id "
        "WHERE c.state='waiting_rag' AND (r.stage IS NULL OR "
        "(r.stage='rag' AND r.state IN ('paused','needs_review')))").fetchall()
requests = {}
for claim, receipt, digest in rows:
    intent = intents[claim]
    job = intent['job']
    requests.setdefault(job['code_sha256'], dict(job=job, server_identity=current['server_identity'],
                                                 receipt_json=receipt, receipt_sha256=digest))
code = '''
import json,sys
from pathlib import Path
app,config_path,config_sha,unread_path=sys.argv[1:]
sys.path.insert(0,app)
import server_job_service as service
from server_receipt_query import inspect_requests
import importlib.util
spec=importlib.util.spec_from_file_location('candidate_unread',unread_path)
unread=importlib.util.module_from_spec(spec);spec.loader.exec_module(unread)
config=service.load_config(config_path,config_sha)
rows=json.load(sys.stdin);results=[]
for req in rows:
 try:
  output=inspect_requests(config,[req])
  check=unread.execute(config,dict(req,nonce='00000000-0000-0000-0000-000000000001'))
  results.append({'release':req['job']['code_sha256'][:12],'inspected':len(output)==1,'unread':check['status']})
 except Exception as exc:
  results.append({'release':req['job']['code_sha256'][:12],'inspected':False,'error_type':type(exc).__name__})
print(json.dumps(results))
'''
spec = importlib.util.spec_from_file_location('existing_server_monitor', Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py'))
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
client = monitor.connect('192.168.1.120', 'wzy', password=getpass.getpass(''))
try:
    argv = [current['transport_config']['server_python'], '-B', '-c', code,
            str(PurePosixPath(prepared['config_path']).parent), prepared['config_path'],
            prepared['config_sha256'], prepared['unread_path']]
    stdin, stdout, stderr = client.exec_command(' '.join(shlex.quote(value) for value in argv), timeout=120)
    stdin.write(json.dumps(list(requests.values()), ensure_ascii=False))
    stdin.close()
    out, err = stdout.read(65536), stderr.read(65536)
    if stdout.channel.recv_exit_status() != 0:
        raise RuntimeError('read-only receipt inspection failed: ' + err.decode('utf8', 'replace')[-500:])
    result = json.loads(out)
    (here / 'old-receipt-readonly-observation.json').write_text(json.dumps(result, indent=2), encoding='utf8')
    print(json.dumps(result))
finally:
    client.close()
