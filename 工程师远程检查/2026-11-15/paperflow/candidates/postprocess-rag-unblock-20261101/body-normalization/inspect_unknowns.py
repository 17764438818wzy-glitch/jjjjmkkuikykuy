import getpass
import importlib.util
import json
from pathlib import Path
import shlex
import sqlite3


here = Path(__file__).resolve().parent
root = here.parents[2]
config = json.loads((here / 'remote-config.json').read_text(encoding='utf8'))
with sqlite3.connect(Path(config['journal_database']).resolve().as_uri() + '?mode=ro', uri=True) as db:
    settings = dict(db.execute('SELECT name,value FROM journal_settings'))
with sqlite3.connect(Path(settings['control_database']).resolve().as_uri() + '?mode=ro', uri=True) as db:
    rows = db.execute("SELECT group_id,job_id FROM paperflow_postprocess_stage_jobs WHERE state='execution_unknown'").fetchall()
code = '''
import collections,json,re,sqlite3,sys
from pathlib import Path
c=json.loads(Path(sys.argv[1]).read_bytes()); rows=json.load(sys.stdin)
with sqlite3.connect(Path(c['ledger']['database']).resolve().as_uri()+'?mode=ro',uri=True) as db:
 db.row_factory=sqlite3.Row
 result=[]
 for gid,job in rows:
  executor=db.execute('SELECT * FROM executor_jobs WHERE job_id=?',(job,)).fetchone()
  service=db.execute('SELECT * FROM service_jobs WHERE job_id=?',(job,)).fetchone()
  snapshot=Path(c['staging_root'])/job/'drives/f'
  log=Path(c['jobs_root'])/job/'worker.stderr.log'
  diagnostics=list((snapshot/'pfwire').rglob('diagnostic.json')) if (snapshot/'pfwire').is_dir() else []
  codes=[]
  for path in diagnostics:
   value=json.loads(path.read_bytes())
   reason=value.get('reason')
   codes.append(reason if reason in ('unknown_source_ref','conflicting_source_ref','candidate_validation_failed','source_ref_out_of_bounds','unknown_reading_ref') else 'other')
  detail=json.loads(service['detail_json']) if service else {}
  cleanup=detail.get('cleanup',{})
  result.append(dict(group_id=gid,job_id=job,executor_state=executor['state'] if executor else None,
   service_phase=service['phase'] if service else None,codes=codes,diagnostic_exists=log.is_file(),
   wire_result_exists=(snapshot/'wire-result.json').is_file(),
   cleanup_terminated=cleanup.get('terminated'),cleanup_scope_removed=cleanup.get('scope_removed')))
print(json.dumps(result))
'''
monitor_path = Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py')
spec = importlib.util.spec_from_file_location('existing_server_monitor', monitor_path)
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
client = monitor.connect('192.168.1.120', 'wzy', password=getpass.getpass(''))
try:
    args = [config['transport_config']['server_python'], '-c', code, config['transport_config']['service_config']]
    stdin, stdout, stderr = client.exec_command(' '.join(shlex.quote(a) for a in args), timeout=30)
    stdin.write(json.dumps(rows))
    stdin.close()
    out, err = stdout.read(65536), stderr.read(65536)
    if stdout.channel.recv_exit_status() != 0:
        raise RuntimeError(err.decode('utf8', 'replace')[-1500:])
    result = json.loads(out)
    (here / 'unknown-observation.json').write_text(json.dumps(result, indent=2), encoding='utf8')
    print(json.dumps(result, ensure_ascii=True))
finally:
    client.close()
