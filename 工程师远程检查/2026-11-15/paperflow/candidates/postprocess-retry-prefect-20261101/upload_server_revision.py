"""Copy the candidate to an inactive server revision while the task is paused."""
import getpass
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath
import shlex
from uuid import uuid4

import requests

here = Path(__file__).resolve().parent
root = here.parents[1]
bindings = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/bindings.json').read_text(encoding='utf8'))
config = json.loads(Path(bindings['remote_config']).read_text(encoding='utf8'))
task = requests.get('http://127.0.0.1:14203/api/v1/first-step/tasks/6cb5a766-94f9-4613-aa5e-17a883be9016', timeout=5).json()
if task['desired_state'] != 'paused' or task['inflight_requests'] != 0:
    raise ValueError('parent task is not paused and idle')
tag = uuid4().hex[:12]
base = PurePosixPath('/home/wzy/paperflow-postprocess/candidate-20261101')
stage, revision = base / ('retry-stage-' + tag), base / ('retry-release-' + tag)
files = [here / 'body_adapter.py', here / 'remote_jobs.py', here / 'prepare_server_revision.py',
         root / 'candidates/postprocess-rag-unblock-20261101/body-normalization/preextract_contract.py',
         here / 'bootstrap_service_revision.py', here / 'server_receipt_query.py', here / 'server_unread_scope_query.py']
registry = json.loads(Path(bindings['registry_path']).read_bytes())
history = {row['service_config_sha256']: row['service_config'] for row in registry['versions']}
monitor = importlib.util.module_from_spec(importlib.util.spec_from_file_location(
    'existing_server_monitor', Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py')))
monitor.__spec__.loader.exec_module(monitor)
client = monitor.connect('192.168.1.120', 'wzy', password=getpass.getpass(''))
try:
    with client.open_sftp() as sftp:
        sftp.mkdir(str(stage), mode=0o700)
        for source in files:
            target = str(stage / source.name)
            sftp.put(str(source), target)
            with sftp.open(target, 'rb') as copied:
                if hashlib.sha256(copied.read()).digest() != hashlib.sha256(source.read_bytes()).digest():
                    raise ValueError('candidate file changed during transfer')
        with sftp.open(str(stage / 'historical-configs.json'), 'w') as stream:
            stream.write(json.dumps(history, sort_keys=True))
    transport = config['transport_config']
    argv = [transport['server_python'], str(stage / 'prepare_server_revision.py'), str(stage),
            transport['service_config'], str(revision), transport['service_config_sha256'],
            config['snapshot_options']['code_sha256']]
    stdin, stdout, stderr = client.exec_command(' '.join(shlex.quote(value) for value in argv), timeout=120)
    stdin.close()
    out, err = stdout.read(65536), stderr.read(65536)
    if stdout.channel.recv_exit_status() != 0:
        raise RuntimeError('server revision preparation failed: ' + err.decode('utf8', 'replace')[-2000:])
    result = json.loads(out)
    (here / 'prepared-server.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf8')
    print(json.dumps({'prepared': True, 'revision_selected': result['revision_selected'],
                      'business_work_started': result['business_work_started']}))
finally:
    client.close()
