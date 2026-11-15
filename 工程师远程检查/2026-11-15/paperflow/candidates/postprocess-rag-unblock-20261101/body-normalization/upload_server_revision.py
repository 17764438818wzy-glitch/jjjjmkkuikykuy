import getpass
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath
import shlex
import uuid

import requests


here = Path(__file__).resolve().parent
root = here.parents[2]
config = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/remote-config.json').read_text(encoding='utf8'))
task = requests.get('http://127.0.0.1:14203/api/v1/first-step/tasks/6cb5a766-94f9-4613-aa5e-17a883be9016', timeout=5).json()
if task['desired_state'] != 'paused' or task['inflight_requests'] != 0:
    raise ValueError('parent task is not paused and idle')
old_config = config['transport_config']['service_config']
config_sha = config['transport_config']['service_config_sha256']
candidate = here / 'body_adapter.py'
body_sha = hashlib.sha256(candidate.read_bytes()).hexdigest()
base = PurePosixPath('/home/wzy/paperflow-postprocess/candidate-20261101')
tag = body_sha[:12] + '-' + uuid.uuid4().hex[:8]
stage = base / ('postprocess-normalize-stage-' + tag)
revision = base / ('postprocess-normalize-' + tag)
bootstrap = root / 'candidates/postprocess-server-20261101/optimization-20261101/final/bootstrap_service_revision.py'
files = (candidate, bootstrap, here / 'prepare_server_revision.py')
monitor_path = Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py')
spec = importlib.util.spec_from_file_location('existing_server_monitor', monitor_path)
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
password = getpass.getpass('')
client = monitor.connect('192.168.1.120', 'wzy', password=password)
try:
    with client.open_sftp() as sftp:
        sftp.mkdir(str(stage), mode=0o700)
        for source in files:
            target = str(stage / source.name)
            sftp.put(str(source), target)
            with sftp.open(target, 'rb') as copied:
                if hashlib.sha256(copied.read()).hexdigest() != hashlib.sha256(source.read_bytes()).hexdigest():
                    raise ValueError('staged file changed during transfer')
    argv = [config['transport_config']['server_python'], str(stage / 'prepare_server_revision.py'), str(stage), old_config,
            str(revision), config_sha, body_sha]
    stdin, stdout, stderr = client.exec_command(' '.join(shlex.quote(x) for x in argv), timeout=120)
    stdin.close()
    out, err = stdout.read(65536), stderr.read(65536)
    if stdout.channel.recv_exit_status() != 0:
        raise RuntimeError('server revision preparation failed: ' + err.decode('utf8', 'replace')[-2000:])
    result = json.loads(out)
    (here / 'prepared-server.json').write_text(json.dumps(result, sort_keys=True, indent=2), encoding='utf8')
    print(json.dumps(result, sort_keys=True))
finally:
    client.close()
