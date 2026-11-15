import getpass
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import stat
import sys
from uuid import uuid4


here = Path(__file__).resolve().parent
root = here.parents[2]
release = root / 'candidates/postprocess-prefect-20261031/server_release_next'
sys.path.insert(0, str(release))
from read_only_rpc import VERSION_BOOTSTRAP, command_for

current = json.loads((here.parent / 'remote-config.json').read_text(encoding='utf8'))
candidate = json.loads((here / 'remote-config.json').read_text(encoding='utf8'))
bindings = json.loads((here / 'candidate-bindings.json').read_text(encoding='utf8'))
registry = json.loads(Path(bindings['registry_path']).read_bytes())
bundle = json.loads(Path(bindings['bundle_path']).read_bytes())
index = json.loads(Path(bundle['index_path']).read_bytes())
selected, app = registry['versions'][-1], index['versions'][-1]
gate = '/home/wzy/paperflow-postprocess/candidate-20261101/postprocess-capacity-revision-4e031fec031249178a962e384ffb730b/ssh-command-gate'
expected_gate = '1a47228c42bcf01896f60b46f633626ee5a3353c47fb054b5bccfc1700ef9b32'
new_bytes = (here / 'executor-gate-candidate').read_bytes()
if hashlib.sha256(new_bytes).hexdigest() != bindings['gate_sha256']:
    raise ValueError('candidate routes changed')
monitor_path = Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py')
spec = importlib.util.spec_from_file_location('existing_server_monitor', monitor_path)
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
client = monitor.connect('192.168.1.120', 'wzy', password=getpass.getpass(''))
try:
    suffix = uuid4().hex[:12]
    backup, temporary = gate + '.before-normalize-' + suffix, gate + '.new-' + suffix
    with client.open_sftp() as sftp:
        with sftp.open(gate, 'rb') as stream:
            original = stream.read()
        if hashlib.sha256(original).hexdigest() != expected_gate:
            raise ValueError('existing executor routes changed')
        mode = stat.S_IMODE(sftp.stat(gate).st_mode)
        with sftp.open(backup, 'wx') as stream:
            stream.write(original)
        sftp.chmod(backup, mode)
        with sftp.open(temporary, 'wx') as stream:
            stream.write(new_bytes)
        sftp.chmod(temporary, mode)
        stdin, stdout, stderr = client.exec_command('sh -n ' + shlex.quote(temporary), timeout=5)
        stdin.close()
        if stdout.channel.recv_exit_status() != 0:
            raise ValueError('candidate executor routes failed shell syntax check')
        sftp.posix_rename(temporary, gate)
        with sftp.open(gate, 'rb') as stream:
            if hashlib.sha256(stream.read()).hexdigest() != bindings['gate_sha256']:
                raise ValueError('installed executor routes changed')
finally:
    client.close()

connection = current['transport_config']['connection']
client = monitor.connect(connection['host'], connection['username'], key_file=connection['private_key'])
try:
    checks = [('old', command_for(current['transport_config'], 'probe')),
              ('new', command_for(candidate['transport_config'], 'probe'))]
    guarded = shlex.join([selected['server_python'], '-I', '-B', '-c', VERSION_BOOTSTRAP,
        bundle['entry_path'], bundle['entry_sha256'], bundle['guard_path'], bundle['guard_sha256'],
        app['app'], app['manifest_path'], app['manifest_sha256'], selected['rpc_script'],
        selected['rpc_script_sha256'], selected['service_config'], selected['service_config_sha256'], 'probe'])
    checks.append(('new_guarded', guarded))
    for label, command in checks:
        nonce = str(uuid4())
        stdin, stdout, stderr = client.exec_command(command, timeout=15)
        stdin.write(json.dumps({'server_identity': candidate['server_identity'], 'nonce': nonce}))
        stdin.close()
        raw = stdout.read(65536)
        if stdout.channel.recv_exit_status() != 0:
            raise RuntimeError(label + ' route probe failed: ' + stderr.read(1024).decode('utf8', 'replace'))
        result = json.loads(raw)
        if result.get('nonce') != nonce or result.get('server_identity') != candidate['server_identity'] or result.get('ledger_identity_verified') is not True:
            raise ValueError(label + ' route returned an unexpected identity')
    print(json.dumps({'installed_new_routes': True, 'old_probe': True, 'new_probe': True,
                      'new_guarded_probe': True, 'backup': backup, 'business_work_started': False}))
finally:
    client.close()
