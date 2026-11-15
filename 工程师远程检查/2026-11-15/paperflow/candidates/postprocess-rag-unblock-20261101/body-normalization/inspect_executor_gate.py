import getpass
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shlex

import paramiko


here = Path(__file__).resolve().parent
root = here.parents[2]
config = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/remote-config.json').read_text(encoding='utf8'))
key = paramiko.RSAKey.from_private_key_file(config['transport_config']['connection']['private_key'])
monitor_path = Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py')
spec = importlib.util.spec_from_file_location('existing_server_monitor', monitor_path)
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
client = monitor.connect('192.168.1.120', 'wzy', password=getpass.getpass(''))
try:
    with client.open_sftp() as sftp:
        with sftp.open('/home/wzy/.ssh/authorized_keys', 'r') as stream:
            lines = stream.read().decode('utf8').splitlines()
        matches = [line for line in lines if key.get_base64() in line]
        if len(matches) != 1:
            raise ValueError('executor key entry not unique')
        match = re.search(r'command="([^"]+)"', matches[0])
        if not match:
            raise ValueError('executor forced command missing')
        words = shlex.split(match.group(1))
        scripts = [word for word in words if word.startswith('/')]
        if len(scripts) != 1:
            print(json.dumps({'forced_command_paths': scripts, 'word_count': len(words)}))
            raise ValueError('executor gate script not identified')
        gate = scripts[0]
        with sftp.open(gate, 'r') as stream:
            source = stream.read().decode('utf8')
        (here / 'executor-gate-current.py').write_text(source, encoding='utf8')
        print(json.dumps({'gate_path': gate, 'source_sha256': hashlib.sha256(source.encode('utf8')).hexdigest(),
                          'line_count': len(source.splitlines())}, ensure_ascii=True))
finally:
    client.close()
