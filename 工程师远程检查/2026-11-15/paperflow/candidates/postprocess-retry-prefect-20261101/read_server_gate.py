"""Read current executor routes without altering the active SSH gate."""
import getpass
import hashlib
import importlib.util
from pathlib import Path

here = Path(__file__).resolve().parent
gate = '/home/wzy/paperflow-postprocess/candidate-20261101/postprocess-capacity-revision-4e031fec031249178a962e384ffb730b/ssh-command-gate'
spec = importlib.util.spec_from_file_location('existing_server_monitor', Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py'))
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
client = monitor.connect('192.168.1.120', 'wzy', password=getpass.getpass(''))
try:
    with client.open_sftp() as sftp:
        with sftp.open(gate, 'rb') as stream:
            raw = stream.read()
    (here / 'server-gate-current.sh').write_bytes(raw)
    print('read current routes:', hashlib.sha256(raw).hexdigest())
finally:
    client.close()
