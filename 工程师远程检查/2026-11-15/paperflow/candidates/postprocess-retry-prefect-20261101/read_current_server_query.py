"""Read the installed receipt reader as the basis for the new single reader."""
import getpass
import importlib.util
import json
from pathlib import Path, PurePosixPath

here = Path(__file__).resolve().parent
root = here.parents[1]
bindings = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/bindings.json').read_bytes())
config = json.loads(Path(bindings['remote_config']).read_bytes())
app = PurePosixPath(config['transport_config']['service_config']).parent
spec = importlib.util.spec_from_file_location('existing_server_monitor', Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py'))
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
client = monitor.connect('192.168.1.120', 'wzy', password=getpass.getpass(''))
try:
    with client.open_sftp() as sftp:
        with sftp.open(str(app / 'server_receipt_query.py'), 'rb') as stream:
            (here / 'server_receipt_query.py').write_bytes(stream.read())
    print('Current server receipt reader copied to candidate; live service unchanged.')
finally:
    client.close()
