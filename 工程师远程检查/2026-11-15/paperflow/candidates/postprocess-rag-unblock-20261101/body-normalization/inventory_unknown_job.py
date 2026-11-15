import getpass
import importlib.util
import json
from pathlib import Path
import shlex

here = Path(__file__).resolve().parent
config = json.loads((here / 'remote-config.json').read_text(encoding='utf8'))
code = '''
import json,sys
from pathlib import Path
c=json.loads(Path(sys.argv[1]).read_bytes())
for job in sys.argv[2:]:
 root=Path(c['staging_root'])/job/'drives/f'
 print(json.dumps({'job':job,'entries':[{'name':p.name,'directory':p.is_dir(),
   'children':[q.name for q in p.iterdir()][:40] if p.is_dir() and p.name not in ('prefix','wine-prefix') else []}
   for p in root.iterdir()]}))
'''
monitor_path = Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py')
spec = importlib.util.spec_from_file_location('existing_server_monitor', monitor_path)
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
client = monitor.connect('192.168.1.120', 'wzy', password=getpass.getpass(''))
try:
    args = [config['transport_config']['server_python'], '-c', code, config['transport_config']['service_config'],
            '7af819c6-293e-5fcf-967f-c1fbea392900', 'ce14b233-ea5e-513d-9307-513a4e8324d0']
    stdin, stdout, stderr = client.exec_command(' '.join(shlex.quote(a) for a in args), timeout=15)
    stdin.close()
    output = stdout.read(32768)
    if stdout.channel.recv_exit_status() != 0:
        raise RuntimeError(stderr.read(1000).decode('utf8', 'replace'))
    print(output.decode('utf8'))
finally:
    client.close()
