"""Measure the existing read-only native executor observer without dispatching work."""
import hashlib
import getpass
import importlib.util
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parents[1] / 'prefect-paperflow'
OBSERVATION_ROOT = ROOT / 'monitor-test'
MONITOR = Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py')
MONITOR_CONFIG = Path(r'F:\论文预处理系统\看板\_ops\formal59-native-monitor-20261028\rag-runtime-monitor.json')
EXPECTED = {'rag_runtime_observe.py': '327c1120a956ce64e627ff262fed3a8a40b28d19dd6da9557ccccde6005da43b',
            'rag_runtime_snapshot.py': 'e7b3920d35a16e22d156a7f5a07149ef9a4b560e3122805820fcf620d45024b8'}

password = getpass.getpass('') if sys.stdin.isatty() else sys.stdin.readline().rstrip('\r\n')
if not password:
    raise ValueError('server credential unavailable')
settings = json.loads(MONITOR_CONFIG.read_text(encoding='utf8'))
cache = OBSERVATION_ROOT / 'private' / 'preextract_delivery' / 'native-runtime-observation.json'
cache.parent.mkdir(parents=True, exist_ok=True)
settings.update(data_root=str(OBSERVATION_ROOT), output_path=str(cache))
config = ROOT / 'monitor-test-config.json'
config.write_text(json.dumps(settings, ensure_ascii=False), encoding='utf8')
spec = importlib.util.spec_from_file_location('monitor_candidate', MONITOR)
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
sys.path.insert(0, str(PROJECT))
from paperflow_prefect.settings import Settings
from paperflow_prefect.legacy import bind
bind(Settings.load())
from preextract_native_runtime import native_runtime_readiness

client = monitor.connect('192.168.1.120', 'wzy', password=password)
try:
    with client.open_sftp() as sftp:
        for name, expected in EXPECTED.items():
            path = settings['observer_script'].rsplit('/', 1)[0] + '/' + name
            with sftp.open(path, 'rb') as stream:
                if hashlib.sha256(stream.read(1024 * 1024)).hexdigest() != expected:
                    raise ValueError('server observer source differs')
    for index in range(3):
        started = time.monotonic()
        monitor.publish_native_runtime(client, config, password=password, timeout=25)
        observed = json.loads(cache.read_text(encoding='utf8'))
        readiness = native_runtime_readiness(observed, service=settings['service'], pipeline_id=settings['pipeline_id'])
        print(json.dumps({'sample': index + 1, 'elapsed_seconds': round(time.monotonic() - started, 3),
                          'ready': readiness['ready'], 'reason_code': readiness['reason_code'],
                          'age_seconds': round(readiness['age_seconds'], 3) if readiness['age_seconds'] is not None else None}),
              flush=True)
        if index < 2:
            time.sleep(2)
finally:
    client.close()
