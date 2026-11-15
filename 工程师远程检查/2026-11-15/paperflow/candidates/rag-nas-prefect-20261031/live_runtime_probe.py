"""Collect one bound RAG executor observation into this test case only.

Reads the SSH sudo password from stdin; does not store it or touch a scheduler.
The reused collector only inspects server metadata and task executor state.
"""
import importlib.util
import json
from pathlib import Path
import sys
import getpass
import shlex
import hashlib


WORKSPACE = Path(__file__).resolve().parents[2]
CASE_ROOT = Path(__file__).resolve().parent / 'live-tests'
MONITOR = Path(r'F:\论文预处理系统\看板\dashboard\server_monitor.py')
MONITOR_CONFIG = Path(r'F:\论文预处理系统\看板\_ops\formal59-native-monitor-20261028\rag-runtime-monitor.json')


def main():
    if len(sys.argv) != 2:
        raise ValueError('a single existing test case ID is required')
    case = (CASE_ROOT / sys.argv[1]).resolve()
    if (case.parent != CASE_ROOT.resolve()
            or not any((case / name).is_file() for name in ('report.json', 'desktop-sample-copy-receipt.json'))):
        raise ValueError('unknown test case')
    password = getpass.getpass('') if sys.stdin.isatty() else sys.stdin.readline().rstrip('\r\n')
    if not password:
        raise ValueError('credential unavailable')
    sys.path.insert(0, str(WORKSPACE / 'prefect-paperflow'))
    from paperflow_prefect.settings import Settings
    from paperflow_prefect.legacy import bind
    settings = Settings.load()
    bind(settings)
    from auto_config import load_key
    from ragflow_client import RagflowClient
    from preextract_batch_delivery import load_native_batch_config
    from preextract_native_runtime import readiness_for_client
    client = RagflowClient(settings['rag_url'], load_key(settings.path('settings_root'), 'ragflow'))
    config = load_native_batch_config(Path(r'E:\uv\ragflow_upstream\data'), client)
    monitor_config = json.loads(MONITOR_CONFIG.read_text(encoding='utf8'))
    isolated_data = case / 'observer-data'
    cache = isolated_data / 'private/preextract_delivery/native-runtime-observation.json'
    cache.parent.mkdir(parents=True, exist_ok=True)
    monitor_config['data_root'] = str(isolated_data)
    monitor_config['output_path'] = str(cache)
    config_path = case / 'observer-config.json'
    config_path.write_text(json.dumps(monitor_config, ensure_ascii=False, indent=2), encoding='utf8')
    import paramiko
    ssh = paramiko.SSHClient()
    ssh.load_host_keys(config.memory_settings['known_hosts'])
    ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        ssh.connect(config.memory_settings['host'], port=config.memory_settings['port'],
                    username=config.memory_settings['username'],
                    password=password, look_for_keys=False,
                    allow_agent=False, timeout=5, auth_timeout=5, banner_timeout=5)
        expected = {'rag_runtime_observe.py': '327c1120a956ce64e627ff262fed3a8a40b28d19dd6da9557ccccde6005da43b',
                    'rag_runtime_snapshot.py': 'e7b3920d35a16e22d156a7f5a07149ef9a4b560e3122805820fcf620d45024b8'}
        sftp = ssh.open_sftp()
        try:
            for name, digest in expected.items():
                path = monitor_config['observer_script'].rsplit('/', 1)[0] + '/' + name
                with sftp.open(path, 'rb') as stream:
                    if hashlib.sha256(stream.read(1024 * 1024)).hexdigest() != digest:
                        raise ValueError('server observer source identity differs')
        finally:
            sftp.close()
        spec = importlib.util.spec_from_file_location('paperflow_test_observer', MONITOR)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.publish_native_runtime(ssh, config_path, password=password, timeout=25)
        config.native_runtime_observation = lambda: json.loads(cache.read_text(encoding='utf8'))
        readiness = readiness_for_client(client, config)
        row = json.loads(cache.read_text(encoding='utf8'))
        result = {k: readiness.get(k) for k in ('state', 'ready', 'reason_code', 'age_seconds',
                                                'observed_at', 'ready_valid_until')}
        result.update(collection_complete=row.get('collection_complete'),
                      collection_errors=row.get('collection_errors'),
                      error_type=row.get('error_type'), case_id=case.name,
                      observer_sources_match=True)
        if not result['ready']:
            # Read-only diagnosis of the pinned collector; never write the
            # production observer cache or control a server-side task.
            binding = {key: monitor_config[key] for key in
                       ('container', 'container_id', 'service', 'pipeline_id', 'stream', 'consumer_group')}
            remote = '''import importlib.util,json,sys
try:
 spec=importlib.util.spec_from_file_location('obs',sys.argv[1])
 mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
 row=mod.collect(**json.loads(sys.argv[2]))
 print(json.dumps({'ok':True,'collection_complete':row.get('collection_complete'),
                   'collection_errors':row.get('collection_errors')}))
except Exception as exc:
 print(json.dumps({'ok':False,'error_type':type(exc).__name__,'error':str(exc)[:250]}))
'''
            command = ('sudo -S -p "" -- python3 -B -c ' + shlex.quote(remote) + ' '
                       + shlex.quote(monitor_config['observer_script']) + ' '
                       + shlex.quote(json.dumps(binding)))
            stdin, stdout, stderr = ssh.exec_command(command, timeout=30)
            stdin.write(password + '\n')
            stdin.flush()
            diagnostic = stdout.read(65537)
            status = stdout.channel.recv_exit_status()
            stderr.read(65537)
            try:
                detail = json.loads(diagnostic) if status == 0 else {'sudo_or_collector_exit': status}
            except (ValueError, UnicodeError):
                detail = {'sudo_or_collector_exit': status, 'output_unreadable': True}
            result['diagnostic'] = detail
        (case / 'runtime-readiness.json').write_text(json.dumps(result, ensure_ascii=False, indent=2),
                                                     encoding='utf8')
        print(json.dumps(result, ensure_ascii=True), flush=True)
    finally:
        ssh.close()


if __name__ == '__main__':
    main()
