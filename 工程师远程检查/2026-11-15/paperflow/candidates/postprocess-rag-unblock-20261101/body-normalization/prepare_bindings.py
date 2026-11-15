import copy
import hashlib
import json
from pathlib import Path, PurePosixPath


here = Path(__file__).resolve().parent
root = here.parents[2]
old_dir = root / 'candidates/postprocess-server-20261101/optimization-20261101/final'
old_bindings = json.loads((old_dir / 'bindings.json').read_text(encoding='utf8'))
current = json.loads((here.parent / 'remote-config.json').read_text(encoding='utf8'))
prepared = json.loads((here / 'prepared-server.json').read_text(encoding='utf8'))
verified = json.loads((here / 'verified-server.json').read_text(encoding='utf8'))


def save(name, value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf8')
    path = here / name
    path.write_bytes(raw)
    return str(path), hashlib.sha256(raw).hexdigest()


for key in ('registry', 'bundle'):
    path = Path(old_bindings[key + '_path'])
    if hashlib.sha256(path.read_bytes()).hexdigest() != old_bindings[key + '_sha256']:
        raise ValueError('existing observer binding changed')
registry = json.loads(Path(old_bindings['registry_path']).read_bytes())
bundle = json.loads(Path(old_bindings['bundle_path']).read_bytes())
index = json.loads(Path(bundle['index_path']).read_bytes())
old_sha = current['transport_config']['service_config_sha256']
old_row = next(row for row in registry['versions'] if row['service_config_sha256'] == old_sha)
old_index = next(row for row in index['versions'] if row['canonical_sha256'] == old_row['canonical_sha256'])
new_app = str(PurePosixPath(prepared['config_path']).parent)
old_revision = str(PurePosixPath(old_row['service_config']).parent.parent)
new_revision = str(PurePosixPath(prepared['config_path']).parent.parent)
new_row = dict(old_row, canonical_sha256=prepared['config_sha256'],
               service_config=prepared['config_path'], service_config_sha256=prepared['config_sha256'],
               rpc_script=str(PurePosixPath(new_app) / 'server_rpc.py'))
registry['versions'].append(new_row)
index['versions'].append(dict(canonical_sha256=prepared['config_sha256'], app=new_app,
                              manifest_path=verified['guard_manifest'], manifest_sha256=verified['guard_sha256']))
registry_path, registry_sha = save('history-registry.json', registry)
index_path, index_sha = save('guard-index.json', index)
bundle.update(index_path=index_path, index_sha256=index_sha)
bundle_path, bundle_sha = save('guard-bundle.json', bundle)
new_config = copy.deepcopy(current)
transport = new_config['transport_config']
transport.update(service_config=prepared['config_path'], service_config_sha256=prepared['config_sha256'],
                 rpc_script=new_row['rpc_script'], staging_script=str(PurePosixPath(new_app) / 'server_staging_cli.py'))
encoded = json.dumps(transport, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf8')
new_config['transport_config_sha256'] = hashlib.sha256(encoded).hexdigest()
new_config['snapshot_options']['code_sha256'] = prepared['code_sha256']
config_path, config_sha = save('remote-config.json', new_config)
source = (here / 'executor-gate-current.py').read_text(encoding='utf8')
lines = [line for line in source.splitlines() if old_revision + '/' in line]
if len(lines) != 11:
    raise ValueError('existing executor commands changed')
new_lines = [line.replace(old_revision, new_revision).replace(old_sha, prepared['config_sha256'])
             .replace(old_index['manifest_sha256'], verified['guard_sha256']) for line in lines]
gate = source.replace('  *) exit 126 ;;', '\n'.join(new_lines) + '\n  *) exit 126 ;;')
if gate == source:
    raise ValueError('executor command insertion point changed')
(here / 'executor-gate-candidate').write_bytes(gate.encode('utf8'))
save('candidate-bindings.json', dict(registry_path=registry_path, registry_sha256=registry_sha,
     bundle_path=bundle_path, bundle_sha256=bundle_sha, remote_config=config_path,
     remote_config_sha256=config_sha, gate_sha256=hashlib.sha256(gate.encode('utf8')).hexdigest()))
print(json.dumps({'historical_versions': len(registry['versions']), 'new_commands': len(new_lines),
                  'mainline_changed': False, 'model_requests_sent': 0}))
