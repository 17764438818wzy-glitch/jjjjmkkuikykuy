"""Point the next release to one reader for new and historical receipts."""
import copy
import hashlib
import json
from pathlib import Path, PurePosixPath

here = Path(__file__).resolve().parent
root = here.parents[1]
bindings = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/bindings.json').read_bytes())
current = json.loads(Path(bindings['remote_config']).read_bytes())
prepared = json.loads((here / 'prepared-server.json').read_bytes())
verified = json.loads((here / 'verified-server.json').read_bytes())
registry = json.loads(Path(bindings['registry_path']).read_bytes())
bundle = json.loads(Path(bindings['bundle_path']).read_bytes())
index = json.loads(Path(bundle['index_path']).read_bytes())
old_sha = current['transport_config']['service_config_sha256']
old_row = next(row for row in registry['versions'] if row['service_config_sha256'] == old_sha)
old_index = next(row for row in index['versions'] if row['canonical_sha256'] == old_row['canonical_sha256'])
new_app = str(PurePosixPath(prepared['config_path']).parent)
new_row = dict(old_row, canonical_sha256=prepared['config_sha256'], service_config=prepared['config_path'],
               service_config_sha256=prepared['config_sha256'], rpc_script=str(PurePosixPath(new_app) / 'server_rpc.py'))
registry['versions'] = [new_row]
index['versions'] = [dict(canonical_sha256=prepared['config_sha256'], app=new_app,
                          manifest_path=verified['guard_manifest'], manifest_sha256=verified['guard_sha256'])]

def save(name, value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf8')
    path = here / name
    path.write_bytes(raw)
    return str(path), hashlib.sha256(raw).hexdigest()

registry_path, registry_sha = save('single-reader-registry.json', registry)
index_path, index_sha = save('single-reader-index.json', index)
old_unread_path, old_unread_sha = bundle['unread_path'], bundle['unread_sha256']
bundle.update(index_path=index_path, index_sha256=index_sha, unread_path=prepared['unread_path'],
              unread_sha256=prepared['unread_sha256'])
bundle_path, bundle_sha = save('single-reader-bundle.json', bundle)
new_config = copy.deepcopy(current)
transport = new_config['transport_config']
transport.update(service_config=prepared['config_path'], service_config_sha256=prepared['config_sha256'],
                 rpc_script=new_row['rpc_script'], staging_script=str(PurePosixPath(new_app) / 'server_staging_cli.py'))
encoded = json.dumps(transport, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf8')
new_config['transport_config_sha256'] = hashlib.sha256(encoded).hexdigest()
new_config['snapshot_options']['code_sha256'] = prepared['code_sha256']
for name in ('live_runtime.py', 'stage_runtime.py'):
    new_config['runtime_sources'][name] = hashlib.sha256((here / name).read_bytes()).hexdigest()
config_path, config_sha = save('remote-config-next.json', new_config)

source = (here / 'server-gate-current.sh').read_text(encoding='utf8')
old_revision = str(PurePosixPath(old_row['service_config']).parent.parent)
new_revision = str(PurePosixPath(prepared['config_path']).parent.parent)
lines = [line for line in source.splitlines() if old_revision + '/' in line]
if len(lines) != 11:
    raise ValueError('the current executor routes changed')
added = [line.replace(old_revision, new_revision).replace(old_sha, prepared['config_sha256'])
         .replace(old_index['manifest_sha256'], verified['guard_sha256'])
         .replace(old_unread_path, prepared['unread_path']).replace(old_unread_sha, prepared['unread_sha256'])
         for line in lines]
if '  *) exit 126 ;;' not in source:
    raise ValueError('the executor route insertion point changed')
gate = source.replace('  *) exit 126 ;;', '\n'.join(added) + '\n  *) exit 126 ;;')
(here / 'server-gate-next.sh').write_text(gate, encoding='utf8', newline='\n')
save('candidate-bindings.json', dict(registry_path=registry_path, registry_sha256=registry_sha,
     bundle_path=bundle_path, bundle_sha256=bundle_sha, remote_config=config_path, remote_config_sha256=config_sha,
     gate_sha256=hashlib.sha256(gate.encode('utf8')).hexdigest()))
print(json.dumps({'readers': len(registry['versions']), 'history_configs': len(json.loads(
    Path(bindings['registry_path']).read_bytes())['versions']), 'new_routes': len(added),
    'live_service_changed': False, 'business_work_started': False}))
