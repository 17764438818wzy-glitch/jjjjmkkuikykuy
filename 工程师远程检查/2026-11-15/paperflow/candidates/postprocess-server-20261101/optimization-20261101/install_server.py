"""Prepare an independent server revision; do not start business work."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys


stage = Path(sys.argv[1])
old_config_path = Path(sys.argv[2])
old_config_sha = sys.argv[3]
revision_name = sys.argv[4]
old_raw = old_config_path.read_bytes()
if hashlib.sha256(old_raw).hexdigest() != old_config_sha:
    raise ValueError('the current server configuration changed')
old = json.loads(old_raw)
base = Path(old['base'])
revision = base / revision_name
business = stage / 'business-code'
business.mkdir()

manifest_raw = Path(old['release']['code_manifest']).read_bytes()
rows = json.loads(manifest_raw)
names = {'folder_processor.py', 'body_adapter.py', 'generation_attachment_adapter.py'}
replaced = set()
for row in rows:
    source = Path(old['release']['code_root']) / row['relative']
    raw = source.read_bytes()
    if len(raw) != row['bytes'] or hashlib.sha256(raw).hexdigest() != row['sha256']:
        raise ValueError('the existing business release changed')
    name = Path(row['relative']).name
    if name in names and '/candidates/postprocess-prefect-20261031/' in row['relative']:
        raw = (stage / name).read_bytes()
        row['bytes'], row['sha256'] = len(raw), hashlib.sha256(raw).hexdigest()
        replaced.add(name)
    target = business / row['relative']
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(raw)
if replaced != names:
    raise ValueError('the three business adapters were not found')
new_manifest_raw = json.dumps(rows, ensure_ascii=False, sort_keys=True,
                              separators=(',', ':'), allow_nan=False).encode('utf8')
business_manifest = stage / 'business-manifest.json'
business_manifest.write_bytes(new_manifest_raw)
business_sha = hashlib.sha256(new_manifest_raw).hexdigest()

sys.path.insert(0, str(stage))
import bootstrap_service_revision
replacements = {}
for name in ('server_release.py', 'server_job_service.py'):
    path = stage / name
    replacements[name] = {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
prepared = bootstrap_service_revision.prepare(str(old_config_path), old_config_sha, str(revision),
    str(business), str(business_manifest), business_sha, replacements=replacements)

template_manifest = Path(old['release']['template_manifest']).read_bytes()
template_inventory = json.loads(template_manifest)
template = Path(old['release']['template'])
shared = revision / 'shared-runtime'
shared.mkdir()
shared_roots = ('drive_c/windows/system32/', 'drive_c/windows/winsxs/', 'drive_c/Python/')
files = total = 0
for row in template_inventory['entries']:
    if row['kind'] != 'file' or not row['path'].startswith(shared_roots):
        continue
    source = template / row['path']
    target = shared / row['path']
    target.parent.mkdir(parents=True, exist_ok=True)
    digest, size = hashlib.sha256(), 0
    with source.open('rb') as inp, target.open('xb') as out:
        for data in iter(lambda: inp.read(1024 * 1024), b''):
            out.write(data)
            digest.update(data)
            size += len(data)
    if size != row['bytes'] or digest.hexdigest() != row['sha256']:
        raise ValueError('a shared runtime file differs from the installed template')
    os.chmod(target, row['mode'] & ~0o222)
    files += 1
    total += size

app = revision / 'app'
guard = dict(schema='paperflow-page-release/1', python=None, packages={}, files={})
for path in sorted(app.glob('*.py')):
    guard['files'][str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
guard_raw = json.dumps(guard, ensure_ascii=False, sort_keys=True,
                       separators=(',', ':'), allow_nan=False).encode('utf8')
guard_path = revision / 'page-app-manifest.json'
guard_path.write_bytes(guard_raw)
result = dict(config_path=prepared['config_path'], config_sha256=prepared['config_sha256'],
    business_sha256=business_sha, shared_files=files, shared_bytes=total,
    shared_path=str(shared), guard_manifest=str(guard_path),
    guard_sha256=hashlib.sha256(guard_raw).hexdigest(),
    rpc_script=str(app / 'server_rpc.py'),
    rpc_sha256=hashlib.sha256((app / 'server_rpc.py').read_bytes()).hexdigest(),
    staging_script=str(app / 'server_staging_cli.py'))
(stage / 'prepared.json').write_text(json.dumps(result, sort_keys=True), encoding='utf8')
print(json.dumps(result, sort_keys=True), flush=True)
