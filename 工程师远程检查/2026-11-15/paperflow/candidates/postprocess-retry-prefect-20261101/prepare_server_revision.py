"""Prepare an inactive server revision; do not change the running service."""
import hashlib
import json
from pathlib import Path
import sys

stage, old_config, revision = map(Path, sys.argv[1:4])
expected_config, expected_code = sys.argv[4:6]
old_raw = old_config.read_bytes()
if hashlib.sha256(old_raw).hexdigest() != expected_config:
    raise ValueError('server configuration changed')
old = json.loads(old_raw)
manifest_raw = Path(old['release']['code_manifest']).read_bytes()
if hashlib.sha256(manifest_raw).hexdigest() != expected_code:
    raise ValueError('server business release changed')
rows = json.loads(manifest_raw)
business = stage / 'business-code'
business.mkdir()
replaced = 0
for row in rows:
    relative = Path(row['relative'])
    raw = (Path(old['release']['code_root']) / relative).read_bytes()
    if len(raw) != row['bytes'] or hashlib.sha256(raw).hexdigest() != row['sha256']:
        raise ValueError('server business file changed')
    if row['relative'].endswith('/postprocess-prefect-20261031/body_adapter.py'):
        raw = (stage / 'body_adapter.py').read_bytes()
        row['bytes'], row['sha256'] = len(raw), hashlib.sha256(raw).hexdigest()
        replaced += 1
    target = business / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(raw)
if replaced != 1:
    raise ValueError('body adapter path is not unique')
relative = 'drives/f/论文预处理系统/预处理系统2/prefect-paperflow/paperflow_prefect/clean_overlay/preextract_contract.py'
if any(row['relative'] == relative for row in rows):
    raise ValueError('validator overlay already exists')
raw = (stage / 'preextract_contract.py').read_bytes()
target = business / relative
target.parent.mkdir(parents=True, exist_ok=True)
target.write_bytes(raw)
rows.append(dict(relative=relative, bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()))
rows.sort(key=lambda row: row['relative'])
manifest_raw = json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf8')
manifest = stage / 'business-manifest.json'
manifest.write_bytes(manifest_raw)
history = json.loads((stage / 'historical-configs.json').read_bytes())
for digest, name in history.items():
    if hashlib.sha256(Path(name).read_bytes()).hexdigest() != digest:
        raise ValueError('historical configuration changed')
query = stage / 'server_receipt_query.py'
source = query.read_text(encoding='utf8')
if source.count('HISTORICAL_CONFIGS = {}') != 1:
    raise ValueError('receipt reader source changed')
query.write_text(source.replace('HISTORICAL_CONFIGS = {}', 'HISTORICAL_CONFIGS = ' + repr(history)), encoding='utf8')
replacements = {}
for name in ('remote_jobs.py', 'server_receipt_query.py', 'server_job_service.py', 'server_termination_proof.py', 'server_admission.py'):
    path = stage / name
    if name not in ('remote_jobs.py', 'server_receipt_query.py'):
        path.write_bytes((old_config.parent / name).read_bytes())
    replacements[name] = dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())
sys.path.insert(0, str(stage))
import bootstrap_service_revision
prepared = bootstrap_service_revision.prepare(str(old_config), expected_config, str(revision),
    str(business), str(manifest), hashlib.sha256(manifest_raw).hexdigest(), replacements=replacements)
shared = (old_config.parent.parent / 'shared-runtime').resolve(strict=True)
(revision / 'shared-runtime').symlink_to(shared, target_is_directory=True)
unread = revision / 'server_unread_scope_query.py'
unread.write_bytes((stage / 'server_unread_scope_query.py').read_bytes())
prepared.update(code_manifest=str(manifest), code_sha256=hashlib.sha256(manifest_raw).hexdigest(),
                body_sha256=hashlib.sha256((stage / 'body_adapter.py').read_bytes()).hexdigest(),
                validator_sha256=hashlib.sha256((stage / 'preextract_contract.py').read_bytes()).hexdigest(),
                server_jobs_sha256=hashlib.sha256((stage / 'remote_jobs.py').read_bytes()).hexdigest(),
                server_query_sha256=hashlib.sha256(query.read_bytes()).hexdigest(),
                unread_path=str(unread), unread_sha256=hashlib.sha256(unread.read_bytes()).hexdigest(),
                shared_path=str(shared), revision_selected=False)
print(json.dumps(prepared, ensure_ascii=False, sort_keys=True), flush=True)
