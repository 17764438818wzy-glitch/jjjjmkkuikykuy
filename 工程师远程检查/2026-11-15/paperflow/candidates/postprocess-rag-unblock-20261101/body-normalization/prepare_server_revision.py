"""Prepare the changed business file in a separate, inactive server revision."""
import hashlib
import json
import os
from pathlib import Path
import sys


stage, old_config, new_root = map(Path, sys.argv[1:4])
expected_config, expected_body = sys.argv[4:6]
old_raw = old_config.read_bytes()
if hashlib.sha256(old_raw).hexdigest() != expected_config:
    raise ValueError('server configuration changed')
old = json.loads(old_raw)
replacement = (stage / 'body_adapter.py').read_bytes()
if hashlib.sha256(replacement).hexdigest() != expected_body:
    raise ValueError('candidate body adapter changed')
rows = json.loads(Path(old['release']['code_manifest']).read_bytes())
business = stage / 'business-code'
business.mkdir()
replaced = 0
for row in rows:
    relative = Path(row['relative'])
    source = Path(old['release']['code_root']) / relative
    raw = source.read_bytes()
    if len(raw) != row['bytes'] or hashlib.sha256(raw).hexdigest() != row['sha256']:
        raise ValueError('existing business release changed')
    if row['relative'].endswith('/postprocess-prefect-20261031/body_adapter.py'):
        raw = replacement
        row['bytes'], row['sha256'] = len(raw), hashlib.sha256(raw).hexdigest()
        replaced += 1
    target = business / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(raw)
if replaced != 1:
    raise ValueError('body adapter identity is not unique')
manifest_raw = json.dumps(rows, ensure_ascii=False, sort_keys=True,
                          separators=(',', ':'), allow_nan=False).encode('utf8')
manifest = stage / 'business-manifest.json'
manifest.write_bytes(manifest_raw)
sys.path.insert(0, str(stage))
import bootstrap_service_revision
prepared = bootstrap_service_revision.prepare(str(old_config), expected_config, str(new_root),
    str(business), str(manifest), hashlib.sha256(manifest_raw).hexdigest())
shared = (old_config.parent.parent / 'shared-runtime').resolve(strict=True)
(new_root / 'shared-runtime').symlink_to(shared, target_is_directory=True)
result = dict(prepared, code_manifest=str(manifest), code_sha256=hashlib.sha256(manifest_raw).hexdigest(),
              body_sha256=expected_body, shared_path=str(shared), revision_selected=False)
(stage / 'prepared.json').write_text(json.dumps(result, sort_keys=True), encoding='utf8')
print(json.dumps(result, sort_keys=True), flush=True)
