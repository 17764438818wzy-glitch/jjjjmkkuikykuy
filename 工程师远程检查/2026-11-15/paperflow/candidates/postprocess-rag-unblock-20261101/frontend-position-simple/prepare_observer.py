import hashlib
import json
import os
from pathlib import Path

here = Path(__file__).resolve().parent
root = here.parents[2]
binding_path = root / 'candidates/postprocess-rag-unblock-20261101/bindings.json'
binding = json.loads(binding_path.read_text(encoding='utf8'))
old_manifest = Path(binding['release_inventory'])
old_bytes = old_manifest.read_bytes()
if hashlib.sha256(old_bytes).hexdigest() != binding['release_inventory_sha256']:
    raise ValueError('existing release inventory differs')
manifest = json.loads(old_bytes)
target = root / 'candidates/postprocess-prefect-20261031/server_release_next/ticket_positions.py'
key = os.path.normcase(os.path.abspath(target))
candidate = here / 'ticket_positions.py'
changed = [path for path, digest in manifest['files'].items()
           if hashlib.sha256(Path(path).read_bytes()).hexdigest() != digest]
if changed or key not in manifest['files']:
    raise ValueError('existing release sources differ from the approved inventory')
manifest['files'][key] = hashlib.sha256(candidate.read_bytes()).hexdigest()
raw = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf8')
manifest_path = here / 'release-inventory.json'
manifest_path.write_bytes(raw)
manifest_sha = hashlib.sha256(raw).hexdigest()
old_observer = Path(binding['observer']).read_text(encoding='utf8')
observer = old_observer.replace(binding['release_inventory'], str(manifest_path))
observer = observer.replace(binding['release_inventory_sha256'], manifest_sha)
if observer == old_observer or binding['release_inventory'] in observer:
    raise ValueError('observer release binding was not replaced')
observer_path = here / 'observer.ps1'
observer_path.write_text(observer, encoding='utf8')
new = dict(binding, release_inventory=str(manifest_path), release_inventory_sha256=manifest_sha,
           observer=str(observer_path), observer_sha256=hashlib.sha256(observer_path.read_bytes()).hexdigest())
(here / 'bindings.json').write_text(json.dumps(new, ensure_ascii=False, sort_keys=True), encoding='utf8')
print(json.dumps({'approved_files_checked': len(manifest['files']), 'changed_sources': 1,
                  'new_manifest_sha256': manifest_sha, 'prepared_only': True}, ensure_ascii=False))
