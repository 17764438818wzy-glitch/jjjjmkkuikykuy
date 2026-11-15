import hashlib
import json
from pathlib import Path


here = Path(__file__).resolve().parent
parent = here.parent
old = json.loads((parent / 'bindings.json').read_bytes())
old_observer = (parent / 'observer.ps1').read_text(encoding='utf8')
previous = json.loads((here.parents[1] / 'postprocess-server-20261101/optimization-20261101/final/bindings.json').read_bytes())
new = json.loads((here / 'candidate-bindings.json').read_bytes())
replacements = {
    old['remote_config']: new['remote_config'], old['remote_config_sha256']: new['remote_config_sha256'],
    previous['registry_path']: new['registry_path'], previous['registry_sha256']: new['registry_sha256'],
    previous['bundle_path']: new['bundle_path'], previous['bundle_sha256']: new['bundle_sha256'],
}
for before, after in replacements.items():
    if before not in old_observer:
        raise ValueError('observer binding changed')
    old_observer = old_observer.replace(before, after)
observer = here / 'observer.ps1'
observer.write_bytes(old_observer.encode('utf8'))
updated = dict(old, **new, observer=str(observer), observer_sha256=hashlib.sha256(observer.read_bytes()).hexdigest())
(here / 'bindings-mainline.json').write_text(json.dumps(updated, ensure_ascii=False, sort_keys=True), encoding='utf8')
access = (parent / 'receipt_access.py').read_text(encoding='utf8')
before = "previous = json.loads((PREVIOUS / 'bindings.json').read_bytes())"
if access.count(before) != 1:
    raise ValueError('receipt access source changed')
(here / 'receipt_access.py').write_bytes(access.replace(before, 'previous = binding').encode('utf8'))
print(json.dumps({'prepared_only': True, 'observer_sha256': updated['observer_sha256'],
                  'source_receipt_access_preserved': True}))
