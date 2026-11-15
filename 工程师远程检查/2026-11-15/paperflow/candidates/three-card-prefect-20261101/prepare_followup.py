"""Prepare the display-only stopped-retry adjustment against the installed release."""
from pathlib import Path
import ast
import hashlib
import json
import shutil

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
RELEASE = ROOT / 'candidates/postprocess-prefect-20261031/server_release_next'
DEPLOY = ROOT / 'candidates/postprocess-prefect-20261031/deployment-live-20261101'
INTAKE = ROOT / 'candidates/local-intake-live-link-20261031'
OLD_MANIFEST = RELEASE / 'release-inventory-three-card-20261101.json'
OLD_WRAPPER = DEPLOY / 'run_mainline_observer_three_card_20261101.ps1'
NEW_MANIFEST = 'release-inventory-three-card-v2-20261101.json'
BACKUP = HERE / 'before-stopped-retry'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    ast.parse((HERE / 'llm_queue_summary.py').read_text(encoding='utf-8'))
    manifest = json.loads(OLD_MANIFEST.read_text(encoding='utf-8'))
    bad = [name for name, expected in manifest['files'].items()
           if not Path(name).is_file() or digest(Path(name)) != expected]
    if bad:
        raise RuntimeError(f'Installed inventory differs for {len(bad)} files: {bad[:5]}')
    target = str((INTAKE / 'llm_queue_summary.py').resolve()).lower()
    manifest['files'][target] = digest(HERE / 'llm_queue_summary.py')
    output = HERE / NEW_MANIFEST
    output.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    wrapper = OLD_WRAPPER.read_text(encoding='utf-8-sig')
    old_hash, new_hash = digest(OLD_MANIFEST), digest(output)
    if wrapper.count(OLD_MANIFEST.name) != 1 or wrapper.count(old_hash) != 1:
        raise RuntimeError('Installed observer wrapper does not match its manifest')
    wrapper = wrapper.replace(OLD_MANIFEST.name, NEW_MANIFEST).replace(old_hash, new_hash)
    (HERE / 'observer-wrapper-v2.ps1').write_text(wrapper, encoding='utf-8', newline='\n')
    BACKUP.mkdir(exist_ok=True)
    source = INTAKE / 'llm_queue_summary.py'
    if (BACKUP / source.name).exists():
        raise RuntimeError('Follow-up backup already exists')
    shutil.copy2(source, BACKUP / source.name)
    baseline = {str(path): digest(path) for path in (source, OLD_MANIFEST, OLD_WRAPPER)}
    (HERE / 'followup-baseline.json').write_text(json.dumps(baseline, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'verified_files': len(manifest['files']), 'manifest_sha256': new_hash,
                      'wrapper_sha256': digest(HERE / 'observer-wrapper-v2.ps1')}))


if __name__ == '__main__':
    main()
