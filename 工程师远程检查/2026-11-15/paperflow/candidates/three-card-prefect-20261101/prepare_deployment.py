"""Prepare only the read-only three-card display and observer manifest."""
from pathlib import Path
import hashlib
import json
import shutil

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
RELEASE = ROOT / 'candidates/postprocess-prefect-20261031/server_release_next'
SITE = RELEASE / 'site-live-release-20261101'
DEPLOY = ROOT / 'candidates/postprocess-prefect-20261031/deployment-live-20261101'
INTAKE = ROOT / 'candidates/local-intake-live-link-20261031'
OLD_MANIFEST = RELEASE / 'release-inventory-prefect-positions-20261101.json'
NEW_MANIFEST_NAME = 'release-inventory-three-card-20261101.json'
OLD_WRAPPER = DEPLOY / 'run_mainline_observer_prefect_positions_20261101.ps1'
BACKUP = HERE / 'before-deployment'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def one_replace(text, old, new):
    if text.count(old) != 1:
        raise RuntimeError('Expected exactly one matching site or wrapper anchor')
    return text.replace(old, new, 1)


def main():
    manifest = json.loads(OLD_MANIFEST.read_text(encoding='utf-8'))
    mismatches = []
    for name, expected in manifest['files'].items():
        path = Path(name)
        if not path.is_file() or digest(path) != expected:
            mismatches.append(name)
    if mismatches:
        raise RuntimeError(f'Current release inventory differs for {len(mismatches)} file(s): {mismatches[:5]}')
    for name in ('llm_queue_summary.py', 'waiting_api.py'):
        target = str((INTAKE / name).resolve()).lower()
        if target not in manifest['files']:
            raise RuntimeError(f'{name} is missing from current release inventory')
        manifest['files'][target] = digest(HERE / name)
    manifest_path = HERE / NEW_MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + '\n', encoding='utf-8')

    wrapper = OLD_WRAPPER.read_text(encoding='utf-8-sig')
    wrapper = one_replace(wrapper, OLD_MANIFEST.name, NEW_MANIFEST_NAME)
    wrapper = one_replace(wrapper, digest(OLD_MANIFEST), digest(manifest_path))
    (HERE / 'observer-wrapper.ps1').write_text(wrapper, encoding='utf-8', newline='\n')

    index = (SITE / 'index.html').read_text(encoding='utf-8')
    old_script = '<script src="prefect-ticket-counts-server-20261101.js?v=20261101-ticket-positions-1" defer></script>'
    new_scripts = ('<script src="three-card-positions-20261101.js?v=20261101-1" defer></script>\n'
                   '  <script src="prefect-ticket-counts-server-20261101.js?v=20261101-three-card-1" defer></script>')
    index = one_replace(index, old_script, new_scripts)
    anchor = '          <div><span class="metric-label">附件识别</span>'
    insertion = ('          <div><span class="metric-label">处理中工单</span> '
                 '<strong id="llm-folder-count" class="metric-value" '
                 'aria-label="LLM 处理中工单数量：数据待接入">—</strong></div>\n' + anchor)
    index = one_replace(index, anchor, insertion)
    (HERE / 'index.html').write_text(index, encoding='utf-8', newline='\n')

    # The candidate JS was assembled from the other window's current release.
    # Recheck its baseline at installation so neither window overwrites the other.
    BACKUP.mkdir(exist_ok=True)
    tracked = {
        'llm_queue_summary.py': INTAKE / 'llm_queue_summary.py',
        'waiting_api.py': INTAKE / 'waiting_api.py',
        'index.html': SITE / 'index.html',
        'prefect-ticket-counts.js': SITE / 'prefect-ticket-counts-server-20261101.js',
    }
    if any((BACKUP / name).exists() for name in tracked):
        raise RuntimeError('Deployment backup already exists; inspect before repeating preparation')
    for name, source in tracked.items():
        shutil.copy2(source, BACKUP / name)
    expected = {str(path): digest(path) for path in [*tracked.values(), OLD_MANIFEST, OLD_WRAPPER]}
    (HERE / 'deployment-baseline.json').write_text(json.dumps(expected, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'inventory_files_verified': len(manifest['files']),
                      'new_manifest_sha256': digest(manifest_path),
                      'new_wrapper_sha256': digest(HERE / 'observer-wrapper.ps1')}, ensure_ascii=False))


if __name__ == '__main__':
    main()
