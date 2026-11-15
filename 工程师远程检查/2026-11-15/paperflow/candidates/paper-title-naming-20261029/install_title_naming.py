"""Prepare and install the reviewed title-naming modules without changing data."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
from datetime import datetime


CANDIDATE = Path(__file__).resolve().parent
LIVE = CANDIDATE.parent.parent / 'prefect-paperflow'
FILES = [
    'paperflow_prefect/attachment_ingest.py',
    'paperflow_prefect/attachment_publish.py',
    'paperflow_prefect/attachments.py',
    'paperflow_prefect/backend.py',
    'paperflow_prefect/body.py',
    'paperflow_prefect/filename_title.py',
    'paperflow_prefect/flows.py',
    'paperflow_prefect/generated_title_journal.py',
    'paperflow_prefect/group_classification.py',
    'paperflow_prefect/group_evidence.py',
    'paperflow_prefect/group_sort_cli.py',
    'paperflow_prefect/intake_handoff.py',
    'paperflow_prefect/named_delivery.py',
    'paperflow_prefect/nimble_group.py',
    'paperflow_prefect/resources.py',
    'paperflow_prefect/stage3_handoff.py',
    'paperflow_prefect/unified.py',
    'paperflow_prefect/unified_attachments.py',
    'paperflow_prefect/unified_flows.py',
    'paperflow_prefect/workflow.py',
    'title_llm_supplement.py',
]


def sha(path):
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_manifest(path, data):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf8')
    os.replace(temporary, path)


def prepare():
    old_manifest = json.loads((CANDIDATE / 'candidate-change-manifest-v3.json').read_text(encoding='utf8'))
    baseline = {item['relative']: item['live_sha256_at_snapshot'] for item in old_manifest['files']}
    config_hash = sha(LIVE / 'config.json')
    if config_hash != old_manifest['live_config_sha256_at_snapshot']:
        raise RuntimeError('Live config changed since review; refusing to install.')
    root = LIVE / 'runtime' / 'installations' / ('title-naming-' + datetime.now().strftime('%Y%m%d-%H%M%S-%f'))
    root.mkdir(parents=True, exist_ok=False)
    entries = []
    for name in FILES:
        live = LIVE / name
        candidate = CANDIDATE / name
        old_hash, new_hash = sha(live), sha(candidate)
        if new_hash is None:
            raise RuntimeError('Reviewed candidate missing: ' + name)
        if name in baseline and baseline[name] != old_hash:
            raise RuntimeError('Live source changed since review: ' + name)
        if old_hash == new_hash:
            raise RuntimeError('Expected a changed source file: ' + name)
        staged = root / 'staged' / name
        staged.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(candidate, staged)
        if sha(staged) != new_hash:
            raise RuntimeError('Staged copy hash mismatch: ' + name)
        if old_hash is not None:
            backup = root / 'backup' / name
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(live, backup)
            if sha(backup) != old_hash:
                raise RuntimeError('Backup hash mismatch: ' + name)
        entries.append({'relative': name, 'old_sha256': old_hash, 'new_sha256': new_hash})
    manifest = {'status': 'prepared', 'config_sha256': config_hash, 'files': entries,
                'candidate': str(CANDIDATE), 'live': str(LIVE)}
    write_manifest(root / 'manifest.json', manifest)
    print(str(root))


def apply(root):
    root = Path(root).resolve()
    allowed = (LIVE / 'runtime' / 'installations').resolve()
    if root.parent != allowed or not root.name.startswith('title-naming-'):
        raise RuntimeError('Unexpected installation directory')
    path = root / 'manifest.json'
    data = json.loads(path.read_text(encoding='utf8'))
    if data['status'] != 'prepared' or [x['relative'] for x in data['files']] != FILES:
        raise RuntimeError('Installation manifest mismatch')
    if sha(LIVE / 'config.json') != data['config_sha256']:
        raise RuntimeError('Live config changed during preparation')
    for item in data['files']:
        name = item['relative']
        if sha(LIVE / name) != item['old_sha256'] or sha(root / 'staged' / name) != item['new_sha256']:
            raise RuntimeError('Source changed during preparation: ' + name)
        if sha(CANDIDATE / name) != item['new_sha256']:
            raise RuntimeError('Candidate changed during preparation: ' + name)
        if item['old_sha256'] is not None and sha(root / 'backup' / name) != item['old_sha256']:
            raise RuntimeError('Backup changed during preparation: ' + name)
    installed = []
    try:
        for item in data['files']:
            name = item['relative']
            os.replace(root / 'staged' / name, LIVE / name)
            installed.append(item)
            if sha(LIVE / name) != item['new_sha256']:
                raise RuntimeError('Installed hash mismatch: ' + name)
    except BaseException:
        for item in reversed(installed):
            name = item['relative']
            destination = LIVE / name
            if item['old_sha256'] is None:
                destination.unlink()
            else:
                temporary = root / 'restore' / name
                temporary.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(root / 'backup' / name, temporary)
                os.replace(temporary, destination)
        data['status'] = 'rolled_back_after_error'
        write_manifest(path, data)
        raise
    data['status'] = 'installed'
    data['installed_at'] = datetime.now().isoformat()
    write_manifest(path, data)
    print('Installed', len(installed), 'reviewed files; backup:', root / 'backup')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('operation', choices=['prepare', 'apply'])
    parser.add_argument('--install-dir')
    args = parser.parse_args()
    if args.operation == 'prepare':
        prepare()
    elif not args.install_dir:
        parser.error('--install-dir is required')
    else:
        apply(args.install_dir)
