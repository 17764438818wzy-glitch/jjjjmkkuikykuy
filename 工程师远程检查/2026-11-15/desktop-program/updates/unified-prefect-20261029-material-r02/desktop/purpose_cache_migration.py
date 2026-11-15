"""Verified cross-volume cache relocation with unchanged historical references."""
from __future__ import annotations

import os
from pathlib import Path
import shutil
import threading
import time

from preextract_material import write_json
from purpose_cache_cleanup import read, sha
from paperflow.cleanup_paths import inventory, delete_inventory


def create_junction(source, target):
    if os.name == 'nt':
        import _winapi
        _winapi.CreateJunction(str(target), str(source))
    else:
        os.symlink(target, source, target_is_directory=True)


def _plain(path):
    path = Path(path).absolute()
    for p in (path, *path.parents):
        if p.is_symlink() or p.is_junction():
            raise ValueError('migration links forbidden')
    return path.resolve()


def _matches(path, expected, *, subset=False):
    actual = inventory(path, [path.parent], [])
    if subset:
        valid = all(expected['files'].get(k) == v for k, v in actual['files'].items())
        valid = valid and set(actual['directories']) <= set(expected['directories'])
    else:
        valid = actual['files'] == expected['files'] and set(actual['directories']) == set(expected['directories'])
    if not valid:
        raise ValueError('migration content or entry set changed')
    return actual


def _disposable(name):
    p = Path(name); parts = p.parts
    if len(parts) < 4 or parts[0] != 'reviews' or len(parts[1]) != 64:
        return False
    if parts[2] == 'model':
        return len(parts) == 4 and p.name in {'request.json', 'planned-request.json', 'response-events.jsonl'}
    return (parts[2] == 'evidence' and ('converted' in parts[3:] or
            p.suffix.lower() in {'.png', '.jpg', '.jpeg', '.webp', '.bmp', '.tif', '.tiff'}))


def _verify_retained(target, expected):
    for name, meta in expected['files'].items():
        if _disposable(name) or Path(name).name.endswith('.lock') or name == 'generation.json':
            continue
        p = _plain(target / name)
        if not p.is_file() or p.stat().st_size != meta['bytes'] or sha(p) != meta['sha256']:
            raise ValueError('migrated retained answer or receipt changed')


def migrate_cache(source, target, journal, *, admit):
    """The installer owns GUI/worker locks. False admission performs no writes."""
    if not admit():
        return {'status': 'waiting', 'reason': 'owners_or_peer_not_ready'}
    source, target, journal = Path(source).absolute(), Path(target).absolute(), Path(journal).absolute()
    backup = source.with_name(source.name + '.migration-backup')
    state = None
    try:
        if source == target or source.is_relative_to(target) or target.is_relative_to(source):
            raise ValueError('migration roots overlap')
        if source == source.parent or target == target.parent:
            raise ValueError('migration root is a volume')
        _plain(source.parent); _plain(target)
        if journal.exists():
            state = read(journal)
            if state.get('source') != str(source) or state.get('target') != str(target):
                raise ValueError('migration intent roots changed')
            if state.get('schema') != 'purpose-cache-migration/1':
                raise ValueError('unsupported migration intent')
        if source.is_junction() or source.is_symlink():
            if source.resolve() != target.resolve() or not state:
                raise ValueError('unexpected existing cache junction')
        elif not source.exists() and not (state and backup.is_dir()):
            raise ValueError('source cache missing')
        elif source.exists():
            _plain(source)
        manifest_path = journal.with_name(journal.stem + '-inventory.json')
        if not state:
            if target.exists() and any(target.iterdir()):
                raise ValueError('destination is not an empty owned migration target')
            expected = inventory(source, [source.parent], [])
            if any(name.endswith('.purpose-migrating') for name in expected['files']):
                raise ValueError('source conflicts with migration temporary names')
            size = sum(v['bytes'] for v in expected['files'].values())
            probe = target.parent
            while not probe.exists():
                probe = probe.parent
            if shutil.disk_usage(probe).free < size + 5 * 1024**3:
                raise ValueError('destination reserve insufficient')
            journal.parent.mkdir(parents=True, exist_ok=True)
            write_json(manifest_path, expected)
            state = {'schema': 'purpose-cache-migration/1', 'source': str(source), 'target': str(target),
                     'status': 'copying', 'inventory': str(manifest_path), 'inventory_sha256': sha(manifest_path),
                     'source_bytes': size, 'source_files': len(expected['files']), 'created_at': time.time()}
            write_json(journal, state)
        if state['inventory'] != str(manifest_path) or sha(manifest_path) != state['inventory_sha256']:
            raise ValueError('migration inventory changed')
        expected = read(manifest_path)
        if expected['path'] != str(source.resolve()) and not (source.is_junction() or not source.exists()):
            raise ValueError('migration source identity changed')
        for name in [*expected['files'], *expected['directories']]:
            p = Path(name)
            if p.is_absolute() or '..' in p.parts or str(p) in {'.', ''}:
                raise ValueError('migration inventory path escape')
        if state.get('status') == 'complete':
            _verify_retained(target, expected)
            return dict(state, status='already_complete', retained_reverified_at=time.time())
        if not source.is_junction() and not source.is_symlink() and source.exists():
            _matches(source, expected)
            target.mkdir(parents=True, exist_ok=True)
            # The target was empty at intent creation. Unexpected paths are never overwritten.
            allowed = set(expected['files'])
            for p in target.rglob('*'):
                _plain(p)
                if p.is_file():
                    name = p.relative_to(target).as_posix()
                    if name not in allowed and not (name.endswith('.purpose-migrating') and name[:-18] in allowed):
                        raise ValueError('unexpected destination file')
            for name in expected['directories']:
                (target / name).mkdir(parents=True, exist_ok=True)
            for name, meta in expected['files'].items():
                origin, destination = source / name, target / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                if destination.exists():
                    if sha(destination) == meta['sha256'] and destination.stat().st_size == meta['bytes']:
                        continue
                    raise ValueError('existing destination file changed')
                temporary = destination.with_name(destination.name + '.purpose-migrating')
                _plain(temporary)
                shutil.copy2(origin, temporary)
                if sha(temporary) != meta['sha256'] or temporary.stat().st_size != meta['bytes']:
                    raise ValueError('copied file failed hash verification')
                os.replace(temporary, destination)
            _matches(target, expected); _matches(source, expected)
            if not admit():
                state.update(status='copy_verified_waiting', checked_at=time.time()); write_json(journal, state)
                return {'status': 'waiting', 'reason': 'admission_changed_after_copy'}
            state.update(status='copy_verified', verified_at=time.time()); write_json(journal, state)
            if backup.exists():
                raise ValueError('source backup already exists')
            os.replace(source, backup)
        if not source.exists():
            _matches(target, expected); _matches(backup, expected, subset=True)
            try:
                create_junction(source, target)
                if source.resolve() != target.resolve():
                    raise ValueError('compatibility junction points elsewhere')
            except BaseException:
                if not source.exists() and backup.exists():
                    os.replace(backup, source)
                raise
        if not (source.is_junction() or source.is_symlink()) or source.resolve() != target.resolve():
            raise ValueError('compatibility path is not verified')
        _matches(target, expected)
        state.update(status='linked', linked_at=time.time()); write_json(journal, state)
        if not admit():
            return {'status': 'waiting', 'reason': 'admission_changed_before_copy_retirement'}
        if backup.exists():
            # Only the saved same-byte duplicate is retired. Business cache remains on E.
            _plain(backup); _matches(backup, expected, subset=True)
            resource = dict(expected, path=str(backup.resolve()))
            state.update(status='retiring_verified_duplicate'); write_json(journal, state)
            delete_inventory(resource, [source.parent], [], threading.Event())
        state.update(status='complete', completed_at=time.time(), removed_source_copy_bytes=state['source_bytes'],
                     original_receipt_bytes_rewritten=0, business_cache_deleted=False)
        write_json(journal, state)
        return state
    except Exception as exc:
        if state:
            state.update(last_error_type=type(exc).__name__, last_error=str(exc), failed_at=time.time())
            write_json(journal, state)
        return {'status': 'blocked', 'reason': str(exc), 'error_type': type(exc).__name__}
