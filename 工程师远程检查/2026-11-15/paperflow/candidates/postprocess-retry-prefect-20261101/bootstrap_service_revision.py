"""Prepare a distinct server app revision against the existing execution ledger.

Called explicitly on the Linux server. It never starts a job, alters the old
app or initializes/replaces the ledger. The requested new directory must not
exist, so a failed preparation remains an inert, separately inspectable record.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import sqlite3
import stat
import sys


def require(value, reason):
    if not value:
        raise ValueError(reason)


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def regular(path, max_bytes=64 * 1024 * 1024):
    path = Path(path)
    require(path.is_absolute() and '..' not in path.parts, 'absolute input required')
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= max_bytes,
            'independent regular input required')
    with path.open('rb') as stream:
        opened = os.fstat(stream.fileno())
        require((opened.st_dev, opened.st_ino, opened.st_size) == (info.st_dev, info.st_ino, info.st_size),
                'input changed on open')
        raw = stream.read(max_bytes + 1)
        after = os.fstat(stream.fileno())
    require(len(raw) <= max_bytes and (opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns)
            == (after.st_size, after.st_mtime_ns, after.st_ctime_ns), 'input changed during read')
    return raw


def write_once(path, raw, mode=0o600):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def safe_revision_target(new_root, base, old_app, jobs, staging):
    new_root, base, old_app, jobs, staging = map(Path, (new_root, base, old_app, jobs, staging))
    require(new_root.is_absolute() and '..' not in new_root.parts and not new_root.exists()
            and not new_root.is_symlink(), 'new revision root must be fresh')
    parent = new_root.parent.resolve(strict=True)
    require(parent == new_root.parent and parent.is_relative_to(base.resolve(strict=True)),
            'revision parent must be canonical and inside the runtime')
    target = parent / new_root.name
    for protected in (old_app, jobs, staging):
        protected = protected.resolve(strict=True)
        require(not target.is_relative_to(protected) and not protected.is_relative_to(target),
                'revision cannot overlap existing app, job or staging data')
    return target


def verified_source_replacements(replacements):
    """Read one complete, separately pinned capacity-reconciliation revision."""
    if replacements is None:
        return {}
    names = {'remote_jobs.py', 'server_job_service.py', 'server_termination_proof.py', 'server_admission.py',
             'server_receipt_query.py'}
    require(isinstance(replacements, dict) and set(replacements) == names,
            'all and only the reviewed capacity source replacements are required')
    result = {}
    for name, spec in replacements.items():
        require(isinstance(spec, dict) and set(spec) == {'path', 'sha256'}
                and Path(spec['path']).name == name, 'invalid replacement source descriptor')
        raw = regular(spec['path'])
        require(sha(raw) == spec['sha256'], 'replacement source bytes changed')
        result[name] = raw
    return result


def prepare(old_config_path, old_config_sha, new_root, code_root, code_manifest, code_sha, replacements=None):
    old_config_path, new_root, code_root, code_manifest = map(Path,
        (old_config_path, new_root, code_root, code_manifest))
    old_raw = regular(old_config_path, 65536)
    require(sha(old_raw) == old_config_sha and old_config_path.name == 'server-job-service.json'
            and regular(old_config_path.with_suffix('.sha256'), 128).decode().strip() == old_config_sha,
            'the original app default config is not independently pinned')
    old = json.loads(old_raw)
    base, old_app = Path(old['base']).resolve(strict=True), old_config_path.parent.resolve(strict=True)
    replacement_bytes = verified_source_replacements(replacements)
    if replacements is not None:
        require(all(Path(spec['path']).resolve(strict=True).is_relative_to(base) for spec in replacements.values()),
                'replacement source must be inside the existing runtime')
    new_root = safe_revision_target(new_root, base, old_app, old['jobs_root'], old['staging_root'])
    code_root, code_manifest = code_root.resolve(strict=True), code_manifest.resolve(strict=True)
    require(code_root.is_relative_to(base) and code_manifest.is_relative_to(base), 'release must be under the runtime')
    release_raw = regular(code_manifest)
    require(sha(release_raw) == code_sha, 'new business manifest is not pinned')
    rows = json.loads(release_raw)
    require(isinstance(rows, list) and rows, 'new business inventory is empty')
    seen = set()
    for row in rows:
        relative = PurePosixPath(row['relative'])
        require(not relative.is_absolute() and all(part not in ('', '.', '..') for part in relative.parts)
                and row['relative'] not in seen, 'invalid business inventory path')
        seen.add(row['relative'])
        raw = regular(code_root.joinpath(*relative.parts))
        require(len(raw) == row['bytes'] and sha(raw) == row['sha256'], 'new business release changed')
    native_raw = regular(old['native_manifest'])
    require(sha(native_raw) == old['native_manifest_sha256'], 'original native inventory changed')
    inventory = json.loads(native_raw)
    app_files = [row for row in inventory['files'] if Path(row['path']).parent == old_app]
    require(app_files and all(Path(row['path']).suffix == '.py' for row in app_files),
            'original app inventory is incomplete')
    require(len({Path(row['path']).name for row in app_files}) == len(app_files), 'duplicate app filenames')
    require(Path(inventory['roles']['service_script']).parent == old_app
            and Path(inventory['roles']['worker_script']).parent == old_app, 'original app roles changed')

    app_sources = {}
    for row in app_files:
        raw = regular(row['path'])
        require(len(raw) == row['bytes'] and sha(raw) == row['sha256'], 'original app source changed')
        app_sources[Path(row['path']).name] = raw
    require(not replacement_bytes or {'remote_jobs.py', 'server_job_service.py'} <= set(app_sources),
            'the original capacity and service modules are unavailable')
    app_sources.update(replacement_bytes)
    os.mkdir(new_root, 0o700)
    new_app = new_root / 'app'
    os.mkdir(new_app, 0o700)
    for name, raw in app_sources.items():
        write_once(new_app / name, raw)
    revised = copy.deepcopy(inventory)
    revised['files'] = [row for row in revised['files'] if Path(row['path']).parent != old_app]
    revised['files'].extend(dict(path=str(new_app / name), bytes=len(raw), sha256=sha(raw))
                            for name, raw in app_sources.items())
    for role, path in revised['roles'].items():
        if Path(path).parent == old_app:
            revised['roles'][role] = str(new_app / Path(path).name)
    revised['files'].sort(key=lambda row: row['path'])
    native_bytes = encode(revised)
    native_path = new_root / 'native-inventory.json'
    write_once(native_path, native_bytes)
    config = copy.deepcopy(old)
    config['native_manifest'] = str(native_path)
    config['native_manifest_sha256'] = sha(native_bytes)
    config['release']['code_root'] = str(code_root)
    config['release']['code_manifest'] = str(code_manifest)
    config_bytes = encode(config)
    config_path = new_app / 'server-job-service.json'
    write_once(config_path, config_bytes)
    write_once(config_path.with_suffix('.sha256'), sha(config_bytes).encode('ascii'))

    # Import from the NEW app only: preflight later compares its own default.
    sys.dont_write_bytecode = True
    module_names = {Path(name).stem for name in app_sources}
    require(not any(name in sys.modules for name in module_names),
            'a fresh process without cached service modules is required')
    sys.path.insert(0, str(new_app))
    import server_job_service as service
    require(Path(service.__file__).resolve() == (new_app / 'server_job_service.py').resolve(), 'wrong app imported')
    for name in module_names & sys.modules.keys():
        require(Path(getattr(sys.modules[name], '__file__', '')).resolve() == (new_app / (name + '.py')).resolve(),
                'cached service dependency belongs to another app')
    verified = service.load_config(str(config_path), sha(config_bytes))
    service._assert_installed_config(verified)
    service.verify_native_runtime(verified)
    database = Path(old['ledger']['database'])
    with sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=5) as db:
        settings = dict(db.execute('SELECT name,value FROM executor_settings'))
        occupied = db.execute("SELECT count(*) FROM executor_jobs WHERE state IN ('reserved','running','unknown')").fetchone()[0]
    require(settings == {'capacity': str(old['ledger']['capacity']),
                         'ledger_id': old['server_identity']['ledger_id']}, 'original ledger identity changed')
    return dict(config_path=str(config_path), config_sha256=sha(config_bytes),
                server_identity=old['server_identity'], capacity=old['ledger']['capacity'],
                occupied=occupied, app_files=len(app_sources), business_files=len(rows),
                old_config_preserved=True, existing_ledger_reused=True, business_work_started=False)


def main():
    if len(sys.argv) not in (7, 9):
        raise SystemExit('old config, old SHA, new root, code root, code manifest, code SHA; optional replacement file and SHA required')
    replacements = None
    if len(sys.argv) == 9:
        raw = regular(sys.argv[7], 65536)
        require(sha(raw) == sys.argv[8], 'replacement descriptor changed')
        replacements = json.loads(raw)
    print(json.dumps(prepare(*sys.argv[1:7], replacements=replacements), ensure_ascii=False, sort_keys=True))


if __name__ == '__main__':
    main()
