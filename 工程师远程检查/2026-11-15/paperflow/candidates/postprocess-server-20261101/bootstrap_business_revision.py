"""Freeze a new business release with only the reviewed body adapter replaced.

All old files and their manifest are hash-checked and left untouched. This is
an explicit installer primitive: it does not execute or schedule any folder.
"""
import copy
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
import sys


def require(value, reason):
    if not value:
        raise ValueError(reason)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def regular(path, limit=64 * 1024 * 1024):
    path = Path(path)
    require(path.is_absolute() and '..' not in path.parts and path.resolve(strict=True) == path,
            'canonical absolute regular input required')
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= limit,
            'independent bounded regular input required')
    with path.open('rb') as stream:
        opened = os.fstat(stream.fileno())
        require((opened.st_dev, opened.st_ino, opened.st_size) == (info.st_dev, info.st_ino, info.st_size),
                'input changed before opening')
        raw = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
    require(len(raw) <= limit and (opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns)
            == (after.st_size, after.st_mtime_ns, after.st_ctime_ns), 'input changed while reading')
    return raw


def write_once(path, raw):
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0)
    with os.fdopen(os.open(path, flags, 0o600), 'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def prepare(base, old_root, old_manifest, old_sha, new_root, replacement, replacement_sha):
    base, old_root, old_manifest, new_root, replacement = map(Path,
        (base, old_root, old_manifest, new_root, replacement))
    require(base.is_absolute() and base.resolve(strict=True) == base and base.is_dir(),
            'canonical runtime base required')
    require(old_root.is_absolute() and old_root.resolve(strict=True) == old_root
            and old_root.is_relative_to(base) and old_root.is_dir(), 'old release root is unavailable')
    require(old_manifest.is_relative_to(base) and replacement.is_relative_to(base)
            and replacement.name == 'body_adapter.py', 'reviewed runtime inputs required')
    require(new_root.is_absolute() and new_root.parent == base and not new_root.exists()
            and not new_root.is_symlink(), 'new release must be a fresh direct child of the runtime')
    manifest_raw, patch = regular(old_manifest), regular(replacement)
    require(sha(manifest_raw) == old_sha and sha(patch) == replacement_sha,
            'independent release or replacement digest changed')
    rows = json.loads(manifest_raw)
    require(isinstance(rows, list) and rows, 'the old business inventory is empty')
    contents, names, sources, target_count = [], set(), set(), 0
    updated = copy.deepcopy(rows)
    for index, row in enumerate(rows):
        require(isinstance(row, dict) and set(row) == {'source', 'relative', 'bytes', 'sha256'}
                and isinstance(row['relative'], str) and isinstance(row['source'], str)
                and row['source'] and type(row['bytes']) is int and row['bytes'] >= 0,
                'business inventory row is incomplete')
        parts = PurePosixPath(row['relative'])
        require(not parts.is_absolute() and '\\' not in row['relative']
                and all(part not in ('', '.', '..') for part in row['relative'].split('/'))
                and row['relative'] not in names and row['source'].lower() not in sources,
                'inventory paths must be independent and within the release')
        names.add(row['relative']); sources.add(row['source'].lower())
        raw = regular(old_root.joinpath(*parts.parts))
        require(len(raw) == row['bytes'] and sha(raw) == row['sha256'], 'old business bytes changed')
        if row['relative'].endswith('/candidates/postprocess-prefect-20261031/body_adapter.py'):
            raw = patch
            updated[index]['bytes'], updated[index]['sha256'] = len(raw), sha(raw)
            target_count += 1
        contents.append((parts, raw))
    require(target_count == 1, 'one exact body adapter must be present')
    os.mkdir(new_root, 0o700)
    code_root = new_root / 'code'; os.mkdir(code_root, 0o700)
    for relative, raw in contents:
        target = code_root.joinpath(*relative.parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        write_once(target, raw)
    result_raw = json.dumps(updated, ensure_ascii=False, sort_keys=True,
                            separators=(',', ':'), allow_nan=False).encode('utf8')
    manifest = new_root / 'code-manifest.json'
    write_once(manifest, result_raw)
    return dict(code_root=str(code_root), manifest=str(manifest), manifest_sha256=sha(result_raw),
                files=len(updated), changed_files=1, old_release_preserved=True, business_work_started=False)


if __name__ == '__main__':
    if len(sys.argv) != 8:
        raise SystemExit('base, old root, old manifest, old SHA, new root, replacement and SHA required')
    print(json.dumps(prepare(*sys.argv[1:]), ensure_ascii=False, sort_keys=True))
