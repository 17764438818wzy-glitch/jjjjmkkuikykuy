"""Prepare one private Wine prefix without starting execution.

Code and template inventories are byte-bound to the original snapshot SHA.
Native Wine, its loader/libraries and the Linux venv remain caller obligations;
this module never claims the entire native runtime is pinned or starts a service.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import stat

from remote_jobs import _validated
from snapshot_import import SnapshotImporter
from snapshot_wire import snapshot_sha
from windows_snapshot import EvidenceMap


class ReleaseError(ValueError):
    pass


def _require(good, detail):
    if not good: raise ReleaseError(detail)


def _encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf8')


def _relative(value):
    _require(isinstance(value, str) and value and '\\' not in value
        and not PurePosixPath(value).is_absolute()
        and all(p not in ('', '.', '..') for p in value.split('/')), 'invalid relative release path')
    return tuple(value.split('/'))


def _identity(info): return info.st_dev, info.st_ino
def _version(info): return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_mode, info.st_nlink


class _Tree:
    """Every source and destination operation stays under pinned directory FDs."""
    def __init__(self, root, *, private=False):
        _require(os.name == 'posix' and hasattr(os, 'O_NOFOLLOW') and hasattr(os, 'O_TMPFILE'),
            'Linux no-follow filesystem support is required')
        self.root = Path(root)
        _require(self.root.is_absolute() and '..' not in self.root.parts, 'absolute server root required')
        self.fd = self._absolute()
        self.info = os.fstat(self.fd)
        if private and (self.info.st_uid != os.getuid() or self.info.st_mode & 0o077):
            self.close(); raise ReleaseError('private snapshot required')

    def _absolute(self):
        fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for part in self.root.parts[1:]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd); fd = child
            return fd
        except BaseException:
            os.close(fd); raise

    def close(self):
        if self.fd is not None: os.close(self.fd); self.fd = None

    def visible(self):
        fd = self._absolute()
        try: _require(_identity(os.fstat(fd)) == _identity(self.info), 'release root changed')
        finally: os.close(fd)

    def parent(self, parts):
        fd = os.dup(self.fd)
        try:
            for part in parts:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd); fd = child
            return fd
        except BaseException:
            os.close(fd); raise

    def named(self, relative, expected):
        parts = _relative(relative); fd = self.parent(parts[:-1])
        try:
            current = os.stat(parts[-1], dir_fd=fd, follow_symlinks=False)
            _require(_version(current) == _version(expected), 'release file or directory path changed')
        finally: os.close(fd)
        self.visible()

    @contextmanager
    def regular(self, relative):
        parts = _relative(relative); self.visible(); parent = self.parent(parts[:-1])
        try:
            fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            with os.fdopen(fd, 'rb') as stream:
                before = os.fstat(stream.fileno())
                _require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1, 'independent regular release file required')
                yield stream, before
                after = os.fstat(stream.fileno())
            _require(_version(before) == _version(after), 'release file changed while reading')
            self.named(relative, after)
        finally: os.close(parent)

    def read(self, relative):
        with self.regular(relative) as (stream, _): return stream.read()

    def inventory(self):
        self.visible(); entries = []
        def walk(fd, base):
            before = os.fstat(fd)
            for name in sorted(os.listdir(fd)):
                relative = '/'.join((*base, name)); _relative(relative)
                info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                    try:
                        _require(_version(os.fstat(child)) == _version(info), 'template directory changed on opening')
                        entries.append(dict(path=relative, kind='directory', mode=stat.S_IMODE(info.st_mode)))
                        walk(child, (*base, name)); self.named(relative, os.fstat(child))
                    finally: os.close(child)
                elif stat.S_ISLNK(info.st_mode):
                    target = os.readlink(name, dir_fd=fd)
                    _require(_version(os.stat(name, dir_fd=fd, follow_symlinks=False)) == _version(info), 'template link changed')
                    entries.append(dict(path=relative, kind='link', target=target))
                elif stat.S_ISREG(info.st_mode):
                    with self.regular(relative) as (stream, opened):
                        _require(_version(opened) == _version(info), 'template file changed on opening')
                        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
                    entries.append(dict(path=relative, kind='file', mode=stat.S_IMODE(info.st_mode), bytes=info.st_size, sha256=digest))
                else: raise ReleaseError('unsupported template filesystem entry')
            _require(_version(before) == _version(os.fstat(fd)), 'template directory changed while inspecting')
        walk(self.fd, ()); self.visible()
        return dict(schema='paperflow-runtime-template/1', root_mode=stat.S_IMODE(os.fstat(self.fd).st_mode),
            entries=sorted(entries, key=lambda e:e['path']))

    def mkdir(self, relative, mode):
        parts = _relative(relative); parent = self.parent(parts[:-1])
        try:
            os.mkdir(parts[-1], mode, dir_fd=parent)
            child = os.open(parts[-1], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            try: os.fchmod(child, mode); os.fsync(child)
            finally: os.close(child)
            os.fsync(parent)
        finally: os.close(parent)

    def link(self, relative, target):
        parts = _relative(relative); parent = self.parent(parts[:-1])
        try: os.symlink(target, parts[-1], dir_fd=parent); os.fsync(parent)
        finally: os.close(parent)


def _read_trusted_file(path):
    path = Path(path); _require(path.is_absolute(), 'absolute trusted manifest path required')
    parent = _Tree(path.parent)
    try: return parent.read(path.name)
    finally: parent.close()


def _template_policy(inventory):
    _require(inventory.get('schema') == 'paperflow-runtime-template/1'
        and isinstance(inventory.get('entries'), list) and inventory['entries'], 'invalid runtime template inventory')
    by_name = {e['path']: e for e in inventory['entries']}
    _require(len(by_name) == len(inventory['entries']) and by_name.get('drive_c/Python/python.exe', {}).get('kind') == 'file'
        and by_name.get('dosdevices/c:') == dict(path='dosdevices/c:', kind='link', target='../drive_c'),
        'template Windows Python or isolated C: device missing')
    for entry in inventory['entries']:
        parts = _relative(entry['path'])
        if entry['kind'] == 'link':
            _require((len(parts) == 2 and parts[0] == 'dosdevices')
                or (len(parts) >= 3 and parts[:2] == ('drive_c', 'users')), 'unsupported template link')


def template_manifest_bytes(template):
    """Explicit initialization helper: return canonical bytes, write nothing.

    Save these bytes independently of the template and freeze their SHA256 in a
    NEW transport manifest as runtime_template_sha256 before calculating its SHA.
    This inventory does not pin native Wine/venv/loader dependencies.
    """
    source = _Tree(template)
    try:
        inventory = source.inventory(); _template_policy(inventory); return _encode(inventory)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        if isinstance(exc, ReleaseError): raise
        raise ReleaseError('template could not be inventoried safely') from exc
    finally: source.close()


def _copy_template(source, destination, inventory, snapshot):
    """Copy declared entries through fixed FDs, applying isolated link rules."""
    entries = []
    for entry in sorted(inventory['entries'], key=lambda e:(len(_relative(e['path'])), e['path'])):
        relative, kind = entry['path'], entry['kind']; parts = _relative(relative)
        if kind == 'directory': destination.mkdir(relative, entry['mode']); entries.append(dict(entry))
        elif kind == 'link':
            if parts[0] == 'dosdevices':
                if relative == 'dosdevices/c:': destination.link(relative, '../drive_c'); entries.append(dict(entry))
            else:
                destination.mkdir(relative, 0o700); entries.append(dict(path=relative, kind='directory', mode=0o700))
        elif kind == 'file':
            parent = destination.parent(parts[:-1])
            try:
                fd = os.open(parts[-1], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
                with os.fdopen(fd, 'wb') as out, source.regular(relative) as (inp, before):
                    digest, size = hashlib.sha256(), 0
                    for data in iter(lambda: inp.read(1024 * 1024), b''):
                        size += len(data); digest.update(data); out.write(data)
                    _require(size == entry['bytes'] == before.st_size and digest.hexdigest() == entry['sha256'], 'template bytes changed during copy')
                    out.flush(); os.fchmod(out.fileno(), entry['mode']); os.fsync(out.fileno())
                os.fsync(parent)
            finally: os.close(parent)
            entries.append(dict(entry))
        else: raise ReleaseError('unknown template entry type')
    snapshot.visible(); target_fd = snapshot.parent(('drives', 'f')); os.close(target_fd)
    target = str(snapshot.root / 'drives/f'); destination.link('dosdevices/f:', target)
    entries.append(dict(path='dosdevices/f:', kind='link', target=target))
    expected = dict(schema='paperflow-runtime-template/1', root_mode=0o700, entries=sorted(entries, key=lambda e:e['path']))
    _require(destination.inventory() == expected, 'copied prefix inventory differs from isolated template')


def prepare_environment(*, job, snapshot, code_root, code_manifest, template, template_manifest, prefix):
    """Exclusively prepare one job; no fallback for missing frozen inventories."""
    opened = []
    try:
        _validated(job)
        snapshot, code_root, code_manifest, template, template_manifest, prefix = map(Path,
            (snapshot, code_root, code_manifest, template, template_manifest, prefix))
        for path in (snapshot, code_root, code_manifest, template, template_manifest, prefix):
            _require(path.is_absolute() and '..' not in path.parts, 'absolute canonical server paths required')
        _require(not template_manifest.is_relative_to(template), 'trusted template inventory must be independent')
        _require(all(not prefix.is_relative_to(root) and not root.is_relative_to(prefix) for root in (snapshot, code_root, template)),
            'prefix overlaps source or snapshot')
        staged = _Tree(snapshot, private=True); opened.append(staged)
        code = _Tree(code_root); opened.append(code)
        source_template = _Tree(template); opened.append(source_template)
        prefix_parent = _Tree(prefix.parent); opened.append(prefix_parent)
        manifest = json.loads(staged.read('transport-manifest.json')); marker = json.loads(staged.read('INPUT_VERIFIED.json'))
        expected_job = {key:manifest[key] for key in ('job_id','claim_id','identity','generation','manifest_sha256','code_sha256')}
        expected_job['snapshot_sha256'] = snapshot_sha(manifest)
        _require(expected_job == job and marker.get('evidence_scope') == 'declared_files_only'
            and all(marker.get(k) == v for k,v in job.items()), 'staged evidence identity changed')
        expected_runtime_sha = manifest.get('runtime_template_sha256')
        _require(isinstance(expected_runtime_sha, str) and re.fullmatch('[a-f0-9]{64}', expected_runtime_sha), 'frozen runtime template SHA is required')
        template_raw = _read_trusted_file(template_manifest)
        _require(hashlib.sha256(template_raw).hexdigest() == expected_runtime_sha, 'trusted runtime template manifest changed')
        template_inventory = json.loads(template_raw); _template_policy(template_inventory)
        _require(source_template.inventory() == template_inventory, 'runtime template differs from frozen inventory')
        # One open/read supplies BOTH the pinned hash and parsed code list.
        code_raw = _read_trusted_file(code_manifest)
        _require(hashlib.sha256(code_raw).hexdigest() == job['code_sha256'], 'code manifest identity changed')
        rows = json.loads(code_raw); _require(isinstance(rows, list) and rows, 'code manifest is empty')
        mapper, names, code_files = EvidenceMap(snapshot), set(), []
        for row in manifest['files']:
            _require(PureWindowsPath(row['path']).drive.lower() == 'f:', 'declared F: inputs required')
            key = str(PureWindowsPath(row['path'])).lower(); _require(key not in names, 'duplicate input identity'); names.add(key)
            mapper.bind(row['path'], row['sha256'], row['bytes'])
        for row in rows:
            parts = _relative(row['relative'])
            _require(parts[:2] == ('drives','f') and PureWindowsPath(row['source']).drive.lower() == 'f:', 'code path escapes pinned release')
            key = str(PureWindowsPath(row['source'])).lower()
            _require(key not in names, 'code overlaps an existing Windows input identity'); names.add(key)
            target = mapper.bind(row['source'], row['sha256'], row['bytes'])
            _require(target.relative_to(snapshot).as_posix() == row['relative'], 'ambiguous code layout')
            code_files.append(dict(path=row['source'], sha256=row['sha256'], bytes=row['bytes']))
        original = SnapshotImporter(snapshot, manifest['files'])
        try:
            _require(original._root_identity == _identity(staged.info), 'snapshot root changed before verification'); original.verify_complete()
        finally: original.close()
        try: os.stat(prefix.name, dir_fd=prefix_parent.fd, follow_symlinks=False)
        except FileNotFoundError: pass
        else: raise ReleaseError('execution prefix already exists')
        importer = SnapshotImporter(snapshot, code_files)
        try:
            _require(importer._root_identity == _identity(staged.info), 'snapshot root changed before code installation')
            for row in rows:
                with code.regular(row['relative']) as (stream, before):
                    _require(before.st_size == row['bytes'], 'pinned source code size changed')
                    importer.put(row['source'], stream); _require(not stream.read(1), 'pinned code grew during installation')
            importer.verify_complete()
        finally: importer.close()
        code.visible(); staged.visible(); prefix_parent.visible(); prefix_parent.mkdir(prefix.name, 0o700)
        destination = _Tree(prefix, private=True); opened.append(destination)
        _copy_template(source_template, destination, template_inventory, staged)
        _require(source_template.inventory() == template_inventory, 'runtime template changed during copy')
        staged.visible(); code.visible(); prefix_parent.visible(); destination.visible()
        return dict(job=dict(job), prefix=str(prefix), snapshot=str(snapshot), verified_code_files=len(rows),
            template_identity_verified=True, runtime_template_sha256=expected_runtime_sha,
            native_runtime_identity_verified=False, native_runtime_scope='Wine loader/libraries and Linux venv are not pinned here')
    except ReleaseError: raise
    except (OSError, KeyError, TypeError, ValueError, AttributeError) as exc:
        raise ReleaseError('server environment could not be prepared safely') from exc
    finally:
        for tree in reversed(opened): tree.close()
