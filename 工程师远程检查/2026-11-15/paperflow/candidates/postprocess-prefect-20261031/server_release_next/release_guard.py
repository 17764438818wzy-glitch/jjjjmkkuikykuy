"""Verify independently pinned page/runtime sources without mutating state."""
import hashlib
import hmac
import importlib.metadata
import importlib.abc
import importlib.machinery
import importlib.util
import json
import os
from pathlib import Path
import re
import sys


HEX = re.compile(r'[a-f0-9]{64}')
MAX_FILES = 10000
MAX_TOTAL_BYTES = 64 * 1024 * 1024
MAX_MANIFEST_BYTES = 4 * 1024 * 1024


def _path(value):
    path = Path(value)
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('release source path must be absolute')
    return path.resolve(strict=True)


def _key(path):
    return os.path.normcase(str(path))


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate release manifest key')
        result[key] = value
    return result


def _root_policy(entry):
    if not isinstance(entry, (tuple, list)) or len(entry) not in (3, 4):
        raise ValueError('release source root policy is invalid')
    root, suffixes, recursive = entry[:3]
    names = entry[3] if len(entry) == 4 else None
    if names is not None:
        if (recursive or not isinstance(names, (tuple, list, frozenset)) or not names
                or any(not isinstance(name, str) or Path(name).name != name for name in names)
                or len(set(names)) != len(names)):
            raise ValueError('release source filename allowlist is invalid')
        names = frozenset(names)
    return root, suffixes, recursive, names


def inventory(roots, required_files):
    """Return the exact allowed inventory; never descend into links or caches."""
    paths = {}
    for entry in roots:
        root, suffixes, recursive, names = _root_policy(entry)
        root = _path(root)
        if not root.is_dir() or not isinstance(suffixes, tuple) or type(recursive) is not bool:
            raise ValueError('release source root is invalid')
        entries = root.rglob('*') if recursive else root.glob('*')
        for entry in entries:
            relative = entry.relative_to(root)
            if any(part in {'__pycache__', '.pytest_cache', 'tests'} for part in relative.parts):
                continue
            if names is not None and entry.name not in names:
                continue
            if entry.is_symlink() or (hasattr(entry, 'is_junction') and entry.is_junction()):
                raise ValueError('release source links are not accepted')
            if not entry.is_file() or (suffixes and entry.suffix.lower() not in suffixes):
                continue
            resolved = _path(entry)
            if not resolved.is_relative_to(root):
                raise ValueError('release source escaped its root')
            paths[_key(resolved)] = resolved
    for entry in required_files:
        path = _path(entry)
        if not path.is_file() or Path(entry).is_symlink():
            raise ValueError('a required release source is unavailable')
        paths[_key(path)] = path
    if not paths or len(paths) > MAX_FILES:
        raise ValueError('release source inventory is empty or too large')
    return paths


def file_hashes(paths):
    total, result = 0, {}
    for key, path in paths.items():
        before = path.stat()
        size = before.st_size
        total += size
        if total > MAX_TOTAL_BYTES:
            raise ValueError('release source inventory exceeds its byte budget')
        with path.open('rb') as stream:
            opened = os.fstat(stream.fileno())
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        after = path.stat()
        identity = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)
        signature = lambda value: (*identity(value), value.st_ctime_ns)
        if identity(before) != identity(opened) or signature(before) != signature(after):
            raise ValueError('release source changed while being read')
        result[key] = digest
    return result


class ReleaseGuard:
    def __init__(self, manifest_path, manifest_sha256, *, roots, required_files, required_packages=()):
        if not isinstance(manifest_sha256, str) or not HEX.fullmatch(manifest_sha256):
            raise ValueError('an independent release manifest hash is required')
        self.path = _path(manifest_path)
        self.sha256 = manifest_sha256
        self.roots = tuple(roots)
        self.required_files = tuple(required_files)
        self.required_packages = frozenset(required_packages)
        self._approved = None

    def _manifest(self):
        with self.path.open('rb') as stream:
            raw = stream.read(MAX_MANIFEST_BYTES + 1)
        if (len(raw) > MAX_MANIFEST_BYTES or
                not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), self.sha256)):
            raise ValueError('release manifest changed')
        value = json.loads(raw, object_pairs_hook=_unique_object)
        if (not isinstance(value, dict) or set(value) != {'schema', 'files', 'python', 'packages'}
                or value['schema'] != 'paperflow-page-release/1' or not isinstance(value['files'], dict)
                or not isinstance(value['packages'], dict)
                or not self.required_packages <= set(value['packages'])):
            raise ValueError('release manifest is incomplete')
        if self.required_packages and not isinstance(value['python'], str):
            raise ValueError('the interpreter version is required')
        if value['python'] is not None and value['python'] != '.'.join(map(str, sys.version_info[:3])):
            raise ValueError('Python version differs from the release')
        for name, expected in value['packages'].items():
            if (not isinstance(name, str) or not isinstance(expected, str)
                    or importlib.metadata.version(name) != expected):
                raise ValueError('a required package version changed')
        expected = {}
        for name, digest in value['files'].items():
            if not isinstance(name, str) or not isinstance(digest, str) or not HEX.fullmatch(digest):
                raise ValueError('invalid pinned release file')
            path = _path(name)
            key = _key(path)
            if key in expected:
                raise ValueError('duplicate pinned release file')
            expected[key] = digest
        return expected

    def verify(self):
        expected = self._manifest()
        current = file_hashes(inventory(self.roots, self.required_files))
        if current != expected:
            raise ValueError('release source inventory changed')
        if self._approved is not None and self._approved != expected:
            raise ValueError('approved release identity changed')
        self._approved = expected
        return True

    __call__ = verify

    def read_verified(self, source):
        """Read exact approved source bytes, without consulting a pyc cache."""
        path = _path(source)
        expected = self._approved.get(_key(path)) if self._approved is not None else None
        if expected is None:
            raise ValueError('source is not in the approved release')
        before = path.stat()
        with path.open('rb') as stream:
            opened = os.fstat(stream.fileno())
            raw = stream.read(MAX_TOTAL_BYTES + 1)
        after = path.stat()
        identity = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)
        signature = lambda value: (*identity(value), value.st_ctime_ns)
        if (len(raw) > MAX_TOTAL_BYTES or identity(before) != identity(opened)
                or signature(before) != signature(after)
                or not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), expected)):
            raise ValueError('source changed while being loaded')
        return raw

    def load_source(self, name, source):
        path = Path(source)
        if not path.is_absolute() or '..' in path.parts:
            raise ValueError('release source path must be absolute')
        path = Path(os.path.abspath(path))
        expected = self._approved.get(_key(path)) if self._approved is not None else None
        if expected is None or path.is_symlink():
            raise ValueError('source is not in the approved release')
        raw = self.read_verified(path)
        if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), expected):
            raise ValueError('source differs from its approved path')
        if name in sys.modules:
            raise ValueError('release module name is already in use')
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise ValueError('release module is unavailable')
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        try:
            exec(compile(raw, str(path), 'exec', dont_inherit=True), module.__dict__)
        except BaseException:
            sys.modules.pop(name, None)
            raise
        return module


class _VerifiedLoader(importlib.abc.Loader):
    def __init__(self, guard, path):
        self.guard, self.path = guard, path

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        source = self.guard.read_verified(self.path)
        exec(compile(source, str(self.path), 'exec', dont_inherit=True), module.__dict__)


class PinnedFinder(importlib.abc.MetaPathFinder):
    """Load managed Python sources directly from checked bytes, never pyc."""
    def __init__(self, guard):
        self.guard = guard

    def find_spec(self, fullname, path=None, target=None):
        spec = importlib.machinery.PathFinder.find_spec(fullname, path)
        if spec is None or not isinstance(spec.origin, str) or not spec.origin.lower().endswith('.py'):
            return None
        source = Path(spec.origin).resolve()
        for entry in self.guard.roots:
            root, suffixes, recursive, _names = _root_policy(entry)
            root = Path(root).resolve()
            relative = source.is_relative_to(root) if recursive else source.parent == root
            if relative and (not suffixes or source.suffix.lower() in suffixes):
                # read_verified also rejects omitted files, even if a matching
                # source was added to an otherwise valid import directory.
                self.guard.read_verified(source)
                spec.loader = _VerifiedLoader(self.guard, source)
                return spec
        return None
