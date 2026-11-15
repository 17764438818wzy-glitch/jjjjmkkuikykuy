"""Bootstrap a separately pinned, selected-receipt observation only."""
import argparse
import hashlib
import hmac
import importlib.abc
import importlib.machinery
import importlib.util
import json
import os
from pathlib import Path
import re
import sys
from uuid import uuid4


HERE = Path(__file__).resolve().parent


def _bootstrap(name, path, expected):
    if not re.fullmatch(r'[0-9a-f]{64}', expected) or name in sys.modules:
        raise ValueError('an independently fixed fresh bootstrap is required')
    raw = path.read_bytes()
    if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), expected):
        raise ValueError('bootstrap source differs from its review')
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError('bootstrap source is unavailable')
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        exec(compile(raw, str(path), 'exec', dont_inherit=True), module.__dict__)
    except BaseException:
        del sys.modules[name]
        raise
    return module


class _ExactSourceLoader(importlib.abc.Loader):
    def __init__(self, guard, source, digest):
        self.guard, self.source, self.digest = guard, source, digest

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        raw = self.guard.read_verified(self.source)
        if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), self.digest):
            raise ImportError('candidate source changed during module loading')
        exec(compile(raw, str(self.source), 'exec', dont_inherit=True), module.__dict__)


class _SourceOnlyFinder(importlib.abc.MetaPathFinder):
    """Reject unmanaged shadows of approved candidate module names."""
    def __init__(self, guard, release):
        self.guard, self.release = guard, release
        self.roots = [(Path(entry[0]).resolve(), entry[2]) for entry in guard.roots]
        self.protected = set()
        for name in guard._approved:
            source = Path(name)
            if source.suffix.lower() != '.py':
                continue
            for root, recursive in self.roots:
                if (source.is_relative_to(root) if recursive else source.parent == root):
                    parts = list(source.relative_to(root).with_suffix('').parts)
                    if parts[-1] == '__init__':
                        parts.pop()
                    if recursive:
                        parts.insert(0, root.name)
                    if parts:
                        self.protected.add('.'.join(parts))

    def _managed(self, path):
        return any(path.is_relative_to(root) if recursive else path.parent == root
                   for root, recursive in self.roots)

    def find_spec(self, fullname, path=None, target=None):
        spec = importlib.machinery.PathFinder.find_spec(fullname, path)
        if spec is None:
            if fullname in self.protected:
                raise ImportError('an approved candidate module is unavailable')
            return None
        origin = spec.origin
        if not isinstance(origin, str) or origin in {'built-in', 'frozen'}:
            locations = spec.submodule_search_locations or ()
            if fullname in self.protected or any(self._managed(Path(os.path.abspath(p))) for p in locations):
                raise ImportError('unfixed candidate namespaces are not accepted')
            return None
        lexical = Path(os.path.abspath(origin))
        managed = self._managed(lexical)
        if not managed and fullname not in self.protected:
            return None
        if (not managed or lexical.suffix.lower() != '.py' or lexical.is_symlink()
                or os.path.normcase(str(lexical.resolve())) != os.path.normcase(str(lexical))):
            raise ImportError('candidate modules must be approved source files')
        digest = self.guard._approved.get(os.path.normcase(str(lexical)))
        raw = self.guard.read_verified(lexical)
        if digest is None or not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), digest):
            raise ImportError('candidate source differs from its approved path')
        if spec.submodule_search_locations is not None:
            locations = [Path(os.path.abspath(p)) for p in spec.submodule_search_locations]
            if locations != [lexical.parent]:
                raise ImportError('candidate package paths are not fixed')
        spec.loader = _ExactSourceLoader(self.guard, lexical, digest)
        return spec


class _QuietParser(argparse.ArgumentParser):
    def error(self, _message):
        raise ValueError('invalid observation arguments')


def main(argv=None):
    parser = _QuietParser(add_help=False)
    parser.add_argument('--inventory', required=True)
    parser.add_argument('--inventory-sha256', required=True)
    parser.add_argument('--guard-sha256', required=True)
    parser.add_argument('--layout-sha256', required=True)
    parser.add_argument('observation_args', nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    cache = HERE / ('.selected-probe-bytecode-disabled-' + uuid4().hex)
    if cache.exists():
        raise ValueError('private bytecode prefix already exists')
    sys.pycache_prefix, sys.dont_write_bytecode = str(cache), True
    release = _bootstrap('release_guard', HERE / 'release_guard.py', args.guard_sha256)
    layout = _bootstrap('release_layout', HERE / 'release_layout.py', args.layout_sha256)
    roots, required = layout.layout()
    guard = release.ReleaseGuard(args.inventory, args.inventory_sha256, roots=roots,
                                 required_files=required, required_packages=layout.PACKAGES)
    guard.verify()
    finder = _SourceOnlyFinder(guard, release)
    # Do not reuse an unmanaged import from a candidate source directory or
    # a shadow of any approved candidate module name.
    for name, module in list(sys.modules.items()):
        if name in {'release_guard', 'release_layout'}:
            continue
        source = getattr(module, '__file__', None)
        if not source:
            continue
        path = Path(os.path.abspath(source))
        if name in finder.protected or finder._managed(path):
            raise ValueError('a candidate module was loaded before verification')
    sys.meta_path.insert(0, finder)
    observer = guard.load_source('_paperflow_selected_receipt_observer', HERE / 'observe_selected_receipts.py')
    observation_args = args.observation_args
    if observation_args and observation_args[0] == '--':
        observation_args = observation_args[1:]
    return observer.main(guard, observation_args)


if __name__ == '__main__':
    try:
        result = main()
    except SystemExit as exc:
        if exc.code is None or exc.code == 0:
            raise
        print(json.dumps({'status': 'observation_unavailable', 'phase': 'setup_or_report'}))
        raise SystemExit(1) from None
    except BaseException:
        print(json.dumps({'status': 'observation_unavailable', 'phase': 'setup_or_report'}))
        raise SystemExit(1) from None
    raise SystemExit(result)
