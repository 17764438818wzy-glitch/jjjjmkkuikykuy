"""Candidate read-only entry that runs pinned app source bytes, not app pyc.

The interpreter, its venv/site startup, standard library, native extensions,
OS and account are external trust roots. The entry and guard bytes must be
verified by the caller before execution; this file does not install a gate.
"""
import hashlib
import hmac
import importlib.machinery
import os
from pathlib import Path
import re
import sys
import types


SHA = re.compile(r'[0-9a-f]{64}')
LIMIT = 1024 * 1024


def _read(path, expected):
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts or not SHA.fullmatch(expected or ''):
        raise ValueError('an absolute source and independent digest are required')
    if path.is_symlink() or path.resolve(strict=True) != path:
        raise ValueError('linked entry sources are not permitted')
    with path.open('rb') as stream:
        data = stream.read(LIMIT + 1)
    if len(data) > LIMIT or not hmac.compare_digest(hashlib.sha256(data).hexdigest(), expected):
        raise ValueError('entry source differs from its independent digest')
    return data


def run(guard_path, guard_sha, app_path, manifest_path, manifest_sha, target_path, target_sha,
        config_path, config_sha, mode):
    if not sys.flags.isolated or not sys.dont_write_bytecode:
        raise ValueError('isolated Python without bytecode writing is required')
    app = Path(app_path)
    target = Path(target_path)
    if (not app.is_absolute() or '..' in app.parts or app.is_symlink()
            or app.resolve(strict=True) != app or not app.is_dir()
            or not Path(config_path).is_absolute() or '..' in Path(config_path).parts
            or not SHA.fullmatch(config_sha or '') or mode not in {'inspect', 'probe', 'unread', 'native'}):
        raise ValueError('fixed app, config and read-only operation are required')
    guard_source = _read(guard_path, guard_sha)
    guard_module = types.ModuleType('_paperflow_reviewed_guard')
    guard_module.__file__ = guard_path
    exec(compile(guard_source, guard_path, 'exec', dont_inherit=True), guard_module.__dict__)
    root_policy = ((app, ('.py',), False),)
    guard = guard_module.ReleaseGuard(manifest_path, manifest_sha, roots=root_policy,
        required_files=(app / 'server_rpc.py',))
    guard.verify()
    sources = {}
    for approved_path in guard._approved:
        source = Path(approved_path)
        if (source.parent != app or source.suffix.lower() != '.py'
                or not source.stem.isidentifier() or source.stem in sources):
            raise ValueError('historical app module name is ambiguous')
        sources[source.stem] = source
    if any(name in sys.modules for name in sources):
        raise ValueError('an app module was loaded before source checks')

    class ExactSourceLoader(guard_module._VerifiedLoader):
        def __init__(self, expected):
            super().__init__(guard, expected)
            self.expected = expected
            self.digest = guard._approved[os.path.normcase(str(expected))]

        def exec_module(self, module):
            source = self.guard.read_verified(self.expected)
            if not hmac.compare_digest(hashlib.sha256(source).hexdigest(), self.digest):
                raise ImportError('app source differs from its own approved digest')
            exec(compile(source, str(self.expected), 'exec', dont_inherit=True), module.__dict__)

    class VerifiedAppFinder(guard_module.PinnedFinder):
        def find_spec(self, fullname, path=None, target=None):
            spec = importlib.machinery.PathFinder.find_spec(fullname, path)
            if spec is None:
                if fullname in sources:
                    raise ImportError('approved historical app module is unavailable')
                return None
            expected = sources.get(fullname)
            lexical = (Path(os.path.abspath(spec.origin)) if isinstance(spec.origin, str)
                       and spec.origin not in {'built-in', 'frozen'} else None)
            origin = lexical.resolve() if lexical is not None else None
            if expected is not None:
                if lexical != expected or lexical.is_symlink() or origin != expected or expected.suffix != '.py':
                    raise ImportError('historical app import resolved outside its approved source')
                spec.loader = ExactSourceLoader(expected)
                return spec
            if ((lexical is not None and lexical.is_relative_to(app))
                    or (origin is not None and origin.is_relative_to(app))):
                raise ImportError('unlisted app source or bytecode is not permitted')
            if spec.origin is None and spec.submodule_search_locations:
                if any(Path(os.path.abspath(location)).is_relative_to(app)
                       or Path(location).resolve().is_relative_to(app) for location in spec.submodule_search_locations):
                    raise ImportError('unlisted historical app package is not permitted')
            return None

    # The outer verified-byte bootstrap adds its own directory; remove it so
    # app imports cannot fall back to an unreviewed sibling of this entry.
    excluded = {str(Path(guard_path).parent.resolve()), str(Path(__file__).parent.resolve()), str(app)}
    sys.path[:] = [str(app)] + [item for item in sys.path
        if isinstance(item, str) and item and str(Path(item).resolve()) not in excluded]
    sys.meta_path.insert(0, VerifiedAppFinder(guard))
    if mode in {'inspect', 'probe', 'native'}:
        if target != app / 'server_rpc.py':
            raise ValueError('only the historical inspect entry is supported')
        data = guard.read_verified(target)
        if not SHA.fullmatch(target_sha or '') or not hmac.compare_digest(hashlib.sha256(data).hexdigest(), target_sha):
            raise ValueError('historical RPC digest differs from its registry')
        if mode == 'native':
            # Import the same RPC dependencies without invoking its business
            # main, then perform the existing read-only native verification.
            sys.argv = [str(target), config_path, config_sha, 'inspect']
            namespace = {'__name__': '_paperflow_native_probe', '__file__': str(target)}
            exec(compile(data, str(target), 'exec', dont_inherit=True), namespace)
            import server_job_service as service
            config = service.load_config(config_path, config_sha)
            service.verify_native_runtime(config)
            print('{"schema":"paperflow-native-preflight/1","status":"passed"}')
            return
        argv = [str(target), config_path, config_sha, mode]
    else:
        data = _read(target, target_sha)
        argv = [str(target), str(app), config_path, config_sha]
    sys.argv = argv
    exec(compile(data, str(target), 'exec', dont_inherit=True), {'__name__': '__main__', '__file__': str(target)})


def main():
    if len(sys.argv) != 11:
        raise SystemExit('fixed app, manifest, entry, configuration and operation are required')
    try:
        run(*sys.argv[1:])
    except Exception as exc:
        print('verified server entry failed: ' + type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
