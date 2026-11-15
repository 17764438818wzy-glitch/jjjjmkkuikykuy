"""Work around Wine's WinError 234 for verified long F: paths only.

Wine can read long paths but may fail GetFinalPathNameByHandleW. Its lstat also
follows native Linux symlinks, so the fallback must ask the authenticated Linux
supervisor to verify every component instead of trusting Wine's view.
"""
import json
import ntpath
import os
import socket


class UnsafeWinePath(RuntimeError):
    pass


def checked_resolver(original, *, verify):
    if not callable(original) or not callable(verify):
        raise UnsafeWinePath('a path resolver and authenticated verifier are required')

    def canonical_f_path(path):
        value = os.fspath(path)
        if not isinstance(value, str):
            if isinstance(value, bytes):
                candidate = value[4:] if value.startswith(b'\\\\?\\') else value
                if value.startswith(b'\\\\.\\') or ntpath.splitdrive(candidate)[0].lower() == b'f:':
                    raise UnsafeWinePath('byte paths cannot be verified')
            return None, False
        if value.startswith('\\\\.\\'):
            raise UnsafeWinePath('Windows device aliases are not permitted')
        extended = value.startswith('\\\\?\\')
        candidate = value[4:] if extended else value
        if ntpath.splitdrive(candidate)[0].lower() != 'f:':
            return None, extended
        if (not ntpath.isabs(candidate) or ntpath.normpath(candidate) != candidate
                or candidate.startswith(('\\\\?\\', '\\\\.\\'))):
            raise UnsafeWinePath('unsupported F: path')
        # The native snapshot mapper uses a canonical uppercase drive.
        return 'F:' + candidate[2:], extended

    def resolve(path):
        candidate, extended = canonical_f_path(path)
        try:
            result = original(path)
        except OSError as exc:
            if getattr(exc, 'winerror', None) != 234:
                raise
            if candidate is None:
                raise UnsafeWinePath('unsupported long path') from exc
            if verify(candidate) is not True:
                raise UnsafeWinePath('long path did not pass the server verifier') from exc
            return ('\\\\?\\' if extended else '') + candidate
        # Wine may report a Linux symlink as a regular directory without any
        # error. Every successful F: resolution must therefore be checked by
        # the server-native no-follow verifier, including short paths.
        if candidate is not None:
            resolved = result[4:] if isinstance(result, str) and result.startswith('\\\\?\\') else result
            if (not isinstance(resolved, str)
                    or ntpath.normcase(ntpath.normpath(resolved)) != ntpath.normcase(candidate)
                    or verify(candidate) is not True):
                raise UnsafeWinePath('snapshot path did not pass the server verifier')
        return result
    return resolve


def install(*, port, secret, snapshot_sha256):
    if (os.name != 'nt' or type(port) is not int or not 1 <= port <= 65535
            or not isinstance(secret, str) or not secret
            or not isinstance(snapshot_sha256, str) or len(snapshot_sha256) != 64):
        raise UnsafeWinePath('private Wine path verifier is not configured')

    def verify(path):
        request = json.dumps(dict(secret=secret, operation='verify_snapshot_path', path=path),
                             ensure_ascii=True, separators=(',', ':')).encode('ascii') + b'\n'
        if len(request) > 65536:
            raise UnsafeWinePath('long path request is too large')
        with socket.create_connection(('127.0.0.1', port), timeout=30) as conn:
            conn.sendall(request)
            with conn.makefile('rb') as stream:
                raw = stream.readline(65537)
        if not raw.endswith(b'\n') or len(raw) > 65536:
            raise UnsafeWinePath('long path response is invalid')
        response = json.loads(raw)
        return response == dict(schema='paperflow-wine-verified-path/1', path=path,
                                snapshot_sha256=snapshot_sha256, verified=True)

    ntpath._getfinalpathname = checked_resolver(ntpath._getfinalpathname, verify=verify)
