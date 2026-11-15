"""Reuse bounded SSH while executing approved local helper bytes in the child.

The local interpreter, site startup and third-party packages remain external
trust roots. This module does not dispatch work or change the SSH protocol.
"""
import base64
import hashlib
import hmac
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time


CHILD_BOOTSTRAP = r'''
import base64, hashlib, hmac, importlib.util, io, json, os, sys, types
encoded = sys.stdin.buffer.read(16 * 1024 * 1024 + 1)
if len(encoded) > 16 * 1024 * 1024: raise SystemExit(1)
value = json.loads(encoded.decode('utf8'))
sys.path.append(sys.argv[5])
client = base64.b64decode(value['client_source'], validate=True)
helper = base64.b64decode(value['helper_source'], validate=True)
if not hmac.compare_digest(hashlib.sha256(client).hexdigest(), sys.argv[2]): raise SystemExit(1)
if not hmac.compare_digest(hashlib.sha256(helper).hexdigest(), sys.argv[4]): raise SystemExit(1)
module = types.ModuleType('ssh_rpc_client')
module.__file__ = sys.argv[1]
sys.modules['ssh_rpc_client'] = module
exec(compile(client, sys.argv[1], 'exec', dont_inherit=True), module.__dict__)
spec = importlib.util.find_spec('paramiko')
expected = os.path.normcase(os.path.realpath(sys.argv[6]))
if spec is None or not isinstance(spec.origin, str) or os.path.normcase(os.path.realpath(spec.origin)) != expected: raise SystemExit(1)
import paramiko
if os.path.normcase(os.path.realpath(paramiko.__file__)) != expected: raise SystemExit(1)
raw = json.dumps(value['request'], ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf8')
sys.stdin = io.TextIOWrapper(io.BytesIO(raw), encoding='utf8')
exec(compile(helper, sys.argv[3], 'exec', dont_inherit=True), {'__name__':'__main__', '__file__':sys.argv[3]})
'''


def build(guard, client_path, helper_path):
    def exact_path(value):
        path = Path(value)
        if not path.is_absolute() or '..' in path.parts:
            raise ValueError('local SSH source path must be absolute')
        path = Path(os.path.abspath(path))
        if path.is_symlink() or os.path.normcase(str(path.resolve(strict=True))) != os.path.normcase(str(path)):
            raise ValueError('local SSH source links are not accepted')
        return path
    client_path, helper_path = exact_path(client_path), exact_path(helper_path)
    approved = getattr(guard, '_approved', None)
    if not isinstance(approved, dict):
        raise ValueError('an already verified release is required')
    expected = [approved.get(os.path.normcase(str(path))) for path in (client_path, helper_path)]
    if any(not isinstance(value, str) or len(value) != 64 for value in expected):
        raise ValueError('local SSH sources must be in the fixed release')
    package = importlib.util.find_spec('paramiko')
    if package is None or not isinstance(package.origin, str):
        raise ValueError('the configured Python cannot locate the SSH dependency')
    package_origin = Path(package.origin).resolve(strict=True)
    package_root = package_origin.parent.parent
    if not package_root.is_absolute() or not package_root.is_dir():
        raise ValueError('the configured SSH dependency path is unavailable')
    client = guard.load_source('_paperflow_verified_ssh_client', client_path)

    def execute(_original_command, request, *, timeout, max_bytes):
        deadline = time.monotonic() + timeout
        sources = [guard.read_verified(path) for path in (client_path, helper_path)]
        if any(not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), digest)
               for raw, digest in zip(sources, expected)):
            raise ValueError('a local SSH source differs from its fixed digest')
        envelope = dict(client_source=base64.b64encode(sources[0]).decode('ascii'),
                        helper_source=base64.b64encode(sources[1]).decode('ascii'), request=request)
        raw = json.dumps(envelope, ensure_ascii=False, sort_keys=True,
                         separators=(',', ':'), allow_nan=False).encode('utf8')
        if len(raw) > max_bytes:
            raise client.RemoteCallError('metadata request is too large')
        command = [sys.executable, '-I', '-B', '-c', CHILD_BOOTSTRAP,
                   str(client_path), expected[0], str(helper_path), expected[1], str(package_root), str(package_origin)]
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError('local SSH source verification timed out')
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(command, timeout)
            output, error = process.communicate(raw, timeout=remaining)
        except subprocess.TimeoutExpired as exc:
            process.kill()
            try:
                process.communicate(timeout=2)
            except subprocess.TimeoutExpired as kill_error:
                raise client.RemoteCallError('server outcome and local helper termination are unknown') from kill_error
            raise client.RemoteCallError('server request outcome is unknown after timeout') from exc
        if process.returncode != 0:
            # The helper prints only an exception class; never retain arbitrary
            # stderr, request contents, server responses, paths or credentials.
            classes = {
                b'isolated SSH request failed: AuthenticationException': 'authentication_failed',
                b'isolated SSH request failed: SSHException': 'host_or_protocol_error',
                b'isolated SSH request failed: FileNotFoundError': 'key_file_unavailable',
                b'isolated SSH request failed: RemoteCallError': 'request_refused_or_unknown',
            }
            reason = classes.get(error.strip(), 'helper_failed') if len(error) <= 65536 else 'helper_failed'
            failure = client.RemoteCallError('isolated_ssh:' + reason)
            failure.observation_reason = reason
            raise failure
        if len(output) > max_bytes or len(error) > 65536:
            raise client.RemoteCallError('server request was refused or its outcome is unknown')
        try:
            result = json.loads(output)
        except (ValueError, UnicodeError) as exc:
            raise client.RemoteCallError('server returned invalid metadata') from exc
        if time.monotonic() >= deadline:
            raise ValueError('local SSH call timed out')
        return result

    client._run_bounded = execute
    return client.call_bounded
