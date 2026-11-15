"""Stream an already-collected input closure to an authenticated server.

Staging never starts work. The caller must supply independently trusted roots,
an already authenticated, host-key-verified SSH client and a fixed command.
This transport does not create credentials, retry, dispatch or upload to RAG.
"""
import hashlib
import json
import os
from pathlib import Path
import stat
import struct
import time

from remote_jobs import _validated, JobConflict
from source_handle import check_opened_source, SourceHandleError
from snapshot_wire import snapshot_sha
from windows_snapshot import _no_links, EvidenceError


class TransferError(ValueError):
    pass


def _write_all(stream, data):
    view = memoryview(data)
    while view:
        count = stream.write(view)
        if not isinstance(count, int) or count <= 0 or count > len(view):
            raise TransferError('snapshot stream did not accept the data')
        view = view[count:]


def write_snapshot(stream, manifest, *, allowed_roots, trusted_files=None,
                   max_header_bytes=16 * 1024 * 1024, verify_locally=True):
    try:
        if (not isinstance(manifest, dict) or manifest.get('schema') != 'paperflow-remote-snapshot/1'
                or type(verify_locally) is not bool):
            raise TransferError('invalid snapshot manifest')
        if not verify_locally and manifest.get('binary_hash_scope') != 'server':
            raise TransferError('server-only hashing must be declared in the frozen snapshot')
        sha = snapshot_sha(manifest)
        job = {key: manifest[key] for key in ('job_id', 'claim_id', 'identity', 'generation',
                                               'manifest_sha256', 'code_sha256')}
        job['snapshot_sha256'] = sha
        _validated(job)
        roots = []
        if not isinstance(allowed_roots, (list, tuple)) or not allowed_roots:
            raise TransferError('independent trusted source roots are required')
        for value in allowed_roots:
            root = Path(value)
            if not root.is_absolute():
                raise TransferError('source root must be absolute')
            _no_links(root)
            if not root.is_dir():
                raise TransferError('source root is unavailable')
            roots.append(root.resolve(strict=True))
        # An independently configured file grants access to that exact file,
        # never to its containing directory or to paths named by the input.
        if trusted_files is None:
            trusted_files = []
        if not isinstance(trusted_files, (list, tuple)):
            raise TransferError('exact trusted file references must be a list')
        trusted = {}
        for reference in trusted_files:
            if (not isinstance(reference, dict) or set(reference) != {'path', 'sha256'}
                    or not isinstance(reference['path'], str)
                    or not isinstance(reference['sha256'], str) or len(reference['sha256']) != 64
                    or any(c not in '0123456789abcdef' for c in reference['sha256'])):
                raise TransferError('invalid exact trusted file reference')
            path = Path(reference['path'])
            if not path.is_absolute() or '..' in path.parts:
                raise TransferError('exact trusted file path must be absolute')
            _no_links(path)
            actual = path.resolve(strict=True)
            key = os.path.normcase(os.path.normpath(str(path)))
            if (key != os.path.normcase(os.path.normpath(str(actual))) or not actual.is_file()
                    or key in trusted):
                raise TransferError('exact trusted file identity is unavailable or duplicated')
            trusted[key] = reference['sha256']
        rows = manifest['files']
        if not isinstance(rows, list) or not rows:
            raise TransferError('source evidence is missing')
        header = json.dumps(dict(manifest=manifest, snapshot_sha256=sha), ensure_ascii=False,
                            sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf8')
        if type(max_header_bytes) is not int or max_header_bytes < 1 or len(header) > max_header_bytes:
            raise TransferError('snapshot header is too large')
        _write_all(stream, struct.pack('!Q', len(header)))
        _write_all(stream, header)
        seen = set()
        for row in rows:
            path = Path(row['path'])
            key = os.path.normcase(os.path.normpath(str(path)))
            exact_sha = trusted.get(key)
            within_root = any(path.is_relative_to(root) for root in roots)
            if (not path.is_absolute() or '..' in path.parts or type(row['bytes']) is not int
                    or row['bytes'] < 0 or not isinstance(row['sha256'], str)
                    or len(row['sha256']) != 64 or any(c not in '0123456789abcdef' for c in row['sha256'])
                    or (not within_root and exact_sha is None)
                    or (exact_sha is not None and exact_sha != row['sha256'])):
                raise TransferError('source entry is invalid or outside the allowed roots')
            if key in seen:
                raise TransferError('duplicate source entry')
            seen.add(key)
            _no_links(path)
            real = path.resolve(strict=True)
            if (within_root and not any(real.is_relative_to(root) for root in roots)) or (
                    not within_root and os.path.normcase(os.path.normpath(str(real))) != key):
                raise TransferError('resolved source escaped its allowed roots')
            before = path.stat()
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                raise TransferError('source is not an independent regular file')
            with path.open('rb') as src:
                opened = os.fstat(src.fileno())
                if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino) or opened.st_nlink != 1:
                    raise TransferError('source changed while opening')
                check_opened_source(src.fileno(), path, roots if within_root else [real.parent])
                digest, size = hashlib.sha256(), 0
                hash_this_file = verify_locally or exact_sha is not None
                for chunk in iter(lambda: src.read(1024 * 1024), b''):
                    size += len(chunk)
                    if size > row['bytes']:
                        raise TransferError('source grew during transfer')
                    if hash_this_file:
                        digest.update(chunk)
                    _write_all(stream, chunk)
                after = os.fstat(src.fileno())
            if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                    != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
                    or after.st_nlink != 1 or size != row['bytes']
                    or (hash_this_file and digest.hexdigest() != row['sha256'])):
                raise TransferError('source evidence changed during transfer')
        return dict(job_id=job['job_id'], claim_id=job['claim_id'], snapshot_sha256=sha, files=len(rows))
    except TransferError:
        raise
    except (EvidenceError, SourceHandleError, JobConflict, KeyError, TypeError, ValueError, OSError) as exc:
        raise TransferError('source evidence or transfer is invalid') from exc


class _ChannelWriter:
    def __init__(self, channel):
        self.channel = channel

    def write(self, data):
        return self.channel.send(data)


def stage_over_ssh(client, fixed_command, manifest, *, allowed_roots, trusted_files=None,
                   io_timeout=30, verify_locally=True):
    """One SSH attempt. Any interruption is unknown and MUST NOT be retried."""
    if (not isinstance(fixed_command, str) or not fixed_command or '\n' in fixed_command
            or '\r' in fixed_command or isinstance(io_timeout, bool) or io_timeout <= 0):
        raise TransferError('fixed transport command and positive timeout required')
    transport = client.get_transport()
    if transport is None or not transport.is_active() or not transport.is_authenticated():
        raise TransferError('authenticated SSH connection is unavailable')
    channel = transport.open_session(timeout=io_timeout)
    channel.settimeout(io_timeout)
    try:
        channel.exec_command(fixed_command)
        local = write_snapshot(_ChannelWriter(channel), manifest, allowed_roots=allowed_roots,
                               trusted_files=trusted_files, verify_locally=verify_locally)
        channel.shutdown_write()
        deadline, data, error = time.monotonic() + io_timeout, bytearray(), bytearray()
        while not channel.exit_status_ready() or channel.recv_ready() or channel.recv_stderr_ready():
            if time.monotonic() >= deadline:
                raise TransferError('server staging acknowledgement timed out')
            if channel.recv_ready():
                data.extend(channel.recv(4096))
            if channel.recv_stderr_ready():
                error.extend(channel.recv_stderr(4096))
            if len(data) > 65536 or len(error) > 65536:
                raise TransferError('server staging response exceeded its limit')
            time.sleep(.01)
        if channel.recv_exit_status() != 0:
            raise TransferError('server refused or did not finish input staging')
        ack = json.loads(data)
        if (not isinstance(ack, dict) or ack.get('schema') != 'paperflow-remote-snapshot-verified/1'
                or ack.get('evidence_scope') != 'declared_files_only'
                or ack.get('dependency_closure_verified') is not False
                or any(ack.get(k) != v for k, v in local.items())
                or ack.get('identity') != manifest['identity']
                or ack.get('generation') != manifest['generation']
                or ack.get('manifest_sha256') != manifest['manifest_sha256']
                or ack.get('code_sha256') != manifest['code_sha256']):
            raise TransferError('server staging acknowledgement does not match the input')
        return ack
    except TransferError:
        raise
    except Exception as exc:
        raise TransferError('server staging outcome is unknown') from exc
    finally:
        channel.close()
