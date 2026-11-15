"""Bind one already-staged snapshot to one server service directory.

This does not reserve a worker, launch a process, alter the original marker or
retry an interrupted transfer. Partially created descriptors are retained and
cannot be overwritten; an operator must reconcile any ambiguous outcome.
"""
import hashlib
import json
import os
from pathlib import Path
import stat

from remote_jobs import _uuid, _validated
from snapshot_import import _PinnedDirectory
from snapshot_wire import snapshot_sha


class DescriptorError(RuntimeError):
    pass


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf8')


def _read_small(directory, name, maximum=16 * 1024 * 1024):
    directory.check_visible()
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory.fd)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_uid != os.getuid()
                or before.st_mode & 0o077
                or before.st_size > maximum):
            raise DescriptorError('staged metadata must be an independent private file')
        raw = stream.read(maximum + 1)
        after = os.fstat(stream.fileno())
        if (len(raw) > maximum or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)):
            raise DescriptorError('staged metadata changed while reading')
    visible = os.stat(name, dir_fd=directory.fd, follow_symlinks=False)
    if (visible.st_dev, visible.st_ino) != (before.st_dev, before.st_ino):
        raise DescriptorError('staged metadata path changed')
    directory.check_visible()
    return raw


def create_descriptor(*, job, server_identity, staging_root, jobs_root, staging_receipt):
    try:
        _validated(job)
        job, server_identity, staging_receipt = (json.loads(_json(value)) for value in
                                                  (job, server_identity, staging_receipt))
        if (not isinstance(server_identity, dict) or set(server_identity) != {'server_id', 'ledger_id'}
                or not isinstance(server_identity['server_id'], str) or not server_identity['server_id']
                or not _uuid(server_identity['ledger_id'])):
            raise DescriptorError('original server and ledger identity are required')
        staging_root, jobs_root = Path(staging_root), Path(jobs_root)
        if (not staging_root.is_absolute() or not jobs_root.is_absolute()
                or '..' in staging_root.parts or '..' in jobs_root.parts
                or staging_root.is_relative_to(jobs_root) or jobs_root.is_relative_to(staging_root)):
            raise DescriptorError('separate canonical staging and job roots are required')
        snapshot = staging_root / job['job_id']
        with _PinnedDirectory(staging_root) as staged_parent, _PinnedDirectory(jobs_root) as jobs_parent:
            with _PinnedDirectory(snapshot, parent=staged_parent) as staged:
                manifest_raw = _read_small(staged, 'transport-manifest.json')
                marker_raw = _read_small(staged, 'INPUT_VERIFIED.json')
                manifest, marker = json.loads(manifest_raw), json.loads(marker_raw)
                if (_json(manifest) != manifest_raw or snapshot_sha(manifest) != job['snapshot_sha256']
                        or any(manifest.get(k) != job[k] for k in
                               ('job_id', 'claim_id', 'identity', 'generation', 'manifest_sha256', 'code_sha256'))
                        or marker != staging_receipt or _json(marker) != marker_raw
                        or set(marker) != {'schema', 'job_id', 'claim_id', 'identity', 'generation',
                                           'manifest_sha256', 'snapshot_sha256', 'code_sha256', 'files',
                                           'evidence_scope', 'dependency_closure_verified'}
                        or marker.get('schema') != 'paperflow-remote-snapshot-verified/1'
                        or marker.get('evidence_scope') != 'declared_files_only'
                        or marker.get('dependency_closure_verified') is not False
                        or any(marker.get(k) != v for k, v in job.items())
                        or marker.get('files') != len(manifest['files'])):
                    raise DescriptorError('staging receipt or snapshot identity changed')
                descriptor = dict(schema='paperflow-service-staging/1', job=job,
                    server_identity=server_identity, snapshot_root=str(snapshot), staging_receipt=marker)
                jobs_parent.check_visible()
                with _PinnedDirectory(jobs_root / job['job_id'], create=True, parent=jobs_parent) as target:
                    for name, value in [('job.json', job), ('STAGING.json', descriptor)]:
                        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                     0o400, dir_fd=target.fd)
                        with os.fdopen(fd, 'wb') as stream:
                            stream.write(_json(value)); stream.flush(); os.fsync(stream.fileno())
                    os.fsync(target.fd)
                    target.check_visible()
                    staged.check_visible()
                    jobs_parent.check_visible()
                    return dict(job=job, server_identity=server_identity,
                                descriptor_sha256=hashlib.sha256(_json(descriptor)).hexdigest())
    except DescriptorError:
        raise
    except Exception as exc:
        raise DescriptorError('service staging descriptor was not safely created') from exc
