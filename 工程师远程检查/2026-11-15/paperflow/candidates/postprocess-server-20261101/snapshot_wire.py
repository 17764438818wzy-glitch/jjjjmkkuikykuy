"""Stage one immutable-identity folder snapshot; never launch work here."""
import hashlib
import json
from pathlib import Path
import re
import os

from remote_jobs import _validated, JobConflict
from snapshot_import import SnapshotImporter, SnapshotImportError, _PinnedDirectory


class SnapshotWireError(ValueError):
    pass


def _encode(value):
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(',', ':'), allow_nan=False).encode('utf8')
    except (ValueError, TypeError) as exc:
        raise SnapshotWireError('snapshot cannot be encoded') from exc


def snapshot_sha(manifest):
    return hashlib.sha256(_encode(manifest)).hexdigest()


def receive_snapshot(root, manifest, expected_sha, stream, *, _parent=None):
    if (not isinstance(manifest, dict) or manifest.get('schema') != 'paperflow-remote-snapshot/1'
            or not isinstance(expected_sha, str) or not re.fullmatch('[0-9a-f]{64}', expected_sha)
            or snapshot_sha(manifest) != expected_sha):
        raise SnapshotWireError('snapshot identity is invalid')
    try:
        job = {key: manifest[key] for key in ('job_id', 'claim_id', 'identity', 'generation',
                                               'manifest_sha256', 'code_sha256')}
        job['snapshot_sha256'] = expected_sha
        _validated(job)
        inputs = manifest['inputs']
        if (not isinstance(inputs, dict) or inputs.get('identity') != job['identity']
                or inputs.get('manifest_sha256') != job['manifest_sha256']
                or not isinstance(inputs.get('body'), dict)
                or inputs['body'].get('identity') != job['identity']
                or not isinstance(inputs.get('attachments'), list)
                or any(not isinstance(x, dict) or x.get('ticket') != job['identity']
                       for x in inputs['attachments'])):
            raise SnapshotWireError('snapshot belongs to a different folder')
        files = manifest['files']
        if not isinstance(files, list) or not files:
            raise SnapshotWireError('snapshot evidence is missing')
    except (KeyError, JobConflict) as exc:
        raise SnapshotWireError('snapshot folder identity is invalid') from exc

    root = Path(root).absolute()
    try:
        with _PinnedDirectory(root, create=True, parent=_parent) as directory:
            importer = SnapshotImporter(root, files, _directory=directory)
            marker_identity = None
            try:
                for row in files:
                    importer.put(row['path'], stream)
                importer.verify_complete()
                record = {'schema': 'paperflow-remote-snapshot-verified/1',
                          'job_id': job['job_id'], 'claim_id': job['claim_id'], 'identity': job['identity'],
                          'generation': job['generation'], 'manifest_sha256': job['manifest_sha256'],
                          'snapshot_sha256': expected_sha, 'code_sha256': job['code_sha256'],
                          'files': len(files), 'evidence_scope': 'declared_files_only',
                          'dependency_closure_verified': False}
                # Install metadata through the same pinned private directory as
                # the evidence. No success is returned for a replaced ancestor.
                for name, value in [('transport-manifest.json', manifest), ('INPUT_VERIFIED.json', record)]:
                    importer._check_root()
                    fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                 0o400, dir_fd=importer._root_fd)
                    with os.fdopen(fd, 'wb') as out:
                        if name == 'INPUT_VERIFIED.json':
                            info = os.fstat(out.fileno())
                            marker_identity = info.st_dev, info.st_ino
                        out.write(_encode(value))
                        out.flush()
                        os.fsync(out.fileno())
                os.fsync(importer._root_fd)
                importer._check_root()
            except BaseException as exc:
                # A failed final ancestry check must not leave OUR unaccepted
                # marker behind in a detached original directory. Never unlink
                # an existing/replaced record owned by somebody else.
                if marker_identity is not None:
                    try:
                        current = os.stat('INPUT_VERIFIED.json', dir_fd=importer._root_fd,
                                          follow_symlinks=False)
                        if (current.st_dev, current.st_ino) == marker_identity:
                            os.unlink('INPUT_VERIFIED.json', dir_fd=importer._root_fd)
                            os.fsync(importer._root_fd)
                    except FileNotFoundError:
                        pass
                    except OSError as cleanup_error:
                        if hasattr(exc, 'add_note'):
                            exc.add_note('could not remove unaccepted marker: ' + type(cleanup_error).__name__)
                raise
            finally:
                importer.close()
        return record
    except (OSError, SnapshotImportError) as exc:
        raise SnapshotWireError('snapshot transfer is incomplete or unsafe') from exc
