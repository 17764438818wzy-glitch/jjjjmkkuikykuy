"""Exclusive server-side material import through directory file descriptors.

This validates only the declared transport list; upstream must separately prove
that the list is the complete closure of a particular original folder claim.
"""
import hashlib
import ctypes
import errno
import os
from pathlib import Path, PureWindowsPath
import stat

from windows_snapshot import EvidenceMap, EvidenceError


class SnapshotImportError(ValueError):
    pass


class _PinnedDirectory:
    """Pin every ancestor from '/' using only relative no-follow directory opens.

    A dir fd prevents a rename/symlink race from redirecting an operation. The
    independent visible rewalk additionally rejects a detached or replaced
    ancestor, including one that still leads to the same final directory inode.
    Only the final directory must be private; system ancestors such as /tmp are
    checked for identity and no-follow traversal, not private ownership.
    """
    def __init__(self, path, *, create=False, parent=None):
        self.path = Path(path).absolute()
        self._fds, self._parts, self._identities = [], [], []
        self._complete = False
        if (os.name != 'posix' or not hasattr(os, 'O_NOFOLLOW')
                or self.path.anchor != '/' or '..' in self.path.parts):
            raise SnapshotImportError('absolute Linux directory without parent traversal required')
        try:
            if parent is None:
                fd = os.open('/', self._flags())
                self._fds.append(fd)
                self._identities.append(self._identity(os.fstat(fd)))
                parts = self.path.parts[1:]
            else:
                if not isinstance(parent, _PinnedDirectory) or parent.path != self.path.parent:
                    raise SnapshotImportError('snapshot parent binding is invalid')
                parent.check_visible()
                for fd in parent._fds:
                    self._fds.append(os.dup(fd))
                self._parts = list(parent._parts)
                self._identities = list(parent._identities)
                parts = (self.path.name,)
            if create and not parts:
                raise SnapshotImportError('filesystem root cannot be a new snapshot')
            for index, part in enumerate(parts):
                self.check_visible()
                if create and index == len(parts) - 1:
                    # Exclusive creation under a fixed parent, never an absolute
                    # pathname checked separately before mkdir/open.
                    os.mkdir(part, 0o700, dir_fd=self.fd)
                    os.fsync(self.fd)
                child = os.open(part, self._flags(), dir_fd=self.fd)
                self._fds.append(child)
                self._parts.append(part)
                self._identities.append(self._identity(os.fstat(child)))
                self.check_visible()
            self._complete = True
            self.check_visible()
        except BaseException as exc:
            self.close()
            if isinstance(exc, OSError):
                raise SnapshotImportError('snapshot directory ancestry is missing, changed or unsafe') from exc
            raise

    @staticmethod
    def _flags():
        return os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW

    @staticmethod
    def _identity(info):
        return info.st_dev, info.st_ino

    @property
    def fd(self):
        if not self._fds:
            raise SnapshotImportError('snapshot directory binding is closed')
        return self._fds[-1]

    def check_visible(self):
        if not self._fds:
            raise SnapshotImportError('snapshot directory binding is closed')
        visible = None
        try:
            visible = os.open('/', self._flags())
            if self._identity(os.fstat(visible)) != self._identities[0]:
                raise SnapshotImportError('filesystem root identity changed')
            for part, expected in zip(self._parts, self._identities[1:]):
                child = os.open(part, self._flags(), dir_fd=visible)
                os.close(visible)
                visible = child
                if self._identity(os.fstat(visible)) != expected:
                    raise SnapshotImportError('snapshot directory ancestor changed')
            if self._complete:
                info = os.fstat(visible)
                if info.st_uid != os.getuid() or info.st_mode & 0o077:
                    raise SnapshotImportError('snapshot directory must be private to the executor')
        except OSError as exc:
            raise SnapshotImportError('snapshot directory ancestry is unavailable or unsafe') from exc
        finally:
            if visible is not None:
                os.close(visible)

    def clone(self):
        self.check_visible()
        other = object.__new__(type(self))
        other.path, other._fds = self.path, []
        other._parts, other._identities = list(self._parts), list(self._identities)
        other._complete = self._complete
        try:
            for fd in self._fds:
                other._fds.append(os.dup(fd))
            other.check_visible()
            return other
        except BaseException:
            other.close()
            raise

    def close(self):
        descriptors, self._fds = getattr(self, '_fds', []), []
        for fd in reversed(descriptors):
            os.close(fd)

    def __enter__(self):
        self.check_visible()
        return self

    def __exit__(self, *exc):
        self.close()

    def __del__(self):
        self.close()


class SnapshotImporter:
    def __init__(self, root, files, *, _directory=None):
        if os.name != 'posix' or not hasattr(os, 'O_NOFOLLOW') or not hasattr(os, 'O_TMPFILE'):
            raise SnapshotImportError('server Linux no-follow and anonymous temporary files required')
        self.root = Path(root).absolute()
        self._directory = None
        try:
            if _directory is not None:
                if not isinstance(_directory, _PinnedDirectory) or _directory.path != self.root:
                    raise SnapshotImportError('snapshot root binding is invalid')
                self._directory = _directory.clone()
            else:
                self._directory = _PinnedDirectory(self.root)
            self._root_fd = self._directory.fd
            # Existing release/run-wire callers bind this original root inode.
            opened = os.fstat(self._root_fd)
            self._root_identity = opened.st_dev, opened.st_ino
            if not isinstance(files, list) or not files:
                raise SnapshotImportError('empty evidence list')
            mapper, self.files, seen = EvidenceMap(self.root), {}, set()
            for row in files:
                if not isinstance(row, dict) or not {'path', 'sha256', 'bytes'} <= set(row):
                    raise SnapshotImportError('invalid evidence entry')
                key = str(PureWindowsPath(row['path'])).lower()
                if key in seen:
                    raise SnapshotImportError('duplicate evidence identity')
                seen.add(key)
                physical = mapper.bind(row['path'], row['sha256'], row['bytes'])
                self.files[row['path']] = {'relative': physical.relative_to(self.root).parts,
                                           'sha256': row['sha256'], 'bytes': row['bytes']}
            self._check_root()
        except (OSError, EvidenceError, TypeError, KeyError) as exc:
            self.close()
            raise SnapshotImportError('invalid evidence or snapshot root') from exc
        except BaseException:
            self.close()
            raise

    def close(self):
        directory, self._directory = getattr(self, '_directory', None), None
        self._root_fd = None
        if directory is not None:
            directory.close()

    def __del__(self):
        self.close()

    def _check_root(self):
        if self._directory is None:
            raise SnapshotImportError('snapshot root binding is closed')
        self._directory.check_visible()

    def _parent(self, parts, *, create):
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        self._check_root()
        fd = os.dup(self._root_fd)
        try:
            for part in parts:
                if create:
                    try:
                        os.mkdir(part, 0o700, dir_fd=fd)
                        os.fsync(fd)
                    except FileExistsError:
                        pass
                child = os.open(part, flags, dir_fd=fd)
                info = os.fstat(child)
                if info.st_uid != os.getuid() or info.st_mode & 0o077:
                    os.close(child)
                    raise SnapshotImportError('snapshot directory is not private')
                os.close(fd)
                fd = child
            return fd
        except BaseException:
            os.close(fd)
            raise

    def _check_name(self, row, expected):
        """Rewalk the visible path and ensure it still names the opened file."""
        parent = self._parent(row['relative'][:-1], create=False)
        try:
            current = os.stat(row['relative'][-1], dir_fd=parent, follow_symlinks=False)
            if (not stat.S_ISREG(current.st_mode) or current.st_nlink != 1
                    or (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino)):
                raise SnapshotImportError('snapshot path changed')
        finally:
            os.close(parent)

    def put(self, source, stream):
        if source not in self.files:
            raise SnapshotImportError('unregistered evidence source')
        row = self.files[source]
        try:
            parent = self._parent(row['relative'][:-1], create=True)
        except OSError as exc:
            raise SnapshotImportError('unsafe snapshot directory') from exc
        try:
            # A nameless inode cannot be replaced between the hash and commit.
            # linkat(AT_EMPTY_PATH) is an exclusive install under the pinned fd.
            file_fd = os.open('.', os.O_RDWR | os.O_TMPFILE, 0o600, dir_fd=parent)
            with os.fdopen(file_fd, 'w+b') as out:
                digest, remaining = hashlib.sha256(), row['bytes']
                while remaining:
                    data = stream.read(min(1024 * 1024, remaining))
                    if not isinstance(data, bytes) or not data or len(data) > remaining:
                        raise SnapshotImportError('evidence changed or transfer incomplete')
                    digest.update(data)
                    remaining -= len(data)
                    out.write(data)
                out.flush()
                os.fsync(out.fileno())
                if digest.hexdigest() != row['sha256']:
                    raise SnapshotImportError('evidence changed during transfer')
                self._check_root()
                os.fchmod(out.fileno(), 0o400)
                libc = ctypes.CDLL(None, use_errno=True)
                result = libc.linkat(out.fileno(), ctypes.c_char_p(b''), parent,
                                     ctypes.c_char_p(os.fsencode(row['relative'][-1])), 0x1000)
                if result != 0:
                    error = ctypes.get_errno()
                    if error == errno.EEXIST:
                        raise SnapshotImportError('snapshot evidence already exists')
                    raise OSError(error, os.strerror(error))
                os.fsync(parent)
                self._check_name(row, os.fstat(out.fileno()))
        except OSError as exc:
            raise SnapshotImportError('snapshot evidence could not be written') from exc
        finally:
            os.close(parent)

    def verify_complete(self):
        for row in self.files.values():
            try:
                parent = self._parent(row['relative'][:-1], create=False)
                try:
                    fd = os.open(row['relative'][-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                    with os.fdopen(fd, 'rb') as stream:
                        before = os.fstat(stream.fileno())
                        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                            raise SnapshotImportError('snapshot evidence is not an independent regular file')
                        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
                        after = os.fstat(stream.fileno())
                        if (digest != row['sha256'] or before.st_size != row['bytes']
                                or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)):
                            raise SnapshotImportError('snapshot evidence changed')
                        self._check_root()
                        self._check_name(row, after)
                finally:
                    os.close(parent)
            except OSError as exc:
                raise SnapshotImportError('snapshot evidence is missing or unsafe') from exc
        return True
