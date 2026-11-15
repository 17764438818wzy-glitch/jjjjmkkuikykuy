"""Validate Wine F: paths using Linux no-follow directory descriptors.

Only the private, already verified snapshot's F: tree is reachable. This is a
path-resolution helper; it does not prove a file's business identity or hash.
"""
import os
from pathlib import Path
import stat

from snapshot_import import _PinnedDirectory
from windows_snapshot import _windows_name


class UnsafeServerPath(RuntimeError):
    pass


def verify_snapshot_path(snapshot, windows_path):
    try:
        parsed, root_parts, components = _windows_name(windows_path)
        if (root_parts != ('drives', 'f') or str(parsed) != windows_path
                or not hasattr(os, 'O_PATH')):
            raise UnsafeServerPath('only an existing canonical snapshot F: path is allowed')
        root = Path(snapshot).absolute() / 'drives' / 'f'
        flags = os.O_PATH | os.O_NOFOLLOW
        with _PinnedDirectory(root) as pinned:
            held, identities = [os.dup(pinned.fd)], []
            try:
                def valid(info, index):
                    return (not stat.S_ISLNK(info.st_mode)
                            and (not stat.S_ISREG(info.st_mode) or info.st_nlink == 1)
                            and (index == len(components) - 1 or stat.S_ISDIR(info.st_mode))
                            and (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)))
                for index, component in enumerate(components):
                    fd = os.open(component, flags, dir_fd=held[-1])
                    held.append(fd)
                    info = os.fstat(fd)
                    if not valid(info, index):
                        raise UnsafeServerPath('snapshot path contains a link or unsupported entry')
                    identities.append((info.st_dev, info.st_ino))
                # Rewalk the visible names: an open-but-renamed ancestor is not
                # sufficient evidence that Wine will access the same object.
                visible = os.dup(pinned.fd)
                try:
                    for index, (component, expected) in enumerate(zip(components, identities)):
                        child = os.open(component, flags, dir_fd=visible)
                        os.close(visible)
                        visible = child
                        info = os.fstat(visible)
                        if not valid(info, index) or (info.st_dev, info.st_ino) != expected:
                            raise UnsafeServerPath('snapshot path changed while checking')
                finally:
                    os.close(visible)
                pinned.check_visible()
                if any(not valid(os.fstat(fd), index) for index, fd in enumerate(held[1:])):
                    raise UnsafeServerPath('snapshot link identity changed while checking')
                return True
            finally:
                for fd in reversed(held):
                    os.close(fd)
    except UnsafeServerPath:
        raise
    except Exception as exc:
        raise UnsafeServerPath('snapshot path cannot be safely resolved') from exc
