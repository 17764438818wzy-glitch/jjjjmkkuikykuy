"""Verify an opened source handle before reading any potentially private data."""
import os
from pathlib import Path
import sys


class SourceHandleError(ValueError):
    pass


def _opened_path(fd):
    if os.name == 'nt':
        import ctypes
        from ctypes import wintypes
        import msvcrt
        get_name = ctypes.WinDLL('kernel32', use_last_error=True).GetFinalPathNameByHandleW
        get_name.argtypes = [wintypes.HANDLE, wintypes.LPWSTR, wintypes.DWORD, wintypes.DWORD]
        get_name.restype = wintypes.DWORD
        handle = msvcrt.get_osfhandle(fd)
        length = get_name(handle, None, 0, 0)
        if not length:
            raise SourceHandleError('cannot resolve opened source handle')
        buffer = ctypes.create_unicode_buffer(length + 1)
        result = get_name(handle, buffer, len(buffer), 0)
        if not result or result >= len(buffer):
            raise SourceHandleError('cannot resolve opened source handle')
        name = buffer.value
        if name.startswith('\\\\?\\UNC\\'):
            return '\\\\' + name[8:]
        if name.startswith('\\\\?\\'):
            return name[4:]
        return name
    if sys.platform == 'linux':
        return os.readlink('/proc/self/fd/' + str(fd))
    raise SourceHandleError('opened source path cannot be verified on this platform')


def check_opened_source(fd, expected, roots):
    """Reject a redirected handle before reading; digest checks remain required."""
    try:
        name = _opened_path(fd)
        actual = os.path.normcase(os.path.normpath(name))
        required = os.path.normcase(os.path.normpath(str(Path(expected).absolute())))
        if not os.path.isabs(name) or actual != required:
            raise SourceHandleError('opened source does not match the allowed evidence path')
        actual_path = Path(name)
        if not any(actual_path.is_relative_to(root) for root in roots):
            raise SourceHandleError('opened source is outside the allowed roots')
    except (OSError, ValueError) as exc:
        if isinstance(exc, SourceHandleError):
            raise
        raise SourceHandleError('opened source handle is unavailable') from exc
