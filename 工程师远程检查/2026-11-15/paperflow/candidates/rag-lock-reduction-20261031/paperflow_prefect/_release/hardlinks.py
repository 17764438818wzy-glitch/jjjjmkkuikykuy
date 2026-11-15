"""Enumerate current Windows hard-link names; results are not a lock."""

import ctypes
from ctypes import wintypes
import os
from pathlib import Path


def enumerate_links(path):
    if os.name != "nt":
        raise OSError("Windows hard-link enumeration is unavailable")
    path = Path(path)
    if not path.is_absolute() or len(path.drive) != 2 or not path.drive.endswith(":"):
        raise ValueError("only absolute local drive paths are supported")
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetDriveTypeW.argtypes = [wintypes.LPCWSTR]
    kernel.GetDriveTypeW.restype = wintypes.UINT
    if kernel.GetDriveTypeW(path.anchor) != 3:
        raise ValueError("fixed local volume required")
    kernel.GetVolumePathNameW.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD]
    kernel.GetVolumePathNameW.restype = wintypes.BOOL
    volume = ctypes.create_unicode_buffer(32768)
    if not kernel.GetVolumePathNameW(str(path), volume, len(volume)):
        raise ctypes.WinError(ctypes.get_last_error())
    if os.path.normcase(volume.value) != os.path.normcase(path.anchor):
        raise ValueError("nested volume mounts are not supported")
    kernel.FindFirstFileNameW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD,
                                         ctypes.POINTER(wintypes.DWORD), wintypes.LPWSTR]
    kernel.FindFirstFileNameW.restype = wintypes.HANDLE
    kernel.FindNextFileNameW.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD), wintypes.LPWSTR]
    kernel.FindNextFileNameW.restype = wintypes.BOOL
    kernel.FindClose.argtypes = [wintypes.HANDLE]
    kernel.FindClose.restype = wintypes.BOOL
    buffer = ctypes.create_unicode_buffer(32768)
    size = wintypes.DWORD(len(buffer))
    handle = kernel.FindFirstFileNameW(str(path), 0, ctypes.byref(size), buffer)
    if handle == wintypes.HANDLE(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        names = [buffer.value]
        while True:
            size.value = len(buffer)
            if not kernel.FindNextFileNameW(handle, ctypes.byref(size), buffer):
                error = ctypes.get_last_error()
                if error != 38:
                    raise ctypes.WinError(error)
                break
            names.append(buffer.value)
    finally:
        kernel.FindClose(handle)
    paths = []
    for name in names:
        relative = Path(name.lstrip("\\/"))
        if not name.startswith("\\") or relative.is_absolute() or ".." in relative.parts or ":" in name:
            raise ValueError("unexpected hard-link name")
        paths.append(str(Path(path.anchor) / relative))
    if not paths or len({os.path.normcase(p) for p in paths}) != len(paths):
        raise ValueError("hard-link enumeration ambiguous")
    return tuple(sorted(paths, key=os.path.normcase))
