"""Detect Windows named streams; detection alone is never a delete proof.

The existing NAS writer saves default-stream bytes only.  A later deletion
worker must therefore hold each file securely and refuse any file with named
streams unless those streams are independently retained and verified.
"""

import ctypes
from ctypes import wintypes
import os
from pathlib import Path


def enumerate_streams(path):
    if os.name != "nt":
        raise OSError("Windows stream enumeration is unavailable")
    path = Path(path)
    if not path.is_absolute():
        raise ValueError("stream path must be absolute")

    class StreamData(ctypes.Structure):
        _fields_ = [("size", ctypes.c_longlong), ("name", ctypes.c_wchar * 296)]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.FindFirstStreamW.argtypes = [wintypes.LPCWSTR, ctypes.c_int, ctypes.POINTER(StreamData), wintypes.DWORD]
    kernel.FindFirstStreamW.restype = wintypes.HANDLE
    kernel.FindNextStreamW.argtypes = [wintypes.HANDLE, ctypes.POINTER(StreamData)]
    kernel.FindNextStreamW.restype = wintypes.BOOL
    kernel.FindClose.argtypes = [wintypes.HANDLE]
    kernel.FindClose.restype = wintypes.BOOL
    data = StreamData()
    handle = kernel.FindFirstStreamW(str(path), 0, ctypes.byref(data), 0)
    if handle == wintypes.HANDLE(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        streams = [{"name": data.name, "bytes": data.size}]
        while kernel.FindNextStreamW(handle, ctypes.byref(data)):
            streams.append({"name": data.name, "bytes": data.size})
        error = ctypes.get_last_error()
        if error != 38:  # ERROR_HANDLE_EOF
            raise ctypes.WinError(error)
    finally:
        kernel.FindClose(handle)
    if not streams or sum(row["name"].upper() == "::$DATA" for row in streams) != 1:
        raise ValueError("default file stream cannot be confirmed")
    return streams


def require_default_stream_only(path):
    streams = enumerate_streams(path)
    if any(row["name"].upper() != "::$DATA" for row in streams):
        raise ValueError("named streams are not covered by the current NAS archive")
