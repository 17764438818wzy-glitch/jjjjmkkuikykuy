"""Get a file object's Windows 128-bit identity from an already open handle."""

import ctypes
from ctypes import wintypes
import os


def identity_from_stream(stream):
    if os.name != "nt":
        raise OSError("Windows handle identity is unavailable")
    import msvcrt

    class FileIdInfo(ctypes.Structure):
        _fields_ = [("volume_serial", ctypes.c_ulonglong), ("file_id", ctypes.c_ubyte * 16)]

    handle = msvcrt.get_osfhandle(stream.fileno())
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetFileInformationByHandleEx.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    kernel.GetFileInformationByHandleEx.restype = wintypes.BOOL
    info = FileIdInfo()
    if not kernel.GetFileInformationByHandleEx(handle, 0x12, ctypes.byref(info), ctypes.sizeof(info)):
        raise ctypes.WinError(ctypes.get_last_error())
    file_id = bytes(info.file_id)
    if info.volume_serial == 0 or file_id in {bytes(16), bytes([255]) * 16}:
        raise ValueError("filesystem did not provide a usable file identity")
    return {"volume_serial": info.volume_serial, "file_id": file_id.hex()}
