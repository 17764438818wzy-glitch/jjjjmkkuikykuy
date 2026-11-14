"""Pin existing local directory names for a bounded Windows operation.

The access/share flags follow the native archive implementation and its
Windows link tests. This is a namespace guard, not a consumer fence, delivery
proof, consent record, or a lock over all possible hard-link names. It never
creates a directory and cannot authorize a delete.
"""

from contextlib import contextmanager, ExitStack
import ctypes
from ctypes import wintypes as w
import os
from pathlib import Path


def _lexical(value):
    path = Path(value)
    if (not path.is_absolute() or len(path.drive) != 2 or path.drive[1] != ":"
            or ".." in path.parts or any(":" in part for part in path.parts[1:])
            or any(part.endswith((".", " ")) for part in path.parts[1:])):
        raise ValueError("an unambiguous absolute local drive path is required")
    return path


def _kernel():
    if os.name != "nt":
        raise ValueError("namespace guards require Windows")
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateFileW.argtypes = [w.LPCWSTR, w.DWORD, w.DWORD, w.LPVOID, w.DWORD, w.DWORD, w.HANDLE]
    kernel.CreateFileW.restype = w.HANDLE
    kernel.CloseHandle.argtypes = [w.HANDLE]
    kernel.CloseHandle.restype = w.BOOL
    kernel.GetFileInformationByHandleEx.argtypes = [w.HANDLE, ctypes.c_int, w.LPVOID, w.DWORD]
    kernel.GetFileInformationByHandleEx.restype = w.BOOL
    kernel.GetFinalPathNameByHandleW.argtypes = [w.HANDLE, w.LPWSTR, w.DWORD, w.DWORD]
    kernel.GetFinalPathNameByHandleW.restype = w.DWORD
    kernel.GetDriveTypeW.argtypes = [w.LPCWSTR]
    kernel.GetDriveTypeW.restype = w.UINT
    kernel.GetVolumePathNameW.argtypes = [w.LPCWSTR, w.LPWSTR, w.DWORD]
    kernel.GetVolumePathNameW.restype = w.BOOL
    return kernel


def _open_directory(kernel, path, exclusive):
    # FILE_LIST_DIRECTORY; omit DELETE sharing for every ancestor, and
    # use the tested exclusive leaf-directory sharing for affected parents.
    handle = kernel.CreateFileW(str(path), 1, 0 if exclusive else 3, None, 3,
                                0x02000000 | 0x00200000, None)
    if handle == w.HANDLE(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    return handle


def _observed(kernel, handle, expected):
    class Attributes(ctypes.Structure):
        _fields_ = [("attributes", w.DWORD), ("tag", w.DWORD)]

    class Identity(ctypes.Structure):
        _fields_ = [("volume", ctypes.c_ulonglong), ("file_id", ctypes.c_ubyte * 16)]

    attributes, identity = Attributes(), Identity()
    for kind, value in ((9, attributes), (18, identity)):
        if not kernel.GetFileInformationByHandleEx(handle, kind, ctypes.byref(value), ctypes.sizeof(value)):
            raise ctypes.WinError(ctypes.get_last_error())
    if not attributes.attributes & 0x10 or attributes.attributes & 0x400:
        raise ValueError("a directory is a reparse point or has changed type")
    buffer = ctypes.create_unicode_buffer(32768)
    count = kernel.GetFinalPathNameByHandleW(handle, buffer, len(buffer), 0)
    if not count or count >= len(buffer):
        raise ValueError("opened directory name could not be checked")
    actual = buffer.value
    if actual.startswith("\\\\?\\"):
        actual = actual[4:]
    if Path(actual) != expected:
        raise ValueError("opened directory name differs from the requested path")
    ident = bytes(identity.file_id)
    if not identity.volume or ident in {bytes(16), b"\xff" * 16}:
        raise ValueError("directory object identity unavailable")
    return identity.volume, ident.hex()


class _HeldDirectories:
    def __init__(self, kernel, pinned, leaves):
        self._kernel, self._pinned, self._closed = kernel, pinned, False
        self._leaves = leaves

    def assert_held(self):
        if self._closed:
            raise ValueError("namespace guard is closed")
        for path, handle, expected in self._pinned:
            if _observed(self._kernel, handle, path) != expected:
                raise ValueError("pinned directory identity changed")

    def link_from_stream(self, stream, destination):
        """Low-level no-replace link; caller must prove consent and content.

        FILE_LINK_INFORMATION binds the source object and target directory
        handles. No pathname-based or unlock/relock fallback is allowed.
        https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_file_link_information
        """
        import msvcrt
        from paperflow.workbench_nas import opened_path
        from .file_identity import identity_from_stream

        self.assert_held()
        destination = _lexical(destination)
        source = _lexical(opened_path(stream))
        if (destination.parent not in self._leaves or source.parent not in self._leaves
                or destination.name in {"", ".", ".."} or source == destination):
            raise ValueError("source and target parents must both be pinned")
        parent, expected = next((handle, identity) for path, handle, identity in self._pinned
                                if path == destination.parent)
        if identity_from_stream(stream)["volume_serial"] != expected[0]:
            raise ValueError("hard-link restoration cannot cross volumes")

        class Link(ctypes.Structure):
            _fields_ = [("replace", ctypes.c_ubyte), ("directory", w.HANDLE),
                        ("length", w.DWORD), ("name", w.WCHAR * 1)]

        class IoStatus(ctypes.Structure):
            _fields_ = [("status", ctypes.c_void_p), ("information", ctypes.c_size_t)]

        name = destination.name.encode("utf-16-le")
        raw = ctypes.create_string_buffer(max(ctypes.sizeof(Link), Link.name.offset + len(name)))
        value = Link.from_buffer(raw)
        value.replace, value.directory, value.length = 0, parent, len(name)
        ctypes.memmove(ctypes.addressof(raw) + Link.name.offset, name, len(name))
        native = ctypes.WinDLL("ntdll")
        native.NtSetInformationFile.argtypes = [w.HANDLE, ctypes.POINTER(IoStatus), w.LPVOID, w.ULONG, ctypes.c_int]
        native.NtSetInformationFile.restype = w.LONG
        native.RtlNtStatusToDosError.argtypes = [w.LONG]
        native.RtlNtStatusToDosError.restype = w.ULONG
        status = native.NtSetInformationFile(msvcrt.get_osfhandle(stream.fileno()),
                                             ctypes.byref(IoStatus()), raw, len(raw), 11)
        if status != 0:
            raise ctypes.WinError(native.RtlNtStatusToDosError(status))
        self.assert_held()


@contextmanager
def hold_directories(directories, allowed_roots, *, lock_children=True):
    """Pin directory identities; optionally prohibit child-name operations.

    Hard-link creation requires ``lock_children=False``: the kernel opens
    the destination namespace internally. Source-object and target-directory
    handles plus no-replace semantics still bind that link operation. This
    mode does not freeze child names or replace a consumer/operation lock.
    """
    if (not isinstance(directories, (list, tuple)) or not directories
            or not isinstance(allowed_roots, (list, tuple)) or not allowed_roots
            or type(lock_children) is not bool):
        raise ValueError("finite nonempty directory and root lists are required")
    requested = {_lexical(path) for path in directories}
    roots = {_lexical(path) for path in allowed_roots}
    if any(not any(path.is_relative_to(root) for root in roots) for path in requested):
        raise ValueError("directory is outside the approved roots")
    # Lexical checks finish before any directory handle is opened.
    kernel = _kernel()
    ancestors = {parent for path in requested for parent in (path, *path.parents)}
    pinned = []
    with ExitStack() as stack:
        for path in sorted(ancestors, key=lambda item: (len(item.parts), os.path.normcase(str(item)))):
            if kernel.GetDriveTypeW(path.anchor) != 3:
                raise ValueError("namespace operations require fixed local volumes")
            handle = _open_directory(kernel, path, lock_children and path in requested)
            stack.callback(kernel.CloseHandle, handle)
            pinned.append((path, handle, _observed(kernel, handle, path)))
            volume = ctypes.create_unicode_buffer(32768)
            if not kernel.GetVolumePathNameW(str(path), volume, len(volume)):
                raise ctypes.WinError(ctypes.get_last_error())
            if Path(volume.value) != Path(path.anchor):
                raise ValueError("nested volume mounts are not supported")
        guard = _HeldDirectories(kernel, pinned, requested)
        try:
            guard.assert_held()
            yield guard
        finally:
            guard._closed = True
