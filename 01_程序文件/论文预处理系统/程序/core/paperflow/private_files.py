"""Bounded private-file reads bound to the opened object, not a prior path check."""
import os
from pathlib import Path
import stat


def _dos_alias(value):
    # resolve() can retain an extended prefix for missing descendants while a
    # concurrent worker creates their common parent. Only standard DOS/UNC
    # aliases are equivalent; device/volume namespaces must remain distinct.
    if os.name=='nt':
        if value.startswith('\\\\?\\UNC\\'):return '\\\\'+value[8:]
        if (value.startswith('\\\\?\\') and len(value)>6
                and value[4].isascii() and value[4].isalpha() and value[5:7]==':\\'):
            return value[4:]
    return value


def resolve_private_path(path):
    return Path(_dos_alias(str(Path(path).resolve())))


def _ordinary(root, path):
    for item in (path, *path.parents):
        if item.is_symlink() or item.is_junction():raise ValueError('private file link')
    if not resolve_private_path(path).is_relative_to(resolve_private_path(root)):raise ValueError('private file outside owner')


def read_owned_bytes(root, path, max_bytes):
    root,path=Path(root).absolute(),Path(path).absolute()
    _ordinary(root,path)
    with path.open('rb') as stream:
        info=os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode):raise ValueError('private file is not regular')
        if os.name=='nt':
            import ctypes
            from ctypes import wintypes
            import msvcrt
            kernel=ctypes.WinDLL('kernel32',use_last_error=True)
            final=kernel.GetFinalPathNameByHandleW
            final.argtypes=(wintypes.HANDLE,wintypes.LPWSTR,wintypes.DWORD,wintypes.DWORD)
            final.restype=wintypes.DWORD
            handle=msvcrt.get_osfhandle(stream.fileno())
            size=final(handle,None,0,0)
            if not size or size>32768:raise OSError('private file handle path unavailable')
            buffer=ctypes.create_unicode_buffer(size+1)
            length=final(handle,buffer,size+1,0)
            if not length or length>size:raise OSError('private file handle path unavailable')
            actual=_dos_alias(buffer.value)
            if os.path.normcase(os.path.normpath(actual))!=os.path.normcase(os.path.normpath(_dos_alias(str(path)))):
                raise ValueError('private file handle escaped checked path')
        else:
            fd_path=Path('/proc/self/fd')/str(stream.fileno())
            if fd_path.exists() and fd_path.resolve()!=path:
                raise ValueError('private file handle escaped checked path')
        _ordinary(root,path)
        current=os.stat(path,follow_symlinks=False)
        if (current.st_dev,current.st_ino)!=(info.st_dev,info.st_ino):
            raise ValueError('private file replaced during open')
        raw=stream.read(max_bytes+1)
        if len(raw)>max_bytes:raise ValueError('private file exceeds limit')
        return raw
