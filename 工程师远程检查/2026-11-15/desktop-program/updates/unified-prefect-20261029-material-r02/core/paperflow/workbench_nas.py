"""Bounded, read-only NAS selection over the existing owned upload contract."""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import threading
import uuid


def file_version(value):
    fields=(value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns)
    return hashlib.sha256(repr(fields).encode()).hexdigest()


def is_link(path):
    value=path.stat(follow_symlinks=False)
    return (stat.S_ISLNK(value.st_mode) or path.is_junction()
            or bool(getattr(value,'st_file_attributes',0)&0x400))


def open_descriptor(path,*,directory=False):
    """On Windows, hold a read handle that denies concurrent writes/deletion."""
    if os.name!='nt':return os.open(path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)|(getattr(os,'O_DIRECTORY',0) if directory else 0))
    import ctypes
    from ctypes import wintypes as w
    import msvcrt
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.CreateFileW.argtypes=[w.LPCWSTR,w.DWORD,w.DWORD,w.LPVOID,w.DWORD,w.DWORD,w.HANDLE]
    kernel.CreateFileW.restype=w.HANDLE
    kernel.CloseHandle.argtypes=[w.HANDLE]
    flags=0x00200000|(0x02000000 if directory else 0x08000000)
    handle=kernel.CreateFileW(str(path),0x80000000,1,None,3,flags,None)
    if handle==w.HANDLE(-1).value:raise ctypes.WinError(ctypes.get_last_error())
    try:descriptor=msvcrt.open_osfhandle(handle,os.O_RDONLY|os.O_BINARY)
    except BaseException:
        kernel.CloseHandle(handle);raise
    return descriptor


def open_source(path):return os.fdopen(open_descriptor(path),'rb')


def opened_path(stream):
    """Use the actual opened object, not another pre-open pathname check."""
    descriptor=stream if isinstance(stream,int) else stream.fileno()
    if os.name!='nt':return Path(os.readlink('/proc/self/fd/'+str(descriptor))).resolve(strict=True)
    import ctypes
    from ctypes import wintypes as w
    import msvcrt
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    call=kernel.GetFinalPathNameByHandleW
    call.argtypes=[w.HANDLE,w.LPWSTR,w.DWORD,w.DWORD];call.restype=w.DWORD
    buffer=ctypes.create_unicode_buffer(32768)
    count=call(msvcrt.get_osfhandle(descriptor),buffer,len(buffer),0)
    if not count or count>=len(buffer):raise ctypes.WinError(ctypes.get_last_error())
    name=buffer.value
    if name.startswith('\\\\?\\UNC\\'):name='\\\\'+name[8:]
    elif name.startswith('\\\\?\\'):name=name[4:]
    return Path(name)


@contextmanager
def guard_directory(path,root):
    descriptor=open_descriptor(path,directory=True)
    try:
        initial=os.fstat(descriptor)
        if (opened_path(descriptor)!=path or not path.is_relative_to(root)
                or not stat.S_ISDIR(initial.st_mode) or getattr(initial,'st_file_attributes',0)&0x400):
            raise ValueError('NAS 文件夹的实际位置不在所选目录内')
        yield
        final=path.stat(follow_symlinks=False)
        if (opened_path(descriptor)!=path or is_link(path)
                or (initial.st_dev,initial.st_ino)!=(final.st_dev,final.st_ino)):
            raise ValueError('NAS 文件夹已经改变，请刷新后重试')
    finally:os.close(descriptor)


class NasInputs:
    def __init__(self,workbench):
        self.workbench=workbench
        self.lock=threading.Lock()

    @contextmanager
    def reserve(self):
        # Called on the event loop BEFORE dispatching to the shared thread pool.
        if not self.lock.acquire(blocking=False):raise RuntimeError('NAS 正在读取，请稍后再试；暂停仍可使用')
        try:yield
        finally:self.lock.release()

    def roots(self):
        config=self.workbench.root/'nas-roots.json'
        if not config.exists():return {'roots':[]}
        try:value=json.loads(config.read_text(encoding='utf-8-sig'))
        except (OSError,ValueError) as exc:raise RuntimeError('NAS 目录配置暂不可读') from exc
        rows=value.get('roots') if isinstance(value,dict) else None
        if not isinstance(rows,list) or len(rows)>10:raise RuntimeError('NAS 目录配置无效')
        seen=set();result=[]
        for row in rows:
            if (not isinstance(row,dict) or not isinstance(row.get('id'),str)
                    or not re.fullmatch(r'[a-zA-Z0-9_-]{1,40}',row['id']) or row['id'] in seen
                    or not isinstance(row.get('name'),str) or not 1<=len(row['name'])<=80
                    or not isinstance(row.get('path'),str) or not Path(row['path']).is_absolute()):
                raise RuntimeError('NAS 目录配置无效')
            seen.add(row['id']);result.append({k:row[k] for k in ('id','name','path')})
        return {'roots':result}

    def resolve(self,root_id,relative):
        roots={r['id']:r for r in self.roots()['roots']}
        if not isinstance(root_id,str) or root_id not in roots:raise ValueError('请选择已配置的 NAS 目录')
        if (not isinstance(relative,str) or len(relative)>2048 or
                re.search(r'[\\:<>"|?*\x00-\x1f]',relative) or
                (relative and any(not p or p in ('.','..') or p.endswith((' ','.')) for p in relative.split('/')))):
            raise ValueError('NAS 相对路径无效，不能访问指定目录之外的文件')
        root=Path(roots[root_id]['path'])
        for parent in (*reversed(root.parents),root):
            if is_link(parent):raise ValueError('NAS 链接目录不能用于选稿')
        root=root.resolve(strict=True);path=root
        for part in relative.split('/') if relative else []:
            path=path/part
            if is_link(path):raise ValueError('NAS 链接不能用于选稿')
        path=path.resolve(strict=True)
        if not path.is_relative_to(root):raise ValueError('NAS 路径越过了指定目录')
        return roots[root_id],root,path

    def browse(self,root_id,relative='',offset=0,limit=100):
        if type(offset) is not int or not 0<=offset<=100000 or type(limit) is not int or not 1<=limit<=100:
            raise ValueError('NAS 分页参数无效')
        try:
            row,root,path=self.resolve(root_id,relative)
            if not path.is_dir():raise ValueError('请选择 NAS 文件夹')
            entries=[];next_offset=None
            from .workbench import FORMATS
            with guard_directory(path,root):
                with os.scandir(path) as stream:
                    position=0;examined=0
                    for entry in stream:
                        position+=1
                        if position<=offset:continue
                        examined+=1
                        p=Path(entry.path)
                        try:
                            if not entry.name.startswith(('#','.','@')) and not is_link(p):
                                # DirEntry.stat on Windows has zero dev/ino.
                                value=p.stat(follow_symlinks=False)
                                directory=stat.S_ISDIR(value.st_mode)
                                if directory or (stat.S_ISREG(value.st_mode) and p.suffix.lower() in FORMATS):
                                    entries.append(dict(name=entry.name,path=(relative+'/' if relative else '')+entry.name,
                                        kind='directory' if directory else 'file',bytes=None if directory else value.st_size,
                                        version=None if directory else file_version(value)))
                        except OSError:pass
                        if len(entries)>=limit or examined>=200:
                            if next(stream,None) is not None:next_offset=position
                            break
                _,current_root,current_path=self.resolve(root_id,relative)
                if current_root!=root or current_path!=path:raise ValueError('NAS 文件夹已经改变，请刷新后重试')
            entries.sort(key=lambda e:(e['kind']!='directory',e['name'].casefold()))
            return dict(root=row['id'],name=row['name'],path=relative,
                parent=relative.rpartition('/')[0] if relative else None,
                entries=entries,next_offset=next_offset)
        except OSError as exc:raise RuntimeError('NAS 目录暂时无法读取，请检查连接、路径和读取权限后刷新') from exc

    def import_file(self,root_id,relative,version):
        folder=None;target=None
        try:
            from .workbench import MAX_UPLOAD
            _,root,path=self.resolve(root_id,relative)
            self.workbench.check_name(path.name)
            before=path.stat()
            if not stat.S_ISREG(before.st_mode):raise ValueError('请选择 NAS 文件，不是文件夹')
            if not isinstance(version,str) or file_version(before)!=version:
                raise ValueError('NAS 文件已经改变，请刷新目录后重新选择')
            if not 0<before.st_size<=MAX_UPLOAD:raise ValueError('文件不能为空，且不能超过 2 GiB')
            reserve=max(0,self.workbench.config.min_free_gib)*1024**3
            if shutil.disk_usage(self.workbench.root).free<before.st_size+reserve:
                raise RuntimeError('电脑可用磁盘不足，暂不能准备 NAS 输入副本')
            with open_source(path) as source:
                actual=opened_path(source)
                if not actual.is_relative_to(root) or actual!=path:
                    raise ValueError('NAS 文件的实际位置不在所选目录内')
                if file_version(os.fstat(source.fileno()))!=version:
                    raise ValueError('NAS 文件已经改变，请刷新目录后重新选择')
                folder=self.workbench.root/'uploads'/uuid.uuid4().hex
                folder.mkdir(parents=True)
                target=folder/path.name;count=0
                with target.open('xb') as destination:
                    while block:=source.read(1024*1024):
                        count+=len(block)
                        if count>MAX_UPLOAD:raise ValueError('文件超过 2 GiB')
                        destination.write(block)
                    destination.flush();os.fsync(destination.fileno())
                if (count!=before.st_size or file_version(os.fstat(source.fileno()))!=version
                        or file_version(path.stat())!=version or is_link(path)):
                    raise ValueError('NAS 文件在复制期间发生改变，未加入待处理')
            result=self.workbench.record_upload(path.name,target,relative_path=relative)
            return {k:result[k] for k in ('id','name','bytes','created','relative_path')}
        except BaseException as exc:
            # Only our own local UUID copy is removed; the NAS is never written.
            if folder:
                try:
                    if target:target.unlink(missing_ok=True)
                    folder.rmdir()
                except OSError:pass
            if isinstance(exc,OSError):raise RuntimeError('NAS 读取或本地复制未完成，请检查连接和空间后重试') from exc
            raise
