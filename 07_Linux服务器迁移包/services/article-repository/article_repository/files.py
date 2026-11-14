"""Read-only NAS access and one-article archives; no processing-PC dependency."""
import hashlib
import errno
import os
from pathlib import Path,PurePosixPath,PureWindowsPath
import shutil
import tempfile
import zipfile
from contextlib import contextmanager


class Files:
    def __init__(self,root,*,username=None,password=None):
        self.root=root.rstrip('/\\')
        self.smb=self.root.startswith('\\\\')
        if self.smb:
            import smbclient
            from smbprotocol.exceptions import SMBException
            server=PureWindowsPath(root).anchor.strip('\\').split('\\')[0]
            def session():
                smbclient.register_session(server,username=username,password=password,connection_timeout=8)
            def stat(path):
                try:
                    session(); return smbclient.stat(path)
                except FileNotFoundError: raise
                except (ValueError,OSError,SMBException) as exc:
                    if isinstance(exc,OSError) and exc.errno in (errno.ENOENT,errno.ENOTDIR): raise FileNotFoundError(path) from exc
                    raise OSError('NAS 暂不可读取') from exc
            @contextmanager
            def read(path,mode):
                try:
                    session()
                    with smbclient.open_file(path,mode) as stream: yield stream
                except FileNotFoundError: raise
                except (ValueError,OSError,SMBException) as exc:
                    if isinstance(exc,OSError) and exc.errno in (errno.ENOENT,errno.ENOTDIR): raise FileNotFoundError(path) from exc
                    raise OSError('NAS 暂不可读取') from exc
            self.stat=stat
            self.open=read
        else:
            self.stat=os.stat
            self.open=open

    def path(self,file):
        relative=file.get('relative') or ''
        if not relative: return None
        path=PurePosixPath(relative.replace('\\','/'))
        if path.is_absolute() or '..' in path.parts or ':' in relative: raise ValueError('文件路径不在归档范围内')
        if self.smb: return str(PureWindowsPath(self.root).joinpath(*path.parts))
        target=Path(self.root).joinpath(*path.parts).resolve()
        if not target.is_relative_to(Path(self.root).resolve()): raise ValueError('文件路径不在归档范围内')
        return str(target)

    def observe(self,files):
        rows=[]
        for file in files:
            row=dict(file); path=self.path(file)
            row['availability']='unknown'
            if path:
                try:
                    info=self.stat(path)
                    row['availability']='available' if file.get('bytes') in (None,info.st_size) else 'unknown'
                except FileNotFoundError: row['availability']='missing'
                except OSError: pass
            rows.append(row)
        return rows

    def download(self,file):
        path=self.path(file)
        if not path: raise LookupError('原件位置未记录')
        fd,name=tempfile.mkstemp(prefix='article-download-')
        target=Path(name)
        try:
            digest=hashlib.sha256()
            with os.fdopen(fd,'wb') as out,self.open(path,'rb') as source:
                while chunk:=source.read(256*1024):
                    digest.update(chunk); out.write(chunk)
            if file.get('sha256') and digest.hexdigest()!=file['sha256']: raise ValueError('原件校验未通过，暂不提供此副本')
            if file.get('bytes') is not None and target.stat().st_size!=file['bytes']: raise ValueError('原件大小与登记不符')
            return target
        except BaseException:
            target.unlink(missing_ok=True)
            raise

    def bundle(self,article):
        files=self.observe(article['files'])
        available=[f for f in files if f['availability']=='available']
        if not available: raise LookupError('暂无已确认可下载的原文或附件')
        fd,name=tempfile.mkstemp(prefix='article-bundle-',suffix='.zip'); os.close(fd)
        path=Path(name)
        try:
            names=set()
            with zipfile.ZipFile(path,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=1,allowZip64=True) as bundle:
                for file in available:
                    part=self.download(file)
                    try:
                        safe=PureWindowsPath(file.get('name') or file['id']).name
                        if safe in names: safe=file['id'][:12]+'_'+safe
                        names.add(safe); bundle.write(part,safe)
                    finally: part.unlink(missing_ok=True)
                complete=article.get('inventory_complete') and len(available)==len(files)
                if not complete:
                    note='资料包不完整：仅包含当前确认可下载的原文和附件。\n'
                    note+='\n'.join((f.get('name') or f['id'])+'：'+f['availability'] for f in files if f['availability']!='available')
                    if not article.get('inventory_complete'): note+='\n历史附件清单尚未确认。'
                    bundle.writestr('资料完整性.txt',note)
            return path
        except BaseException:
            path.unlink(missing_ok=True); raise
