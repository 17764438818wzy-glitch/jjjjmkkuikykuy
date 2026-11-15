"""Durable original-file jobs, immutable NAS objects and exact local releases.

NAS transfer never owns the source-admission mutex or any native task lock.
Release keeps native reference guards while inspecting dependencies, then uses
the shared admission mutex only to commit each checked deletion intent. This
module has no LLM, upload, resume, model/client or scheduling dependency.
"""
from contextlib import closing, contextmanager, ExitStack
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import time
import uuid

from filelock import FileLock
from .archive import ArchiveStore, ArchiveWriter, checked_id, encoded, MAX_BYTES
from .artifacts import ArtifactReader


SCHEMA = '''
CREATE TABLE IF NOT EXISTS nas_jobs(id TEXT PRIMARY KEY,asset_id TEXT NOT NULL,
 payload TEXT NOT NULL,state TEXT NOT NULL,result TEXT NOT NULL DEFAULT '{}',
 next_attempt REAL NOT NULL DEFAULT 0,error TEXT NOT NULL DEFAULT '',updated REAL NOT NULL);
CREATE INDEX IF NOT EXISTS nas_jobs_ready ON nas_jobs(state,next_attempt,updated);
CREATE INDEX IF NOT EXISTS nas_jobs_asset ON nas_jobs(asset_id,updated);
CREATE TABLE IF NOT EXISTS nas_uses(id TEXT PRIMARY KEY,paths TEXT NOT NULL,
 state TEXT NOT NULL,result TEXT NOT NULL DEFAULT '{}',created REAL NOT NULL);
CREATE TABLE IF NOT EXISTS nas_releases(path TEXT NOT NULL,sha256 TEXT NOT NULL,
 state TEXT NOT NULL,bytes INTEGER NOT NULL,proof TEXT NOT NULL,updated REAL NOT NULL,
 PRIMARY KEY(path,sha256));
'''


USE_INDEX_OBJECTS={'nas_use_paths','nas_use_paths_owner','nas_uses_insert_paths',
    'nas_uses_update_paths','nas_uses_delete_paths'}
USE_INDEX_DDL=(
    "CREATE TABLE IF NOT EXISTS nas_use_paths(use_id TEXT NOT NULL,path TEXT NOT NULL,PRIMARY KEY(path,use_id)) WITHOUT ROWID",
    "CREATE INDEX IF NOT EXISTS nas_use_paths_owner ON nas_use_paths(use_id)",
    """CREATE TRIGGER IF NOT EXISTS nas_uses_insert_paths AFTER INSERT ON nas_uses
       WHEN NEW.state!='closed' BEGIN
       INSERT INTO nas_use_paths(use_id,path) SELECT DISTINCT NEW.id,value FROM json_each(NEW.paths);
       END""",
    """CREATE TRIGGER IF NOT EXISTS nas_uses_update_paths AFTER UPDATE OF id,paths,state ON nas_uses
       WHEN OLD.id!=NEW.id OR OLD.paths!=NEW.paths OR (OLD.state='closed')!=(NEW.state='closed') BEGIN
       DELETE FROM nas_use_paths WHERE use_id=OLD.id;
       INSERT INTO nas_use_paths(use_id,path) SELECT DISTINCT NEW.id,value FROM json_each(NEW.paths) WHERE NEW.state!='closed';
       END""",
    """CREATE TRIGGER IF NOT EXISTS nas_uses_delete_paths AFTER DELETE ON nas_uses BEGIN
       DELETE FROM nas_use_paths WHERE use_id=OLD.id;
       END""",
)


def path_key(path):
    return os.path.normcase(str(Path(path).resolve()))


def safe_file(path, roots):
    path = Path(path).absolute()
    if not any(path != root and path.is_relative_to(root) for root in roots):
        raise ValueError('原件路径超出已登记目录')
    for part in (path, *path.parents):
        if part.is_symlink() or part.is_junction():
            raise ValueError('链接路径不能释放')
        try:
            if getattr(part.stat(follow_symlinks=False), 'st_file_attributes', 0) & 0x400:
                raise ValueError('重解析路径不能释放')
        except FileNotFoundError:
            pass
    resolved=path.resolve()
    if not any(resolved!=root and resolved.is_relative_to(root) for root in roots):
        raise ValueError('原件实际位置改变')
    return resolved


@contextmanager
def locked_file(path, *, delete=False):
    """Bind validation and optional Windows deletion to one opened file object."""
    if os.name != 'nt':
        # The deployed deleter is intentionally Windows-only. Copy stays portable.
        if delete: raise ValueError('本地释放尚未在此系统验证')
        with Path(path).open('rb') as stream: yield stream, None
        return
    import ctypes
    from ctypes import wintypes as w
    import msvcrt
    from paperflow.workbench_nas import opened_path
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.CreateFileW.argtypes = [w.LPCWSTR,w.DWORD,w.DWORD,w.LPVOID,w.DWORD,w.DWORD,w.HANDLE]
    kernel.CreateFileW.restype = w.HANDLE
    kernel.CloseHandle.argtypes = [w.HANDLE]
    access = 0x80000000 | (0x10000 if delete else 0)
    handle = kernel.CreateFileW(str(path), access, 0 if delete else 1, None, 3, 0x00200000, None)
    if handle == w.HANDLE(-1).value: raise ctypes.WinError(ctypes.get_last_error())
    try: fd = msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY)
    except BaseException:
        kernel.CloseHandle(handle); raise
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if (opened_path(stream) != Path(path) or not stat.S_ISREG(info.st_mode)
                or getattr(info,'st_file_attributes',0) & 0x400):
            raise ValueError('打开的文件身份或位置不符')
        def remove():
            if not delete: raise ValueError('只读句柄')
            if opened_path(stream) != Path(path): raise ValueError('原件路径改变')
            class Disposition(ctypes.Structure):
                _fields_ = [('DeleteFile', w.BOOL)]
            disposition = Disposition(True)
            kernel.SetFileInformationByHandle.argtypes = [w.HANDLE,ctypes.c_int,w.LPVOID,w.DWORD]
            kernel.SetFileInformationByHandle.restype = w.BOOL
            if not kernel.SetFileInformationByHandle(handle,4,ctypes.byref(disposition),ctypes.sizeof(disposition)):
                raise ctypes.WinError(ctypes.get_last_error())
        yield stream, remove


def verify_stream(stream, digest, size):
    if os.fstat(stream.fileno()).st_size != size or hashlib.file_digest(stream,'sha256').hexdigest() != digest:
        raise ValueError('文件内容与归档清单不符')
    stream.seek(0)


def rename_checked(stream,destination):
    """Same-volume, no-replace rename of the already verified Windows handle.

    FileRenameInfo uses the held destination directory and a single basename.
    No copy/unlink, cross-volume or permanent-delete fallback is permitted.
    """
    if os.name!='nt':raise ValueError('recoverable_isolation_requires_windows')
    import ctypes
    from ctypes import wintypes as w
    import msvcrt
    from paperflow.workbench_nas import opened_path
    destination=Path(destination).absolute()
    source=opened_path(stream)
    if ':' in source.name or ':' in destination.name or destination.name in {'','.','..'}:
        raise ValueError('isolation_stream_or_target_name_invalid')
    safe_file(source,[source.parent]);safe_file(destination,[destination.parent])
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.GetDriveTypeW.argtypes=[w.LPCWSTR];kernel.GetDriveTypeW.restype=w.UINT
    if kernel.GetDriveTypeW(source.anchor)!=3 or kernel.GetDriveTypeW(destination.anchor)!=3:
        raise ValueError('recoverable_isolation_requires_fixed_local_volume')
    filesystem=ctypes.create_unicode_buffer(32)
    kernel.GetVolumeInformationW.argtypes=[w.LPCWSTR,w.LPWSTR,w.DWORD,w.LPDWORD,w.LPDWORD,w.LPDWORD,w.LPWSTR,w.DWORD]
    kernel.GetVolumeInformationW.restype=w.BOOL
    if (not kernel.GetVolumeInformationW(source.anchor,None,0,None,None,None,filesystem,len(filesystem))
            or filesystem.value not in {'NTFS','ReFS'}):raise ValueError('recoverable_isolation_filesystem_unconfirmed')
    kernel.CreateFileW.argtypes=[w.LPCWSTR,w.DWORD,w.DWORD,w.LPVOID,w.DWORD,w.DWORD,w.HANDLE]
    kernel.CreateFileW.restype=w.HANDLE
    # Traverse/read attributes; omit FILE_SHARE_DELETE so this directory stays
    # bound while the relative target name is resolved by the kernel.
    parent=kernel.CreateFileW(str(destination.parent),0x20|0x80,3,None,3,0x02000000|0x00200000,None)
    if parent==w.HANDLE(-1).value:raise ctypes.WinError(ctypes.get_last_error())
    kernel.CloseHandle.argtypes=[w.HANDLE];kernel.CloseHandle.restype=w.BOOL
    try:
        class Attributes(ctypes.Structure):_fields_=[('attributes',w.DWORD),('tag',w.DWORD)]
        class Identity(ctypes.Structure):_fields_=[('volume',ctypes.c_ulonglong),('file_id',ctypes.c_ubyte*16)]
        kernel.GetFileInformationByHandleEx.argtypes=[w.HANDLE,ctypes.c_int,w.LPVOID,w.DWORD]
        kernel.GetFileInformationByHandleEx.restype=w.BOOL
        attrs=Attributes()
        if not kernel.GetFileInformationByHandleEx(parent,9,ctypes.byref(attrs),ctypes.sizeof(attrs)):
            raise ctypes.WinError(ctypes.get_last_error())
        if not attrs.attributes&0x10 or attrs.attributes&0x400:raise ValueError('isolation_directory_identity_unconfirmed')
        source_identity=Identity();parent_identity=Identity();handle=msvcrt.get_osfhandle(stream.fileno())
        for h,value in ((handle,source_identity),(parent,parent_identity)):
            if not kernel.GetFileInformationByHandleEx(h,18,ctypes.byref(value),ctypes.sizeof(value)):
                raise ctypes.WinError(ctypes.get_last_error())
        if source_identity.volume!=parent_identity.volume:raise ValueError('recoverable_isolation_cross_volume_held')
        kernel.GetFinalPathNameByHandleW.argtypes=[w.HANDLE,w.LPWSTR,w.DWORD,w.DWORD]
        kernel.GetFinalPathNameByHandleW.restype=w.DWORD
        buffer=ctypes.create_unicode_buffer(32768)
        count=kernel.GetFinalPathNameByHandleW(parent,buffer,len(buffer),0)
        if not count or count>=len(buffer):raise ValueError('isolation_directory_path_unconfirmed')
        actual=buffer.value
        if actual.startswith('\\\\?\\'):actual=actual[4:]
        if Path(actual)!=destination.parent:raise ValueError('isolation_directory_path_changed')
        name=destination.name.encode('utf-16-le')
        class Rename(ctypes.Structure):
            _fields_=[('flags',w.DWORD),('directory',w.HANDLE),('length',w.DWORD),('name',w.WCHAR*1)]
        raw=ctypes.create_string_buffer(max(ctypes.sizeof(Rename),Rename.name.offset+len(name)+2))
        value=Rename.from_buffer(raw);value.flags=0;value.directory=parent;value.length=len(name)
        ctypes.memmove(ctypes.addressof(raw)+Rename.name.offset,name,len(name))
        kernel.SetFileInformationByHandle.argtypes=[w.HANDLE,ctypes.c_int,w.LPVOID,w.DWORD]
        kernel.SetFileInformationByHandle.restype=w.BOOL
        if not kernel.SetFileInformationByHandle(handle,3,raw,len(raw)):
            raise ctypes.WinError(ctypes.get_last_error())
        if opened_path(stream)!=destination:raise ValueError('isolated_file_path_unconfirmed')
    finally:kernel.CloseHandle(parent)


def isolation_record(root,scope_identity,path,digest,size):
    root=Path(root).resolve();path=Path(path).resolve()
    folder=root/'private/original_archive/quarantine'/hashlib.sha256(encoded(scope_identity)).hexdigest()
    folder.mkdir(parents=True,exist_ok=True)
    target=folder/(hashlib.sha256(str(path).encode('utf8')).hexdigest()+path.suffix[:16])
    safe_file(target,[root])
    return dict(schema='same-volume-isolation/1',workspace=str(root),original_path=str(path),
        path=str(target),sha256=digest,bytes=size,scope=scope_identity)


def verify_isolation(value,device,inode):
    if value.get('schema')!='same-volume-isolation/1':raise ValueError('isolation_receipt_missing')
    root=Path(value['workspace']).resolve()
    path=safe_file(value['path'],[root/'private/original_archive/quarantine'])
    with locked_file(path) as (stream,_):
        stat=os.fstat(stream.fileno())
        if (stat.st_dev,stat.st_ino)!=(device,inode):raise ValueError('isolation_file_identity_changed')
        verify_stream(stream,value['sha256'],value['bytes'])
    return path


class RegisteredWriter(ArchiveWriter):
    def inventory(self, row):
        self.registration=row['registered']
        files=[]
        for source in row['registered']['files']:
            path=safe_file(source['path'],self.sources.roots)
            files.append(dict(path=str(path),name=source['name'],kind=source['kind'],
                expected_hash=source['sha256'],available=path.is_file(),bytes=source['bytes'],
                archive_identity_check='native_verified_inventory'))
        return files, [], row['registered']['source_evidence']

    def _publish_manifest(self,asset_id,value):
        return super()._publish_manifest(asset_id,dict(value,native_registration=self.registration))


class ArchiveLifecycle:
    def __init__(self, data, policy):
        self.data=Path(data).resolve(); self.policy=policy
        self.dbpath=Path(policy['database']).resolve()
        self.roots=tuple(Path(p).resolve() for p in policy['source_roots'])
        self.destination=Path(policy['destination'])  # No SMB stat on admission/status.
        self.mutex=self.data/'private/original_archive/lifecycle.lock'
        self.mutex.parent.mkdir(parents=True,exist_ok=True)
        self.dbpath.parent.mkdir(parents=True,exist_ok=True)
        # Normal admission reads schema without taking a schema-writer lock.
        # Only a new/incomplete database needs idempotent DDL.
        with self.db(timeout=0) as db:
            present={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type IN ('table','index','trigger')")}
            if not {'nas_jobs','nas_jobs_ready','nas_jobs_asset','nas_uses','nas_releases'}<=present:
                db.executescript(SCHEMA)
            if not USE_INDEX_OBJECTS<=present:
                # Install triggers and the complete historical index atomically.
                # Legacy writers keep their JSON protocol; their SQL automatically
                # updates this projection in the same commit or rollback.
                db.execute('BEGIN IMMEDIATE')
                for statement in USE_INDEX_DDL:db.execute(statement)
                db.execute('DELETE FROM nas_use_paths')
                db.execute("INSERT INTO nas_use_paths(use_id,path) SELECT DISTINCT u.id,p.value "
                    "FROM nas_uses u,json_each(u.paths) p WHERE u.state!='closed'")

    @contextmanager
    def db(self, timeout=5):
        with closing(sqlite3.connect(self.dbpath,timeout=timeout)) as db,db:
            db.row_factory=sqlite3.Row
            yield db

    def guard(self, timeout=10):
        return FileLock(str(self.mutex),timeout=timeout)

    def register(self, job):
        job=json.loads(encoded(job)); checked_id(job['source_sha256'],64)
        if not job.get('files'): raise ValueError('原件清单为空')
        for file in job['files']:
            file['path']=str(safe_file(file['path'],self.roots))
            checked_id(file['sha256'],64)
            if type(file['bytes']) is not int or file['bytes']<0:raise ValueError('文件长度无效')
        asset=hashlib.sha256(('sha:'+job['source_sha256']).encode()).hexdigest()[:32]
        # Saving an archive inventory does not admit a reader or local deletion.
        # Large inventories must not own the source-admission mutex.
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT payload FROM nas_jobs WHERE id=?',(job['id'],)).fetchone()
            if old:
                if json.loads(old[0])!=job:raise ValueError('归档任务身份或文件清单改变')
                return
            if job.get('task_identity'):
                for file in job['files']:
                    if db.execute("SELECT 1 FROM nas_releases WHERE path=? AND sha256=? AND state='releasing'",
                                  (path_key(file['path']),file['sha256'])).fetchone():
                        raise ValueError('原件已进入另一释放意图，保留新登记待核')
            db.execute('INSERT INTO nas_jobs(id,asset_id,payload,state,updated) VALUES(?,?,?,?,?)',
                (job['id'],asset,encoded(job).decode(),'queued',time.time()))

    def get(self, ident):
        with self.db() as db: row=db.execute('SELECT * FROM nas_jobs WHERE id=?',(ident,)).fetchone()
        if not row:return None
        return dict(row,payload=json.loads(row['payload']),result=json.loads(row['result']))

    def ready(self, ident, identity):
        row=self.get(ident)
        return bool(row and row['state'] in {'verified','waiting','released'}
            and row['payload']['identity']==identity and row['result'].get('status')=='verified')

    def assert_available(self, paths):
        with self.db() as db:
            for name in paths:
                path=Path(name).absolute()
                if db.execute("SELECT 1 FROM nas_releases WHERE path=? AND state IN ('releasing','released') LIMIT 1",(path_key(path),)).fetchone():
                    raise ValueError('原件已经释放或正在释放，请从 NAS 重新选稿')
                if not path.is_file():raise ValueError('原件不可读取；请核对 NAS 保存位置')

    def begin_use(self, paths):
        paths=sorted({path_key(p) for p in paths})
        # Filesystem inspection of a whole batch must not hold admission. Every
        # managed delete records its intent under the same mutex before unlink.
        for path in paths:
            if not Path(path).is_file():raise ValueError('原件不可读取；请核对 NAS 保存位置')
        with self.guard(timeout=0),self.db(timeout=0) as db:
            db.execute('BEGIN IMMEDIATE')
            for start in range(0,len(paths),500):
                chunk=paths[start:start+500]
                if db.execute("SELECT 1 FROM nas_releases WHERE path IN ("+','.join('?' for _ in chunk)+
                        ") AND state IN ('releasing','released') LIMIT 1",chunk).fetchone():
                    raise ValueError('原件已经释放或正在释放，请从 NAS 重新选稿')
            ident=uuid.uuid4().hex
            db.execute('INSERT INTO nas_uses(id,paths,state,created) VALUES(?,?,?,?)',
                (ident,encoded(paths).decode(),'active',time.time()))
        return ident

    def end_use(self, ident, *, closed=False, result=None, timeout=5):
        # closed requires an explicit native proof. A crash/exception stays held.
        value=encoded(result or {}).decode()
        if closed:
            with self.db(timeout=timeout) as db:
                db.execute("UPDATE nas_uses SET state='closed',result=? WHERE id=?",(value,ident))
            return
        # active -> unresolved retains every protected path. It cannot admit a
        # reader or authorize deletion, so it needs no global admission lock.
        # A replay must never reopen a consumer already resolved by native proof.
        with self.db(timeout=timeout) as db:
            changed=db.execute("UPDATE nas_uses SET state='unresolved',result=? WHERE id=? AND state='active'",
                (value,ident)).rowcount
            if not changed and not db.execute('SELECT 1 FROM nas_uses WHERE id=?',(ident,)).fetchone():
                raise ValueError('NAS 使用记录不存在，保留结束回执')

    def defer_end_use(self, ident, result):
        """Persist only the sanitized consumer result; never release any path."""
        checked_id(ident,32)
        directory=self.mutex.parent/'pending-use-end'
        directory.mkdir(parents=True,exist_ok=True)
        path=directory/(ident+'.json')
        temporary=directory/(ident+'.'+uuid.uuid4().hex+'.tmp')
        try:
            with temporary.open('xb') as stream:
                stream.write(encoded(dict(schema='nas-use-end/1',id=ident,result=result)))
                stream.flush();os.fsync(stream.fileno())
            os.replace(temporary,path)
        finally:
            temporary.unlink(missing_ok=True)

    def reconcile_end_uses(self, limit=128, *, use_ids=None, authorize=None):
        """Replay completed exits only; an active consumer without a receipt stays held."""
        directory=self.mutex.parent/'pending-use-end'
        if not directory.is_dir():return
        paths=(directory/(checked_id(ident,32)+'.json') for ident in use_ids) if use_ids is not None else directory.glob('*.json')
        for index,path in enumerate(paths):
            if index>=limit:break
            try:
                value=json.loads(path.read_text(encoding='utf8'))
                if not isinstance(value,dict) or value.get('schema')!='nas-use-end/1' or value.get('id')!=path.stem:
                    continue
                checked_id(value['id'],32)
                if not isinstance(value.get('result'),dict):continue
                if authorize is not None and authorize(value) is not True:continue
                self.end_use(value['id'],closed=False,result=value['result'])
                path.unlink(missing_ok=True)
            except (OSError,ValueError,sqlite3.Error):
                # Keep both the receipt and the original path protections.
                continue

    def uses(self):
        with self.db() as db:rows=db.execute("SELECT * FROM nas_uses WHERE state!='closed'").fetchall()
        return [dict(r,paths=json.loads(r['paths']),result=json.loads(r['result'])) for r in rows]

    def copy_once(self, limit=2, *, priority_ids=None, task_items=None):
        # Native cleaner calls this only outside all processing/cleanup mutexes.
        # Priority is never a whitelist. No task mapping means no storage work.
        if not task_items or not self.policy.get('copy_enabled',True):return
        with FileLock(str(self.dbpath)+'.nas-worker.lock',timeout=0):
            query="SELECT id FROM nas_jobs WHERE state IN ('queued','retry') AND next_attempt<=?"
            args=[time.time()]
            query+=' AND id IN ('+','.join('?' for _ in task_items)+')'
            args.extend(task_items)
            priority_ids=tuple(dict.fromkeys(priority_ids or ()))
            order='updated'
            if priority_ids:
                # Priority only among due, copyable jobs. A held/review/backoff
                # priority must not prevent other registered originals copying.
                order='CASE WHEN id IN ('+','.join('?' for _ in priority_ids)+') THEN 0 ELSE 1 END, updated'
                args.extend(priority_ids)
            with self.db() as db:ids=[r[0] for r in db.execute(query+' ORDER BY '+order+' LIMIT ?',(*args,limit))]
            for ident in ids:
                from global_task_control import GlobalPaused
                try:
                    scope=task_items[ident]
                    if scope.root!=self.data:raise ValueError('NAS scope belongs to another workspace')
                    with scope.stage('archive'):
                        row=self.get(ident);job=row['payload']
                        try:
                            if job.get('task_identity')!=scope.identity:
                                raise ValueError('NAS task/item binding missing or changed')
                            scope.assert_open()
                            if sum(f['bytes'] for f in job['files'])>MAX_BYTES:
                                raise OverflowError('本次归档超过 512 MiB，保留本地文件待处理')
                            sources=ArtifactReader(self.roots,self.dbpath.parent/'spool')
                            writer=RegisteredWriter(ArchiveStore(self.dbpath),sources,self.destination,
                                reserve_bytes=self.policy.get('reserve_bytes',1024**3),admit=scope.assert_open)
                            value=writer.archive(dict(asset_id=row['asset_id'],verified=True,title=job['title'],
                                detail=dict(source_hash=job['source_sha256'],attachment_contract={'known':True}),registered=job))
                            if value['status']!='verified':raise ValueError('原件未完整归档')
                            scope.assert_open()
                            with self.db() as db:db.execute("UPDATE nas_jobs SET state='verified',result=?,error='',updated=? WHERE id=?",
                                (encoded(value).decode(),time.time(),ident))
                        except Exception as exc:
                            with self.db() as db:db.execute('UPDATE nas_jobs SET state=?,error=?,next_attempt=?,updated=? WHERE id=?',
                                ('review' if isinstance(exc,OverflowError) else 'retry',str(exc)[:180],time.time()+60,time.time(),ident))
                except GlobalPaused:break

    @contextmanager
    def verified_files(self, ident, identity):
        """Real NAS readback; keep every immutable object open for the caller."""
        row=self.get(ident)
        if not self.ready(ident,identity):
            raise ValueError('NAS exact inventory is not verified')
        job,result=row['payload'],row['result']
        with ExitStack() as stack:
            manifest=stack.enter_context(locked_file(self.destination/result['manifest_path']))[0]
            raw=manifest.read()
            if hashlib.sha256(raw).hexdigest()!=result['manifest_sha256']:
                raise ValueError('NAS 清单核验不符')
            saved=json.loads(raw)
            expected={f['sha256']:f['bytes'] for f in saved['files']}
            if saved['source_sha256']!=job['source_sha256']:
                raise ValueError('NAS 原稿身份不符')
            opened=set()
            for file in job['files']:
                digest=file['sha256']
                if expected.get(digest)!=file['bytes']:
                    raise ValueError('文件不在核验的 NAS 清单内')
                if digest not in opened:
                    stream,_=stack.enter_context(locked_file(self.destination/'objects'/digest[:2]/digest))
                    verify_stream(stream,digest,file['bytes']);opened.add(digest)
            yield row

    def _mark_released(self,path,sha):
        with self.db() as db:db.execute("UPDATE nas_releases SET state='released',updated=? WHERE path=? AND sha256=?",
            (time.time(),path_key(path),sha))

    def release(self, ident, references):
        from global_task_control import GlobalTaskControl,GlobalPaused
        try:
            with GlobalTaskControl(self.data).stage('cleanup',paper_id=ident):
                return self._release_admitted(ident,references)
        except GlobalPaused:return dict(status='waiting',released_files=0,released_bytes=0,reason='global_paused')

    def _release_admitted(self, ident, references):
        if not self.policy.get('release_enabled',True):
            return dict(status='disabled',released_files=0,released_bytes=0)
        row=self.get(ident)
        if not row or not self.ready(ident,row['payload']['identity']):
            return dict(status='waiting',released_files=0,released_bytes=0)
        job=row['payload'];result=row['result'];waiting=[]
        release_files=[f for f in job['files'] if f.get('release',True)]
        try:
            with self.verified_files(ident,job['identity']),references(job) as check:
                factory=getattr(check,'isolation_record',None)
                if factory is None:raise ValueError('recoverable_isolation_not_provided')
                for file in release_files:
                    path=safe_file(file['path'],self.roots);digest=file['sha256']
                    try:
                        reason=check(path,digest)
                        if reason:raise ValueError(reason)
                        expected=factory(path,digest,file['bytes'])
                        with self.db() as db:
                            prior=db.execute('SELECT * FROM nas_releases WHERE path=? AND sha256=?',
                                (path_key(path),digest)).fetchone()
                        previous=json.loads(prior['proof']) if prior else None
                        if previous and (previous.get('job_id')!=ident or previous.get('identity')!=job['identity']
                                or previous.get('task_identity')!=job.get('task_identity')
                                or previous.get('recovery')!=expected):
                            raise ValueError('local_isolation_belongs_to_another_identity')
                        if not path.exists():
                            if not previous or prior['state'] not in {'releasing','isolated'}:
                                raise ValueError('local_copy_missing_without_isolation_receipt')
                            verify_isolation(expected,previous['device'],previous['inode'])
                            with self.db() as db:
                                db.execute("UPDATE nas_releases SET state='isolated',updated=? WHERE path=? AND sha256=?",
                                    (time.time(),path_key(path),digest))
                            continue
                        if previous and prior['state']=='isolated':raise ValueError('restored_or_duplicate_local_path_held')
                        if Path(expected['path']).exists():raise ValueError('isolation_target_already_exists')
                        with locked_file(path,delete=True) as (stream,_permanent_delete_unused):
                            verify_stream(stream,digest,file['bytes'])
                            info=os.fstat(stream.fileno());key=path_key(path)
                            proof=dict(job_id=ident,identity=job['identity'],task_identity=job.get('task_identity'),
                                manifest_sha256=result['manifest_sha256'],device=info.st_dev,inode=info.st_ino,recovery=expected)
                            with self.guard(timeout=0),self.db(timeout=0) as db:
                                db.execute('BEGIN IMMEDIATE')
                                if db.execute('SELECT 1 FROM nas_use_paths WHERE path=? LIMIT 1',(key,)).fetchone():
                                    raise ValueError('input_lease_or_unsettled_use')
                                if db.execute("""SELECT 1 FROM nas_jobs j,json_each(j.payload,'$.files') f
                                    WHERE j.id!=? AND json_extract(f.value,'$.path')=? COLLATE NOCASE LIMIT 1""",
                                    (ident,str(path))).fetchone():raise ValueError('shared_original_archive_owner')
                                db.execute('INSERT OR REPLACE INTO nas_releases VALUES(?,?,?,?,?,?)',
                                    (key,digest,'releasing',file['bytes'],encoded(proof).decode(),time.time()))
                            commit=getattr(check,'commit_guard',None)
                            if commit is None:raise ValueError('exact_task_isolation_commit_guard_missing')
                            with commit(path):
                                reason=check(path,digest)
                                if reason:raise ValueError(reason)
                                rename_checked(stream,expected['path'])
                        verify_isolation(expected,proof['device'],proof['inode'])
                        if path.exists():raise ValueError('local_copy_path_still_exists')
                        with self.db() as db:
                            db.execute("UPDATE nas_releases SET state='isolated',updated=? WHERE path=? AND sha256=?",
                                (time.time(),path_key(path),digest))
                    except Exception as exc:waiting.append(dict(path=str(path),reason=str(exc)[:180]))
        except Exception as exc:waiting.append(dict(reason=str(exc)[:180]))
        with self.db() as db:
            isolated=[db.execute("SELECT bytes,proof FROM nas_releases WHERE path=? AND sha256=? AND state='isolated'",
                (path_key(f['path']),f['sha256'])).fetchone() for f in release_files]
            isolated=[r for r in isolated if r and json.loads(r['proof']).get('job_id')==ident
                      and json.loads(r['proof']).get('task_identity')==job.get('task_identity')]
            summary=dict(status='waiting' if waiting or len(isolated)!=len(release_files) else 'isolated',
                released_files=0,released_bytes=0,isolated_files=len(isolated),isolated_bytes=sum(r[0] for r in isolated),
                local_mode='same-volume-isolation/1',waiting=waiting)
            # Keep the original NAS verified/readable lane; an isolated local
            # copy is not a physically deleted byte or a retired AI product.
            db.execute("UPDATE nas_jobs SET state='waiting',error=?,updated=? WHERE id=?",
                (encoded(summary).decode(),time.time(),ident))
        return summary
