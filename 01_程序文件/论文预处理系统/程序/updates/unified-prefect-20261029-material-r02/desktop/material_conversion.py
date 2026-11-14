"""Bounded Office conversion inside the original material preparation only."""
from collections import deque
from contextlib import contextmanager, nullcontext
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4
import hashlib
import json
import shutil
import tempfile
import threading
import time

from filelock import FileLock, Timeout

_local = threading.local()
_pools = {}
_pools_lock = threading.Lock()
VERSION = 'material-conversion/1'
LIBREOFFICE_WORKERS = 6


def _sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for part in iter(lambda:stream.read(1024*1024),b''):
            digest.update(part)
    return digest.hexdigest()


def _publish(source,target):
    target.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=target.parent,prefix='.conversion-',suffix='.tmp',delete=False) as stream:
        staged=Path(stream.name)
    try:
        shutil.copy2(source,staged);staged.replace(target)
    finally:
        staged.unlink(missing_ok=True)


def current():
    return getattr(_local,'conversion',None)


def _pool(root):
    key=str(Path(root).resolve())
    with _pools_lock:
        if key not in _pools:_pools[key]=ConversionPool(Path(key)/'private'/'material-conversion')
        return _pools[key]


@contextmanager
def preparation_context(root, *, enabled=True, stop=None, wait_scope=nullcontext):
    previous=current()
    pool=_pool(root) if enabled else None
    _local.conversion=(SimpleNamespace(convert=lambda source, output, **kwargs:
        pool.convert(source,output,stop=stop,wait_scope=wait_scope,**kwargs)) if pool else None)
    try:
        yield
    finally:
        _local.conversion=previous


class ConversionPool:
    def __init__(self,root):
        self.root=Path(root)
        self.condition=threading.Condition()
        self.waiting=deque()
        self.busy=set()

    def _available(self):
        from office_libreoffice import executable
        from office_wps import available
        return ([(f'lo-{i+1}','libreoffice') for i in range(LIBREOFFICE_WORKERS)] if executable() else [])+([('wps','wps')] if available() else [])

    def _identity(self,engine):
        if engine=='libreoffice':
            from office_libreoffice import executable
            path=executable()
        else:
            from office_wps import _registration
            path=_registration()[0]
        stat=path.stat()
        return f'{VERSION}:{engine}:{path}:{stat.st_size}:{stat.st_mtime_ns}'

    def _execute(self,engine,source,output,target_format,timeout,stop=None):
        if engine=='libreoffice':
            from office_libreoffice import convert
            return convert(source,output,target_format=target_format,timeout=timeout,stop=stop)
        from office_wps import convert
        return convert(source,output,target_format=target_format,timeout=timeout)

    def _cached(self,engine,key,target_format):
        from office_wps import _valid_docx,_valid_pdf
        root=self.root/'cache'/engine/key
        path=root/('content.'+target_format)
        try:
            report=json.loads((root/'identity.json').read_text(encoding='utf8'))
            valid=_valid_pdf if target_format=='pdf' else _valid_docx
            if report['sha256']==_sha(path) and valid(path):return path
        except (OSError,ValueError,KeyError):pass
        return None

    @contextmanager
    def _claim(self,available,stop,deadline):
        ticket=(object(),frozenset(candidate for candidate,_ in available));held=None;slot=None
        (self.root/'locks').mkdir(parents=True,exist_ok=True)
        with self.condition:self.waiting.append(ticket)
        try:
            while held is None:
                if stop is not None and stop.is_set():raise InterruptedError('已停止，转换等待材料保留')
                if time.monotonic()>=deadline:raise RuntimeError('格式转换队列等待超时，材料保留')
                with self.condition:
                    earlier=list(self.waiting)[:self.waiting.index(ticket)]
                    for candidate,engine in available:
                        if candidate in self.busy or any(candidate in entry[1] for entry in earlier):continue
                        lock=FileLock(str(self.root/'locks'/(candidate+'.lock')))
                        try:lock.acquire(timeout=0)
                        except Timeout:continue
                        held=lock;slot=candidate;backend=engine
                        self.busy.add(slot);self.waiting.remove(ticket);self.condition.notify_all();break
                    if held is None:self.condition.wait(timeout=.05)
            yield backend
        finally:
            # Converter permits are returned before the parser permit is reacquired.
            if held is not None:held.release()
            with self.condition:
                if slot is not None:self.busy.discard(slot)
                if ticket in self.waiting:self.waiting.remove(ticket)
                self.condition.notify_all()

    def convert(self,source,output,*,target_format='pdf',timeout=120,stop=None,wait_scope=nullcontext):
        if target_format not in {'pdf','docx'}:raise ValueError('格式转换目标无效')
        source,output=Path(source).resolve(),Path(output).resolve()
        target=output/(source.stem+'.'+target_format)
        if source==target:raise ValueError('转换输出不能覆盖原稿')
        if stop is not None and stop.is_set():raise InterruptedError('已停止，原材料保留')
        digest=_sha(source)
        available=self._available()
        if source.suffix.lower()=='.wps':available=[row for row in available if row[1]=='wps']
        if not available:raise RuntimeError('没有可用的材料转换器')
        keys={engine:hashlib.sha256((self._identity(engine)+source.suffix.lower()+':'+target_format+':'+digest).encode()).hexdigest() for _,engine in available}
        for engine in keys:
            cache=self.root/'cache'/engine/keys[engine]
            if not cache.is_dir():continue
            # The same native key lock protects readers from post-upload
            # retirement. Keep the published private copy outside the cache.
            try:
                with FileLock(str(cache)+'.lock',timeout=0):
                    cached=self._cached(engine,keys[engine],target_format)
                    if cached is not None:
                        _publish(cached,target);return target
            except Timeout:pass
        self.root.mkdir(parents=True,exist_ok=True)
        with wait_scope():
            with self._claim(available,stop,time.monotonic()+max(120,timeout*4)) as engine:
                cache=self.root/'cache'/engine/keys[engine]
                cache.parent.mkdir(parents=True,exist_ok=True)
                lock=FileLock(str(cache)+'.lock')
                cache_deadline=time.monotonic()+max(120,timeout*4)
                while True:
                    if stop is not None and stop.is_set():raise InterruptedError('已停止，转换结果保留')
                    if time.monotonic()>=cache_deadline:raise RuntimeError('等待同图转换结果超时，原材料保留')
                    try:lock.acquire(timeout=.1);break
                    except Timeout:continue
                try:
                    cache.mkdir(parents=True,exist_ok=True)
                    cached=self._cached(engine,keys[engine],target_format)
                    if cached is None:
                        with tempfile.TemporaryDirectory(prefix='attempt-',dir=cache) as name:
                            attempt=Path(name)
                            # Keep existing converter signatures usable by tests and adapters.
                            try:
                                if engine=='libreoffice' and stop is not None:
                                    converted=self._execute(engine,source,attempt/'out',target_format,timeout,stop=stop)
                                else:converted=self._execute(engine,source,attempt/'out',target_format,timeout)
                            except BaseException as exc:
                                failed=cache/'failed'/uuid4().hex
                                shutil.copytree(attempt,failed)
                                (failed/'failure.json').write_text(json.dumps(dict(error_type=type(exc).__name__,
                                    source_sha256=digest,engine=engine,failed_at=time.time())),encoding='utf8')
                                raise
                            from office_wps import _valid_docx,_valid_pdf
                            valid=_valid_pdf if target_format=='pdf' else _valid_docx
                            if not valid(converted):raise RuntimeError('材料转换结果无效，原始内容保留')
                            if _sha(source)!=digest:raise RuntimeError('转换期间原材料发生变化')
                            cached=cache/('content.'+target_format)
                            _publish(converted,cached)
                            log=attempt/'out_conversion.log'
                            if log.exists():_publish(log,cache/'conversion.log')
                            report=dict(version=VERSION,engine=engine,source_sha256=digest,
                                        sha256=_sha(cached),completed_at=time.time())
                            temporary=cache/('identity-'+uuid4().hex+'.tmp')
                            temporary.write_text(json.dumps(report,ensure_ascii=False),encoding='utf8')
                            temporary.replace(cache/'identity.json')
                    _publish(cached,target)
                finally:lock.release()
        return target
