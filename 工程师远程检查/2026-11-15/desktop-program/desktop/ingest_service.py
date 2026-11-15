"""Start one hidden local worker for the durable workbench ingest queue."""
from __future__ import annotations

import json
import msvcrt
import subprocess
import sys
import time
from pathlib import Path

from upload_tracking import service_key


def validate_worker_service(root: Path, service_url: str) -> None:
    settings=json.loads((Path(root)/'settings.json').read_text(encoding='utf-8'))
    configured=settings.get('ragflow_url') or ''
    if service_key(configured)!=service_key(service_url):
        raise ValueError('当前连接与后台导入工作进程的 RAGFlow 地址不一致')


def _worker_has_lock(path: Path) -> bool:
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a+b') as stream:
        if path.stat().st_size==0:
            stream.write(b'0')
            stream.flush()
        stream.seek(0)
        try:
            msvcrt.locking(stream.fileno(),msvcrt.LK_NBLCK,1)
        except OSError:
            return True
        msvcrt.locking(stream.fileno(),msvcrt.LK_UNLCK,1)
        return False


def ensure_work_pool_worker(root: Path, queue_path: Path,
                            service_url: str, parse_limit: int = 3) -> bool:
    """Return True if a worker was started, False if one already holds the lock."""
    root=Path(root)
    queue_path=Path(queue_path).resolve()
    validate_worker_service(root,service_url)
    if str(queue_path).startswith('\\\\'):
        raise ValueError('SQLite 工作队列必须位于本地磁盘')
    from ingest_model_routes import load_routes
    routes=load_routes(root/'private/education_dual_channel/routes.json',service_url)
    parse_limit=min(4,max(1,parse_limit))
    if routes: parse_limit=min(parse_limit,routes['total_limit'])
    worker_lock=queue_path.with_suffix(queue_path.suffix+'.worker.lock')
    launch_lock=queue_path.with_suffix(queue_path.suffix+'.launcher.lock')
    launch_lock.parent.mkdir(parents=True,exist_ok=True)
    with launch_lock.open('a+b') as guard:
        guard.seek(0)
        guard.write(b'0')
        guard.flush()
        guard.seek(0)
        msvcrt.locking(guard.fileno(),msvcrt.LK_LOCK,1)
        try:
            if _worker_has_lock(worker_lock):
                return False
            log_dir=root/'private'/'universal_ingest'
            log_dir.mkdir(parents=True,exist_ok=True)
            stamp=time.strftime('%Y%m%d_%H%M%S')
            stdout=log_dir/f'worker_{stamp}.stdout.log'
            stderr=log_dir/f'worker_{stamp}.stderr.log'
            with stdout.open('ab') as out, stderr.open('ab') as err:
                child=subprocess.Popen([
                    sys.executable,str(Path(__file__).with_name('ingest_worker.py')),
                    '--queue',str(queue_path),'--data-root',str(root.resolve()),'--policy','all',
                    '--parse-limit',str(parse_limit)],
                    cwd=str(Path(__file__).resolve().parent),
                    stdin=subprocess.DEVNULL,stdout=out,stderr=err,
                    creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0),
                    close_fds=True)
            deadline=time.monotonic()+5
            while time.monotonic()<deadline:
                if _worker_has_lock(worker_lock):
                    return True
                if child.poll() is not None:
                    raise RuntimeError('后台导入工作进程未能启动，请检查本地工作进程日志')
                time.sleep(.1)
            raise RuntimeError('后台导入工作进程启动后未获取队列锁')
        finally:
            guard.seek(0)
            msvcrt.locking(guard.fileno(),msvcrt.LK_UNLCK,1)
