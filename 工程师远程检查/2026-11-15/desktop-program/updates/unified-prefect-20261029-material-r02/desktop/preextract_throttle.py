"""Shared 429 backoff for new dispatch only; never retries a recorded request."""
from __future__ import annotations

import hashlib
import json
import math
import time
from contextlib import contextmanager
from datetime import timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

from filelock import FileLock,Timeout
from preextract_material import write_json

SCHEMA='preextract-dispatch-pause/1'

def _path(connection):
    root=connection.get('dispatch_backoff_root')
    if not root:return None
    endpoint=connection['base_url'].rstrip('/').removesuffix('/chat/completions')
    identity=hashlib.sha256((endpoint+'\0'+connection['api_key']).encode()).hexdigest()
    return Path(root)/(identity+'.json')

@contextmanager
def _locked(path,stop=None):
    path.parent.mkdir(parents=True,exist_ok=True)
    lock=FileLock(str(path)+'.lock')
    while True:
        if stop is not None and stop.is_set():raise InterruptedError('等待上游限流恢复时已停止')
        try:lock.acquire(timeout=.1);break
        except Timeout:pass
    try:yield
    finally:lock.release()

def _read(path):
    if not path.exists():return dict(schema=SCHEMA,consecutive_429=0,until=0)
    row=json.loads(path.read_text(encoding='utf8'))
    if (row.get('schema')!=SCHEMA or type(row.get('consecutive_429')) is not int or row['consecutive_429']<0
            or not isinstance(row.get('until'),(int,float)) or not math.isfinite(row['until'])):
        raise ValueError('上游限流等待记录无效；未发送新请求')
    return row

def remaining_delay(connection,stop=None):
    path=_path(connection)
    if path is None or not path.exists():return 0
    with _locked(path,stop):return max(0,_read(path)['until']-time.time())

def wait_for_dispatch(connection,stop=None,progress=None):
    next_notice=0
    while True:
        if stop is not None and stop.is_set():raise InterruptedError('等待上游限流恢复时已停止')
        remaining=remaining_delay(connection,stop)
        if remaining<=0:return
        if progress and time.monotonic()>=next_notice:
            progress(f'上游限流，等待 {math.ceil(remaining)} 秒后派发新请求；已有回答继续复用，失败请求不自动重发')
            next_notice=time.monotonic()+15
        delay=min(.1,remaining)
        if stop is not None:
            if stop.wait(delay):raise InterruptedError('等待上游限流恢复时已停止')
        else:time.sleep(delay)

def _retry_after(headers,now):
    value=headers.get('Retry-After') if headers else None
    if value is None:return None
    try:delay=float(value)
    except (ValueError,TypeError):
        try:
            dt=parsedate_to_datetime(value)
            if dt.tzinfo is None:dt=dt.replace(tzinfo=timezone.utc)
            delay=dt.timestamp()-now
        except (ValueError,TypeError,OverflowError):return None
    return delay if math.isfinite(delay) and delay>=0 else None

def record_rate_limit(connection,headers):
    path=_path(connection)
    if path is None:return
    seconds=float(connection.get('dispatch_backoff_seconds',30))
    threshold=int(connection.get('dispatch_backoff_threshold',3))
    if not math.isfinite(seconds) or not 0<seconds<=3600 or threshold<1:
        raise ValueError('上游限流等待配置无效')
    with _locked(path):
        row=_read(path);now=time.time();row['consecutive_429']+=1
        retry_after=_retry_after(headers,now)
        delay=retry_after if retry_after is not None else (seconds if row['consecutive_429']>=threshold else 0)
        row.update(until=max(row['until'],now+delay),updated_at=now,last_http_status=429)
        write_json(path,row)

def record_success(connection):
    path=_path(connection)
    if path is None or not path.exists():return
    with _locked(path):
        row=_read(path)
        # An earlier in-flight request finishing does not cancel an active pause.
        row.update(consecutive_429=0,updated_at=time.time())
        write_json(path,row)
