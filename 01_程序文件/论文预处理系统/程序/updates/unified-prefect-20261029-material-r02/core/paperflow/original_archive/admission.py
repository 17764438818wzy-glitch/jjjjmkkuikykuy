"""Reject stale source admissions after storage retirement, inside native SQL writes."""
import json
from pathlib import Path
import sqlite3
from contextlib import closing,contextmanager
from collections import deque
import threading
from .lifecycle import path_key


def assert_available(database,paths):
    # Current original-file ownership is bounded to the configured native data
    # root. Unrelated workbenches have no policy and keep their existing behavior.
    for root in Path(database).resolve().parents:
        policy=root/'private/preextract_delivery/nas-policy.json'
        if not policy.is_file():continue
        value=json.loads(policy.read_text(encoding='utf8'))
        if not value.get('enabled'):return
        if value.get('schema')!='paper-original-archive-policy/2':raise ValueError('NAS策略无法核对')
        ledger=Path(value['database'])
        if ledger.is_file():
            with closing(sqlite3.connect(ledger.as_uri()+'?mode=ro',uri=True,timeout=1)) as db:
                if db.execute("SELECT 1 FROM sqlite_master WHERE name='nas_releases'").fetchone():
                    for path in paths:
                        if path and db.execute("SELECT 1 FROM nas_releases WHERE path=? AND state IN ('releasing','released') LIMIT 1",
                            (path_key(path),)).fetchone():raise ValueError('原件已释放到 NAS，不能从旧路径恢复任务')
        return


class _AdmissionQueue:
    def __init__(self):
        self.lock=threading.Lock()
        self.waiting=deque()
        self.users=0


_admissions={}
_admissions_lock=threading.Lock()


@contextmanager
def admission_turn(root,*,stop=None,on_wait=None):
    """FIFO for this process's short registrations, never for body/LLM work.

    The existing OS FileLock and SQLite transaction remain the cross-process
    authority. Only this local queue's head retries those native guards.
    Per-ticket events hand off to one successor, avoiding a wake-all burst.
    """
    key=path_key(root)
    with _admissions_lock:
        queue=_admissions.setdefault(key,_AdmissionQueue())
        queue.users+=1
    ticket=threading.Event()
    def check_stop():
        if stop is not None and stop.is_set():
            raise InterruptedError('已停止，已有材料和结果保留')
    try:
        check_stop()
        with queue.lock:
            queue.waiting.append(ticket)
            if queue.waiting[0] is ticket:ticket.set()
        try:
            if not ticket.is_set() and on_wait is not None:on_wait()
            while not ticket.wait(.05 if stop is not None else None):check_stop()
            check_stop()
            yield
        finally:
            with queue.lock:
                if ticket in queue.waiting:queue.waiting.remove(ticket)
                if queue.waiting:queue.waiting[0].set()
    finally:
        with _admissions_lock:
            queue.users-=1
            if not queue.users:_admissions.pop(key,None)
