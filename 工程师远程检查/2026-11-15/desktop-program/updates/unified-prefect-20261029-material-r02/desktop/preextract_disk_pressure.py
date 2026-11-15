"""Admission-only disk pressure for the existing paper/GPT worker gates.

No deletion, request-state changes, cancellation, thread pools or network calls.
The default reserve matches paperflow.config.Config.min_free_gib (5 GiB).
"""
import shutil,threading,time
from pathlib import Path

class DiskPressure:
    def __init__(self,root,*,min_free_gib=5.0,interval=.5,free_bytes=None,admission_check=None):
        self.admission_check=admission_check
        self.root=Path(root);self.minimum=int(float(min_free_gib)*2**30)
        if self.minimum<=0:raise ValueError('Storage reserve must be positive')
        self.interval=max(0,float(interval));self._free=free_bytes or (lambda:shutil.disk_usage(self.root).free)
        self._lock=threading.Lock();self._at=float('-inf');self._snapshot={}

    def snapshot(self):
        with self._lock:
            now=time.monotonic()
            if now-self._at>=self.interval:
                try:
                    free=int(self._free());paused=free<self.minimum
                    reason='low_disk' if paused else ''
                except OSError:
                    free=None;paused=True;reason='storage_unavailable'
                self._snapshot=dict(storage_paused=paused,storage_free_bytes=free,
                    storage_minimum_free_bytes=self.minimum,storage_reason=reason)
                self._at=now
            return dict(self._snapshot)

    def ready(self):return not self.snapshot()['storage_paused']

    def wait(self,stop=None,*,timeout=None,blocking=True):
        deadline=None if timeout is None else time.monotonic()+max(0,timeout)
        while True:
            if self.admission_check:self.admission_check()
            if stop is not None and stop.is_set():raise InterruptedError('空间等待已停止，已有材料保留')
            if self.ready():return True
            if not blocking:return False
            remaining=None if deadline is None else deadline-time.monotonic()
            if remaining is not None and remaining<=0:return False
            delay=.05 if remaining is None else min(.05,remaining)
            if stop is None:time.sleep(delay)
            else:stop.wait(delay)
