"""Existing single-controller lock, shared by old and isolated controllers."""
import os
from pathlib import Path


class InstanceLock:
    def __init__(self,path):
        self.path=Path(path)
        self.stream=None
    def __enter__(self):
        self.path.parent.mkdir(parents=True,exist_ok=True)
        self.stream=self.path.open('a+b')
        if self.path.stat().st_size==0:
            self.stream.write(b'0'); self.stream.flush()
        self.stream.seek(0)
        try:
            if os.name=='nt':
                import msvcrt
                msvcrt.locking(self.stream.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(self.stream,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError:
            self.stream.close(); self.stream=None
            raise RuntimeError('workflow already running') from None
        return self
    def __exit__(self,*args):
        if self.stream:
            if os.name=='nt':
                import msvcrt
                self.stream.seek(0); msvcrt.locking(self.stream.fileno(),msvcrt.LK_UNLCK,1)
            self.stream.close(); self.stream=None

