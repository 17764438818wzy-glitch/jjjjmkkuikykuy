"""A finite queue budget is separate from Docker I/O and still cancellable."""
import sys
import threading
import time

import pytest

from paperflow.remote_transport import request


SCRIPT=r'''
import json,sys,time
h=json.loads(sys.stdin.readline());print(json.dumps(dict(connection_protocol='queued-ssh/1',ready=True)),flush=True)
for i in range(10):
 print(json.dumps(dict(event='heartbeat',phase='control_queue',request_id=h['request_id'])),flush=True);time.sleep(.02)
print(json.dumps(dict(pid=123)),flush=True)
'''


def test_bounded_queue_progress_cannot_be_mistaken_for_the_final_response(monkeypatch):
    from contextlib import nullcontext
    monkeypatch.setattr('paperflow.remote_transport.ssh_args',lambda _:[sys.executable,'-u','-c',SCRIPT])
    monkeypatch.setattr('paperflow.remote_transport.connection_slot',lambda *_:nullcontext())
    value=request({},dict(op='status_job'),timeout=.1,queue_timeout=.4)
    assert value==dict(pid=123)


def test_pause_interrupts_a_status_response_while_it_is_queued(monkeypatch):
    from contextlib import nullcontext
    monkeypatch.setattr('paperflow.remote_transport.ssh_args',lambda _:[sys.executable,'-u','-c',SCRIPT])
    monkeypatch.setattr('paperflow.remote_transport.connection_slot',lambda *_:nullcontext())
    stopped=threading.Event();timer=threading.Timer(.05,stopped.set);timer.start();start=time.monotonic()
    try:
        with pytest.raises(ValueError):request({},dict(op='status_job'),timeout=1,queue_timeout=1,cancelled=stopped.is_set)
    finally:timer.cancel()
    assert time.monotonic()-start<.7


def test_heartbeats_never_extend_the_finite_total_budget(monkeypatch):
    from contextlib import nullcontext
    monkeypatch.setattr('paperflow.remote_transport.ssh_args',lambda _:[sys.executable,'-u','-c',SCRIPT.replace('range(10)','range(100)')])
    monkeypatch.setattr('paperflow.remote_transport.connection_slot',lambda *_:nullcontext())
    start=time.monotonic()
    with pytest.raises(TimeoutError):request({},dict(op='status_job'),timeout=.1,queue_timeout=.15)
    assert time.monotonic()-start<.7


def test_large_upload_drains_queue_progress_before_sending_the_archive(tmp_path,monkeypatch):
    from contextlib import nullcontext
    from paperflow.remote_files import manifest
    script=r'''
import json,sys,tarfile
h=json.loads(sys.stdin.buffer.readline());print(json.dumps(dict(connection_protocol='queued-ssh/1',ready=True)),flush=True)
assert h.get('transfer_protocol')=='queued-transfer/1'
# Exceed both pipe capacities before consuming the incoming archive.
for i in range(3000):
 print(json.dumps(dict(event='heartbeat',phase='transfer_queue',request_id=h['request_id'])),flush=True)
print(json.dumps(dict(event='transfer_ready',request_id=h['request_id'])),flush=True)
with tarfile.open(fileobj=sys.stdin.buffer,mode='r|') as archive:
 for item in archive:
  assert len(archive.extractfile(item).read())==item.size
print(json.dumps(dict(prepared=True)),flush=True)
'''
    monkeypatch.setattr('paperflow.remote_transport.ssh_args',lambda _:[sys.executable,'-u','-c',script])
    monkeypatch.setattr('paperflow.remote_transport.connection_slot',lambda *_:nullcontext())
    path=tmp_path/'large';path.write_bytes(b'x'*(1024*1024));files={'large':path}
    assert request({},dict(op='prepare',files=manifest(files)),files=files,timeout=2,queue_timeout=1)==dict(prepared=True)


def test_late_final_response_is_rejected_even_if_the_timer_thread_was_delayed(monkeypatch):
    import io
    import paperflow.remote_transport as module
    class Delayed:
        def readline(self,*_):time.sleep(.06);return b'{"ok":true}\n'
        closed=False
        def close(self):self.closed=True
    class Process:
        stdin=io.BytesIO();stdout=Delayed();stderr=io.BytesIO();remote_context={}
        returncode=0
        def poll(self):return 0
        def wait(self,**_):return 0
    class Timer:
        def __init__(self,*_):pass
        def start(self):pass
        def cancel(self):pass
    monkeypatch.setattr(module,'spawn',lambda *_a,**_k:Process())
    monkeypatch.setattr(module.threading,'Timer',Timer)
    with pytest.raises(TimeoutError):module.request({},dict(op='status_job'),timeout=.02)
