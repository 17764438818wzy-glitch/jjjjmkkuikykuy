from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys
import threading
import time

import pytest

from paperflow.control import Manager
from paperflow.control_ipc import WorkerClient
from paperflow.winjob import Job
from test_control import manager, start_worker
from test_control_supervision import hold
from test_winjob import wait_until


def test_delivery_and_cleanup_share_target_ownership(manager,tmp_path):
    execution,_=start_worker(manager,tmp_path,'delivery','same-target','rag_write')
    with pytest.raises(ValueError,match='owned'):
        manager.launch('cleanup','same-target',[sys.executable,'-c','pass'],cwd=Path.cwd())


def test_unresolved_delivery_blocks_cleanup_for_same_target(manager,tmp_path):
    execution,directory=start_worker(manager,tmp_path,'delivery','same-target','rag_write')
    action=manager.request_action(execution.execution_id,'lost','rag_write',{'document':'synthetic'},0,30)['ticket']
    checked=manager.check_begin(execution.execution_id,action)
    manager.grant_begin(execution.execution_id,action,checked)
    # A granted request with no receipt remains uncertain after real process-tree termination.
    execution.job.terminate()
    wait_until(lambda:execution.exit_saved)
    cleanup,root=start_worker(manager,tmp_path,'cleanup','same-target','cleanup_write')
    wait_until(lambda:cleanup.peer is not None)
    with pytest.raises(ValueError,match='unresolved'):
        manager.request_action(cleanup.execution_id,'cleanup','cleanup_write',{'document':'synthetic'},0,30)


def test_shutdown_keeps_lock_and_pausing_status_during_suspended_creation(manager,tmp_path,monkeypatch):
    entered,release=threading.Event(),threading.Event()
    original=Job.spawn
    def spawn(*args,**kwargs):
        result=original(*args,**kwargs)
        entered.set();release.wait(3)
        return result
    monkeypatch.setattr(Job,'spawn',spawn)
    with ThreadPoolExecutor(1) as pool:
        future=pool.submit(manager.launch,'graph','creating',[sys.executable,'-c','pass'],cwd=Path.cwd())
        assert entered.wait(2)
        try:
            manager.pause()
            wait_until(lambda:manager.status()['pause_persisted'])
            assert manager.status()['state']=='pausing'
            with pytest.raises(TimeoutError):manager.shutdown(timeout=.1)
            second=Manager(manager.store,tmp_path/'another')
            with pytest.raises(RuntimeError,match='running'):second.start()
        finally:release.set()
        with pytest.raises(ValueError):future.result(timeout=3)
    wait_until(lambda:manager.status()['live_units']==0)


def test_worker_rejects_permission_past_original_deadline():
    client=object.__new__(WorkerClient)
    client.epoch='test';client.expected_generation=0
    client.stopped=threading.Event();client.closed=threading.Event();client.local_cancel=threading.Event()
    replies=iter([{'ticket':'one'},dict(action_id='one',epoch='test',expires=time.monotonic()+1,deadline=time.monotonic()-1),{}])
    client.rpc=lambda *_args:next(replies)
    calls=[]
    with pytest.raises(ValueError):
        client._perform('rag_write',{},lambda _cancel:calls.append('sent'),1,None)
    assert calls==[]


def test_delayed_pipe_permit_cannot_start_after_deadline(manager,tmp_path,monkeypatch):
    original=manager.grant_begin
    def delayed(*args):
        permit=original(*args)
        time.sleep(.25)
        return permit
    monkeypatch.setattr(manager,'grant_begin',delayed)
    # Hold supervisor so this exercises client-side deadline rejection, independent of a cancel notification.
    original_stop=manager._stop_unit_locked
    def stop(execution,reason,now):
        if reason=='deadline or lease expired':return
        original_stop(execution,reason,now)
    monkeypatch.setattr(manager,'_stop_unit_locked',stop)
    root=tmp_path/'delayed';root.mkdir()
    code='''
import sys
from pathlib import Path
from paperflow.control_ipc import WorkerClient
root=Path(sys.argv[1]);client=WorkerClient.from_environment()
try:client.perform('rag_write',{},lambda cancel:(root/'sent').touch(),total_timeout=.15)
except ValueError:(root/'denied').touch()
finally:client.close()
'''
    execution=manager.launch('delivery','delay',[sys.executable,'-c',code,str(root)],cwd=Path.cwd())
    wait_until(lambda:execution.exit_saved)
    assert (root/'denied').exists() and not (root/'sent').exists()
