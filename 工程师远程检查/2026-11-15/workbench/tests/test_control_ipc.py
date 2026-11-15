from multiprocessing.connection import Listener
from pathlib import Path
import sys
import threading
import time
import uuid

import pytest

from paperflow.control import Manager
from paperflow.control_ipc import WorkerClient, encode
from paperflow.store import Store
from test_control import manager, start_worker
from test_control_supervision import hold
from test_winjob import wait_until


def test_handshake_timeout_is_bounded_without_server_response():
    address=r'\\.\pipe\pf-stall-'+uuid.uuid4().hex
    listener=Listener(address,family='AF_PIPE',authkey=None)
    connections=[]
    def accept(): connections.append(listener.accept())
    thread=threading.Thread(target=accept,daemon=True);thread.start()
    started=time.monotonic()
    try:
        with pytest.raises(TimeoutError):
            WorkerClient(address,b'synthetic','unknown','expired',timeout=.15)
        assert time.monotonic()-started<.6
    finally:
        listener.close()
        for connection in connections:connection.close()


def test_ipc_disconnect_denies_new_work(manager,tmp_path):
    execution,directory=start_worker(manager,tmp_path)
    execution.peer.close()
    (directory/'go').touch()
    wait_until(lambda:execution.exit_saved)
    assert not (directory/'performed').exists()
    with pytest.raises(ValueError):manager.snapshot(execution.execution_id,'local')


def test_heartbeat_continues_during_long_business_callback(tmp_path):
    manager=Manager(Store(tmp_path/'db.sqlite3'),tmp_path/'manager',heartbeat_interval=.08,lease_seconds=.5).start()
    manager.resume()
    try:
        execution,directory=hold(manager,tmp_path,'long')
        time.sleep(1)
        assert execution.eligible and execution.stop_at is None
        assert manager.ledger.rows('SELECT last_heartbeat FROM control_executions')[0]['last_heartbeat']>time.time()-.5
        (directory/'finish').touch()
        wait_until(lambda:execution.exit_saved)
    finally:manager.shutdown()


def test_persistent_state_can_deny_even_when_immediate_state_is_open(manager,tmp_path):
    execution,_=start_worker(manager,tmp_path)
    with manager.store.db() as c:c.execute('UPDATE controller_state SET paused=1')
    assert not manager.stopped.is_set()
    with pytest.raises(ValueError,match='persistent control'):
        manager.request_action(execution.execution_id,'db-denial','local',{},0,30)


def test_remote_pause_does_not_invoke_local_cooperative_close(manager,tmp_path):
    directory=tmp_path/'remote';directory.mkdir()
    code='''
import sys,time
from pathlib import Path
from paperflow.control_ipc import WorkerClient
root=Path(sys.argv[1]);client=WorkerClient.from_environment(on_cancel=lambda:(root/'local_close').touch())
def work(cancel):
    (root/'entered').touch()
    while not (root/'finish').exists():time.sleep(.01)
    return {'cancelled':cancel.is_set()}
client.perform('rag_write',{},work,total_timeout=40)
client.close()
'''
    execution=manager.launch('delivery','remote',[sys.executable,'-c',code,str(directory)],cwd=Path.cwd())
    wait_until(lambda:(directory/'entered').exists())
    manager.pause()
    time.sleep(.1)
    assert not (directory/'local_close').exists()
    (directory/'finish').touch()
    wait_until(lambda:execution.exit_saved)


def test_json_message_size_is_bounded():
    with pytest.raises(ValueError,match='large'):encode({'data':'x'*65536})


def test_manager_sets_per_execution_temp_directory(manager,tmp_path):
    marker=tmp_path/'temp.json'
    code='import os,json,sys;from pathlib import Path;Path(sys.argv[1]).write_text(json.dumps(dict(temp=os.environ["TEMP"],tmp=os.environ["TMP"])))'
    execution=manager.launch('graph','temp',[sys.executable,'-c',code,str(marker)],cwd=Path.cwd())
    wait_until(marker.exists)
    import json
    value=json.loads(marker.read_text())
    assert value['temp']==value['tmp']
    assert Path(value['temp']).is_relative_to(manager.root/execution.execution_id)
