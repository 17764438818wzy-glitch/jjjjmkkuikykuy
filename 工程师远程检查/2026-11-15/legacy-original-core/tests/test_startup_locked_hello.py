"""A connected worker is not an admitted business action."""
import json
from pathlib import Path
import sys
import time

import pytest

from test_control import manager
from test_winjob import wait_until


CONNECT = '''
import json,sys,time
from pathlib import Path
from paperflow.control_ipc import WorkerClient
root=Path(sys.argv[1]);(root/'waiting').touch()
while not (root/'connect').exists():time.sleep(.01)
try:
    client=WorkerClient.from_environment()
    (root/'connected').touch()
    while not (root/'go').exists() and not client.stopped.wait(.01):pass
    try:
        client.perform('local',{},lambda cancel:(root/'performed').touch(),total_timeout=10)
    except Exception as exc:(root/'denied').write_text(type(exc).__name__)
    finally:client.close()
except Exception as exc:(root/'bootstrap-error').write_text(type(exc).__name__)
'''


def waiting_worker(manager, tmp_path, name='worker'):
    root=tmp_path/name;root.mkdir()
    ex=manager.launch('graph',name,[sys.executable,'-B','-c',CONNECT,str(root)],cwd=Path.cwd())
    wait_until(lambda:(root/'waiting').exists())
    return ex,root


def test_hello_does_not_wait_for_sqlite_or_grant_business_work(manager,tmp_path):
    ex,root=waiting_worker(manager,tmp_path)
    with manager.store.db() as db:
        db.execute('BEGIN IMMEDIATE')
        (root/'connect').touch()
        wait_until(lambda:(root/'connected').exists() or (root/'bootstrap-error').exists(),seconds=4)
        assert (root/'connected').exists(), 'SQLite writer killed the IPC bootstrap'
        assert ex.peer and ex.peer.execution_id==ex.execution_id
        assert not (root/'performed').exists()
        # The old authentication timer must not close a successfully bound peer.
        time.sleep(2.1)
        assert not ex.peer.closed.is_set()
    wait_until(lambda:bool(manager.ledger.rows(
        'SELECT worker_identity FROM control_executions WHERE execution_id=? AND worker_identity IS NOT NULL',
        (ex.execution_id,))),seconds=7)
    row=manager.ledger.rows('SELECT worker_identity,members FROM control_executions WHERE execution_id=?',
                            (ex.execution_id,))[0]
    identity=json.loads(row['worker_identity'])
    assert identity in json.loads(row['members'])
    (root/'go').touch()
    wait_until(lambda:ex.exit_saved)
    assert (root/'performed').exists()


def test_pausing_while_hello_database_is_locked_never_admits_work(manager,tmp_path):
    ex,root=waiting_worker(manager,tmp_path)
    with manager.store.db() as db:
        db.execute('BEGIN IMMEDIATE')
        (root/'connect').touch()
        wait_until(lambda:(root/'connected').exists() or (root/'bootstrap-error').exists(),seconds=4)
        assert (root/'connected').exists(), 'SQLite writer killed the IPC bootstrap'
        start=time.monotonic();manager.pause('held SQLite writer')
        assert time.monotonic()-start<.3
        assert not manager.status()['admission_open']
        (root/'go').touch()
        time.sleep(.2)
        assert not (root/'performed').exists()
    wait_until(lambda:ex.exit_saved)
    assert not (root/'performed').exists()
    row=manager.ledger.rows('SELECT worker_identity,exit_evidence FROM control_executions WHERE execution_id=?',
                            (ex.execution_id,))[0]
    assert row['worker_identity']
    assert json.loads(row['worker_identity']) in json.loads(row['exit_evidence'])['identities']


def test_pause_during_identity_validation_rejects_hello(manager,tmp_path,monkeypatch):
    import threading
    import paperflow.control as control
    ex,root=waiting_worker(manager,tmp_path)
    entered=threading.Event();release=threading.Event()
    original=control.process_identity
    def identity(pid):
        if threading.current_thread() is not manager.monitor:
            entered.set();release.wait(2)
        return original(pid)
    monkeypatch.setattr(control,'process_identity',identity)
    (root/'connect').touch()
    assert entered.wait(2)
    manager.pause('identity check in progress');release.set()
    wait_until(lambda:ex.exit_saved)
    assert not (root/'connected').exists()
    assert not (root/'performed').exists()


def test_burst_of_real_worker_connections_survives_one_locked_writer(manager,tmp_path):
    manager.max_live_units=24
    workers=[waiting_worker(manager,tmp_path,'burst-'+str(n)) for n in range(24)]
    with manager.store.db() as db:
        db.execute('BEGIN IMMEDIATE')
        for _,root in workers:(root/'connect').touch()
        wait_until(lambda:all((root/'connected').exists() for _,root in workers),seconds=4)
        assert all(ex.worker_identity and ex.peer and not ex.peer.closed.is_set() for ex,_ in workers)
        assert not any((root/'performed').exists() for _,root in workers)
        manager.pause('end isolated connection burst')
    wait_until(lambda:manager.status()['live_units']==0,seconds=12)
    assert not any((root/'performed').exists() for _,root in workers)
    assert all(manager.ledger.rows('SELECT worker_identity FROM control_executions WHERE execution_id=?',
                                  (ex.execution_id,))[0]['worker_identity'] for ex,_ in workers)
