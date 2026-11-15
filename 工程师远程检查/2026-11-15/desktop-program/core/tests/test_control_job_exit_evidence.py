import ctypes
import json
from pathlib import Path
import sys
import time
import uuid

import pytest

import paperflow.control as module
from paperflow.control import Manager
from paperflow.control_store import ControlLedger
from paperflow.store import Store
from paperflow.winjob import Job
from test_control import manager,start_worker
from test_winjob import wait_until


def test_empty_owned_job_is_exit_evidence_even_when_historical_pid_access_is_denied(manager,tmp_path,monkeypatch):
    execution,folder=start_worker(manager,tmp_path)
    def denied(identity):raise ctypes.WinError(5)
    with monkeypatch.context() as patch:
        patch.setattr(module,'identity_alive',denied)
        (folder/'go').touch()
        wait_until(lambda:execution.job_empty_at is not None,seconds=3)
        wait_until(lambda:execution.exit_saved,seconds=2)
    assert execution.exit_evidence['active_processes']==0
    assert execution.exit_evidence['all_recorded_processes_exited'] is True


def test_startup_recovery_uses_owned_empty_job_instead_of_unrelated_recycled_pids(tmp_path,monkeypatch):
    store=Store(tmp_path/'state.sqlite3');ledger=ControlLedger(store)
    job=Job('Local\\paperflow-test-'+uuid.uuid4().hex)
    process=job.spawn([sys.executable,'-c','pass'],cwd=Path.cwd());process.resume()
    try:
        wait_until(lambda:job.active_count()==0)
        identity=process.identity;now=time.time();execution=uuid.uuid4().hex
        with store.db() as db:
            db.execute('INSERT INTO control_executions(execution_id,epoch,unit_kind,unit_key,job_name,root_identity,members,state,lease_until,last_heartbeat,deadline) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                (execution,'old','delivery','owned',job.name,json.dumps(identity),json.dumps([identity]),'active',now+20,now,now+40))
        controller=Manager(store,tmp_path/'manager');controller.ledger=ledger
        monkeypatch.setattr(module,'identity_alive',lambda _:(_ for _ in ()).throw(ctypes.WinError(5)))
        controller._recover()
        row=ledger.rows('SELECT state,exit_evidence FROM control_executions')[0]
        assert row['state']=='exited' and json.loads(row['exit_evidence'])['active_processes']==0
    finally:
        job.close();process.close()
