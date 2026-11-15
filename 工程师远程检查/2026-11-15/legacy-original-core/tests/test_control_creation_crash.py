import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from paperflow.control import Manager
from paperflow.store import Store
from paperflow.winjob import identity_alive
from test_winjob import wait_until


@pytest.mark.parametrize('boundary',['before_job','suspended_before_pid_save','pid_saved_before_resume','after_resume'])
def test_crash_at_creation_boundaries_never_leaves_unmanaged_worker(tmp_path,boundary):
    code='''
import json,os,sys,time
from pathlib import Path
import paperflow.control as control
from paperflow.store import Store
from paperflow.winjob import Job,ManagedProcess,process_identity
root=Path(sys.argv[1]);boundary=sys.argv[2]
def crash(identities):
    (root/'crashed.json').write_text(json.dumps(identities));os._exit(86)
original_job=control.Job
if boundary=='before_job':
    def create(name):crash([])
    control.Job=create
elif boundary=='suspended_before_pid_save':
    original=Job.spawn
    def spawn(*args,**kwargs):
        process=original(*args,**kwargs);crash([process.identity])
    Job.spawn=spawn
else:
    original=ManagedProcess.resume
    def resume(self):
        if boundary=='after_resume':original(self);time.sleep(.1)
        crash([self.identity])
    ManagedProcess.resume=resume
manager=control.Manager(Store(root/'ledger.sqlite3'),root/'manager').start();manager.resume()
manager.launch('graph','crash-boundary',[sys.executable,'-c','import time;time.sleep(120)'],cwd=Path.cwd())
'''
    owner=subprocess.Popen([sys.executable,'-c',code,str(tmp_path),boundary],creationflags=0x08000000)
    try:
        assert owner.wait(timeout=6)==86
        identities=json.loads((tmp_path/'crashed.json').read_text())
        reopened=Manager(Store(tmp_path/'ledger.sqlite3'),tmp_path/'manager').start()
        try:
            assert reopened.status()['admission_open'] is False
            assert reopened.ledger.rows('SELECT state FROM control_executions')[0]['state']=='exited'
            wait_until(lambda:all(not identity_alive(item) for item in identities))
        finally:reopened.shutdown()
    finally:
        if owner.poll() is None:owner.kill()
        owner.wait(timeout=5)
