import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from paperflow.control import Manager
from paperflow.remote_control import RemoteExecutionGuard
from paperflow.store import Store
from test_winjob import wait_until


@pytest.fixture
def guarded(tmp_path,monkeypatch):
    monkeypatch.setattr('paperflow.remote_control.ssh_args',lambda _:[])
    monkeypatch.setattr('paperflow.remote_control.endpoint_identity',lambda _:dict(pinned=True))
    manager=Manager(Store(tmp_path/'state.sqlite3'),tmp_path/'manager').start();manager.resume()
    guard=RemoteExecutionGuard(manager,dict(host='isolated',image='test-image',deployment_id='test-deployment'))
    ex=manager.launch('graph','synthetic',[sys.executable,'-c','pass'],cwd=Path.cwd())
    manager.ledger._write(lambda c:guard.register_sql(c,ex.execution_id,manager.epoch))
    try:yield manager,guard,ex
    finally:
        guard.close();manager.shutdown()


def test_remote_failure_is_saved_with_phase_and_identity_and_still_pauses(guarded,monkeypatch):
    manager,guard,ex=guarded
    def fail(*args,**kwargs):raise TimeoutError('api_key=SECRET_FIXTURE_ONLY manuscript=DO_NOT_RECORD')
    monkeypatch.setattr('paperflow.remote_control.request',fail)
    with pytest.raises(TimeoutError):guard.reap_sync(ex.execution_id,manager.epoch)
    path=manager.root/'remote-exit-errors.jsonl'
    assert path.is_file(),'the actual exit failure must survive a manager restart'
    raw=path.read_text(encoding='utf-8');record=json.loads(raw.splitlines()[-1])
    assert record['phase']=='remote_request' and record['error']['type']=='TimeoutError'
    assert record['execution_id']==ex.execution_id and record['epoch']==manager.epoch
    assert record['elapsed_seconds']>=0 and 'SECRET_FIXTURE_ONLY' not in raw and 'DO_NOT_RECORD' not in raw
    assert not manager.status()['admission_open']
    assert manager.ledger.rows('SELECT state FROM remote_harness_executions WHERE execution_id=?',(ex.execution_id,))[0]['state']=='owned'


def test_valid_remote_proof_with_busy_sqlite_is_identified_as_local_persistence(guarded,monkeypatch):
    manager,guard,ex=guarded
    wait_until(lambda:ex.exit_saved)
    proof=dict(execution_id=ex.execution_id,epoch=manager.epoch,all_exited=True,execution_closed=True,jobs=[])
    monkeypatch.setattr('paperflow.remote_control.request',lambda *a,**k:proof)
    lock=sqlite3.connect(manager.store.path);lock.execute('BEGIN IMMEDIATE')
    try:
        with pytest.raises(sqlite3.OperationalError):guard.reap_sync(ex.execution_id,manager.epoch)
        path=manager.root/'remote-exit-errors.jsonl'
        assert path.is_file()
        record=json.loads(path.read_text(encoding='utf-8').splitlines()[-1])
        assert record['phase']=='proof_persist' and record['error']['reason']=='database_busy'
        assert not manager.status()['admission_open']
    finally:lock.rollback();lock.close()


def test_docker_diagnostic_is_specific_without_dumping_command_or_stderr():
    from paperflow.remote_diagnostics import describe
    exc=subprocess.CalledProcessError(1,['docker','rm','--force','SECRET_ARGUMENT'],stderr='removal is already in progress SECRET_BODY')
    value=describe(exc);raw=json.dumps(value)
    assert value['reason']=='removal_in_progress' and value['returncode']==1
    assert 'SECRET_ARGUMENT' not in raw and 'SECRET_BODY' not in raw


def test_diagnostic_disk_failure_keeps_original_error_and_pause(guarded,monkeypatch):
    manager,guard,ex=guarded
    def fail(*args,**kwargs):raise TimeoutError('controlled timeout')
    def disk_full(record):raise OSError('controlled disk failure')
    monkeypatch.setattr('paperflow.remote_control.request',fail)
    monkeypatch.setattr(guard.diagnostics.handler,'emit',disk_full)
    with pytest.raises(TimeoutError):guard.reap_sync(ex.execution_id,manager.epoch)
    assert not manager.status()['admission_open']
    assert guard.last_error['phase']=='remote_request'
    assert guard.diagnostics.error=='diagnostic_write_failed'


def test_protocol_failure_retains_remote_phase_and_deadline_without_payload(tmp_path,monkeypatch):
    from paperflow import remote_transport as transport
    from paperflow.remote_diagnostics import describe
    program='''import json,sys
h=json.loads(sys.stdin.buffer.readline())
print(json.dumps(dict(connection_protocol='queued-ssh/1',ready=True)),flush=True)
print(json.dumps(dict(error='CalledProcessError',detail=dict(type='CalledProcessError',reason='removal_in_progress',returncode=1,context=dict(phase='container_remove',job_id='d'*32)),payload='DO_NOT_LOG')),flush=True)
'''
    monkeypatch.setattr(transport,'ssh_args',lambda _:[sys.executable,'-B','-c',program])
    settings=dict(host='isolated',username='test',key_file=str(tmp_path/'key'))
    with pytest.raises(ConnectionError) as caught:transport.request(settings,dict(op='reap_execution'),timeout=1)
    detail=describe(caught.value)
    assert detail['context']['phase']=='request_response'
    assert detail['remote']['reason']=='removal_in_progress'
    assert detail['remote']['context']['phase']=='container_remove'
    assert detail['context']['operation']=='reap_execution' and len(detail['context']['request_id'])==32
    assert 'DO_NOT_LOG' not in json.dumps(detail)


def test_operation_deadline_is_reported_as_response_timeout(tmp_path,monkeypatch):
    from paperflow import remote_transport as transport
    program="import json,sys,time;sys.stdin.buffer.readline();print(json.dumps(dict(connection_protocol='queued-ssh/1',ready=True)),flush=True);time.sleep(10)"
    monkeypatch.setattr(transport,'ssh_args',lambda _:[sys.executable,'-B','-c',program])
    settings=dict(host='isolated',username='test',key_file=str(tmp_path/'key'))
    with pytest.raises(TimeoutError) as caught:transport.request(settings,dict(op='reap_execution'),timeout=.2)
    context=caught.value.remote_context
    assert context['phase']=='request_response' and context['timeout_seconds']==.2
    assert context['queue_seconds']>=0 and context['auth_seconds']>=0 and context['operation_seconds']>=0
