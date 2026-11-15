import json
from pathlib import Path
import subprocess
import sys
import time

import pytest

from paperflow.control import Manager
from paperflow.store import Store
from paperflow.winjob import identity_alive
from test_control import manager, start_worker
from test_winjob import wait_until


HOLD = '''
import json, os, sys, time
from pathlib import Path
from paperflow.control_ipc import WorkerClient
root=Path(sys.argv[1]);root.mkdir(parents=True,exist_ok=True)
client=WorkerClient.from_environment()
def work(cancel):
    (root/'entered').write_text(str(time.monotonic()))
    while not (root/'finish').exists(): time.sleep(.02)
    return {'complete_receipt':'synthetic', 'cancelled':cancel.is_set()}
try:
    client.perform(sys.argv[2], {'fixture':str(root)}, work,total_timeout=float(sys.argv[3]))
finally: client.close()
'''


def hold(controller, root, name, kind='graph', action='llm', timeout=120):
    directory = root / name
    execution = controller.launch(kind, name, [sys.executable, '-c', HOLD, str(directory), action, str(timeout)],
                                  cwd=Path.cwd(), total_timeout=180)
    wait_until(lambda: (directory / 'entered').exists())
    return execution, directory


def test_real_ten_and_thirty_second_deadlines_ignore_sqlite_lock(manager, tmp_path):
    llm, llm_dir = hold(manager, tmp_path, 'llm')
    rag, rag_dir = hold(manager, tmp_path, 'rag', 'delivery', 'rag_write')
    with manager.store.db() as c:
        c.execute('BEGIN IMMEDIATE')
        manager.pause('real grace windows under DB lock')
        start = llm.stop_at
        with pytest.raises(ValueError):
            manager.resume()
        wait_until(lambda: llm.exit_evidence is not None, seconds=12)
        assert rag.exit_evidence is None and rag.job.active_count() > 0
        wait_until(lambda: rag.exit_evidence is not None, seconds=22)
        assert 9.9 <= llm.kill_sent - start < 10.5
        assert 29.9 <= rag.kill_sent - start < 30.5
        assert manager.status()['pause_persisted'] is False
    wait_until(lambda: manager.status()['live_units'] == 0)
    rows = manager.ledger.rows('SELECT kind,state FROM control_actions')
    assert {r['kind']: r['state'] for r in rows} == {'llm': 'diagnostic', 'rag_write': 'unresolved'}
    Path('evidence/grace-windows.json').write_text(json.dumps(dict(llm=llm.exit_evidence,
        rag=rag.exit_evidence, events=manager.events), indent=2))
    manager.resume()
    again, directory = start_worker(manager, tmp_path, 'delivery', 'rag', 'rag_write')
    (directory / 'go').touch()
    wait_until(lambda: (directory / 'denied').exists())
    assert not (directory / 'performed').exists()


def test_inflight_remote_receipt_can_finish_during_pause_without_next_action(manager, tmp_path):
    execution, directory = hold(manager, tmp_path, 'finish', 'delivery', 'rag_write')
    manager.pause()
    (directory / 'finish').touch()
    wait_until(lambda: execution.exit_saved)
    row = manager.ledger.rows('SELECT state,result FROM control_actions')[0]
    assert row['state'] == 'complete'
    assert json.loads(row['result']) == {'complete_receipt': 'synthetic', 'cancelled': False}
    assert execution.kill_sent is None


def test_original_remote_deadline_is_not_extended_by_pause(manager, tmp_path):
    execution, directory = hold(manager, tmp_path, 'short', 'delivery', 'rag_write', timeout=.5)
    deadline = execution.active_action['deadline']
    manager.pause()
    wait_until(lambda: execution.exit_saved)
    assert execution.kill_at == deadline
    assert execution.kill_sent - deadline < .5


def test_lease_expiry_does_not_release_live_tree_or_allow_duplicate(tmp_path):
    controller = Manager(Store(tmp_path / 'lease.sqlite3'), tmp_path / 'manager', heartbeat_interval=.1,
                         lease_seconds=.35, grace_seconds=.25)
    controller.start(); controller.resume()
    try:
        code = 'import time;time.sleep(120)'  # no hello/heartbeat, but still owned
        execution = controller.launch('graph', 'lease', [sys.executable, '-c', code], cwd=Path.cwd())
        wait_until(lambda: execution.stop_at is not None)
        assert execution.job.active_count() > 0
        with pytest.raises(ValueError, match='owned'):
            controller.launch('graph', 'lease', [sys.executable, '-c', 'pass'], cwd=Path.cwd())
        wait_until(lambda: execution.exit_saved)
        assert execution.exit_evidence['all_recorded_processes_exited']
    finally:
        controller.shutdown()


def test_manager_crash_recovery_rejects_old_epoch_and_preserves_remote_barrier(tmp_path):
    root = tmp_path / 'crash'; root.mkdir()
    code = '''
import json, os, sys, time
from pathlib import Path
from paperflow.control import Manager
from paperflow.store import Store
from test_control_supervision import hold
root=Path(sys.argv[1]);manager=Manager(Store(root/'ledger.sqlite3'),root/'manager')
manager.start();manager.resume()
execution,_=hold(manager,root,'crashed','delivery','rag_write')
while len(execution.identities)<2: time.sleep(.02)
(root/'ready.json').write_text(json.dumps(dict(epoch=manager.epoch,execution_id=execution.execution_id,
    action_id=execution.active_action['action_id'],identities=list(execution.identities.values()))))
while not (root/'crash').exists():time.sleep(.02)
os._exit(86)
'''
    import os
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([os.environ.get('PYTHONPATH',''),str(Path.cwd()),str(Path.cwd()/'tests')]))
    owner = subprocess.Popen([sys.executable, '-c', code, str(root)], env=env, creationflags=0x08000000)
    try:
        wait_until(lambda: (root / 'ready.json').exists())
        before = json.loads((root / 'ready.json').read_text())
        (root / 'crash').touch()
        assert owner.wait(timeout=5) == 86
        reopened = Manager(Store(root / 'ledger.sqlite3'), root / 'manager').start()
        try:
            assert not reopened.status()['admission_open'] and reopened.epoch != before['epoch']
            assert all(not identity_alive(item) for item in before['identities'])
            assert reopened.ledger.rows('SELECT state FROM control_actions')[0]['state'] == 'unresolved'
            late = reopened.record_result(before['execution_id'], before['action_id'], before['epoch'], 'complete', {'late': True})
            assert late == {'saved_fact': True, 'current_qualification': False}
            assert reopened.ledger.rows('SELECT state FROM control_actions')[0]['state'] == 'unresolved'
            with pytest.raises(ValueError):
                reopened.grant_begin(before['execution_id'], before['action_id'], {})
        finally:
            reopened.shutdown()
    finally:
        if owner.poll() is None: owner.kill()
        owner.wait(timeout=5)
