from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import time

import pytest

from paperflow.control import Manager
from paperflow.store import Store
from test_control import manager, start_worker
from test_winjob import wait_until


@pytest.mark.parametrize('target,changes', [
    ('unit', {'frozen': True}), ('unit', {'expected_generation': 1}),
    ('unit', {'owner': 'another'}), ('service:llm', {'enabled': False}),
])
def test_every_admission_change_invalidates_checked_ticket(manager, tmp_path, target, changes):
    execution, _ = start_worker(manager, tmp_path)
    ticket = manager.request_action(execution.execution_id, 'race', 'llm', {}, 0, 30)['ticket']
    checked = manager.check_begin(execution.execution_id, ticket)
    manager.change_admission_state(execution.scope if target == 'unit' else target, 'change', changes)
    with pytest.raises(ValueError):
        manager.grant_begin(execution.execution_id, ticket, checked)


def test_sqlite_waiting_begin_cannot_grant_after_pause(manager, tmp_path):
    execution, _ = start_worker(manager, tmp_path)
    ticket = manager.request_action(execution.execution_id, 'waiting-begin', 'llm', {}, 0, 30)['ticket']
    with ThreadPoolExecutor(1) as pool, manager.store.db() as c:
        c.execute('BEGIN IMMEDIATE')
        future = pool.submit(manager.check_begin, execution.execution_id, ticket)
        time.sleep(.04)
        manager.pause()
        c.commit()
        with pytest.raises(ValueError):
            future.result(timeout=3)
    assert not any(event['event'] == 'granted' for event in manager.events)


def test_check_begin_rechecks_immediate_stop_after_sql_write(manager,tmp_path,monkeypatch):
    import threading
    execution,_=start_worker(manager,tmp_path)
    ticket=manager.request_action(execution.execution_id,'pause-after-db','llm',{},0,30)['ticket']
    original=manager.ledger._write
    caller=threading.get_ident();stopped=[]
    def after_sql(*args,**kwargs):
        result=original(*args,**kwargs)
        if threading.get_ident()==caller and not stopped:
            stopped.append(True);manager.pause('stop before checked-ticket return')
        return result
    monkeypatch.setattr(manager.ledger,'_write',after_sql)
    with pytest.raises(ValueError):
        manager.check_begin(execution.execution_id,ticket)
    assert ticket not in manager.resource_holds
    assert not any(event['event']=='granted' for event in manager.events)


def test_ticket_single_use_and_expiration(manager, tmp_path):
    execution, _ = start_worker(manager, tmp_path)
    ticket = manager.request_action(execution.execution_id, 'once', 'local', {}, 0, 30)['ticket']
    checked = manager.check_begin(execution.execution_id, ticket)
    manager.grant_begin(execution.execution_id, ticket, checked)
    with pytest.raises(ValueError):
        manager.grant_begin(execution.execution_id, ticket, checked)
    manager.record_result(execution.execution_id, ticket, manager.epoch, 'complete', {})
    ticket = manager.request_action(execution.execution_id, 'old', 'local', {}, 0, 30)['ticket']
    manager.tickets[ticket]['expires'] = time.monotonic() - 1
    with pytest.raises(ValueError):
        manager.check_begin(execution.execution_id, ticket)


def test_final_grant_rechecks_monotonic_execution_deadline(manager, tmp_path):
    execution, _ = start_worker(manager, tmp_path)
    ticket = manager.request_action(execution.execution_id, 'deadline', 'local', {}, 0, 30)['ticket']
    checked = manager.check_begin(execution.execution_id, ticket)
    with manager.gate_lock:
        execution.deadline = time.monotonic() - 1
        with pytest.raises(ValueError):
            manager.grant_begin(execution.execution_id, ticket, checked)


def test_same_ledger_cannot_have_two_managers_with_different_runtime_paths(manager, tmp_path):
    second = Manager(manager.store, tmp_path / 'different-root')
    try:
        with pytest.raises(RuntimeError, match='running'):
            second.start()
    finally:
        if second.started:
            second.shutdown()


def test_ipc_arguments_cannot_supply_an_admission_snapshot(manager, tmp_path):
    execution, _ = start_worker(manager, tmp_path)
    with pytest.raises(ValueError):
        manager.handle(execution.peer, dict(epoch=manager.epoch, method='request_action', args=dict(
            request_id='bad', kind='local', resource={}, expected_generation=0, total_timeout=20,
            snapshot=manager.snapshot(execution.execution_id, 'local'))))


def test_pause_during_suspended_spawn_never_runs_worker(manager, tmp_path, monkeypatch):
    from paperflow.winjob import Job
    original = Job.spawn
    def spawn(*args, **kwargs):
        result = original(*args, **kwargs)
        manager.pause('before resume')
        return result
    monkeypatch.setattr(Job, 'spawn', spawn)
    marker = tmp_path / 'unexpected'
    import sys
    with pytest.raises(ValueError):
        manager.launch('graph', 'during-spawn', [sys.executable, '-c',
            'from pathlib import Path;Path(__import__("sys").argv[1]).touch()', str(marker)], cwd=Path.cwd())
    assert not marker.exists()
    wait_until(lambda: manager.status()['live_units'] == 0)
