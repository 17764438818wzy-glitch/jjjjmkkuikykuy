from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import sys
import threading
import time

import pytest

from paperflow.store import Store
from test_winjob import wait_until


@pytest.fixture
def manager(tmp_path):
    from paperflow.control import Manager
    controller = Manager(Store(tmp_path / 'ledger.sqlite3'), tmp_path / 'manager')
    controller.start()
    controller.resume()
    yield controller
    controller.shutdown(timeout=35)


WORKER = '''
import json, os, sys, time
from pathlib import Path
from paperflow.control_ipc import WorkerClient
client=WorkerClient.from_environment()
root=Path(sys.argv[1]); root.mkdir(parents=True,exist_ok=True)
(root/'ready').touch()
while not (root/'go').exists() and not client.stopped.wait(.01): pass
try:
    kind=sys.argv[2]
    result=client.perform(kind, {'fixture':str(root)}, lambda cancel:(root/'performed').write_text('once'),total_timeout=60)
    (root/'result').write_text(json.dumps(result))
except Exception as exc:
    (root/'denied').write_text(type(exc).__name__)
finally:
    client.close()
'''


def start_worker(manager, tmp_path, unit='graph', name='one', action='local'):
    directory = tmp_path / name
    directory.mkdir(exist_ok=True)
    execution = manager.launch(unit, name, [sys.executable, '-c', WORKER, str(directory), action],
                               cwd=Path.cwd(), total_timeout=90)
    wait_until(lambda: (directory / 'ready').exists())
    return execution, directory


def test_three_business_entries_have_no_admission_while_pause_write_is_blocked(manager, tmp_path):
    workers = [start_worker(manager, tmp_path, kind, kind, action) for kind, action in
               [('graph', 'llm'), ('delivery', 'rag_write'), ('cleanup', 'cleanup_write')]]
    with manager.ledger.store.db() as writer:
        writer.execute('BEGIN IMMEDIATE')
        started = time.monotonic()
        manager.pause('test locked pause')
        assert time.monotonic() - started < 1
        assert manager.status()['admission_open'] is False
        assert manager.status()['pause_persisted'] is False
        for execution, directory in workers:
            (directory / 'go').touch()
        time.sleep(.2)
        assert not any((directory / 'performed').exists() for _, directory in workers)
    wait_until(lambda: manager.status()['pause_persisted'])
    wait_until(lambda: manager.status()['live_units'] == 0)
    assert not any((directory / 'performed').exists() for _, directory in workers)


def test_each_action_uses_new_permission_and_unit_is_exclusive(manager, tmp_path):
    execution, directory = start_worker(manager, tmp_path)
    with pytest.raises(ValueError, match='owned'):
        manager.launch('graph', 'one', [sys.executable, '-c', 'pass'], cwd=Path.cwd())
    (directory / 'go').touch()
    wait_until(lambda: (directory / 'performed').exists())
    wait_until(lambda: manager.status()['live_units'] == 0)
    with manager.ledger.store.db() as c:
        rows = c.execute('SELECT state FROM control_actions').fetchall()
    assert len(rows) == 1 and rows[0]['state'] == 'complete'


def test_pause_races_with_waiting_request_and_final_begin(manager, tmp_path):
    execution, directory = start_worker(manager, tmp_path)
    snapshot = manager.snapshot(execution.execution_id, 'local')
    with manager.ledger.store.db() as writer, ThreadPoolExecutor(1) as pool:
        writer.execute('BEGIN IMMEDIATE')
        future = pool.submit(manager.request_action, execution.execution_id, 'waiting', 'local',
                             {'file': 'one'}, 0, 30, snapshot)
        time.sleep(.04)
        manager.pause('request was waiting')
        writer.commit()
        with pytest.raises(ValueError):
            future.result(timeout=3)
    assert not (directory / 'performed').exists()


def test_scope_change_between_database_check_and_final_grant_rejects_old_ticket(manager, tmp_path):
    execution, directory = start_worker(manager, tmp_path)
    snapshot = manager.snapshot(execution.execution_id, 'local')
    ticket = manager.request_action(execution.execution_id, 'race', 'local', {'x': 1}, 0, 30, snapshot)
    checked = manager.check_begin(execution.execution_id, ticket['ticket'])
    manager.change_admission_state(execution.scope, 'freeze', {'frozen': True})
    with pytest.raises(ValueError):
        manager.grant_begin(execution.execution_id, ticket['ticket'], checked)
    assert not (directory / 'performed').exists()


def test_one_use_ticket_expires_on_pause_and_restart(tmp_path):
    from paperflow.control import Manager
    controller = Manager(Store(tmp_path / 'state.sqlite3'), tmp_path / 'manager')
    controller.start()
    epoch = controller.epoch
    assert controller.status()['admission_open'] is False
    controller.resume()
    controller.pause('persistent stop')
    controller.shutdown()
    reopened = Manager(Store(tmp_path / 'state.sqlite3'), tmp_path / 'manager')
    reopened.start()
    try:
        assert reopened.epoch != epoch
        assert reopened.status()['admission_open'] is False
    finally:
        reopened.shutdown()
