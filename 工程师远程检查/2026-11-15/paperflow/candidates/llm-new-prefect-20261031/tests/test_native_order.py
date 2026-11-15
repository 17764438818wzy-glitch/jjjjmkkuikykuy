"""New real folder TaskRun creation against an isolated Prefect server."""
import importlib.util
from pathlib import Path
import sqlite3
from uuid import uuid4

from prefect import flow
from prefect.client.orchestration import get_client
from prefect.states import Running
from prefect.testing.utilities import prefect_test_harness
import pytest


spec = importlib.util.spec_from_file_location('native_order', Path(__file__).parent.parent / 'native_order.py')
native_order = importlib.util.module_from_spec(spec)
spec.loader.exec_module(native_order)
task_spec = importlib.util.spec_from_file_location('native_task', Path(__file__).parent.parent / 'native_task.py')
native_task = importlib.util.module_from_spec(task_spec)
task_spec.loader.exec_module(native_task)


def test_real_folder_ticket_is_native_idempotent_and_bound_to_parent(tmp_path):
    marker = tmp_path / 'executed.txt'

    @flow
    def parent_flow(task_id: str):
        pass

    def folder_fn():
        marker.write_text('actual work', encoding='utf8')
    executable_folder_task = native_task.FolderTask(folder_fn)

    task_id, group_id = str(uuid4()), uuid4().hex
    order = {'task_id': task_id, 'group_id': group_id, 'paper_id': 'PF_' + uuid4().hex[:24],
             'receipt_id': 'a' * 64, 'attempt_id': uuid4().hex[:24]}
    db_path = tmp_path / 'orders.sqlite3'
    with sqlite3.connect(db_path):
        pass
    store = native_order.TicketStore(db_path)
    store.initialize()

    with prefect_test_harness(server_startup_timeout=60):
        with get_client(sync_client=True) as client:
            flow_id = client.create_flow(parent_flow)
            deployment = client.create_deployment(flow_id, 'new-parent', tags=[native_order.MARKER])
            root = client.create_flow_run_from_deployment(
                deployment, parameters={'task_id': task_id}, state=Running(),
                idempotency_key='first-step-order-v2-' + task_id,
            )
            first = native_order.ensure_ticket(client, store, root.id, order, executable_folder_task)
            second = native_order.ensure_ticket(client, store, root.id, order, executable_folder_task)
            assert first.id == second.id
            assert first.flow_run_id == root.id and first.dynamic_key == group_id
            assert first.task_key == executable_folder_task.task_key
            assert first.task_key != 'paperflow-folder-waiting-v2'
            assert first.state.is_pending()
            assert not marker.exists()  # creation alone does not run work
            with sqlite3.connect(db_path) as db:
                assert db.execute('SELECT COUNT(*) FROM native_folder_tickets').fetchone()[0] == 1
            with pytest.raises(ValueError, match='身份'):
                native_order.ensure_ticket(client, store, root.id, dict(order, receipt_id='b' * 64), executable_folder_task)


def test_nonmatching_deployment_is_rejected_before_reservation(tmp_path):
    @flow
    def parent_flow(task_id: str):
        pass
    def folder_fn():
        return 'executed'
    executable_folder_task = native_task.FolderTask(folder_fn)
    task_id, group_id = str(uuid4()), uuid4().hex
    order = {'task_id': task_id, 'group_id': group_id, 'paper_id': 'PF_test',
             'receipt_id': 'a' * 64, 'attempt_id': uuid4().hex[:24]}
    db_path = tmp_path / 'orders.sqlite3'
    with sqlite3.connect(db_path):
        pass
    store = native_order.TicketStore(db_path)
    store.initialize()
    with prefect_test_harness(server_startup_timeout=60):
        with get_client(sync_client=True) as client:
            flow_id = client.create_flow(parent_flow)
            deployment = client.create_deployment(flow_id, 'wrong-parent', tags=['unrelated'])
            root = client.create_flow_run_from_deployment(
                deployment, parameters={'task_id': task_id}, state=Running(),
                idempotency_key='first-step-order-v2-' + task_id,
            )
            with pytest.raises(ValueError, match='Prefect'):
                native_order.ensure_ticket(client, store, root.id, order, executable_folder_task)
    with sqlite3.connect(db_path) as db:
        assert db.execute('SELECT COUNT(*) FROM native_folder_tickets').fetchone()[0] == 0
