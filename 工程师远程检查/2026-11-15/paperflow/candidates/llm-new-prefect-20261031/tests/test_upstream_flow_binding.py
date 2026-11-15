"""Bind the retained real folder FlowRun rather than a fake TaskRun."""
from contextlib import closing
import sqlite3
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from test_upstream_binding import bound
from upstream_flow_binding import UpstreamFlowBinding


@pytest.fixture
def actual(bound):
    original, database, identity, manifest, client, parent, _, deployment = bound
    folder_id = uuid4()
    identity['task_run_id'] = str(folder_id)
    folder_deployment = SimpleNamespace(id=uuid4(), tags=['paperflow-order-v2-live-20261031'],
                                        work_pool_name='paperflow-unified-preprocessing')
    folder = SimpleNamespace(id=folder_id, deployment_id=folder_deployment.id, parent_task_run_id=None,
        idempotency_key='folder-waiting-v2-' + identity['task_id'] + '-' + identity['group_id'],
        work_queue_name='task-' + identity['task_id'],
        parameters={key: identity[key] for key in ('task_id', 'group_id', 'receipt_id', 'attempt_id')},
        state=parent.state)
    with closing(sqlite3.connect(database)) as db, db:
        db.execute('CREATE TABLE preprocessing_folder_orders(task_id TEXT,group_id TEXT,receipt_id TEXT,'
                   'attempt_id TEXT,prefect_run_id TEXT)')
        db.execute('INSERT INTO preprocessing_folder_orders VALUES(?,?,?,?,?)',
                   tuple(identity[key] for key in ('task_id','group_id','receipt_id','attempt_id','task_run_id')))
    client.read_flow_run = lambda value: parent if value == parent.id else folder
    client.read_deployment = lambda value: deployment if value == parent.deployment_id else folder_deployment
    verifier = UpstreamFlowBinding(database, database, client, str(client.api_url), 'local-intake',
                                   original.verify_receipt)
    return verifier, database, identity, manifest, client, folder, folder_deployment


def test_actual_folder_and_frozen_handoff_are_accepted(actual):
    verifier, _, identity, manifest, *_ = actual
    assert verifier(identity) == manifest


@pytest.mark.parametrize('field,value', [
    ('idempotency_key', 'wrong'), ('work_queue_name', 'task-other'), ('parent_task_run_id', uuid4())])
def test_unrelated_flow_never_admitted(actual, field, value):
    verifier, _, identity, _, _, folder, _ = actual
    setattr(folder, field, value)
    with pytest.raises(ValueError, match='工单'):
        verifier(identity)


def test_changed_folder_binding_or_old_deployment_is_rejected(actual):
    verifier, database, identity, _, _, _, deployment = actual
    with closing(sqlite3.connect(database)) as db, db:
        db.execute('UPDATE preprocessing_folder_orders SET prefect_run_id=?', (str(uuid4()),))
    with pytest.raises(ValueError, match='绑定'):
        verifier(identity)
    with closing(sqlite3.connect(database)) as db, db:
        db.execute('UPDATE preprocessing_folder_orders SET prefect_run_id=?', (identity['task_run_id'],))
    deployment.tags = ['unrelated']
    with pytest.raises(ValueError, match='Prefect'):
        verifier(identity)
