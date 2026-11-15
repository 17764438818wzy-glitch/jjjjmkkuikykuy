"""Synthetic metadata-only server fence protocol; no network or live queue."""
from uuid import uuid4

import pytest

from remote_jobs import JobConflict
from task_fence_rpc import FenceProtocolError, execute
from test_task_generation_fence import initialized, item


def context(tmp_path):
    ledger, fence = initialized(tmp_path)
    server = dict(server_id='synthetic-server', ledger_id=ledger.ledger_id)
    request = dict(server_identity=server, nonce=str(uuid4()), task_id=str(uuid4()), generation=2)
    return ledger, fence, server, request


def test_close_rpc_binds_server_nonce_and_exact_task_generation(tmp_path):
    ledger, fence, server, request = context(tmp_path)
    result = execute('close', request, fence=fence, server_identity=server)
    assert result == dict(schema='paperflow-server-task-fence/1', server_identity=server,
        nonce=request['nonce'], task_id=request['task_id'], requested_generation=2,
        closed_through=2, jobs=dict(reserved=0, running=0, unknown=0, finished=0),
        stopped_or_terminated_not_proven=True)
    with pytest.raises(JobConflict):
        ledger.reserve(item(request['task_id'], 2))
    assert execute('close', request, fence=fence, server_identity=server) == result


@pytest.mark.parametrize('change', ['server', 'ledger', 'nonce', 'task', 'generation', 'extra', 'operation'])
def test_bad_rpc_never_closes_generation(tmp_path, change):
    _, fence, server, request = context(tmp_path)
    if change == 'server': request['server_identity'] = dict(server, server_id='other')
    if change == 'ledger': request['server_identity'] = dict(server, ledger_id=str(uuid4()))
    if change == 'nonce': request['nonce'] = 'invalid'
    if change == 'task': request['task_id'] = 'invalid'
    if change == 'generation': request['generation'] = True
    if change == 'extra': request['unexpected'] = 1
    with pytest.raises(FenceProtocolError):
        execute('other' if change == 'operation' else 'close', request, fence=fence,
                server_identity=server)
    with fence.ledger._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM task_generation_fences').fetchone()[0] == 0
