"""Experimental bounded metadata protocol for one server task fence.

The caller must independently authenticate SSH and pin the server ledger.
This module does not expose a command, open a socket or modify the live system.
"""
import copy

from remote_jobs import _uuid
from task_generation_fence import TaskGenerationFence


class FenceProtocolError(ValueError):
    pass


def execute(operation, request, *, fence, server_identity):
    if (operation != 'close' or not isinstance(fence, TaskGenerationFence)
            or not isinstance(server_identity, dict) or set(server_identity) != {'server_id', 'ledger_id'}
            or not isinstance(server_identity['server_id'], str) or not server_identity['server_id']
            or server_identity['ledger_id'] != fence.ledger.ledger_id
            or not isinstance(request, dict)
            or set(request) != {'server_identity', 'nonce', 'task_id', 'generation'}
            or request['server_identity'] != server_identity
            or not _uuid(request['nonce']) or not _uuid(request['task_id'])
            or type(request['generation']) is not int or not 1 <= request['generation'] < 2**63):
        raise FenceProtocolError('exact server, request and task generation are required')
    result = fence.close(request['task_id'], request['generation'])
    return dict(schema='paperflow-server-task-fence/1', server_identity=copy.deepcopy(server_identity),
        nonce=request['nonce'], task_id=request['task_id'], requested_generation=request['generation'],
        closed_through=result['closed_through'], jobs=result['jobs'],
        stopped_or_terminated_not_proven=True)
