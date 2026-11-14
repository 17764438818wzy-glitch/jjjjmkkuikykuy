"""Per-specialty body delivery using the existing formal publisher.

The caller must supply the packet from the native sealed-package validator,
a client guarded at every write by the new parent-task control, and a durable
server receipt writer. This module is not a scheduler or a live integration.
"""

import copy

from preextract_batch_delivery import delivery_publisher
from ragflow_client import RagflowHTTPError

from .routes import select_formal_routes
from .transport import TaskPaused, definite_rejection


def publish_body_targets(paper, identity, handoff, prepared, packet, client, targets,
                         save_targets, check_running, *, retry_failed=False):
    routes = packet['routes']
    if (not 1 <= len(routes) <= 3
            or select_formal_routes(packet['profile'], packet['assets'], prepared['catalog']) != routes
            or packet['manifest_sha256'] != handoff['body']['manifest_sha256']
            or prepared['task']['source_sha256'] != paper['body']['sha256']
            or prepared['validation']['embedding']['admission_verified'] is not True):
        raise ValueError('sealed body or formal targets are not ready')
    for route in routes:
        target = targets.get(route['code'], {})
        state = target.get('body_delivery_state')
        if state and (target.get('formal_route') != route or target.get('ticket_identity') != identity):
            raise ValueError('formal target changed after publication began')
        if state in ('in_flight', 'unknown'):
            raise ValueError('body target outcome unknown; reconciliation required')
        if state == 'failed' and not retry_failed:
            raise ValueError('body target requires a user-selected retry')

    def save(rows):
        if not save_targets(rows):
            raise ValueError('server body receipt was not saved')

    publisher = delivery_publisher(client, save, prepared, packet)
    for route in routes:
        code = route['code']
        if targets.get(code, {}).get('body_delivery_state') == 'complete':
            continue
        if check_running() is not True:
            raise InterruptedError('parent task is paused')
        target = targets.setdefault(code, {})
        target['formal_route'] = copy.deepcopy(route)
        target['ticket_identity'] = copy.deepcopy(identity)
        target['body_delivery_state'] = 'in_flight'
        save(targets)
        selected = dict(packet, routes=[route])
        try:
            publisher.publish(prepared['task'], selected, targets)
            publisher.verify(prepared['task'], selected, targets)
            publisher.set_enabled({code: target}, True)
            publisher.verify_retrieval(prepared['task'], selected, targets)
        except TaskPaused:
            value = copy.deepcopy(targets)
            value[code]['body_delivery_state'] = 'paused'
            save(value)
            targets.clear()
            targets.update(value)
            raise
        except RagflowHTTPError as exc:
            if definite_rejection(exc):
                value = copy.deepcopy(targets)
                value[code].update(body_delivery_state='failed', http_status=exc.status_code)
                save(value)
                targets.clear()
                targets.update(value)
            raise
        value = copy.deepcopy(targets)
        value[code]['body_delivery_state'] = 'complete'
        save(value)
        targets.clear()
        targets.update(value)
    return targets
