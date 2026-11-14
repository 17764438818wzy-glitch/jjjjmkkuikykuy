"""Publish complete attachment handoffs through the existing formal script."""

import copy

from ragflow_client import RagflowHTTPError

from .attachment_delivery import ingest_attachment
from .transport import TaskPaused, definite_rejection


def publish_attachments(paper, identity, handoff, packet, body_targets, client, receipt_root, budget,
                        original_folder_id, history_reviewed, results, save_results, check_running,
                        *, retry_failed=False):
    routes = packet['routes']
    if (not 1 <= len(routes) <= 3 or packet['manifest_sha256'] != handoff['body']['manifest_sha256']
            or not history_reviewed):
        raise ValueError('attachment route or history is not ready')
    if not paper['attachments']:
        return results
    profiles = [row for row in packet['manifest']['files'] if row['path'] == 'profile.json']
    if len(profiles) != 1 or not original_folder_id:
        raise ValueError('sealed profile or formal attachment folder is unavailable')
    parents, destinations = [], []
    for route in routes:
        target = body_targets.get(route['code']) or {}
        body = target.get('body') or {}
        if (target.get('body_delivery_state') != 'complete' or target.get('verified') is not True
                or target.get('retrieval_verified') is not True
                or target.get('formal_route') != route
                or target.get('ticket_identity') != identity
                or body.get('dataset_id') != route['paper_dataset_id'] or not body.get('document_id')
                or not route.get('image_dataset_id')):
            raise ValueError('all formal body targets must be complete')
        parents.append({'code': route['code'], 'dataset_id': body['dataset_id'],
                        'document_id': body['document_id'], 'identity': body['identity']})
        destinations.append({'code': route['code'], 'name': route['name'], 'dataset_id': route['image_dataset_id']})
    destinations[0]['parent_id'] = original_folder_id
    route = {'paper_id': paper['paper_id'], 'body_source_sha256': paper['body']['sha256'],
             'profile_sha256': profiles[0]['sha256'], 'routing_codes': [row['code'] for row in destinations],
             'primary': destinations[0], 'references': destinations[1:],
             'history_reviewed': True, 'parent_targets': parents}
    for item in handoff['attachments']:
        previous = results.get(item['attachment_id']) or {}
        if previous.get('delivery_state') and previous.get('ticket_identity') != identity:
            raise ValueError('attachment result belongs to another child ticket')
        if previous.get('delivery_state') in ('in_flight', 'unknown'):
            raise ValueError('attachment outcome unknown; reconciliation required')
        if previous.get('delivery_state') == 'failed' and not retry_failed:
            raise ValueError('attachment requires a user-selected retry')
    for item in handoff['attachments']:
        attachment_id = item['attachment_id']
        if results.get(attachment_id, {}).get('delivery_state') == 'complete':
            continue
        if check_running() is not True:
            raise InterruptedError('parent task is paused')
        results[attachment_id] = {'delivery_state': 'in_flight', 'ticket_identity': copy.deepcopy(identity)}
        if not save_results(results):
            raise ValueError('server attachment intent was not saved')
        try:
            root = receipt_root[attachment_id] if isinstance(receipt_root, dict) else receipt_root
            result = ingest_attachment(item, route, client, root, budget)
        except TaskPaused:
            value = copy.deepcopy(results)
            value[attachment_id]['delivery_state'] = 'paused'
            if not save_results(value):
                raise ValueError('server attachment pause was not saved')
            results.clear()
            results.update(value)
            raise
        except RagflowHTTPError as exc:
            if definite_rejection(exc):
                value = copy.deepcopy(results)
                value[attachment_id].update(delivery_state='failed', http_status=exc.status_code)
                if not save_results(value):
                    raise ValueError('server attachment failure was not saved') from exc
                results.clear()
                results.update(value)
            raise
        result['delivery_state'] = 'complete'
        result['ticket_identity'] = copy.deepcopy(identity)
        value = copy.deepcopy(results)
        value[attachment_id] = result
        if not save_results(value):
            raise ValueError('server attachment result was not saved')
        results.clear()
        results.update(value)
    return results
