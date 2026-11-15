"""Publish the already processed body to this run's isolated native targets."""
from pathlib import Path

from paperflow.native_delivery import NativeDeliveryPublisher
from paperflow_prefect.records import read_json, write_json
from preextract_batch_delivery import _qualified

from effect_journal import EffectUnknown
from folder_native_client import FolderNativeClient


def publish_body(prepared, packet, route_plan, raw, journal, admit, require_runtime, config, receipt_path):
    task = prepared['task']
    plan_routes = route_plan['routes']
    codes = [row['code'] for row in packet['routes']]
    body_targets = [row['paper_dataset_id'] for row in plan_routes]
    targets = body_targets + ([row['image_dataset_id'] for row in plan_routes]
                              if route_plan['body_images_required'] else [])
    formal = {value for route in packet['routes'] for key, value in route.items()
              if key in ('paper_dataset_id', 'image_dataset_id') and value}
    if (route_plan.get('schema') != 'paperflow-isolated-test-route-plan/1'
            or route_plan['manifest_sha256'] != packet['manifest_sha256']
            or [row['code'] for row in plan_routes] != codes or len(set(codes)) != len(codes)
            or len(set(targets)) != len(targets) or not targets or formal.intersection(targets)):
        raise ValueError('isolated target plan does not match the sealed paper')
    packet = dict(packet, routes=plan_routes)
    client = FolderNativeClient(raw, journal, targets, config.native_pipeline_id, admit, require_runtime)
    path = Path(receipt_path)
    binding = {'task_id': task['task_id'], 'source_sha256': task['source_sha256'],
               'revision': task['revision'], 'manifest_sha256': packet['manifest_sha256'],
               'run_id': route_plan['run_id'], 'targets': dict(zip(codes, body_targets)),
               'journal_id': journal.identity()}
    state = read_json(path) if path.is_file() else {
        'schema': 'paperflow-isolated-native-body/1', 'binding': binding, 'targets': {},
        'body_verified': False, 'group_rag_complete': False}
    if state.get('schema') != 'paperflow-isolated-native-body/1' or state.get('binding') != binding:
        raise ValueError('body receipt belongs to a different test or publication')
    state['body_verified'] = False
    def save(receipts):
        state['targets'] = receipts
        write_json(path, state)
    save(state['targets'])
    publisher = NativeDeliveryPublisher(client, save, config)
    if not _qualified(prepared, packet, publisher):
        raise ValueError('body records are not qualified for native delivery')
    if bool(publisher.records(packet)['images']) != route_plan['body_images_required']:
        raise ValueError('body image plan does not match the sealed packet')
    for route in plan_routes:
        code = route['code']
        subset = dict(packet, routes=[route])
        target = state['targets'].get(code, {})
        body = target.get('body', {})
        native = body.get('native_batch')
        if native and body.get('document_id'):
            ds, did = body['dataset_id'], body['document_id']
            submission = client.ingest_state(ds, did, config.native_pipeline_id)
            if submission == 'intent':
                raise EffectUnknown('native response requires reconciliation')
            if submission is None and native.get('state') != 'intent':
                raise EffectUnknown('native submission history is unavailable')
            if submission == 'returned' and native.get('state') == 'intent':
                client.ingest(ds, did, config.native_pipeline_id, timeout=config.native_ingest_timeout)
        if not target.get('verified'):
            publisher.publish(task, subset, state['targets'])
        publisher.verify(task, subset, state['targets'])
        selected = {code: state['targets'][code]}
        if not target.get('retrieval_verified'):
            publisher.set_enabled(selected, True)
        else:
            publisher.states(selected, enabled=True)
        publisher.verify(task, subset, state['targets'])
        obj = selected[code]['body']
        doc = publisher.documents(obj['dataset_id'], id=obj['document_id'])[0]
        if (doc.get('pipeline_id') != config.native_pipeline_id or str(doc.get('run')) not in ('3', 'DONE')
                or float(doc.get('progress', 0)) < 1):
            raise EffectUnknown('body native pipeline has not reached a verified terminal state')
        publisher.verify_retrieval(task, subset, state['targets'])
    state['body_verified'] = True
    save(state['targets'])
    return state
