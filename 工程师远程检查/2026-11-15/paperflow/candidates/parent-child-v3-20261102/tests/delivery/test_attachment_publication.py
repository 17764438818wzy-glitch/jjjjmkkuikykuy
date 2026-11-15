import copy
from importlib import import_module

import pytest
from test_attachment_ingest import handoff as attachment_handoff
from test_attachment_publish import Counter, FakeStore
from test_transport_journal import event_store
from preextract_publish import _Budget


@pytest.fixture
def attachment_input(tmp_path):
    item = attachment_handoff(tmp_path)
    paper = {'paper_id': item['paper_id'], 'group_id': 'group-1', 'body': {'file_id': 'body', 'sha256': 'b' * 64},
             'attachments': [{'file_id': item['attachment_id'], 'sha256': item['source_sha256']}]}
    identity = {'task_id': 'task-1', 'group_id': 'group-1', 'paper_id': item['paper_id'], 'receipt_id': 'r',
                'attempt_id': 'a', 'parent_run_id': 'p', 'task_run_id': 'c'}
    receipt = {'schema': 'paperflow-postprocessing-receipt/1', 'identity': dict(identity),
               'validation_status': 'passed', 'body': {'package_path': 'pkg', 'manifest_sha256': 'd' * 64},
               'attachments': [item]}
    routes = [{'code': '081301', 'name': '化学工程与工艺', 'paper_dataset_id': 'body-1', 'image_dataset_id': 'ds-main'},
              {'code': '080206', 'name': '过程装备与控制工程', 'paper_dataset_id': 'body-2', 'image_dataset_id': 'ds-ref'}]
    packet = {'routes': routes, 'manifest_sha256': 'd' * 64,
              'manifest': {'files': [{'path': 'profile.json', 'sha256': 'c' * 64}]}}
    client, body_targets = FakeStore(), {}
    for route in routes:
        code, ds = route['code'], route['paper_dataset_id']
        meta = {'paperflow_task': 'task-1', 'paperflow_source_sha256': 'b' * 64,
                'paperflow_manifest_sha256': 'd' * 64, 'paperflow_revision': '1',
                'paperflow_specialty': code, 'paperflow_role': 'body'}
        did = 'parent-' + code
        client.docs[did] = {'id': did, 'name': did, 'dataset_id': ds, 'meta_fields': dict(meta),
                            'status': '1', 'run': 'DONE', 'chunk_count': 0, '_bytes': b''}
        client.chunks[did] = []
        body_targets[code] = {'body_delivery_state': 'complete', 'verified': True, 'retrieval_verified': True,
                              'formal_route': copy.deepcopy(route), 'ticket_identity': dict(identity),
                              'body': {'dataset_id': ds, 'document_id': did, 'identity': meta}}
    return paper, identity, receipt, packet, body_targets, client, tmp_path / 'receipt-work', _Budget(Counter())


def test_uses_existing_attachment_publisher_for_each_formal_target(attachment_input):
    module = import_module('paperflow_v3.delivery.attachments')
    paper, identity, receipt, packet, body, client, root, budget = attachment_input
    results, snapshots = {}, []

    def save(value):
        snapshots.append(copy.deepcopy(value))
        return 'server-artifact'

    module.publish_attachments(paper, identity, receipt, packet, body, client, root, budget,
                               'folder-main', True, results, save, lambda: True)
    result = results['ATT_sample']
    assert result['status'] == 'verified' and result['delivery_state'] == 'complete'
    published = result['publications'][result['current_md_sha256']]
    assert [(code, published[code]['dataset_id']) for code in ['081301', '080206']] == [
        ('081301', 'ds-main'), ('080206', 'ds-ref')]
    assert len(client.files) == 1
    before = list(client.sent)
    module.publish_attachments(paper, identity, receipt, packet, body, client, root, budget,
                               'folder-main', True, results, save, lambda: True)
    assert client.sent == before
    assert snapshots[-1]['ATT_sample']['status'] == 'verified'


def test_unknown_attachment_result_is_not_resent(attachment_input):
    module = import_module('paperflow_v3.delivery.attachments')
    paper, identity, receipt, packet, body, client, root, budget = attachment_input
    client.fault = 'file_lost'
    results = {}
    with pytest.raises(TimeoutError):
        module.publish_attachments(paper, identity, receipt, packet, body, client, root, budget,
                                   'folder-main', True, results, lambda _: 'artifact', lambda: True)
    sent = list(client.sent)
    with pytest.raises(ValueError, match='unknown'):
        module.publish_attachments(paper, identity, receipt, packet, body, client, root, budget,
                                   'folder-main', True, results, lambda _: 'artifact', lambda: True,
                                   retry_failed=True)
    assert client.sent == sent


def test_failed_body_stops_all_attachment_publication(attachment_input):
    module = import_module('paperflow_v3.delivery.attachments')
    paper, identity, receipt, packet, body, client, root, budget = attachment_input
    body['080206']['body_delivery_state'] = 'failed'
    with pytest.raises(ValueError):
        module.publish_attachments(paper, identity, receipt, packet, body, client, root, budget,
                                   'folder-main', True, {}, lambda _: 'artifact', lambda: True)
    assert client.sent == []


def test_lost_server_completion_ack_leaves_attachment_unknown(attachment_input):
    module = import_module('paperflow_v3.delivery.attachments')
    paper, identity, receipt, packet, body, client, root, budget = attachment_input
    results = {}

    def save(value):
        return None if value.get('ATT_sample', {}).get('delivery_state') == 'complete' else 'artifact'

    with pytest.raises(ValueError, match='server'):
        module.publish_attachments(paper, identity, receipt, packet, body, client, root, budget,
                                   'folder-main', True, results, save, lambda: True)
    assert len(client.files) == 1
    assert results['ATT_sample']['delivery_state'] == 'in_flight'


def test_attachment_cannot_be_redirected_outside_the_recorded_formal_route(attachment_input):
    module = import_module('paperflow_v3.delivery.attachments')
    paper, identity, receipt, packet, body, client, root, budget = attachment_input
    packet['routes'][0]['image_dataset_id'] = 'SELFTEST'
    with pytest.raises(ValueError):
        module.publish_attachments(paper, identity, receipt, packet, body, client, root, budget,
                                   'folder-main', True, {}, lambda _: 'artifact', lambda: True)
    assert client.sent == []


def test_resume_after_paused_original_upload_does_not_upload_it_again(attachment_input):
    module = import_module('paperflow_v3.delivery.attachments')
    control = import_module('paperflow_v3.delivery.transport')
    paper, identity, receipt, packet, body, raw, root, budget = attachment_input
    events, save_event = event_store()
    running = {'value': True}
    original_upload = raw.upload_file

    def upload_then_pause(*args):
        result = original_upload(*args)
        running['value'] = False
        return result

    raw.upload_file = upload_then_pause
    results = {}
    client = control.TaskCheckedClient(raw, lambda: running['value'], control.ServerEffects(identity, events, save_event))
    with pytest.raises(InterruptedError):
        module.publish_attachments(paper, identity, receipt, packet, body, client, root, budget,
                                   'folder-main', True, results, lambda _: 'artifact', lambda: running['value'])
    assert results['ATT_sample']['delivery_state'] == 'paused'
    assert len(client.files) == 1
    running['value'] = True
    raw.upload_file = original_upload
    client = control.TaskCheckedClient(raw, lambda: running['value'], control.ServerEffects(identity, events, save_event))
    module.publish_attachments(paper, identity, receipt, packet, body, client, root, budget,
                               'folder-main', True, results, lambda _: 'artifact', lambda: running['value'])
    assert len(client.files) == 1
    assert results['ATT_sample']['status'] == 'verified'


def test_each_attachment_uses_its_own_audited_receipt_root(attachment_input, monkeypatch):
    module = import_module('paperflow_v3.delivery.attachments')
    paper, identity, receipt, packet, body, client, root, budget = attachment_input
    second = copy.deepcopy(receipt['attachments'][0])
    second['attachment_id'] = 'second'
    paper['attachments'].append({'file_id': 'second', 'sha256': second['source_sha256']})
    receipt['attachments'].append(second)
    roots = {'ATT_sample': root / 'first', 'second': root / 'second'}
    received = []

    def ingest(item, route, client, receipt_root, budget):
        received.append((item['attachment_id'], receipt_root))
        return {'status': 'verified'}

    monkeypatch.setattr(module, 'ingest_attachment', ingest)
    module.publish_attachments(paper, identity, receipt, packet, body, client, roots, budget,
                               'folder-main', True, {}, lambda _: 'artifact', lambda: True)
    assert received == [('ATT_sample', roots['ATT_sample']), ('second', roots['second'])]


def test_rejected_attachment_upload_retries_only_after_explicit_user_retry(attachment_input):
    from ragflow_client import RagflowHTTPError
    from paperflow_v3.delivery.transport import ServerEffects, TaskCheckedClient

    module = import_module('paperflow_v3.delivery.attachments')
    paper, identity, receipt, packet, body, raw, root, budget = attachment_input
    events, save_event = event_store()
    original = raw.upload_file
    attempts = []

    def rejected_once(*args):
        attempts.append(args[1])
        if len(attempts) == 1:
            raise RagflowHTTPError(403)
        return original(*args)

    raw.upload_file = rejected_once
    results = {}
    client = TaskCheckedClient(raw, lambda: True, ServerEffects(identity, events, save_event))
    with pytest.raises(RagflowHTTPError):
        module.publish_attachments(paper, identity, receipt, packet, body, client, root, budget,
                                   'folder-main', True, results, lambda _: 'artifact', lambda: True)
    assert results['ATT_sample']['delivery_state'] == 'failed'
    assert len(attempts) == 1 and raw.files == {}
    with pytest.raises(ValueError, match='retry'):
        module.publish_attachments(paper, identity, receipt, packet, body, client, root, budget,
                                   'folder-main', True, results, lambda _: 'artifact', lambda: True)
    client = TaskCheckedClient(raw, lambda: True, ServerEffects(identity, events, save_event, retry_failed=True))
    module.publish_attachments(paper, identity, receipt, packet, body, client, root, budget,
                               'folder-main', True, results, lambda _: 'artifact', lambda: True,
                               retry_failed=True)
    assert len(attempts) == 2 and len(raw.files) == 1
    assert results['ATT_sample']['status'] == 'verified'


def test_rejected_second_specialty_keeps_the_first_attachment_publication(attachment_input):
    from ragflow_client import RagflowHTTPError
    from paperflow_v3.delivery.transport import ServerEffects, TaskCheckedClient

    module = import_module('paperflow_v3.delivery.attachments')
    paper, identity, receipt, packet, body, raw, root, budget = attachment_input
    events, save_event = event_store()
    original = raw.upload_bytes
    failed = {'value': False}

    def reject_second(dataset, name, content):
        if dataset == 'ds-ref' and not failed['value']:
            failed['value'] = True
            raise RagflowHTTPError(403)
        return original(dataset, name, content)

    raw.upload_bytes = reject_second
    results = {}
    client = TaskCheckedClient(raw, lambda: True, ServerEffects(identity, events, save_event))
    with pytest.raises(RagflowHTTPError):
        module.publish_attachments(paper, identity, receipt, packet, body, client, root, budget,
                                   'folder-main', True, results, lambda _: 'artifact', lambda: True)
    before = set(raw.docs)
    assert results['ATT_sample']['delivery_state'] == 'failed' and len(raw.files) == 1
    client = TaskCheckedClient(raw, lambda: True, ServerEffects(identity, events, save_event, retry_failed=True))
    module.publish_attachments(paper, identity, receipt, packet, body, client, root, budget,
                               'folder-main', True, results, lambda _: 'artifact', lambda: True, retry_failed=True)
    assert len(raw.files) == 1 and len(set(raw.docs) - before) == 1
    assert results['ATT_sample']['status'] == 'verified'


def test_pause_after_native_intent_but_before_send_resumes_without_false_unknown(attachment_input):
    from paperflow_v3.delivery.transport import ServerEffects, TaskCheckedClient

    module = import_module('paperflow_v3.delivery.attachments')
    paper, identity, receipt, packet, body, raw, root, budget = attachment_input
    events, persist = event_store()
    running = {'value': True}

    def pause_after_intent(event_id, kind, data):
        result = persist(event_id, kind, data)
        if kind == 'rag_effect_intent':
            running['value'] = False
        return result

    results = {}
    client = TaskCheckedClient(raw, lambda: running['value'], ServerEffects(identity, events, pause_after_intent))
    with pytest.raises(InterruptedError):
        module.publish_attachments(paper, identity, receipt, packet, body, client, root, budget,
                                   'folder-main', True, results, lambda _: 'artifact', lambda: running['value'])
    assert raw.files == {} and results['ATT_sample']['delivery_state'] == 'paused'
    running['value'] = True
    client = TaskCheckedClient(raw, lambda: True, ServerEffects(identity, events, persist))
    module.publish_attachments(paper, identity, receipt, packet, body, client, root, budget,
                               'folder-main', True, results, lambda _: 'artifact', lambda: True)
    assert len(raw.files) == 1 and results['ATT_sample']['status'] == 'verified'
