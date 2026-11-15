import copy
from importlib import import_module

import pytest
from local_rag import LocalRag
from test_transport_journal import event_store


class FailThird(LocalRag):
    def __init__(self):
        super().__init__()
        self.fail_third = True

    def upload_bytes(self, dataset, name, content):
        if dataset == 'formal-c' and self.fail_third:
            raise ConnectionError('confirmed fixture failure before upload')
        return super().upload_bytes(dataset, name, content)


@pytest.fixture
def publication(tmp_path):
    identity = {'task_id': 'task-1', 'group_id': 'group-1', 'paper_id': 'paper-1',
                'receipt_id': 'receipt-1', 'attempt_id': 'attempt-1',
                'parent_run_id': 'parent-1', 'task_run_id': 'child-1'}
    paper = {'paper_id': 'paper-1', 'group_id': 'group-1', 'body': {'file_id': 'body-1', 'sha256': 'a' * 64},
             'attachments': [{'file_id': 'attach-1'}]}
    receipt = {'schema': 'paperflow-postprocessing-receipt/1', 'identity': dict(identity),
               'validation_status': 'passed', 'body': {'package_path': 'publication', 'manifest_sha256': 'b' * 64},
               'attachments': [{'attachment_id': 'attach-1'}]}
    task = {'task_id': 'task-1', 'source_sha256': 'a' * 64, 'revision': 1}
    packet = {'manifest_sha256': 'b' * 64, 'root': str(tmp_path),
              'routes': [{'code': code, 'paper_dataset_id': 'formal-' + code} for code in ['a', 'b', 'c']],
              'profile': {'article': {'title': 'Synthetic title'},
                          'routing_candidates': [{'code': code} for code in ['a', 'b', 'c']]},
              'tree': {'nodes': []},
              'chunks': [{'id': 'body-1', 'content': 'Synthetic body.', 'kind': 'body'}], 'assets': []}
    prepared = {'task': task, 'validation': {'embedding': {'admission_verified': True}},
                'catalog': {'majors': copy.deepcopy(packet['routes'])}}
    return paper, identity, receipt, prepared, packet


def test_retry_one_failed_formal_target_preserves_two_successes(publication):
    module = import_module('paperflow_v3.delivery.publication')
    paper, identity, receipt, prepared, packet = publication
    client = FailThird()
    targets, saved = {}, []
    def save(rows):
        saved.append(copy.deepcopy(rows))
        return 'server-artifact'
    with pytest.raises(ConnectionError):
        module.publish_body_targets(paper, identity, receipt, prepared, packet, client, targets, save, lambda: True)
    assert targets['a']['body_delivery_state'] == targets['b']['body_delivery_state'] == 'complete'
    assert targets['c']['body_delivery_state'] == 'in_flight'
    before = set(client.docs)

    # An operator has established that the third target failed before sending.
    targets['c']['body_delivery_state'] = 'failed'
    client.fail_third = False
    module.publish_body_targets(paper, identity, receipt, prepared, packet, client, targets, save, lambda: True,
                                retry_failed=True)
    assert set(client.docs) - before == {('formal-c', 'doc-3')}
    assert all(targets[code]['body_delivery_state'] == 'complete' for code in ['a', 'b', 'c'])
    assert len(saved) > 0


def test_unknown_upload_is_not_resent(publication):
    module = import_module('paperflow_v3.delivery.publication')
    paper, identity, receipt, prepared, packet = publication
    client = LocalRag()
    client.lose_upload = True
    targets = {}
    with pytest.raises(TimeoutError):
        module.publish_body_targets(paper, identity, receipt, prepared, packet, client, targets, lambda _: 'artifact',
                                    lambda: True)
    calls = len(client.calls)
    with pytest.raises(ValueError, match='unknown'):
        module.publish_body_targets(paper, identity, receipt, prepared, packet, client, targets, lambda _: 'artifact',
                                    lambda: True, retry_failed=True)
    assert len(client.calls) == calls
    assert client.uploads == 1


def test_explicit_http_rejection_marks_only_its_target_failed_and_reuses_successes(publication):
    from ragflow_client import RagflowHTTPError
    from paperflow_v3.delivery.transport import ServerEffects, TaskCheckedClient

    module = import_module('paperflow_v3.delivery.publication')
    paper, identity, receipt, prepared, packet = publication
    events, save_event = event_store()

    class RejectedThird(LocalRag):
        def __init__(self):
            super().__init__()
            self.rejected = False

        def upload_bytes(self, dataset, name, content):
            if dataset == 'formal-c' and not self.rejected:
                self.rejected = True
                raise RagflowHTTPError(403)
            return super().upload_bytes(dataset, name, content)

    remote, targets = RejectedThird(), {}
    client = TaskCheckedClient(remote, lambda: True, ServerEffects(identity, events, save_event))
    with pytest.raises(RagflowHTTPError):
        module.publish_body_targets(paper, identity, receipt, prepared, packet, client, targets,
                                    lambda _: 'artifact', lambda: True)
    assert targets['a']['body_delivery_state'] == targets['b']['body_delivery_state'] == 'complete'
    assert targets['c']['body_delivery_state'] == 'failed'
    completed_documents = set(remote.docs)
    client = TaskCheckedClient(remote, lambda: True, ServerEffects(identity, events, save_event, retry_failed=True))
    module.publish_body_targets(paper, identity, receipt, prepared, packet, client, targets,
                                lambda _: 'artifact', lambda: True, retry_failed=True)
    assert set(remote.docs) - completed_documents == {('formal-c', 'doc-3')}
    assert targets['c']['body_delivery_state'] == 'complete'


def test_attachment_failure_blocks_body_publication(publication):
    module = import_module('paperflow_v3.delivery.publication')
    require_complete_handoff = import_module('paperflow_v3.delivery.handoff').require_complete_handoff
    paper, identity, receipt, prepared, packet = publication
    receipt['attachments'] = []
    client = LocalRag()
    with pytest.raises(ValueError):
        require_complete_handoff(paper, identity, receipt)
        module.publish_body_targets(paper, identity, receipt, prepared, packet, client, {}, lambda _: 'artifact',
                                    lambda: True)
    assert client.calls == []


def test_paused_before_target_starts_sends_nothing(publication):
    module = import_module('paperflow_v3.delivery.publication')
    paper, identity, receipt, prepared, packet = publication
    client, targets = LocalRag(), {}

    def paused():
        return False

    with pytest.raises(InterruptedError):
        module.publish_body_targets(paper, identity, receipt, prepared, packet, client, targets, lambda _: 'artifact', paused)
    assert client.calls == []
    assert targets == {}


def test_unconfirmed_server_intent_prevents_publication(publication):
    module = import_module('paperflow_v3.delivery.publication')
    paper, identity, receipt, prepared, packet = publication
    client = LocalRag()
    with pytest.raises(ValueError, match='server'):
        module.publish_body_targets(paper, identity, receipt, prepared, packet, client, {}, lambda _: None,
                                    lambda: True)
    assert client.calls == []


def test_pause_after_upload_response_stops_before_metadata_write(publication):
    module = import_module('paperflow_v3.delivery.publication')
    control = import_module('paperflow_v3.delivery.transport')
    paper, identity, receipt, prepared, packet = publication
    running = {'value': True}

    class PauseAfterUpload(LocalRag):
        def upload_bytes(self, *args):
            result = super().upload_bytes(*args)
            running['value'] = False
            return result

    raw = PauseAfterUpload()
    events, save_event = event_store()
    effects = control.ServerEffects(identity, events, save_event)
    client = control.TaskCheckedClient(raw, lambda: running['value'], effects)
    targets, saved = {}, []

    def save(rows):
        saved.append(copy.deepcopy(rows))
        return 'artifact'

    with pytest.raises(InterruptedError):
        module.publish_body_targets(paper, identity, receipt, prepared, packet, client, targets, save,
                                    lambda: running['value'])
    assert raw.uploads == 1
    assert not any(row[0] == 'PATCH' for row in raw.calls)
    assert saved[-1]['a']['body']['document_id'] == 'doc-1'


def test_lost_server_completion_ack_does_not_mark_body_target_complete(publication):
    module = import_module('paperflow_v3.delivery.publication')
    paper, identity, receipt, prepared, packet = publication
    client, targets = LocalRag(), {}

    def save(rows):
        return None if rows.get('a', {}).get('body_delivery_state') == 'complete' else 'artifact'

    with pytest.raises(ValueError, match='server'):
        module.publish_body_targets(paper, identity, receipt, prepared, packet, client, targets, save, lambda: True)
    assert client.uploads == 1
    assert targets['a']['body_delivery_state'] == 'in_flight'


def test_body_cannot_be_redirected_to_a_nonformal_dataset(publication):
    module = import_module('paperflow_v3.delivery.publication')
    paper, identity, receipt, prepared, packet = publication
    packet['routes'][0]['paper_dataset_id'] = 'SELFTEST'
    client = LocalRag()
    with pytest.raises(ValueError, match='formal'):
        module.publish_body_targets(paper, identity, receipt, prepared, packet, client, {}, lambda _: 'artifact',
                                    lambda: True)
    assert client.calls == []


def test_resume_after_paused_metadata_reuses_successful_server_effects(publication):
    module = import_module('paperflow_v3.delivery.publication')
    control = import_module('paperflow_v3.delivery.transport')
    paper, identity, receipt, prepared, packet = publication
    running, events = {'value': True}, []

    class PauseAfterPatch(LocalRag):
        def request(self, method, endpoint, body=None):
            result = super().request(method, endpoint, body)
            if method == 'PATCH' and running['value']:
                running['value'] = False
            return result

    def save_event(event_id, kind, data):
        events.insert(0, {'event_id': event_id, 'kind': kind, 'data': copy.deepcopy(data)})
        return 'artifact'

    raw, targets = PauseAfterPatch(), {}
    client = control.TaskCheckedClient(raw, lambda: running['value'], control.ServerEffects(identity, events, save_event))
    with pytest.raises(InterruptedError):
        module.publish_body_targets(paper, identity, receipt, prepared, packet, client, targets,
                                    lambda _: 'artifact', lambda: running['value'])
    assert targets['a']['body_delivery_state'] == 'paused'
    first_patch = next(row for row in raw.calls if row[0] == 'PATCH')
    running['value'] = True
    client = control.TaskCheckedClient(raw, lambda: running['value'], control.ServerEffects(identity, events, save_event))
    # Resume should be allowed; the raw test client now stays running.
    raw.request = LocalRag.request.__get__(raw, PauseAfterPatch)
    module.publish_body_targets(paper, identity, receipt, prepared, packet, client, targets,
                                lambda _: 'artifact', lambda: running['value'])
    assert raw.calls.count(first_patch) == 1
    assert targets['a']['body_delivery_state'] == 'complete'
