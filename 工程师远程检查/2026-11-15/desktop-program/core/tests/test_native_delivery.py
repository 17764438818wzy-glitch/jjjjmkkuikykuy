"""Native batching must preserve the existing ledger and uncertainty barriers."""
import copy
import json
import threading

import pytest

from test_control import manager
from test_delivery_state import delivery_case
from test_delivery_flow import IndexedRag, flow


class NativeServer(IndexedRag):
    def __init__(self):
        super().__init__()
        self.files = {}
        self.ingests = []
        self.ingest_attempts = 0
        self.lose_ingest = False
        self.accepted = threading.Event()
        self.polled = threading.Event()
        self.finish = threading.Event()
        self.finish.set()

    def upload_bytes(self, ds, name, data):
        did = super().upload_bytes(ds, name, data)
        self.files[did] = data
        self.docs[(ds, did)].update(run='UNSTART', progress=0)
        return did

    def download_bytes(self, ds, did):
        return self.files[did]

    def request(self, method, endpoint, body=None, content_type=None):
        if endpoint == '/documents/ingest':
            self.ingest_attempts += 1
            assert method == 'POST' and body['run'] == '1' and body['delete'] is False
            assert len(body['doc_ids']) == 1
            did = body['doc_ids'][0]
            ds, _ = next(k for k in self.docs if k[1] == did)
            doc = self.docs[(ds, did)]
            assert doc['status'] == '0' and doc['pipeline_id'] == 'pipeline-test'
            self.ingests.append(did)
            self.accepted.set()
            payload = json.loads(self.files[did])
            for row in payload['chunks']:
                super().request('POST', f'/datasets/{ds}/documents/{did}/chunks', {'content': row['text']})
            doc.update(run='RUNNING', progress=.5)
            if self.lose_ingest:
                raise TimeoutError('accepted native job response lost')
            return True
        if method == 'GET' and '/documents?' in endpoint and self.ingests:
            self.polled.set()
            if self.finish.is_set():
                for did in self.ingests:
                    next(d for d in self.docs.values() if d['id'] == did).update(run='DONE', progress=1)
        return super().request(method, endpoint, body, content_type)


def native_flow(case, tmp_path, server):
    case[1].native_pipeline_id = 'pipeline-test'
    return flow(case, tmp_path, server)


def test_native_batch_preserves_records_metadata_and_active_idempotence(delivery_case, tmp_path):
    server = NativeServer()
    workflow = native_flow(delivery_case, tmp_path, server)
    assert workflow.run()['state'] == 'active'
    assert len(server.ingests) == 1
    task, packet = workflow.packet(workflow.digest)
    expected = workflow.publisher.records(packet)['body']
    doc = server.docs[('paper-ds', server.ingests[0])]
    assert len(server.chunks[doc['id']]) == len(expected)
    assert {'__profile', '__tree'} <= set(workflow.receipts[workflow.code]['body']['chunks'])
    assert json.loads(doc['meta_fields']['two_pass_profile_json']) == packet['profile']
    assert json.loads(doc['meta_fields']['two_pass_tree_json']) == packet['tree']
    writes = server.writes
    assert workflow.run()['already_active']
    assert server.writes == writes and len(server.ingests) == 1


def test_lost_native_response_blocks_replay_even_when_chunks_are_present(delivery_case, tmp_path):
    server = NativeServer()
    server.lose_ingest = True
    workflow = native_flow(delivery_case, tmp_path, server)
    with pytest.raises(TimeoutError):
        workflow.run()
    writes = server.writes
    assert workflow.run()['state'] == 'blocked'
    assert len(server.ingests) == 1 and server.writes == writes
    assert workflow.ledger.active(workflow.identity['source_id'], workflow.code) is None


def test_native_acceptance_does_not_finish_action_before_terminal(delivery_case, tmp_path):
    server = NativeServer()
    server.finish.clear()
    workflow = native_flow(delivery_case, tmp_path, server)
    errors = []
    def run():
        try:
            workflow.run()
        except Exception as exc:
            errors.append(exc)
    worker = threading.Thread(target=run)
    worker.start()
    try:
        assert server.polled.wait(5)
        assert workflow.actions.ex.active_action['kind'] == 'rag_write'
        assert workflow.ledger.active(workflow.identity['source_id'], workflow.code) is None
        workflow.actions.m.pause('pause native write')
        server.finish.set()
        worker.join(10)
        assert not worker.is_alive() and errors
        assert all(d['status'] == '0' for d in server.docs.values())
        assert len(server.ingests) == 1
    finally:
        server.finish.set()
        worker.join(10)


def test_native_endpoint_cannot_borrow_foreign_document(delivery_case, tmp_path):
    server = NativeServer()
    workflow = native_flow(delivery_case, tmp_path, server)
    with pytest.raises(ValueError):
        workflow.client.ingest('paper-ds', 'foreign-doc', 'pipeline-test', timeout=1)
    assert not server.ingests


def test_saved_intent_before_first_admission_can_resume_once(delivery_case, tmp_path):
    server = NativeServer()
    workflow = native_flow(delivery_case, tmp_path, server)
    saved = workflow.publisher.on_change
    def crash_after_intent(receipts):
        saved(receipts)
        if receipts.get(workflow.code, {}).get('body', {}).get('native_batch', {}).get('state') == 'intent':
            raise RuntimeError('crash before first ingest admission')
    workflow.publisher.on_change = crash_after_intent
    with pytest.raises(RuntimeError, match='before first'):
        workflow.run()
    assert not server.ingests
    workflow.publisher.on_change = saved
    assert workflow.run()['state'] == 'active'
    assert len(server.ingests) == 1


@pytest.mark.parametrize('accepted_pipeline', [None, 'different-pipeline', 'pipeline-test'])
def test_native_visibility_acceptance_must_bind_selected_pipeline(delivery_case, tmp_path, accepted_pipeline):
    from paperflow.delivery import DeliveryWorkflow
    from paperflow.package import save
    workflow = native_flow(delivery_case, tmp_path, NativeServer())
    route = workflow.ledger.context(workflow.digest, workflow.code)['route']
    identity = dict(deployment='test', dataset_ids=[route['paper_dataset_id'], route['image_dataset_id']])
    probe = dict(passed=True, index=dict(available_int=0, vectors={'v': True}),
                 query={}, disabled_reads=[{}, {}])
    path = tmp_path/'visibility.json'
    save(path, dict(passed=True, endpoint=workflow.client.base_url, identity=identity,
                    native_pipeline_id=accepted_pipeline, probes=dict(a_body=probe, a_images=probe)))
    workflow.config.delivery_acceptance_report = str(path)
    workflow.client.deployment_identity = lambda: identity
    if accepted_pipeline == 'pipeline-test':
        DeliveryWorkflow.check_deployment(workflow)
    else:
        with pytest.raises(ValueError, match='pipeline'):
            DeliveryWorkflow.check_deployment(workflow)


def test_resume_rejects_changed_remote_pipeline_before_submit(delivery_case, tmp_path):
    from paperflow.rag import RemoteConflict
    server = NativeServer()
    workflow = native_flow(delivery_case, tmp_path, server)
    saved = workflow.publisher.on_change
    def crash_after_intent(receipts):
        saved(receipts)
        if receipts.get(workflow.code, {}).get('body', {}).get('native_batch', {}).get('state') == 'intent':
            raise RuntimeError('before first ingest admission')
    workflow.publisher.on_change = crash_after_intent
    with pytest.raises(RuntimeError, match='before first'):
        workflow.run()
    workflow.publisher.on_change = saved
    next(iter(server.docs.values()))['pipeline_id'] = 'changed-during-pause'
    with pytest.raises(RemoteConflict, match='pipeline'):
        workflow.run()
    assert server.ingest_attempts == 0
