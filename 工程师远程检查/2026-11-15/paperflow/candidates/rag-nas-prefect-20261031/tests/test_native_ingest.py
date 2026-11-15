"""Native ingest uses synthetic documents and never contacts a server."""
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import importlib.util
from pathlib import Path
import threading
import time

import pytest

from effect_journal import EffectConflict, EffectJournal, EffectUnknown
from guarded_rag_client import GuardedRagClient


path = Path(__file__).resolve().parents[1] / 'native_ingest.py'
if path.exists():
    spec = importlib.util.spec_from_file_location('native_ingest', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
else:
    module = None


def test_native_ingest_adapter_is_available():
    assert module is not None, 'new native ingest adapter is missing'


class Permit:
    def check_current(self):
        pass

    def start_transport(self, record):
        record()

    def start_observation(self):
        pass


@contextmanager
def admit(effect_id, request_sha256):
    yield Permit()


@contextmanager
def observe():
    yield Permit()


class Remote:
    base_url = 'http://fake-rag.invalid:8088'
    def __init__(self, metadata):
        self.doc = dict(id='document-1', pipeline_id='pipeline-1', status='0',
                        run='UNSTART', progress=0, meta_fields=metadata)
        self.sends = 0
        self.drop_response = False
        self.stay_unstarted = False

    def request(self, method, endpoint, body=None):
        if method == 'GET':
            return {'docs': [dict(self.doc)]}
        assert method == 'POST' and endpoint == '/documents/ingest'
        self.sends += 1
        assert body == {'doc_ids': ['document-1'], 'run': '1', 'delete': False}
        if self.drop_response:
            raise TimeoutError('response unknown')
        if not self.stay_unstarted:
            self.doc.update(run='DONE', progress=1)
        return {'accepted': True}


@pytest.fixture
def case(tmp_path):
    assert module is not None, 'new native ingest adapter is missing'
    task = {'task_id': 'task-1', 'source_sha256': 'a' * 64, 'revision': 1}
    packet = {'manifest_sha256': 'b' * 64,
              'routes': [{'code': 'S1', 'paper_dataset_id': 'dataset-1'}]}
    identity = {'paperflow_task': 'task-1', 'paperflow_source_sha256': 'a' * 64,
                'paperflow_manifest_sha256': 'b' * 64, 'paperflow_revision': '1',
                'paperflow_specialty': 'S1', 'paperflow_role': 'body'}
    remote = Remote(identity)
    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    guarded = GuardedRagClient(remote, journal, 'c' * 64, admit, observe, lambda operation, request: True)
    return task, packet, remote, guarded


def adapter(case, fresh=lambda identity: True):
    task, packet, _, guarded = case
    return module.NativeIngestClient(guarded, task, packet, 'pipeline-1', fresh)


def test_submits_verified_native_target_once_and_reads_terminal(case):
    _, _, remote, _ = case
    native = adapter(case)
    assert native.ingest('dataset-1', 'document-1', 'pipeline-1', timeout=1)['run'] == 'DONE'
    assert native.ingest('dataset-1', 'document-1', 'pipeline-1', timeout=1)['run'] == 'DONE'
    assert remote.sends == 1


def test_missing_prior_history_proof_blocks_first_submission(case):
    _, _, remote, _ = case
    with pytest.raises(ValueError):
        adapter(case, fresh=lambda identity: False).ingest('dataset-1', 'document-1', 'pipeline-1', timeout=1)
    assert remote.sends == 0


def test_lost_submission_response_is_not_sent_again(case):
    _, _, remote, _ = case
    remote.drop_response = True
    native = adapter(case)
    with pytest.raises(TimeoutError):
        native.ingest('dataset-1', 'document-1', 'pipeline-1', timeout=1)
    remote.drop_response = False
    with pytest.raises(EffectUnknown):
        native.ingest('dataset-1', 'document-1', 'pipeline-1', timeout=1)
    assert remote.sends == 1


def test_existing_remote_run_without_original_submission_evidence_is_not_adopted(case):
    _, _, remote, _ = case
    remote.doc.update(run='RUNNING', progress=0.5)
    with pytest.raises(EffectUnknown):
        adapter(case).ingest('dataset-1', 'document-1', 'pipeline-1', timeout=1)
    assert remote.sends == 0


def test_mismatched_remote_document_cannot_be_submitted(case):
    _, _, remote, _ = case
    remote.doc['meta_fields'] = {**remote.doc['meta_fields'], 'paperflow_specialty': 'wrong'}
    with pytest.raises(ValueError):
        adapter(case).ingest('dataset-1', 'document-1', 'pipeline-1', timeout=1)
    assert remote.sends == 0


def test_same_native_document_is_not_resubmitted_from_another_folder_scope(case):
    task, packet, remote, guarded = case
    remote.stay_unstarted = True
    with pytest.raises(EffectUnknown):
        adapter(case).ingest('dataset-1', 'document-1', 'pipeline-1', timeout=0.01)
    other_guard = GuardedRagClient(remote, guarded.journal, 'd' * 64, admit, observe,
                                   lambda operation, request: True)
    other = module.NativeIngestClient(other_guard, task, packet, 'pipeline-1', lambda identity: True)
    with pytest.raises(EffectUnknown):
        other.ingest('dataset-1', 'document-1', 'pipeline-1', timeout=0.01)
    assert remote.sends == 1


def test_native_ingest_cannot_be_called_as_an_unchecked_generic_post(case):
    _, _, remote, guarded = case
    body = {'doc_ids': ['document-1'], 'run': '1', 'delete': False}
    with pytest.raises(ValueError):
        guarded.request('POST', '/documents/ingest', body)
    with pytest.raises(ValueError):
        adapter(case).request('POST', '/documents/ingest', body)
    assert remote.sends == 0


def test_fresh_history_is_rechecked_after_waiting_for_send_admission(case):
    task, packet, remote, guarded = case
    is_fresh = {'value': True}
    @contextmanager
    def paused_before_admission(effect_id, request_sha256):
        is_fresh['value'] = False
        yield Permit()
    changed_guard = GuardedRagClient(remote, guarded.journal, 'c' * 64, paused_before_admission, observe,
                                     lambda operation, request: True)
    native = module.NativeIngestClient(changed_guard, task, packet, 'pipeline-1',
                                       lambda identity: is_fresh['value'])
    with pytest.raises(ValueError):
        native.ingest('dataset-1', 'document-1', 'pipeline-1', timeout=1)
    assert remote.sends == 0


def test_completed_document_cannot_be_adopted_by_a_different_sealed_package(case):
    task, packet, remote, guarded = case
    adapter(case).ingest('dataset-1', 'document-1', 'pipeline-1', timeout=1)
    newer_task = {**task, 'task_id': 'task-2'}
    newer_packet = {**packet, 'manifest_sha256': 'e' * 64}
    remote.doc['pipeline_id'] = 'pipeline-2'
    remote.doc['meta_fields'] = {**remote.doc['meta_fields'], 'paperflow_task': 'task-2',
                                 'paperflow_manifest_sha256': 'e' * 64}
    other = module.NativeIngestClient(guarded, newer_task, newer_packet, 'pipeline-2', lambda _: False)
    with pytest.raises(EffectConflict):
        other.ingest('dataset-1', 'document-1', 'pipeline-2', timeout=1)
    assert remote.sends == 1


def test_native_document_is_rechecked_after_waiting_for_admission(case):
    task, packet, remote, guarded = case
    @contextmanager
    def remote_changes_while_waiting(effect_id, request_sha256):
        remote.doc['pipeline_id'] = 'different-pipeline'
        yield Permit()
    changed_guard = GuardedRagClient(remote, guarded.journal, 'c' * 64,
                                     remote_changes_while_waiting, observe, lambda *_: True)
    native = module.NativeIngestClient(changed_guard, task, packet, 'pipeline-1', lambda _: True)
    with pytest.raises(ValueError):
        native.ingest('dataset-1', 'document-1', 'pipeline-1', timeout=1)
    assert remote.sends == 0


def test_native_preflight_and_other_candidate_document_writes_share_a_fence(case):
    task, packet, remote, guarded = case
    checking, release, patch_entered = threading.Event(), threading.Event(), threading.Event()
    original = remote.request
    def request(method, endpoint, body=None):
        if method == 'PATCH':
            patch_entered.set()
            return {'ok': True}
        return original(method, endpoint, body)
    remote.request = request
    count = []
    def fresh(_):
        count.append(1)
        if len(count) == 2:
            checking.set()
            assert release.wait(5)
        return True
    native = module.NativeIngestClient(guarded, task, packet, 'pipeline-1', fresh)
    with ThreadPoolExecutor(max_workers=2) as pool:
        ingest = pool.submit(native.ingest, 'dataset-1', 'document-1', 'pipeline-1', timeout=3)
        try:
            assert checking.wait(5)
            patch = pool.submit(guarded.request, 'PATCH', '/datasets/dataset-1/documents/document-1',
                                {'enabled': 1})
            assert not patch_entered.wait(.2)
        finally:
            release.set()
        assert ingest.result(timeout=5)['run'] == 'DONE'
        try:
            patch.result(timeout=5)
        except EffectUnknown:
            # A durable native reservation may outlive the HTTP request until
            # the same document's terminal state is verified.
            pass
    assert remote.sends == 1


def test_native_document_stays_reserved_after_terminal_until_readback_and_receipt(case):
    _, _, remote, guarded = case
    remote.stay_unstarted = True
    sent, patched = threading.Event(), threading.Event()
    original = remote.request
    def request(method, endpoint, body=None):
        if method == 'PATCH':
            patched.set()
            return {'ok': True}
        result = original(method, endpoint, body)
        if method == 'POST':
            remote.doc.update(run='RUNNING', progress=.5)
            sent.set()
        return result
    remote.request = request
    with ThreadPoolExecutor(max_workers=1) as pool:
        running = pool.submit(adapter(case).ingest, 'dataset-1', 'document-1', 'pipeline-1', timeout=3)
        assert sent.wait(5)
        with pytest.raises(EffectUnknown):
            guarded.request('PATCH', '/datasets/dataset-1/documents/document-1', {'enabled': 1})
        assert not patched.is_set()
        remote.doc.update(run='DONE', progress=1)
        assert running.result(timeout=5)['run'] == 'DONE'
    with pytest.raises(EffectUnknown):
        guarded.request('PATCH', '/datasets/dataset-1/documents/document-1', {'enabled': 1})
    assert not patched.is_set()


def test_lost_native_response_keeps_document_reserved_across_client_recreation(case):
    task, packet, remote, guarded = case
    remote.drop_response = True
    with pytest.raises(TimeoutError):
        adapter(case).ingest('dataset-1', 'document-1', 'pipeline-1', timeout=1)
    new_guard = GuardedRagClient(remote, EffectJournal(guarded.journal.path), 'd' * 64,
                                 admit, observe, lambda *_: True)
    with pytest.raises(EffectUnknown):
        new_guard.request('PATCH', '/datasets/dataset-1/documents/document-1', {'enabled': 1})
    assert remote.sends == 1


def test_native_reservation_cannot_use_synchronous_release(case):
    _, _, _, guarded = case
    adapter(case).ingest('dataset-1', 'document-1', 'pipeline-1', timeout=1)
    with guarded.journal._connect() as db:
        row = db.execute('SELECT * FROM remote_document_reservations').fetchone()
    with pytest.raises(EffectUnknown):
        guarded.journal.release_synchronous_document_if_owned(row['document_key'], row['effect_id'],
                                                              row['request_sha256'])
    with pytest.raises(EffectUnknown):
        guarded.request('PATCH', '/datasets/dataset-1/documents/document-1', {'enabled': 1})


def test_expired_admission_deadline_does_not_send_after_slow_health_check(case):
    """Waiting for health must not start a remote effect after its deadline."""
    _, _, remote, guarded = case
    def slow_but_healthy(_binding):
        time.sleep(0.03)
        return True
    with pytest.raises((TimeoutError, EffectUnknown)):
        adapter(case, fresh=slow_but_healthy).ingest('dataset-1', 'document-1', 'pipeline-1', timeout=0.01)
    assert remote.sends == 0
    with guarded.journal._connect() as db:
        assert db.execute('SELECT count(*) FROM remote_effects').fetchone()[0] == 0


def test_expired_deadline_during_final_preflight_cannot_create_remote_intent(case):
    _, _, remote, guarded = case
    calls = []
    def healthy_but_slow_in_final_check(_binding):
        calls.append(1)
        if len(calls) == 2:
            time.sleep(0.03)
        return True
    with pytest.raises((TimeoutError, EffectUnknown)):
        adapter(case, fresh=healthy_but_slow_in_final_check).ingest(
            'dataset-1', 'document-1', 'pipeline-1', timeout=0.01)
    assert len(calls) == 2
    assert remote.sends == 0
    with guarded.journal._connect() as db:
        assert db.execute('SELECT count(*) FROM remote_effects').fetchone()[0] == 0


def test_expired_deadline_inside_task_admission_does_not_record_or_send(case):
    task, packet, remote, guarded = case
    class SlowPermit(Permit):
        def start_transport(self, record):
            time.sleep(0.03)
            record()
    @contextmanager
    def slow_admit(*_):
        yield SlowPermit()
    slow_guard = GuardedRagClient(remote, guarded.journal, 'c' * 64, slow_admit, observe,
                                  lambda *_: True)
    native = module.NativeIngestClient(slow_guard, task, packet, 'pipeline-1', lambda _: True)
    with pytest.raises((TimeoutError, EffectUnknown)):
        native.ingest('dataset-1', 'document-1', 'pipeline-1', timeout=0.01)
    assert remote.sends == 0
    with guarded.journal._connect() as db:
        assert db.execute('SELECT count(*) FROM remote_effects').fetchone()[0] == 0


def test_health_wait_occurs_before_document_fence_and_then_sends_once(case):
    task, packet, remote, guarded = case
    entered, continue_wait = threading.Event(), threading.Event()
    def wait_for_health(_binding, _deadline):
        entered.set()
        assert continue_wait.wait(5)
        return True
    native = module.NativeIngestClient(guarded, task, packet, 'pipeline-1', lambda _: True,
                                       wait_before_submission=wait_for_health)
    with ThreadPoolExecutor(max_workers=1) as pool:
        running = pool.submit(native.ingest, 'dataset-1', 'document-1', 'pipeline-1', timeout=3)
        try:
            assert entered.wait(2)
            _key, lock = guarded._document_fence('document-1')
            with lock.acquire(timeout=0.5):
                pass
        finally:
            continue_wait.set()
        assert running.result(timeout=5)['run'] == 'DONE'
    assert remote.sends == 1


def test_expired_health_wait_cannot_create_native_intent(case):
    task, packet, remote, guarded = case
    def wait_past_deadline(_binding, _deadline):
        time.sleep(0.03)
        return True
    native = module.NativeIngestClient(guarded, task, packet, 'pipeline-1', lambda _: True,
                                       wait_before_submission=wait_past_deadline)
    with pytest.raises((TimeoutError, EffectUnknown)):
        native.ingest('dataset-1', 'document-1', 'pipeline-1', timeout=0.01)
    assert remote.sends == 0
    with guarded.journal._connect() as db:
        assert db.execute('SELECT count(*) FROM remote_effects').fetchone()[0] == 0


def test_failed_health_wait_does_not_fall_through_to_send(case):
    task, packet, remote, guarded = case
    native = module.NativeIngestClient(guarded, task, packet, 'pipeline-1', lambda _: True,
                                       wait_before_submission=lambda *_: False)
    with pytest.raises(ValueError):
        native.ingest('dataset-1', 'document-1', 'pipeline-1', timeout=1)
    assert remote.sends == 0


def test_deadline_after_committed_intent_preserves_unknown_and_never_resends(case):
    task, packet, remote, guarded = case
    class SlowAfterIntent(Permit):
        def start_transport(self, record):
            record()
            time.sleep(0.03)
    @contextmanager
    def admit_then_wait(*_):
        yield SlowAfterIntent()
    slow_guard = GuardedRagClient(remote, guarded.journal, 'c' * 64, admit_then_wait, observe,
                                  lambda *_: True)
    native = module.NativeIngestClient(slow_guard, task, packet, 'pipeline-1', lambda _: True)
    with pytest.raises(TimeoutError):
        native.ingest('dataset-1', 'document-1', 'pipeline-1', timeout=0.01)
    assert remote.sends == 0
    with guarded.journal._connect() as db:
        assert db.execute('SELECT state FROM remote_effects').fetchone()['state'] == 'intent'
    with pytest.raises(EffectUnknown):
        adapter(case).ingest('dataset-1', 'document-1', 'pipeline-1', timeout=1)
    assert remote.sends == 0


@pytest.mark.parametrize('after_intent', [False, True])
def test_expired_final_runtime_proof_never_sends(case, after_intent):
    task, packet, remote, guarded = case
    def fresh(binding):
        return module.FreshSubmissionProof.for_binding(binding, time.monotonic() + .01)
    class SlowPermit(Permit):
        def start_transport(self, record):
            if after_intent:
                record()
            time.sleep(.03)
            if not after_intent:
                record()
    @contextmanager
    def slow_admit(*_):
        yield SlowPermit()
    guard = GuardedRagClient(remote, guarded.journal, 'c' * 64, slow_admit, observe, lambda *_: True)
    native = module.NativeIngestClient(guard, task, packet, 'pipeline-1', fresh,
                                       require_bound_proof=True)
    with pytest.raises(TimeoutError):
        native.ingest('dataset-1', 'document-1', 'pipeline-1', timeout=1)
    assert remote.sends == 0
    with guarded.journal._connect() as db:
        rows = db.execute('SELECT state FROM remote_effects').fetchall()
    assert [row['state'] for row in rows] == (['intent'] if after_intent else [])


def test_bound_proof_must_match_document_and_cannot_be_plain_true(case):
    task, packet, remote, guarded = case
    bad_binding = {'dataset_id': 'another-dataset', 'document_id': 'another-document'}
    for proof in (True, module.FreshSubmissionProof.for_binding(bad_binding, time.monotonic() + 5)):
        native = module.NativeIngestClient(guarded, task, packet, 'pipeline-1', lambda _: proof,
                                           require_bound_proof=True)
        with pytest.raises(ValueError):
            native.ingest('dataset-1', 'document-1', 'pipeline-1', timeout=1)
    assert remote.sends == 0


def test_concurrent_documents_cannot_replace_each_others_proof_deadline(case):
    from urllib.parse import parse_qs, urlsplit
    task, packet, remote, guarded = case
    a_waiting = threading.Event()
    b_finished = threading.Event()
    class MultiRemote(Remote):
        def __init__(self, metadata):
            super().__init__(metadata)
            self.docs = {name: dict(self.doc, id=name) for name in ('doc-a', 'doc-b')}
            self.sent_ids = []
        def request(self, method, endpoint, body=None):
            if method == 'GET':
                did = parse_qs(urlsplit(endpoint).query)['id'][0]
                return {'docs': [dict(self.docs[did])]}
            did = body['doc_ids'][0]
            self.sent_ids.append(did)
            self.docs[did].update(run='DONE', progress=1)
            return {'accepted': True}
    multi = MultiRemote(remote.doc['meta_fields'])
    class APermit(Permit):
        def start_transport(self, record):
            a_waiting.set()
            assert b_finished.wait(3)
            time.sleep(.04)
            record()
    @contextmanager
    def admit_a(*_):
        yield APermit()
    guard_a = GuardedRagClient(multi, guarded.journal, 'c' * 64, admit_a, observe, lambda *_: True)
    guard_b = GuardedRagClient(multi, guarded.journal, 'c' * 64, admit, observe, lambda *_: True)
    def fresh(binding):
        lifetime = .02 if binding['document_id'] == 'doc-a' else 5
        return module.FreshSubmissionProof.for_binding(binding, time.monotonic() + lifetime)
    a = module.NativeIngestClient(guard_a, task, packet, 'pipeline-1', fresh, require_bound_proof=True)
    b = module.NativeIngestClient(guard_b, task, packet, 'pipeline-1', fresh, require_bound_proof=True)
    with ThreadPoolExecutor(max_workers=2) as pool:
        result_a = pool.submit(a.ingest, 'dataset-1', 'doc-a', 'pipeline-1', timeout=3)
        try:
            assert a_waiting.wait(2)
            assert b.ingest('dataset-1', 'doc-b', 'pipeline-1', timeout=2)['run'] == 'DONE'
        finally:
            b_finished.set()
        with pytest.raises(TimeoutError):
            result_a.result(timeout=3)
    assert multi.sent_ids == ['doc-b']


def test_proof_expiring_while_waiting_for_sqlite_writer_never_sends(case):
    task, packet, remote, guarded = case
    writer_locked, release_writer, final_proof_sampled = threading.Event(), threading.Event(), threading.Event()
    def hold_writer():
        with guarded.journal._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            writer_locked.set()
            assert release_writer.wait(3)
            db.rollback()
    proof_calls = []
    def fresh(binding):
        proof_calls.append(1)
        if len(proof_calls) == 2:
            final_proof_sampled.set()
        return module.FreshSubmissionProof.for_binding(binding, time.monotonic() + .05)
    native = module.NativeIngestClient(guarded, task, packet, 'pipeline-1', fresh, require_bound_proof=True)
    with ThreadPoolExecutor(max_workers=2) as pool:
        writer = pool.submit(hold_writer)
        assert writer_locked.wait(2)
        submission = pool.submit(native.ingest, 'dataset-1', 'document-1', 'pipeline-1', timeout=2)
        try:
            assert final_proof_sampled.wait(2)
            time.sleep(.1)
        finally:
            release_writer.set()
        writer.result(timeout=3)
        with pytest.raises(TimeoutError):
            submission.result(timeout=3)
    assert remote.sends == 0
    with guarded.journal._connect() as db:
        assert db.execute('SELECT count(*) FROM remote_effects').fetchone()[0] == 0
