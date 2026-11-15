"""RAG transport wrapper exercises real local journals and a fake remote."""
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import importlib.util
from pathlib import Path
import subprocess
import sys
import threading
import time

import pytest

from effect_journal import EffectConflict, EffectJournal, EffectNeedsReadback, EffectUnknown


path = Path(__file__).resolve().parents[1] / 'guarded_rag_client.py'
if path.exists():
    spec = importlib.util.spec_from_file_location('guarded_rag_client', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
else:
    module = None


def test_guarded_rag_transport_is_available():
    assert module is not None, 'new guarded RAG client is missing'


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


def authorized(operation, request):
    return True


def fake_verified(operation, request, result):
    # Only the isolated fake supplies this; no real readback is implied.
    return True


class Remote:
    base_url = 'http://fake-rag.invalid:8088'
    def __init__(self):
        self.calls = []
        self.fail = False

    def request(self, method, endpoint, body=None, *args, **kwargs):
        self.calls.append((method, endpoint, body))
        if self.fail:
            raise TimeoutError('lost remote response')
        if method == 'POST' and endpoint.endswith('/chunks'):
            return {'chunk': {'id': 'chunk-fixture'}}
        return {'ok': True}

    def upload_bytes(self, dataset, name, content):
        self.calls.append(('upload_bytes', dataset, name, content))
        if self.fail:
            raise TimeoutError('lost remote response')
        return 'document-1'

    def upload_file(self, parent, name, content):
        self.calls.append(('upload_file', parent, name, content))
        return 'file-1'

    def parse(self, dataset, document):
        return self.request('POST', f'/datasets/{dataset}/documents/parse', {'id': document})


@pytest.fixture
def client(tmp_path):
    assert module is not None, 'new guarded RAG client is missing'
    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    remote = Remote()
    return module.GuardedRagClient(remote, journal, 'b' * 64, admit, observe, authorized, fake_verified), remote


def test_repeated_patch_requires_readback_across_client_recreation(client):
    guarded, remote = client
    assert guarded.request('PATCH', '/datasets/one/documents/two', {'enabled': 1}) == {'ok': True}
    again = module.GuardedRagClient(remote, guarded.journal, 'b' * 64, admit, observe, authorized, fake_verified)
    with pytest.raises(RuntimeError, match='readback'):
        again.request('PATCH', '/datasets/one/documents/two', {'enabled': 1})
    assert len(remote.calls) == 1


def test_timeout_on_upload_never_causes_automatic_second_upload(client):
    guarded, remote = client
    remote.fail = True
    with pytest.raises(TimeoutError):
        guarded.upload_bytes('dataset', 'paper.md', b'payload')
    remote.fail = False
    with pytest.raises(EffectUnknown):
        guarded.upload_bytes('dataset', 'paper.md', b'payload')
    assert len(remote.calls) == 1


def test_read_only_get_and_retrieval_are_not_cached_as_remote_mutations(client):
    guarded, remote = client
    for _ in range(2):
        guarded.request('GET', '/datasets')
        guarded.request('POST', '/retrieval', {'question': 'paper'})
    assert len(remote.calls) == 4
    with guarded.journal._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM remote_effects').fetchone()[0] == 0


def test_changed_bytes_for_same_remote_file_are_rejected(client):
    guarded, remote = client
    guarded.upload_file('parent', 'file.bin', b'one')
    with pytest.raises(EffectConflict):
        guarded.upload_file('parent', 'file.bin', b'two')
    assert len(remote.calls) == 1


def test_changed_content_for_same_chunk_marker_is_rejected(client):
    guarded, remote = client
    endpoint = '/datasets/ds/documents/doc/chunks'
    guarded.request('POST', endpoint, {'content': '[PAPERFLOW]{"record":"one"}[/PAPERFLOW]\nfirst'})
    with pytest.raises(EffectConflict):
        guarded.request('POST', endpoint, {'content': '[PAPERFLOW]{"record":"one"}[/PAPERFLOW]\nchanged'})
    guarded.request('POST', endpoint, {'content': '[PAPERFLOW]{"record":"two"}[/PAPERFLOW]\nsecond'})
    assert len(remote.calls) == 2


def test_legacy_write_methods_cannot_bypass_guard(client):
    guarded, _ = client
    with pytest.raises(AttributeError):
        getattr(guarded, 'upload')
    with pytest.raises(AttributeError):
        getattr(guarded, 'ingest')
    with pytest.raises(AttributeError):
        getattr(guarded, 'parse')
    with pytest.raises(AttributeError):
        getattr(guarded, 'opener')


def test_enable_disable_enable_never_returns_old_enable_as_new_success(client):
    guarded, remote = client
    endpoint = '/datasets/one/documents/two'
    guarded.request('PATCH', endpoint, {'enabled': 1})
    guarded.request('PATCH', endpoint, {'enabled': 0})
    with pytest.raises(RuntimeError, match='readback'):
        guarded.request('PATCH', endpoint, {'enabled': 1})
    assert [item[2]['enabled'] for item in remote.calls] == [1, 0]


def test_shared_folder_creation_is_deduplicated_across_different_folder_scopes(client):
    guarded, remote = client
    other = module.GuardedRagClient(remote, guarded.journal, 'd' * 64, admit, observe, authorized, fake_verified)
    payload = {'parent_id': 'root', 'name': 'specialty', 'type': 'folder'}
    guarded.request('POST', '/files', payload)
    other.request('POST', '/files', payload)
    assert len(remote.calls) == 1


def test_paused_task_cannot_start_new_remote_observation(client):
    guarded, remote = client
    @contextmanager
    def stopped():
        raise InterruptedError('paused')
        yield
    blocked = module.GuardedRagClient(remote, guarded.journal, 'b' * 64, admit, stopped, authorized)
    with pytest.raises(InterruptedError):
        blocked.request('GET', '/datasets')
    with pytest.raises(InterruptedError):
        blocked.request('POST', '/retrieval', {'question': 'paper'})
    assert remote.calls == []


def test_mutating_callers_payload_while_waiting_does_not_change_remote_request(client):
    guarded, remote = client
    payload = {'enabled': 1}
    @contextmanager
    def delayed(effect_id, request_sha256):
        payload['enabled'] = 0
        yield Permit()
    guarded = module.GuardedRagClient(remote, guarded.journal, 'b' * 64, delayed, observe, authorized, fake_verified)
    guarded.request('PATCH', '/datasets/one/documents/two', payload)
    assert remote.calls[0][2] == {'enabled': 1}


@pytest.mark.parametrize('endpoint,body', [('/datasets/one', {'name': 'changed'}),
                                            ('/datasets/one/documents/two', {'unrelated': 'changed'})])
def test_unrelated_patch_is_rejected_before_any_remote_call(client, endpoint, body):
    guarded, remote = client
    with pytest.raises(ValueError):
        guarded.request('PATCH', endpoint, body)
    assert remote.calls == []


def test_authorizer_must_approve_the_specific_remote_mutation(client):
    guarded, remote = client
    denied = module.GuardedRagClient(remote, guarded.journal, 'b' * 64, admit, observe,
                                     lambda operation, request: False)
    with pytest.raises(PermissionError):
        denied.request('PATCH', '/datasets/one/documents/two', {'enabled': 1})
    assert remote.calls == []


@pytest.mark.parametrize('endpoint', ['/files?', '/files?x=1&x=2', '/files#fragment',
                                      '/%66iles', '/datasets/one/../one/documents/two',
                                      '/datasets/one/documents/%74wo'])
def test_noncanonical_write_path_is_rejected(client, endpoint):
    guarded, remote = client
    payload = ({'parent_id': 'root', 'name': 'specialty', 'type': 'folder'}
               if 'files' in endpoint or '%66iles' in endpoint else {'enabled': 1})
    method = 'POST' if 'files' in endpoint or '%66iles' in endpoint else 'PATCH'
    with pytest.raises(ValueError):
        guarded.request(method, endpoint, payload)
    assert remote.calls == []


def test_attachment_link_accepts_only_the_existing_exact_add_mode(client):
    guarded, remote = client
    payload = {'file_ids': ['file-1'], 'kb_ids': ['dataset-1']}
    assert guarded.request('POST', '/files/link-to-datasets?mode=add', payload) == {'ok': True}
    for endpoint in ('/files/link-to-datasets?mode=add&mode=add',
                     '/files/link-to-datasets?mode=remove', '/files/link-to-datasets?mode=%61dd'):
        with pytest.raises(ValueError):
            guarded.request('POST', endpoint, payload)
    with pytest.raises(ValueError):
        guarded.request('POST', '/files/link-to-datasets?mode=add',
                        {'file_ids': ['file-1', 'file-2'], 'kb_ids': ['dataset-1']})
    assert len(remote.calls) == 1


def test_same_remote_objects_are_deduplicated_across_folder_scopes(client):
    guarded, remote = client
    other = module.GuardedRagClient(remote, guarded.journal, 'd' * 64, admit, observe, authorized, fake_verified)
    endpoint = '/datasets/ds/documents/doc/chunks'
    chunk = {'content': '[PAPERFLOW]{"record":"one"}[/PAPERFLOW]\nfirst'}
    for candidate in (guarded, other):
        candidate.request('POST', endpoint, chunk)
        candidate.upload_bytes('ds', 'paper.md', b'body')
        candidate.upload_file('folder', 'attachment.bin', b'attachment')
        candidate.request('POST', '/files/link-to-datasets?mode=add',
                          {'file_ids': ['file-1'], 'kb_ids': ['ds']})
    assert len(remote.calls) == 4


def test_opposing_updates_to_one_document_do_not_overlap(client):
    guarded, remote = client
    first_entered, release, second_entered = threading.Event(), threading.Event(), threading.Event()
    original = remote.request
    def blocking(method, endpoint, body=None, *args, **kwargs):
        if body['enabled'] == 1:
            first_entered.set()
            assert release.wait(5)
        else:
            second_entered.set()
        return original(method, endpoint, body, *args, **kwargs)
    remote.request = blocking
    endpoint = '/datasets/ds/documents/doc'
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(guarded.request, 'PATCH', endpoint, {'enabled': 1})
        try:
            assert first_entered.wait(5)
            second = pool.submit(guarded.request, 'PATCH', endpoint, {'enabled': 0})
            assert not second_entered.wait(.2)
        finally:
            release.set()
        assert first.result(timeout=5) == {'ok': True}
        assert second.result(timeout=5) == {'ok': True}


def test_document_fence_coordinates_separate_python_processes(client, tmp_path):
    guarded, _ = client
    worker = r'''
import sys, time
from contextlib import contextmanager
from pathlib import Path
from effect_journal import EffectJournal
from guarded_rag_client import GuardedRagClient
db, root, number = sys.argv[1:]
root = Path(root)
class Permit:
    def check_current(self): pass
    def start_transport(self, record): record()
    def start_observation(self): pass
@contextmanager
def permit(*args): yield Permit()
class Remote:
    base_url = 'http://fake-rag.invalid:8088'
    def request(self, method, endpoint, body):
        with (root / 'writes.txt').open('a', encoding='utf8') as out: out.write('start ' + number + '\n')
        time.sleep(.3)
        with (root / 'writes.txt').open('a', encoding='utf8') as out: out.write('end ' + number + '\n')
        return {'ok': True}
client = GuardedRagClient(Remote(), EffectJournal(db), 'a' * 64, permit, permit, lambda *_: True,
                          lambda *_: True)
(root / ('fence-ready' + number)).touch()
while not (root / 'fence-go').exists(): time.sleep(.01)
client.request('PATCH', '/datasets/ds/documents/doc', {'enabled': int(number)})
'''
    processes = [subprocess.Popen([sys.executable, '-c', worker, str(guarded.journal.path), str(tmp_path), str(index)],
                                  cwd=str(path.parent), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  text=True) for index in range(2)]
    try:
        deadline = time.monotonic() + 10
        while not all((tmp_path / ('fence-ready' + str(index))).exists() for index in range(2)):
            assert time.monotonic() < deadline
            time.sleep(.01)
        (tmp_path / 'fence-go').touch()
        for process in processes:
            out, err = process.communicate(timeout=10)
            assert process.returncode == 0, (out, err)
        events = (tmp_path / 'writes.txt').read_text(encoding='utf8').splitlines()
        assert len(events) == 4
        assert events[0].startswith('start ') and events[1] == events[0].replace('start ', 'end ')
        assert events[2].startswith('start ') and events[3] == events[2].replace('start ', 'end ')
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)


def test_unknown_document_write_blocks_other_mutations_after_lock_is_released(client):
    guarded, remote = client
    remote.fail = True
    with pytest.raises(TimeoutError):
        guarded.request('PATCH', '/datasets/ds/documents/doc', {'enabled': 0})
    remote.fail = False
    with pytest.raises(EffectUnknown):
        guarded.request('PATCH', '/datasets/ds/documents/doc', {'enabled': 1})
    assert len(remote.calls) == 1


def test_document_write_without_verifier_remains_held_after_response(client):
    guarded, remote = client
    unverified = module.GuardedRagClient(remote, guarded.journal, 'b' * 64, admit, observe, authorized)
    with pytest.raises(EffectNeedsReadback):
        unverified.request('PATCH', '/datasets/ds/documents/doc', {'enabled': 0})
    with pytest.raises(EffectUnknown):
        unverified.request('PATCH', '/datasets/ds/documents/doc', {'enabled': 1})
    assert len(remote.calls) == 1


def test_chunk_response_without_id_cannot_be_marked_verified(client):
    guarded, remote = client
    calls = []
    def missing_id(method, endpoint, body=None):
        calls.append((method, endpoint))
        return {}
    remote.request = missing_id
    with pytest.raises(EffectNeedsReadback):
        guarded.request('POST', '/datasets/ds/documents/doc/chunks',
                        {'content': '[PAPERFLOW]{"record":"one"}[/PAPERFLOW]\nfirst'})
    with pytest.raises(EffectUnknown):
        guarded.request('PATCH', '/datasets/ds/documents/doc', {'enabled': 1})
    assert len(calls) == 1
