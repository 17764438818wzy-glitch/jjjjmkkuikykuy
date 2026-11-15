from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import time

import pytest

from effect_journal import EffectConflict, EffectJournal, EffectUnknown
from folder_native_client import FolderNativeClient


class Raw:
    base_url = 'http://synthetic'
    api_key = 'synthetic'
    opener = None

    def __init__(self):
        self.calls = 0
        self.posts = 0
        self.run = '0'
        self.timeout_ingest = False

    def upload_bytes(self, *_):
        self.calls += 1
        raise TimeoutError('send result is unknown')

    def request(self, method, endpoint, body=None):
        if method == 'GET':
            return {'docs': [{'id': 'doc-1', 'pipeline_id': 'pipeline', 'status': '0',
                              'run': self.run, 'progress': 1 if self.run == '3' else 0}]}
        self.posts += 1
        if self.timeout_ingest:
            raise TimeoutError('ingest outcome unknown')
        self.run = '3'
        return {'accepted': True}


def ready():
    return {'ready': True, 'ready_valid_until': time.time() + 5}


@contextmanager
def admission(running):
    class Permit:
        def check_current(self):
            if not running:
                raise InterruptedError('paused')
        def start_transport(self, mark):
            mark()
    yield Permit()


def test_parent_pause_stops_a_new_native_write(tmp_path):
    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    raw = Raw()
    client = FolderNativeClient(raw, journal, {'test-dataset'}, 'pipeline',
                                lambda *_: admission(False), ready)
    with pytest.raises(InterruptedError):
        client.upload_bytes('test-dataset', 'body', b'data')
    assert raw.calls == 0


def test_unknown_native_write_is_not_sent_again(tmp_path):
    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    raw = Raw()
    client = FolderNativeClient(raw, journal, {'test-dataset'}, 'pipeline',
                                lambda *_: admission(True), ready)
    with pytest.raises(TimeoutError):
        client.upload_bytes('test-dataset', 'body', b'data')
    with pytest.raises(EffectUnknown):
        client.upload_bytes('test-dataset', 'body', b'data')
    assert raw.calls == 1


def test_stale_executor_observation_prevents_native_ingest(tmp_path):
    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    raw = Raw()
    def unavailable():
        raise RuntimeError('executor observation is stale')
    client = FolderNativeClient(raw, journal, {'test-dataset'}, 'pipeline',
                                lambda *_: admission(True), unavailable)
    with pytest.raises(RuntimeError, match='stale'):
        client.ingest('test-dataset', 'doc-1', 'pipeline')
    assert raw.posts == 0


def test_runtime_observation_does_not_hold_parent_admission(tmp_path):
    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    raw = Raw()
    held = []
    @contextmanager
    def parent_admission(*_):
        held.append(True)
        try:
            with admission(True) as permit:
                yield permit
        finally:
            held.pop()
    def observed():
        assert not held
        return ready()
    client = FolderNativeClient(raw, journal, {'test-dataset'}, 'pipeline', parent_admission, observed)
    assert client.ingest('test-dataset', 'doc-1', 'pipeline')['run'] == '3'
    assert raw.posts == 1


def test_temporarily_unready_native_executor_sends_once_after_recovery(tmp_path):
    from folder_native_client import NativeNotReady
    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    raw = Raw()
    observations = iter([{'ready': False}, ready()])
    client = FolderNativeClient(raw, journal, {'test-dataset'}, 'pipeline',
                                lambda *_: admission(True), lambda: next(observations))
    with pytest.raises(NativeNotReady):
        client.ingest('test-dataset', 'doc-1', 'pipeline')
    assert raw.posts == 0
    assert client.ingest('test-dataset', 'doc-1', 'pipeline')['run'] == '3'
    assert raw.posts == 1


def test_running_document_without_matching_submission_is_not_started_again(tmp_path):
    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    raw = Raw()
    raw.run = '1'
    client = FolderNativeClient(raw, journal, {'test-dataset'}, 'pipeline',
                                lambda *_: admission(True), ready)
    with pytest.raises(EffectUnknown):
        client.ingest('test-dataset', 'doc-1', 'pipeline')
    assert raw.posts == 0


def test_unknown_ingest_cannot_be_reissued_with_another_pipeline(tmp_path):
    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    raw = Raw()
    raw.timeout_ingest = True
    first = FolderNativeClient(raw, journal, {'test-dataset'}, 'pipeline',
                               lambda *_: admission(True), ready)
    with pytest.raises(TimeoutError):
        first.ingest('test-dataset', 'doc-1', 'pipeline')
    second = FolderNativeClient(raw, journal, {'test-dataset'}, 'another-pipeline',
                                lambda *_: admission(True), ready)
    with pytest.raises(EffectConflict):
        second.ingest('test-dataset', 'doc-1', 'another-pipeline')
    assert raw.posts == 1


def test_returned_ingest_is_polled_without_resubmission(tmp_path):
    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    raw = Raw()
    client = FolderNativeClient(raw, journal, {'test-dataset'}, 'pipeline',
                                lambda *_: admission(True), ready)
    assert client.ingest('test-dataset', 'doc-1', 'pipeline')['run'] == '3'
    assert client.ingest('test-dataset', 'doc-1', 'pipeline')['run'] == '3'
    assert raw.posts == 1


def test_interrupted_observation_after_ingest_is_unknown_and_not_resent(tmp_path):
    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    class InterruptedRaw(Raw):
        def request(self, method, endpoint, body=None):
            if method == 'GET' and self.posts:
                raise InterruptedError('observation interrupted')
            return super().request(method, endpoint, body)
    raw = InterruptedRaw()
    client = FolderNativeClient(raw, journal, {'test-dataset'}, 'pipeline', lambda *_: admission(True), ready)
    with pytest.raises(EffectUnknown, match='observation'):
        client.ingest('test-dataset', 'doc-1', 'pipeline')
    assert raw.posts == 1


def test_native_observation_expiring_while_journal_is_busy_does_not_send(tmp_path):
    path = tmp_path / 'effects.sqlite3'
    journal = EffectJournal(path)
    journal.initialize()
    raw = Raw()
    client = FolderNativeClient(raw, journal, {'test-dataset'}, 'pipeline',
                                lambda *_: admission(True),
                                lambda: {'ready': True, 'ready_valid_until': time.time() + .05})
    lock = sqlite3.connect(str(path))
    lock.execute('BEGIN IMMEDIATE')
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(client.ingest, 'test-dataset', 'doc-1', 'pipeline')
        time.sleep(.15)
        lock.commit()
        lock.close()
        with pytest.raises(TimeoutError, match='deadline'):
            future.result()
    assert raw.posts == 0
