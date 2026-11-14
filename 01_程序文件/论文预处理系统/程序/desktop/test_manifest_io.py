"""Regression coverage for Windows manifest replacement and history readers."""
import errno
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest

import engine


def windows_error(code):
    error = PermissionError(errno.EACCES, 'file is temporarily in use')
    error.winerror = code
    return error


def fake_clock(monkeypatch):
    elapsed = [0.0]
    sleeps = []

    def sleep(seconds):
        assert seconds > 0
        elapsed[0] += seconds
        sleeps.append(seconds)

    monkeypatch.setattr(engine, 'time', SimpleNamespace(
        monotonic=lambda: elapsed[0], sleep=sleep))
    return elapsed, sleeps


@pytest.mark.parametrize('code', [5, 32, 33])
def test_json_save_retries_transient_windows_replacement(tmp_path, monkeypatch, code):
    path = tmp_path / 'manifest.json'
    engine.write_json(path, {'version': 'old'})
    replace = Path.replace
    attempts = []
    _, sleeps = fake_clock(monkeypatch)

    def busy_then_replace(source, target):
        attempts.append(source)
        if len(attempts) <= 2:
            assert json.loads(path.read_text()) == {'version': 'old'}
            raise windows_error(code)
        return replace(source, target)

    monkeypatch.setattr(Path, 'replace', busy_then_replace)
    engine.write_json(path, {'version': 'new', 'title': '论文'})
    assert engine.read_json(path) == {'version': 'new', 'title': '论文'}
    assert len(attempts) == 3 and len(sleeps) == 2
    assert not list(tmp_path.glob('*.tmp'))


def test_persistent_file_lock_is_bounded_and_preserves_original(tmp_path, monkeypatch):
    path = tmp_path / 'manifest.json'
    engine.write_json(path, {'complete': True})
    original = path.read_bytes()
    error = windows_error(5)
    elapsed, sleeps = fake_clock(monkeypatch)

    def busy(*args):
        raise error

    monkeypatch.setattr(Path, 'replace', busy)
    with pytest.raises(PermissionError) as caught:
        engine.write_json(path, {'complete': False})
    assert caught.value is error
    assert 1 <= elapsed[0] <= 3 and len(sleeps) > 1
    assert path.read_bytes() == original
    assert not list(tmp_path.glob('*.tmp'))


@pytest.mark.parametrize('error', [
    OSError(errno.ENOSPC, 'disk full'),
    PermissionError(errno.EACCES, 'non-Windows permission failure'),
    windows_error(87),
])
def test_unrelated_save_errors_are_not_retried(tmp_path, monkeypatch, error):
    path = tmp_path / 'manifest.json'
    engine.write_json(path, {'version': 'old'})
    original = path.read_bytes()
    _, sleeps = fake_clock(monkeypatch)

    def fail(*args):
        raise error

    monkeypatch.setattr(Path, 'replace', fail)
    with pytest.raises(OSError) as caught:
        engine.write_json(path, {'version': 'new'})
    assert caught.value is error
    assert not sleeps
    assert path.read_bytes() == original
    assert not list(tmp_path.glob('*.tmp'))


def test_temporary_cleanup_does_not_hide_save_failure(tmp_path, monkeypatch):
    path = tmp_path / 'manifest.json'
    engine.write_json(path, {'version': 'old'})
    original = path.read_bytes()
    error = OSError(errno.ENOSPC, 'original save failure')

    def fail(*args):
        raise error

    def cleanup_denied(*args, **kwargs):
        raise windows_error(5)

    with monkeypatch.context() as patch:
        patch.setattr(Path, 'replace', fail)
        patch.setattr(Path, 'unlink', cleanup_denied)
        with pytest.raises(OSError) as caught:
            engine.write_json(path, {'version': 'new'})
    assert caught.value is error
    assert path.read_bytes() == original
    for temp in tmp_path.glob('*.tmp'):
        temp.unlink()


def test_serialization_failure_preserves_original(tmp_path):
    path = tmp_path / 'manifest.json'
    engine.write_json(path, {'version': 'old'})
    original = path.read_bytes()
    with pytest.raises(TypeError):
        engine.write_json(path, {'unsupported': object()})
    assert path.read_bytes() == original
    assert not list(tmp_path.glob('*.tmp'))


@pytest.mark.skipif(os.name != 'nt', reason='Uses real Windows file sharing semantics')
def test_save_recovers_after_real_windows_reader_releases_file(tmp_path, monkeypatch):
    path = tmp_path / 'manifest.json'
    engine.write_json(path, {'version': 'old'})
    replace = Path.replace
    denied = threading.Event()
    errors = []

    def observed_replace(source, target):
        try:
            return replace(source, target)
        except OSError as exc:
            errors.append(exc.winerror)
            denied.set()
            raise

    monkeypatch.setattr(Path, 'replace', observed_replace)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with path.open('rb') as reader:
            future = pool.submit(engine.write_json, path, {'version': 'new'})
            assert denied.wait(3), 'Windows reader did not block replacement'
            assert json.loads(reader.read()) == {'version': 'old'}
        future.result(timeout=3)
    assert errors and all(code in {5, 32, 33} for code in errors)
    assert engine.read_json(path) == {'version': 'new'}
    assert not list(tmp_path.glob('*.tmp'))


def test_history_reader_and_save_are_serialized(tmp_path, monkeypatch):
    store = engine.Store(tmp_path)
    manifest = {'run_id': 'test', 'status': 'completed', 'version': 'old'}
    store.save(manifest)
    path = store.manifest_path('test')
    read = engine.read_json
    write = engine.write_json
    reading = threading.Event()
    release_reader = threading.Event()
    writer_started = threading.Event()
    writing = threading.Event()

    def slow_read(target, default=None):
        if target != path:
            return read(target, default)
        with target.open(encoding='utf-8') as stream:
            reading.set()
            assert release_reader.wait(3)
            return json.load(stream)

    def observed_write(*args):
        writing.set()
        return write(*args)

    def save():
        writer_started.set()
        store.save({**manifest, 'version': 'new'})

    monkeypatch.setattr(engine, 'read_json', slow_read)
    monkeypatch.setattr(engine, 'write_json', observed_write)
    with ThreadPoolExecutor(max_workers=2) as pool:
        history = pool.submit(store.history)
        try:
            assert reading.wait(3)
            saved = pool.submit(save)
            assert writer_started.wait(3)
            assert not writing.wait(.15), 'save bypassed an active history reader'
        finally:
            release_reader.set()
        assert history.result(timeout=3) == [manifest]
        saved.result(timeout=3)
    assert read(path)['version'] == 'new'


def test_history_still_skips_corrupt_manifests(tmp_path):
    store = engine.Store(tmp_path)
    good = {'run_id': 'good', 'status': 'completed'}
    store.save(good)
    corrupt = store.manifest_path('corrupt')
    corrupt.parent.mkdir()
    corrupt.write_text('{broken', encoding='utf-8')
    assert store.history() == [good]
