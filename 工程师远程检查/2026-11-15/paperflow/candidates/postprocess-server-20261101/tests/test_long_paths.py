import os
from pathlib import Path
import socket
import stat
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from server_wine_pathfix import UnsafeWinePath, checked_resolver
from tokenizer_service import serve_client


def long_path_error():
    error = OSError('synthetic Wine failure')
    error.winerror = 234
    return error


def test_wine_fallback_preserves_verified_path_and_other_errors():
    path = 'F:\\paper\\' + 'a' * 280
    def original(_):
        raise long_path_error()
    seen = []
    wrapped = checked_resolver(original, verify=lambda name: seen.append(name) or True)
    assert wrapped(path) == path
    assert seen == [path]
    for forbidden in ('C:\\other', 'F:\\a\\..\\secret', b'F:\\other'):
        with pytest.raises(UnsafeWinePath):
            wrapped(forbidden)
    with pytest.raises(UnsafeWinePath):
        checked_resolver(original, verify=lambda _: False)(path)
    def different_error(_):
        raise FileNotFoundError('missing')
    with pytest.raises(FileNotFoundError):
        checked_resolver(different_error, verify=lambda _: True)(path)


def test_successful_wine_resolution_still_requires_native_guard():
    # Wine can treat a Linux symlink as an ordinary directory, without raising
    # WinError 234. A successful Wine result alone must not authorize it.
    path = r'F:\paper\linked\body.txt'
    seen = []
    wrapped = checked_resolver(lambda value: value, verify=lambda value: seen.append(value) or False)
    with pytest.raises(UnsafeWinePath):
        wrapped(path)
    assert seen == [path]
    escaped = checked_resolver(lambda _: r'C:\outside\secret.txt', verify=lambda _: True)
    with pytest.raises(UnsafeWinePath):
        escaped(path)
    device_alias = checked_resolver(lambda value: value, verify=lambda _: False)
    with pytest.raises(UnsafeWinePath):
        device_alias(r'\\.\F:\paper\body.txt')
    with pytest.raises(UnsafeWinePath):
        device_alias(b'\\\\.\\F:\\paper\\body.txt')


@pytest.mark.skipif(os.name != 'posix' or not hasattr(os, 'O_PATH'), reason='Linux no-follow descriptors required')
def test_native_guard_refuses_links_and_missing_names(tmp_path):
    from server_path_guard import UnsafeServerPath, verify_snapshot_path
    os.chmod(tmp_path, 0o700)
    root = tmp_path / 'drives' / 'f'
    (root / 'paper').mkdir(parents=True, mode=0o700)
    os.chmod(root, 0o700)
    (root / 'paper' / 'body.txt').write_text('synthetic', encoding='utf8')
    outside = tmp_path / 'outside'
    outside.mkdir()
    (outside / 'secret.txt').write_text('synthetic', encoding='utf8')
    (root / 'link').symlink_to(outside, target_is_directory=True)
    os.link(outside / 'secret.txt', root / 'hardlink.txt')
    assert verify_snapshot_path(tmp_path, 'F:\\') is True
    assert verify_snapshot_path(tmp_path, r'F:\paper\body.txt') is True
    assert verify_snapshot_path(tmp_path, r'F:\paper') is True
    for unsafe in (r'F:\link\secret.txt', r'F:\hardlink.txt', r'F:\paper\missing.txt',
                   r'F:\paper\..\link\secret.txt', r'C:\paper\body.txt'):
        with pytest.raises(UnsafeServerPath):
            verify_snapshot_path(tmp_path, unsafe)


@pytest.mark.skipif(os.name != 'posix' or not hasattr(os, 'O_PATH'), reason='Linux no-follow descriptors required')
def test_native_guard_detects_ancestor_replacement_during_walk(tmp_path, monkeypatch):
    from server_path_guard import UnsafeServerPath, verify_snapshot_path
    os.chmod(tmp_path, 0o700)
    root = tmp_path / 'drives' / 'f'
    (root / 'folder').mkdir(parents=True, mode=0o700)
    os.chmod(root, 0o700)
    (root / 'folder' / 'data').write_bytes(b'synthetic')
    original_open, changed = os.open, []
    def swap(name, flags, *args, **kwargs):
        fd = original_open(name, flags, *args, **kwargs)
        if name == 'folder' and not changed:
            changed.append(True)
            (root / 'folder').rename(root / 'moved')
            (root / 'folder').symlink_to(root / 'moved', target_is_directory=True)
        return fd
    monkeypatch.setattr(os, 'open', swap)
    with pytest.raises(UnsafeServerPath):
        verify_snapshot_path(tmp_path, r'F:\folder\data')
    assert changed


@pytest.mark.skipif(os.name != 'posix' or not hasattr(os, 'O_PATH'), reason='Linux no-follow descriptors required')
def test_native_guard_detects_hardlink_added_during_second_walk(tmp_path, monkeypatch):
    from server_path_guard import UnsafeServerPath, verify_snapshot_path
    os.chmod(tmp_path, 0o700)
    root = tmp_path / 'drives' / 'f'
    root.mkdir(parents=True, mode=0o700)
    target = root / 'data'; target.write_bytes(b'synthetic')
    original_fstat, inode, calls = os.fstat, target.stat().st_ino, []
    def change(fd):
        info = original_fstat(fd)
        if info.st_ino == inode:
            calls.append(True)
            if len(calls) == 2:
                os.link(target, root / 'late-hardlink')
                info = original_fstat(fd)
        return info
    monkeypatch.setattr(os, 'fstat', change)
    with pytest.raises(UnsafeServerPath):
        verify_snapshot_path(tmp_path, r'F:\data')
    assert len(calls) >= 2


def test_authenticated_path_request_is_bound_to_snapshot():
    import json
    client, server = socket.socketpair()
    class Counter:
        identity = {'test': True}
        def count_many(self, texts):
            return []
    sha = 'a' * 64
    worker = threading.Thread(target=serve_client, args=(server, Counter(), 'secret'),
                              kwargs=dict(path_verifier=lambda path: path == r'F:\paper\body',
                                          snapshot_sha256=sha))
    worker.start()
    with client:
        client.sendall(json.dumps(dict(secret='<REDACTED_SECRET>', operation='verify_snapshot_path',
                                       path=r'F:\paper\body')).encode() + b'\n')
        with client.makefile('rb') as stream:
            response = json.loads(stream.readline())
    worker.join(timeout=3)
    server.close()
    assert not worker.is_alive()
    assert response == dict(schema='paperflow-wine-verified-path/1', path=r'F:\paper\body',
                            snapshot_sha256=sha, verified=True)


def test_a_rejected_connection_does_not_stop_the_worker_from_serving_next_request():
    import json
    from server_wine_worker import _serve_connection
    class Counter:
        identity = {'test': True}
        def count_many(self, texts):
            return [{'model_tokens': 1, 'cl100k_tokens': 1} for _ in texts]
    sha = 'a' * 64
    for key, expected in [('wrong-secret', False), ('中文', False), ('secret', True)]:
        client, server = socket.socketpair()
        with client, server:
            client.sendall(json.dumps({'secret': key, 'texts': ['x']}).encode() + b'\n')
            assert _serve_connection(server, Counter(), 'secret', path_verifier=lambda _: True,
                                     snapshot_sha256=sha) is expected
            if expected:
                response = json.loads(client.recv(4096))
                assert response['counts'][0]['model_tokens'] == 1
