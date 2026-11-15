"""Snapshot transport uses only synthetic files and never starts processing."""
import hashlib
import io
import json
import os
from pathlib import Path
import struct
from uuid import uuid4, uuid5

import pytest
from unittest.mock import patch

from remote_jobs import NAMESPACE
from snapshot_transport import TransferError, write_snapshot, stage_over_ssh
from snapshot_wire import snapshot_sha


def manifest_for(root):
    identity = dict(task_id=str(uuid4()), group_id='a' * 32, paper_id='synthetic',
                    receipt_id='b' * 64, attempt_id='test', parent_run_id=str(uuid4()),
                    task_run_id=str(uuid4()))
    rows = []
    for name, value in [('first.json', b'{}'), ('second.txt', b'reply')]:
        path = root / name
        path.write_bytes(value)
        rows.append(dict(path=str(path), bytes=len(value), sha256=hashlib.sha256(value).hexdigest()))
    return dict(schema='paperflow-remote-snapshot/1', job_id=str(uuid5(NAMESPACE, identity['task_run_id'])),
                claim_id=str(uuid4()), identity=identity, generation=1, manifest_sha256='c' * 64,
                code_sha256='d' * 64, inputs=dict(identity=identity, manifest_sha256='c' * 64,
                body=dict(identity=identity), attachments=[]), files=rows)


def test_framing_preserves_manifest_and_file_bytes(tmp_path):
    manifest = manifest_for(tmp_path)
    output = io.BytesIO()
    record = write_snapshot(output, manifest, allowed_roots=[tmp_path])
    raw = output.getvalue()
    size = struct.unpack('!Q', raw[:8])[0]
    header = json.loads(raw[8:8 + size])
    assert header == dict(manifest=manifest, snapshot_sha256=snapshot_sha(manifest))
    assert raw[8 + size:] == b'{}reply'
    assert record['snapshot_sha256'] == snapshot_sha(manifest)


def test_missing_or_tampered_file_fails_and_never_counts_as_sent(tmp_path):
    manifest = manifest_for(tmp_path)
    Path(manifest['files'][1]['path']).write_bytes(b'other')
    with pytest.raises(TransferError):
        write_snapshot(io.BytesIO(), manifest, allowed_roots=[tmp_path])


def test_sender_can_defer_content_hashing_to_receiving_server(tmp_path):
    manifest = manifest_for(tmp_path)
    manifest['binary_hash_scope'] = 'server'
    Path(manifest['files'][1]['path']).write_bytes(b'other')
    output = io.BytesIO()
    write_snapshot(output, manifest, allowed_roots=[tmp_path], verify_locally=False)
    raw = output.getvalue()
    size = struct.unpack('!Q', raw[:8])[0]
    assert raw[8 + size:] == b'{}other'  # Server must reject its mismatched digest.


def test_outside_allowed_root_and_links_fail(tmp_path):
    inside = tmp_path / 'inside'
    inside.mkdir()
    manifest = manifest_for(tmp_path)
    with pytest.raises(TransferError):
        write_snapshot(io.BytesIO(), manifest, allowed_roots=[inside])
    if hasattr(os, 'link'):
        os.link(manifest['files'][0]['path'], tmp_path / 'another-name')
        with pytest.raises(TransferError):
            write_snapshot(io.BytesIO(), manifest, allowed_roots=[tmp_path])


def test_header_cannot_be_enormous_or_malformed(tmp_path):
    manifest = manifest_for(tmp_path)
    manifest['inputs']['body']['filler'] = 'x' * (1024 * 1024 + 1)
    with pytest.raises(TransferError):
        write_snapshot(io.BytesIO(), manifest, allowed_roots=[tmp_path], max_header_bytes=1024 * 1024)
    manifest['generation'] = 0
    with pytest.raises(TransferError):
        write_snapshot(io.BytesIO(), manifest, allowed_roots=[tmp_path])


def test_opened_file_must_still_be_the_allowed_path_before_reading(tmp_path):
    manifest = manifest_for(tmp_path)
    output = io.BytesIO()
    with patch('source_handle._opened_path', return_value=str(tmp_path.parent / 'outside-secret')):
        with pytest.raises(TransferError):
            write_snapshot(output, manifest, allowed_roots=[tmp_path])
    raw = output.getvalue()
    size = struct.unpack('!Q', raw[:8])[0]
    assert raw[8 + size:] == b''


def test_exact_independently_trusted_file_can_be_sent_without_authorizing_its_siblings(tmp_path):
    inside = tmp_path / 'inside'; inside.mkdir()
    outside = tmp_path / 'outside'; outside.mkdir()
    manifest = manifest_for(inside)
    catalog = outside / 'catalog.json'; catalog.write_bytes(b'catalog')
    ref = dict(path=str(catalog), bytes=7, sha256=hashlib.sha256(b'catalog').hexdigest())
    manifest['files'].append(ref)
    trusted = [dict(path=str(catalog), sha256=ref['sha256'])]
    with pytest.raises(TransferError):
        write_snapshot(io.BytesIO(), manifest, allowed_roots=[inside])
    result = write_snapshot(io.BytesIO(), manifest, allowed_roots=[inside], trusted_files=trusted)
    assert result['files'] == 3
    other = outside / 'other.json'; other.write_bytes(b'other')
    manifest['files'].append(dict(path=str(other), bytes=5, sha256=hashlib.sha256(b'other').hexdigest()))
    with pytest.raises(TransferError):
        write_snapshot(io.BytesIO(), manifest, allowed_roots=[inside], trusted_files=trusted)


def test_exact_file_digest_is_required_even_with_server_only_binary_hashing(tmp_path):
    inside = tmp_path / 'inside'; inside.mkdir()
    outside = tmp_path / 'outside'; outside.mkdir()
    manifest = manifest_for(inside)
    manifest['binary_hash_scope'] = 'server'
    catalog = outside / 'catalog.json'; catalog.write_bytes(b'original')
    digest = hashlib.sha256(b'original').hexdigest()
    ref = dict(path=str(catalog), bytes=8, sha256=digest)
    manifest['files'].append(ref)
    trusted = [dict(path=str(catalog), sha256=digest)]
    write_snapshot(io.BytesIO(), manifest, allowed_roots=[inside], trusted_files=trusted, verify_locally=False)
    catalog.write_bytes(b'tampered')  # same length, so size is not sufficient
    with pytest.raises(TransferError):
        write_snapshot(io.BytesIO(), manifest, allowed_roots=[inside], trusted_files=trusted, verify_locally=False)


def test_exact_file_must_match_the_frozen_row_and_the_opened_handle(tmp_path):
    inside = tmp_path / 'inside'; inside.mkdir()
    outside = tmp_path / 'outside'; outside.mkdir()
    manifest = manifest_for(inside)
    catalog = outside / 'catalog.json'; catalog.write_bytes(b'catalog')
    digest = hashlib.sha256(b'catalog').hexdigest()
    manifest['files'].append(dict(path=str(catalog), bytes=7, sha256=digest))
    trusted = [dict(path=str(catalog), sha256='0' * 64)]
    with pytest.raises(TransferError):
        write_snapshot(io.BytesIO(), manifest, allowed_roots=[inside], trusted_files=trusted)
    trusted[0]['sha256'] = digest
    from source_handle import _opened_path
    def redirected(fd):
        actual = _opened_path(fd)
        return str(outside / 'other.json') if Path(actual) == catalog else actual
    with patch('source_handle._opened_path', side_effect=redirected):
        with pytest.raises(TransferError):
            write_snapshot(io.BytesIO(), manifest, allowed_roots=[inside], trusted_files=trusted)


def test_exact_file_never_grants_a_linked_alias_or_hardlink(tmp_path):
    inside = tmp_path / 'inside'; inside.mkdir()
    outside = tmp_path / 'outside'; outside.mkdir()
    manifest = manifest_for(inside)
    catalog = outside / 'catalog.json'; catalog.write_bytes(b'catalog')
    digest = hashlib.sha256(b'catalog').hexdigest()
    manifest['files'].append(dict(path=str(catalog), bytes=7, sha256=digest))
    trusted = [dict(path=str(catalog), sha256=digest)]
    os.link(catalog, outside / 'hardlink.json')
    with pytest.raises(TransferError):
        write_snapshot(io.BytesIO(), manifest, allowed_roots=[inside], trusted_files=trusted)


def test_lost_server_ack_is_unknown_and_never_automatically_resends(tmp_path):
    manifest = manifest_for(tmp_path)
    class Channel:
        def __init__(self): self.attempts, self.closed = 0, False
        def settimeout(self, timeout): pass
        def exec_command(self, command): self.attempts += 1
        def send(self, data): return len(data)
        def shutdown_write(self): pass
        def exit_status_ready(self): return True
        def recv_ready(self): return False
        def recv_stderr_ready(self): return False
        def recv_exit_status(self): return 1
        def close(self): self.closed = True
    class Transport:
        def __init__(self): self.channel, self.opens = Channel(), 0
        def is_active(self): return True
        def is_authenticated(self): return True
        def open_session(self, timeout): self.opens += 1; return self.channel
    class Client:
        def __init__(self): self.transport = Transport()
        def get_transport(self): return self.transport
    client = Client()
    with pytest.raises(TransferError):
        stage_over_ssh(client, 'fixed-server-command', manifest, allowed_roots=[tmp_path])
    assert client.transport.opens == client.transport.channel.attempts == 1
    assert client.transport.channel.closed


def test_ssh_staging_passes_only_the_explicit_exact_file_references(tmp_path):
    manifest = manifest_for(tmp_path)
    trusted = [dict(path=str(tmp_path / 'catalog.json'), sha256='a' * 64)]
    class Channel:
        def settimeout(self, timeout): pass
        def exec_command(self, command): pass
        def close(self): pass
    class Transport:
        def is_active(self): return True
        def is_authenticated(self): return True
        def open_session(self, timeout): return Channel()
    class Client:
        def get_transport(self): return Transport()
    with patch('snapshot_transport.write_snapshot', side_effect=TransferError('stop test')) as writer:
        with pytest.raises(TransferError):
            stage_over_ssh(Client(), 'fixed-command', manifest, allowed_roots=[tmp_path], trusted_files=trusted)
    assert writer.call_args.kwargs['trusted_files'] == trusted
