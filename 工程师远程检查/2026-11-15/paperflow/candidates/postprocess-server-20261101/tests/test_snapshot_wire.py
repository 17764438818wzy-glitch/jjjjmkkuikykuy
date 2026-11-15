"""A staged snapshot is bound to one folder and cannot start partial work."""
import hashlib
import io
import os
from pathlib import Path
from uuid import UUID, uuid4, uuid5

import pytest

from snapshot_wire import receive_snapshot, snapshot_sha, SnapshotWireError


pytestmark = pytest.mark.skipif(os.name != 'posix', reason='server-side snapshot staging')


def make_manifest():
    identity = {'task_id': str(uuid4()), 'group_id': 'a' * 32, 'paper_id': 'paper',
                'receipt_id': 'b' * 64, 'attempt_id': 'attempt-1',
                'parent_run_id': str(uuid4()), 'task_run_id': str(uuid4())}
    job_id = str(uuid5(UUID('5e9b7732-458e-50cb-8916-6b53007c204f'), identity['task_run_id']))
    files = [{'path': r'F:\paper\request.json', 'sha256': hashlib.sha256(b'{}').hexdigest(), 'bytes': 2},
             {'path': r'F:\paper\reply.txt', 'sha256': hashlib.sha256(b'reply').hexdigest(), 'bytes': 5}]
    return {'schema': 'paperflow-remote-snapshot/1', 'job_id': job_id, 'claim_id': str(uuid4()),
            'identity': identity, 'generation': 1, 'manifest_sha256': 'c' * 64, 'code_sha256': 'd' * 64,
            'inputs': {'identity': identity, 'manifest_sha256': 'c' * 64,
                       'body': {'identity': identity}, 'attachments': []}, 'files': files}


def test_one_complete_snapshot_is_staged_with_the_original_identity(tmp_path):
    manifest = make_manifest(); root = tmp_path / 'job'
    result = receive_snapshot(root, manifest, snapshot_sha(manifest), io.BytesIO(b'{}reply'))
    assert result['snapshot_sha256'] == snapshot_sha(manifest)
    assert result['identity'] == manifest['identity']
    assert result['evidence_scope'] == 'declared_files_only'
    assert result['dependency_closure_verified'] is False
    assert (root / 'drives/f/paper/request.json').read_bytes() == b'{}'
    assert (root / 'drives/f/paper/reply.txt').read_bytes() == b'reply'
    assert (root / 'INPUT_VERIFIED.json').is_file()


def test_interrupted_transfer_never_produces_a_verified_marker(tmp_path):
    manifest = make_manifest(); root = tmp_path / 'job'
    with pytest.raises(SnapshotWireError):
        receive_snapshot(root, manifest, snapshot_sha(manifest), io.BytesIO(b'{}re'))
    assert not (root / 'INPUT_VERIFIED.json').exists()
    with pytest.raises(SnapshotWireError):
        receive_snapshot(root, manifest, snapshot_sha(manifest), io.BytesIO(b'{}reply'))


def test_wrong_snapshot_digest_or_folder_identity_is_rejected_before_writes(tmp_path):
    manifest = make_manifest(); root = tmp_path / 'job'
    with pytest.raises(SnapshotWireError):
        receive_snapshot(root, manifest, '0' * 64, io.BytesIO(b'{}reply'))
    assert not root.exists()
    manifest['inputs']['identity'] = dict(manifest['identity'], paper_id='another-paper')
    with pytest.raises(SnapshotWireError):
        receive_snapshot(root, manifest, snapshot_sha(manifest), io.BytesIO(b'{}reply'))
    assert not root.exists()


def test_tampered_source_bytes_do_not_create_verified_marker(tmp_path):
    manifest = make_manifest(); root = tmp_path / 'job'
    with pytest.raises(SnapshotWireError):
        receive_snapshot(root, manifest, snapshot_sha(manifest), io.BytesIO(b'{}other'))
    assert not (root / 'INPUT_VERIFIED.json').exists()


@pytest.mark.parametrize('replacement_kind', ['symlink', 'directory'])
def test_ancestor_swap_before_mkdir_cannot_redirect_snapshot_creation(tmp_path, monkeypatch, replacement_kind):
    ancestor = tmp_path / 'ancestor'; ancestor.mkdir(mode=0o700)
    parent = ancestor / 'staging'; parent.mkdir(mode=0o700)
    replacement = tmp_path / 'replacement'; replacement.mkdir(mode=0o700)
    (replacement / 'staging').mkdir(mode=0o700)
    moved = tmp_path / 'moved-ancestor'
    root = parent / 'job'
    mkdir = os.mkdir
    swapped = []
    def swap_before_leaf(path, mode=0o777, *, dir_fd=None):
        if Path(path).name == 'job' and not swapped:
            swapped.append(True)
            ancestor.rename(moved)
            if replacement_kind == 'symlink':
                ancestor.symlink_to(replacement, target_is_directory=True)
            else:
                replacement.rename(ancestor)
        return mkdir(path, mode, dir_fd=dir_fd)
    monkeypatch.setattr(os, 'mkdir', swap_before_leaf)
    manifest = make_manifest()
    with pytest.raises(SnapshotWireError):
        receive_snapshot(root, manifest, snapshot_sha(manifest), io.BytesIO(b'{}reply'))
    visible_replacement = replacement if replacement_kind == 'symlink' else ancestor
    assert not (visible_replacement / 'staging/job').exists()
    assert not (moved / 'staging/job/INPUT_VERIFIED.json').exists()


def test_ancestor_swap_while_receiving_never_leaves_a_verified_marker(tmp_path):
    ancestor = tmp_path / 'ancestor'; ancestor.mkdir(mode=0o700)
    root = ancestor / 'job'
    moved = tmp_path / 'moved-ancestor'
    class SwapAncestor(io.BytesIO):
        def read(self, length=-1):
            if not moved.exists():
                ancestor.rename(moved)
                ancestor.symlink_to(moved, target_is_directory=True)
            return super().read(length)
    manifest = make_manifest()
    with pytest.raises(SnapshotWireError):
        receive_snapshot(root, manifest, snapshot_sha(manifest), SwapAncestor(b'{}reply'))
    assert not (moved / 'job/INPUT_VERIFIED.json').exists()


def test_ancestor_swap_during_marker_creation_revokes_its_unaccepted_marker(tmp_path, monkeypatch):
    ancestor = tmp_path / 'ancestor'; ancestor.mkdir(mode=0o700)
    root = ancestor / 'job'
    moved = tmp_path / 'moved-ancestor'
    opened = os.open
    def swap_before_marker(path, flags, mode=0o777, *, dir_fd=None):
        if path == 'INPUT_VERIFIED.json' and flags & os.O_CREAT:
            ancestor.rename(moved)
            ancestor.symlink_to(moved, target_is_directory=True)
        return opened(path, flags, mode, dir_fd=dir_fd)
    monkeypatch.setattr(os, 'open', swap_before_marker)
    manifest = make_manifest()
    with pytest.raises(SnapshotWireError):
        receive_snapshot(root, manifest, snapshot_sha(manifest), io.BytesIO(b'{}reply'))
    assert not (moved / 'job/INPUT_VERIFIED.json').exists()
