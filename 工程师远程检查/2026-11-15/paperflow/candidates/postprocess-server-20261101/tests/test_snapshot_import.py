"""Only explicitly declared original evidence may enter a server snapshot."""
import hashlib
import io
import os
from pathlib import Path

import pytest

from snapshot_import import SnapshotImporter, SnapshotImportError


pytestmark = pytest.mark.skipif(os.name != 'posix', reason='server POSIX file-descriptor boundary')


def sha(data):
    return hashlib.sha256(data).hexdigest()


def entry(name, raw):
    return {'path': name, 'sha256': sha(raw), 'bytes': len(raw)}


def test_snapshot_streams_declared_evidence_without_changing_bytes(tmp_path):
    root = tmp_path / 'snapshot'; root.mkdir(mode=0o700)
    files = [entry(r'F:\papers\one.json', b'{"n":1}'), entry(r'F:\papers\image.png', b'pixels')]
    target = SnapshotImporter(root, files)
    for item, data in zip(files, [b'{"n":1}', b'pixels']):
        target.put(item['path'], io.BytesIO(data))
    assert target.verify_complete() is True
    assert (root / 'drives/f/papers/one.json').read_bytes() == b'{"n":1}'


def test_tampered_or_unregistered_payload_never_becomes_committed(tmp_path):
    root = tmp_path / 'snapshot'; root.mkdir(mode=0o700)
    target = SnapshotImporter(root, [entry(r'F:\data\a', b'good')])
    with pytest.raises(SnapshotImportError, match='unregistered'):
        target.put(r'F:\data\else', io.BytesIO(b'good'))
    with pytest.raises(SnapshotImportError, match='changed'):
        target.put(r'F:\data\a', io.BytesIO(b'evil'))
    assert not (root / 'drives/f/data/a').exists()
    with pytest.raises(SnapshotImportError):
        target.verify_complete()


def test_existing_file_is_never_overwritten(tmp_path):
    root = tmp_path / 'snapshot'; root.mkdir(mode=0o700)
    target = SnapshotImporter(root, [entry(r'F:\data\a', b'good')])
    target.put(r'F:\data\a', io.BytesIO(b'good'))
    with pytest.raises(SnapshotImportError, match='exists'):
        target.put(r'F:\data\a', io.BytesIO(b'good'))
    assert (root / 'drives/f/data/a').read_bytes() == b'good'


def test_parent_symlink_cannot_redirect_snapshot_writes(tmp_path):
    root = tmp_path / 'snapshot'; root.mkdir(mode=0o700)
    outside = tmp_path / 'outside'; outside.mkdir()
    (root / 'drives').symlink_to(outside, target_is_directory=True)
    target = SnapshotImporter(root, [entry(r'F:\data\a', b'good')])
    with pytest.raises(SnapshotImportError):
        target.put(r'F:\data\a', io.BytesIO(b'good'))
    assert list(outside.iterdir()) == []


def test_replacing_snapshot_root_mid_transfer_cannot_redirect_writes(tmp_path):
    root = tmp_path / 'snapshot'; root.mkdir(mode=0o700)
    outside = tmp_path / 'outside'; outside.mkdir(mode=0o700)
    target = SnapshotImporter(root, [entry(r'F:\data\a', b'good')])
    class SwapRoot(io.BytesIO):
        def read(self, length=-1):
            root.rename(tmp_path / 'original-snapshot')
            root.symlink_to(outside, target_is_directory=True)
            return super().read(length)
    with pytest.raises(SnapshotImportError):
        target.put(r'F:\data\a', SwapRoot(b'good'))
    assert list(outside.iterdir()) == []


def test_incomplete_or_altered_snapshot_is_not_ready(tmp_path):
    root = tmp_path / 'snapshot'; root.mkdir(mode=0o700)
    target = SnapshotImporter(root, [entry(r'F:\data\a', b'good'), entry(r'F:\data\b', b'x')])
    target.put(r'F:\data\a', io.BytesIO(b'good'))
    with pytest.raises(SnapshotImportError):
        target.verify_complete()
    target.put(r'F:\data\b', io.BytesIO(b'x'))
    (root / 'drives/f/data/a').chmod(0o600)
    (root / 'drives/f/data/a').write_bytes(b'evil')
    with pytest.raises(SnapshotImportError, match='changed'):
        target.verify_complete()


def test_duplicate_and_escaping_windows_source_names_are_rejected(tmp_path):
    root = tmp_path / 'snapshot'; root.mkdir(mode=0o700)
    item = entry(r'F:\data\a', b'x')
    with pytest.raises(SnapshotImportError):
        SnapshotImporter(root, [item, item])
    with pytest.raises(SnapshotImportError):
        SnapshotImporter(root, [entry(r'F:\data\..\secret', b'x')])


def test_ancestor_symlink_swap_during_root_open_is_rejected_even_if_root_inode_is_same(tmp_path, monkeypatch):
    ancestor = tmp_path / 'ancestor'; ancestor.mkdir(mode=0o700)
    root = ancestor / 'snapshot'; root.mkdir(mode=0o700)
    moved = tmp_path / 'moved-ancestor'
    opened = os.open
    swapped = []
    def swap_before_snapshot(path, flags, mode=0o777, *, dir_fd=None):
        if Path(path).name == 'snapshot' and flags & os.O_DIRECTORY and not swapped:
            swapped.append(True)
            ancestor.rename(moved)
            ancestor.symlink_to(moved, target_is_directory=True)
        return opened(path, flags, mode, dir_fd=dir_fd)
    monkeypatch.setattr(os, 'open', swap_before_snapshot)
    with pytest.raises(SnapshotImportError):
        SnapshotImporter(root, [entry(r'F:\data\a', b'good')])
    assert not (moved / 'snapshot/drives').exists()


@pytest.mark.parametrize('replacement_kind', ['symlink', 'directory'])
def test_visible_ancestor_binding_is_rechecked_before_existing_importer_writes(tmp_path, replacement_kind):
    ancestor = tmp_path / 'ancestor'; ancestor.mkdir(mode=0o700)
    root = ancestor / 'snapshot'; root.mkdir(mode=0o700)
    moved = tmp_path / 'moved-ancestor'
    target = SnapshotImporter(root, [entry(r'F:\data\a', b'good')])
    ancestor.rename(moved)
    if replacement_kind == 'symlink':
        ancestor.symlink_to(moved, target_is_directory=True)
    else:
        ancestor.mkdir(mode=0o700)
        (moved / 'snapshot').rename(root)
    try:
        with pytest.raises(SnapshotImportError):
            target.put(r'F:\data\a', io.BytesIO(b'good'))
        assert not (root / 'drives').exists()
    finally:
        target.close()
