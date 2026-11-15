import hashlib
from pathlib import Path
import sys

import pytest

WORKSPACE = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(WORKSPACE / 'prefect-paperflow'))

from isolated_copy import create_test_copy
from isolated_archive import archive_test_copy


@pytest.fixture
def group(tmp_path):
    from paperflow_prefect.legacy import bind
    from paperflow_prefect.settings import Settings
    bind(Settings.load())
    originals = tmp_path / 'originals'
    originals.mkdir()
    files = []
    for kind, name, content in [('body', 'body.docx', b'body'), ('body', 'body.pdf', b'body pdf'),
                                 ('attachment', 'extra.pdf', b'attachment')]:
        source = originals / name
        source.write_bytes(content)
        files.append({'file_id': name, 'kind': kind, 'relative': name, 'path': str(source),
                      'sha256': hashlib.sha256(content).hexdigest(), 'bytes': len(content)})
    inventory = {'group_id': 'group-1', 'paper_id': 'paper-1', 'receipt_id': 'receipt-1', 'files': files}
    return create_test_copy(inventory, tmp_path / 'test-root'), files


def test_reuses_archive_writer_and_reads_back_every_group_file(tmp_path, group):
    copied, originals = group
    result = archive_test_copy(copied['receipt_path'], tmp_path / 'work', tmp_path / 'nas', admit=lambda: True)
    assert result['group_id'] == copied['group_id']
    assert result['readback_verified'] is True
    assert {(item['name'], item['sha256']) for item in result['files']} == {
        (item['relative'], item['sha256']) for item in copied['files']}
    assert all(Path(item['path']).exists() for item in copied['files'])
    assert all(Path(item['path']).exists() for item in originals)


def test_isolated_group_archive_does_not_take_legacy_writer_lock(tmp_path, group, monkeypatch):
    import paperflow.original_archive.archive as archive_module
    def unexpected_lock(*_args, **_kwargs):
        raise AssertionError('isolated one-group archive must not take the legacy file lock')
    monkeypatch.setattr(archive_module, 'writer_lock', unexpected_lock)
    copied, _ = group
    result = archive_test_copy(copied['receipt_path'], tmp_path / 'work', tmp_path / 'nas', admit=lambda: True)
    assert result['readback_verified'] is True


def test_uses_saved_copy_receipt_as_the_complete_file_inventory(tmp_path, group):
    copied, _ = group
    copied['files'].pop()
    result = archive_test_copy(copied['receipt_path'], tmp_path / 'work', tmp_path / 'nas', admit=lambda: True)
    assert len(result['files']) == 3


def test_paused_admission_does_not_archive_or_delete(tmp_path, group):
    copied, originals = group
    with pytest.raises(InterruptedError):
        archive_test_copy(copied['receipt_path'], tmp_path / 'work', tmp_path / 'nas', admit=lambda: False)
    assert not (tmp_path / 'nas').exists()
    assert not (tmp_path / 'work').exists()
    assert all(Path(item['path']).exists() for item in copied['files'])
    assert all(Path(item['path']).exists() for item in originals)
    result = archive_test_copy(copied['receipt_path'], tmp_path / 'work', tmp_path / 'nas', admit=lambda: True)
    assert result['readback_verified'] is True


def test_pause_during_archive_can_resume_using_the_same_group_record(tmp_path, group):
    copied, _ = group
    calls = 0
    def stop_during_write():
        nonlocal calls
        calls += 1
        return calls < 4
    with pytest.raises(InterruptedError):
        archive_test_copy(copied['receipt_path'], tmp_path / 'work', tmp_path / 'nas', admit=stop_during_write)
    result = archive_test_copy(copied['receipt_path'], tmp_path / 'work', tmp_path / 'nas', admit=lambda: True)
    assert result['readback_verified'] is True
    assert len(result['files']) == len(copied['files'])
