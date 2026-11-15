from pathlib import Path
import json

import pytest

from test_isolated_group_rag import case as group
from test_folder_native_client import admission
from isolated_archive import archive_test_copy


@pytest.fixture
def archived(group, tmp_path):
    rag = group['execute']()
    nas = tmp_path / 'nas-test' / group['ticket']['group_id']
    work = tmp_path / 'archive-work'
    archive_test_copy(group['copied']['receipt_path'], work, nas, admit=lambda: True)
    def cleanup(running=True):
        from isolated_cleanup import cleanup_test_copy
        return cleanup_test_copy(group['copied']['receipt_path'], rag['receipt_path'], work,
                                 tmp_path / 'copies', nas, tmp_path / 'cleanup.json',
                                 admit=lambda *_: admission(running))
    return locals()


def test_deletes_only_created_copies_after_whole_group_rag_and_nas_readback(archived):
    result = archived['cleanup']()
    assert result['status'] == 'completed'
    assert len(result['files']) == 2
    assert all(not Path(row['path']).exists() for row in archived['group']['copied']['files'])
    assert all(Path(row['source_delivery_path']).exists() for row in archived['group']['copied']['files'])
    assert Path(archived['group']['copied']['receipt_path']).exists()


def test_incomplete_rag_prevents_all_deletion(archived):
    path = Path(archived['rag']['receipt_path'])
    rag = json.loads(path.read_bytes())
    rag['status'] = 'pending'
    path.write_text(json.dumps(rag), encoding='utf8')
    with pytest.raises(ValueError, match='RAG'):
        archived['cleanup']()
    assert all(Path(row['path']).exists() for row in archived['group']['copied']['files'])


def test_damaged_nas_archive_prevents_all_deletion(archived):
    result = json.loads((archived['work'] / 'archive-complete.json').read_bytes())
    first = result['files'][0]
    (archived['nas'] / 'objects' / first['sha256'][:2] / first['sha256']).write_bytes(b'damaged')
    with pytest.raises((ValueError, OSError)):
        archived['cleanup']()
    assert all(Path(row['path']).exists() for row in archived['group']['copied']['files'])


def test_missing_nas_manifest_prevents_all_deletion(archived):
    result = json.loads((archived['work'] / 'archive-complete.json').read_bytes())
    manifest = archived['nas'] / 'papers' / result['asset_id'] / (result['manifest_sha256'] + '.json')
    manifest.write_bytes(b'corrupted')
    with pytest.raises(ValueError, match='manifest'):
        archived['cleanup']()
    assert all(Path(row['path']).exists() for row in archived['group']['copied']['files'])


def test_pause_before_delete_preserves_copies_and_can_resume(archived):
    with pytest.raises(InterruptedError):
        archived['cleanup'](False)
    assert all(Path(row['path']).exists() for row in archived['group']['copied']['files'])
    assert archived['cleanup']()['status'] == 'completed'


def test_pause_between_copy_deletions_resumes_without_touching_originals(archived, tmp_path):
    from isolated_cleanup import cleanup_test_copy
    count = 0
    def pause_after_first(*_):
        nonlocal count
        count += 1
        return admission(count == 1)
    group = archived['group']
    with pytest.raises(InterruptedError):
        cleanup_test_copy(group['copied']['receipt_path'], archived['rag']['receipt_path'], archived['work'],
                          tmp_path / 'copies', archived['nas'], tmp_path / 'cleanup.json', admit=pause_after_first)
    assert sum(Path(row['path']).exists() for row in group['copied']['files']) == 1
    assert archived['cleanup']()['status'] == 'completed'
    assert all(not Path(row['path']).exists() for row in group['copied']['files'])
    assert all(Path(row['source_delivery_path']).exists() for row in group['copied']['files'])


def test_same_content_replacement_is_not_treated_as_the_created_copy(archived, tmp_path):
    target = Path(archived['group']['copied']['files'][0]['path'])
    replacement = tmp_path / 'replacement'
    replacement.write_bytes(target.read_bytes())
    replacement.replace(target)
    with pytest.raises(ValueError, match='object'):
        archived['cleanup']()
    assert target.exists()
