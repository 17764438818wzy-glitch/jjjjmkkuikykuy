"""Only inspect synthetic files; these tests never remove business data."""
from pathlib import Path

import pytest

from test_delivery_inventory import make_receipt


def test_cleanup_preparation_snapshots_exact_delivery_files_without_authorizing_release(tmp_path):
    from cleanup_snapshot import prepare_cleanup_snapshot
    from paperflow_prefect.legacy import bind
    from paperflow_prefect.settings import Settings
    bind(Settings.load())
    receipt, handoff, delivery = make_receipt(tmp_path)
    snapshot = prepare_cleanup_snapshot(receipt)
    assert snapshot['release_authorized'] is False
    assert {row['kind'] for row in snapshot['files']} == {'body', 'attachment'}
    assert len(snapshot['files']) == 2
    assert all(row['object_identity'] and row['object_fingerprint'] for row in snapshot['files'])
    assert all(Path(row['path']).is_relative_to(delivery / 'files') for row in snapshot['files'])
    assert all(Path(row['path']).exists() for row in snapshot['files'])
    assert Path(handoff['body']['path']).exists()
    assert Path(handoff['attachments'][0]['path']).exists()
    assert Path(handoff['trash'][0]['path']).exists()


def test_cleanup_preparation_rejects_changed_delivery_copy(tmp_path):
    from cleanup_snapshot import prepare_cleanup_snapshot
    from paperflow_prefect.legacy import bind
    from paperflow_prefect.settings import Settings
    bind(Settings.load())
    receipt, _, delivery = make_receipt(tmp_path)
    (delivery / 'files' / 'attachment.zip').write_bytes(b'changed')
    with pytest.raises(ValueError):
        prepare_cleanup_snapshot(receipt)
