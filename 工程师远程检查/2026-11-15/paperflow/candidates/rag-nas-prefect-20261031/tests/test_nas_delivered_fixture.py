"""Exercise existing archive primitives on local synthetic delivery copies only.

This is not a new handoff adapter or permission to use production NAS/cleanup.
"""
import hashlib
import json
from pathlib import Path
import sys

import pytest


WORKSPACE = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(WORKSPACE / 'prefect-paperflow'))


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


@pytest.fixture
def fixture(tmp_path):
    from paperflow_prefect.legacy import bind
    from paperflow_prefect.settings import Settings
    bind(Settings.load())
    source, delivered, work, nas = (tmp_path / name for name in ('originals', 'delivered', 'work', 'fake-nas'))
    source.mkdir(); delivered.mkdir(); work.mkdir()
    rows = []
    for name, content, kind in [('body.docx', b'synthetic body', '原始来源'),
                                ('attachment.zip', b'synthetic attachment', '附件')]:
        (source / name).write_bytes(content)
        copy = delivered / name
        copy.write_bytes(content)
        rows.append({'path': str(copy), 'name': name, 'kind': kind,
                     'sha256': sha(copy), 'bytes': len(content)})
    return source, delivered, work, nas, rows


def archive(fixture):
    from paperflow.original_archive.archive import ArchiveReader, ArchiveStore, writer_lock
    from paperflow.original_archive.artifacts import ArtifactReader
    from paperflow.original_archive.lifecycle import RegisteredWriter

    source, delivered, work, nas, rows = fixture
    registration = {'id': 'synthetic-task', 'identity': {'paper_id': 'fixture', 'revision': '1'},
                    'source_sha256': rows[0]['sha256'], 'files': rows}
    evidence = work / 'registration.json'
    evidence.write_text(json.dumps(registration, ensure_ascii=False), encoding='utf8')
    expected_evidence = sha(evidence)
    registered = {**registration, 'source_evidence': [{'path': str(evidence), 'sha256': expected_evidence}]}
    asset_id = hashlib.sha256(b'synthetic-asset').hexdigest()[:32]
    row = {'asset_id': asset_id, 'title': 'fixture', 'verified': True,
           'detail': {'source_hash': rows[0]['sha256'],
                      'attachment_contract': {'known': True, 'required': True}},
           'registered': registered}
    store = ArchiveStore(work / 'nas.sqlite')
    writer = RegisteredWriter(store, ArtifactReader([delivered, work], work / 'readback'), nas,
                              reserve_bytes=0, admit=lambda: sha(evidence) == expected_evidence)
    with writer_lock(store.path.with_suffix('.lock')):
        result = writer._copy(row, rows[0]['sha256'], sum(item['bytes'] for item in rows))
        assert result['status'] == 'verified' and result['warnings'] == []
        result['root'] = str(nas)
        manifest_path = nas / result['manifest_path']
        assert sha(manifest_path) == result['manifest_sha256']
        manifest = json.loads(manifest_path.read_text(encoding='utf8'))
        assert manifest['native_registration'] == registered
        assert {(r['name'], r['sha256'], r['bytes']) for r in manifest['files']} == {
            (r['name'], r['sha256'], r['bytes']) for r in rows}
        store.save(asset_id, result)
        reader = ArchiveReader(store, work / 'readback')
        for item in result['files']:
            _, stream = reader.download(asset_id, item['id'])
            with stream:
                assert hashlib.file_digest(stream, 'sha256').hexdigest() == item['sha256']
    return result, reader, asset_id


def test_native_archive_copies_delivery_files_and_keeps_originals(fixture):
    source, delivered, _, _, rows = fixture
    result, _, _ = archive(fixture)
    assert len(result['files']) == 2
    assert all(Path(row['path']).parent == delivered for row in rows)
    assert all((source / row['name']).exists() for row in rows)
    assert all(Path(row['path']).exists() for row in rows)


def test_changed_delivery_copy_cannot_be_archived_as_the_original_identity(fixture):
    source, delivered, _, _, rows = fixture
    (delivered / 'attachment.zip').write_bytes(b'changed attachment')
    with pytest.raises(ValueError):
        archive(fixture)
    assert all((source / row['name']).exists() for row in rows)


def test_corrupt_archived_copy_fails_readback_and_keeps_delivery_files(fixture):
    _, delivered, _, nas, rows = fixture
    result, reader, asset_id = archive(fixture)
    item = result['files'][0]
    (nas / item['object_path']).write_bytes(b'corrupt')
    with pytest.raises(ValueError):
        reader.download(asset_id, item['id'])
    assert all((delivered / row['name']).exists() for row in rows)


def test_verified_copy_inventory_flows_into_native_archive_primitive(tmp_path):
    from delivery_inventory import prepare_inventory
    from test_delivery_inventory import make_receipt
    from paperflow_prefect.legacy import bind
    from paperflow_prefect.settings import Settings
    bind(Settings.load())
    receipt, handoff, delivery = make_receipt(tmp_path)
    inventory = prepare_inventory(receipt)
    rows = [{'path': item['path'], 'name': item['relative'],
             'kind': '原始来源' if item['kind'] == 'body' else '附件',
             'sha256': item['sha256'], 'bytes': item['bytes']} for item in inventory['files']]
    work, nas = tmp_path / 'archive-work', tmp_path / 'fake-nas'
    work.mkdir()
    result, _, _ = archive((tmp_path / 'source', delivery / 'files', work, nas, rows))
    assert {(row['name'], row['sha256']) for row in result['files']} == {
        (row['name'], row['sha256']) for row in rows}
    assert Path(handoff['body']['path']).exists()
    assert Path(handoff['attachments'][0]['path']).exists()
    assert Path(handoff['trash'][0]['path']).exists()
