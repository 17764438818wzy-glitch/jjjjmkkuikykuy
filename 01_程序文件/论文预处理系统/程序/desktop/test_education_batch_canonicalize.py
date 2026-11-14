import json

import pytest

from education_batch_canonicalize import canonicalize_batch
from engine import Store, digest
from evidence import export_package, package_fingerprint
from knowledge_package import knowledge_files
from test_knowledge_package import sample


def _batch(tmp_path, status='completed'):
    store = Store(tmp_path / 'data')
    run_id = 'AUTO_batch_test'
    package = store.runs / run_id / 'packages' / 'AUTO_test'
    package.mkdir(parents=True)
    meta = sample(package)
    meta.update(major='小学教育', category='教育教学类')
    export_package(package, meta, canonical_paper_id=False)
    source_manifest = json.loads((package / 'manifest.json').read_text(encoding='utf-8'))
    item = {
        'id': 'AUTO_test', 'status': 'pass', 'category': '教育教学类',
        'paper_id': source_manifest['paper_id'], 'package_path': str(package),
        'path': str(package / 'ingest.md'), 'display_path': str(package / 'document.md'),
        'knowledge_manifest_path': str(package / 'manifest.json'),
        'reviewed_hash': digest(package / 'ingest.md'),
        'reviewed_package_hash': package_fingerprint(package), 'uploads': {},
    }
    batch = {'run_id': run_id, 'status': status, 'items': [item]}
    store.save(batch)
    return store, batch, package


def test_completed_batch_repoints_to_canonical_copy_and_keeps_source(tmp_path):
    store, batch, source = _batch(tmp_path)
    original = (source / 'ingest.md').read_bytes()
    assert canonicalize_batch(store, batch['run_id']) == {'needs_repair': 1, 'repaired': 0}
    assert store.load(batch['run_id'])['items'][0]['package_path'] == str(source)

    assert canonicalize_batch(store, batch['run_id'], apply=True) == {'needs_repair': 1, 'repaired': 1}
    item = store.load(batch['run_id'])['items'][0]
    destination = store.runs / batch['run_id'] / 'canonical_packages' / item['id']
    assert item['package_path'] == str(destination)
    assert source.joinpath('ingest.md').read_bytes() == original
    assert knowledge_files(destination, require_stable_paper_id=True)
    assert item['reviewed_hash'] == digest(destination / 'ingest.md')
    assert item['reviewed_package_hash'] == package_fingerprint(destination)
    assert canonicalize_batch(store, batch['run_id'], apply=True) == {'needs_repair': 0, 'repaired': 0}


def test_active_or_uploaded_batch_is_not_modified(tmp_path):
    store, batch, source = _batch(tmp_path, status='running')
    with pytest.raises(ValueError, match='尚未正常结束'):
        canonicalize_batch(store, batch['run_id'], apply=True)
    assert not (store.runs / batch['run_id'] / 'canonical_packages').exists()

    batch['status'] = 'completed'
    batch['items'][0]['uploads'] = {'remote': {'document_id': 'remote-doc'}}
    store.save(batch)
    with pytest.raises(ValueError, match='已上传'):
        canonicalize_batch(store, batch['run_id'], apply=True)
    assert source.is_dir()
