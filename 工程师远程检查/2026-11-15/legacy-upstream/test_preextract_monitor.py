"""Read-only progress projection must never turn incomplete evidence into success."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path

import pytest


def monitor():
    assert importlib.util.find_spec('preextract_monitor') is not None, 'read-only monitor is not implemented'
    from preextract_monitor import PreextractMonitorError, PreextractMonitorReader
    return PreextractMonitorReader, PreextractMonitorError


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding='utf8')
    return path


def paper(tmp_path, *, stage='material_prepared'):
    root = tmp_path / 'data'
    batch = root / 'private' / 'trial'
    work = batch / 'materials' / 'paper-01'
    item = dict(number=1, name='研究.docx', source_sha256='a' * 64,
                path=str(tmp_path / 'original-do-not-read.docx'),
                prepared_checkpoint={'path': str(work / 'material.json')})
    put(batch / 'prepared-selection.json', [item])
    put(work / 'material.json', {'source': {'source_sha256': 'a' * 64}})
    put(work / 'status.json', dict(stage=stage, source_sha256='a' * 64, completed=False))
    return root, batch, work, item


def sealed(work, *, image=True):
    task = dict(task_id='T1', source_id='S1', source_sha256='a' * 64, revision=1)
    routes = [dict(code='080801', paper_dataset_id='body1', image_dataset_id='image1'),
              dict(code='080802', paper_dataset_id='body2', image_dataset_id='image2')]
    package = work / 'publication'
    ident = {k: task[k] for k in ('task_id', 'source_id', 'source_sha256')}
    manifest = put(package / 'manifest.json', dict(ident, revision='r001', files=[]))
    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    put(package / 'READY.json', dict(ident, status='ready', manifest_sha256=digest))
    put(package / 'profile.json', dict(ident, routing_candidates=[{'code': r['code']} for r in routes]))
    put(package / 'assets.json', dict(ident, items=[{'id': 'A1', 'storage': 'image'}] if image else []))
    put(work / 'private' / 'prepared-publication.json', dict(package_path=str(package), task=task,
        validation=dict(status='publication_ready', manifest_sha256=digest), catalog={'majors': routes}))
    receipt = dict(status='complete', stage='verified', task=task, manifest_sha256=digest,
        targets={r['code']: dict(verified=True, retrieval_verified=True,
            body=dict(dataset_id=r['paper_dataset_id'], document_id='B' + r['code']),
            images=dict(dataset_id=r['image_dataset_id'], document_id='I' + r['code'])) for r in routes})
    put(work / 'delivery-receipt.json', receipt)
    return receipt


def read(root):
    cls, _ = monitor()
    return cls(root).read('trial')


def test_discovery_prefers_prepared_and_skips_unlocatable_selections(tmp_path):
    root, batch, work, item = paper(tmp_path)
    put(batch / 'selection.json', [item, item])
    put(root / 'private' / 'unrelated' / 'selection.json', [{'name': 'unrelated'}])
    put(root / 'private' / 'fallback' / 'selection.json', [dict(item, prepared_checkpoint=None, work_dir=str(work))])
    cls, _ = monitor()
    batches = cls(root).discover()
    assert {b['id']: b['count'] for b in batches} == {'trial': 1, 'fallback': 1}
    assert next(b for b in batches if b['id'] == 'trial')['path'].endswith('prepared-selection.json')


def test_new_revision_request_wins_over_parent_failure_with_newer_mtime(tmp_path):
    root, _, work, _ = paper(tmp_path, stage='stopped')
    put(work / 'model' / 'request-state.json', dict(status='request_failed_or_uncertain', started_at=100, finished_at=110))
    revision = work / 'revisions' / 'jpeg-recovery'
    put(revision / 'revision.json', dict(schema='preextract-authorized-revision/1', parent=str(work),
        source_sha256='a' * 64, mode='rerun'))
    put(revision / 'status.json', dict(stage='extracting_once', source_sha256='a' * 64))
    put(revision / 'model' / 'request-state.json', dict(status='request_in_flight', started_at=200))
    os.utime(work / 'status.json', (5000, 5000))
    row = read(root)['rows'][0]
    assert row['work_dir'] == str(revision)
    assert row['model_status'] == 'request_in_flight'
    assert len(row['attempts']) == 2
    assert '待响应' in row['label']
    assert '正在' not in row['label']
    assert read(root)['counts']['attention'] == 0


def test_foreign_revision_cannot_replace_bound_source(tmp_path):
    root, _, work, _ = paper(tmp_path)
    revision = work / 'revisions' / 'foreign'
    put(revision / 'revision.json', dict(schema='preextract-authorized-revision/1', parent=str(work),
        source_sha256='b' * 64, mode='rerun'))
    put(revision / 'model' / 'request-state.json', dict(status='request_in_flight', started_at=9999))
    row = read(root)['rows'][0]
    assert row['work_dir'] == str(work)
    assert len(row['attempts']) == 1


def test_all_route_documents_and_readbacks_are_required_for_verified(tmp_path):
    root, _, work, _ = paper(tmp_path, stage='package_verified')
    sealed(work)
    snapshot = read(root)
    assert snapshot['rows'][0]['stage'] == 'verified'
    assert snapshot['rows'][0]['verified_targets'] == 2
    assert snapshot['counts'] == dict(total=1, packaged=1, verified=1, attention=0, pending=0)


@pytest.mark.parametrize('damage', ['target', 'retrieval', 'image_id', 'dataset', 'task', 'digest', 'ready', 'manifest'])
def test_claimed_complete_with_missing_or_mismatched_proof_is_attention(tmp_path, damage):
    root, _, work, _ = paper(tmp_path, stage='ingested_and_verified')
    receipt = sealed(work)
    if damage == 'target': receipt['targets'].pop('080802')
    if damage == 'retrieval': receipt['targets']['080802']['retrieval_verified'] = False
    if damage == 'image_id': receipt['targets']['080802']['images'].pop('document_id')
    if damage == 'dataset': receipt['targets']['080802']['body']['dataset_id'] = 'wrong'
    if damage == 'task': receipt['task'] = dict(receipt['task'], task_id='other')
    if damage == 'digest': receipt['manifest_sha256'] = 'b' * 64
    if damage == 'ready': (work / 'publication' / 'READY.json').unlink()
    if damage == 'manifest': put(work / 'publication' / 'manifest.json', {'task_id': 'other'})
    put(work / 'delivery-receipt.json', receipt)
    snapshot = read(root)
    assert snapshot['rows'][0]['stage'] == 'attention'
    assert snapshot['counts']['verified'] == 0
    assert snapshot['counts']['attention'] == 1


def test_publishing_and_uncertain_readback_are_not_success(tmp_path):
    root, batch, work, _ = paper(tmp_path, stage='package_verified')
    receipt = sealed(work, image=False)
    receipt.update(status='pending', stage='publishing', targets={})
    put(work / 'delivery-receipt.json', receipt)
    put(batch / 'progress.json', dict(finished=True, completed=30))
    snapshot = read(root)
    assert snapshot['rows'][0]['stage'] == 'delivering'
    assert snapshot['counts']['verified'] == 0
    receipt.update(status='uncertain', stage='readback')
    put(work / 'delivery-receipt.json', receipt)
    assert read(root)['counts']['attention'] == 1


def test_partial_model_reply_needs_attention_without_leaking_errors(tmp_path):
    root, _, work, _ = paper(tmp_path, stage='stopped')
    put(work / 'model' / 'request-state.json', dict(status='partial_response', error='SECRET-CREDENTIAL', finished_at=200))
    put(work / 'status.json', dict(stage='stopped', source_sha256='a' * 64,
        error='SECRET-CREDENTIAL', failed_stage='extracting_once'))
    (work / 'model' / 'http-error.txt').write_text('SECRET-CREDENTIAL')
    snapshot = read(root)
    assert snapshot['counts']['attention'] == 1
    assert snapshot['rows'][0]['model_status'] == 'partial_response'
    assert 'SECRET-CREDENTIAL' not in json.dumps(snapshot)


def test_half_written_json_raises_safe_error_instead_of_empty_snapshot(tmp_path):
    root, _, work, _ = paper(tmp_path)
    cls, error = monitor()
    reader = cls(root)
    assert reader.read('trial')['counts']['total'] == 1
    (work / 'status.json').write_text('{"error":"SECRET-CREDENTIAL', encoding='utf8')
    with pytest.raises(error) as caught:
        reader.read('trial')
    assert 'SECRET-CREDENTIAL' not in str(caught.value)


def test_paths_outside_data_root_are_rejected(tmp_path):
    root, batch, _, item = paper(tmp_path)
    put(batch / 'prepared-selection.json', [dict(item, prepared_checkpoint={'path': str(tmp_path / 'outside' / 'material.json')})])
    cls, error = monitor()
    with pytest.raises(error): cls(root).read('trial')
    with pytest.raises(error): cls(root).read('../outside')


@pytest.mark.parametrize('stage,expected', [('adapting_images', 'adapting_images'),
    ('extracting_parts', 'extracting_parts'), ('assembling_parts', 'assembling_parts'),
    ('reconciling_parts', 'synthesizing_parts')])
def test_image_and_partition_stages_are_preserved(tmp_path, stage, expected):
    root, _, _, _ = paper(tmp_path, stage=stage)
    assert read(root)['rows'][0]['stage'] == expected


def test_missing_receipt_cannot_make_ingested_status_success(tmp_path):
    root, _, _, _ = paper(tmp_path, stage='ingested_and_verified')
    assert read(root)['counts']['verified'] == 0
    assert read(root)['counts']['attention'] == 1


def test_ui_has_numeric_evidence_times_and_safe_chinese_status_labels(tmp_path):
    root, _, work, _ = paper(tmp_path, stage='extracting_once')
    put(work / 'model' / 'request-state.json', dict(status='request_in_flight', started_at=200))
    snapshot = read(root)
    row = snapshot['rows'][0]
    assert isinstance(snapshot['updated_at'], (int, float))
    assert isinstance(row['updated_at'], (int, float))
    assert '待响应' in row['model_label']
    assert '未投递' in row['delivery_label']


def test_discovery_places_newest_selection_first(tmp_path):
    root, batch, work, item = paper(tmp_path)
    newer = put(root / 'private' / 'a-new-batch' / 'selection.json', [dict(item, work_dir=str(work))])
    os.utime(batch / 'prepared-selection.json', (100, 100))
    os.utime(newer, (200, 200))
    cls, _ = monitor()
    assert cls(root).discover()[0]['id'] == 'a-new-batch'


def test_discovery_uses_preferred_list_time_not_directory_or_fallback_time(tmp_path):
    root, batch, work, item = paper(tmp_path)
    fallback = put(batch / 'selection.json', [dict(item, work_dir=str(work))])
    newer = put(root / 'private' / 'a-new-batch' / 'selection.json', [dict(item, work_dir=str(work))])
    os.utime(batch / 'prepared-selection.json', (100, 100))
    os.utime(newer, (200, 200))
    os.utime(fallback, (900, 900))
    os.utime(batch, (1000, 1000))
    cls, _ = monitor()
    batches = cls(root).discover()
    assert [row['id'] for row in batches] == ['a-new-batch', 'trial']
    assert batches[1]['path'].endswith('prepared-selection.json')


def test_complete_receipt_label_does_not_claim_success_when_identity_differs(tmp_path):
    root, _, work, _ = paper(tmp_path)
    receipt = sealed(work)
    receipt['manifest_sha256'] = 'b' * 64
    put(work / 'delivery-receipt.json', receipt)
    snapshot = read(root)
    row = snapshot['rows'][0]
    assert row['delivery_status'] == 'complete'
    assert row['stage'] == 'attention' and snapshot['counts']['verified'] == 0
    assert '核对' in row['delivery_label'] and '已入库' not in row['delivery_label']


def test_partition_recovery_uses_its_new_request_without_parent_failure(tmp_path):
    root, _, work, _ = paper(tmp_path, stage='extracting_parts')
    put(work / 'model' / 'part_001' / 'request-state.json', dict(status='request_failed_or_uncertain'))
    put(work / 'model' / 'part_001' / 'revisions' / 'new' / 'request-state.json', dict(status='request_in_flight'))
    put(work / 'model' / 'partition-state.json', dict(status='parts_pending',
        requests=[{'id': 'part_001'}], recoveries=[dict(id='part_001', directory='part_001/revisions/new')]))
    snapshot = read(root)
    assert snapshot['rows'][0]['stage'] == 'extracting_parts'
    assert snapshot['counts']['attention'] == 0


def test_partition_recovery_cannot_read_another_request(tmp_path):
    root, _, work, _ = paper(tmp_path, stage='extracting_parts')
    put(work / 'model' / 'partition-state.json', dict(status='parts_pending',
        requests=[{'id': 'part_001'}], recoveries=[dict(id='part_001', directory='part_002')]))
    cls, error = monitor()
    with pytest.raises(error): cls(root).read('trial')
