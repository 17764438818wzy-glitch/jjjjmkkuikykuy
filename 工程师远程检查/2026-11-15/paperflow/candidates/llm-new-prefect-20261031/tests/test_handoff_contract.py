"""Synthetic frozen handoff checks; no service, model, or real documents."""
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest


spec = importlib.util.spec_from_file_location('handoff_contract', Path(__file__).parent.parent / 'handoff_contract.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def write_ref(path, data):
    raw = json.dumps(data, ensure_ascii=False).encode('utf8')
    path.write_bytes(raw)
    return {'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest()}


@pytest.fixture
def handoff(tmp_path):
    body = {'file_id': 'body-1', 'sha256': 'a' * 64, 'path': str(tmp_path / 'delivery.docx')}
    attachment = {'file_id': 'attachment-1', 'sha256': 'b' * 64, 'path': str(tmp_path / 'attachment.pdf')}
    paper = {'paper_id': 'paper-1', 'revision': 'revision-1', 'body': body, 'attachments': [attachment]}
    material = write_ref(tmp_path / 'material.json', {'source': {'source_sha256': body['sha256']}})
    reading = write_ref(tmp_path / 'reading.json', {'source': {'source_sha256': body['sha256']}, 'blocks': []})
    receipt = write_ref(tmp_path / 'receipt.json', {'test': 'receipt'})
    identity = {'task_id': 'task-1', 'group_id': 'group-1', 'paper_id': 'paper-1',
                'receipt_id': 'receipt-1', 'attempt_id': 'attempt-1'}
    manifest = {'schema': 'paperflow-local-preprocessing-handoff/1', 'consumer_id': 'prefect-local-preprocessing',
                **identity, 'revision': paper['revision'], 'body': body, 'attachments': [attachment],
                'receipt': receipt, 'material': material, 'reading_material': reading,
                'body_text': None, 'index': None, 'body_work': str(tmp_path), 'gaps': []}
    ref = write_ref(tmp_path / 'handoff.json', manifest)
    # The actual GET waiting list omits attempt_id. It must come separately
    # from the verified preprocessing claim / ticket, not the manifest alone.
    record = {**{k: v for k, v in identity.items() if k != 'attempt_id'},
              'manifest_path': ref['path'], 'manifest_sha256': ref['sha256']}
    verified = {**identity, 'receipt_path': receipt['path'], 'receipt_sha256': receipt['sha256'], 'paper': paper}
    return record, identity, verified, manifest


def test_reads_exact_registered_projection_without_processing(handoff):
    record, identity, verified, manifest = handoff
    calls = []
    def verify(path, receipt_id):
        calls.append((path, receipt_id))
        return verified
    result = module.load_frozen_handoff(record, identity, verify)
    assert result['manifest'] == manifest
    assert result['reading_material']['blocks'] == []
    assert calls == [(manifest['receipt']['path'], identity['receipt_id'])]
    assert not (Path(manifest['body_work']) / 'model').exists()


def test_rejects_changed_reading_material(handoff):
    record, identity, verified, manifest = handoff
    Path(manifest['reading_material']['path']).write_text('{}', encoding='utf8')
    with pytest.raises(ValueError, match='阅读材料'):
        module.load_frozen_handoff(record, identity, lambda *_: verified)


def test_rejects_receipt_or_folder_identity_mismatch(handoff):
    record, identity, verified, manifest = handoff
    wrong = dict(identity, group_id='another-group')
    with pytest.raises(ValueError, match='身份'):
        module.load_frozen_handoff(record, wrong, lambda *_: verified)
    different = dict(verified, paper=dict(verified['paper'], attachments=[]))
    with pytest.raises(ValueError, match='身份'):
        module.load_frozen_handoff(record, identity, lambda *_: different)


def test_rejects_changed_manifest_before_reading_it(handoff):
    record, identity, verified, _ = handoff
    Path(record['manifest_path']).write_text('{}', encoding='utf8')
    with pytest.raises(ValueError, match='交接'):
        module.load_frozen_handoff(record, identity, lambda *_: verified)
