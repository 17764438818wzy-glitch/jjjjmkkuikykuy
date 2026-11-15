"""New inventory preparation with synthetic initial-step delivery receipts."""
from pathlib import Path
import sys

import pytest

WORKSPACE = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(WORKSPACE / 'prefect-paperflow'))
sys.path.insert(0, str(WORKSPACE / 'prefect-paperflow' / 'tests'))


def make_receipt(root, *, evidence=True):
    from receipt_h21_fixture import handoff, save
    from paperflow_prefect.attachment_evidence_handoff import write_evidence_handoff
    from paperflow_prefect.group_delivery import package_result
    from paperflow_prefect.intake_handoff import receive_handoff
    from paperflow_prefect.receipts import ReceiptStore

    library = root / 'source'
    handoff_value = handoff(library, library / 'person', attachments=[('attachment.zip', b'attachment')],
                            trash=[('discard.txt', b'not for cleanup')])
    path = root / 'handoff.json'
    save(path, handoff_value)
    attachments = handoff_value['attachments']
    refs = []
    if evidence:
        row = attachments[0]
        status = lambda value, reason=None: {'status': value, 'reason': reason}
        origin = {'kind': 'current_read', 'record_ref': None, 'run_ref': None,
                  'entry_ref': None, 'record_sha256': None}
        record = {'schema': 'attachment-evidence-handoff/1', 'group_id': handoff_value['group_id'],
                  'source': {'handoff_id': handoff_value['handoff_id'],
                             'result_version': handoff_value['result_version'],
                             'file_id': row['file_id'], 'source_sha256': row['sha256']},
                  'evidence_version': 1, 'previous_evidence_version': None,
                  'read_scope': {'directory': status('not_read'), 'text': status('not_read'),
                                 'images': status('not_read'), 'request_records': status('unknown', 'fixture'),
                                 'errors': status('unknown', 'fixture')},
                  'archive_listing': {'evidence_id': None, 'status': 'not_read', 'total_entries': None,
                                      'listed_entries': 0, 'entries': [], 'reason': 'fixture', 'provenance': None},
                  'text_samples': [], 'images': [], 'decision_basis': {'provenance': origin},
                  'limitations': [], 'errors': [], 'existing_identification': None, 'request_records': []}
        refs.append(str(write_evidence_handoff(record, root / 'evidence', handoff=handoff_value)))
    delivery = package_result({'handoff': str(path), 'evidence_handoffs': refs}, root / 'output')
    result = receive_handoff(store=ReceiptStore(root / 'receipt-store'), delivery_dir=delivery['delivery_dir'])
    return result['record_path'], handoff_value, Path(delivery['delivery_dir'])


def test_inventory_uses_only_received_body_and_attachment_delivery_copies(tmp_path):
    from delivery_inventory import prepare_inventory
    receipt, handoff_value, delivery = make_receipt(tmp_path)
    inventory = prepare_inventory(receipt)
    assert inventory['status'] == 'ready'
    assert {row['kind'] for row in inventory['files']} == {'body', 'attachment'}
    assert len(inventory['files']) == 2
    assert all(Path(row['path']).is_relative_to(delivery / 'files') for row in inventory['files'])
    assert all(Path(row['path']) != Path(row['source_path']) for row in inventory['files'])
    assert Path(handoff_value['body']['path']).exists()
    assert Path(handoff_value['trash'][0]['path']).exists()


def test_inventory_stops_when_attachment_evidence_is_missing(tmp_path):
    from delivery_inventory import prepare_inventory
    receipt, _, _ = make_receipt(tmp_path, evidence=False)
    with pytest.raises(ValueError):
        prepare_inventory(receipt)


def test_inventory_stops_when_a_delivery_copy_changes(tmp_path):
    from delivery_inventory import prepare_inventory
    receipt, _, delivery = make_receipt(tmp_path)
    (delivery / 'files' / 'attachment.zip').write_bytes(b'changed')
    with pytest.raises(ValueError):
        prepare_inventory(receipt)


def test_inventory_rechecks_sidecar_after_reading_members(tmp_path, monkeypatch):
    from delivery_inventory import prepare_inventory
    from paperflow_prefect import intake_handoff
    receipt, _, delivery = make_receipt(tmp_path)
    original = intake_handoff.verify_received_file
    calls = []
    def remove_after_initial_receipt(*args, **kwargs):
        result = original(*args, **kwargs)
        calls.append(1)
        if len(calls) == 3:
            next((delivery / 'attachment-evidence').rglob('1.json')).unlink()
        return result
    monkeypatch.setattr(intake_handoff, 'verify_received_file', remove_after_initial_receipt)
    with pytest.raises(ValueError):
        prepare_inventory(receipt)
