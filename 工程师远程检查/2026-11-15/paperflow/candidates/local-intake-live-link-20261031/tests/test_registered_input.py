from pathlib import Path
import json
import subprocess
import sys

import pytest


CANDIDATES = Path(__file__).resolve().parents[2]
UPSTREAM = CANDIDATES / 'first-step-control-20261029'
FIXTURES = CANDIDATES / 'first-step-auto-registration-20261030' / 'tests'
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture
def registered(tmp_path):
    script = '''
import json, sys
from pathlib import Path
sys.path[:0] = sys.argv[1:3]
from paperflow_prefect.first_step_registration import register_delivery
from test_first_step_registration import make_delivery
base = Path(sys.argv[3])
group, delivery = make_delivery(base, attachment=True)
root = base / 'receipts'
receipt = register_delivery(delivery['delivery_dir'], receipt_root=root,
                            history_roots=(), lock_path=base / 'lock' / 'receipt.lock')
item = dict(status='passed', registration_status='registered', delivery_complete=True,
            group_id=receipt['group_id'], paper_id=receipt['paper_id'],
            receipt_id=receipt['receipt_id'], receipt_path=receipt['record_path'],
            delivery_dir=delivery['delivery_dir'])
print(json.dumps(dict(group=str(group), root=str(root), receipt=receipt, item=item), ensure_ascii=False))
'''
    result = subprocess.run(
        [sys.executable, '-B', '-X', 'utf8', '-c', script, str(UPSTREAM), str(FIXTURES), str(tmp_path)],
        capture_output=True, text=True, encoding='utf8', timeout=30,
        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    return Path(data['group']), Path(data['root']), data['receipt'], data['item']


def test_registered_body_and_attachment_preserve_upstream_identity(registered):
    from local_handoff import read_registered_item

    group, root, receipt, item = registered
    original_receipt = Path(item['receipt_path']).read_bytes()
    first = read_registered_item(item, receipt_root=root)
    second = read_registered_item(item, receipt_root=root)

    assert first == second
    assert first['paper_id'] == receipt['paper_id']
    assert first['receipt_id'] == receipt['receipt_id']
    assert first['receipt_schema'] == 'paperflow-receipt/1'
    assert first['body_available'] is True
    assert first['paper']['body']['file_id'] == receipt['inventory']['body']['file_id']
    assert len(first['paper']['attachments']) == 1
    for entry in [first['paper']['body'], *first['paper']['attachments']]:
        assert Path(entry['path']).is_relative_to(Path(item['delivery_dir']) / 'files')
        assert Path(entry['source_path']).is_relative_to(group)
        assert entry['path'] == entry['delivered_path']
    assert Path(item['receipt_path']).read_bytes() == original_receipt


@pytest.mark.parametrize('change', [
    {'status': 'unrecognized'},
    {'registration_status': 'failed'},
    {'delivery_complete': False},
])
def test_nonregistered_input_is_rejected_before_consumption(registered, change):
    from local_handoff import read_registered_item

    _, root, _, item = registered
    with pytest.raises(ValueError, match='登记'):
        read_registered_item({**item, **change}, receipt_root=root)


@pytest.mark.parametrize('field,value', [
    ('group_id', 'wrong-group'),
    ('paper_id', 'PF_wrong'),
    ('receipt_id', '0' * 64),
    ('delivery_dir', 'wrong-delivery'),
])
def test_mismatched_identity_or_delivery_is_rejected(registered, field, value):
    from local_handoff import read_registered_item

    _, root, _, item = registered
    with pytest.raises(ValueError):
        read_registered_item({**item, field: value}, receipt_root=root)


def test_receipt_must_be_within_declared_registration_root(registered, tmp_path):
    from local_handoff import read_registered_item

    _, root, _, item = registered
    outside = tmp_path / 'outside' / Path(item['receipt_path']).name
    outside.parent.mkdir()
    outside.write_bytes(Path(item['receipt_path']).read_bytes())
    with pytest.raises(ValueError, match='回执'):
        read_registered_item({**item, 'receipt_path': str(outside)}, receipt_root=root)


def test_missing_source_still_uses_verified_delivery_copy(registered):
    from local_handoff import read_registered_item

    group, root, receipt, item = registered
    group.rename(group.with_name('source-moved-away'))
    result = read_registered_item(item, receipt_root=root)
    assert result['body_available'] is True
    assert result['paper']['body']['file_id'] == receipt['inventory']['body']['file_id']
    assert len(result['paper']['attachments']) == 1


def test_reader_does_not_replace_previously_loaded_old_package(registered):
    _, root, _, item = registered
    script = '''
import importlib, json, sys
sys.path[:0] = sys.argv[1:3]
old = importlib.import_module('paperflow_prefect.stage3_handoff')
from local_handoff import read_registered_item
data = json.loads(sys.stdin.read())
result = read_registered_item(data['item'], receipt_root=data['root'])
assert result['body_available'] and len(result['paper']['attachments']) == 1
assert sys.modules['paperflow_prefect.stage3_handoff'] is old
'''
    result = subprocess.run(
        [sys.executable, '-B', '-X', 'utf8', '-c', script,
         str(CANDIDATES.parent / 'prefect-paperflow'), str(Path(__file__).resolve().parents[1])],
        input=json.dumps({'item': item, 'root': str(root)}, ensure_ascii=False),
        capture_output=True, text=True, encoding='utf8', timeout=30,
        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    assert result.returncode == 0, result.stderr


def test_damaged_attachment_does_not_erase_verified_body(registered):
    from local_handoff import read_registered_item

    _, root, receipt, item = registered
    old_receipt = Path(item['receipt_path']).read_bytes()
    attachment = next(row for row in receipt['items'] if row['kind'] == 'attachment')
    Path(attachment['path']).write_bytes(b'damaged test copy')

    result = read_registered_item(item, receipt_root=root)
    assert result['receipt_schema'] == 'paperflow-receipt-observation/1'
    assert result['body_available'] is True
    assert result['paper']['body']['file_id'] == receipt['inventory']['body']['file_id']
    assert result['paper']['attachments'] == []
    assert result['observation']['file_errors']
    assert Path(item['receipt_path']).read_bytes() == old_receipt


def test_damaged_attachment_evidence_does_not_erase_verified_body(registered):
    from local_handoff import read_registered_item

    _, root, receipt, item = registered
    attachment = receipt['inventory']['attachments'][0]
    assert attachment['evidence_records']
    Path(attachment['evidence_records'][0]['path']).write_text('{}', encoding='utf8')
    result = read_registered_item(item, receipt_root=root)
    assert result['body_available'] is True
    assert result['receipt_schema'] == 'paperflow-receipt-observation/1'
    assert result['observation']['evidence_errors']
    assert result['paper']['attachments'][0]['unverified_evidence']


def test_damaged_body_is_not_reported_as_available(registered):
    from local_handoff import read_registered_item

    _, root, receipt, item = registered
    Path(receipt['inventory']['body']['path']).write_bytes(b'damaged test body')
    result = read_registered_item(item, receipt_root=root)
    assert result['body_available'] is False
    assert result['paper']['body'] is None
    assert len(result['paper']['attachments']) == 1
    assert result['observation']['file_errors']
