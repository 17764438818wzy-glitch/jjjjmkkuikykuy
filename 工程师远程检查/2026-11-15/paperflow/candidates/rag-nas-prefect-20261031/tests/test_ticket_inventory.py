"""Synthetic receipt-to-folder binding; no Prefect, RAG or NAS access."""
import copy
from pathlib import Path
import sys
from uuid import uuid4

import pytest

WORKSPACE = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(WORKSPACE / 'prefect-paperflow'))
sys.path.insert(0, str(WORKSPACE / 'prefect-paperflow' / 'tests'))

from test_delivery_inventory import make_receipt


@pytest.fixture
def ticket_and_receipt(tmp_path):
    from delivery_inventory import prepare_inventory
    receipt_path, _, _ = make_receipt(tmp_path)
    inventory = prepare_inventory(receipt_path)
    ticket = {'task_id': str(uuid4()), 'group_id': inventory['group_id'],
              'paper_id': inventory['paper_id'], 'receipt_id': inventory['receipt_id'],
              'attempt_id': 'synthetic-attempt-1', 'parent_run_id': str(uuid4()),
              'task_run_id': str(uuid4())}
    return ticket, receipt_path, inventory


def test_multiple_materials_are_bound_once_to_one_folder_ticket(ticket_and_receipt):
    from delivery_inventory import prepare_ticket_inventory
    ticket, receipt_path, inventory = ticket_and_receipt
    seen = []
    def verify(identity):
        seen.append(copy.deepcopy(identity))
        identity['paper_id'] = 'mutated-by-caller'
        return True
    result = prepare_ticket_inventory(receipt_path, ticket, verify_ticket=verify)
    assert seen and all(value == ticket for value in seen)
    assert result['ticket'] == ticket
    assert result['inventory'] == inventory
    assert {row['kind'] for row in result['inventory']['files']} == {'body', 'attachment'}
    assert result['postprocess_verified'] is False
    assert result['rag_start_authorized'] is False
    assert result['archive_authorized'] is False
    assert result['release_authorized'] is False


@pytest.mark.parametrize('field,value', [
    ('group_id', 'f' * 32), ('receipt_id', 'e' * 64), ('paper_id', 'different-paper')])
def test_ticket_receipt_identity_mismatch_is_rejected(ticket_and_receipt, field, value):
    from delivery_inventory import prepare_ticket_inventory
    ticket, receipt_path, _ = ticket_and_receipt
    ticket[field] = value
    with pytest.raises(ValueError):
        prepare_ticket_inventory(receipt_path, ticket, verify_ticket=lambda _: True)


def test_ticket_verification_is_required_and_fails_closed(ticket_and_receipt):
    from delivery_inventory import prepare_ticket_inventory
    ticket, receipt_path, _ = ticket_and_receipt
    with pytest.raises((ValueError, PermissionError)):
        prepare_ticket_inventory(receipt_path, ticket, verify_ticket=lambda _: False)
    with pytest.raises((TypeError, ValueError)):
        prepare_ticket_inventory(receipt_path, ticket, verify_ticket=None)


def test_ticket_changed_during_inventory_preparation_is_rejected(ticket_and_receipt):
    from delivery_inventory import prepare_ticket_inventory
    ticket, receipt_path, _ = ticket_and_receipt
    observations = iter([True, False])
    with pytest.raises(PermissionError):
        prepare_ticket_inventory(receipt_path, ticket, verify_ticket=lambda _: next(observations))


def test_missing_folder_flowrun_identity_is_rejected(ticket_and_receipt):
    from delivery_inventory import prepare_ticket_inventory
    ticket, receipt_path, _ = ticket_and_receipt
    ticket.pop('task_run_id')
    with pytest.raises(ValueError):
        prepare_ticket_inventory(receipt_path, ticket, verify_ticket=lambda _: True)


def test_non_string_attempt_is_rejected_before_ticket_verification(ticket_and_receipt):
    from delivery_inventory import prepare_ticket_inventory
    ticket, receipt_path, _ = ticket_and_receipt
    ticket['attempt_id'] = 1
    calls = []
    with pytest.raises(ValueError):
        prepare_ticket_inventory(receipt_path, ticket, verify_ticket=lambda _: calls.append(1) or True)
    assert not calls
