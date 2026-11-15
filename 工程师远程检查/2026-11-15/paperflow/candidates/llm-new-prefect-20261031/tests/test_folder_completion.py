"""One folder ticket must include its body parts and all attachments."""
import copy
import importlib.util
from pathlib import Path

import pytest


spec = importlib.util.spec_from_file_location('folder_completion',
    Path(__file__).parent.parent / 'folder_completion.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


@pytest.fixture
def folder():
    manifest = {'task_id': 'task-1', 'group_id': 'folder-1', 'receipt_id': 'e' * 64,
                'body': {'file_id': 'body-1', 'sha256': 'a' * 64},
                'attachments': [{'file_id': 'att-1', 'sha256': 'b' * 64},
                                {'file_id': 'att-2', 'sha256': 'c' * 64}],
                'reading_material': {'sha256': 'd' * 64},
                'capacity': {'status': 'planned', 'mode': 'partitioned', 'parts': 2,
                             'plan_identity': {'schema': 'paperflow-body-input-plan/1',
                                 'source_sha256': 'a' * 64, 'reading_sha256': 'd' * 64,
                                 'plan_sha256': '5' * 64, 'coverage_sha256': '6' * 64,
                                 'mode': 'partitioned', 'part_count': 2,
                                 'parts': [{'id': 'part-001', 'request_sha256': '1' * 64},
                                           {'id': 'part-002', 'request_sha256': '2' * 64}]}}}
    ticket_id = 'folder-ticket-1'
    components = module.expected_components(manifest, ticket_id)
    rows = []
    for item in components:
        rows.append({'task_id': manifest['task_id'], 'component_id': item['component_id'],
                     'receipt_id': manifest['receipt_id'], 'task_run_id': ticket_id,
                     'kind': item['kind'], 'source_sha256': item['source_sha256'],
                     'request_sha256': item.get('request_sha256') or '3' * 64,
                     'response_sha256': '4' * 64, 'state': 'raw_complete'})
    return manifest, ticket_id, components, rows


def test_folder_waits_until_every_body_part_and_attachment_is_verified(folder):
    manifest, ticket, components, rows = folder
    checked = []
    incomplete = module.inspect_folder(manifest, ticket, rows[:-1], lambda row: checked.append(row['component_id']))
    assert incomplete['status'] == 'incomplete' and incomplete['pending_count'] == 1
    assert len(checked) == 3
    checked.clear()
    ready = module.inspect_folder(manifest, ticket, rows, lambda row: checked.append(row['component_id']))
    assert ready == {'status': 'raw_complete', 'task_id': 'task-1', 'group_id': 'folder-1',
                     'task_run_id': ticket, 'body_parts': 2, 'attachments': 2}
    assert len(checked) == len(components)


def test_unknown_or_foreign_evidence_cannot_complete_folder(folder):
    manifest, ticket, _, rows = folder
    pending = copy.deepcopy(rows)
    pending[0]['state'] = 'unknown'
    result = module.inspect_folder(manifest, ticket, pending, lambda _: None)
    assert result['status'] == 'incomplete' and result['pending_count'] == 1
    foreign = copy.deepcopy(rows)
    foreign[0]['receipt_id'] = 'different'
    with pytest.raises(ValueError, match='身份'):
        module.inspect_folder(manifest, ticket, foreign, lambda _: None)
    changed_request = copy.deepcopy(rows)
    changed_request[0]['request_sha256'] = 'f' * 64
    with pytest.raises(ValueError, match='身份'):
        module.inspect_folder(manifest, ticket, changed_request, lambda _: None)


def test_missing_or_unavailable_upstream_plan_never_means_no_body(folder):
    manifest, ticket, _, rows = folder
    manifest['capacity'] = {'status': 'unavailable'}
    with pytest.raises(ValueError, match='分段计划'):
        module.inspect_folder(manifest, ticket, rows, lambda _: None)


def test_single_body_and_no_attachments_can_finish(folder):
    manifest, ticket, _, _ = folder
    manifest['attachments'] = []
    manifest['capacity'].update(mode='single', parts=1)
    identity = manifest['capacity']['plan_identity']
    identity.update(mode='single', part_count=1, parts=[identity['parts'][0]])
    item = module.expected_components(manifest, ticket)[0]
    row = {'task_id': manifest['task_id'], 'component_id': item['component_id'],
           'receipt_id': manifest['receipt_id'], 'task_run_id': ticket, 'kind': 'body',
           'source_sha256': item['source_sha256'], 'request_sha256': item['request_sha256'],
           'response_sha256': '4' * 64, 'state': 'raw_complete'}
    assert module.inspect_folder(manifest, ticket, [row], lambda _: None)['status'] == 'raw_complete'
