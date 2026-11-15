import pytest

from isolated_attachment_routes import attachment_route


@pytest.fixture
def case():
    plan = {'schema': 'paperflow-isolated-test-route-plan/1', 'run_id': 'a' * 32,
            'manifest_sha256': 'b' * 64, 'attachments_required': True,
            'routes': [{'code': '080901', 'paper_dataset_id': 'test-body-1', 'image_dataset_id': 'test-attachment-1'},
                       {'code': '080902', 'paper_dataset_id': 'test-body-2', 'image_dataset_id': 'test-attachment-2'}]}
    body = {'body_verified': True, 'binding': {'run_id': plan['run_id'], 'manifest_sha256': 'b' * 64,
            'source_sha256': 'c' * 64, 'journal_id': 'journal',
            'targets': {'080901': 'test-body-1', '080902': 'test-body-2'}}, 'targets': {}}
    for row in plan['routes']:
        code = row['code']
        body['targets'][code] = {'verified': True, 'retrieval_verified': True,
            'body': {'dataset_id': row['paper_dataset_id'], 'document_id': 'doc-' + code,
                     'identity': {'paperflow_task': 'task', 'paperflow_source_sha256': 'c' * 64,
                       'paperflow_manifest_sha256': 'b' * 64, 'paperflow_revision': '1',
                       'paperflow_specialty': code, 'paperflow_role': 'body'}}}
    folder = {'schema': 'paperflow-isolated-files-folder/1', 'status': 'verified', 'folder_id': 'test-folder',
              'binding': {'run_id': plan['run_id'], 'journal_id': 'journal'}}
    return locals()


def test_maps_each_attachment_to_its_test_library_and_one_test_folder(case):
    result = attachment_route('paper-1', 'd' * 64, case['plan'], case['body'], case['folder'])
    assert result['primary']['dataset_id'] == 'test-attachment-1'
    assert result['primary']['parent_id'] == 'test-folder'
    assert result['references'][0]['dataset_id'] == 'test-attachment-2'
    assert [row['dataset_id'] for row in result['parent_targets']] == ['test-body-1', 'test-body-2']


@pytest.mark.parametrize('change', ['no_body', 'different_run', 'different_journal', 'missing_attachment_target'])
def test_incomplete_or_mismatched_evidence_never_builds_a_route(case, change):
    if change == 'no_body':
        case['body']['body_verified'] = False
    elif change == 'different_run':
        case['folder']['binding']['run_id'] = 'wrong'
    elif change == 'different_journal':
        case['folder']['binding']['journal_id'] = 'wrong'
    else:
        del case['plan']['routes'][0]['image_dataset_id']
    with pytest.raises(ValueError):
        attachment_route('paper-1', 'd' * 64, case['plan'], case['body'], case['folder'])
