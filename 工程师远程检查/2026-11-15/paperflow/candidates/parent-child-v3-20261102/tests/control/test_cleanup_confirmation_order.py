import pytest

from paperflow_v3.control.folder_status import folder_status
from paperflow_v3.delivery.progress import merge_records
from paperflow_v3.control.dashboard_cache import aggregate, compact_folder

IDENTITY = {'task_id': 'task', 'group_id': 'group', 'paper_id': 'paper', 'receipt_id': 'receipt',
            'attempt_id': 'child', 'parent_run_id': 'task', 'task_run_id': 'child'}

def row(kind, data, stamp, **flags):
    return {'kind': kind, 'data': data, 'recorded_at': stamp,
            'task_id': 'task', 'folder_id': 'group', **flags}

def upstream():
    return [row('postprocessing_complete', {'validation_status': 'passed'}, '2026-11-05T00:00:03+00:00'),
            row('llm_folder_complete', {'status': 'raw_complete', 'ranking_complete': True}, '2026-11-05T00:00:02+00:00'),
            row('preprocessing_complete', {}, '2026-11-05T00:00:01+00:00')]

def local_cleanup(status='complete'):
    return row('cleanup', {'identity': IDENTITY, 'status': status,
                          'files': {'source': {'status': 'complete' if status == 'complete' else 'unknown'}}},
               '2026-11-05T00:50:20.427181+00:00', local_only=True)

def remote_cleanup(status='complete'):
    return row('cleanup', {'identity': IDENTITY, 'status': status, 'files': {}},
               '2026-11-05T00:50:20.285615+00:00', folder_completion=True)

@pytest.mark.parametrize('local_is_later', [True, False])
def test_server_confirmed_cleanup_remains_completed_with_local_details(local_is_later):
    local = local_cleanup()
    if not local_is_later:
        local['recorded_at'] = '2026-11-05T00:50:20.100000+00:00'
    records = merge_records([local], [remote_cleanup(), *upstream()])
    result = folder_status(records, identity=IDENTITY)
    assert result['stage'] == 'completed'
    assert result['cleanup_status'] == 'complete'
    folder = compact_folder({'ticket_id': 'child', 'folder_id': 'group', **result,
                             'rag_result': 'passed', 'nas_result': 'passed', 'cleanup_result': 'passed',
                             'stage_snapshots': {}})
    summary = aggregate([folder])
    assert summary['counts']['completed'] == summary['results']['cleanup']['passed'] == 1

def test_local_cleanup_without_server_confirmation_remains_unresolved():
    result = folder_status(merge_records([local_cleanup()], upstream()), identity=IDENTITY)
    assert result['stage'] == 'unresolved'
    assert result['cleanup_status'] is None

def test_unconfirmed_remote_cleanup_does_not_confirm_local_success():
    records = merge_records([local_cleanup()], [remote_cleanup('incomplete'), *upstream()])
    assert folder_status(records, identity=IDENTITY)['stage'] != 'completed'

def test_later_unknown_local_cleanup_still_requires_verification():
    records = merge_records([local_cleanup('incomplete')], [remote_cleanup(), *upstream()])
    assert folder_status(records, identity=IDENTITY)['stage'] == 'unresolved'

def test_server_cleanup_requires_upstream_success():
    records = merge_records([local_cleanup()], [remote_cleanup()])
    assert folder_status(records, identity=IDENTITY)['stage'] != 'completed'
