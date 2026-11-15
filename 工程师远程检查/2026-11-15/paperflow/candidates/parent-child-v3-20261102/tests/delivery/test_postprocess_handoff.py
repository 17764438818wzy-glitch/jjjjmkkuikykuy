from importlib import import_module

import pytest


@pytest.fixture
def complete_handoff():
    identity = {'task_id': 'task-1', 'group_id': 'group-1', 'paper_id': 'paper-1',
                'receipt_id': 'receipt-1', 'attempt_id': 'attempt-1',
                'parent_run_id': 'parent-1', 'task_run_id': 'child-1'}
    paper = {'paper_id': 'paper-1', 'group_id': 'group-1',
             'body': {'file_id': 'body-1'},
             'attachments': [{'file_id': 'attach-1'}, {'file_id': 'attach-2'}]}
    receipt = {'schema': 'paperflow-postprocessing-receipt/1', 'identity': dict(identity),
               'validation_status': 'passed',
               'body': {'package_path': 'publication', 'manifest_sha256': 'body-manifest'},
               'attachments': [{'attachment_id': 'attach-1'}, {'attachment_id': 'attach-2'}]}
    return paper, identity, receipt


def test_complete_body_and_every_required_attachment_are_admitted(complete_handoff):
    module = import_module('paperflow_v3.delivery.handoff')
    paper, identity, receipt = complete_handoff
    assert module.require_complete_handoff(paper, identity, receipt) is None


@pytest.mark.parametrize('missing', ['body', 'attachment', 'other_child', 'failed_postprocess'])
def test_partial_or_wrong_paper_cannot_start_rag(complete_handoff, missing):
    module = import_module('paperflow_v3.delivery.handoff')
    paper, identity, receipt = complete_handoff
    if missing == 'body':
        receipt['body'] = None
    elif missing == 'attachment':
        receipt['attachments'].pop()
    elif missing == 'other_child':
        receipt['identity']['task_run_id'] = 'different-child'
    else:
        receipt['validation_status'] = 'failed'
    with pytest.raises(ValueError):
        module.require_complete_handoff(paper, identity, receipt)
