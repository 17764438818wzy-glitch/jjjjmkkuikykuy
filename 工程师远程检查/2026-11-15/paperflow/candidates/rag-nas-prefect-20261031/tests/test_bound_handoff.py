import hashlib
import json
from pathlib import Path

import pytest

from bound_handoff import bind_handoff


@pytest.fixture
def case(tmp_path):
    ticket = {'task_id': 'task', 'group_id': 'group', 'paper_id': 'paper', 'receipt_id': 'receipt',
              'attempt_id': 'attempt', 'parent_run_id': 'parent', 'task_run_id': 'folder'}
    stage = {**ticket, 'postprocess_job_id': 'job', 'claim_id': 'claim', 'source_generation': 4,
             'upstream_manifest_sha256': 'a' * 64, 'postprocess_receipt_sha256': 'b' * 64}
    prepared = {'schema': 'body-postprocess-result/1', 'status': 'body_package_verified',
                'identity': ticket, 'manifest_sha256': 'c' * 64, 'task': {'source_sha256': 'd' * 64},
                'validation': {'embedding': {}}}
    raw = json.dumps(prepared).encode()
    artifact = {'schema': 'paperflow-linux-artifact-map/1', 'identity': ticket,
                'receipt_canonical_sha256': 'b' * 64, 'integrity_status': 'passed',
                'job': {'job_id': 'job', 'claim_id': 'claim', 'generation': 4, 'manifest_sha256': 'a' * 64},
                'files': [{'path': r'F:\pfwire\job\body\body-result.json',
                           'linux_path': '/job/body/body-result.json', 'bytes': len(raw),
                           'sha256': hashlib.sha256(raw).hexdigest()}],
                'attachments': [{'attachment_id': 'att', 'source': {'sha256': 'e' * 64},
                                 'markdown': {'sha256': 'f' * 64}, 'reported_privacy_redaction': 'not_performed'}]}
    receipt = {'schema': 'paperflow-postprocessing-receipt/1', 'validation_status': 'passed',
               'identity': ticket, 'claim_id': 'claim', 'body': {'manifest_sha256': 'c' * 64},
               'attachments': [{'paper_id': 'paper', 'attachment_id': 'att', 'source_path': 'old',
                                'source_sha256': 'e' * 64, 'md_path': 'old', 'md_sha256': 'f' * 64,
                                'delivery_version': '1' * 64}]}
    receipt_sha = hashlib.sha256(json.dumps(receipt, ensure_ascii=False, sort_keys=True,
                                           separators=(',', ':')).encode()).hexdigest()
    stage['postprocess_receipt_sha256'] = artifact['receipt_canonical_sha256'] = receipt_sha
    local = {'body_path': tmp_path / 'body', 'attachments': [{'attachment_id': 'att',
             'markdown_path': tmp_path / 'processed.md', 'source_path': tmp_path / 'att.pdf'}]}
    def run(): return bind_handoff(stage, artifact, receipt, local, raw)
    return locals()


def test_binds_original_job_and_receipt_to_the_existing_folder(case):
    result = case['run']()
    assert result['prepared']['task']['source_sha256'] == 'd' * 64
    assert result['attachments'][0]['source_path'] == str(case['tmp_path'] / 'att.pdf')
    assert result['attachments'][0]['md_path'] == str(case['tmp_path'] / 'processed.md')
    assert result['reported_privacy_redaction'] == ['not_performed']


@pytest.mark.parametrize('change', ['job', 'generation', 'receipt', 'body', 'attachment'])
def test_another_job_or_changed_handoff_cannot_be_used_for_the_folder(case, change):
    if change == 'job': case['artifact']['job']['job_id'] = 'another-job'
    elif change == 'generation': case['artifact']['job']['generation'] = 3
    elif change == 'receipt': case['artifact']['receipt_canonical_sha256'] = '0' * 64
    elif change == 'body': case['receipt']['body']['manifest_sha256'] = '0' * 64
    else: case['receipt']['attachments'][0]['md_sha256'] = '0' * 64
    with pytest.raises(ValueError): case['run']()
