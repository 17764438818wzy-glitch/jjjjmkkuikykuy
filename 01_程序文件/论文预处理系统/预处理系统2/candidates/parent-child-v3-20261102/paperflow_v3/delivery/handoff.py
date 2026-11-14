"""Admit the existing whole-paper postprocessing receipt for publication."""


def require_complete_handoff(paper, identity, receipt):
    body = receipt.get('body') or {}
    expected = [row['file_id'] for row in paper['attachments']]
    delivered = [row['attachment_id'] for row in receipt.get('attachments', [])]
    if (receipt.get('schema') != 'paperflow-postprocessing-receipt/1'
            or receipt.get('validation_status') != 'passed'
            or receipt.get('identity') != identity
            or paper['paper_id'] != identity['paper_id']
            or paper['group_id'] != identity['group_id']
            or not paper.get('body')
            or not body.get('package_path') or not body.get('manifest_sha256')
            or len(expected) != len(set(expected))
            or len(delivered) != len(set(delivered))
            or set(expected) != set(delivered)):
        raise ValueError('complete body and required attachment handoff is required')
