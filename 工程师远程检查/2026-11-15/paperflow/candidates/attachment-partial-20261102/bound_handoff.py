"""Bind one inspected server handoff to the original folder reservation."""
import hashlib
import json

from group_stage import FIELDS


def bind_handoff(stage, artifact_map, receipt, local, body_result_bytes):
    ticket = {key: stage[key] for key in FIELDS}
    job = artifact_map['job']
    digest = hashlib.sha256(json.dumps(receipt, ensure_ascii=False, sort_keys=True,
                                       separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    if (artifact_map.get('schema') != 'paperflow-linux-artifact-map/1'
            or artifact_map.get('integrity_status') != 'passed' or artifact_map['identity'] != ticket
            or artifact_map['receipt_canonical_sha256'] != stage['postprocess_receipt_sha256']
            or digest != stage['postprocess_receipt_sha256']
            or job['job_id'] != stage['postprocess_job_id'] or job['claim_id'] != stage['claim_id']
            or job['generation'] != stage['source_generation']
            or job['manifest_sha256'] != stage['upstream_manifest_sha256']
            or receipt.get('schema') != 'paperflow-postprocessing-receipt/1'
            or receipt.get('validation_status') != 'passed' or receipt['identity'] != ticket
            or receipt['claim_id'] != stage['claim_id']):
        raise ValueError('server handoff does not belong to this folder reservation')
    evidence = [row for row in artifact_map['files']
                if row['path'].replace('\\', '/').endswith('/body/body-result.json')]
    if (len(evidence) != 1 or len(body_result_bytes) != evidence[0]['bytes']
            or hashlib.sha256(body_result_bytes).hexdigest() != evidence[0]['sha256']):
        raise ValueError('body processing evidence does not match the accepted handoff')
    prepared = json.loads(body_result_bytes)
    if (prepared.get('schema') != 'body-postprocess-result/1'
            or prepared.get('status') != 'body_package_verified' or prepared['identity'] != ticket
            or prepared['manifest_sha256'] != receipt['body']['manifest_sha256']):
        raise ValueError('body package does not match the original receipt')
    original = {row['attachment_id']: row for row in receipt['attachments']}
    accepted = {row['attachment_id']: row for row in artifact_map['attachments']}
    fetched = {row['attachment_id']: row for row in local['attachments']}
    if (len(original) != len(receipt['attachments']) or len(accepted) != len(artifact_map['attachments'])
            or len(fetched) != len(local['attachments']) or set(original) != set(accepted) or set(original) != set(fetched)):
        raise ValueError('attachment handoff is incomplete')
    attachments, privacy = [], []
    for attachment_id, row in original.items():
        observed, paths = accepted[attachment_id], fetched[attachment_id]
        if (row['paper_id'] != ticket['paper_id'] or row['source_sha256'] != observed['source']['sha256']
                or row['md_sha256'] != observed['markdown']['sha256']):
            raise ValueError('attachment handoff identity changed')
        attachments.append({**row, 'source_path': str(paths['source_path']), 'md_path': str(paths['markdown_path'])})
        privacy.append(observed['reported_privacy_redaction'])
    return {'ticket': ticket, 'prepared': prepared, 'body_path': local['body_path'],
            'attachments': attachments, 'failed_attachments': receipt.get('failed_attachments', []),
            'reported_privacy_redaction': privacy}
