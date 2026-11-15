"""Read the completed group receipt and the evidence it names."""
from paperflow_prefect.records import read_json, sha_file


def read_verified_group(rag_path, copy_path):
    rag, copied = read_json(rag_path), read_json(copy_path)
    if (rag.get('schema') != 'paperflow-isolated-group-rag/1' or rag.get('status') != 'verified'
            or rag.get('group_count') != 1 or copied.get('schema') != 'paperflow-isolated-test-copy/1'
            or rag['binding']['copy_receipt_sha256'] != sha_file(copy_path)
            or any(rag['binding']['ticket'][key] != copied[key] for key in ('group_id', 'paper_id', 'receipt_id'))):
        raise ValueError('RAG has not verified the complete test group')
    body_path = rag['body_receipt_path']
    body = read_json(body_path)
    body_rows = [row for row in copied['files'] if row['kind'] == 'body']
    if (sha_file(body_path) != rag['body_receipt_sha256'] or body.get('body_verified') is not True
            or len(body_rows) != 1 or body['binding']['source_sha256'] != body_rows[0]['sha256']
            or body['binding']['manifest_sha256'] != rag['binding']['manifest_sha256']
            or any(body['binding'][key] != rag['binding'][key] for key in ('run_id', 'journal_id'))):
        raise ValueError('RAG body receipt is not verified')
    expected = {row['file_id']: row['sha256'] for row in copied['files'] if row['kind'] == 'attachment'}
    if {row['attachment_id']: row['source_sha256'] for row in rag['attachments']} != expected:
        raise ValueError('RAG attachment receipts do not cover the group')
    for row in rag['attachments']:
        attachment = read_json(row['receipt_path'])
        if (sha_file(row['receipt_path']) != row['receipt_sha256'] or attachment.get('status') != 'verified'
                or attachment['binding']['attachment_id'] != row['attachment_id']
                or attachment['binding']['source_sha256'] != row['source_sha256']
                or attachment['journal_id'] != rag['binding']['journal_id']
                or attachment.get('current_md_sha256') != row['md_sha256']):
            raise ValueError('RAG attachment receipt is not verified')
    return rag, copied
