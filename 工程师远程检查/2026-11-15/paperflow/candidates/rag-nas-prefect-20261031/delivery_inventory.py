"""Prepare a read-only inventory from an existing verified delivery receipt.

This candidate does not schedule, publish, archive, or delete.  It validates
the first-step copy receipt only; it is not a postprocess or RAG completion
receipt, nor does it authorize releasing any file.
"""
import os
from pathlib import Path


def _path_key(value):
    return os.path.normcase(os.path.normpath(os.fspath(value)))


def prepare_inventory(receipt_path):
    # Reuse the receipt reader and original copy-verification functions without
    # invoking their old stage runner or scheduler.
    from paperflow_prefect.stage3_handoff import read_receipt
    from paperflow_prefect.intake_handoff import verify_received_file
    from paperflow_prefect.records import read_json, sha_file
    from paperflow_prefect.group_delivery import _inside

    path = Path(receipt_path).absolute()
    receipt_hash = sha_file(path)
    receipt, paper = read_receipt(path)
    delivery = receipt.get('delivery') or {}
    reference = receipt.get('delivery_ref') or {}
    handoff = receipt.get('handoff_ref') or {}
    if (receipt.get('schema') != 'paperflow-receipt/1' or receipt.get('state') != '已接收'
            or not isinstance(paper, dict) or not paper.get('body')
            or delivery.get('ready_for_receipt') is not True
            or delivery.get('files_complete') is not True
            or delivery.get('file_errors') or delivery.get('evidence_errors')
            or not reference.get('path') or not reference.get('sha256')
            or not handoff.get('path') or not handoff.get('sha256')):
        raise ValueError('complete verified delivery receipt is required')
    if (sha_file(reference['path']) != reference['sha256']
            or sha_file(handoff['path']) != handoff['sha256']):
        raise ValueError('delivery or handoff evidence changed')
    files_root = Path(reference['path']).absolute().parent / 'files'
    rows = paper.get('archive_files') or []
    if (not rows or rows != [*paper.get('body_copies', []), *paper.get('attachments', [])]
            or paper['body'] not in paper.get('body_copies', [])):
        raise ValueError('received body and attachment membership is incomplete')
    files, seen = [], set()
    for row in rows:
        if row.get('kind') not in {'body', 'attachment'} or row.get('unverified_evidence'):
            raise ValueError('received member is not eligible for an inventory')
        verify_received_file(row)
        relative = row['relative']
        expected = files_root / relative
        actual = row.get('delivered_path')
        if (not actual or not row.get('source_path')
                or _path_key(row['path']) != _path_key(actual)
                or _path_key(actual) != _path_key(expected)
                or _path_key(actual) == _path_key(row['source_path'])
                or _path_key(actual) in seen):
            raise ValueError('member is not the unique declared delivery copy')
        seen.add(_path_key(actual))
        files.append({key: row[key] for key in ('file_id', 'kind', 'relative', 'sha256', 'bytes', 'source_path')}
                     | {'path': str(Path(actual).absolute())})
    # Re-read the full evidence chain after the members: the intake verifier
    # can otherwise report a copy while an attachment sidecar has disappeared.
    again, current_paper = read_receipt(path)
    if again != receipt or current_paper != paper:
        raise ValueError('delivery receipt or attachment evidence changed')
    delivery_root = Path(reference['path']).absolute().parent
    sidecars = []
    for item in read_json(reference['path']).get('evidence_records', []):
        sidecar = _inside(delivery_root, item['path'])
        if sha_file(sidecar) != item['sha256']:
            raise ValueError('attachment evidence changed')
        sidecars.append({'path': str(sidecar), 'sha256': item['sha256']})
    if (sha_file(path) != receipt_hash or sha_file(reference['path']) != reference['sha256']
            or sha_file(handoff['path']) != handoff['sha256']):
        raise ValueError('receipt evidence changed while preparing inventory')
    return {'schema': 'rag-nas-delivery-inventory-candidate/1', 'status': 'ready',
            'paper_id': paper['paper_id'], 'revision': paper['revision'],
            'group_id': receipt['group_id'], 'handoff_id': receipt['handoff_id'],
            'receipt_id': receipt['receipt_id'], 'receipt_path': str(path), 'receipt_sha256': receipt_hash,
            'delivery_ref': dict(reference), 'handoff_ref': dict(handoff), 'sidecars': sidecars, 'files': files,
            'archive_authorized': False, 'release_authorized': False}


def prepare_ticket_inventory(receipt_path, ticket, *, verify_ticket):
    """Bind one delivery inventory to one caller-verified folder ticket.

    The caller's verifier is not provided here: it must eventually verify the
    real folder FlowRun and its parent/control state. This read-only candidate
    does not prove postprocessing readiness or grant an operational permit.
    """
    from paperflow_prefect.llm_queue.folder_ledger import _identity

    identity = _identity(ticket)
    if any(not isinstance(value, str) or not value.strip() for value in identity.values()):
        raise ValueError('folder ticket identity fields must be nonempty strings')
    if not callable(verify_ticket):
        raise ValueError('an explicit folder ticket verifier is required')
    if verify_ticket(dict(identity)) is not True:
        raise PermissionError('folder ticket identity cannot be verified')
    inventory = prepare_inventory(receipt_path)
    if any(inventory[key] != identity[key] for key in ('group_id', 'paper_id', 'receipt_id')):
        raise ValueError('received delivery belongs to another folder ticket')
    if verify_ticket(dict(identity)) is not True:
        raise PermissionError('folder ticket changed while preparing inventory')
    return {'schema': 'rag-nas-ticket-inventory-candidate/1',
            'ticket': dict(identity), 'inventory': inventory,
            'ticket_verification': 'caller_supplied', 'postprocess_verified': False,
            'rag_start_authorized': False, 'archive_authorized': False, 'release_authorized': False}
