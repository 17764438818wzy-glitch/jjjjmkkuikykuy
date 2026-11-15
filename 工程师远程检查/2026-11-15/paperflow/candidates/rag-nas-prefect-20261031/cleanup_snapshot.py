"""Read-only cleanup preparation for verified synthetic or real copy receipts.

No NAS or RAG evidence is asserted here and no deletion is implemented.  A
snapshot is not permission to release a file; identities must be rechecked by
the eventual authorized cleanup operation under its real task/reference gate.
"""
import os
from pathlib import Path

from delivery_inventory import prepare_inventory


def prepare_cleanup_snapshot(receipt_path):
    from paperflow.original_archive.lifecycle import locked_file, safe_file, verify_stream
    from paperflow_prefect._release.file_identity import identity_from_stream
    from paperflow_prefect.intake import object_fingerprint

    inventory = prepare_inventory(receipt_path)
    root = (Path(inventory['delivery_ref']['path']).absolute().parent / 'files').resolve(strict=True)
    files = []
    for row in inventory['files']:
        path = safe_file(row['path'], [root])
        with locked_file(path) as (stream, _):
            verify_stream(stream, row['sha256'], row['bytes'])
            item = {key: row[key] for key in ('file_id', 'kind', 'relative', 'sha256', 'bytes')}
            item.update(path=str(path), object_identity=identity_from_stream(stream),
                        object_fingerprint=object_fingerprint(os.fstat(stream.fileno())))
            files.append(item)
    if prepare_inventory(receipt_path) != inventory:
        raise ValueError('delivery inventory changed while preparing cleanup snapshot')
    for item in files:
        path = safe_file(item['path'], [root])
        with locked_file(path) as (stream, _):
            verify_stream(stream, item['sha256'], item['bytes'])
            if (identity_from_stream(stream) != item['object_identity']
                    or object_fingerprint(os.fstat(stream.fileno())) != item['object_fingerprint']):
                raise ValueError('delivery file object changed while preparing cleanup snapshot')
    return {'schema': 'rag-nas-cleanup-snapshot-candidate/1',
            'paper_id': inventory['paper_id'], 'revision': inventory['revision'],
            'receipt_id': inventory['receipt_id'], 'receipt_sha256': inventory['receipt_sha256'],
            'delivery_ref': inventory['delivery_ref'], 'files': files,
            'rag_verified': False, 'nas_verified': False, 'release_authorized': False}
