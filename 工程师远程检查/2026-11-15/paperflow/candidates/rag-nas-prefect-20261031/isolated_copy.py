"""Create one disposable paper-group copy from an already verified inventory."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile


def create_test_copy(inventory, test_root):
    from paperflow_prefect._release.file_identity import identity_from_stream
    test_root = Path(test_root).resolve()
    test_root.mkdir(parents=True, exist_ok=True)
    destination = test_root / inventory['group_id']
    if destination.exists():
        raise FileExistsError(destination)
    files = []
    with tempfile.TemporaryDirectory(prefix='.paperflow-copy-', dir=test_root) as temporary:
        payload = Path(temporary) / 'payload'
        payload.mkdir()
        for row in inventory['files']:
            relative = Path('files') / row['relative']
            target = payload / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            with Path(row['path']).open('rb') as source, target.open('xb') as copied:
                shutil.copyfileobj(source, copied)
            with target.open('rb') as copied:
                digest = hashlib.file_digest(copied, 'sha256').hexdigest()
                object_identity = identity_from_stream(copied)
            if (digest, target.stat().st_size) != (row['sha256'], row['bytes']):
                raise ValueError('delivered file changed while creating the test copy')
            files.append({key: row[key] for key in ('file_id', 'kind', 'relative', 'sha256', 'bytes')}
                         | {'source_delivery_path': row['path'], 'path': str(destination / relative),
                            'object_identity': object_identity})
        receipt = {'schema': 'paperflow-isolated-test-copy/1', 'group_id': inventory['group_id'],
                   'paper_id': inventory['paper_id'], 'receipt_id': inventory['receipt_id'],
                   'files': files, 'receipt_path': str(destination / 'test-copy-receipt.json')}
        with (payload / 'test-copy-receipt.json').open('x', encoding='utf8', newline='\n') as stream:
            json.dump(receipt, stream, ensure_ascii=False, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        payload.rename(destination)
    return receipt
