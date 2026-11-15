"""Archive and read back one created test copy using the existing NAS writer.

The caller controls the paper ticket and calls this after group RAG success.
This function does not delete files or advance the ticket.
"""
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from task_effect_admission import TaskNotRunning


def archive_test_copy(copy_receipt_path, work_dir, nas_dir, *, admit):
    from paperflow.original_archive.archive import ArchiveReader, ArchiveStore, MAX_BYTES
    from paperflow.original_archive.artifacts import ArtifactReader
    from paperflow.original_archive.lifecycle import RegisteredWriter

    copy_receipt_path = Path(copy_receipt_path).resolve(strict=True)
    receipt_bytes = copy_receipt_path.read_bytes()
    copied = json.loads(receipt_bytes)
    if Path(copied['receipt_path']) != copy_receipt_path:
        raise ValueError('test copy receipt path differs')
    def require_admission():
        if admit() is not True:
            raise TaskNotRunning('storage task admission changed')
        return True
    require_admission()
    work = Path(work_dir)
    work.mkdir(parents=True, exist_ok=True)
    body = next(item for item in copied['files'] if item['kind'] == 'body')
    files = []
    for item in copied['files']:
        kind = '原始来源' if item is body else '正文副本' if item['kind'] == 'body' else '附件'
        files.append({'path': item['path'], 'name': item['relative'], 'kind': kind,
                      'sha256': item['sha256'], 'bytes': item['bytes']})
    registration = {'id': copied['group_id'], 'identity': {'paper_id': copied['paper_id'],
                    'receipt_id': copied['receipt_id']}, 'source_sha256': body['sha256'], 'files': files}
    registration_path = work / 'registration.json'
    if registration_path.exists():
        if json.loads(registration_path.read_bytes()) != registration:
            raise ValueError('archive work belongs to a different test group')
    else:
        with registration_path.open('x', encoding='utf8', newline='\n') as stream:
            json.dump(registration, stream, ensure_ascii=False, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
    with registration_path.open('rb') as stream:
        registration_sha = hashlib.file_digest(stream, 'sha256').hexdigest()
    copy_receipt_sha = hashlib.sha256(receipt_bytes).hexdigest()
    registered = {**registration, 'source_evidence': [
        {'path': str(registration_path), 'sha256': registration_sha},
        {'path': copied['receipt_path'], 'sha256': copy_receipt_sha}]}
    asset_id = hashlib.sha256(('sha:' + body['sha256']).encode()).hexdigest()[:32]
    row = {'asset_id': asset_id, 'title': copied['paper_id'], 'verified': True,
           'detail': {'source_hash': body['sha256'], 'attachment_contract': {
               'known': True, 'required': any(item['kind'] == 'attachment' for item in copied['files'])}},
           'registered': registered}
    store = ArchiveStore(work / 'nas.sqlite')
    writer = RegisteredWriter(store, ArtifactReader([Path(copied['receipt_path']).parent, work], work / 'readback'),
                              nas_dir, reserve_bytes=0, admit=require_admission)
    complete_path = work / 'archive-complete.json'
    saved = store.status(asset_id)
    if not complete_path.exists() and saved['status'] != 'verified':
        maximum = sum(item['bytes'] for item in files)
        if not 0 < maximum <= MAX_BYTES:
            raise ValueError('isolated archive exceeds the existing size limit')
        require_admission()
        base = {'asset_id': asset_id, 'root': str(writer.root),
                'attempted_at': datetime.now(timezone.utc).isoformat(),
                'files': [], 'warnings': [], 'local_release_enabled': False}
        store.save(asset_id, base | {'status': 'copying'})
        result = writer._copy(row, body['sha256'], maximum)
        require_admission()
        saved = base | result | {'verified_at': datetime.now(timezone.utc).isoformat()}
        store.save(asset_id, saved)
    if Path(saved['root']).resolve() != Path(nas_dir).resolve():
        raise ValueError('archive belongs to a different test destination')
    expected = {(item['name'], item['sha256'], item['bytes']) for item in files}
    observed = {(item['name'], item['sha256'], item['bytes']) for item in saved['files']}
    if saved['status'] != 'verified' or saved['warnings'] or len(saved['files']) != len(files) or observed != expected:
        raise ValueError('NAS archive does not cover the complete test group')
    reader = ArchiveReader(store, work / 'readback')
    for item in saved['files']:
        _, stream = reader.download(asset_id, item['id'])
        with stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
            size = stream.seek(0, 2)
        if (digest, size) != (item['sha256'], item['bytes']):
            raise ValueError('NAS readback does not match the test copy')
    result = {'schema': 'paperflow-isolated-test-archive/1', 'group_id': copied['group_id'],
              'paper_id': copied['paper_id'], 'receipt_id': copied['receipt_id'], 'asset_id': asset_id,
              'destination': str(nas_dir), 'manifest_sha256': saved['manifest_sha256'],
              'files': saved['files'], 'readback_verified': True}
    if complete_path.exists():
        if json.loads(complete_path.read_bytes()) != result:
            raise ValueError('completed archive receipt changed')
    else:
        with complete_path.open('x', encoding='utf8', newline='\n') as stream:
            json.dump(result, stream, ensure_ascii=False, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
    return result
