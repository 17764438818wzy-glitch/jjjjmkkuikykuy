"""Delete only this run's test copies after group RAG and NAS readback."""
import hashlib
import json
import os
from pathlib import Path
import time

from paperflow.original_archive.archive import ArchiveReader, ArchiveStore
from paperflow.original_archive.lifecycle import locked_file, safe_file, verify_stream
from paperflow_prefect.records import identity, read_json, sha_file, write_json
from paperflow_prefect._release.file_identity import identity_from_stream

from effect_journal import EffectUnknown
from group_rag_evidence import read_verified_group


def cleanup_test_copy(copy_receipt_path, rag_receipt_path, archive_work, test_root, nas_dir, progress_path, *, admit):
    copy_path, rag_path = Path(copy_receipt_path).resolve(strict=True), Path(rag_receipt_path).resolve(strict=True)
    rag, copied = read_verified_group(rag_path, copy_path)
    root = Path(test_root).resolve(strict=True)
    group_root = (root / copied['group_id']).resolve(strict=True)
    if group_root.parent != root or copy_path != group_root / 'test-copy-receipt.json':
        raise ValueError('test copy receipt is outside this run')
    archive_work = Path(archive_work)
    archive_path = archive_work / 'archive-complete.json'
    archive = read_json(archive_path)
    expected = {(row['relative'], row['sha256'], row['bytes']) for row in copied['files']}
    saved = {(row['name'], row['sha256'], row['bytes']) for row in archive['files']}
    if (archive.get('schema') != 'paperflow-isolated-test-archive/1' or archive.get('readback_verified') is not True
            or any(archive[key] != copied[key] for key in ('group_id', 'paper_id', 'receipt_id'))
            or Path(archive['destination']).resolve(strict=True) != Path(nas_dir).resolve(strict=True)
            or len(archive['files']) != len(copied['files']) or saved != expected):
        raise ValueError('NAS archive does not cover the test group')
    store = ArchiveStore(archive_work / 'nas.sqlite')
    saved_archive = store.status(archive['asset_id'])
    nas_root = Path(nas_dir).resolve(strict=True)
    manifest_relative = f"papers/{archive['asset_id']}/{archive['manifest_sha256']}.json"
    if (saved_archive.get('status') != 'verified' or saved_archive.get('manifest_sha256') != archive['manifest_sha256']
            or Path(saved_archive['root']).resolve(strict=True) != nas_root
            or saved_archive.get('manifest_path') != manifest_relative):
        raise ValueError('NAS archive record is not verified')
    with locked_file(nas_root / manifest_relative) as (stream, _):
        manifest_bytes = stream.read()
    if hashlib.sha256(manifest_bytes).hexdigest() != archive['manifest_sha256']:
        raise ValueError('NAS manifest hash does not match')
    manifest = json.loads(manifest_bytes)
    manifest_files = {(row['name'], row['sha256'], row['bytes']) for row in manifest['files']}
    body_sha = next(row['sha256'] for row in copied['files'] if row['kind'] == 'body')
    if (manifest['asset_id'] != archive['asset_id'] or manifest['source_sha256'] != body_sha
            or len(manifest['files']) != len(copied['files']) or manifest_files != expected):
        raise ValueError('NAS manifest does not cover the test group')
    reader = ArchiveReader(store, archive_work / 'readback')
    for row in archive['files']:
        _, stream = reader.download(archive['asset_id'], row['id'])
        with stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
            size = stream.seek(0, 2)
        if (digest, size) != (row['sha256'], row['bytes']):
            raise ValueError('NAS readback differs from the test copy')
    binding = {'copy_receipt_sha256': sha_file(copy_path), 'rag_receipt_sha256': sha_file(rag_path),
               'archive_receipt_sha256': sha_file(archive_path), 'test_root': str(root), 'nas_dir': str(nas_dir)}
    path = Path(progress_path)
    state = read_json(path) if path.is_file() else {'schema': 'paperflow-isolated-cleanup/1',
                                                    'binding': binding, 'status': 'pending', 'files': {}}
    if state.get('binding') != binding:
        raise ValueError('cleanup evidence changed')
    for row in copied['files']:
        target = safe_file(row['path'], [group_root / 'files'])
        previous = state['files'].get(row['file_id'], {})
        if previous.get('state') == 'intent':
            raise EffectUnknown('test copy deletion outcome requires review')
        if previous.get('state') == 'deleted':
            if target.exists():
                raise ValueError('a deleted test copy reappeared')
            continue
        with locked_file(target) as (stream, _):
            verify_stream(stream, row['sha256'], row['bytes'])
            if identity_from_stream(stream) != row['object_identity']:
                raise ValueError('test copy object changed after creation')
            if os.fstat(stream.fileno()).st_nlink != 1 or target.samefile(row['source_delivery_path']):
                raise ValueError('test copy is not a separate file')
    write_json(path, state)
    for row in copied['files']:
        if state['files'].get(row['file_id'], {}).get('state') == 'deleted':
            continue
        target = safe_file(row['path'], [group_root / 'files'])
        effect = identity([copied['group_id'], row['file_id'], row['sha256'], str(target)])
        with locked_file(target, delete=True) as (stream, remove):
            verify_stream(stream, row['sha256'], row['bytes'])
            if identity_from_stream(stream) != row['object_identity']:
                raise ValueError('test copy object changed after creation')
            with admit(effect, effect) as permit:
                permit.check_current()
                def record():
                    state['files'][row['file_id']] = {'state': 'intent', 'path': str(target), 'sha256': row['sha256']}
                    write_json(path, state)
                permit.start_transport(record)
                remove()
        state['files'][row['file_id']]['state'] = 'deleted'
        state['files'][row['file_id']]['deleted_at_ns'] = time.time_ns()
        write_json(path, state)
    state.update(status='completed', group_id=copied['group_id'], completed_at_ns=time.time_ns())
    write_json(path, state)
    return state
