"""Reuse the native copy/hash/manifest core; no local release is reachable."""
import hashlib
from pathlib import Path
from .intake import verify_inventory
from .records import read_json, write_json, sha_file


def archive_originals(paper, work, destination, *, reserve_bytes=1024**3):
    from paperflow.original_archive.archive import ArchiveStore, ArchiveReader, writer_lock
    from paperflow.original_archive.artifacts import ArtifactReader
    from paperflow.original_archive.lifecycle import RegisteredWriter
    verify_inventory(paper)
    work, destination = Path(work).resolve(), Path(destination).resolve()
    person = Path(paper['person_dir']).absolute() if paper.get('delivery_ref') else Path(paper['person_dir']).resolve()
    source_root = person
    if paper.get('delivery_ref') is not None:
        from .delivery_receipt import reload_receipt_delivery
        delivery = reload_receipt_delivery(paper['delivery_ref'], verify_files=False)
        source_root = (Path(delivery['root']) / 'files').resolve()
    if any(root == destination or root.is_relative_to(destination) or destination.is_relative_to(root)
           for root in {person, source_root}):
        raise ValueError('NAS归档目录与本地个人目录不能重叠')
    work.mkdir(parents=True, exist_ok=True)
    registration_file = work / 'original-registration.json'
    registration = {'id': paper['paper_id'] + '-' + paper['revision'],
        'identity': {'paper_id': paper['paper_id'], 'revision': paper['revision']},
        'source_sha256': paper['body']['sha256'],
        'files': [{**r, 'name': r['relative'],
                   'kind': '原始来源' if r == paper['body'] else '附件'} for r in paper['archive_files']]}
    if registration_file.exists() and read_json(registration_file) != registration:
        raise ValueError('冻结归档清单改变')
    write_json(registration_file, registration)
    source_hash = paper['body']['sha256']
    asset_id = hashlib.sha256(('sha:' + source_hash).encode()).hexdigest()[:32]
    row = {'asset_id': asset_id, 'title': paper['paper_id'], 'verified': True,
           'detail': {'source_hash': source_hash, 'attachment_contract':
                      {'known': True, 'required': len(registration['files']) > 1}},
           'registered': {**registration, 'source_evidence':
                          [{'path': str(registration_file), 'sha256': sha_file(registration_file)}]}}
    store = ArchiveStore(work / 'nas.sqlite')
    sources = ArtifactReader([source_root, work], work / 'readback-temp')
    registration_hash = sha_file(registration_file)
    writer = RegisteredWriter(store, sources, destination, reserve_bytes=reserve_bytes,
                              admit=lambda: sha_file(registration_file) == registration_hash)
    # The older public archive() imposes a 512 MiB manual-batch ceiling. This
    # flow already has an exact complete inventory. Reuse its streaming core
    # with that byte budget, retaining all source, manifest and hash checks.
    with writer_lock(store.path.with_suffix('.lock')):
        previous = store.status(asset_id)
        if previous.get('root') and Path(previous['root']) != destination:
            raise ValueError('已登记NAS目标改变')
        store.save(asset_id, {'status': 'copying', 'root': str(destination), 'files': [], 'warnings': []})
        result = writer._copy(row, source_hash, max(1, sum(r['bytes'] for r in registration['files'])))
        result.update(root=str(destination), asset_id=asset_id, local_release_enabled=False)
        if result['status'] != 'verified' or result['warnings']:
            raise ValueError('NAS归档未完整核验')
        manifest_path = destination / result['manifest_path']
        if sha_file(manifest_path) != result['manifest_sha256']: raise ValueError('NAS清单哈希不符')
        manifest = read_json(manifest_path)
        expected = {(r['name'], r['sha256'], r['bytes']) for r in registration['files']}
        observed = {(r['name'], r['sha256'], r['bytes']) for r in manifest['files']}
        if expected != observed or manifest.get('native_registration') != row['registered']:
            raise ValueError('NAS文件清单与登记不符')
        store.save(asset_id, result)
        reader = ArchiveReader(store, work / 'readback-temp')
        for file in result['files']:
            _, stream = reader.download(asset_id, file['id'])
            with stream:
                if hashlib.file_digest(stream, 'sha256').hexdigest() != file['sha256']:
                    raise ValueError('NAS逐文件读回哈希不符')
    verify_inventory(paper)
    result.update(paper_id=paper['paper_id'], revision=paper['revision'], local_folder_retained=True)
    write_json(work / 'nas-receipt.json', result)
    return result
