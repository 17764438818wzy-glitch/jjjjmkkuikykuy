"""After the isolated sample's RAG success, archive and remove its test copy."""
import hashlib
import json
from pathlib import Path

import live_rag_selftest as app
from paperflow.original_archive.archive import ArchiveReader, ArchiveStore
from paperflow.original_archive.artifacts import ArtifactReader
from paperflow.original_archive.lifecycle import RegisteredWriter


def main():
    case = Path(__file__).resolve().parent/'live-tests/batch-309-fc240b92da874cca9a05f9630aeb073e'
    if (case/'nas-sample-intent.json').exists():
        raise ValueError('this sample already has an archive attempt to review')
    rag = json.loads((case/'rag-upload-complete.json').read_text(encoding='utf8'))
    copied = json.loads((case/'desktop-sample-copy-receipt.json').read_text(encoding='utf8'))
    receipts = json.loads((case/'rag-upload-receipts.json').read_text(encoding='utf8'))
    targets = json.loads((case/'rag-datasets-complete.json').read_text(encoding='utf8'))['targets']
    evidence = json.loads((case/('server-inspection-' + rag['job_id'] + '.evidence.json')).read_text(encoding='utf8'))
    if not (rag['group_count'] == 1 and rag['target_count'] == 3
            and rag['receipts_verified'] and rag['retrieval_verified'] and copied['copy_verified']
            and evidence['paper_id'] == copied['paper_id'] and evidence['job_id'] == rag['job_id']
            and evidence['manifest_sha256'] == rag['manifest_sha256']
            and evidence['body_result_task_identity']['source_sha256'] == copied['sha256']
            and set(receipts) == set(targets) and len(receipts) == 3):
        raise ValueError('the whole sample has not completed RAG verification')
    for code, receipt in receipts.items():
        body = receipt['body']
        identity = body['identity']
        if not (receipt['verified'] and receipt['retrieval_verified']
                and body['dataset_id'] == targets[code]['paper_dataset_id']
                and identity['paperflow_specialty'] == code
                and identity['paperflow_manifest_sha256'] == rag['manifest_sha256']
                and identity['paperflow_source_sha256'] == copied['sha256']
                and len(body['chunks']) == rag['record_count']):
            raise ValueError('a specialty receipt does not match the sample')
    test_root = Path.home()/'Desktop'/'PaperFlow-SELFTEST-309-fc240b92da874cca9a05f9630aeb073e'
    test_copy = Path(copied['test_copy'])
    if not test_copy.resolve().is_relative_to(test_root.resolve()):
        raise ValueError('the recorded copy is outside this desktop test directory')
    with test_copy.open('rb') as stream:
        if hashlib.file_digest(stream, 'sha256').hexdigest() != copied['sha256']:
            raise ValueError('the recorded test copy has changed')
    destination = (Path(r'\\zwjy-nas\paper_ai\paperflow-selftest')/
                   'batch-309-fc240b92da874cca9a05f9630aeb073e'/copied['paper_id'])
    work = case/'nas-sample-work'
    work.mkdir()
    files = [{'path': str(test_copy), 'name': test_copy.name, 'kind': '原始来源',
              'sha256': copied['sha256'], 'bytes': copied['bytes']}]
    registration = {'id': copied['paper_id'], 'identity': {'receipt_id': copied['receipt_id'],
                    'job_id': rag['job_id']}, 'source_sha256': copied['sha256'], 'files': files}
    app.save(work/'registration.json', registration)
    evidence_hash = app.digest(work/'registration.json')
    registered = {**registration, 'source_evidence': [{'path': str(work/'registration.json'),
                                                       'sha256': evidence_hash}]}
    asset_id = hashlib.sha256(('sha:' + copied['sha256']).encode()).hexdigest()[:32]
    row = {'asset_id': asset_id, 'title': copied['paper_id'], 'verified': True,
           'detail': {'source_hash': copied['sha256'], 'attachment_contract': {'known': True, 'required': False}},
           'registered': registered}
    store = ArchiveStore(work/'nas.sqlite')
    writer = RegisteredWriter(store, ArtifactReader([test_root, work], work/'readback'), destination,
                              reserve_bytes=0, admit=lambda: True)
    app.save(case/'nas-sample-intent.json', {'paper_id': copied['paper_id'], 'destination': str(destination),
                                            'test_copy': str(test_copy), 'sha256': copied['sha256']})
    result = writer.archive(row, max_bytes=copied['bytes'])
    if result['status'] != 'verified' or result['warnings'] or len(result['files']) != 1:
        raise ValueError('NAS archive has not been verified')
    item = result['files'][0]
    if (item['sha256'], item['bytes']) != (copied['sha256'], copied['bytes']):
        raise ValueError('NAS archive does not match the test copy')
    reader = ArchiveReader(store, work/'readback')
    _, stream = reader.download(asset_id, item['id'])
    with stream:
        archived_hash = hashlib.file_digest(stream, 'sha256').hexdigest()
        archived_size = stream.seek(0, 2)
    if (archived_hash, archived_size) != (copied['sha256'], copied['bytes']):
        raise ValueError('NAS readback does not match the test copy')
    app.save(case/'nas-sample-verified.json', {'paper_id': copied['paper_id'], 'asset_id': asset_id,
        'destination': str(destination), 'manifest_sha256': result['manifest_sha256'],
        'file_id': item['id'], 'sha256': archived_hash, 'bytes': archived_size})
    test_copy.unlink()
    app.save(case/'sample-finished.json', {'paper_id': copied['paper_id'], 'group_count': 1,
        'rag_targets': 3, 'nas_readback_verified': True, 'test_copy_deleted': True,
        'source_copy_retained': Path(copied['source_copy']).exists()})
    print('sample complete: 1 group; RAG 3 targets; NAS readback; test copy deleted', flush=True)


if __name__ == '__main__':
    main()
