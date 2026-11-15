"""One isolated manual NAS self-test; does not run or connect any scheduler.

It archives a copy of one historical paper and retains that copy.  A RAG/NAS
combined release test requires additional verified evidence and is separate.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time
import uuid


WORKSPACE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WORKSPACE / 'prefect-paperflow'))
from paperflow_prefect.legacy import bind
from paperflow_prefect.settings import Settings

bind(Settings.load())
from paperflow.original_archive.archive import ArchiveReader, ArchiveStore
from paperflow.original_archive.artifacts import ArtifactReader
from paperflow.original_archive.lifecycle import RegisteredWriter
from paperflow.package import validate_package


HISTORY = (WORKSPACE / 'prefect-paperflow/runtime/data/papers'
           / 'PF_259e9d53736a4f6395d2d6b9/body/55a61a7021a6c12c56a52547')
NAS_SHARE = Path(r'\\zwjy-nas\paper_ai')


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def durable_json(path, data):
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('x', encoding='utf8', newline='\n') as out:
        json.dump(data, out, ensure_ascii=False, sort_keys=True, indent=2)
        out.flush()
        os.fsync(out.fileno())
    os.replace(tmp, path)


def main():
    case_id = 'body-selftest-' + time.strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:12]
    case = Path(__file__).resolve().parent / 'live-tests' / case_id
    case.mkdir(parents=True, exist_ok=False)
    (case / 'input').mkdir()
    (case / 'nas-work').mkdir()
    destination = NAS_SHARE / 'paperflow-selftest' / case_id
    report = {'schema': 'paperflow-manual-nas-selftest/1', 'case_id': case_id,
              'scope': 'isolated historical replay, no prefect', 'state': 'preparing',
              'nas_destination': str(destination), 'local_release_attempted': False}
    durable_json(case / 'report.json', report)

    source = HISTORY / 'private' / 'original.doc'
    publication = HISTORY / 'publication'
    prepared = json.loads((HISTORY / 'prepared-publication.json').read_text(encoding='utf8'))
    original_packet = validate_package(publication, prepared['task'], prepared['catalog'], strict_seal=True)
    source_hash = prepared['task']['source_sha256']
    if sha(source) != source_hash:
        raise ValueError('historical source does not match sealed task')
    for item in publication.rglob('*'):
        if item.is_symlink() or not item.resolve().is_relative_to(publication.resolve()):
            raise ValueError('historical package contains a link outside its root')
    shutil.copytree(publication, case / 'publication', symlinks=False)
    copy = case / 'input' / 'body.doc'
    shutil.copyfile(source, copy)
    if os.path.samefile(source, copy) or sha(copy) != source_hash:
        raise ValueError('test copy is not a distinct file with original bytes')
    packet = validate_package(case / 'publication', prepared['task'], prepared['catalog'], strict_seal=True)
    if packet['manifest_sha256'] != original_packet['manifest_sha256']:
        raise ValueError('test package does not match original seal')
    durable_json(case / 'prepared-publication.json', prepared)
    rows = [{'path': str(copy), 'name': 'body.doc', 'kind': '原始来源',
             'sha256': source_hash, 'bytes': copy.stat().st_size}]
    registration = {'id': case_id, 'identity': {'kind': 'historical-replay',
        'task_id': prepared['task']['task_id'], 'revision': str(prepared['task']['revision'])},
        'source_sha256': source_hash, 'files': rows}
    evidence = case / 'nas-work' / 'registration.json'
    durable_json(evidence, registration)
    evidence_hash = sha(evidence)
    registered = {**registration, 'source_evidence': [{'path': str(evidence), 'sha256': evidence_hash}]}
    asset_id = hashlib.sha256(('sha:' + source_hash).encode()).hexdigest()[:32]
    row = {'asset_id': asset_id, 'title': case_id, 'verified': True,
           'detail': {'source_hash': source_hash,
                      'attachment_contract': {'known': True, 'required': False}},
           'registered': registered}
    store = ArchiveStore(case / 'nas-work' / 'nas.sqlite')
    writer = RegisteredWriter(store, ArtifactReader([case / 'input', case / 'nas-work'],
                              case / 'nas-work' / 'readback'), destination, reserve_bytes=0,
                              admit=lambda: sha(evidence) == evidence_hash and sha(copy) == source_hash)
    report.update(state='archive_intent', manifest_sha256=packet['manifest_sha256'],
                  source_sha256=source_hash, input_bytes=copy.stat().st_size,
                  test_copy=str(copy), registration_sha256=evidence_hash)
    durable_json(case / 'report.json', report)
    try:
        if not NAS_SHARE.is_dir() or destination.exists():
            raise ValueError('NAS share unavailable or test destination already exists')
        result = writer.archive(row, max_bytes=copy.stat().st_size)
        if result['status'] != 'verified' or result['warnings'] or len(result['files']) != 1:
            raise ValueError('NAS archive is not fully verified')
        manifest_path = destination / result['manifest_path']
        if sha(manifest_path) != result['manifest_sha256']:
            raise ValueError('NAS archive manifest digest mismatch')
        manifest = json.loads(manifest_path.read_text(encoding='utf8'))
        if manifest['native_registration'] != registered:
            raise ValueError('NAS archive registration mismatch')
        archive_file = result['files'][0]
        if (archive_file['sha256'], archive_file['bytes']) != (source_hash, copy.stat().st_size):
            raise ValueError('NAS archived file identity mismatch')
        reader = ArchiveReader(store, case / 'nas-work' / 'readback')
        _, stream = reader.download(asset_id, archive_file['id'])
        with stream:
            downloaded_hash = hashlib.file_digest(stream, 'sha256').hexdigest()
            downloaded_bytes = stream.seek(0, 2)
        if (downloaded_hash, downloaded_bytes) != (source_hash, copy.stat().st_size):
            raise ValueError('NAS download bytes do not match')
        if sha(source) != source_hash or sha(copy) != source_hash:
            raise ValueError('source or test copy changed during archive')
        report.update(state='archive_verified', asset_id=asset_id,
                      archive_manifest_sha256=result['manifest_sha256'],
                      archive_file_id=archive_file['id'], downloaded_sha256=downloaded_hash,
                      downloaded_bytes=downloaded_bytes, original_preserved=True,
                      test_copy_preserved=True)
        durable_json(case / 'report.json', report)
        print(json.dumps(report, ensure_ascii=True))
    except BaseException as exc:
        report.update(state='archive_error', error_type=type(exc).__name__, error=str(exc)[:500])
        durable_json(case / 'report.json', report)
        print(json.dumps(report, ensure_ascii=True))
        raise


if __name__ == '__main__':
    main()
