"""Evidence-bound cleanup scope; no model, uploader or broad source traversal."""
from contextlib import contextmanager, ExitStack
import hashlib
import json
import re
from pathlib import Path
from .intake import object_fingerprint, paper_identity
from .records import Records, identity, read_json, sha_file


def validate_scope(paper, library):
    from paperflow.original_archive.lifecycle import safe_file
    library = Path(library).resolve(strict=True)
    h21 = paper.get('receipt_schema') == 'paperflow-receipt/1'
    if (Path(paper['library_root']) != library or
            (not h21 and paper_identity(paper['person_dir'], library) != paper['paper_id'])):
        raise ValueError('cleanup library or paper identity differs')
    person = safe_file(paper['person_dir'], [library])
    if person == library or not person.is_dir():
        raise ValueError('cleanup person root must be a distinct existing directory')
    if h21:
        if not re.fullmatch(r'PF_[A-Za-z0-9_-]{1,128}', paper['paper_id']):
            raise ValueError('invalid formal paper identity')
        expected_revision = identity([(r['kind'], r['file_id'], r['sha256']) for r in paper['archive_files']])[:24]
        members = [*paper['body_copies'], *paper['attachments']]
        if paper['body'] not in paper['body_copies'] or members != paper['archive_files']:
            raise ValueError('body and attachment membership differs')
    else:
        expected_revision = identity([(r['relative'], r['sha256'], r['bytes']) for r in paper['archive_files']])[:24]
    if paper['revision'] != expected_revision or not paper['archive_files']:
        raise ValueError('frozen revision differs')
    seen = set()
    for row in paper['archive_files']:
        path = safe_file(row['path'], [library])
        relative = Path(row['relative'])
        kind = {'body': '正文', 'attachment': '附件'}.get(row['kind'])
        if (not kind or relative.is_absolute() or not relative.parts or '..' in relative.parts or
                (not h21 and (len(relative.parts) < 2 or relative.parts[0] != kind)) or
                path != person / relative or str(path).casefold() in seen):
            raise ValueError('cleanup only accepts exact frozen body/attachment paths')
        seen.add(str(path).casefold())
    return person


def capture_groups(paper, *, extra_files=()):
    from paperflow.original_archive.lifecycle import locked_file, verify_stream
    from ._release.file_identity import identity_from_stream
    import os
    groups = {}
    for row in [*paper['archive_files'], *extra_files]:
        path = Path(row['path'])
        with locked_file(path) as (stream, _):
            info = os.fstat(stream.fileno())
            if row.get('object_fingerprint') != object_fingerprint(info):
                raise ValueError('file object changed since intake; new review required')
            verify_stream(stream, row['sha256'], row['bytes'])
            file_identity = identity_from_stream(stream)
            key = identity(file_identity)
            group = groups.setdefault(key, {'sha256': row['sha256'], 'bytes': row['bytes'],
                'device': info.st_dev, 'inode': info.st_ino, 'object_identity': file_identity, 'names': []})
            if (group['sha256'], group['bytes']) != (row['sha256'], row['bytes']):
                raise ValueError('shared object has inconsistent content')
            role = {'body': 'origin', 'attachment': 'attachment', 'intermediate': 'intermediate'}[row['kind']]
            group['names'].append({'path': str(path), 'kind': role})
    return list(groups.values())


def upstream_evidence(paper, work, data_root):
    if paper.get('receipt_schema') == 'paperflow-receipt/1':
        from .stage5_handoff import accepted_evidence
        return accepted_evidence(paper, work, data_root)
    records = Records(work)
    results = {stage: records.cached(stage) for stage in ('rag-body', 'rag-attachments', 'nas')}
    if any(not r or r.get('status') != 'verified' for r in results.values()):
        raise ValueError('RAG and NAS must both be verified before cleanup')
    paths = {Path(work) / 'inventory.json', Path(work) / 'archive/original-registration.json'}
    if read_json(Path(work) / 'inventory.json') != paper:
        raise ValueError('original inventory binding differs')
    for stage, result in results.items():
        if result.get('paper_id') != paper['paper_id'] or result.get('revision') != paper['revision']:
            raise ValueError('upstream receipt paper or revision differs')
        if stage.startswith('rag-') and (result.get('inventory_sha256') != identity(paper['archive_files']) or
                                       result.get('source_sha256') != paper['body']['sha256']):
            raise ValueError('upstream receipt inventory differs')
        state = records.status(stage)
        if not state['artifacts']:
            raise ValueError('upstream receipt must retain its original artifacts')
        paths.add(records.path(stage))
        paths.update(Path(p) for p in state['artifacts'])
    body = results['rag-body']
    receipt = read_json(body['receipt'])
    if (receipt.get('status') != 'complete' or receipt.get('stage') != 'verified' or
            receipt.get('manifest_sha256') != body.get('manifest_sha256') or
            receipt.get('targets') != body.get('targets') or not body.get('targets')):
        raise ValueError('body delivery receipt is incomplete or rebound')
    paths.add(Path(body['receipt']))
    expected = {(r['file_id'], target['body']['dataset_id'], target['body']['document_id'])
                for r in paper['attachments'] for target in body['targets'].values()}
    attachments = results['rag-attachments']
    observed = {(r.get('attachment_id'), r.get('dataset_id'), r.get('parent_body_document_id'))
                for r in attachments.get('targets', []) if r.get('status') == 'verified'
                and r.get('paper_id') == paper['paper_id']}
    if observed != expected or attachments.get('attachments') != len(paper['attachments']):
        raise ValueError('attachment delivery coverage differs')
    data_root = Path(data_root).resolve()
    for path in paths:
        if not path.resolve().is_relative_to(data_root):
            raise ValueError('cleanup evidence must stay in the bound records area')
    return {str(p.resolve()): sha_file(p) for p in paths}


@contextmanager
def hold_evidence(association):
    from paperflow.original_archive.lifecycle import locked_file
    with ExitStack() as stack:
        for name, digest in association['evidence'].items():
            stream, _ = stack.enter_context(locked_file(Path(name)))
            if hashlib.file_digest(stream, 'sha256').hexdigest() != digest:
                raise ValueError('frozen evidence changed')
        yield


@contextmanager
def verified_nas(paper, work, destination):
    """Reuse native held-file hash checks and the exact saved archive manifest."""
    from paperflow.original_archive.lifecycle import locked_file, verify_stream
    work, destination = Path(work), Path(destination).resolve()
    result = Records(work).cached('nas')
    receipt_path = work / 'archive/nas-receipt.json'
    if (not result or result.get('status') != 'verified' or result.get('warnings') or
            read_json(receipt_path) != result or Path(result['root']).resolve() != destination or
            result.get('paper_id') != paper['paper_id'] or result.get('revision') != paper['revision']):
        raise ValueError('NAS verified receipt missing or changed')
    registration_path = work / 'archive/original-registration.json'
    registration = read_json(registration_path)
    expected_registration = {'id': paper['paper_id'] + '-' + paper['revision'],
        'identity': {'paper_id': paper['paper_id'], 'revision': paper['revision']},
        'source_sha256': paper['body']['sha256'],
        'files': [{**r, 'name': r['relative'], 'kind': '原始来源' if r == paper['body'] else '附件'}
                  for r in paper['archive_files']]}
    if registration != expected_registration:
        raise ValueError('NAS registration differs from the frozen inventory')
    manifest_path = (destination / result['manifest_path']).resolve()
    if not manifest_path.is_relative_to(destination):
        raise ValueError('NAS manifest escaped its root')
    with ExitStack() as stack:
        stream, _ = stack.enter_context(locked_file(manifest_path))
        raw = stream.read()
        if hashlib.sha256(raw).hexdigest() != result['manifest_sha256']:
            raise ValueError('NAS manifest hash differs')
        manifest = json.loads(raw)
        expected = {(r['relative'], r['sha256'], r['bytes']) for r in paper['archive_files']}
        actual = {(r['name'], r['sha256'], r['bytes']) for r in manifest['files']}
        registered = dict(registration, source_evidence=[{'path': str(registration_path),
                                                        'sha256': sha_file(registration_path)}])
        if (expected != actual or len(manifest['files']) != len(expected) or
                manifest.get('native_registration') != registered or result['files'] != manifest['files']):
            raise ValueError('NAS manifest membership or original binding differs')
        opened = set()
        for row in manifest['files']:
            digest = row['sha256']
            if len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
                raise ValueError('invalid NAS object hash')
            if digest in opened:
                continue
            stream, _ = stack.enter_context(locked_file(destination / 'objects' / digest[:2] / digest))
            verify_stream(stream, digest, row['bytes'])
            opened.add(digest)
        yield result
