"""Release only the single copy created by this manual isolated self-test.

This does not provide a new Prefect release adapter, process a real delivery
receipt, or authorize deletion outside the named case.  It reuses the native
RAG/NAS readers and the Windows opened-file deletion primitive.
"""
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from types import SimpleNamespace

from filelock import FileLock

HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parents[1]
sys.path[:0] = [str(WORKSPACE / 'prefect-paperflow'), str(HERE)]
from paperflow_prefect.legacy import bind
from paperflow_prefect.settings import Settings
settings = Settings.load(); bind(settings)
from auto_config import load_key
from ragflow_client import RagflowClient
from paperflow.package import validate_package
from paperflow.native_delivery import NativeDeliveryPublisher
from paperflow.original_archive.archive import ArchiveReader, ArchiveStore
from paperflow.original_archive.lifecycle import locked_file, verify_stream
from paperflow_prefect._release.file_identity import identity_from_stream
from paperflow_prefect._release.hardlinks import enumerate_links
from paperflow_prefect._release.namespace_guard import hold_directories
from paperflow_prefect._release.streams import require_default_stream_only
from effect_journal import EffectJournal
from guarded_rag_client import GuardedRagClient, _json, _sha
from contextlib import nullcontext

HISTORY = (WORKSPACE / 'prefect-paperflow/runtime/data/papers'
           / 'PF_259e9d53736a4f6395d2d6b9/body/55a61a7021a6c12c56a52547/private/original.doc')
CASE_ID = 'body-selftest-20261101-010654-f4e27f3e5dab'


def sha(path):
    with path.open('rb') as stream: return hashlib.file_digest(stream, 'sha256').hexdigest()


def no_reparse(root, path):
    root, path = Path(root).absolute(), Path(path).absolute()
    if path != root and not path.is_relative_to(root): raise ValueError('path escapes its test root')
    for part in (root, *(root.joinpath(*path.relative_to(root).parts[:i])
                          for i in range(1, len(path.relative_to(root).parts) + 1))):
        status = part.lstat()
        if part.is_symlink() or getattr(status, 'st_file_attributes', 0) & 0x400:
            raise ValueError('reparse point is not permitted in the test path')


def write_new(path, value):
    with path.open('x', encoding='utf8', newline='\n') as out:
        json.dump(value, out, ensure_ascii=False, sort_keys=True, indent=2)
        out.flush(); os.fsync(out.fileno())


def main(verify_only=False):
    case_path = HERE / 'live-tests' / CASE_ID
    no_reparse(HERE, case_path / 'input/body.doc')
    case = case_path.resolve(strict=True)
    if (case != case_path.absolute() or case.name != CASE_ID
            or case.parent != (HERE / 'live-tests').resolve(strict=True)):
        raise ValueError('test case escaped its root')
    intent_path, result_path = case / 'test-release-intent.json', case / 'test-release-result.json'
    if intent_path.exists() or result_path.exists(): raise ValueError('prior release attempt requires review')
    test_file = case / 'input/body.doc'
    no_reparse(HERE, test_file)
    if test_file.resolve(strict=True) != test_file or not test_file.is_file(): raise ValueError('test file path differs')
    no_reparse(WORKSPACE, HISTORY)
    source = HISTORY.resolve(strict=True)
    if os.path.samefile(source, test_file): raise ValueError('test copy aliases historical source')
    if os.stat(source).st_nlink != 1 or os.stat(test_file).st_nlink != 1:
        raise ValueError('source or test copy has a shared hard link')
    with locked_file(source) as (stream, _):
        source_identity = identity_from_stream(stream)
    with locked_file(test_file) as (stream, _):
        test_identity = identity_from_stream(stream)
    if source_identity == test_identity: raise ValueError('source and test copy share a file object')

    archive_report = json.loads((case / 'report.json').read_text(encoding='utf8'))
    rag_report = json.loads((case / 'rag-selftest.json').read_text(encoding='utf8'))
    receipts = json.loads((case / 'rag-receipts.json').read_text(encoding='utf8'))
    prepared = json.loads((case / 'prepared-publication.json').read_text(encoding='utf8'))
    task = prepared['task']
    packet = validate_package(case / 'publication', task, prepared['catalog'], strict_seal=True)
    source_hash = task['source_sha256']; size = test_file.stat().st_size
    if (archive_report.get('state') != 'archive_verified' or rag_report.get('state') != 'verified'
            or rag_report.get('retrieval_verified') is not True or rag_report.get('route_count') != 2
            or archive_report.get('source_sha256') != source_hash
            or rag_report.get('source_sha256') != source_hash
            or archive_report.get('manifest_sha256') != packet['manifest_sha256']
            or rag_report.get('manifest_sha256') != packet['manifest_sha256']
            or size != archive_report['input_bytes'] or sha(source) != source_hash or sha(test_file) != source_hash):
        raise ValueError('RAG, archive, package or source identity is not fully verified')
    if rag_report.get('formal_targets_write_allowed') is not False or rag_report.get('new_prefect_connected') is not False:
        raise ValueError('test scope differs')

    raw = RagflowClient(settings['rag_url'], load_key(settings.path('settings_root'), 'ragflow'))
    datasets = {d['id']: d for d in raw.datasets()}
    formal = {r['paper_dataset_id'] for r in packet['routes']}
    if len(formal) != 2 or not formal <= set(datasets): raise ValueError('formal target identities unavailable')
    tenants = {datasets[ds].get('tenant_id') for ds in formal}
    creators = {datasets[ds].get('created_by') for ds in formal}
    models = {datasets[ds].get('embedding_model') for ds in formal}
    if any(len(s) != 1 or not next(iter(s)) for s in (tenants, creators, models)):
        raise ValueError('formal target ownership or embedding identity differs')
    tenant, creator, model = next(iter(tenants)), next(iter(creators)), next(iter(models))
    test_ids = set()
    for route in packet['routes']:
        code = route['code']; registered = rag_report.get('datasets', {}).get(code, {})
        ds = registered.get('id'); actual = datasets.get(ds, {})
        expected_name = 'PaperFlow SELFTEST ' + case.name + ' ' + code
        if (registered.get('state') != 'created_and_verified' or registered.get('name') != expected_name
                or not ds or ds in formal or ds in test_ids or actual.get('name') != expected_name
                or actual.get('permission') != 'me' or actual.get('tenant_id') != tenant
                or actual.get('created_by') != creator or actual.get('embedding_model') != model
                or receipts.get(code, {}).get('verified') is not True
                or receipts.get(code, {}).get('retrieval_verified') is not True):
            raise ValueError('private test target identity or verification is incomplete')
        test_ids.add(ds); route['paper_dataset_id'] = ds
    if len(test_ids) != 2: raise ValueError('two distinct test targets required')
    publisher = NativeDeliveryPublisher(raw, lambda _: None, SimpleNamespace(native_pipeline_id='unused'))
    if len(publisher.records(packet)['body']) != 59: raise ValueError('sealed record count changed')
    publisher.verify(task, packet, receipts)
    live_states = publisher.states(receipts, True)
    live_documents = {row['document']['id']: row['document'] for row in live_states}

    log = case / 'rag-retrieval.jsonl'
    retrieval = [json.loads(line) for line in log.read_text(encoding='utf8').splitlines() if line]
    for route in packet['routes']:
        obj = receipts[route['code']]['body']; ds, did = obj['dataset_id'], obj['document_id']
        hits = {hit for row in retrieval if row.get('dataset_id') == ds and row.get('document_id') == did
                for hit in row.get('hit_ids', []) if isinstance(hit, str)}
        ids = [r['id'] for r in packet['chunks'] if r.get('kind') == 'body']
        groups = [ids] if ids else [[r[0] for r in publisher.records(packet)['body']]]
        if packet.get('structure_plan') is not None:
            groups += [[r[0] for r in v['records']] for v in packet['structure_plan']['structures'].values()]
        expected_ids = {item['chunk_id'] for item in obj['chunks'].values()}
        if (not expected_ids.issubset(hits)
                or any(not {obj['chunks'][rid]['chunk_id'] for rid in group}.intersection(hits) for group in groups)):
            raise ValueError('recorded retrieval does not cover all required test groups')

    journal = EffectJournal(case / 'rag-effects.sqlite3')
    guard = GuardedRagClient(raw, journal, 'a' * 64, lambda *_: nullcontext(), lambda: nullcontext(), lambda *_: False)
    with journal._connect() as db:
        if db.execute("SELECT count(*) FROM remote_effects WHERE state='intent'").fetchone()[0]:
            raise ValueError('an external write outcome remains unknown')
        if db.execute("SELECT count(*) FROM remote_document_reservations WHERE state='active'").fetchone()[0]:
            raise ValueError('a test document still has an active write reservation')
    for route in packet['routes']:
        obj = receipts[route['code']]['body']; ds, did = obj['dataset_id'], obj['document_id']
        batch = obj['native_batch']; body = {'doc_ids': [did], 'run': '1', 'delete': False}
        binding = {**obj['identity'], 'dataset_id': ds, 'document_id': did, 'pipeline_id': batch['pipeline_id']}
        op, request = guard._native_effect(body, binding)
        effect_id, request_sha = guard._identity(op, request, shared=True)
        if journal.state(effect_id, request_sha) != 'returned': raise ValueError('native effect is not returned')
        proof_path = case / 'rag-proofs' / ('native-' + ds + '.json')
        proof = json.loads(proof_path.read_text(encoding='utf8'))
        snapshot = Path(proof['receipt_snapshot']).resolve(strict=True)
        live_doc = live_documents.get(did, {})
        if (snapshot.parent != (case / 'rag-proofs').resolve(strict=True)
                or proof.get('effect_id') != effect_id or proof.get('request_sha256') != request_sha
                or proof.get('document_id') != did or proof.get('dataset_id') != ds
                or proof.get('remote_identity') != obj['identity']
                or proof.get('remote_document', {}).get('pipeline_id') != batch['pipeline_id']
                or live_doc.get('pipeline_id') != batch['pipeline_id']
                or str(live_doc.get('run')) not in ('3', 'DONE')
                or float(live_doc.get('progress', 0)) < 1
                or digest_if_present(snapshot) != proof.get('receipt_sha256')):
            raise ValueError('native proof or original receipt snapshot differs')
        frozen = json.loads(snapshot.read_text(encoding='utf8')).get(route['code'], {}).get('body', {})
        proof_rows = {row['record_id']: row for row in proof.get('readback_records', [])}
        records = publisher.records(packet)['body']
        if (len(proof_rows) != len(records) or set(proof_rows) != {r[0] for r in records}
                or set(obj.get('chunks', {})) != set(proof_rows)
                or set(frozen.get('chunks', {})) != set(proof_rows)):
            raise ValueError('native proof, snapshot or current receipt record inventory differs')
        for record in records:
            rid = record[0]; saved = obj['chunks'][rid]; snapshot_saved = frozen['chunks'][rid]
            expected_sha = hashlib.sha256(publisher.render(task, packet, record).encode('utf8')).hexdigest()
            if (saved != snapshot_saved or saved.get('chunk_id') != proof_rows[rid].get('chunk_id')
                    or saved.get('content_sha256') != expected_sha
                    or proof_rows[rid].get('content_sha256') != expected_sha
                    or (saved.get('image_id') or None) != (proof_rows[rid].get('image_id') or None)):
                raise ValueError('native proof record mapping differs')
        with journal._connect() as db:
            done = db.execute('SELECT * FROM test_native_completion WHERE effect_id=?', (effect_id,)).fetchone()
        if not done or done['proof_sha256'] != sha(proof_path) or done['receipt_sha256'] != sha(snapshot):
            raise ValueError('native completion evidence is not journaled')

    share = Path(r'\\zwjy-nas\paper_ai')
    expected_nas = share / 'paperflow-selftest' / case.name
    no_reparse(share, expected_nas)
    nas_root = expected_nas.resolve(strict=True)
    if Path(archive_report['nas_destination']).resolve(strict=True) != nas_root:
        raise ValueError('NAS archive root differs from test destination')
    store = ArchiveStore(case / 'nas-work/nas.sqlite')
    archived = store.status(archive_report['asset_id'])
    if (archived.get('status') != 'verified' or archived.get('warnings')
            or Path(archived['root']).resolve(strict=True) != nas_root or len(archived.get('files', [])) != 1
            or archived.get('manifest_sha256') != archive_report['archive_manifest_sha256']):
        raise ValueError('NAS archive receipt is incomplete')
    manifest_path = (nas_root / archived['manifest_path']).resolve(strict=True)
    if not manifest_path.is_relative_to(nas_root) or sha(manifest_path) != archived['manifest_sha256']:
        raise ValueError('NAS archive manifest differs')
    manifest = json.loads(manifest_path.read_text(encoding='utf8'))
    item = archived['files'][0]
    registration_path = case / 'nas-work/registration.json'
    registration = json.loads(registration_path.read_text(encoding='utf8'))
    expected_registration = {**registration, 'source_evidence': [
        {'path': str(registration_path), 'sha256': archive_report['registration_sha256']}]}
    if (manifest.get('source_sha256') != source_hash or item.get('sha256') != source_hash
            or item.get('bytes') != size or item.get('id') != archive_report['archive_file_id']
            or sha(registration_path) != archive_report['registration_sha256']
            or manifest.get('native_registration') != expected_registration
            or manifest.get('asset_id') != archive_report['asset_id']
            or manifest.get('files') != archived.get('files')
            or registration.get('id') != case.name
            or registration.get('source_sha256') != source_hash
            or registration.get('identity', {}).get('task_id') != task['task_id']
            or registration.get('files') != [{'path': str(test_file), 'name': 'body.doc',
                                             'kind': '原始来源', 'sha256': source_hash, 'bytes': size}]):
        raise ValueError('NAS archive object identity differs')
    reader = ArchiveReader(store, case / 'nas-work/readback')
    _, stream = reader.download(archive_report['asset_id'], item['id'])
    with stream:
        downloaded = hashlib.file_digest(stream, 'sha256').hexdigest()
        downloaded_size = stream.seek(0, 2)
    if (downloaded, downloaded_size) != (source_hash, size): raise ValueError('NAS readback differs')

    intent = {'schema': 'paperflow-manual-test-release/1', 'case_id': case.name,
              'target': str(test_file), 'target_identity': test_identity, 'bytes': size,
              'sha256': source_hash, 'original_identity': source_identity,
              'archive_manifest_sha256': archived['manifest_sha256'], 'nas_download_sha256': downloaded,
              'rag_report_sha256': sha(case / 'rag-selftest.json'), 'rag_receipt_sha256': sha(case / 'rag-receipts.json'),
              'retrieval_log_sha256': sha(log), 'rag_targets': sorted(test_ids), 'at_ns': time.time_ns(),
              'scope': 'single manual test copy; not a production release'}
    with hold_directories([test_file.parent], [case / 'input']) as guard:
        with locked_file(test_file, delete=not verify_only) as (stream, remove):
            verify_stream(stream, source_hash, size)
            require_default_stream_only(test_file)
            actual_links = {os.path.normcase(name) for name in enumerate_links(test_file)}
            if (identity_from_stream(stream) != test_identity or os.fstat(stream.fileno()).st_nlink != 1
                    or actual_links != {os.path.normcase(str(test_file))}):
                raise ValueError('test file object or hard-link inventory changed before deletion')
            guard.assert_held()
            if sha(source) != source_hash: raise ValueError('historical source changed before deletion')
            if verify_only:
                print(json.dumps({'state': 'release_checks_passed', 'case_id': case.name,
                                  'rag_targets': len(test_ids), 'records_per_target': 59,
                                  'nas_readback_sha256': downloaded, 'test_copy_preserved': test_file.is_file(),
                                  'historical_source_preserved': source.is_file()}, ensure_ascii=True))
                return
            # No unlink intent is recorded until the exact object, all names,
            # streams and pinned directory have passed their final checks.
            write_new(intent_path, intent)
            remove()
        guard.assert_held()
    if test_file.exists() or sha(source) != source_hash:
        raise ValueError('test deletion postcondition is not confirmed')
    result = {'case_id': case.name, 'state': 'test_copy_deleted', 'deleted_target': str(test_file),
              'release_intent_sha256': sha(intent_path), 'historical_source_preserved': True,
              'historical_source_sha256': sha(source), 'nas_archive_root': str(nas_root),
              'completed_at_ns': time.time_ns(), 'scope': 'manual isolated test only'}
    write_new(result_path, result)
    print(json.dumps(result, ensure_ascii=True))


def digest_if_present(path): return sha(path) if path.is_file() else None


if __name__ == '__main__':
    if sys.argv[1:] not in ([], ['--verify-only']): raise ValueError('unsupported test option')
    case = HERE / 'live-tests' / CASE_ID
    no_reparse(HERE, case / 'input/body.doc')
    with FileLock(str(case / '.rag-selftest.lock'), timeout=0), FileLock(str(case / '.release-test.lock'), timeout=0):
        main(verify_only=sys.argv[1:] == ['--verify-only'])
