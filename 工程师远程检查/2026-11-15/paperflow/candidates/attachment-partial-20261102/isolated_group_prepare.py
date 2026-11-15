"""Prepare one accepted folder for the isolated RAG and NAS stages."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3

from paperflow.native_delivery import NativeDeliveryPublisher
from paperflow.package import validate_package
from paperflow_prefect._release.file_identity import identity_from_stream
from paperflow_prefect.records import read_json, sha_file
from preextract_batch_delivery import _qualified

from batch_test_routes import _json_with_digest, prepare_test_routes
from bound_handoff import bind_handoff
from delivery_inventory import prepare_inventory
from group_stage import FIELDS
from isolated_copy import create_test_copy
from isolated_targets import dataset_by_id, ensure_test_targets
from remote_artifact_fetch import fetch_artifacts


def prepare_group(stage, access, sftp, *, pool_database, test_root, work_root, catalog_path,
                  catalog_sha256, run_id, raw, dataset_journal, admit, dataset_receipts, config,
                  artifact_map=None, coordinate_targets=None):
    ticket = {key: stage[key] for key in FIELDS}
    with closing(sqlite3.connect(Path(pool_database).resolve(strict=True).as_uri() + '?mode=ro', uri=True)) as db:
        row = db.execute('SELECT record FROM groups WHERE task_id=? AND group_id=?',
                         (ticket['task_id'], ticket['group_id'])).fetchone()
    if not row:
        raise ValueError('accepted delivery registration is unavailable')
    registration = json.loads(row[0])
    if (any(registration[key] != ticket[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id'))
            or sha_file(registration['receipt_path']) != registration['receipt_sha256']):
        raise ValueError('accepted delivery registration differs from the folder')
    inventory = prepare_inventory(registration['receipt_path'])
    if any(inventory[key] != ticket[key] for key in ('group_id', 'paper_id', 'receipt_id')):
        raise ValueError('delivery receipt differs from the folder')

    if artifact_map is None:
        artifact_map = access.artifact_map(ticket['task_id'], ticket['group_id'])
    receipt = json.loads(access.row(ticket['task_id'], ticket['group_id'])['receipt_json'])
    work = Path(work_root) / ticket['group_id']
    local = fetch_artifacts(artifact_map, sftp, work / 'server-artifacts')
    evidence = next(row for row in artifact_map['files']
                    if row['path'].replace('\\', '/').endswith('/body/body-result.json'))
    with sftp.open(evidence['linux_path'], 'rb') as stream:
        body_result = stream.read()
    bound = bind_handoff(stage, artifact_map, receipt, local, body_result)
    catalog = _json_with_digest(catalog_path, catalog_sha256, 'formal catalog')
    packet = validate_package(bound['body_path'], bound['prepared']['task'], catalog, strict_seal=True)
    if packet['manifest_sha256'] != bound['prepared']['manifest_sha256']:
        raise ValueError('sealed package differs from the postprocessing receipt')
    publisher = NativeDeliveryPublisher(None, lambda _: None, config)
    if not _qualified(bound['prepared'], packet, publisher):
        raise ValueError('this package is not ready for the isolated native body channel')

    targets = ensure_test_targets(packet['routes'], bool(publisher.records(packet)['images']),
                                  bool(bound['attachments']), run_id, catalog,
                                  raw, dataset_journal, admit, dataset_receipts, coordinate=coordinate_targets)
    plan = prepare_test_routes(bound['body_path'], bound['prepared']['task'], catalog_path,
                               catalog_sha256=catalog_sha256, run_id=run_id, test_targets=targets,
                               inspect_dataset=lambda dataset: dataset_by_id(raw, dataset),
                               attachments_required=bool(bound['attachments']),
                               validated=(packet, catalog))
    entries = [row for row in packet['manifest']['files']
               if (row if isinstance(row, str) else row.get('path')) == 'profile.json']
    profile_sha256 = (packet['manifest'].get('file_hashes', {}).get('profile.json')
                      if len(entries) == 1 and isinstance(entries[0], str)
                      else entries[0].get('sha256') if len(entries) == 1 else None)
    if not profile_sha256:
        raise ValueError('sealed profile identity is unavailable')

    copy_path = Path(test_root) / ticket['group_id'] / 'test-copy-receipt.json'
    if copy_path.exists():
        copied = read_json(copy_path)
        expected = {row['file_id']: row for row in inventory['files']}
        if (copied.get('schema') != 'paperflow-isolated-test-copy/1'
                or any(copied[key] != ticket[key] for key in ('group_id', 'paper_id', 'receipt_id'))
                or set(expected) != {row['file_id'] for row in copied['files']}):
            raise ValueError('existing test copy belongs to a different group')
        for row in copied['files']:
            source = expected[row['file_id']]
            if (any(row[key] != source[key] for key in ('kind', 'relative', 'sha256', 'bytes'))
                    or row['source_delivery_path'] != source['path']
                    or Path(row['path']) != copy_path.parent / 'files' / row['relative']):
                raise ValueError('existing test copy inventory changed')
            with Path(row['path']).open('rb') as stream:
                digest = hashlib.file_digest(stream, 'sha256').hexdigest()
                if digest != row['sha256'] or identity_from_stream(stream) != row['object_identity']:
                    raise ValueError('existing test copy changed')
    else:
        with admit(None, None) as permit:
            permit.check_current()
        copied = create_test_copy(inventory, test_root)
        copy_path = Path(copied['receipt_path'])
    return {'copy_receipt_path': str(copy_path), 'prepared': bound['prepared'], 'packet': packet,
            'plan': plan, 'attachments': bound['attachments'], 'failed_attachments': bound['failed_attachments'],
            'profile_sha256': profile_sha256,
            'reported_privacy_redaction': bound['reported_privacy_redaction'], 'artifact_map': artifact_map}
