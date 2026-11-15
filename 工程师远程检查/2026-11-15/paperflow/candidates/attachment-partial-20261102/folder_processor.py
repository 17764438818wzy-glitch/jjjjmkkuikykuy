"""Postprocess one already-existing folder order; no scheduler or network.

The caller provides the verified input loader, exclusive folder stage store and
business adapters. This module neither creates Prefect work nor uploads to RAG.
The persisted stage-store receipt, not the presence of any file, is authoritative.
"""
import copy
import hashlib
import json
from pathlib import Path, PurePosixPath


def _require(condition, detail):
    if not condition:
        raise ValueError(detail)


def _hash(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'),
                     allow_nan=False).encode('utf8')
    return hashlib.sha256(raw).hexdigest()


def _ref(path):
    path = Path(path).resolve(strict=True)
    with path.open('rb') as stream:
        return {'path': str(path), 'sha256': hashlib.file_digest(stream, 'sha256').hexdigest()}


def _check_inputs(inputs, identity, manifest_sha256):
    _require(isinstance(inputs, dict) and inputs.get('identity') == identity
             and inputs.get('manifest_sha256') == manifest_sha256
             and isinstance(inputs.get('body'), dict) and inputs['body'].get('identity') == identity
             and isinstance(inputs.get('attachments'), list), 'folder input identity changed')
    identifiers = []
    for item in inputs['attachments']:
        _require(isinstance(item, dict) and item.get('ticket') == identity
                 and isinstance(item.get('attachment_id'), str) and item['attachment_id'],
                 'attachment belongs to another folder')
        identifiers.append(item['attachment_id'])
    _require(len(set(identifiers)) == len(identifiers), 'duplicate attachment identity')


def _package_evidence(result, body_dir, identity):
    package = body_dir / 'publication'
    _require(result.get('schema') == 'body-postprocess-result/1'
             and result.get('status') == 'body_package_verified' and result.get('identity') == identity
             and Path(result.get('package_path', '')).resolve() == package.resolve(), 'body output identity changed')
    manifest_ref = _ref(package / 'manifest.json')
    _require(manifest_ref['sha256'] == result.get('manifest_sha256'), 'body manifest changed')
    manifest = json.loads((package / 'manifest.json').read_bytes())
    ready = json.loads((package / 'READY.json').read_bytes())
    _require(isinstance(manifest.get('files'), list) and ready.get('manifest_sha256') == manifest_ref['sha256'],
             'body package seal is incomplete')
    artifacts = [_ref(body_dir / 'body-result.json'), manifest_ref, _ref(package / 'READY.json')]
    seen = set()
    for entry in manifest['files']:
        _require(isinstance(entry, dict) and isinstance(entry.get('path'), str), 'invalid package member')
        relative = PurePosixPath(entry['path'])
        _require(not relative.is_absolute() and '..' not in relative.parts and '\\' not in entry['path']
                 and relative.parts and entry['path'] not in seen, 'invalid package member path')
        seen.add(entry['path'])
        member = package.joinpath(*relative.parts).resolve(strict=True)
        _require(member.is_relative_to(package.resolve()), 'package member escapes output')
        actual = _ref(member)
        _require(actual['sha256'] == entry.get('sha256') and member.stat().st_size == entry.get('bytes'),
                 'body package member changed')
        artifacts.append(actual)
    return artifacts


def _attachment_evidence(result, output, item, identity):
    if result.get('status') == 'failed':
        binding = result['binding']
        _require(result.get('schema') == 'attachment-postprocess-result/1'
                 and result.get('ticket') == identity and binding.get('attachment_id') == item['attachment_id']
                 and result.get('code') == 'attachment_format_error', 'attachment failure identity changed')
        source = _ref(binding['source_path'])
        _require(source['sha256'] == binding['source_sha256'], 'failed attachment original changed')
        artifacts = [_ref(output / 'receipt.json'), source]
        if result.get('diagnostic'):
            _require(_ref(result['diagnostic']['path']) == result['diagnostic'], 'attachment diagnostic changed')
            artifacts.append(result['diagnostic'])
        failure = dict(attachment_id=item['attachment_id'], status='failed', code=result['code'],
            message=result['message'], source_path=source['path'], source_sha256=source['sha256'],
            source_name=Path(source['path']).name, raw_response=result['raw_response'],
            diagnostic=result.get('diagnostic'))
        return artifacts, failure
    delivery = result.get('delivery')
    _require(result.get('schema') == 'attachment-postprocess-result/1' and result.get('status') == 'complete'
             and result.get('ticket') == identity and isinstance(delivery, dict)
             and delivery.get('paper_id') == identity['paper_id']
             and delivery.get('attachment_id') == item['attachment_id'], 'attachment output identity changed')
    md = _ref(delivery['md_path'])
    _require(md['sha256'] == delivery.get('md_sha256') and Path(md['path']).is_relative_to(output.resolve()),
             'attachment MD changed or escaped its output')
    source = _ref(delivery['source_path'])
    _require(source['sha256'] == delivery.get('source_sha256'), 'attachment original changed')
    artifacts = [_ref(output / 'receipt.json'), source]
    _require(isinstance(result.get('artifacts'), dict) and result['artifacts'], 'attachment artifacts missing')
    for ref in result['artifacts'].values():
        actual = _ref(ref['path'])
        _require(actual == ref and Path(actual['path']).is_relative_to(output.resolve()), 'attachment artifact changed')
        artifacts.append(actual)
    _require(md in artifacts, 'attachment MD is missing from its receipt')
    return artifacts, copy.deepcopy(delivery)


def process_folder(*, identity, generation, manifest_sha256, store, load_inputs,
                   body_processor, attachment_processor, output_root, output_guard):
    """Process and verify all parts under one persistent folder claim."""
    original = copy.deepcopy(load_inputs())
    _check_inputs(original, identity, manifest_sha256)
    root = Path(output_root).absolute()
    folder = root / _hash(identity)
    if not callable(output_guard):
        raise ValueError('output safety check is unavailable')
    output_guard(original, folder)
    claim = store.claim(identity, generation, manifest_sha256)
    claim_id = claim['claim_id']
    try:
        root.mkdir(parents=True, exist_ok=True)
        folder.mkdir(exist_ok=False)
        body_dir = folder / 'body'
        body_result = body_processor(original['body'], body_dir)
        body_artifacts = _package_evidence(body_result, body_dir, identity)
        artifacts = list(body_artifacts)
        deliveries, failures = [], []
        for item in original['attachments']:
            path = folder / 'attachments' / _hash(item['attachment_id'])
            result = attachment_processor(identity, item, path)
            current, delivery = _attachment_evidence(result, path, item, identity)
            artifacts.extend(current)
            (failures if result['status'] == 'failed' else deliveries).append(delivery)
        unique = {entry['path']: entry for entry in artifacts}
        _require(len(unique) == len(artifacts), 'duplicate output artifact')
        receipt = dict(schema='paperflow-postprocessing-receipt/1', identity=copy.deepcopy(identity),
            manifest_sha256=manifest_sha256, claim_id=claim_id, validation_status='passed',
            body=dict(package_path=body_result['package_path'], manifest_sha256=body_result['manifest_sha256']),
            attachments=deliveries, failed_attachments=failures, artifacts=artifacts)

        return store.complete(identity, claim_id, receipt, validate_receipt=lambda value: value == receipt)
    except BaseException as exc:
        try:
            store.mark_unknown(identity, claim_id, type(exc).__name__)
        except Exception as record_error:
            exc.add_note('could not persist unknown outcome: ' + type(record_error).__name__)
        raise


def make_existing_receipt_validator(*, identity, claim_id, manifest_sha256, load_inputs,
                                    body_processor, attachment_processor, output_root, output_guard):
    """Build a full validator for an existing receipt without a stage transition."""
    original = copy.deepcopy(load_inputs())
    _check_inputs(original, identity, manifest_sha256)
    folder = Path(output_root).absolute() / _hash(identity)
    if not callable(output_guard):
        raise ValueError('output safety check is unavailable')
    output_guard(original, folder)
    _require(folder.is_dir(), 'held folder output is missing')
    _require((folder / 'body' / 'body-result.json').is_file(), 'held body result is missing')
    for item in original['attachments']:
        path = folder / 'attachments' / _hash(item['attachment_id'])
        _require((path / 'receipt.json').is_file(), 'held attachment result is missing')

    def validate(value):
        if (not isinstance(value, dict) or value.get('identity') != identity
                or value.get('manifest_sha256') != manifest_sha256 or value.get('claim_id') != claim_id
                or value.get('schema') != 'paperflow-postprocessing-receipt/1'
                or value.get('validation_status') != 'passed' or load_inputs() != original):
            return False
        body_dir = folder / 'body'
        body_result = body_processor(original['body'], body_dir)
        artifacts = _package_evidence(body_result, body_dir, identity)
        deliveries, failures = [], []
        for item in original['attachments']:
            path = folder / 'attachments' / _hash(item['attachment_id'])
            result = attachment_processor(identity, item, path)
            current, delivery = _attachment_evidence(result, path, item, identity)
            artifacts.extend(current)
            (failures if result['status'] == 'failed' else deliveries).append(delivery)
        return (value.get('body') == {'package_path': body_result['package_path'],
                                      'manifest_sha256': body_result['manifest_sha256']}
                and value.get('attachments') == deliveries and value.get('failed_attachments', []) == failures
                and value.get('artifacts') == artifacts
                and len({entry['path'] for entry in artifacts}) == len(artifacts)
                and load_inputs() == original)

    return validate


def reconcile_folder(*, identity, claim_id, generation, manifest_sha256, store, load_inputs,
                     body_processor, attachment_processor, output_root, output_guard):
    """Recheck an already-held receipt; never claim again or create outputs."""
    validate = make_existing_receipt_validator(identity=identity, claim_id=claim_id,
        manifest_sha256=manifest_sha256, load_inputs=load_inputs, body_processor=body_processor,
        attachment_processor=attachment_processor, output_root=output_root, output_guard=output_guard)
    return store.reconcile(identity, claim_id, generation, validate_receipt=validate)
