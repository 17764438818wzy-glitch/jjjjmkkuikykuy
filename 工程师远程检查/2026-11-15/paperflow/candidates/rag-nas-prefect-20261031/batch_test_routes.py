"""Build an isolated per-specialty target plan for one sealed paper.

This only reads a caller-supplied sealed package and dataset observations. The
catalog digest must come independently from trusted, approved configuration;
the caller must supply a genuinely read-only dataset observer. It creates no
dataset, sends no document, grants no admission, and does not aggregate a
paper group's attachments or mark a workflow stage complete.
"""
import hashlib
import hmac
import json
from pathlib import Path
import re


_DATASET = re.compile(r'[0-9a-f]{32}\Z')
_DIGEST = re.compile(r'[0-9a-f]{64}\Z')
_CODE = re.compile(r'[A-Za-z0-9]{1,32}\Z')
_MAX_JSON = 16 * 1024 * 1024


def _json_with_digest(path, expected_digest, label):
    if not isinstance(expected_digest, str) or not _DIGEST.fullmatch(expected_digest):
        raise ValueError(f'{label} digest pin is invalid')
    try:
        with Path(path).open('rb') as stream:
            raw = stream.read(_MAX_JSON + 1)
        if len(raw) > _MAX_JSON:
            raise ValueError(f'{label} exceeds the maximum size')
        if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), expected_digest):
            raise ValueError(f'{label} hash does not match its trusted pin')
        return json.loads(raw.decode('utf-8-sig'))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f'{label} cannot be read') from exc


def _sealed_json(root, manifest, name, *, optional=False):
    entries = manifest.get('files', [])
    if not isinstance(entries, list):
        raise ValueError('sealed manifest file list is invalid')
    matches = [entry for entry in entries if
               (entry.replace('\\', '/') if isinstance(entry, str)
                else entry.get('path', '').replace('\\', '/') if isinstance(entry, dict) else None) == name]
    path = Path(root) / name
    if not matches:
        if optional and not path.exists():
            return None
        raise ValueError(f'{name} is not present in the sealed manifest')
    if len(matches) != 1:
        raise ValueError(f'{name} has duplicate sealed entries')
    entry = matches[0]
    digest = (manifest.get('file_hashes', {}).get(name) if isinstance(entry, str)
              else entry.get('sha256'))
    return _json_with_digest(path, digest, name)


def _route_codes(value):
    if not isinstance(value, list) or not value or len(value) > 3:
        raise ValueError('sealed specialty list is invalid')
    if any(not isinstance(row, dict) or not isinstance(row.get('code'), str)
           or not _CODE.fullmatch(row['code']) for row in value):
        raise ValueError('sealed specialty list is invalid')
    codes = [row['code'] for row in value]
    if len(set(codes)) != len(codes):
        raise ValueError('sealed specialty list contains duplicates')
    return codes


def prepare_test_routes(package_path, task, catalog_path, *, catalog_sha256, run_id,
                        test_targets, inspect_dataset, attachments_required=False, validated=None):
    from paperflow.package import asset_storage_mode, validate_package

    if not isinstance(run_id, str) or not _DATASET.fullmatch(run_id):
        raise ValueError('test run identity must be a 32-character lowercase identifier')
    if not callable(inspect_dataset):
        raise ValueError('a read-only dataset observer is required')
    catalog = (_json_with_digest(catalog_path, catalog_sha256, 'formal catalog')
               if validated is None else validated[1])
    if not isinstance(catalog, dict) or not isinstance(catalog.get('majors'), list):
        raise ValueError('formal specialty catalog is unavailable')
    majors = catalog['majors']
    if any(not isinstance(row, dict) or not isinstance(row.get('code'), str)
           or not _CODE.fullmatch(row['code']) for row in majors):
        raise ValueError('formal specialty catalog is invalid')
    by_code = {row['code']: row for row in majors}
    if len(by_code) != len(majors):
        raise ValueError('formal specialty codes are not unique')
    formal_ids = {row.get(field) for row in majors for field in ('paper_dataset_id', 'image_dataset_id')
                  if isinstance(row.get(field), str) and row.get(field)}
    if not formal_ids or any(not _DATASET.fullmatch(value) for value in formal_ids):
        raise ValueError('formal dataset identity inventory is unavailable')
    if not isinstance(test_targets, dict) or not test_targets:
        raise ValueError('test target map is unavailable')
    seen = set()
    for code, mapping in test_targets.items():
        if (code not in by_code or not isinstance(mapping, dict)
                or not set(mapping) <= {'paper_dataset_id', 'image_dataset_id'}
                or not mapping.get('paper_dataset_id')):
            raise ValueError('test target mapping is invalid')
        for target in mapping.values():
            if not isinstance(target, str) or not _DATASET.fullmatch(target):
                raise ValueError('test target identity is invalid')
            if target in formal_ids:
                raise ValueError('test target matches a formal dataset')
            if target in seen:
                raise ValueError('test dataset identities must be unique')
            seen.add(target)

    packet = (validate_package(package_path, task, catalog, strict_seal=True)
              if validated is None else validated[0])
    if packet.get('pending_routes') or not packet.get('routes'):
        raise ValueError('sealed paper has unresolved specialty routes')
    profile = _sealed_json(package_path, packet['manifest'], 'profile.json')
    if not isinstance(profile, dict) or profile != packet.get('profile'):
        raise ValueError('sealed profile changed during specialty validation')
    codes = _route_codes(profile.get('routing_candidates'))
    corrections = _sealed_json(package_path, packet['manifest'], 'corrections.json', optional=True)
    if corrections is not None:
        if not isinstance(corrections, dict):
            raise ValueError('sealed corrections are invalid')
        if ('final_routing_candidates' in corrections
                and _route_codes(corrections['final_routing_candidates']) != codes):
            raise ValueError('sealed specialty corrections conflict with the profile')
    if _route_codes(packet['routes']) != codes:
        raise ValueError('resolved specialty routes conflict with the sealed profile')
    modes = [asset_storage_mode(asset) for asset in packet['assets']]
    if any(mode not in ('image', 'description', 'omit') for mode in modes):
        raise ValueError('unrecognized asset storage mode')
    needs_images = 'image' in modes
    output = []
    for formal in packet['routes']:
        code = formal['code']
        expected = by_code.get(code)
        mapping = test_targets.get(code)
        if (not expected or not mapping or formal.get('paper_dataset_id') != expected.get('paper_dataset_id')
                or formal.get('image_dataset_id') != expected.get('image_dataset_id')):
            raise ValueError('sealed specialty is not bound to the original catalog')
        result = {'code': code, 'paper_dataset_id': mapping['paper_dataset_id']}
        roles = [('paper', 'paper_dataset_id')]
        if needs_images or attachments_required:
            if not mapping.get('image_dataset_id'):
                raise ValueError('test image target is required for this paper')
            result['image_dataset_id'] = mapping['image_dataset_id']
            roles.append(('image', 'image_dataset_id'))
        for role, field in roles:
            original, target = inspect_dataset(expected[field]), inspect_dataset(mapping[field])
            if (not isinstance(original, dict) or original.get('id') != expected[field]
                    or not isinstance(target, dict) or target.get('id') != mapping[field]
                    or target.get('name') != f'PaperFlow SELFTEST {run_id} {code} {role}'
                    or target.get('permission') != 'me'):
                raise ValueError('test target observation is unavailable or differs')
            embedding = original.get('embedding_model')
            if not isinstance(embedding, str) or not embedding.strip() or target.get('embedding_model') != embedding:
                raise ValueError('test target embedding differs from the formal dataset')
        output.append(result)
    return {'schema': 'paperflow-isolated-test-route-plan/1', 'run_id': run_id,
            'manifest_sha256': packet['manifest_sha256'], 'catalog_sha256': catalog_sha256, 'routes': output,
            'body_images_required': needs_images, 'attachments_required': attachments_required, 'upload_authorized': False,
            'attachments_verified': False, 'prefect_connected': False}
