"""Candidate read-only check of unread limitations in one sealed body package.

Only receipt-linked body-result, its private validation references and the
public profile are considered. No raw model answer or report contents leave the
server. The caller must separately authenticate and inspect the full receipt.
This script is not installed or admitted by any server gate here.
"""
import hashlib
import json
from pathlib import PurePosixPath, PureWindowsPath
import re
import sys

REQUIRED_LIMIT = '模型声明部分材料未读，但部分位置无法与材料包明确对应；不能据此认定相关材料已读，需回看原始回答和原稿。'
MAX_INPUT = 16 * 1024 * 1024


def _path(value):
    if not isinstance(value, str) or not value or '\x00' in value:
        raise ValueError('evidence path is invalid')
    path = PureWindowsPath(value)
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('absolute receipt-linked evidence is required')
    return path


def _ref(value):
    if (not isinstance(value, dict) or not isinstance(value.get('sha256'), str)
            or not re.fullmatch(r'[0-9a-f]{64}', value['sha256'])):
        raise ValueError('a hashed evidence reference is required')
    return _path(value.get('path'))


def check_related_files(receipt, read_json, plan_identity):
    """Check only the disclosure rule; missing or ambiguous evidence raises."""
    if not isinstance(receipt, dict) or not callable(read_json) or not isinstance(receipt.get('body'), dict):
        raise ValueError('original receipt and verified reader are required')
    package = _path(receipt['body'].get('package_path'))
    artifacts = receipt.get('artifacts')
    if not isinstance(artifacts, list) or not artifacts:
        raise ValueError('receipt artifact references are unavailable')
    indexed = {}
    for ref in artifacts:
        path = _ref(ref)
        key = str(path).casefold()
        if key in indexed:
            raise ValueError('duplicate receipt artifact')
        indexed[key] = ref
    body_ref = indexed.get(str(package.parent / 'body-result.json').casefold())
    profile_ref = indexed.get(str(package / 'profile.json').casefold())
    if body_ref is None or profile_ref is None:
        raise ValueError('receipt-linked body result and profile are required')
    body, profile = read_json(body_ref), read_json(profile_ref)
    if (not isinstance(body, dict) or body.get('schema') != 'body-postprocess-result/1'
            or body.get('status') != 'body_package_verified' or body.get('identity') != receipt.get('identity')
            or _path(body.get('package_path')) != package
            or body.get('manifest_sha256') != receipt['body'].get('manifest_sha256')
            or not isinstance(body.get('private_artifacts'), dict) or not isinstance(profile, dict)):
        raise ValueError('body result does not belong to the receipt')
    limits = profile.get('migration_boundary', {}).get('global_limits')
    if not isinstance(limits, list) or any(not isinstance(limit, str) for limit in limits):
        raise ValueError('public reading limitations are unavailable')
    parts = {}
    for key, ref in body['private_artifacts'].items():
        match = re.fullmatch(r'part_([1-9][0-9]*)_validation', key) if isinstance(key, str) else None
        if match:
            number = int(match.group(1))
            expected_path = package.parent / 'private' / f'part-{number:03d}-validation.json'
            if number in parts or number > 1000 or _ref(ref) != expected_path:
                raise ValueError('private validation reference is invalid')
            parts[number] = ref
    assembly = body.get('assembly')
    if not isinstance(assembly, dict):
        raise ValueError('body assembly evidence is unavailable')
    if assembly.get('mode') == 'single_saved_response':
        expected_parts = 1
    elif assembly.get('mode') == 'program_merge':
        mappings = assembly.get('pointer_maps')
        if (not isinstance(mappings, list) or not mappings or len(mappings) > 1000
                or any(not isinstance(row, dict) or type(row.get('part')) is not int for row in mappings)
                or [row['part'] for row in mappings] != list(range(1, len(mappings) + 1))):
            raise ValueError('part assembly evidence is incomplete')
        expected_parts = len(mappings)
    else:
        raise ValueError('body assembly mode is unavailable')
    if (not isinstance(plan_identity, dict) or plan_identity.get('schema') != 'paperflow-body-input-plan/1'
            or type(plan_identity.get('part_count')) is not int or plan_identity['part_count'] != expected_parts
            or plan_identity.get('mode') != ('single' if expected_parts == 1 else 'partitioned')
            or not isinstance(plan_identity.get('parts'), list) or len(plan_identity['parts']) != expected_parts
            or any(not isinstance(row, dict) or row.get('id') != f'part-{number:03d}'
                   for number, row in enumerate(plan_identity['parts'], 1))):
        raise ValueError('frozen plan does not confirm every assembled part')
    if set(parts) != set(range(1, expected_parts + 1)):
        raise ValueError('complete ordered part validation references are required')
    unlocated_unread = False
    for number in sorted(parts):
        report = read_json(parts[number])
        if (not isinstance(report, dict) or report.get('valid') is not True
                or not isinstance(report.get('compatibility_report'), list)
                or any(not isinstance(row, dict) for row in report['compatibility_report'])):
            raise ValueError('private validation evidence is incomplete')
        unlocated_unread |= any(row.get('code') == 'normalized_unlocated_reading_declaration'
            and row.get('path') == '/source/reading_scope/unread_refs'
            for row in report['compatibility_report'])
    if unlocated_unread and REQUIRED_LIMIT not in limits:
        return {'status': 'review_required', 'reason': 'unlocated_unread_missing_public_limit'}
    return {'status': 'passed', 'reason': 'unread_disclosure_checked'}


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON key')
        result[key] = value
    return result


def execute(config, request):
    from remote_jobs import _uuid
    from server_receipt_inspect import _LinuxSnapshot
    from server_receipt_query import _finished
    import server_job_service as service
    if (not isinstance(request, dict) or set(request) != {'job', 'server_identity', 'receipt_json', 'receipt_sha256', 'nonce'}
            or request['server_identity'] != config.get('server_identity') or not _uuid(request['nonce'])):
        raise ValueError('exact registered server evidence and nonce required')
    raw, digest, receipt = _finished(service._ledger(config), request['job'], config)
    if raw != request['receipt_json'] or digest != request['receipt_sha256']:
        raise ValueError('original held receipt changed')
    verified = service.verify_job(request['job']['job_id'], config)
    if verified.get('job') != request['job']:
        raise ValueError('frozen server job changed')
    fs = _LinuxSnapshot(verified['snapshot'])
    try:
        def read_json(ref):
            _row, raw = fs.read(ref['path'], ref, json_value='raw')
            return json.loads(raw, object_pairs_hook=_pairs,
                              parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        plan_identity = verified['manifest']['inputs']['body']['plan_identity']
        verdict = check_related_files(receipt, read_json, plan_identity)
        fs.visible()
    finally:
        fs.close()
    return {'schema': 'paperflow-unread-disclosure-check/1', 'server_identity': request['server_identity'],
            'job_id': request['job']['job_id'], 'receipt_sha256': request['receipt_sha256'],
            'nonce': request['nonce'], **verdict}


def main():
    if len(sys.argv) != 4:
        raise SystemExit('reviewed app directory and independently pinned config are required')
    try:
        app = PurePosixPath(sys.argv[1])
        if not app.is_absolute() or '..' in app.parts:
            raise ValueError('reviewed app directory is unavailable')
        sys.path.insert(0, str(app))
        import server_job_service as service
        config = service.load_config(sys.argv[2], sys.argv[3])
        raw = sys.stdin.buffer.read(MAX_INPUT + 1)
        if len(raw) > MAX_INPUT:
            raise ValueError('request is too large')
        request = json.loads(raw, object_pairs_hook=_pairs,
                             parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        response = json.dumps(execute(config, request), ensure_ascii=False, sort_keys=True,
                              separators=(',', ':'), allow_nan=False).encode('utf8')
        if len(response) > 8192:
            raise ValueError('response is too large')
        sys.stdout.buffer.write(response); sys.stdout.buffer.flush()
    except Exception as exc:
        print('unread disclosure check failed: ' + type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
