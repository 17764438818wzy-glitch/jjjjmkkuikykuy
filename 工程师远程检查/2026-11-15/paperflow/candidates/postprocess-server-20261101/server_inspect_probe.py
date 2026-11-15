"""Inspect the real artifacts of a prior synthetic server folder test."""
import json
import hashlib
from pathlib import Path
import sys

from server_receipt_inspect import inspect_receipt
from snapshot_wire import snapshot_sha


def main():
    base = Path('/home/wzy/paperflow-postprocess/candidate-20261101')
    snapshot = base / sys.argv[1] / sys.argv[2]
    manifest = json.loads((snapshot / 'transport-manifest.json').read_bytes())
    job = {key: manifest[key] for key in ('job_id', 'claim_id', 'identity', 'generation',
                                           'manifest_sha256', 'code_sha256')}
    job['snapshot_sha256'] = snapshot_sha(manifest)
    receipt = json.loads((snapshot / 'drives/f/wire-result.json').read_bytes())['receipt']
    try:
        result = inspect_receipt(job=job, manifest=manifest, receipt=receipt, snapshot_root=snapshot,
                                 output_root_windows=r'F:\pfwire')
    except Exception:
        raw_identity = json.dumps(job['identity'], ensure_ascii=False, sort_keys=True,
                                  separators=(',', ':'), allow_nan=False).encode('utf8')
        folder = hashlib.sha256(raw_identity).hexdigest()
        body_result = json.loads((snapshot / 'drives/f/pfwire' / folder / 'body/body-result.json').read_bytes())
        body_input = manifest['inputs']['body']
        fingerprint = hashlib.sha256(json.dumps(body_input, ensure_ascii=False, sort_keys=True,
            separators=(',', ':'), allow_nan=False).encode('utf8')).hexdigest()
        print(json.dumps(dict(schema=body_result.get('schema'), status=body_result.get('status'),
            identity_matches=body_result.get('identity') == job['identity'],
            package_path_matches=body_result.get('package_path') == receipt['body']['package_path'],
            manifest_matches=body_result.get('manifest_sha256') == receipt['body']['manifest_sha256'],
            model_requests_added=body_result.get('model_requests_added'),
            original_responses_retained=body_result.get('original_responses_retained'),
            input_fingerprint_matches=body_result.get('input_fingerprint') == fingerprint,
            input_fingerprint_type=type(body_result.get('input_fingerprint')).__name__,
            private_artifacts=bool(body_result.get('private_artifacts')))))
        raise
    print(json.dumps(dict(integrity_status=result['integrity_status'],
                          files=len(result['files']), attachments=len(result['attachments']),
                          body_package_paths=[row['relative_path'] for row in result['body_package']],
                          semantic_validation=result['semantic_validation'],
                          privacy_validation=result['privacy_validation'],
                          rag_admission=result['rag_admission'])))


if __name__ == '__main__':
    main()
