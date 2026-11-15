"""Read-only production snapshot. Writes only to this candidate's development tree.

Never import production flows: importing them mutates Prefect's environment.
API POSTs below are Prefect read/filter endpoints, never creation/update routes.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
from pathlib import Path
import sys
import urllib.request

CANDIDATE = Path(__file__).resolve().parents[1]
PRODUCTION = CANDIDATE.parents[1] / 'prefect-paperflow'
DEVELOPMENT = CANDIDATE / 'runtime/development/pipeline-v2'


def _load(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def _hash(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def _write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')


def _read_api(api_url, route, data=None):
    request = urllib.request.Request(api_url.rstrip('/') + route,
        data=None if data is None else json.dumps(data).encode(),
        method='GET' if data is None else 'POST',
        headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=15) as response:
            return {'status': response.status, 'data': json.load(response)}
    except Exception as exc:
        return {'status': 'unconfirmed', 'error_type': type(exc).__name__}


def _identity(production):
    path = production / 'runtime/runner-process.json'
    if not path.is_file():
        return {'status': 'runner_receipt_missing'}
    receipt = _load(path)
    evidence = {'receipt': {key: receipt.get(key) for key in
        ('pid', 'create_time', 'entrypoint', 'executable')}, 'receipt_sha256': _hash(path)}
    try:
        import psutil
        process = psutil.Process(receipt['pid'])
        evidence.update(pid=process.pid, executable=process.exe(),
            create_time=process.create_time(), name=process.name(),
            receipt_matches_live_process=abs(process.create_time() - receipt['create_time']) < .1,
            command=process.cmdline())
    except Exception as exc:
        evidence.update(status='live_identity_unconfirmed', error_type=type(exc).__name__)
    return evidence


def capture_baseline(scope_manifest: Path, output: Path) -> Path:
    scope_manifest, output = Path(scope_manifest).resolve(), Path(output).resolve()
    if not output.is_relative_to(DEVELOPMENT):
        raise ValueError('Baseline output must remain under candidate development root')
    manifest = _load(scope_manifest)
    production = Path(manifest['production_root']).resolve()
    paths = {key: Path(manifest[key]).resolve() for key in
             ('copy_manifest', 'diagnosis', 'morning_closeout')}
    scope, diagnosis, morning = (_load(paths[key]) for key in
                                 ('copy_manifest', 'diagnosis', 'morning_closeout'))
    files = scope['files']
    groups = sorted({row['group'] for row in files})
    if len(groups) != scope['groups']:
        raise ValueError('Copy manifest group count does not match exact membership')
    config_path = production / 'config.json'
    config = _load(config_path)
    api = config['prefect_api_url']
    api_evidence = {
        'api_url': api,
        'server_version': _read_api(api, '/admin/version'),
        'deployments': _read_api(api, '/deployments/filter', {'limit': 200}),
        'limits': _read_api(api, '/v2/concurrency_limits/filter', {'limit': 200}),
        'work_pools': _read_api(api, '/work_pools/filter', {'limit': 200}),
    }
    deployments = api_evidence['deployments'].get('data')
    if isinstance(deployments, list):
        api_evidence['deployments']['data'] = [{key: row.get(key) for key in
            ('id', 'name', 'flow_id', 'version', 'entrypoint', 'path', 'work_pool_name',
             'work_queue_name', 'concurrency_limit', 'global_concurrency_limit', 'tags',
             'paused', 'updated')} for row in deployments]
    pools = api_evidence['work_pools'].get('data')
    if isinstance(pools, list):
        api_evidence['work_pools']['data'] = [{key: row.get(key) for key in
            ('id', 'name', 'type', 'status', 'is_paused', 'concurrency_limit', 'updated')}
            for row in pools]
        # Worker identity is separate from the manual Runner process identity.
        api_evidence['workers'] = {}
        from urllib.parse import quote
        for pool in pools:
            workers = _read_api(
                api, '/work_pools/' + quote(pool['name'], safe='') + '/workers/filter', {'limit': 200})
            if isinstance(workers.get('data'), list):
                workers['data'] = [{key: row.get(key) for key in
                    ('id', 'name', 'status', 'last_heartbeat_time', 'heartbeat_interval_seconds')}
                    for row in workers['data']]
            api_evidence['workers'][pool['name']] = workers
    by_person = {row['person']: row for row in diagnosis.get('papers', [])}
    paper_index = []
    for person in groups:
        historical = by_person.get(person, {})
        paper_index.append({'person': person, 'paper_id': historical.get('paper_id'),
            'inputs': [{key: row.get(key) for key in
                       ('relative', 'sha256', 'bytes', 'source', 'copy', 'test100_number')}
                       for row in files if row['group'] == person],
            'historical_category': historical.get('category', 'not_in_diagnostic_snapshot'),
            'historical_profile_state': historical.get('profile_state'),
            'historical_error_code': historical.get('error_code'),
            'evidence_index': {'diagnosis': str(paths['diagnosis']),
                               'json_pointer_person': person,
                               'inventory': historical.get('inventory'),
                               'body_work': historical.get('body_work')},
            'fresh_business_state': 'not_reclassified_by_baseline',
            'automatic_reexecution_authorized': False})
    code_hashes = {}
    for directory in (production / 'paperflow_prefect', CANDIDATE / 'paperflow_prefect'):
        if directory.exists():
            for path in sorted(directory.rglob('*.py')):
                code_hashes[str(path)] = _hash(path)
    for name in ('config.json', 'runtime-config.json', 'model-config.json', 'requirements.txt',
                 'manage.py', 'source-manifest.json'):
        path = production / name
        if path.is_file():
            code_hashes[str(path)] = _hash(path)
    frozen = {'manifest': manifest, 'manifest_sha256': _hash(scope_manifest),
              'sources': {key: {'path': str(path), 'sha256': _hash(path)}
                          for key, path in paths.items()}, 'exact_input_manifest': scope}
    _write(output / 'scope-freeze.json', frozen)
    _write(output / 'paper-evidence-index.json', paper_index)
    _write(output / 'code-config-sha256.json', code_hashes)
    report = {
        'schema': 'paperflow-stage-pipeline-baseline/1',
        'captured_at': datetime.now(timezone.utc).isoformat(),
        'python': sys.executable, 'sdk_version': importlib.metadata.version('prefect'),
        'scope': {'groups': len(groups), 'input_files': len(files),
                  'exact_membership': groups, 'freeze_path': str(output / 'scope-freeze.json')},
        'morning_100': {'status': morning.get('status'),
                        'summary': morning.get('summary'), 'exclude_from_reexecution': True,
                        'source': str(paths['morning_closeout'])},
        'diagnostic_snapshot': {'observed_at': diagnosis.get('observed_at'),
                                'counts': diagnosis.get('counts'),
                                'freshness': 'historical_not_current_classification'},
        'production_api': api_evidence, 'actual_runner_identity': _identity(production),
        'configured_limits': config.get('limits'),
        'evidence_index': str(output / 'paper-evidence-index.json'),
        'production_mutations': 0, 'real_model_requests': 0, 'real_rag_writes': 0,
        'production_ready': False,
    }
    report_path = output / 'baseline.json'
    _write(report_path, report)
    return report_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scope-manifest', type=Path)
    parser.add_argument('--output', type=Path, default=DEVELOPMENT / 'baseline')
    args = parser.parse_args()
    manifest = args.scope_manifest
    if manifest is None:
        manifest = DEVELOPMENT / 'scope-manifest.json'
        _write(manifest, {'production_root': str(PRODUCTION),
            'copy_manifest': str(PRODUCTION / 'runtime/desktop108-20261029/copy-manifest.json'),
            'diagnosis': str(PRODUCTION / 'runtime/desktop108-20261029/status-reports/LLM并发与停滞诊断-20261029-145907.json'),
            'morning_closeout': str(PRODUCTION / 'runtime/desktop100-closeout-20261029/independent-final-acceptance.json')})
    print(capture_baseline(manifest, args.output))


if __name__ == '__main__':
    main()
