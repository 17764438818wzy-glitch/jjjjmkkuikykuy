"""Read-only authenticated-server receipt query candidate; never starts work.

The SSH endpoint must be separately installed and host-key pinned. A caller
must freeze the selected config SHA independently and bound both input/output
and transport time. No implementation here installs that endpoint or a key.
"""
from contextlib import closing
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import sys

from remote_jobs import _uuid, _validated
from server_receipt_inspect import inspect_receipt
from snapshot_import import SnapshotImporter
from windows_snapshot import EvidenceMap
import server_job_service as service


class ReceiptQueryError(RuntimeError):
    pass


def _require(condition, detail):
    if not condition:
        raise ReceiptQueryError(detail)


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _finished(ledger, job, expected_config):
    """Read one consistent ledger snapshot; never update any service state."""
    frozen = _validated(job)
    with closing(ledger._connect()) as db:
        db.execute('BEGIN')
        run = db.execute('SELECT * FROM executor_jobs WHERE job_id=?', (job['job_id'],)).fetchone()
        worker = db.execute('SELECT * FROM supervisor_jobs WHERE job_id=?', (job['job_id'],)).fetchone()
        entry = db.execute('SELECT * FROM service_jobs WHERE job_id=?', (job['job_id'],)).fetchone()
        _require(run is not None and worker is not None and entry is not None,
                 'durable execution evidence is missing')
        run, worker, entry = dict(run), dict(worker), dict(entry)
    _require(run['state'] == 'finished' and run['frozen_json'] == worker['frozen_json']
             == entry['frozen_json'] == frozen and entry['phase'] == 'finished'
             and run['claim_id'] == job['claim_id']
             and all(run[key] == job['identity'][key] for key in ('task_id', 'group_id', 'task_run_id'))
             and entry['config_sha256'] == hashlib.sha256(_json(expected_config).encode('utf8')).hexdigest(),
             'execution or selected service configuration is not finished')
    detail = json.loads(entry['detail_json'])
    cleanup = detail.get('cleanup') if isinstance(detail, dict) else None
    _require(isinstance(cleanup, dict) and cleanup.get('terminated') is True
             and cleanup.get('scope_removed') is True and worker.get('scope_json')
             and worker.get('process_json'), 'complete worker termination is not recorded')
    scope, process = json.loads(worker['scope_json']), json.loads(worker['process_json'])
    launch, clock = json.loads(worker['config_json']), json.loads(worker['clock_json'])
    final = cleanup.get('final_scope')
    scope_id = scope.get('scope_id') if isinstance(scope, dict) else None
    _require(isinstance(scope_id, list) and len(scope_id) == 2
             and all(type(value) is int and value > 0 for value in scope_id)
             and isinstance(process, dict) and type(process.get('pid')) is int and process['pid'] > 0
             and type(process.get('start_ticks')) is int and process['start_ticks'] > 0
             and isinstance(clock, dict) and isinstance(launch, dict)
             and scope.get('boot_id') == process.get('boot_id') == clock.get('boot_id') == cleanup.get('boot_id')
             and _uuid(scope.get('boot_id'))
             and cleanup.get('scope_identity') == scope and cleanup.get('process_identity') == process
             and isinstance(final, dict) and final.get('populated') is False
             and final.get('boot_id') == scope['boot_id'] and final.get('scope_id') == scope_id
             and cleanup.get('parent') == launch.get('parent')
             and isinstance(launch.get('parent'), str) and launch['parent'].startswith('/sys/fs/cgroup/')
             and '..' not in PurePosixPath(launch['parent']).parts
             and PurePosixPath(launch['parent']).name in {'paperflow-pp-' + job['job_id'] + '.service',
                                                         'paperflow-pp-' + job['job_id'] + '.scope'}
             and launch.get('uid') == expected_config.get('worker_uid')
             and launch.get('gid') == expected_config.get('worker_gid')
             and launch.get('clock_basis') == 'linux-monotonic-boot/1'
             and type(cleanup.get('observed_monotonic')) in (int, float)
             and type(clock.get('started_monotonic')) in (int, float)
             and math.isfinite(cleanup['observed_monotonic']) and math.isfinite(clock['started_monotonic'])
             and cleanup['observed_monotonic'] >= clock['started_monotonic'] >= 0,
             'terminated cgroup and process identity proof is incomplete')
    raw, digest = worker['receipt_json'], worker['held_sha256']
    _require(isinstance(raw, str) and isinstance(digest, str) and digest == run['receipt_sha256']
             and hashlib.sha256(raw.encode('utf8')).hexdigest() == digest,
             'durable receipt bytes are not bound to the finished job')
    value = json.loads(raw)
    _require(_json(value) == raw and value.get('identity') == job['identity']
             and value.get('claim_id') == job['claim_id']
             and value.get('manifest_sha256') == job['manifest_sha256'],
             'durable receipt identity or encoding changed')
    return raw, digest, value


def _verify_code(job, config, snapshot):
    raw = service._read(config['release']['code_manifest'])
    _require(hashlib.sha256(raw).hexdigest() == job['code_sha256'], 'business code inventory changed')
    rows = json.loads(raw)
    _require(isinstance(rows, list) and rows, 'business code inventory is empty')
    mapper, files, seen = EvidenceMap(snapshot), [], set()
    for row in rows:
        path, relative = row['source'], row['relative']
        parts = PurePosixPath(relative)
        _require(not parts.is_absolute() and '\\' not in relative and all(p not in ('', '.', '..') for p in relative.split('/')),
                 'business code relative path is invalid')
        mapped = mapper.bind(path, row['sha256'], row['bytes'])
        _require(mapped.relative_to(Path(snapshot)).as_posix() == relative and path.lower() not in seen,
                 'business code mapping changed')
        seen.add(path.lower())
        files.append(dict(path=path, sha256=row['sha256'], bytes=row['bytes']))
    importer = SnapshotImporter(snapshot, files)
    try:
        _require(importer.verify_complete() is True, 'business code files changed')
    finally:
        importer.close()


def inspect_requests(config, requests, *, max_batch=32):
    service._config(config)
    _require(isinstance(requests, list) and 1 <= len(requests) <= max_batch,
             'bounded nonempty receipt batch is required')
    ledger, seen, answers = service._ledger(config), set(), []
    for request in requests:
        _require(isinstance(request, dict)
                 and set(request) == {'job', 'server_identity', 'receipt_json', 'receipt_sha256'}
                 and request['server_identity'] == config['server_identity'], 'server identity changed')
        job = request['job']
        _validated(job)
        _require(job['job_id'] not in seen, 'duplicate execution in receipt batch')
        seen.add(job['job_id'])
        raw, digest, receipt = _finished(ledger, job, config)
        _require(request['receipt_json'] == raw and request['receipt_sha256'] == digest,
                 'request does not contain the exact held receipt')
        verified = service.verify_job(job['job_id'], config)
        _require(verified['job'] == job, 'staged execution identity changed')
        _verify_code(job, config, verified['snapshot'])
        _require(service._business_receipt(job, verified, receipt) is True,
                 'business processor receipt cannot be revalidated')
        mapping = inspect_receipt(job=job, manifest=verified['manifest'], receipt=receipt,
            snapshot_root=verified['snapshot'], output_root_windows=verified['manifest']['output_root_windows'])
        _require(mapping.get('integrity_status') == 'passed' and mapping.get('job') == job
                 and mapping.get('receipt_canonical_sha256') == digest, 'artifact mapping is unavailable')
        answers.append(dict(schema='paperflow-authenticated-remote-result/1', job=job,
            server_identity=config['server_identity'], receipt_json=raw, receipt_sha256=digest,
            ledger_state='finished', worker_terminated=True, business_validation='processor_reported_pass',
            business_code_sha256=job['code_sha256'], artifact_map=mapping))
    return answers


def main():
    if len(sys.argv) != 3:
        raise SystemExit('an independently pinned service config and SHA256 are required')
    try:
        config = service.load_config(sys.argv[1], sys.argv[2])
        data = sys.stdin.buffer.read(16 * 1024 * 1024 + 1)
        _require(len(data) <= 16 * 1024 * 1024, 'receipt query is too large')
        response = _json(inspect_requests(config, json.loads(data))).encode('utf8')
        _require(len(response) <= 16 * 1024 * 1024, 'receipt response is too large')
        sys.stdout.buffer.write(response)
        sys.stdout.buffer.flush()
    except Exception as exc:
        print('receipt query failed: ' + type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
