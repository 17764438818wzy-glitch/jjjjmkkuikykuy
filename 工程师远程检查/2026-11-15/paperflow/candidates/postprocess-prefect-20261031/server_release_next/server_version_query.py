"""Candidate Linux metadata endpoint; read one finished job, never dispatch.

The service config identifies the server ledger. Its own version is not used
as a guess for the job's version: the latter is read from service_jobs. This
only selects an inspection config; it does not validate the output or grant RAG.
Installation, restricted SSH access and the script's immutable hash are external
requirements and are not performed here.
"""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys

SHA = re.compile(r'[0-9a-f]{64}')
MAX_INPUT = 16 * 1024 * 1024


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON key')
        result[key] = value
    return result


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def lookup(config, request):
    from remote_jobs import _uuid, _validated
    import server_job_service as service
    required = {'job', 'server_identity', 'receipt_json', 'receipt_sha256', 'nonce'}
    if (not isinstance(config, dict) or not isinstance(request, dict) or set(request) != required
            or request['server_identity'] != config.get('server_identity') or not _uuid(request['nonce'])):
        raise ValueError('the exact server and one fresh request nonce are required')
    job = request['job']
    frozen = _validated(job)
    raw, digest = request['receipt_json'], request['receipt_sha256']
    if (not isinstance(raw, str) or len(raw.encode('utf8')) > MAX_INPUT
            or not isinstance(digest, str) or not SHA.fullmatch(digest)
            or hashlib.sha256(raw.encode('utf8')).hexdigest() != digest):
        raise ValueError('the exact held receipt is required')
    receipt = json.loads(raw, object_pairs_hook=_pairs, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    if (not isinstance(receipt, dict) or _json(receipt) != raw
            or receipt.get('schema') != 'paperflow-postprocessing-receipt/1'
            or receipt.get('identity') != job['identity'] or receipt.get('claim_id') != job['claim_id']
            or receipt.get('manifest_sha256') != job['manifest_sha256']
            or receipt.get('validation_status') != 'passed'):
        raise ValueError('receipt and job identity differ')
    ledger = service._ledger(config)
    with closing(sqlite3.connect(ledger.database.as_uri() + '?mode=ro', uri=True,
                                 timeout=ledger.busy_timeout)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        settings = dict(db.execute('SELECT name,value FROM executor_settings'))
        run = db.execute('SELECT * FROM executor_jobs WHERE job_id=?', (job['job_id'],)).fetchone()
        worker = db.execute('SELECT * FROM supervisor_jobs WHERE job_id=?', (job['job_id'],)).fetchone()
        entry = db.execute('SELECT * FROM service_jobs WHERE job_id=?', (job['job_id'],)).fetchone()
        if run is None or worker is None or entry is None:
            raise ValueError('original server evidence is unavailable')
        run, worker, entry = dict(run), dict(worker), dict(entry)
    canonical_sha = entry.get('config_sha256')
    if (settings.get('capacity') != str(ledger.capacity)
            or settings.get('ledger_id') != config['server_identity'].get('ledger_id')
            or settings.get('ledger_id') != ledger.ledger_id
            or run.get('state') != 'finished' or entry.get('phase') != 'finished'
            or run.get('frozen_json') != frozen or worker.get('frozen_json') != frozen
            or entry.get('frozen_json') != frozen or run.get('claim_id') != job['claim_id']
            or any(run.get(k) != job['identity'][k] for k in ('task_id', 'group_id', 'task_run_id'))
            or run.get('receipt_sha256') != digest or worker.get('held_sha256') != digest
            or worker.get('receipt_json') != raw
            or not isinstance(canonical_sha, str) or not SHA.fullmatch(canonical_sha)):
        raise ValueError('original service configuration evidence is unavailable')
    return {'schema': 'paperflow-server-job-config/1', 'server_identity': request['server_identity'],
            'job_id': job['job_id'], 'receipt_sha256': digest, 'nonce': request['nonce'],
            'service_config_canonical_sha256': canonical_sha}


def main():
    if len(sys.argv) != 4:
        raise SystemExit('reviewed app directory and independently pinned config are required')
    try:
        app = Path(sys.argv[1])
        if not app.is_absolute() or '..' in app.parts:
            raise ValueError('reviewed app directory is unavailable')
        sys.path.insert(0, str(app))
        import server_job_service as service
        config = service.load_config(sys.argv[2], sys.argv[3])
        raw = sys.stdin.buffer.read(MAX_INPUT + 1)
        if len(raw) > MAX_INPUT:
            raise ValueError('version request is too large')
        request = json.loads(raw, object_pairs_hook=_pairs,
                             parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        sys.stdout.buffer.write(_json(lookup(config, request)).encode('utf8'))
        sys.stdout.buffer.flush()
    except Exception as exc:
        print('server version lookup failed: ' + type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
