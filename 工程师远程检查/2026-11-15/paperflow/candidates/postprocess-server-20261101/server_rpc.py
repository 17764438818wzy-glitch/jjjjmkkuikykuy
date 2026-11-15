"""One bounded SSH-invoked server operation; no queue scan or automatic retry.

This candidate is inert until a host-key-pinned SSH endpoint and independently
pinned service configuration are installed. It does not itself authenticate
the SSH peer, establish pause semantics, or grant RAG access.
"""
from contextlib import closing
import json
import sqlite3
import sys

from remote_jobs import _uuid, _validated
from server_service_descriptor import create_descriptor
from server_receipt_query import inspect_requests, _finished
import server_job_service as service


def execute(operation, value, config):
    if not isinstance(value, dict):
        raise ValueError('one bounded request object is required')
    if operation == 'probe':
        if (set(value) != {'server_identity', 'nonce'}
                or value['server_identity'] != config['server_identity']
                or not _uuid(value['nonce'])):
            raise ValueError('the exact server and one fresh nonce are required')
        # A probe verifies the fixed persistent ledger identity only. It does
        # not start a job, validate outputs, infer capacity, or grant RAG access.
        ledger = service._ledger(config)
        with closing(sqlite3.connect(ledger.database.as_uri() + '?mode=ro',
                                     timeout=ledger.busy_timeout, uri=True)) as db:
            values = dict(db.execute('SELECT name,value FROM executor_settings'))
            if (values.get('capacity') != str(ledger.capacity)
                    or values.get('ledger_id') != config['server_identity']['ledger_id']):
                raise ValueError('the persistent server ledger identity changed')
            db.execute('SELECT 1 FROM executor_jobs LIMIT 1').fetchone()
        return dict(schema='paperflow-server-probe/1', server_identity=config['server_identity'],
                    nonce=value['nonce'], ledger_identity_verified=True)
    if operation == 'descriptor':
        if set(value) != {'job', 'staging_receipt'}:
            raise ValueError('descriptor request is invalid')
        return create_descriptor(job=value['job'], staging_receipt=value['staging_receipt'],
            server_identity=config['server_identity'], staging_root=config['staging_root'],
            jobs_root=config['jobs_root'])
    if operation == 'preflight':
        if set(value) != {'job'}:
            raise ValueError('preflight request is invalid')
        return service.preflight_job(value['job'], config)
    if operation == 'admit':
        if set(value) != {'job', 'preflight_id', 'preflight_sha256'}:
            raise ValueError('admission request is invalid')
        return service.admit_job(value['job'], config, preflight_id=value['preflight_id'],
                                 preflight_sha256=value['preflight_sha256'])
    if operation == 'status':
        if set(value) != {'job'}:
            raise ValueError('status request is invalid')
        job = value['job']
        frozen = _validated(job)
        current = service._ledger(config).status(job['job_id'])
        if (current['frozen_json'] != frozen or current['claim_id'] != job['claim_id']
                or any(current[key] != job['identity'][key] for key in ('task_id', 'group_id', 'task_run_id'))):
            raise ValueError('server ledger belongs to another job')
        state, ready = current['state'], False
        if state == 'finished':
            # A finished executor ledger precedes the service's final cgroup
            # and receipt proof. Poll this read-only state; never resubmit.
            ledger = service._ledger(config)
            with closing(ledger._connect()) as db:
                row = db.execute('SELECT phase FROM service_jobs WHERE job_id=?', (job['job_id'],)).fetchone()
            if row and row['phase'] == 'finished':
                _finished(ledger, job, config)
                ready = True
            elif row and row['phase'] in {'preparing', 'running'}:
                state = 'finishing'
            else:
                state = 'unknown'
        return dict(job=job, server_identity=config['server_identity'], state=state,
                    ledger_state=current['state'], receipt_ready=ready,
                    worker_liveness='not_independently_observed')
    if operation == 'inspect':
        if set(value) != {'requests'}:
            raise ValueError('receipt request is invalid')
        return inspect_requests(config, value['requests'])
    if operation == 'result':
        if set(value) != {'job'}:
            raise ValueError('result request is invalid')
        job = value['job']
        _validated(job)
        # Read only the original durable receipt, after all finished identities
        # and the final empty/removed cgroup proof have been checked. Inspection
        # rereads that proof and the actual fixed business result/artifacts.
        raw, digest, receipt = _finished(service._ledger(config), job, config)
        request = dict(job=job, server_identity=config['server_identity'],
                       receipt_json=raw, receipt_sha256=digest)
        inspected = inspect_requests(config, [request])
        if not isinstance(inspected, list) or len(inspected) != 1:
            raise ValueError('complete result inspection is unavailable')
        evidence = inspected[0]
        expected = dict(schema='paperflow-authenticated-remote-result/1', **request,
            ledger_state='finished', worker_terminated=True,
            business_validation='processor_reported_pass', business_code_sha256=job['code_sha256'])
        if not isinstance(evidence, dict) or any(evidence.get(k) != v for k, v in expected.items()):
            raise ValueError('result inspection changed the original durable receipt')
        return dict(job=job, server_identity=config['server_identity'], state='finished',
            server_validation='passed', receipt=receipt, receipt_json=raw,
            receipt_sha256=digest, server_receipt=evidence)
    raise ValueError('unsupported server operation')


def main():
    if len(sys.argv) != 4 or sys.argv[3] not in {'probe', 'descriptor', 'preflight', 'admit', 'status', 'inspect', 'result'}:
        raise SystemExit('pinned configuration, SHA256 and fixed operation are required')
    try:
        config = service.load_config(sys.argv[1], sys.argv[2])
        raw = sys.stdin.buffer.read(16 * 1024 * 1024 + 1)
        if len(raw) > 16 * 1024 * 1024:
            raise ValueError('request is too large')
        response = json.dumps(execute(sys.argv[3], json.loads(raw), config), ensure_ascii=False,
                              sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf8')
        if len(response) > 16 * 1024 * 1024:
            raise ValueError('response is too large')
        sys.stdout.buffer.write(response); sys.stdout.buffer.flush()
    except Exception as exc:
        print('server request failed: ' + type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
