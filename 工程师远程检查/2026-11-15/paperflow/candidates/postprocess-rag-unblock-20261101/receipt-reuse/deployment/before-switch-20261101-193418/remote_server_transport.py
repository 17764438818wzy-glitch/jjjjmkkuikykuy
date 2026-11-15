"""Inert callbacks for the EXISTING per-folder remote coordinator, not a queue.

The original coordinator/journal must own every call. Its SAME existing task
gate places transfer/descriptor/preflight/result checks outside admission and
only the small admit call inside it. This module adds no gate, lock, scheduler,
work order, retry or automatic reconciliation. Direct callback use cannot prove
pause authority and is not a supported production entry point. Never replay a
callback after a crash: reentering the coordinator is blocked by its original
durable transfer/start intents. Private contexts are deliberately not restored.

SSH credentials and the host-key SHA must be independently provisioned. A key
path is configuration, not evidence that authentication works. The fixed server
config SHA must select the audited service/native/business inventory. A trusted
same-UID server administrator is assumed; hashes are not remote attestation.
All operations return small metadata, never paper/output bytes or RAG admission.

Staging and RPC run in killable child processes. Timeout means UNKNOWN, never
permission to resend. Killing the local helper does not cancel bytes already
received by the server. In particular, physical start before a later task pause
is NOT guaranteed for an admit already sent or accepted after ACK loss. No
server-side pause/fencing protocol or submitted-task recovery is provided.
"""
import copy
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import re
import shlex
import sys
import time
from uuid import uuid4

from remote_jobs import _uuid, _validated
from snapshot_wire import snapshot_sha
from ssh_rpc_client import call_bounded, _run_bounded


class TransportError(RuntimeError):
    """Unconfirmed transfer/start/result; never implies safe retry."""


def _require(value, detail):
    if not value:
        raise TransportError(detail)


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _copy(value):
    return json.loads(_json(value))


def _sha(value):
    return isinstance(value, str) and re.fullmatch('[a-f0-9]{64}', value) is not None


def _remote_path(value):
    _require(isinstance(value, str) and value.startswith('/') and '\\' not in value
        and not any(c in value for c in '\r\n\0') and '..' not in PurePosixPath(value).parts
        and str(PurePosixPath(value)) == value, 'fixed canonical Linux path required')
    return value


def stage_bounded(connection, fixed_command, manifest, *, allowed_roots, timeout, verify_locally, trusted_files=None):
    """One whole-call staging deadline, including connect/exec/upload/ACK.

    The existing helper runner also allows up to two seconds to reap a killed
    helper; inability to confirm that termination remains unknown. No retry.
    """
    _require(type(timeout) in (int, float) and math.isfinite(timeout) and 0 < timeout <= 7200
             and type(verify_locally) is bool, 'bounded staging timeout and verification policy required')
    helper = Path(__file__).with_name('ssh_stage_process.py').resolve(strict=True)
    request = dict(connection=connection, command=fixed_command, manifest=manifest,
        allowed_roots=allowed_roots, io_timeout=min(timeout, 30), verify_locally=verify_locally,
        trusted_files=[] if trusted_files is None else trusted_files)
    try:
        return _run_bounded([sys.executable, '-B', str(helper)], request,
                            timeout=timeout, max_bytes=16 * 1024 * 1024)
    except Exception as exc:
        raise TransportError('staging outcome is unknown; never retransmit') from exc


class RemoteServerTransport:
    def __init__(self, config, *, config_sha256, journal, stage_call=stage_bounded, rpc_call=call_bounded):
        frozen = _copy(config)
        _require(_sha(config_sha256) and hashlib.sha256(_json(frozen).encode('utf8')).hexdigest() == config_sha256,
                 'independently pinned canonical transport configuration required')
        fields = {'schema', 'server_identity', 'connection', 'server_python', 'staging_script', 'rpc_script',
            'service_config', 'service_config_sha256', 'staging_root', 'allowed_roots',
            'staging_timeout_seconds', 'metadata_timeout_seconds', 'admit_timeout_seconds',
            'wait_timeout_seconds', 'poll_seconds'}
        _require(fields <= set(frozen) and set(frozen) <= fields | {'trusted_files'}
                 and frozen['schema'] == 'paperflow-server-transport/1',
                 'complete explicit transport configuration required')
        server = frozen['server_identity']
        _require(isinstance(server, dict) and set(server) == {'server_id', 'ledger_id'}
                 and isinstance(server['server_id'], str) and 0 < len(server['server_id']) <= 256
                 and server['server_id'].strip() == server['server_id'] and _uuid(server['ledger_id']),
                 'fixed server and persistent ledger identity required')
        connection = frozen['connection']
        _require(isinstance(connection, dict)
            and set(connection) == {'host', 'port', 'username', 'private_key', 'server_key_sha256'}
            and isinstance(connection['host'], str) and connection['host']
            and type(connection['port']) is int and 1 <= connection['port'] <= 65535
            and isinstance(connection['username'], str) and connection['username']
            and isinstance(connection['private_key'], str) and Path(connection['private_key']).is_absolute()
            and _sha(connection['server_key_sha256']), 'explicit SSH key path and host-key fingerprint required')
        for field in ('server_python', 'staging_script', 'rpc_script', 'service_config', 'staging_root'):
            _remote_path(frozen[field])
        _require(_sha(frozen['service_config_sha256']), 'independently pinned server service configuration required')
        _require(isinstance(frozen['allowed_roots'], list) and frozen['allowed_roots']
            and all(isinstance(path, str) and Path(path).is_absolute() for path in frozen['allowed_roots']),
            'original independent local source roots required')
        frozen.setdefault('trusted_files', [])
        _require(isinstance(frozen['trusted_files'], list), 'independent exact-file policy required')
        paths = set()
        for row in frozen['trusted_files']:
            _require(isinstance(row, dict) and set(row) == {'path', 'sha256'}
                and isinstance(row['path'], str) and Path(row['path']).is_absolute() and _sha(row['sha256'])
                and row['path'].casefold() not in paths, 'unique exact path and SHA authority required')
            paths.add(row['path'].casefold())
        for key, maximum in [('staging_timeout_seconds', 7200), ('metadata_timeout_seconds', 120),
            ('admit_timeout_seconds', 3), ('wait_timeout_seconds', 7200), ('poll_seconds', 5)]:
            value = frozen[key]
            _require(type(value) in (int, float) and math.isfinite(value) and 0 < value <= maximum,
                     'explicit bounded whole-call time budgets required')
        _require(_uuid(getattr(journal, 'journal_id', None)) and callable(getattr(journal, 'get_transfer', None))
                 and callable(getattr(journal, 'get', None)), 'original initialized remote intent journal required')
        _require(callable(stage_call) and callable(rpc_call), 'bounded authenticated operations required')
        self._config, self._server, self._journal = frozen, server, journal
        self._journal_id = journal.journal_id
        self._stage_call, self._rpc_call = stage_call, rpc_call
        self._contexts, self._submitted = {}, {}

    def _intent(self, job, snapshot=None, staging=None, *, started=False):
        _validated(job)
        _require(self._journal.journal_id == self._journal_id, 'original journal identity changed')
        value = self._journal.get(job['job_id']) if started else self._journal.get_transfer(job['job_id'])
        _require(isinstance(value, dict) and value.get('job') == job
            and value.get('server_identity') == self._server and value.get('journal_id') == self._journal_id,
            'matching original durable intent required; do not reconstruct or retry')
        if snapshot is not None:
            _require(value.get('snapshot') == snapshot, 'frozen transfer snapshot changed')
        if staging is not None:
            _require(value.get('staging') == staging, 'frozen start staging changed')
        return value

    def _rpc(self, operation, payload, *, timeout=None, small=False):
        config = self._config
        command = shlex.join([config['server_python'], '-B', config['rpc_script'],
                              config['service_config'], config['service_config_sha256'], operation])
        supplied = _copy(payload)
        try:
            result = self._rpc_call(_copy(config['connection']), command, supplied,
                timeout=timeout if timeout is not None else config['metadata_timeout_seconds'],
                max_bytes=65536 if small else 16 * 1024 * 1024)
            _require(supplied == payload, 'RPC mutated original metadata')
            return _copy(result)
        except Exception as exc:
            raise TransportError('server metadata outcome is unknown; no automatic retry') from exc

    def _bound(self, result, job):
        _require(isinstance(result, dict) and result.get('job') == job
            and result.get('server_identity') == self._server, 'authenticated server/job identity mismatch')

    def probe_server(self, server_identity):
        """Authenticate a fresh fixed-ledger round trip; make no readiness claim."""
        _require(server_identity == self._server, 'server identity differs from the configured transport')
        nonce = str(uuid4())
        result = self._rpc('probe', dict(server_identity=_copy(self._server), nonce=nonce), small=True)
        _require(isinstance(result, dict) and result.get('ledger_identity_verified') is True
                 and result == dict(schema='paperflow-server-probe/1', server_identity=self._server,
                                    nonce=nonce, ledger_identity_verified=True),
                 'the current server ledger observation is incomplete or changed')
        return True

    def transfer_snapshot(self, job, snapshot, manifest, *, verify_locally=False, trusted_files=None):
        self._intent(job, snapshot)
        _require(self._journal.get(job['job_id']) is None, 'prior start intent forbids upload')
        permitted_files = [] if trusted_files is None else _copy(trusted_files)
        _require(permitted_files == self._config['trusted_files'],
                 'runtime exact-file requests differ from independently frozen authority')
        _require(snapshot.get('server_identity') == self._server
            and snapshot.get('snapshot_sha256') == job['snapshot_sha256']
            and snapshot.get('code_sha256') == job['code_sha256']
            and snapshot_sha(manifest) == job['snapshot_sha256']
            and all(manifest.get(key) == job[key] for key in
                    ('job_id', 'claim_id', 'identity', 'generation', 'manifest_sha256', 'code_sha256')),
            'original manifest/job binding changed')
        # setdefault claims this per-job in-memory context before any I/O. It
        # is not a new admission lock; the original coordinator owns the claim.
        context = dict(state='uploading')
        _require(self._contexts.setdefault(job['job_id'], context) is context,
                 'this upload was already attempted; never repeat it')
        try:
            command = shlex.join([self._config['server_python'], '-B', self._config['staging_script'],
                                  self._config['staging_root']])
            supplied = _copy(manifest)
            ack = self._stage_call(_copy(self._config['connection']), command, supplied,
                allowed_roots=_copy(self._config['allowed_roots']), timeout=self._config['staging_timeout_seconds'],
                verify_locally=verify_locally, trusted_files=permitted_files)
            _require(supplied == manifest, 'staging mutated the frozen manifest')
            _require(isinstance(ack, dict) and ack.get('schema') == 'paperflow-remote-snapshot-verified/1'
                and all(ack.get(key) == value for key, value in job.items())
                and ack.get('files') == len(manifest['files']) and ack.get('evidence_scope') == 'declared_files_only'
                and ack.get('dependency_closure_verified') is False, 'original complete staging ACK is unavailable')
            descriptor = self._rpc('descriptor', dict(job=job, staging_receipt=ack))
            self._bound(descriptor, job)
            _require(_sha(descriptor.get('descriptor_sha256')), 'job-bound private descriptor was not created')
            staging = dict(job=_copy(job), server_identity=_copy(self._server), state='staged',
                snapshot_sha256=job['snapshot_sha256'], code_sha256=job['code_sha256'],
                payload=_copy(snapshot['payload']), staging_receipt=_copy(ack))
            context.update(state='staged', staging=_copy(staging))
            return staging
        except Exception as exc:
            context['state'] = 'unknown'
            raise TransportError('transfer/descriptor outcome is unknown; never retransmit') from exc

    def validate_staging(self, job, snapshot, staging):
        self._intent(job, snapshot)
        context = self._contexts.get(job['job_id'])
        _require(context and context['state'] == 'staged' and context['staging'] == staging,
                 'only the original completed upload can be preflighted once')
        context['state'] = 'preflighting'
        try:
            preflight = self._rpc('preflight', dict(job=job))
            self._bound(preflight, job)
            _require(_uuid(preflight.get('preflight_id')) and _sha(preflight.get('preflight_sha256')),
                     'exact original preflight receipt required')
            context.update(state='prepared', preflight=preflight)
            return True
        except Exception:
            context['state'] = 'unknown'
            raise

    def submit_once(self, job, snapshot, staging):
        # This is the ONLY task-gated callback. No upload, preflight, model hash
        # or result wait occurs here. Consume before sending, even on ACK loss.
        self._intent(job, snapshot, staging, started=True)
        context = self._contexts.pop(job['job_id'], None)
        _require(context and context['state'] == 'prepared' and context['staging'] == staging,
                 'no unconsumed original preflight; never retry or restore context')
        submitted = dict(job=_copy(job), state='unknown')
        _require(self._submitted.setdefault(job['job_id'], submitted) is submitted, 'admission already attempted')
        p = context['preflight']
        response = self._rpc('admit', dict(job=job, preflight_id=p['preflight_id'], preflight_sha256=p['preflight_sha256']),
                             timeout=self._config['admit_timeout_seconds'], small=True)
        self._bound(response, job)
        _require(response.get('accepted') is True, 'admission unconfirmed; never resend')
        submitted['state'] = 'accepted'
        return response

    def status(self, job, *, timeout=None):
        _validated(job)
        result = self._rpc('status', dict(job=job), timeout=timeout)
        self._bound(result, job)
        _require(result.get('state') in {'reserved', 'running', 'unknown', 'finishing', 'finished'}
            and type(result.get('receipt_ready')) is bool
            and (result['receipt_ready'] is False or result['state'] == 'finished'), 'invalid read-only status')
        # worker_liveness is deliberately not promoted into fresh server_alive.
        return result

    def _inspection(self, job, raw, digest, value):
        self._bound(value, job)
        _require(value.get('schema') == 'paperflow-authenticated-remote-result/1'
            and value.get('receipt_json') == raw and value.get('receipt_sha256') == digest
            and value.get('ledger_state') == 'finished' and value.get('worker_terminated') is True
            and value.get('business_validation') == 'processor_reported_pass'
            and value.get('business_code_sha256') == job['code_sha256'], 'complete durable business proof required')
        mapping = value.get('artifact_map')
        _require(isinstance(mapping, dict) and mapping.get('schema') == 'paperflow-linux-artifact-map/1'
            and mapping.get('integrity_status') == 'passed' and mapping.get('job') == job
            and mapping.get('receipt_canonical_sha256') == digest and mapping.get('rag_admission') == 'not_granted',
            'original server artifact inspection required')

    def _result(self, job, result):
        self._bound(result, job)
        receipt = result.get('receipt')
        _require(result.get('state') == 'finished' and result.get('server_validation') == 'passed'
            and isinstance(receipt, dict) and receipt.get('schema') == 'paperflow-postprocessing-receipt/1'
            and receipt.get('identity') == job['identity'] and receipt.get('claim_id') == job['claim_id']
            and receipt.get('manifest_sha256') == job['manifest_sha256'] and receipt.get('validation_status') == 'passed',
            'original finished receipt identity required')
        raw = _json(receipt); digest = hashlib.sha256(raw.encode('utf8')).hexdigest()
        _require(result.get('receipt_json') == raw and result.get('receipt_sha256') == digest,
                 'original canonical receipt bytes changed')
        self._inspection(job, raw, digest, result.get('server_receipt'))

    def wait_for_result(self, job):
        submitted = self._submitted.get(job['job_id'])
        _require(submitted and submitted['job'] == job and submitted['state'] == 'accepted',
                 'no successful current-process admission; recovery requires separate review')
        deadline = time.monotonic() + self._config['wait_timeout_seconds']
        try:
            while True:
                remaining = deadline - time.monotonic()
                _require(remaining > 0, 'read-only result wait expired; outcome remains unknown')
                status = self.status(job, timeout=min(remaining, self._config['metadata_timeout_seconds']))
                _require(status['state'] != 'unknown', 'server outcome is unknown; stop polling and never restart')
                if status['receipt_ready']:
                    remaining = deadline - time.monotonic()
                    _require(remaining > 0, 'read-only result wait expired')
                    result = self._rpc('result', dict(job=job), timeout=min(remaining, self._config['metadata_timeout_seconds']))
                    self._result(job, result)
                    submitted.update(state='fetched', result_json=_json(result))
                    return result
                time.sleep(max(0, min(self._config['poll_seconds'], deadline - time.monotonic())))
        except Exception:
            submitted['state'] = 'unknown'
            raise

    def inspect_batch(self, requests):
        _require(isinstance(requests, list) and 1 <= len(requests) <= 32, 'bounded nonempty metadata batch required')
        for request in requests:
            _require(isinstance(request, dict) and set(request) == {'job', 'server_identity', 'receipt_json', 'receipt_sha256'}
                and request['server_identity'] == self._server, 'inspection server binding changed')
            _validated(request['job'])
            raw = request['receipt_json']
            _require(isinstance(raw, str) and _json(json.loads(raw)) == raw
                and hashlib.sha256(raw.encode('utf8')).hexdigest() == request['receipt_sha256'], 'original receipt bytes required')
        results = self._rpc('inspect', dict(requests=requests))
        _require(isinstance(results, list) and len(results) == len(requests), 'complete server inspection batch required')
        for request, result in zip(requests, results):
            self._inspection(request['job'], request['receipt_json'], request['receipt_sha256'], result)
        return results

    def validate_result(self, job, result):
        submitted = self._submitted.get(job['job_id'])
        _require(submitted and submitted['state'] == 'fetched' and submitted['result_json'] == _json(result),
                 'result was not fetched through the original admission')
        self._result(job, result)
        inspected = self.inspect_batch([dict(job=job, server_identity=self._server,
            receipt_json=result['receipt_json'], receipt_sha256=result['receipt_sha256'])])[0]
        _require(inspected == result['server_receipt'], 'fresh inspection changed original result evidence')
        return True

    def coordinator_callbacks(self):
        """Pass ONLY to the original RemoteRuntimeCoordinator, never call as a queue."""
        return {name: getattr(self, name) for name in ('transfer_snapshot', 'validate_staging',
            'submit_once', 'wait_for_result', 'validate_result')}
