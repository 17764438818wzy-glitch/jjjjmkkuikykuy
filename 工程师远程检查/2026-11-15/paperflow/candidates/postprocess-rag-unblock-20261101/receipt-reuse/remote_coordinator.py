"""Isolated local seam for one existing folder claim, never production wiring.

The caller owns the existing folder/Prefect capacity lease for this entire call.
There is no new scheduler, control DB copy, retry, LLM or RAG operation here.
Callbacks are trusted application boundaries and MUST obey these contracts:

* prepare_snapshot(claim): LOCAL ONLY, outside the task gate, freeze/verify the
  original manifest and all inputs. No network, upload or remote staging calls.
  Return JSON {snapshot_sha256, code_sha256, payload}. Never dispatch.
  The dependency must be configured for the selected server_identity. The payload
  is an opaque SMALL staging locator, not file bytes or a control DB. The
  coordinator adds server_identity before supplying snapshot to submit_once.
* transfer_snapshot(job, snapshot): transfer to NONEXECUTABLE staging, outside the
  task gate and only AFTER the durable transfer intent. Return job, server_identity,
  state='staged', snapshot_sha256, code_sha256, unchanged payload and a nonempty
  staging_receipt. No remote execution. Every ambiguous transfer is terminal here.
* validate_staging(job, snapshot, staging): outside the gate, independently verify
  authenticated server/ledger identity and the complete frozen snapshot represented
  by staging_receipt. Return exactly True without mutating arguments. Mere hashes
  and echoed identity fields do not authenticate a server acknowledgement.
* observe_execution(claim, job, clock): outside the gate, independently verify the real
  current Prefect stage context. Return identity, claim_id, generation, job_id,
  state='RUNNING', observed_at_ns (wall clock, display only), the unchanged clock,
  binding={nonempty persisted owner/version facts}. clock is minted BEFORE the
  observer call and binds the process boot token, PID, individual observation,
  monotonic start and fixed deadline; slow observations consume the same budget.
  Cached observations or clocks from another process cannot authorize start.
* bind_execution(claim, job, observation): inside the original task gate, ONLY
  bounded local reads to compare observation.binding with the original current
  stage ownership/version. No network, FlowRun observation or file hashing.
  Return exactly True. A timestamp alone is not an ownership binding.
* submit_once(job, snapshot, staging): a strictly bounded SMALL start request, inside the
  SAME admission gate used by pause/delete. No upload, hash, processing or wait.
  Return {job: job, accepted: True} after authenticated one-time admission.
* wait_for_result(job): outside the gate; obtain the result and pull artifacts
  into a safe local evidence area without rewriting any sealed JSON/paths.
* validate_result(job, result): outside the gate; independently verify the
  authenticated persistent server ledger/receipt, terminated execution, frozen
  original input coverage, whole body then each attachment, all local artifacts
  and business acceptance. Return exactly True without changing either argument.

Acknowledgement and result must also bind the selected server_identity (server_id
and persistent ledger_id). Result JSON must contain job, state='finished', server_validation='passed',
receipt, its canonical receipt_json and SHA256, plus a nonempty server_receipt.
Hashes are byte binding only: the mandatory validator supplies actual trust.
The explicitly initialized RemoteSubmissionJournal stores an immutable transfer
intent BEFORE any transport I/O. After authenticated staging it records the
linked start intent BEFORE the remote start request. Both admissions recheck the
original claim and task state under the SAME task gate; upload occurs outside it.
get_transfer(job_id) and get(job_id) are read-only. Neither intent authorizes retry,
including a crash between commit and sending. It is a separate local audit journal, not a copy or
replacement of FolderStageStore or its control DB. Unknown submissions and crashes
cannot be retried through this API: the original claim remains durable.
Evidence reconciliation is a separate
administrative action, deliberately absent here. Callback failures never replace
or edit saved receipts. No transport or snapshot implementation is supplied.
"""
from contextlib import closing, nullcontext
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
from uuid import UUID, uuid4, uuid5

from folder_state import FolderStageStore, StageConflict, _identity, _digest
from remote_jobs import NAMESPACE, _validated


EXECUTION_MAX_AGE_NS = 5_000_000_000
# A fresh import/process lifetime deliberately starts a new authority domain.
# PID additionally invalidates a fork inheriting this token. Historical journal
# records remain readable, but cannot authorize work in the current domain.
_PROCESS_BOOT_ID = str(uuid4())
START_METADATA_MAX_BYTES = 65_536
INTENT_SCHEMA = 'paperflow-remote-start-intent/1'
INTENT_REASON = 'start_once_outcome_unknown_until_verified'
TRANSFER_SCHEMA = 'paperflow-remote-transfer-intent/1'
TRANSFER_REASON = 'transfer_once_outcome_unknown_until_verified'
JOURNAL_SCHEMA = 'paperflow-remote-submission-journal/3'


def _encode(value):
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError('remote evidence must be finite JSON') from exc


def _detached(value):
    return json.loads(_encode(value))


def _require(condition, detail):
    if not condition:
        raise ValueError(detail)


def _uuid(value):
    try:
        return isinstance(value, str) and str(UUID(value)) == value
    except (ValueError, TypeError, AttributeError):
        return False


def _server_identity(value):
    value = _detached(value)
    _require(isinstance(value, dict) and set(value) == {'server_id', 'ledger_id'}
             and isinstance(value['server_id'], str) and 0 < len(value['server_id']) <= 256
             and value['server_id'].strip() == value['server_id'] and _uuid(value['ledger_id']),
             'explicit server and persistent ledger identity are required')
    return value


def _new_execution_clock():
    observed = time.monotonic_ns()
    _require(type(observed) is int and observed >= 0, 'local monotonic clock is unavailable')
    return dict(boot_id=_PROCESS_BOOT_ID, process_id=os.getpid(), observation_id=str(uuid4()),
                observed_monotonic_ns=observed, deadline_monotonic_ns=observed + EXECUTION_MAX_AGE_NS)


def _inspect_execution_clock(clock):
    _require(isinstance(clock, dict) and set(clock) == {'boot_id', 'process_id', 'observation_id',
             'observed_monotonic_ns', 'deadline_monotonic_ns'}
             and _uuid(clock['boot_id']) and _uuid(clock['observation_id'])
             and type(clock['process_id']) is int and clock['process_id'] > 0
             and type(clock['observed_monotonic_ns']) is int and clock['observed_monotonic_ns'] >= 0
             and type(clock['deadline_monotonic_ns']) is int
             and clock['deadline_monotonic_ns'] == clock['observed_monotonic_ns'] + EXECUTION_MAX_AGE_NS,
             'execution monotonic clock domain or fixed deadline is invalid')


def _execution_evidence(value, claim, job, *, fresh=True, clock=None):
    _require(isinstance(value, dict) and value.get('identity') == job['identity']
             and value.get('claim_id') == claim['claim_id'] and value.get('job_id') == job['job_id']
             and type(value.get('generation')) is int and value['generation'] == job['generation']
             and value.get('state') == 'RUNNING' and type(value.get('observed_at_ns')) is int
             and isinstance(value.get('binding'), dict) and value['binding'],
             'real execution context is not bound to the original claim')
    observed_clock = value.get('clock')
    _inspect_execution_clock(observed_clock)
    if fresh:
        _require(clock is not None and _encode(observed_clock) == _encode(clock)
                 and observed_clock['boot_id'] == _PROCESS_BOOT_ID
                 and observed_clock['process_id'] == os.getpid(),
                 'execution observation belongs to another process boot or clock domain')
        current = time.monotonic_ns()
        _require(type(current) is int and observed_clock['observed_monotonic_ns'] <= current
                 <= observed_clock['deadline_monotonic_ns'],
                 'execution monotonic observation is stale or cannot be verified')


def _inspect_binding(value):
    _require(isinstance(value, dict) and {'job', 'server_identity', 'snapshot', 'journal_id'} <= set(value)
             and _uuid(value['journal_id']), 'invalid immutable remote evidence binding')
    job = value['job']
    _validated(job)
    _server_identity(value['server_identity'])
    snapshot = value['snapshot']
    _require(isinstance(snapshot, dict)
             and set(snapshot) == {'snapshot_sha256', 'code_sha256', 'payload', 'server_identity'}
             and snapshot['snapshot_sha256'] == job['snapshot_sha256']
             and snapshot['code_sha256'] == job['code_sha256']
             and snapshot['server_identity'] == value['server_identity'], 'snapshot and job binding changed')


def _inspect_transfer_intent(value):
    _inspect_binding(value)
    _require(set(value) == {'schema', 'transfer_intent', 'job', 'server_identity', 'snapshot', 'journal_id'}
             and value['schema'] == TRANSFER_SCHEMA and value['transfer_intent'] == TRANSFER_REASON,
             'invalid immutable remote transfer intent')


def _inspect_staging(job, snapshot, server_identity, staging):
    _require(isinstance(staging, dict) and _encode(staging.get('job')) == _encode(job)
             and staging.get('server_identity') == server_identity and staging.get('state') == 'staged'
             and staging.get('snapshot_sha256') == job['snapshot_sha256']
             and staging.get('code_sha256') == job['code_sha256']
             and _encode(staging.get('payload')) == _encode(snapshot['payload'])
             and isinstance(staging.get('staging_receipt'), dict) and staging['staging_receipt'],
             'staging acknowledgement does not bind the complete frozen job')


def _inspect_intent(value):
    _inspect_binding(value)
    _require(set(value) == {'schema', 'submission_intent', 'job', 'server_identity', 'snapshot',
             'execution', 'journal_id', 'staging', 'transfer_intent_sha256'}
             and value['schema'] == INTENT_SCHEMA and value['submission_intent'] == INTENT_REASON,
             'invalid immutable remote start intent')
    _digest(value['transfer_intent_sha256'])
    _execution_evidence(value['execution'], value['job'], value['job'], fresh=False)
    _inspect_staging(value['job'], value['snapshot'], value['server_identity'], value['staging'])


class RemoteSubmissionJournal:
    """Immutable transfer/start intents; does not claim, dispatch or resolve work.

    initialize() is an explicit installation step on a NEW separate local file.
    Reopen using the returned journal_id; a missing/replaced journal fails closed.
    Only process_remote_folder writes intents. get()/get_transfer() never update a record or
    returns permission to retry. The original control DB remains authoritative.
    """
    def __init__(self, database, *, control_database, admission_gate, journal_id=None):
        self.database = Path(database).resolve()
        self.control_database = Path(control_database).resolve(strict=True)
        _require(self.database != self.control_database
                 and not (self.database.exists() and self.database.samefile(self.control_database)),
                 'remote intent journal must be separate from the control database')
        _require(callable(admission_gate), 'original task admission gate is required')
        _require(journal_id is None or _uuid(journal_id), 'invalid persistent journal identity')
        self.admission_gate = admission_gate
        self.journal_id = journal_id

    def initialize(self):
        """Create only a new journal; never alter an existing file or control DB."""
        journal_id = self.journal_id or str(uuid4())
        try:
            with self.database.open('xb'):
                pass
        except FileExistsError as exc:
            raise ValueError('remote intent journal already exists') from exc
        with closing(sqlite3.connect(self.database, timeout=3)) as db, db:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('BEGIN IMMEDIATE')
            db.execute('CREATE TABLE journal_settings(name TEXT PRIMARY KEY,value TEXT NOT NULL)')
            db.executemany('INSERT INTO journal_settings VALUES(?,?)',
                [('schema', JOURNAL_SCHEMA), ('control_database', str(self.control_database)),
                 ('journal_id', journal_id)])
            for table in ('remote_transfer_intents', 'remote_start_intents'):
                db.execute('CREATE TABLE ' + table + '('
                    'job_id TEXT PRIMARY KEY,claim_id TEXT NOT NULL UNIQUE,task_run_id TEXT NOT NULL UNIQUE,'
                    'task_id TEXT NOT NULL,group_id TEXT NOT NULL,intent_json TEXT NOT NULL,'
                    'intent_sha256 TEXT NOT NULL,created_at_ns INTEGER NOT NULL,UNIQUE(task_id,group_id))')
        self.journal_id = journal_id
        return journal_id

    def _connect(self, *, readonly):
        _require(_uuid(self.journal_id), 'explicitly initialized journal identity is required')
        try:
            db = sqlite3.connect(self.database.as_uri() + ('?mode=ro' if readonly else '?mode=rw'),
                                 uri=True, timeout=3)
            db.row_factory = sqlite3.Row
            settings = dict(db.execute('SELECT name,value FROM journal_settings'))
            if settings != {'schema': JOURNAL_SCHEMA, 'control_database': str(self.control_database),
                            'journal_id': self.journal_id}:
                raise ValueError('remote intent journal identity or control binding changed')
            if not readonly:
                db.execute('PRAGMA synchronous=FULL')
            return db
        except (OSError, sqlite3.Error, ValueError) as exc:
            if 'db' in locals():
                db.close()
            raise ValueError('remote intent journal is unavailable or changed') from exc

    def _ready(self, store):
        _require(self.control_database == Path(store.database).resolve()
                 and self.admission_gate is store.admission_gate,
                 'journal must bind the original control database and SAME task gate')
        with closing(self._connect(readonly=True)) as db:
            db.execute('SELECT 1 FROM remote_start_intents LIMIT 1')
            db.execute('SELECT 1 FROM remote_transfer_intents LIMIT 1')

    def _unreserved(self, identity):
        """Existing evidence blocks even a partially restored original control DB."""
        with closing(self._connect(readonly=True)) as db:
            for table in ('remote_transfer_intents', 'remote_start_intents'):
                existing = db.execute('SELECT 1 FROM ' + table + ' WHERE task_run_id=? '
                    'OR (task_id=? AND group_id=?) LIMIT 1',
                    (identity['task_run_id'], identity['task_id'], identity['group_id'])).fetchone()
                if existing:
                    raise StageConflict('durable remote intent exists; no new claim, upload or start is allowed')

    def _record(self, table, value, raw, digest):
        """Internal: caller holds the original task gate; no remote I/O here."""
        job, identity = value['job'], value['job']['identity']
        try:
            with closing(self._connect(readonly=False)) as db, db:
                db.execute('BEGIN IMMEDIATE')
                if table == 'remote_start_intents':
                    transfer = db.execute('SELECT * FROM remote_transfer_intents WHERE job_id=?',
                                          (job['job_id'],)).fetchone()
                    _require(transfer is not None and transfer['intent_sha256'] == value['transfer_intent_sha256']
                             and hashlib.sha256(transfer['intent_json'].encode('utf8')).hexdigest()
                             == value['transfer_intent_sha256'], 'matching prior transfer intent is missing or changed')
                    previous = json.loads(transfer['intent_json'])
                    _inspect_transfer_intent(previous)
                    _require(all(previous[key] == value[key] for key in
                        ('job', 'snapshot', 'server_identity', 'journal_id')), 'start changed the frozen transfer binding')
                db.execute('INSERT INTO ' + table + ' VALUES(?,?,?,?,?,?,?,?)',
                    (job['job_id'], job['claim_id'], identity['task_run_id'], identity['task_id'],
                     identity['group_id'], raw, digest, time.time_ns()))
        except sqlite3.IntegrityError as exc:
            raise ValueError('remote intent already exists; never transfer or submit again') from exc

    def _record_transfer(self, value, raw, digest):
        return self._record('remote_transfer_intents', value, raw, digest)

    def _record_start(self, value, raw, digest):
        return self._record('remote_start_intents', value, raw, digest)

    def _get(self, job_id, table, inspect):
        _require(_uuid(job_id), 'canonical remote job identity is required')
        with closing(self._connect(readonly=True)) as db:
            row = db.execute('SELECT * FROM ' + table + ' WHERE job_id=?', (job_id,)).fetchone()
        if row is None:
            return None
        raw = row['intent_json']
        _require(hashlib.sha256(raw.encode('utf8')).hexdigest() == row['intent_sha256'],
                 'durable remote start intent changed')
        value = json.loads(raw)
        inspect(value)
        job, identity = value['job'], value['job']['identity']
        _require(_encode(value) == raw and value['journal_id'] == self.journal_id
                 and job['job_id'] == job_id and job['claim_id'] == row['claim_id']
                 and identity['task_run_id'] == row['task_run_id']
                 and identity['task_id'] == row['task_id'] and identity['group_id'] == row['group_id'],
                 'durable intent identity changed')
        return value

    def get_transfer(self, job_id):
        """Read frozen upload identity even if no ACK/start exists. Never retransmit."""
        return self._get(job_id, 'remote_transfer_intents', _inspect_transfer_intent)

    def get(self, job_id):
        """Read a linked start intent, or None. Neither result authorizes start."""
        value = self._get(job_id, 'remote_start_intents', _inspect_intent)
        if value is not None:
            transfer = self.get_transfer(job_id)
            _require(transfer is not None and hashlib.sha256(_encode(transfer).encode('utf8')).hexdigest()
                     == value['transfer_intent_sha256']
                     and all(value[key] == transfer[key] for key in
                         ('job', 'snapshot', 'server_identity', 'journal_id')),
                     'start has no matching frozen transfer intent')
        return value


def _active_claim(store, identity, claim_id, generation, manifest_sha256):
    """Only local reads; caller holds the original task gate for admission."""
    with closing(store._connect(readonly=True)) as db:
        db.execute('BEGIN')
        owner = store._owned(db, identity, claim_id)
        if (owner['state'] != 'processing' or owner['generation'] != generation
                or owner['manifest_sha256'] != manifest_sha256
                or not store._active(db, identity, generation)):
            raise StageConflict('task paused, deleted, generation changed or claim no longer active')
        store._upstream(db, identity, manifest_sha256)


def _unchanged_call(callback, *values):
    """Callbacks cannot mutate a retained job, result, or caller-owned object."""
    before = [_encode(value) for value in values]
    supplied = [json.loads(raw) for raw in before]
    result = callback(*supplied)
    _require([_encode(value) for value in supplied] == before, 'dependency mutated frozen evidence')
    return _detached(result)


def _validate_envelope(job, result, server_identity):
    _require(isinstance(result, dict) and result.get('job') == job
             and _encode(result.get('job')) == _encode(job)
             and result.get('state') == 'finished' and result.get('server_validation') == 'passed'
             and result.get('server_identity') == server_identity
             and isinstance(result.get('server_receipt'), dict) and result['server_receipt'],
             'reliable server completion evidence is missing or mismatched')
    receipt = result.get('receipt')
    _require(isinstance(receipt, dict) and receipt.get('identity') == job['identity']
             and receipt.get('claim_id') == job['claim_id']
             and receipt.get('manifest_sha256') == job['manifest_sha256']
             and receipt.get('schema') == 'paperflow-postprocessing-receipt/1'
             and receipt.get('validation_status') == 'passed', 'remote folder receipt identity changed')
    raw = _encode(receipt)
    _require(result.get('receipt_json') == raw
             and result.get('receipt_sha256') == hashlib.sha256(raw.encode('utf8')).hexdigest(),
             'sealed remote receipt bytes changed')


def process_remote_folder(*, identity, generation, manifest_sha256, store,
                          prepare_snapshot, submit_once, wait_for_result, validate_result,
                          journal, server_identity, observe_execution, bind_execution,
                          transfer_snapshot, validate_staging, accepted_hook=None, admission_wait=None,
                          check_gate=None):
    """Claim locally once; stage, start once, pull and complete the SAME claim.

    Only the original initialized FolderStageStore is accepted. It must already
    bind the real control DB, independent FlowRun observer and original task
    admission gate. Missing dependencies fail before persisting a new claim.
    A successful return is the existing store's result (waiting_rag/receipt_held),
    never a new work order, new manifest or an assertion of RAG acceptance.
    """
    identity, manifest_sha256 = _identity(_detached(identity)), _digest(manifest_sha256)
    _require(type(generation) is int and generation >= 1, 'server generation must be a positive integer')
    _require(isinstance(store, FolderStageStore) and callable(store.admission_gate)
             and callable(store.observe_run), 'original gated folder stage store is required')
    _require(all(callable(value) for value in (prepare_snapshot, submit_once, wait_for_result,
                                              validate_result, observe_execution, bind_execution,
                                              transfer_snapshot, validate_staging)),
             'snapshot, bounded transport, fresh execution binding and full verifier are required')
    _require(accepted_hook is None or callable(accepted_hook), 'accepted hook must be callable')
    _require(check_gate is None or callable(check_gate), 'gate recheck must be callable')
    _require(admission_wait is None or (isinstance(admission_wait, dict)
             and set(admission_wait) == {'wait', 'max_attempts', 'check_gate'}
             and callable(admission_wait['wait']) and callable(admission_wait['check_gate'])
             and (admission_wait['max_attempts'] is None or
                  (type(admission_wait['max_attempts']) is int and 1 <= admission_wait['max_attempts'] <= 1000))),
             'capacity wait must be explicitly configured and independently provided')
    server_identity = _server_identity(server_identity)
    _require(isinstance(journal, RemoteSubmissionJournal), 'explicit local submission journal is required')
    journal._ready(store)
    journal._unreserved(identity)
    claim = store.claim(identity, generation, manifest_sha256)
    claim_id = claim['claim_id']
    try:
        snapshot = _unchanged_call(prepare_snapshot, claim)
        _require(isinstance(snapshot, dict)
                 and set(snapshot) == {'snapshot_sha256', 'code_sha256', 'payload'},
                 'nonexecuting staged snapshot evidence is incomplete')
        snapshot['server_identity'] = _detached(server_identity)
        job = dict(job_id=str(uuid5(NAMESPACE, identity['task_run_id'])), claim_id=claim_id,
                   identity=_detached(identity), generation=generation, manifest_sha256=manifest_sha256,
                   snapshot_sha256=snapshot['snapshot_sha256'], code_sha256=snapshot['code_sha256'])
        _validated(job)  # Same strict identity contract as the server job ledger.
        transfer_intent = dict(schema=TRANSFER_SCHEMA, transfer_intent=TRANSFER_REASON, job=_detached(job),
            server_identity=_detached(server_identity), snapshot=_detached(snapshot), journal_id=journal.journal_id)
        _inspect_transfer_intent(transfer_intent)
        transfer_raw = _encode(transfer_intent)
        _require(len(transfer_raw.encode('utf8')) <= START_METADATA_MAX_BYTES,
                 'transport metadata must be small; freeze file bytes locally before admission')
        transfer_digest = hashlib.sha256(transfer_raw.encode('utf8')).hexdigest()
        # No transport I/O has occurred. Persist exact upload identity first,
        # after a fresh pause/delete/claim check following potentially long freezing.
        with store.admission_gate(identity['task_id']):
            _active_claim(store, identity, claim_id, generation, manifest_sha256)
            journal._record_transfer(transfer_intent, transfer_raw, transfer_digest)
        # This potentially large transfer never holds a gate or SQLite transaction.
        staging = _unchanged_call(transfer_snapshot, job, snapshot)
        _inspect_staging(job, snapshot, server_identity, staging)
        _require(_unchanged_call(validate_staging, job, snapshot, staging) is True,
                 'complete staging was not independently authenticated')
        clock = _new_execution_clock()
        execution = _unchanged_call(observe_execution, claim, job, clock)
        _execution_evidence(execution, claim, job, clock=clock)
        intent = dict(schema=INTENT_SCHEMA, submission_intent=INTENT_REASON, job=_detached(job),
                      server_identity=_detached(server_identity), snapshot=_detached(snapshot),
                      execution=_detached(execution), journal_id=journal.journal_id,
                      staging=_detached(staging), transfer_intent_sha256=transfer_digest)
        _inspect_intent(intent)
        raw_intent = _encode(intent)
        _require(len(raw_intent.encode('utf8')) <= START_METADATA_MAX_BYTES,
                 'start metadata must be small; stage large payloads before admission')
        intent_sha256 = hashlib.sha256(raw_intent.encode('utf8')).hexdigest()
        # Large transfers and evidence checks have finished. Pause/delete is
        # serialized against the small, bounded start admission only.
        if admission_wait is None:
            with store.admission_gate(identity['task_id']):
                if check_gate is not None:
                    check_gate()
                _active_claim(store, identity, claim_id, generation, manifest_sha256)
                _execution_evidence(execution, claim, job, clock=clock)
                _require(_unchanged_call(bind_execution, claim, job, execution) is True,
                         'original execution ownership/version changed before start')
                _execution_evidence(execution, claim, job, clock=clock)
                journal._record_start(intent, raw_intent, intent_sha256)
                # The intent transaction has committed and CLOSED before remote I/O.
                _execution_evidence(execution, claim, job, clock=clock)
                if check_gate is not None:
                    check_gate()
                acknowledgement = _unchanged_call(submit_once, job, snapshot, staging)
                _require(isinstance(acknowledgement, dict) and acknowledgement.get('accepted') is True
                         and _encode(acknowledgement.get('job')) == _encode(job)
                         and acknowledgement.get('server_identity') == server_identity,
                         'remote start acknowledgement is ambiguous or mismatched')
        else:
            from admission_wait import wait_for_admission
            current = {'clock': clock, 'execution': execution, 'intent_recorded': False}
            def observe_attempt():
                admission_wait['check_gate']()
                if current['intent_recorded']:
                    current['clock'] = _new_execution_clock()
                    current['execution'] = _unchanged_call(observe_execution, claim, job, current['clock'])
                _execution_evidence(current['execution'], claim, job, clock=current['clock'])
            def check_attempt():
                admission_wait['check_gate']()
                _active_claim(store, identity, claim_id, generation, manifest_sha256)
                _execution_evidence(current['execution'], claim, job, clock=current['clock'])
                _require(_unchanged_call(bind_execution, claim, job, current['execution']) is True,
                         'original execution ownership/version changed before start')
                _execution_evidence(current['execution'], claim, job, clock=current['clock'])
                if not current['intent_recorded']:
                    journal._record_start(intent, raw_intent, intent_sha256)
                    current['intent_recorded'] = True
                else:
                    _require(_encode(journal.get(job['job_id'])) == raw_intent,
                             'the original durable start intent changed')
                _execution_evidence(current['execution'], claim, job, clock=current['clock'])
            def send_attempt():
                # This recheck narrows the close/start window; a strict global
                # close barrier still requires an explicit shared protocol.
                admission_wait['check_gate']()
                return _unchanged_call(submit_once, job, snapshot, staging)
            acknowledgement = wait_for_admission(job, server_identity,
                gate=lambda: store.admission_gate(identity['task_id']), before_gate=observe_attempt,
                check=check_attempt, send=send_attempt,
                wait=admission_wait['wait'], max_attempts=admission_wait['max_attempts'])
        result = _unchanged_call(wait_for_result, job)
        _validate_envelope(job, result, server_identity)
        expected = _encode(result['receipt'])
        try:
            accepted = _unchanged_call(validate_result, job, result)
        except StageConflict:
            raise
        except Exception as exc:
            raise StageConflict('remote receipt was not accepted') from exc
        if accepted is not True:
            raise StageConflict('remote receipt was not accepted')

        def validate(receipt):
            return _encode(receipt) == expected and store._observed(identity)

        inspector = getattr(store, 'inspect_receipt', None)
        scope = (inspector.prefetched([dict(identity, claim_id=claim_id, receipt_json=result['receipt_json'],
                    receipt_sha256=result['receipt_sha256'])], accepted_evidence=result['server_receipt'])
                 if inspector is not None else nullcontext())
        with scope:
            stage_result = store.complete(identity, claim_id, result['receipt'], validate_receipt=validate)
            if accepted_hook is not None:
                supplied = _detached(stage_result)
                original = _encode(supplied)
                accepted_hook(supplied, validate)
                _require(_encode(supplied) == original, 'accepted hook mutated the completed stage result')
        return stage_result
    except BaseException as exc:
        # The store only permits processing -> unknown. It cannot alter a held
        # or published receipt even if another actor accepted one concurrently.
        try:
            store.mark_unknown(identity, claim_id, type(exc).__name__)
        except Exception as record_error:
            exc.add_note('could not persist unknown outcome: ' + type(record_error).__name__)
        raise
