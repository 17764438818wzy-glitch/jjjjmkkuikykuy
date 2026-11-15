"""Durable stage facts for an existing folder FlowRun, never a dispatcher.

Only initialize() creates tables. The upstream LLM completion row is immutable
here. Production wiring must pass admission_gate(task_id), using the SAME short
preprocessing-{task_id}.lock as pause/delete. SQLite transactions also serialize
control writes, but do not replace that upstream lock contract. No validator,
file hashing, or FlowRun observation is performed under this gate/transaction.

observe_run(identity) must independently verify the actual folder FlowRun and
return exactly True. It confirms identity, not execution liveness.
summary() additionally requires observe_activity(identity, generation) for every
persisted processing row. It must independently obtain current FlowRun state AND
worker/process liveness, returning identity, generation, flow_run_state='RUNNING',
process_alive=True and observed_at_ns (local observer clock, at most 30 seconds
old). A cached FlowRun state alone is insufficient. Observers must use bounded
I/O; callbacks run outside SQLite transactions and admission gates. Missing,
stale, paused, suspended or unavailable activity makes postprocessing=None while
independently verified waiting_rag counts remain available. This read never
changes a claim or authorizes a retry. No Prefect client is imported here.
Artifact hashes establish byte identity, never semantic acceptance: completion
also requires the caller's real validate_receipt(receipt) acceptance function.
The caller must retain its folder/work locks during production acceptance.
"""
from contextlib import closing, nullcontext
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time
from uuid import UUID, uuid4


IDENTITY = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id',
            'parent_run_id', 'task_run_id')
COUNTS = ('waiting_postprocessing', 'postprocessing', 'waiting_rag', 'held', 'unresolved')
STATES = {'processing', 'unknown', 'failed', 'receipt_held', 'waiting_rag'}
ACTIVITY_MAX_AGE_NS = 30_000_000_000


class StageConflict(RuntimeError):
    """Evidence/control/ownership cannot safely authorize a stage transition."""


def _digest(value):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{64}', value):
        raise StageConflict('invalid evidence digest')
    return value


def _identity(value):
    if not isinstance(value, dict) or set(value) != set(IDENTITY):
        raise StageConflict('the complete seven-field folder identity is required')
    try:
        if any(str(UUID(value[key])) != value[key] for key in ('task_id', 'parent_run_id', 'task_run_id')):
            raise ValueError('UUID is not canonical')
    except (TypeError, ValueError, AttributeError) as exc:
        raise StageConflict('invalid folder FlowRun identity') from exc
    if (not isinstance(value['paper_id'], str) or not value['paper_id']
            or not isinstance(value['group_id'], str) or not re.fullmatch('[0-9a-f]{32}', value['group_id'])
            or not isinstance(value['attempt_id'], str)
            or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', value['attempt_id'])):
        raise StageConflict('invalid folder identity')
    _digest(value['receipt_id'])
    return dict(value)


def _encode(value):
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise StageConflict('receipt is not a finite JSON value') from exc


class FolderStageStore:
    def __init__(self, database, *, observe_run=None, observe_activity=None, admission_gate=None):
        self.database = Path(database).resolve()
        self.observe_run = observe_run
        self.observe_activity = observe_activity
        self.admission_gate = admission_gate

    def _connect(self, *, readonly=False):
        db = sqlite3.connect(self.database.as_uri() + ('?mode=ro' if readonly else '?mode=rw'),
                             uri=True, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    def _gate(self, task_id):
        return nullcontext() if self.admission_gate is None else self.admission_gate(task_id)

    def _observed(self, identity):
        try:
            return callable(self.observe_run) and self.observe_run(dict(identity)) is True
        except Exception:
            return False

    def initialize(self):
        """Explicit schema setup on an existing synthetic/selected control DB."""
        with closing(self._connect()) as db, db:
            db.execute('''CREATE TABLE IF NOT EXISTS postprocessing_claims (
                task_id TEXT NOT NULL,group_id TEXT NOT NULL,paper_id TEXT NOT NULL,
                receipt_id TEXT NOT NULL,attempt_id TEXT NOT NULL,parent_run_id TEXT NOT NULL,
                task_run_id TEXT NOT NULL UNIQUE,manifest_sha256 TEXT NOT NULL,
                claim_id TEXT NOT NULL UNIQUE,generation INTEGER NOT NULL,state TEXT NOT NULL,
                detail TEXT,created_at_ns INTEGER NOT NULL,updated_at_ns INTEGER NOT NULL,
                PRIMARY KEY(task_id,group_id))''')
            db.execute('''CREATE TABLE IF NOT EXISTS postprocessing_receipts (
                claim_id TEXT PRIMARY KEY,receipt_json TEXT NOT NULL,receipt_sha256 TEXT NOT NULL,
                created_at_ns INTEGER NOT NULL)''')

    @staticmethod
    def _active(db, identity, generation):
        task = db.execute('SELECT * FROM tasks WHERE task_id=?', (identity['task_id'],)).fetchone()
        deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (identity['task_id'],)).fetchone()
        return FolderStageStore._active_control(task, deleted, identity, generation)

    @staticmethod
    def _can_finish(db, identity, generation):
        task = db.execute('SELECT * FROM tasks WHERE task_id=?', (identity['task_id'],)).fetchone()
        deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (identity['task_id'],)).fetchone()
        if not task or deleted or task['task_status'] in {'deleting', 'delete_failed', 'unknown'}:
            return False
        if FolderStageStore._active_control(task, deleted, identity, generation):
            return True
        return bool(db.execute("SELECT 1 FROM paperflow_postprocess_stage_jobs WHERE task_id=? AND group_id=? "
                               "AND folder_run_id=? AND generation=? AND state='executing'",
                               (identity['task_id'], identity['group_id'], identity['task_run_id'], generation)).fetchone())

    @staticmethod
    def _active_control(task, deleted, identity, generation):
        return (type(generation) is int and generation >= 0 and task is not None and not deleted
                and task['desired_state'] == 'running' and task['start_generation'] == generation
                and not task['start_pending'] and task['task_status'] not in {'deleting', 'delete_failed', 'unknown'}
                and task['prefect_order_model'] == 'v2' and task['prefect_run_id'] == identity['parent_run_id'])

    def _processing_active(self, identity, stage, task, deleted):
        """Read fresh execution evidence, then recheck control after external I/O."""
        generation = stage['generation']
        if (not self._active_control(task, deleted, identity, generation)
                or not callable(self.observe_activity)):
            return False
        try:
            observed = self.observe_activity(dict(identity), generation)
            if (not isinstance(observed, dict) or observed.get('identity') != identity
                    or type(observed.get('generation')) is not int or observed['generation'] != generation
                    or observed.get('flow_run_state') != 'RUNNING' or observed.get('process_alive') is not True
                    or type(observed.get('observed_at_ns')) is not int
                    or not 0 <= time.time_ns() - observed['observed_at_ns'] <= ACTIVITY_MAX_AGE_NS):
                return False
        except Exception:
            return False
        with closing(self._connect(readonly=True)) as db:
            db.execute('BEGIN')
            if not self._active(db, identity, generation):
                return False
            current = self._owned(db, identity, stage['claim_id'])
            return (current['state'] == 'processing' and current['generation'] == generation
                    and current['updated_at_ns'] == stage['updated_at_ns'])

    @staticmethod
    def _upstream(db, identity, manifest_sha256):
        row = db.execute('SELECT * FROM llm_folder_queue WHERE task_id=? AND group_id=?',
                         (identity['task_id'], identity['group_id'])).fetchone()
        if (not row or any(row[key] != identity[key] for key in IDENTITY)
                or row['manifest_sha256'] != manifest_sha256 or row['state'] != 'waiting_postprocessing'):
            raise StageConflict('upstream folder completion or frozen identity changed')
        tables = {value[0] for value in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'paperflow_postprocess_stage_jobs' in tables:
            job = db.execute('SELECT * FROM paperflow_postprocess_stage_jobs WHERE task_id=? AND group_id=?',
                             (identity['task_id'], identity['group_id'])).fetchone()
            if job and ('source_retry_sha256' in job.keys() or 'paperflow_llm_retry_items' in tables):
                try:
                    from stage_runtime import _retry_source_snapshot_db
                    if (any(job[key] != identity[key] for key in IDENTITY)
                            or 'source_llm_generation' not in job.keys()
                            or job['source_llm_generation'] != row['generation']):
                        raise ValueError('stage source identity changed')
                    selected = _retry_source_snapshot_db(db, identity, row['generation'])
                    actual = selected['sha256'] if selected else None
                    frozen = job['source_retry_sha256'] if 'source_retry_sha256' in job.keys() else None
                    prefect_proof = (job['source_retry_prefect_sha256']
                                     if 'source_retry_prefect_sha256' in job.keys() else None)
                    if (actual != frozen or (actual is None) != (prefect_proof is None)
                            or (prefect_proof is not None
                                and not re.fullmatch('[0-9a-f]{64}', str(prefect_proof)))):
                        raise ValueError('retry source changed')
                except (ImportError, KeyError, TypeError, ValueError, RuntimeError, sqlite3.Error) as exc:
                    raise StageConflict('upstream retry source changed or cannot be checked') from exc
        return row

    @staticmethod
    def _owned(db, identity, claim_id):
        row = db.execute('SELECT * FROM postprocessing_claims WHERE task_id=? AND group_id=?',
                         (identity['task_id'], identity['group_id'])).fetchone()
        if (not row or row['claim_id'] != claim_id or any(row[key] != identity[key] for key in IDENTITY)
                or row['state'] not in STATES):
            raise StageConflict('folder stage owner or identity mismatch')
        return row

    @staticmethod
    def _result(row):
        return {'identity': {key: row[key] for key in IDENTITY},
                'claim_id': row['claim_id'], 'generation': row['generation'],
                'state': row['state'], 'manifest_sha256': row['manifest_sha256']}

    def claim(self, identity, generation, manifest_sha256):
        """Atomic one-time ownership. Existing/crashed/unknown rows never retry."""
        identity, manifest_sha256 = _identity(identity), _digest(manifest_sha256)
        if not self._observed(identity):
            raise StageConflict('actual folder FlowRun is not confirmed')
        with self._gate(identity['task_id']), closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            if not self._can_finish(db, identity, generation):
                raise StageConflict('task paused, deleted, or generation changed')
            self._upstream(db, identity, manifest_sha256)
            if db.execute('SELECT 1 FROM postprocessing_claims WHERE task_id=? AND group_id=?',
                          (identity['task_id'], identity['group_id'])).fetchone():
                raise StageConflict('folder already claimed; explicit reconciliation is required')
            claim_id, now = str(uuid4()), time.time_ns()
            try:
                db.execute('INSERT INTO postprocessing_claims VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                           tuple(identity[key] for key in IDENTITY) +
                           (manifest_sha256, claim_id, generation, 'processing', None, now, now))
            except sqlite3.IntegrityError as exc:
                raise StageConflict('folder FlowRun already owned') from exc
            return self._result(self._owned(db, identity, claim_id))

    def _accepted_receipt(self, identity, claim_id, receipt, validate_receipt):
        # Detach callbacks from the input, and reject validator mutation.
        encoded = _encode(receipt)
        value = json.loads(encoded)
        if not callable(validate_receipt):
            raise StageConflict('receipt identity or semantic validator is missing')
        try:
            self._inspect_receipt(value, identity, claim_id)
            if validate_receipt(value) is not True or _encode(value) != encoded:
                raise StageConflict('receipt was not accepted or validator changed it')
            self._inspect_receipt(value, identity, claim_id)
        except StageConflict:
            raise
        except Exception as exc:
            raise StageConflict('receipt acceptance or output evidence failed') from exc
        return encoded, hashlib.sha256(encoded.encode('utf8')).hexdigest()

    @staticmethod
    def _inspect_receipt(value, identity, claim_id):
        if (not isinstance(value, dict) or value.get('schema') != 'paperflow-postprocessing-receipt/1'
                or value.get('identity') != identity or value.get('claim_id') != claim_id
                or value.get('validation_status') != 'passed'):
            raise StageConflict('receipt identity or prior acceptance is invalid')
        _digest(value.get('manifest_sha256'))
        artifacts = value.get('artifacts')
        if not isinstance(artifacts, list) or not artifacts:
            raise StageConflict('receipt has no output evidence')
        seen = set()
        for artifact in artifacts:
            if not isinstance(artifact, dict) or not isinstance(artifact.get('path'), str):
                raise StageConflict('invalid artifact reference')
            path = Path(artifact['path'])
            if not path.is_absolute():
                raise StageConflict('artifact path must be absolute')
            resolved = path.resolve(strict=True)
            if resolved in seen:
                raise StageConflict('duplicate artifact reference')
            seen.add(resolved)
            with resolved.open('rb') as stream:
                actual = hashlib.file_digest(stream, 'sha256').hexdigest()
            if actual != _digest(artifact.get('sha256')):
                raise StageConflict('artifact bytes changed')

    def complete(self, identity, claim_id, receipt, *, validate_receipt):
        """Persist accepted safe outputs; publish only while original gate is active."""
        identity = _identity(identity)
        # Wrong owners are rejected before running an expensive validator.
        with closing(self._connect(readonly=True)) as db:
            owner = self._owned(db, identity, claim_id)
            if owner['state'] not in {'processing', 'receipt_held', 'waiting_rag'}:
                raise StageConflict('unknown/failed stages require manual evidence reconciliation')
        encoded, digest = self._accepted_receipt(identity, claim_id, receipt, validate_receipt)
        return self._save(identity, claim_id, encoded, digest, owner['generation'], reconcile=False)

    def _save(self, identity, claim_id, encoded, digest, generation, *, reconcile):
        observed = self._observed(identity)
        with self._gate(identity['task_id']), closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            owner = self._owned(db, identity, claim_id)
            value = json.loads(encoded)
            if value['manifest_sha256'] != owner['manifest_sha256']:
                raise StageConflict('receipt refers to another frozen manifest')
            self._upstream(db, identity, owner['manifest_sha256'])
            existing = db.execute('SELECT * FROM postprocessing_receipts WHERE claim_id=?', (claim_id,)).fetchone()
            if existing and (existing['receipt_sha256'] != digest or existing['receipt_json'] != encoded):
                raise StageConflict('a durable receipt cannot be overwritten')
            if owner['state'] == 'waiting_rag' and existing:
                return self._result(owner)
            if owner['state'] not in ({'receipt_held'} if reconcile else {'processing', 'receipt_held'}):
                raise StageConflict('stage changed while receipt was being verified')
            if reconcile and (not existing or not self._active(db, identity, generation) or not observed):
                raise StageConflict('saved receipt cannot yet be reconciled')
            active = observed and (self._active(db, identity, generation) if reconcile
                                   else self._can_finish(db, identity, generation))
            now = time.time_ns()
            db.execute('INSERT OR IGNORE INTO postprocessing_receipts VALUES(?,?,?,?)',
                       (claim_id, encoded, digest, now))
            state = 'waiting_rag' if active else 'receipt_held'
            changed = db.execute('UPDATE postprocessing_claims SET state=?,generation=?,updated_at_ns=? '
                                 'WHERE claim_id=? AND state=?',
                                 (state, generation, now, claim_id, owner['state']))
            if changed.rowcount != 1:
                raise StageConflict('folder stage changed concurrently')
            return self._result(self._owned(db, identity, claim_id))

    def reconcile(self, identity, claim_id, generation, *, validate_receipt):
        """Only revalidate a previously saved receipt; never rerun business work."""
        identity = _identity(identity)
        with closing(self._connect(readonly=True)) as db:
            owner = self._owned(db, identity, claim_id)
            saved = db.execute('SELECT * FROM postprocessing_receipts WHERE claim_id=?', (claim_id,)).fetchone()
            if owner['state'] != 'receipt_held' or not saved:
                raise StageConflict('no held safe receipt to reconcile')
            value = json.loads(saved['receipt_json'])
        encoded, digest = self._accepted_receipt(identity, claim_id, value, validate_receipt)
        if digest != saved['receipt_sha256']:
            raise StageConflict('saved receipt metadata changed')
        return self._save(identity, claim_id, encoded, digest, generation, reconcile=True)

    def mark_unknown(self, identity, claim_id, detail):
        identity = _identity(identity)
        if not isinstance(detail, str) or not detail.strip():
            raise StageConflict('unknown outcome needs a reason')
        with self._gate(identity['task_id']), closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            row = self._owned(db, identity, claim_id)
            if row['state'] != 'processing':
                raise StageConflict('only an unresolved running stage can become unknown')
            db.execute("UPDATE postprocessing_claims SET state='unknown',detail=?,updated_at_ns=? WHERE claim_id=?",
                       (detail, time.time_ns(), claim_id))
            return self._result(self._owned(db, identity, claim_id))

    def summary(self):
        """Pure read. Any unconfirmed evidence yields unknown counts, never zeros."""
        unknown = dict.fromkeys(COUNTS)
        if not callable(self.observe_run):
            return unknown
        try:
            with closing(self._connect(readonly=True)) as db:
                db.execute('BEGIN')
                rows = [dict(row) for row in db.execute('SELECT * FROM llm_folder_queue')]
                stages = {(row['task_id'], row['group_id']): dict(row) for row in db.execute(
                    'SELECT * FROM postprocessing_claims')}
                tasks = {row['task_id']: dict(row) for row in db.execute('SELECT * FROM tasks')}
                deleted = {row[0] for row in db.execute('SELECT task_id FROM task_deletions')}
                receipts = {row['claim_id']: dict(row) for row in db.execute('SELECT * FROM postprocessing_receipts')}
            counts, seen = dict.fromkeys(COUNTS, 0), set()
            for row in rows:
                key = (row['task_id'], row['group_id'])
                if key in seen or row['state'] not in {'waiting_llm', 'llm_processing', 'waiting_postprocessing'}:
                    return unknown
                seen.add(key)
                identity = _identity({field: row[field] for field in IDENTITY})
                task = tasks.get(row['task_id'])
                if row['task_id'] in deleted:
                    continue
                if (not task or task['prefect_run_id'] != identity['parent_run_id']
                        or task['prefect_order_model'] != 'v2' or not self._observed(identity)):
                    return unknown
                stage = stages.get(key)
                if stage is None:
                    counts['waiting_postprocessing'] += row['state'] == 'waiting_postprocessing'
                    continue
                if (stage['state'] not in STATES or row['state'] != 'waiting_postprocessing'
                        or any(stage[field] != row[field] for field in IDENTITY)
                        or stage['manifest_sha256'] != row['manifest_sha256']):
                    return unknown
                if stage['state'] in {'waiting_rag', 'receipt_held'}:
                    proof = receipts.get(stage['claim_id'])
                    if not proof or hashlib.sha256(proof['receipt_json'].encode('utf8')).hexdigest() != proof['receipt_sha256']:
                        return unknown
                    saved = json.loads(proof['receipt_json'])
                    self._inspect_receipt(saved, identity, stage['claim_id'])
                    if saved['manifest_sha256'] != stage['manifest_sha256']:
                        return unknown
                bucket = {'processing': 'postprocessing', 'waiting_rag': 'waiting_rag',
                          'receipt_held': 'held', 'unknown': 'unresolved', 'failed': 'unresolved'}[stage['state']]
                if stage['state'] == 'processing' and not self._processing_active(identity, stage, task, False):
                    counts['postprocessing'] = None
                elif counts[bucket] is not None:
                    counts[bucket] += 1
            if set(stages) - seen:
                return unknown
            return counts
        except (OSError, sqlite3.Error, StageConflict, ValueError, KeyError, TypeError):
            return unknown
