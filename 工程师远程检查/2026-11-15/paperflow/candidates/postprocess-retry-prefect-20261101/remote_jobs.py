"""Server-only execution ledger; never a replacement for the control database.

The Windows control side must obtain its existing claim before invoking this
ledger.  An unknown server job keeps its slot until separately reconciled.
There is intentionally no retry, scheduler, LLM or RAG operation here.
"""
from contextlib import closing
import json
import math
from pathlib import Path
import re
import sqlite3
import time
from uuid import UUID, uuid4, uuid5


FIELDS = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id',
          'parent_run_id', 'task_run_id')
JOB_FIELDS = ('job_id', 'claim_id', 'identity', 'generation', 'manifest_sha256',
              'snapshot_sha256', 'code_sha256')
SHA = re.compile(r'^[0-9a-f]{64}$')
NAMESPACE = UUID('5e9b7732-458e-50cb-8916-6b53007c204f')


class JobConflict(ValueError):
    pass


class CapacityFull(RuntimeError):
    pass


class AdmissionBusy(RuntimeError):
    """The reservation transaction could not start; this attempt did not launch."""


def _uuid(value):
    try:
        return isinstance(value, str) and str(UUID(value)) == value
    except (ValueError, AttributeError):
        return False


def _validated(item):
    if not isinstance(item, dict) or set(item) != set(JOB_FIELDS):
        raise JobConflict('complete execution identity required')
    identity = item['identity']
    if (not _uuid(item['job_id']) or not _uuid(item['claim_id'])
            or not isinstance(identity, dict) or set(identity) != set(FIELDS)
            or any(not _uuid(identity[k]) for k in ('task_id', 'parent_run_id', 'task_run_id'))
            or not isinstance(identity['group_id'], str)
            or not re.fullmatch(r'[0-9a-f]{32}', identity['group_id'])
            or not isinstance(identity['paper_id'], str) or not identity['paper_id']
            or not isinstance(identity['receipt_id'], str) or not SHA.fullmatch(identity['receipt_id'])
            or not isinstance(identity['attempt_id'], str)
            or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', identity['attempt_id'])
            or type(item['generation']) is not int or item['generation'] < 1
            or any(not isinstance(item[k], str) or not SHA.fullmatch(item[k])
                   for k in ('manifest_sha256', 'snapshot_sha256', 'code_sha256'))):
        raise JobConflict('execution identity is invalid')
    return json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


class RemoteJobs:
    def __init__(self, database, *, capacity=8, ledger_id=None, busy_timeout=30):
        if type(capacity) is not int or not 1 <= capacity <= 8:
            raise ValueError('server capacity must be between 1 and 8')
        self.database = Path(database).absolute()
        self.capacity = capacity
        if (isinstance(busy_timeout, bool) or not isinstance(busy_timeout, (int, float))
                or not math.isfinite(busy_timeout) or not 0 < busy_timeout <= 30):
            raise ValueError('server ledger wait must be a finite positive number up to 30 seconds')
        self.busy_timeout = busy_timeout
        if ledger_id is not None and not _uuid(ledger_id):
            raise ValueError('invalid server ledger identity')
        self.ledger_id = ledger_id

    def _connect(self):
        if not _uuid(self.ledger_id):
            raise JobConflict('persistent server ledger identity required')
        try:
            db = sqlite3.connect(self.database.as_uri() + '?mode=rw', timeout=self.busy_timeout, uri=True)
        except sqlite3.OperationalError as exc:
            raise JobConflict('persistent server ledger is unavailable') from exc
        db.row_factory = sqlite3.Row
        try:
            values = dict(db.execute('SELECT name, value FROM executor_settings'))
            if values.get('capacity') != str(self.capacity) or values.get('ledger_id') != self.ledger_id:
                raise JobConflict('persistent server ledger identity changed')
            if db.execute("SELECT 1 FROM executor_jobs WHERE state NOT IN ('reserved','running','unknown','finished') LIMIT 1").fetchone():
                raise JobConflict('server ledger has an unknown state')
        except Exception:
            db.close()
            raise
        return db

    def initialize(self):
        # Explicit installation only. A missing or existing ledger is never
        # silently replaced by a fresh empty one during service startup.
        self.ledger_id = self.ledger_id or str(uuid4())
        try:
            with self.database.open('xb'):
                pass
        except FileExistsError as exc:
            raise JobConflict('server ledger already exists') from exc
        with closing(sqlite3.connect(self.database, timeout=30)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('CREATE TABLE executor_settings (name TEXT PRIMARY KEY, value TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS executor_jobs ('
                       'job_id TEXT PRIMARY KEY, claim_id TEXT NOT NULL UNIQUE, task_id TEXT NOT NULL, '
                       'group_id TEXT NOT NULL, task_run_id TEXT NOT NULL, frozen_json TEXT NOT NULL, '
                       "state TEXT NOT NULL CHECK(state IN ('reserved','running','unknown','finished')), "
                       'receipt_sha256 TEXT, created_at_ns INTEGER NOT NULL, updated_at_ns INTEGER NOT NULL)')
            db.execute("INSERT INTO executor_settings VALUES('capacity',?)", (str(self.capacity),))
            db.execute("INSERT INTO executor_settings VALUES('ledger_id',?)", (self.ledger_id,))
        return self.ledger_id

    def allow_multiple_attempts(self):
        """Keep existing jobs while allowing another job for the same folder."""
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            schema = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='executor_jobs'").fetchone()[0]
            if 'task_run_id TEXT NOT NULL UNIQUE' not in schema:
                return
            db.execute('ALTER TABLE executor_jobs RENAME TO executor_jobs_previous')
            db.execute('CREATE TABLE executor_jobs ('
                       'job_id TEXT PRIMARY KEY, claim_id TEXT NOT NULL UNIQUE, task_id TEXT NOT NULL, '
                       'group_id TEXT NOT NULL, task_run_id TEXT NOT NULL, frozen_json TEXT NOT NULL, '
                       "state TEXT NOT NULL CHECK(state IN ('reserved','running','unknown','finished')), "
                       'receipt_sha256 TEXT, created_at_ns INTEGER NOT NULL, updated_at_ns INTEGER NOT NULL)')
            db.execute('INSERT INTO executor_jobs SELECT * FROM executor_jobs_previous')
            db.execute('DROP TABLE executor_jobs_previous')

    def reserve(self, item):
        frozen = _validated(item)
        identity = item['identity']
        with closing(self._connect()) as db, db:
            try:
                db.execute('BEGIN IMMEDIATE')
            except sqlite3.OperationalError as exc:
                code = getattr(exc, 'sqlite_errorcode', None)
                if (type(code) is int and (code & 0xff) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
                        and not db.in_transaction):
                    raise AdmissionBusy('server admission transaction is busy before reservation') from exc
                raise
            settings = db.execute("SELECT value FROM executor_settings WHERE name='capacity'").fetchone()
            if settings is None or settings['value'] != str(self.capacity):
                raise JobConflict('server ledger not initialized for capacity')
            existing = db.execute('SELECT * FROM executor_jobs WHERE job_id=?', (item['job_id'],)).fetchone()
            if existing:
                if existing['frozen_json'] != frozen:
                    raise JobConflict('existing execution identity changed')
                return {'may_start': False, 'state': existing['state']}
            occupied = self._occupied_db(db)
            if occupied >= self.capacity:
                raise CapacityFull('server postprocessing capacity is occupied')
            now = time.time_ns()
            try:
                db.execute('INSERT INTO executor_jobs VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (item['job_id'], item['claim_id'], identity['task_id'], identity['group_id'],
                     identity['task_run_id'], frozen, 'reserved', None, now, now))
            except sqlite3.IntegrityError as exc:
                raise JobConflict('folder or claim already has an execution') from exc
            return {'may_start': True, 'state': 'reserved'}

    def _transition(self, job_id, claim_id, expected, target, receipt=None):
        if not _uuid(job_id) or not _uuid(claim_id):
            raise JobConflict('invalid execution owner')
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM executor_jobs WHERE job_id=?', (job_id,)).fetchone()
            if not row or row['claim_id'] != claim_id or row['state'] not in expected:
                raise JobConflict('execution owner or state changed')
            db.execute('UPDATE executor_jobs SET state=?, receipt_sha256=?, updated_at_ns=? WHERE job_id=?',
                       (target, receipt, time.time_ns(), job_id))
        return target

    def start(self, job_id, claim_id):
        return self._transition(job_id, claim_id, {'reserved'}, 'running')

    def mark_unknown(self, job_id, claim_id):
        return self._transition(job_id, claim_id, {'reserved', 'running'}, 'unknown')

    def finish(self, job_id, claim_id, receipt_sha256, *, confirm=None):
        if not isinstance(receipt_sha256, str) or not SHA.fullmatch(receipt_sha256):
            raise JobConflict('confirmed receipt digest is required')
        row = self.status(job_id)
        if (row['claim_id'] != claim_id or row['state'] != 'running' or not callable(confirm)
                or confirm(json.loads(row['frozen_json']), receipt_sha256) is not True):
            raise JobConflict('receipt and terminated execution were not independently confirmed')
        return self._transition(job_id, claim_id, {'running'}, 'finished', receipt_sha256)

    def status(self, job_id):
        with closing(self._connect()) as db:
            row = db.execute('SELECT * FROM executor_jobs WHERE job_id=?', (job_id,)).fetchone()
            if not row:
                raise JobConflict('execution was not reserved')
            return dict(row)

    def occupied(self):
        with closing(self._connect()) as db:
            return self._occupied_db(db)

    @staticmethod
    def _capacity_reconciliation_present(db):
        present = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='terminated_capacity_releases'").fetchone()
        if not present:
            return False
        fields = [row['name'] for row in db.execute('PRAGMA table_info(terminated_capacity_releases)')]
        if fields != ['job_id', 'claim_id', 'frozen_json', 'config_json', 'evidence_sha256', 'created_at_ns']:
            raise JobConflict('termination capacity evidence table is invalid')
        return True

    @classmethod
    def _occupied_db(cls, db):
        if not cls._capacity_reconciliation_present(db):
            return db.execute("SELECT COUNT(*) FROM executor_jobs WHERE state IN ('reserved','running','unknown')").fetchone()[0]
        occupied = db.execute("SELECT COUNT(*) FROM executor_jobs WHERE state IN ('reserved','running')").fetchone()[0]
        # A release is valid only while its original proof remains intact.
        # Damaged or missing proof counts as occupied; it can never admit work.
        from server_termination_proof import prove_unknown_termination
        for run in db.execute("SELECT * FROM executor_jobs WHERE state='unknown'").fetchall():
            try:
                row = db.execute('SELECT * FROM terminated_capacity_releases WHERE job_id=?', (run['job_id'],)).fetchone()
                worker = db.execute('SELECT * FROM supervisor_jobs WHERE job_id=?', (run['job_id'],)).fetchone()
                entry = db.execute('SELECT * FROM service_jobs WHERE job_id=?', (run['job_id'],)).fetchone()
                if (row is None or worker is None or entry is None
                        or row['claim_id'] != run['claim_id'] or row['frozen_json'] != run['frozen_json']):
                    raise ValueError('original termination evidence is incomplete')
                proof = prove_unknown_termination(json.loads(run['frozen_json']), dict(run), dict(worker),
                                                  dict(entry), json.loads(row['config_json']))
                if row['evidence_sha256'] != proof['evidence_sha256']:
                    raise ValueError('termination evidence changed')
            except (ValueError, TypeError, KeyError, sqlite3.DatabaseError):
                occupied += 1
        return occupied

    def initialize_capacity_reconciliation(self):
        """Explicitly create only the additive evidence table; never reinitialize the ledger."""
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('CREATE TABLE IF NOT EXISTS terminated_capacity_releases ('
                'job_id TEXT PRIMARY KEY, claim_id TEXT NOT NULL, frozen_json TEXT NOT NULL, '
                'config_json TEXT NOT NULL, evidence_sha256 TEXT NOT NULL, created_at_ns INTEGER NOT NULL)')
            if not self._capacity_reconciliation_present(db):
                raise JobConflict('termination capacity evidence table is unavailable')

    def release_terminated_capacity(self, job_id, claim_id, original_config):
        """Release only a proven-dead process slot; retain unknown result and identity."""
        if not _uuid(job_id) or not _uuid(claim_id) or not isinstance(original_config, dict):
            raise JobConflict('exact original job and service configuration required')
        from server_termination_proof import prove_unknown_termination
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            if not self._capacity_reconciliation_present(db):
                raise JobConflict('termination capacity reconciliation is not installed')
            run = db.execute('SELECT * FROM executor_jobs WHERE job_id=?', (job_id,)).fetchone()
            worker = db.execute('SELECT * FROM supervisor_jobs WHERE job_id=?', (job_id,)).fetchone()
            entry = db.execute('SELECT * FROM service_jobs WHERE job_id=?', (job_id,)).fetchone()
            if run is None or worker is None or entry is None or run['claim_id'] != claim_id:
                raise JobConflict('original job and worker evidence are unavailable')
            job = json.loads(run['frozen_json'])
            proof = prove_unknown_termination(job, dict(run), dict(worker), dict(entry), original_config)
            config_json = json.dumps(original_config, ensure_ascii=False, sort_keys=True,
                                     separators=(',', ':'), allow_nan=False)
            existing = db.execute('SELECT * FROM terminated_capacity_releases WHERE job_id=?', (job_id,)).fetchone()
            if existing is not None:
                if (existing['claim_id'] != claim_id or existing['frozen_json'] != run['frozen_json']
                        or existing['config_json'] != config_json or existing['evidence_sha256'] != proof['evidence_sha256']):
                    raise JobConflict('existing termination capacity evidence differs')
            else:
                db.execute('INSERT INTO terminated_capacity_releases VALUES(?,?,?,?,?,?)',
                    (job_id, claim_id, run['frozen_json'], config_json, proof['evidence_sha256'], time.time_ns()))
            return proof
