"""Experimental durable admission attempts in the existing local journal.

This module does not schedule, resume or send a job. The caller must hold the
existing task gate, freshly verify the original Prefect owner and task state,
and authenticate the server reply before recording it. A missing reply remains
pending/unknown and never becomes permission to submit again. No file lock or
cross-ticket snapshot/transfer lock is added. The reply contract requires the
server to echo this attempt's number and unique token under authenticated SSH;
the current deployed server RPC does not implement that contract.
"""
from contextlib import closing
import copy
import hashlib
import json
import re
import sqlite3
import time
from uuid import uuid4

from remote_coordinator import (RemoteSubmissionJournal, _encode, _inspect_intent,
                                _inspect_transfer_intent, _uuid)


class AttemptConflict(ValueError):
    pass


TABLE_SQL = """CREATE TABLE remote_admission_attempts(
    job_id TEXT NOT NULL, attempt_number INTEGER NOT NULL, token TEXT NOT NULL UNIQUE,
    intent_sha256 TEXT NOT NULL, preflight_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','no_start','accepted')),
    response_json TEXT, response_sha256 TEXT, created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL, PRIMARY KEY(job_id,attempt_number)
)"""


def _require(condition, message):
    if not condition:
        raise AttemptConflict(message)


def _sha(raw):
    return hashlib.sha256(raw.encode('utf8')).hexdigest()


def _preflight(value):
    _require(isinstance(value, dict) and set(value) == {'preflight_id', 'preflight_sha256'}
        and _uuid(value['preflight_id']) and isinstance(value['preflight_sha256'], str)
        and re.fullmatch('[a-f0-9]{64}', value['preflight_sha256']) is not None,
        'the exact original server preflight is required')
    return _encode(value)


def _verify_table(db):
    row = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='remote_admission_attempts'").fetchone()
    normalize = lambda sql: ' '.join(sql.strip().rstrip(';').split())
    _require(row is not None and normalize(row['sql']) == normalize(TABLE_SQL),
             'durable attempt table is missing or changed')


def _intent_in_transaction(db, job_id, journal):
    row = db.execute('SELECT * FROM remote_start_intents WHERE job_id=?', (job_id,)).fetchone()
    _require(row is not None and _sha(row['intent_json']) == row['intent_sha256'],
             'the original start intent is missing or changed')
    intent = json.loads(row['intent_json'])
    _inspect_intent(intent)
    job, identity = intent['job'], intent['job']['identity']
    _require(_encode(intent) == row['intent_json'] and intent['journal_id'] == journal.journal_id
             and job['job_id'] == job_id and job['claim_id'] == row['claim_id']
             and identity['task_run_id'] == row['task_run_id'] and identity['task_id'] == row['task_id']
             and identity['group_id'] == row['group_id'], 'the original start identity changed')
    transfer = db.execute('SELECT * FROM remote_transfer_intents WHERE job_id=?', (job_id,)).fetchone()
    _require(transfer is not None and _sha(transfer['intent_json']) == transfer['intent_sha256']
             and transfer['intent_sha256'] == intent['transfer_intent_sha256']
             and transfer['claim_id'] == job['claim_id'] and transfer['task_run_id'] == identity['task_run_id']
             and transfer['task_id'] == identity['task_id'] and transfer['group_id'] == identity['group_id'],
             'the original transfer intent is missing or changed')
    previous = json.loads(transfer['intent_json'])
    _inspect_transfer_intent(previous)
    _require(_encode(previous) == transfer['intent_json']
             and all(_encode(previous[key]) == _encode(intent[key])
                     for key in ('job', 'snapshot', 'server_identity', 'journal_id')),
             'the original transfer binding changed')
    return intent, row['intent_sha256']


def _response_state(response, intent, number, token):
    job, server = intent['job'], intent['server_identity']
    _require(isinstance(response, dict), 'the server reply must be an object')
    try:
        raw = _encode(response)
    except (TypeError, ValueError) as exc:
        raise AttemptConflict('the server reply is not canonical JSON') from exc
    if raw == _encode(dict(job=job, server_identity=server, accepted=True,
                           attempt_number=number, attempt_token=token)) and type(response.get('accepted')) is bool:
        return 'accepted'
    reason = response.get('reason')
    expected = dict(job=job, server_identity=server, accepted=False, reason=reason, this_attempt_started=False,
                    attempt_number=number, attempt_token=token)
    if (reason in {'capacity_full', 'admission_busy'} and raw == _encode(expected)
            and type(response.get('accepted')) is bool and type(response.get('this_attempt_started')) is bool):
        return 'no_start'
    raise AttemptConflict('the exact authenticated no-start or accepted reply is required')


def _history(rows, intent, digest):
    _require(rows, 'no durable attempt evidence exists; never infer safe retry')
    preflight = None
    for number, row in enumerate(rows, 1):
        _require(row['attempt_number'] == number and _uuid(row['token'])
                 and row['intent_sha256'] == digest, 'attempt history is incomplete or changed')
        value = json.loads(row['preflight_json'])
        _require(_preflight(value) == row['preflight_json'], 'server preflight evidence changed')
        if preflight is None: preflight = row['preflight_json']
        _require(row['preflight_json'] == preflight, 'server preflight changed between attempts')
        if row['state'] == 'pending':
            _require(row['response_json'] is None and row['response_sha256'] is None,
                     'pending attempt contains an unverified reply')
        else:
            _require(isinstance(row['response_json'], str) and _sha(row['response_json']) == row['response_sha256'],
                     'server reply evidence changed')
            response = json.loads(row['response_json'])
            _require(_encode(response) == row['response_json']
                     and _response_state(response, intent, number, row['token']) == row['state'],
                     'server reply identity changed')
        if number < len(rows):
            _require(row['state'] == 'no_start', 'later attempts follow an unconfirmed or accepted send')
    return rows, json.loads(preflight)


def _snapshot(db, job_id, journal):
    """Copy a consistent evidence snapshot, then audit it without DB locks.

    data_version is meaningful only on the *same* SQLite connection. Any
    other commit between this snapshot and the later write invalidates it,
    even an unrelated ticket's commit; callers must re-audit, never proceed
    from stale evidence. This is deliberately conservative and not yet a
    production wait/recovery implementation.
    """
    version = db.execute('PRAGMA data_version').fetchone()[0]
    db.execute('BEGIN')
    _verify_table(db)
    intent, digest = _intent_in_transaction(db, job_id, journal)
    rows = db.execute('SELECT * FROM remote_admission_attempts WHERE job_id=? ORDER BY attempt_number',
                      (job_id,)).fetchall()
    db.rollback()
    rows, preflight = _history(rows, intent, digest)
    return version, intent, digest, rows, preflight


def _check_snapshot_version(db, version):
    _require(db.execute('PRAGMA data_version').fetchone()[0] == version,
             'the journal changed during admission audit; re-audit before proceeding')


class AdmissionAttempts:
    def __init__(self, journal):
        if not isinstance(journal, RemoteSubmissionJournal):
            raise AttemptConflict('the original pinned submission journal is required')
        self.journal = journal

    def install(self):
        with closing(self.journal._connect(readonly=False)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            exists = db.execute("SELECT 1 FROM sqlite_master WHERE name='remote_admission_attempts'").fetchone()
            if not exists: db.execute(TABLE_SQL)
            _verify_table(db)

    def record_initial(self, intent, raw, digest, preflight):
        """Atomically save the original start intent and first pending attempt."""
        _inspect_intent(intent)
        _require(isinstance(raw, str) and _encode(intent) == raw and _sha(raw) == digest
                 and intent['journal_id'] == self.journal.journal_id, 'original start intent changed')
        preflight_raw = _preflight(preflight)
        job, identity = intent['job'], intent['job']['identity']
        token, now = str(uuid4()), time.time_ns()
        try:
            with closing(self.journal._connect(readonly=False)) as db, db:
                db.execute('BEGIN IMMEDIATE')
                _verify_table(db)
                transfer = db.execute('SELECT * FROM remote_transfer_intents WHERE job_id=?',
                                      (job['job_id'],)).fetchone()
                _require(transfer is not None and _sha(transfer['intent_json']) == transfer['intent_sha256']
                         and transfer['intent_sha256'] == intent['transfer_intent_sha256']
                         and transfer['claim_id'] == job['claim_id']
                         and transfer['task_run_id'] == identity['task_run_id']
                         and transfer['task_id'] == identity['task_id'] and transfer['group_id'] == identity['group_id'],
                         'the original transfer evidence is unavailable')
                previous = json.loads(transfer['intent_json'])
                _inspect_transfer_intent(previous)
                _require(_encode(previous) == transfer['intent_json'] and all(_encode(previous[key]) == _encode(intent[key])
                         for key in ('job', 'snapshot', 'server_identity', 'journal_id')),
                         'the original transfer identity changed')
                _require(db.execute('SELECT 1 FROM remote_admission_attempts WHERE job_id=?',
                                    (job['job_id'],)).fetchone() is None,
                         'an admission attempt already exists')
                db.execute('INSERT INTO remote_start_intents VALUES(?,?,?,?,?,?,?,?)',
                    (job['job_id'], job['claim_id'], identity['task_run_id'], identity['task_id'],
                     identity['group_id'], raw, digest, now))
                db.execute('INSERT INTO remote_admission_attempts VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (job['job_id'], 1, token, digest, preflight_raw, 'pending', None, None, now, now))
        except sqlite3.IntegrityError as exc:
            raise AttemptConflict('existing original intent forbids another initial admission') from exc
        return dict(number=1, token=token, job=copy.deepcopy(job), server_identity=copy.deepcopy(intent['server_identity']),
                    preflight=copy.deepcopy(preflight))

    def status(self, job_id):
        _require(_uuid(job_id), 'original job identity is required')
        with closing(self.journal._connect(readonly=True)) as db:
            version, intent, _, rows, preflight = _snapshot(db, job_id, self.journal)
            _check_snapshot_version(db, version)
            return dict(state=rows[-1]['state'], number=rows[-1]['attempt_number'],
                        job=copy.deepcopy(intent['job']), server_identity=copy.deepcopy(intent['server_identity']),
                        preflight=preflight)

    def record_response(self, job_id, number, token, response):
        _require(_uuid(job_id) and type(number) is int and number >= 1 and _uuid(token),
                 'original attempt identity is required')
        with closing(self.journal._connect(readonly=False)) as db, db:
            version, intent, _, rows, _ = _snapshot(db, job_id, self.journal)
            last = rows[-1]
            _require(last['attempt_number'] == number and last['token'] == token and last['state'] == 'pending',
                     'the pending admission attempt changed')
            state = _response_state(response, intent, number, token)
            raw = _encode(response)
            _require(len(raw.encode('utf8')) <= 65536, 'server admission reply exceeds the metadata bound')
            db.execute('BEGIN IMMEDIATE')
            _check_snapshot_version(db, version)
            changed = db.execute('UPDATE remote_admission_attempts SET state=?,response_json=?,response_sha256=?,'
                'updated_at_ns=? WHERE job_id=? AND attempt_number=? AND token=? AND state=?',
                (state, raw, _sha(raw), time.time_ns(), job_id, number, token, 'pending')).rowcount
            _require(changed == 1, 'the pending admission attempt changed')
        return state

    def begin_next(self, job_id):
        _require(_uuid(job_id), 'original job identity is required')
        with closing(self.journal._connect(readonly=False)) as db, db:
            version, intent, digest, rows, preflight = _snapshot(db, job_id, self.journal)
            _require(rows[-1]['state'] == 'no_start', 'last admission was not definitely rejected before start')
            number, token, now = rows[-1]['attempt_number'] + 1, str(uuid4()), time.time_ns()
            db.execute('BEGIN IMMEDIATE')
            _check_snapshot_version(db, version)
            db.execute('INSERT INTO remote_admission_attempts VALUES(?,?,?,?,?,?,?,?,?,?)',
                (job_id, number, token, digest, _preflight(preflight), 'pending', None, None, now, now))
            return dict(number=number, token=token, job=copy.deepcopy(intent['job']),
                        server_identity=copy.deepcopy(intent['server_identity']), preflight=preflight)
