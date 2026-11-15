"""Persist externally visible effects, without scheduling or sending them.

The caller must supply the new task control's admission context. Its token must
run ``record_intent`` atomically with permission to begin transport and expose
in-flight work to pause/delete control. This module alone is not an admission
implementation and must not be connected to live work before that integration.
"""
from contextlib import closing
import json
import math
from pathlib import Path
import re
import sqlite3
import time
import uuid


class EffectConflict(ValueError):
    """The same effect identity was presented with different request bytes."""


class EffectUnknown(RuntimeError):
    """An effect was admitted, but its remote outcome is not known locally."""


class EffectNeedsReadback(RuntimeError):
    """A previous result exists but cannot prove the current remote state."""


class _AlreadyReturned(Exception):
    def __init__(self, result):
        self.result = result


def _digest(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{64}', value):
        raise ValueError('effect identities must be lowercase SHA-256 values')
    return value


class EffectJournal:
    def __init__(self, path):
        self.path = Path(path).resolve()

    def _connect(self):
        db = sqlite3.connect(str(self.path), timeout=10)
        db.row_factory = sqlite3.Row
        return db

    def initialize(self):
        """Explicitly create only the local durable evidence table."""
        with closing(self._connect()) as db, db:
            db.execute('''CREATE TABLE IF NOT EXISTS journal_identity (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1), id TEXT NOT NULL
            )''')
            db.execute('INSERT OR IGNORE INTO journal_identity VALUES(1,?)', (uuid.uuid4().hex,))
            db.execute('''CREATE TABLE IF NOT EXISTS remote_effects (
                effect_id TEXT PRIMARY KEY,
                request_sha256 TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('intent', 'returned')),
                response_json TEXT,
                created_at_ns INTEGER NOT NULL,
                updated_at_ns INTEGER NOT NULL
            )''')
            db.execute('''CREATE TABLE IF NOT EXISTS remote_document_reservations (
                document_key TEXT PRIMARY KEY,
                effect_id TEXT NOT NULL,
                request_sha256 TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('active', 'released')),
                updated_at_ns INTEGER NOT NULL,
                kind TEXT NOT NULL CHECK(kind IN ('native', 'sync'))
            )''')
            if 'kind' not in {row['name'] for row in db.execute('PRAGMA table_info(remote_document_reservations)')}:
                raise ValueError('old document reservation schema requires explicit offline migration')

    def identity(self):
        with closing(self._connect()) as db:
            row = db.execute('SELECT id FROM journal_identity WHERE singleton=1').fetchone()
        if not row:
            raise ValueError('effect journal identity is unavailable')
        return row['id']

    @staticmethod
    def _interpret(row, request_sha256, *, reuse_returned=True):
        if row['request_sha256'] != request_sha256:
            raise EffectConflict('recorded effect has different request content')
        if row['state'] == 'intent':
            raise EffectUnknown('prior remote effect has an unresolved outcome')
        if row['state'] != 'returned' or row['response_json'] is None:
            raise EffectUnknown('recorded remote effect is not verifiable')
        if not reuse_returned:
            raise EffectNeedsReadback('prior write requires a current remote readback')
        try:
            return json.loads(row['response_json'])
        except (TypeError, ValueError) as exc:
            raise EffectUnknown('recorded remote response is unreadable') from exc

    def _read(self, effect_id):
        with closing(self._connect()) as db:
            return db.execute('SELECT * FROM remote_effects WHERE effect_id=?', (effect_id,)).fetchone()

    def state(self, effect_id, request_sha256):
        effect_id, request_sha256 = _digest(effect_id), _digest(request_sha256)
        row = self._read(effect_id)
        if row is None:
            return None
        if row['request_sha256'] != request_sha256:
            raise EffectConflict('recorded effect has different request content')
        return row['state']

    def assert_document_available(self, document_key):
        document_key = _digest(document_key)
        with closing(self._connect()) as db:
            row = db.execute('SELECT state FROM remote_document_reservations WHERE document_key=?',
                             (document_key,)).fetchone()
        if row is not None and row['state'] == 'active':
            raise EffectUnknown('document has an active or unresolved native submission')

    def release_synchronous_document_if_owned(self, document_key, effect_id, request_sha256):
        document_key, effect_id, request_sha256 = map(_digest, (document_key, effect_id, request_sha256))
        with closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            reservation = db.execute('SELECT * FROM remote_document_reservations WHERE document_key=?',
                                     (document_key,)).fetchone()
            if reservation is None or reservation['state'] == 'released':
                db.commit()
                return
            effect = db.execute('SELECT state,request_sha256 FROM remote_effects WHERE effect_id=?',
                                (effect_id,)).fetchone()
            if (effect is None or effect['state'] != 'returned' or effect['request_sha256'] != request_sha256
                    or reservation['kind'] != 'sync' or reservation['effect_id'] != effect_id
                    or reservation['request_sha256'] != request_sha256
                    or reservation['state'] != 'active'):
                raise EffectUnknown('synchronous reservation and returned submission do not match')
            db.execute("UPDATE remote_document_reservations SET state='released',updated_at_ns=? WHERE document_key=?",
                       (time.time_ns(), document_key))
            db.commit()

    def perform(self, effect_id, request_sha256, send, admission, *, reuse_returned=True,
                before_intent=None, reserve_document=None, reservation_kind=None, not_after=None):
        """Send once after task admission; retain returned data or unknown intent."""
        effect_id, request_sha256 = _digest(effect_id), _digest(request_sha256)
        if not callable(send):
            raise TypeError('send must be callable')
        if before_intent is not None and not callable(before_intent):
            raise TypeError('preflight must be callable')
        if not_after is not None and not callable(not_after) and (
                type(not_after) not in (int, float) or not math.isfinite(not_after)):
            raise ValueError('transport deadline must be a finite monotonic time')
        if reserve_document is not None:
            reserve_document = _digest(reserve_document)
            if reservation_kind not in {'native', 'sync'}:
                raise ValueError('document reservation kind is required')
        elif reservation_kind is not None:
            raise ValueError('reservation kind has no document')
        intent_committed = False

        def check_deadline():
            # A provider must be local-only and nonblocking: it can run while
            # the SQLite transaction is open, after the final network proof.
            deadline = not_after() if callable(not_after) else not_after
            if deadline is not None and (type(deadline) not in (int, float) or not math.isfinite(deadline)):
                raise ValueError('transport deadline must be a finite monotonic time')
            if callable(not_after) and deadline is None:
                raise ValueError('fresh transport deadline is unavailable')
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError('transport deadline expired before the remote send')

        def record_intent():
            nonlocal intent_committed
            if intent_committed:
                raise RuntimeError('intent callback may only run once')
            check_deadline()
            with closing(self._connect()) as db:
                db.execute('BEGIN IMMEDIATE')
                check_deadline()
                existing = db.execute('SELECT * FROM remote_effects WHERE effect_id=?', (effect_id,)).fetchone()
                if existing is not None:
                    result = self._interpret(existing, request_sha256, reuse_returned=reuse_returned)
                    raise _AlreadyReturned(result)
                now = time.time_ns()
                if reserve_document is not None:
                    reservation = db.execute('SELECT state FROM remote_document_reservations WHERE document_key=?',
                                             (reserve_document,)).fetchone()
                    if reservation is not None and reservation['state'] == 'active':
                        raise EffectUnknown('document has an active or unresolved native submission')
                    db.execute('INSERT OR REPLACE INTO remote_document_reservations VALUES(?,?,?,?,?,?)',
                               (reserve_document, effect_id, request_sha256, 'active', now, reservation_kind))
                db.execute('INSERT INTO remote_effects VALUES(?,?,?,?,?,?)',
                           (effect_id, request_sha256, 'intent', None, now, now))
                db.commit()
                intent_committed = True

        try:
            with admission as token:
                token.check_current()
                row = self._read(effect_id)
                if row is not None:
                    return self._interpret(row, request_sha256, reuse_returned=reuse_returned)
                # Network checks must not hold SQLite's global writer lock.
                # The caller must hold the relevant shared document fence.
                if before_intent is not None and before_intent() is not True:
                    raise ValueError('submission history was not proven fresh at transport admission')
                check_deadline()
                token.start_transport(record_intent)
                if not intent_committed:
                    raise RuntimeError('transport admission did not commit intent synchronously')
                # Preserve a committed intent as unresolved if the deadline
                # expires before the call; never send it late or replay it.
                check_deadline()
                result = send()
                # Keep the effect visible to pause/delete until the returned
                # response is durable. A crash here still leaves an intent.
                response = json.dumps(result, ensure_ascii=False, allow_nan=False, sort_keys=True,
                                      separators=(',', ':'))
                with closing(self._connect()) as db, db:
                    db.execute('BEGIN IMMEDIATE')
                    updated = db.execute("UPDATE remote_effects SET state='returned',response_json=?,updated_at_ns=? "
                                         "WHERE effect_id=? AND request_sha256=? AND state='intent'",
                                         (response, time.time_ns(), effect_id, request_sha256))
                    if updated.rowcount != 1:
                        raise EffectUnknown('remote response could not be durably bound to its intent')
        except _AlreadyReturned as exc:
            return exc.result
        return result
