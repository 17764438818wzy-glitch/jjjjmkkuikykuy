"""Durable, default-closed admission barriers for the new processing lanes.

The RAG flag blocks a consumer that explicitly calls its admission method; it
does not control unrelated legacy uploaders. This module dispatches no work and
has no network clients. ``initialize`` operates only on an existing database.
"""
from contextlib import closing
from pathlib import Path
import sqlite3
import time


class QueueBarriers:
    def __init__(self, database):
        self.database = Path(database).resolve(strict=True)
        if not self.database.is_file():
            raise ValueError('queue control database is not a file')

    def _connect(self, readonly=False):
        suffix = '?mode=ro' if readonly else '?mode=rw'
        db = sqlite3.connect(self.database.as_uri() + suffix, uri=True, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    @staticmethod
    def _states(db):
        rows = db.execute('SELECT name,blocked FROM paperflow_queue_barriers').fetchall()
        states = {row['name']: row['blocked'] for row in rows}
        if (len(rows) != 2 or set(states) != {'rag_upload', 'postprocessing'}
                or any(type(value) is not int or value not in (0, 1) for value in states.values())):
            raise ValueError('queue barriers are missing or invalid')
        return states

    def initialize(self):
        """Create both closed barriers exactly once; never unlock on startup."""
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            present = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='paperflow_queue_barriers'").fetchone()
            if not present:
                db.execute('''CREATE TABLE paperflow_queue_barriers (
                    name TEXT PRIMARY KEY, blocked INTEGER NOT NULL CHECK(blocked IN (0,1)),
                    updated_at_ns INTEGER NOT NULL)''')
                now = time.time_ns()
                db.executemany('INSERT INTO paperflow_queue_barriers VALUES(?,?,?)',
                               [('rag_upload', 1, now), ('postprocessing', 1, now)])
            self._states(db)

    def _read(self):
        try:
            with closing(self._connect(readonly=True)) as db:
                return self._states(db)
        except (OSError, sqlite3.Error, ValueError):
            return None

    def rag_is_held(self):
        states = self._read()
        return states is not None and states['rag_upload'] == 1

    def postprocess_is_open(self):
        states = self._read()
        return states is not None and states['rag_upload'] == 1 and states['postprocessing'] == 0

    def open_postprocessing(self):
        """Atomically admit postprocessing only with the RAG barrier already held."""
        try:
            with closing(self._connect()) as db, db:
                db.execute('BEGIN IMMEDIATE')
                states = self._states(db)
                if states['rag_upload'] != 1:
                    raise ValueError('RAG upload is not held')
                db.execute("UPDATE paperflow_queue_barriers SET blocked=0,updated_at_ns=? WHERE name='postprocessing'",
                           (time.time_ns(),))
        except sqlite3.Error as exc:
            raise ValueError('queue barriers are unavailable') from exc

    def hold_postprocessing(self):
        try:
            with closing(self._connect()) as db, db:
                db.execute('BEGIN IMMEDIATE')
                self._states(db)
                db.execute("UPDATE paperflow_queue_barriers SET blocked=1,updated_at_ns=? WHERE name='postprocessing'",
                           (time.time_ns(),))
        except sqlite3.Error as exc:
            raise ValueError('queue barriers are unavailable') from exc

    def require_rag_upload_allowed(self):
        states = self._read()
        if states is None or states['rag_upload'] != 0:
            raise PermissionError('RAG upload queue is held or unavailable')
