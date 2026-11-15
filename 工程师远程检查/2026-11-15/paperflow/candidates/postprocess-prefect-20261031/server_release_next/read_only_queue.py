"""Reuse existing queue reads without initializing state or probing work locks."""
from contextlib import closing
from pathlib import Path
import sqlite3


def open_queue(path):
    from local_queue import LocalQueue

    class ObservationQueue(LocalQueue):
        def __init__(self, source):
            self.path = Path(source).resolve(strict=True)
            if not self.path.is_file():
                raise ValueError('an existing queue database is required')
            self.require_retention = True
            self.require_live_watcher = True
            self.consumer_id = 'local-intake'
            self.receiver_session_id = None
            self.watch_owner_thread = None
            with closing(self._connect()) as db:
                row = db.execute("SELECT value FROM metadata WHERE key='queue_schema'").fetchone()
                if row is None or row[0] != 'paperflow-local-waiting/1':
                    raise ValueError('queue schema is unavailable')

        def _connect(self):
            db = sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True, timeout=5)
            db.row_factory = sqlite3.Row
            return db

        def snapshot(self, *, max_age_seconds=60, include_in_progress=False):
            # Activity belongs to the existing 14205 API; never inspect its
            # per-folder file locks from this postprocessing observer.
            return super().snapshot(max_age_seconds=max_age_seconds, include_in_progress=False)

    return ObservationQueue(path)
