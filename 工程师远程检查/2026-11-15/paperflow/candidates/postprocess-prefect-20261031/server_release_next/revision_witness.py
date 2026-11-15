"""Observe control-DB changes without claiming work or modifying its schema."""
from pathlib import Path
import sqlite3
from threading import Lock


class ReadOnlyRevision:
    def __init__(self, database):
        self.path = Path(database).resolve(strict=True)
        if not self.path.is_file():
            raise ValueError('control database is unavailable')
        stat = self.path.stat()
        self.identity = (stat.st_dev, stat.st_ino)
        self.lock = Lock()
        self.db = sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True,
                                  timeout=.05, check_same_thread=False)
        self.db.execute('PRAGMA query_only=ON')

    def current(self):
        with self.lock:
            if self.db is None:
                raise ValueError('control revision witness is closed')
            stat = self.path.stat()
            if (stat.st_dev, stat.st_ino) != self.identity:
                raise ValueError('control database identity changed')
            row = self.db.execute('PRAGMA data_version').fetchone()
            if not row or type(row[0]) is not int:
                raise ValueError('control database revision is unavailable')
            return (self.identity, row[0])

    def close(self):
        with self.lock:
            if self.db is not None:
                self.db.close()
                self.db = None
