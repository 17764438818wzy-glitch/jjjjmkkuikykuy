"""Read the original server capacity ledger without reserving a slot.

This is an observation candidate, not an admission lease or permission to
start work. Unknown results count as occupied unless their original terminal
process proof is still intact. No table, file or server job is modified.
"""
from contextlib import closing
from pathlib import Path
import sqlite3

from remote_jobs import RemoteJobs, _uuid


def query_capacity(database, *, capacity, server_identity, nonce):
    if (type(capacity) is not int or not 1 <= capacity <= 8 or not _uuid(nonce)
            or not isinstance(server_identity, dict) or set(server_identity) != {'server_id', 'ledger_id'}
            or not isinstance(server_identity['server_id'], str) or not server_identity['server_id']
            or not _uuid(server_identity['ledger_id'])):
        raise ValueError('the exact server, ledger and fresh nonce are required')
    path = Path(database)
    if not path.is_absolute() or '..' in path.parts or path.resolve(strict=True) != path or not path.is_file():
        raise ValueError('the existing canonical server ledger is required')
    try:
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=3)) as db:
            db.row_factory = sqlite3.Row
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            settings = dict(db.execute('SELECT name,value FROM executor_settings'))
            if settings != {'capacity': str(capacity), 'ledger_id': server_identity['ledger_id']}:
                raise ValueError('the original server ledger identity changed')
            if db.execute("SELECT 1 FROM executor_jobs WHERE state NOT IN ('reserved','running','unknown','finished') LIMIT 1").fetchone():
                raise ValueError('the original server ledger state is invalid')
            occupied = RemoteJobs._occupied_db(db)
        if type(occupied) is not int or not 0 <= occupied <= capacity:
            raise ValueError('the observed server occupancy is inconsistent')
        return dict(schema='paperflow-server-capacity-observation/1', server_identity=dict(server_identity),
                    nonce=nonce, capacity=capacity, occupied=occupied, available=capacity-occupied,
                    admission_reserved=False)
    except (sqlite3.Error, OSError) as exc:
        raise ValueError('the existing server capacity cannot be observed') from exc
