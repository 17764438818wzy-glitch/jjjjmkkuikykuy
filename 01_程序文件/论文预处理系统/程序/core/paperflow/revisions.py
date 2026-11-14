"""Shared SQLite revision reservation scan; callers hold BEGIN IMMEDIATE."""
import json


def next_revision_sql(c,task_id):
    revisions=[int(r['revision'][1:]) for r in c.execute('SELECT revision FROM delivery_packages WHERE task_id=?',(task_id,))]
    for row in c.execute('SELECT data FROM publication_intents WHERE task_id=?',(task_id,)):
        revisions.append(json.loads(row['data'])['revision'])
    for table,column in (('repair_requests','run_data'),('structure_migrations','data')):
        if c.execute('SELECT 1 FROM sqlite_master WHERE type=\'table\' AND name=?',(table,)).fetchone():
            for row in c.execute(f'SELECT {column} FROM {table} WHERE task_id=? AND {column} IS NOT NULL',(task_id,)):
                revisions.append(json.loads(row[column])['revision'])
    return max(revisions,default=0)+1
