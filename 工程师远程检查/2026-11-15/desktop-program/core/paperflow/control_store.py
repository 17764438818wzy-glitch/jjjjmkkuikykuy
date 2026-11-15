"""Control facts and intent storage; no process/IPC work runs inside transactions."""
from __future__ import annotations

import json
import time

from .handoff_store import HandoffLedger, canonical


SCHEMA = (
    '''CREATE TABLE IF NOT EXISTS control_execution_results (
        execution_id TEXT PRIMARY KEY REFERENCES control_executions(execution_id),
        epoch TEXT NOT NULL,outcome TEXT NOT NULL,data TEXT NOT NULL,received REAL NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS controller_state (
        id INTEGER PRIMARY KEY CHECK(id=1),epoch TEXT NOT NULL,generation INTEGER NOT NULL,
        paused INTEGER NOT NULL,reason TEXT NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS admission_scopes (
        scope TEXT PRIMARY KEY,generation INTEGER NOT NULL,change_id TEXT NOT NULL,state TEXT NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS control_executions (
        execution_id TEXT PRIMARY KEY,epoch TEXT NOT NULL,unit_kind TEXT NOT NULL,unit_key TEXT NOT NULL,
        job_name TEXT UNIQUE NOT NULL,root_identity TEXT NOT NULL DEFAULT '{}',worker_identity TEXT,
        members TEXT NOT NULL DEFAULT '[]',state TEXT NOT NULL,lease_until REAL NOT NULL,
        last_heartbeat REAL NOT NULL,deadline REAL NOT NULL,exit_evidence TEXT)''',
    '''CREATE UNIQUE INDEX IF NOT EXISTS single_unit_owner ON control_executions(unit_kind,unit_key)
        WHERE state != 'exited' ''',
    '''CREATE UNIQUE INDEX IF NOT EXISTS single_target_owner ON control_executions(
        CASE WHEN unit_kind IN ('delivery','cleanup') THEN 'target' ELSE unit_kind END,unit_key)
        WHERE state != 'exited' ''',
    '''CREATE INDEX IF NOT EXISTS control_execution_history ON control_executions(unit_kind,unit_key)''',
    '''CREATE TABLE IF NOT EXISTS control_actions (
        action_id TEXT PRIMARY KEY,epoch TEXT NOT NULL,request_id TEXT NOT NULL,
        execution_id TEXT NOT NULL REFERENCES control_executions(execution_id),kind TEXT NOT NULL,
        request TEXT NOT NULL,snapshot TEXT NOT NULL,state TEXT NOT NULL,deadline REAL NOT NULL,
        result TEXT,UNIQUE(epoch,request_id))''',
    '''CREATE INDEX IF NOT EXISTS control_actions_execution_state ON control_actions(execution_id,state)''',
    '''CREATE INDEX IF NOT EXISTS control_unresolved_writes ON control_actions(kind)
        WHERE state='unresolved' AND kind IN ('rag_write','cleanup_write')''',
    '''CREATE INDEX IF NOT EXISTS control_native_ingest_history ON control_actions(
        json_extract(request,'$.resource.dataset_id'),json_extract(request,'$.resource.document_id'))
        WHERE kind='rag_write' AND state IN ('possible','complete','unresolved','reconciled','operator_abandoned')
        AND json_extract(request,'$.resource.write_operation')='native_ingest' ''',
    '''CREATE TABLE IF NOT EXISTS control_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,action_id TEXT NOT NULL REFERENCES control_actions(action_id),
        received REAL NOT NULL,report_epoch TEXT NOT NULL,current_qualification INTEGER NOT NULL,result TEXT NOT NULL)''',
)


class ControlLedger(HandoffLedger):
    def __init__(self, store):
        super().__init__(store)
        self._write(lambda c: [c.execute(sql) for sql in SCHEMA])

    def rows(self, query, args=()):
        with self.store.db(timeout=.1) as c:
            return [dict(row) for row in c.execute(query, args)]

    def boot(self, epoch):
        def write(c):
            current = c.execute('SELECT generation FROM controller_state WHERE id=1').fetchone()
            generation = current['generation'] + 1 if current else 1
            c.execute('INSERT INTO controller_state VALUES(1,?,?,1,?) ON CONFLICT(id) DO UPDATE SET '
                      'epoch=excluded.epoch,generation=excluded.generation,paused=1,reason=excluded.reason',
                      (epoch, generation, 'startup requires explicit resume'))
            for scope in ('service:llm', 'service:rag'):
                c.execute('INSERT OR IGNORE INTO admission_scopes VALUES(?,0,?,?)',
                          (scope, 'initial', canonical(dict(enabled=True, frozen=False, expected_generation=0))))
            return generation
        return self._write(write)

    def persist_control(self, epoch, generation, paused, reason, *, deadline=None):
        def write(c):
            changed = c.execute('UPDATE controller_state SET generation=?,paused=?,reason=? '
                                'WHERE id=1 AND epoch=? AND generation<=?',
                                (generation, int(paused), reason, epoch, generation)).rowcount
            if changed != 1:
                raise ValueError('stale controller persistence')
        self._write(write, deadline=deadline)

    def ensure_scope(self, scope):
        self._write(lambda c: c.execute('INSERT OR IGNORE INTO admission_scopes VALUES(?,0,?,?)',
                    (scope, 'initial', canonical(dict(enabled=True, frozen=False, expected_generation=0)))))

    def check(self, c, snapshot, execution_id, kind, expected_generation, *, allow_frozen=False):
        control = c.execute('SELECT * FROM controller_state WHERE id=1').fetchone()
        if not control or control['paused'] or control['epoch'] != snapshot['epoch'] or control['generation'] != snapshot['control_generation']:
            raise ValueError('persistent control denies admission')
        execution = c.execute('SELECT * FROM control_executions WHERE execution_id=?', (execution_id,)).fetchone()
        if (not execution or execution['epoch'] != snapshot['epoch'] or execution['state'] != 'active'
                or execution['lease_until'] <= time.time() or execution['deadline'] <= time.time()):
            raise ValueError('execution no longer eligible')
        for scope, generation in snapshot['scopes'].items():
            row = c.execute('SELECT * FROM admission_scopes WHERE scope=?', (scope,)).fetchone()
            if not row or row['generation'] != generation:
                raise ValueError('scope generation changed')
            state = json.loads(row['state'])
            if not state.get('enabled', True) or (state.get('frozen', False) and not (allow_frozen and scope.startswith('unit:'))):
                raise ValueError('scope closed or frozen')
            if scope.startswith('unit:'):
                if state.get('owner') != execution_id or state.get('expected_generation', 0) != expected_generation:
                    raise ValueError('owner or expected generation changed')
        if kind in ('rag_write', 'cleanup_write'):
            # No automatic release: uncertain writes survive process/lease recovery.
            for row in c.execute("SELECT snapshot FROM control_actions WHERE state='unresolved' AND kind IN ('rag_write','cleanup_write')"):
                previous = json.loads(row['snapshot'])['scopes']
                if any(scope.startswith('unit:') and scope in previous for scope in snapshot['scopes']):
                    raise ValueError('unresolved remote write barrier')

    def heartbeat_batch(self, epoch, updates, lease_seconds, *, deadline=None):
        if not updates:return
        def write(c):
            for execution_id, heartbeat, members, identity in updates:
                c.execute('UPDATE control_executions SET lease_until=?,last_heartbeat=?,members=?,worker_identity=COALESCE(?,worker_identity) '
                          "WHERE execution_id=? AND epoch=? AND state='active' AND deadline>?",
                          (heartbeat + lease_seconds, heartbeat, canonical(members),canonical(identity) if identity else None,
                           execution_id, epoch, time.time()))
        self._write(write, deadline=deadline)

    def confirm_exit(self, execution_id, evidence, *, deadline=None):
        confirmed=dict(evidence,confirmed_at=time.time(),confirmed_monotonic=time.monotonic())
        def write(c):
            identity=evidence.get('worker_identity')
            c.execute("UPDATE control_executions SET state='exited',lease_until=0,exit_evidence=?,worker_identity=COALESCE(?,worker_identity) WHERE execution_id=?",
                      (canonical(confirmed),canonical(identity) if identity else None, execution_id))
            c.execute("UPDATE control_actions SET state=CASE WHEN kind IN ('rag_write','cleanup_write') "
                      "THEN 'unresolved' ELSE 'diagnostic' END WHERE execution_id=? AND state='possible'", (execution_id,))
            c.execute("UPDATE control_actions SET state='abandoned' WHERE execution_id=? AND state='prepared'", (execution_id,))
        self._write(write, deadline=deadline)
