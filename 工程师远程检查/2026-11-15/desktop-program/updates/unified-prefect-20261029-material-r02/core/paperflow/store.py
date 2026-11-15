"""短事务台账；模型和网络请求不占用数据库事务。"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import time


TERMINAL = ('complete', 'held', 'parked', 'superseded','code_error')


class StaleSourceVersion(ValueError):
    pass


class UpdateConflict(ValueError):
    pass


class Store:
    def __init__(self, path: Path | str, contract_version='paperflow/1.0'):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.contract_version = contract_version
        # Ledgers sharing this Store also share the local SQLite writer lane.
        # SQLite remains the authority for writers in other processes.
        self.write_lock = threading.RLock()
        with self.db() as c:
            c.executescript('''
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY, source_sha256 TEXT NOT NULL,
                    stage TEXT NOT NULL, kind TEXT NOT NULL, due REAL NOT NULL,
                    owner TEXT, lease_until REAL NOT NULL DEFAULT 0,
                    created REAL NOT NULL, updated REAL NOT NULL, data TEXT NOT NULL,version INTEGER NOT NULL DEFAULT 0);
                CREATE INDEX IF NOT EXISTS task_queue ON tasks(stage,due,lease_until);
                CREATE TABLE IF NOT EXISTS sources (
                    path TEXT PRIMARY KEY, source_id TEXT NOT NULL,
                    task_id TEXT NOT NULL, active_task TEXT,active_revision INTEGER);
                CREATE TABLE IF NOT EXISTS source_versions (
                    path TEXT NOT NULL,task_id TEXT NOT NULL,PRIMARY KEY(path,task_id));
                CREATE TABLE IF NOT EXISTS receipts (
                    task_id TEXT NOT NULL, revision INTEGER NOT NULL, target TEXT NOT NULL,
                    data TEXT NOT NULL, PRIMARY KEY(task_id,revision,target));
                CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, at REAL NOT NULL,
                    task_id TEXT, kind TEXT NOT NULL, data TEXT NOT NULL);
            ''')
            if 'version' not in {r[1] for r in c.execute('PRAGMA table_info(tasks)')}:
                c.execute('ALTER TABLE tasks ADD COLUMN version INTEGER NOT NULL DEFAULT 0')
            if 'active_revision' not in {r[1] for r in c.execute('PRAGMA table_info(sources)')}:
                c.execute('ALTER TABLE sources ADD COLUMN active_revision INTEGER')
                for row in c.execute('SELECT s.path,t.data FROM sources s JOIN tasks t ON s.active_task=t.task_id').fetchall():
                    c.execute('UPDATE sources SET active_revision=? WHERE path=?',
                              (json.loads(row['data'])['revision'],row['path']))
            c.execute('INSERT OR IGNORE INTO source_versions(path,task_id) SELECT path,task_id FROM sources')

    @contextmanager
    def db(self, *, timeout=30):
        c = sqlite3.connect(self.path, timeout=timeout)
        c.row_factory = sqlite3.Row
        try:
            c.execute('PRAGMA journal_mode=WAL')
            c.execute('PRAGMA synchronous=FULL')
            yield c
            c.commit()
        except BaseException:
            c.rollback()
            raise
        finally:
            c.close()

    @staticmethod
    def decode(row):
        if row is None:
            return None
        data = json.loads(row['data'])
        for key in ('task_id', 'stage', 'kind', 'due', 'owner', 'lease_until','version'):
            data[key] = row[key]
        return data

    def register_source(self, path, sha256, *, identity_path=None, supersede_identity=False, **fields):
        with self.db() as c:
            return self.register_source_sql(c, path, sha256, identity_path=identity_path,
                                            supersede_identity=supersede_identity, **fields)

    def register_source_sql(self, c, path, sha256, *, identity_path=None, supersede_identity=False, **fields):
        from .original_archive.admission import assert_available
        # The tombstone check and source creation must share the same native
        # writer lane held by the releaser; a pre-transaction check can race it.
        if not c.in_transaction:c.execute('BEGIN IMMEDIATE')
        assert_available(self.path,[path,identity_path])
        path = str(Path(path).resolve())
        identity_path=str(Path(identity_path).resolve()) if identity_path else path
        source_id = 'src_' + hashlib.sha256(identity_path.casefold().encode()).hexdigest()[:24]
        task_id = 'PW_' + hashlib.sha256((sha256 + self.contract_version).encode()).hexdigest()[:24]
        now = time.time()
        data = dict(task_id=task_id, source_id=source_id, source_sha256=sha256,
                    source_path=path, revision=1, repair_count=0, stagnant_count=0,
                    contract_version=self.contract_version, **fields)
        c.execute('INSERT OR IGNORE INTO tasks(task_id,source_sha256,stage,kind,due,owner,lease_until,created,updated,data) VALUES(?,?,?,?,?,?,?,?,?,?)',
                  (task_id, sha256, 'round1', 'new', 0, None, 0, now, now, json.dumps(data, ensure_ascii=False)))
        c.execute('INSERT INTO sources(path,source_id,task_id) VALUES(?,?,?) '
                  'ON CONFLICT(path) DO UPDATE SET task_id=excluded.task_id', (path, source_id, task_id))
        c.execute('INSERT OR IGNORE INTO source_versions VALUES(?,?)',(path,task_id))
        if supersede_identity:
            c.execute('UPDATE sources SET task_id=? WHERE source_id=?',(task_id,source_id))
            c.execute('INSERT OR IGNORE INTO source_versions(path,task_id) SELECT path,task_id FROM sources WHERE source_id=?',(source_id,))
        return self.decode(c.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone())

    def get(self, task_id):
        with self.db() as c:
            return self.decode(c.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone())

    def update(self, task_id, expected_version=None,drop_fields=(),**fields):
        with self.db() as c:
            c.execute('BEGIN IMMEDIATE')
            data = self.decode(c.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone())
            if data is None:
                raise KeyError(task_id)
            version=data['version']
            if expected_version is not None and version!=expected_version:raise UpdateConflict(task_id)
            data.update(fields)
            for key in drop_fields:data.pop(key,None)
            data['version']=version+1
            result=c.execute('UPDATE tasks SET stage=?,kind=?,due=?,owner=?,lease_until=?,updated=?,data=?,version=version+1 WHERE task_id=? AND version=?',
                      (data['stage'], data['kind'], data.get('due', 0), data.get('owner'), data.get('lease_until', 0),
                       time.time(), json.dumps(data, ensure_ascii=False), task_id,version))
            if result.rowcount!=1:raise UpdateConflict(task_id)
        return data

    def request(self,task_id,**fields):
        """外部接管/唤醒提出控制请求，由 LangGraph 接受后同步阶段副本。"""
        return self.update(task_id,control_request=dict(id=str(time.time_ns()),fields=fields))

    def claim(self, stages, owner, now=None, lease_seconds=3000,exclude_ids=(),prefer_kind=None):
        if not stages:
            return None
        now = time.time() if now is None else now
        placeholders = ','.join('?' for _ in stages)
        turn_key='dispatch_turn:'+','.join(sorted(stages))
        with self.db() as c:
            c.execute('BEGIN IMMEDIATE')
            turn = c.execute('SELECT value FROM settings WHERE key=?',(turn_key,)).fetchone()
            turn = int(turn[0]) if turn else 0
            prefer = prefer_kind or ('repair' if turn % 3 == 2 else 'new')
            exclusion=' AND task_id NOT IN ('+','.join('?' for _ in exclude_ids)+')' if exclude_ids else ''
            row = c.execute(f'''SELECT * FROM tasks WHERE stage IN ({placeholders})
                            AND due<=? AND lease_until<=?
                            {exclusion}
                            ORDER BY CASE WHEN kind=? THEN 0 ELSE 1 END, created,task_id LIMIT 1''',
                            (*stages, now, now, *exclude_ids, prefer)).fetchone()
            if row is None:
                return None
            c.execute('UPDATE tasks SET owner=?,lease_until=?,updated=? WHERE task_id=?',
                      (owner, now + lease_seconds, now, row['task_id']))
            c.execute('INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                      (turn_key,str(turn + 1)))
            data = self.decode(row)
            data.update(owner=owner, lease_until=now + lease_seconds)
            return data

    def all_tasks(self):
        with self.db() as c:
            return [self.decode(r) for r in c.execute('SELECT * FROM tasks ORDER BY created,task_id')]

    def counts(self):
        with self.db() as c:
            return {r[0]: r[1] for r in c.execute('SELECT stage,count(*) FROM tasks GROUP BY stage')}

    def active_for(self, path):
        with self.db() as c:
            row = c.execute('SELECT active_task FROM sources WHERE path=?', (str(Path(path).resolve()),)).fetchone()
            return row[0] if row else None

    def mark_active(self, task_id, activation_revision=None):
        with self.db() as c:
            c.execute('BEGIN IMMEDIATE')
            if not c.execute('SELECT 1 FROM sources WHERE task_id=?',(task_id,)).fetchone():
                raise StaleSourceVersion('source has a newer version')
            revision=activation_revision
            if revision is None:
                revision=json.loads(c.execute('SELECT data FROM tasks WHERE task_id=?',(task_id,)).fetchone()[0])['revision']
            c.execute('UPDATE sources SET active_task=?,active_revision=? WHERE task_id=?', (task_id,revision,task_id))
            if activation_revision is not None:
                row=c.execute('SELECT data FROM receipts WHERE task_id=? AND revision=? AND target=?',
                              (task_id,activation_revision,'activation')).fetchone()
                if row is None:raise ValueError('activation intent missing')
                journal=json.loads(row[0]);journal.update(status='committed',committed_at=time.time())
                c.execute('UPDATE receipts SET data=? WHERE task_id=? AND revision=? AND target=?',
                          (json.dumps(journal,ensure_ascii=False),task_id,activation_revision,'activation'))

    def is_current(self,task_id):
        with self.db() as c:
            return c.execute('SELECT 1 FROM sources WHERE task_id=?',(task_id,)).fetchone() is not None

    def previous_active(self, task_id):
        with self.db() as c:
            return [r[0] for r in c.execute('SELECT DISTINCT s.active_task FROM source_versions v JOIN sources s ON v.path=s.path '
                                          'WHERE v.task_id=? AND s.active_task IS NOT NULL AND s.active_task<>?',(task_id,task_id))]

    def active_versions_for(self,task_id):
        with self.db() as c:
            return [dict(task_id=r[0],revision=r[1]) for r in c.execute(
                'SELECT DISTINCT s.active_task,s.active_revision FROM source_versions v JOIN sources s ON v.path=s.path '
                'WHERE v.task_id=? AND s.active_task IS NOT NULL',(task_id,))]

    def heartbeat(self,task_ids,owner,now=None,lease_seconds=3000):
        now=time.time() if now is None else now
        with self.db() as c:
            for task_id in task_ids:
                c.execute('UPDATE tasks SET lease_until=? WHERE task_id=? AND owner=?',
                          (now+lease_seconds,task_id,owner))

    def release_lease(self,task_id,owner):
        if owner is None:return
        with self.db() as c:
            c.execute('UPDATE tasks SET owner=NULL,lease_until=0 WHERE task_id=? AND owner=?',(task_id,owner))

    def receipt(self, task_id, revision, target, value=None):
        with self.db() as c:
            if value is not None:
                c.execute('INSERT INTO receipts VALUES(?,?,?,?) ON CONFLICT(task_id,revision,target) '
                          'DO UPDATE SET data=excluded.data', (task_id, revision, target, json.dumps(value, ensure_ascii=False)))
            row = c.execute('SELECT data FROM receipts WHERE task_id=? AND revision=? AND target=?',
                            (task_id, revision, target)).fetchone()
            return json.loads(row[0]) if row else {}

    def target_receipts(self,task_id,revision):
        with self.db() as c:
            rows=c.execute('SELECT target,data FROM receipts WHERE task_id=? AND revision=?',(task_id,revision)).fetchall()
        targets={r['target']:json.loads(r['data']) for r in rows if r['target'] not in ('all','activation')}
        targets={k:v for k,v in targets.items() if isinstance(v,dict) and any(role in v for role in ('body','images','legacy_images'))}
        return targets or self.receipt(task_id,revision,'all')

    def event(self, task_id, kind, data):
        with self.db() as c:
            c.execute('INSERT INTO events(at,task_id,kind,data) VALUES(?,?,?,?)',
                      (time.time(), task_id, kind, json.dumps(data, ensure_ascii=False)))

    def recover_leases(self):
        # 只能在进程独占锁获取成功后调用，不能夺走另一实例的工作。
        with self.db() as c:
            c.execute('UPDATE tasks SET owner=NULL,lease_until=0 WHERE lease_until>0')

    def retired_versions(self,now=None,stable_seconds=7*86400):
        now=time.time() if now is None else now
        with self.db() as c:
            active={(r[0],r[1]) for r in c.execute('SELECT active_task,active_revision FROM sources WHERE active_task IS NOT NULL')}
            journals=[(r[0],r[1],json.loads(r[2])) for r in c.execute("SELECT task_id,revision,data FROM receipts WHERE target='activation'")]
        stable=set()
        for task_id,revision,journal in journals:
            if ((task_id,revision) in active and journal.get('status')=='committed' and
                    journal.get('committed_at') is not None and now-journal['committed_at']>=stable_seconds):
                stable.add((task_id,revision))
        retired={}
        for _,_,journal in journals:
            if journal.get('status')!='committed':continue
            for old in journal.get('previous_versions',[]):
                identity=(old['task_id'],old['revision'])
                current={(a['task_id'],a['revision']) for a in self.active_versions_for(old['task_id'])}
                if identity not in active and current and current.issubset(stable):retired[identity]=old
        return list(retired.values())

    def metrics(self,now=None):
        now=time.time() if now is None else now
        with self.db() as c:
            events=[(r[0],r[1],json.loads(r[2])) for r in c.execute(
                "SELECT at,task_id,data FROM events WHERE kind='stage' AND at>=?",(now-86400,))]
            tasks=c.execute('SELECT task_id,stage,created,data FROM tasks').fetchall()
            target_rows=c.execute('''SELECT r.task_id,r.target,
                json_extract(r.data,'$.verified') AS verified,
                json_extract(r.data,'$.retrieval_verified') AS indexed,
                COALESCE(json_extract(r.data,'$.body.document_id'),json_extract(r.data,'$.images.document_id'),
                         json_extract(r.data,'$.legacy_images[0].document_id')) AS document_id,
                EXISTS(SELECT 1 FROM sources s WHERE s.active_task=r.task_id AND s.active_revision=r.revision) AS active
                FROM receipts r JOIN tasks t ON r.task_id=t.task_id AND r.revision=json_extract(t.data,'$.revision')
                WHERE t.stage NOT IN ('complete','held','superseded') AND r.target NOT IN ('all','activation','gc')''').fetchall()
        successes={};stages={}
        for at,task_id,value in events:
            stage=value.get('stage','unknown')
            item=stages.setdefault(stage,dict(calls=0,total_seconds=0,failures={k:0 for k in ('infrastructure','content','controller')}))
            item['calls']+=1;item['total_seconds']+=value.get('seconds',0)
            if value.get('error_kind') in item['failures']:item['failures'][value['error_kind']]+=1
            if stage=='cleanup' and value.get('next_stage')=='complete':successes[(task_id,value.get('revision'))]=at
        for item in stages.values():item['average_seconds']=round(item.pop('total_seconds')/item['calls'],3)
        parked=[];bugs=[];backlog=[];partial=[];display_counts={};targets={}
        for row in target_rows:
            if row['verified']==1 and row['document_id']:
                value=targets.setdefault(row['task_id'],dict(written=set(),searchable=set()))
                value['written'].add(row['target'])
                if row['indexed']==1 and row['active']:value['searchable'].add(row['target'])
        for row in tasks:
            value=json.loads(row['data'])
            public=dict(task_id=row['task_id'],revision=value.get('revision'),error_kind=value.get('error_kind'))
            display_counts[row['stage']]=display_counts.get(row['stage'],0)+1
            progress=targets.get(row['task_id'])
            if progress and row['stage']!='complete':
                known={str(code) for code in value.get('route_codes',[]) if code is not None}
                pending={str(item.get('code') or '未确定专业') for item in value.get('pending_routes',[]) if isinstance(item,dict)}
                pending.update(known-progress['written'])
                partial.append(dict(public,stage=row['stage'],written_targets=sorted(progress['written']),
                                    searchable_targets=sorted(progress['searchable']),pending_targets=sorted(pending)))
                display_counts[row['stage']]-=1
                display_counts['partial_complete']=display_counts.get('partial_complete',0)+1
            if row['stage']=='parked':parked.append(public)
            elif row['stage']=='code_error':bugs.append(public)
            elif row['stage'] not in TERMINAL:backlog.append(row['created'])
        hour=sum(at>=now-3600 for at in successes.values())
        alerts=[]
        if bugs:alerts.append('code_error')
        if backlog and hour==0 and min(backlog)<=now-3600:alerts.append('no_success_1h_with_backlog')
        return dict(success_1h=hour,success_24h=len(successes),stages=stages,parked=parked,code_error=bugs,
                    backlog=len(backlog),oldest_backlog_seconds=round(now-min(backlog)) if backlog else 0,alerts=alerts,
                    partial=partial,display_counts={stage:count for stage,count in display_counts.items() if count})
