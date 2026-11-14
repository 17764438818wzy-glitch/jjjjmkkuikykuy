"""Frozen prepared scope/control facts on the existing Store writer lane.

Execution ownership, fairness, retry and service state remain SchedulingLedger's.
"""
from __future__ import annotations
from collections import Counter
import json
import os
from pathlib import Path
import sqlite3
import time
import uuid

from .handoff_store import HandoffLedger,canonical
from .prepared_protocol import PreparedIdentity,PreparedObservation,fingerprint

SCHEMA=(
    '''CREATE TABLE IF NOT EXISTS prepared_scopes (
        scope_id TEXT PRIMARY KEY,manifest_sha256 TEXT NOT NULL,member_count INTEGER NOT NULL,
        intent TEXT NOT NULL,generation INTEGER NOT NULL,created REAL NOT NULL)''',
    "CREATE UNIQUE INDEX IF NOT EXISTS prepared_one_active ON prepared_scopes(intent) WHERE intent='running'",
    '''CREATE TABLE IF NOT EXISTS prepared_members (
        scope_id TEXT NOT NULL REFERENCES prepared_scopes(scope_id),task_id TEXT NOT NULL,
        record TEXT NOT NULL,identity TEXT NOT NULL,context_sha256 TEXT NOT NULL,
        PRIMARY KEY(scope_id,task_id))''',
    '''CREATE TABLE IF NOT EXISTS prepared_events (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,scope_id TEXT NOT NULL,event TEXT NOT NULL,
        data TEXT NOT NULL,at REAL NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS prepared_observations (
        task_id TEXT PRIMARY KEY,data TEXT NOT NULL,updated REAL NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS prepared_http (
        request_key TEXT PRIMARY KEY,scope_id TEXT NOT NULL,task_id TEXT NOT NULL,stage TEXT NOT NULL,
        request_sha256 TEXT NOT NULL,generation INTEGER NOT NULL,state TEXT NOT NULL,
        started REAL NOT NULL,finished REAL,result TEXT)''',
    '''CREATE TABLE IF NOT EXISTS prepared_tombstones (
        task_id TEXT PRIMARY KEY,scope_id TEXT NOT NULL,proof TEXT NOT NULL,created REAL NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS prepared_exclusions (
        scope_id TEXT NOT NULL,task_id TEXT NOT NULL,proof TEXT NOT NULL,created REAL NOT NULL,
        PRIMARY KEY(scope_id,task_id))''',
    '''CREATE TABLE IF NOT EXISTS prepared_cleanup (
        scope_id TEXT PRIMARY KEY,generation INTEGER NOT NULL,evidence_sha256 TEXT NOT NULL)''',
)

def _member(entry):
    observation=entry['observation']
    return dict(record=entry['record'],identity=observation['identity'],context_sha256=observation['context_sha256'])

def scope_manifest(records):
    return fingerprint(sorted((_member(row) for row in records),key=canonical))

def task_key(identity,context):
    return 'PP_'+fingerprint(dict(identity=identity,context=context))[:40]

def _durable_json(path,value,*,exclusive=False):
    path=Path(path)
    with path.open('x' if exclusive else 'w',encoding='utf8') as handle:
        json.dump(value,handle,ensure_ascii=False,sort_keys=True)
        handle.flush();os.fsync(handle.fileno())


def mark_stop_pending(database,scope_id=None):
    """A sticky stop latch must not depend on reading a busy control database."""
    path=Path(database).with_suffix('.prepared-stop-pending.json')
    path.parent.mkdir(parents=True,exist_ok=True)
    try:_durable_json(path,dict(scope_id=scope_id),exclusive=True)
    except FileExistsError:pass

class PreparedRegistry(HandoffLedger):
    def __init__(self,store):
        super().__init__(store)
        self.authority_path=store.path.with_suffix('.prepared-authority.json')
        self.stop_path=store.path.with_suffix('.prepared-stop-pending.json')
        self._write(lambda c:[c.execute(sql) for sql in SCHEMA])
        with store.db() as c:
            row=c.execute("SELECT value FROM settings WHERE key='prepared_authority'").fetchone()
            scopes=c.execute('SELECT COUNT(*) FROM prepared_scopes').fetchone()[0]
        if row is None:
            if self.authority_path.exists() or scopes:
                raise ValueError('prepared control facts missing; inventory only')
            identity=uuid.uuid4().hex
            _durable_json(self.authority_path,dict(schema='prepared-authority/1',identity=identity),exclusive=True)
            self._write(lambda c:c.execute("INSERT INTO settings VALUES('prepared_authority',?)",(identity,)))
        self._authority()

    def _authority(self):
        try:
            anchor=json.loads(self.authority_path.read_text(encoding='utf8'))
            with self.store.db() as c:
                row=c.execute("SELECT value FROM settings WHERE key='prepared_authority'").fetchone()
            if not row or anchor.get('schema')!='prepared-authority/1' or row['value']!=anchor.get('identity'):
                raise ValueError('prepared control authority changed')
        except (OSError,ValueError,KeyError,TypeError) as exc:
            raise ValueError('prepared control authority missing or unreadable') from exc

    def _scope(self,c,scope_id):
        # These related facts must share a SQLite snapshot. Without an explicit
        # read transaction, another worker can commit between the event scan
        # and HTTP rows and make intact control history appear corrupt.
        # Existing writer transactions retain their original admission lock.
        if not c.in_transaction:c.execute('BEGIN')
        row=c.execute('SELECT * FROM prepared_scopes WHERE scope_id=?',(scope_id,)).fetchone()
        if row is None:raise ValueError('unknown prepared scope')
        members=c.execute('SELECT record,identity,context_sha256 FROM prepared_members WHERE scope_id=?',(scope_id,)).fetchall()
        actual=[dict(record=json.loads(r['record']),identity=json.loads(r['identity']),context_sha256=r['context_sha256']) for r in members]
        if len(members)!=row['member_count'] or fingerprint(sorted(actual,key=canonical))!=row['manifest_sha256']:
            raise ValueError('prepared control members missing or changed')
        event=c.execute("SELECT data FROM prepared_events WHERE scope_id=? AND event IN ('created','intent') ORDER BY sequence DESC LIMIT 1",(scope_id,)).fetchone()
        if not event or json.loads(event['data'])!=dict(generation=row['generation'],intent=row['intent']):
            raise ValueError('prepared control intent history missing')
        attempts={}
        for saved in c.execute("SELECT event,data FROM prepared_events WHERE scope_id=? AND event IN ('http_started','http_result') ORDER BY sequence",(scope_id,)):
            fact=json.loads(saved['data']);key=fact['request_key']
            if saved['event']=='http_started':
                if key in attempts:raise ValueError('prepared control HTTP duplicate history')
                attempts[key]=dict(fact,state='possible')
            else:
                if key not in attempts:raise ValueError('prepared control HTTP start missing')
                attempts[key].update(state=fact['outcome'],result=fact['result'])
        actual_http=c.execute('SELECT * FROM prepared_http WHERE scope_id=?',(scope_id,)).fetchall()
        if len(attempts)!=len(actual_http):raise ValueError('prepared control HTTP budget missing')
        for attempt in actual_http:
            fact=attempts.get(attempt['request_key'],{})
            if (any(fact.get(k)!=attempt[k] for k in ('task_id','stage','request_sha256','generation','state'))
                    or fact.get('result')!=json.loads(attempt['result'] or '{}')):
                raise ValueError('prepared control HTTP facts missing or changed')
        for table,event_name in (('prepared_tombstones','cleaned'),('prepared_exclusions','excluded')):
            facts={r['task_id']:json.loads(r['proof']) for r in c.execute('SELECT task_id,proof FROM '+table+' WHERE scope_id=?',(scope_id,))}
            events={}
            for r in c.execute('SELECT data FROM prepared_events WHERE scope_id=? AND event=? ORDER BY sequence',(scope_id,event_name)):
                event_data=json.loads(r['data']);events[event_data['task_id']]=event_data['proof']
            if facts!=events:raise ValueError('prepared control completion facts missing or changed')
        return dict(row)

    @staticmethod
    def _event(c,scope_id,event,data):
        c.execute('INSERT INTO prepared_events(scope_id,event,data,at) VALUES(?,?,?,?)',(scope_id,event,canonical(data),time.time()))

    def create_scope(self,records,*,scope_id,manifest_sha256):
        self._authority()
        if not scope_id or not records or scope_manifest(records)!=manifest_sha256:
            raise ValueError('invalid frozen scope manifest')
        def write(c):
            old=c.execute('SELECT * FROM prepared_scopes WHERE scope_id=?',(scope_id,)).fetchone()
            if old:
                if old['manifest_sha256']!=manifest_sha256:raise ValueError('scope is frozen')
                return self._scope(c,scope_id)
            from .original_archive.admission import assert_available
            assert_available(self.store.path,[entry['record'].get('path') or entry['record'].get('source_path') for entry in records])
            c.execute('INSERT INTO prepared_scopes VALUES(?,?,?,?,?,?)',(scope_id,manifest_sha256,len(records),'stopped',0,time.time()))
            seen=set()
            for entry in records:
                member=_member(entry);key=task_key(member['identity'],member['context_sha256'])
                if key in seen:raise ValueError('duplicate material/context in frozen scope')
                seen.add(key)
                if (c.execute('SELECT 1 FROM prepared_members WHERE task_id=?',(key,)).fetchone()
                        and not c.execute('SELECT 1 FROM prepared_tombstones WHERE task_id=?',(key,)).fetchone()):
                    raise ValueError('新范围含已有范围的未完成材料；请继续当前范围，核验完成后再建立新范围')
                identity=PreparedIdentity(**member['identity'])
                source=entry['record'].get('path') or entry['record'].get('source_path')
                source_id='prepared_'+member['context_sha256'][:24]
                data=dict(task_id=key,source_id=source_id,source_sha256=identity.source_sha256,
                    source_path=source,revision=1,repair_count=0,stagnant_count=0,
                    contract_version='paper-preextract-prepared/1',execution_engine='paper_preextract',
                    material_identity=member['identity'],source_context_sha256=member['context_sha256'])
                now=time.time()
                c.execute('INSERT OR IGNORE INTO tasks(task_id,source_sha256,stage,kind,due,created,updated,data) VALUES(?,?,?,?,?,?,?,?)',
                    (key,identity.source_sha256,'prepared','historical',0,now,now,canonical(data)))
                c.execute('INSERT INTO prepared_members VALUES(?,?,?,?,?)',
                    (scope_id,key,canonical(member['record']),canonical(member['identity']),member['context_sha256']))
                c.execute('INSERT OR IGNORE INTO prepared_observations VALUES(?,?,?)',(key,canonical(entry['observation']),now))
            self._event(c,scope_id,'created',dict(generation=0,intent='stopped'))
            return self._scope(c,scope_id)
        return self._write(write)

    def members(self,scope_id):
        self._authority()
        with self.store.db() as c:
            self._scope(c,scope_id)
            rows=c.execute('SELECT * FROM prepared_members WHERE scope_id=? ORDER BY rowid',(scope_id,)).fetchall()
            return [dict(r,record=json.loads(r['record']),identity=json.loads(r['identity'])) for r in rows]

    def set_intent(self,scope_id,*,expected_generation,intent):
        if intent not in ('running','stopped'):raise ValueError('invalid intent')
        if intent=='stopped':
            # Sticky fail-closed marker is written before touching a possibly busy DB.
            mark_stop_pending(self.store.path,scope_id)
        elif self.stop_path.exists():
            raise ValueError('pending stop must be persisted before explicit resume')
        self._authority()
        def write(c):
            row=self._scope(c,scope_id)
            if row['generation']!=expected_generation:raise ValueError('stale scope generation')
            if intent=='running' and c.execute("SELECT 1 FROM prepared_scopes WHERE intent='running' AND scope_id!=?",(scope_id,)).fetchone():
                raise ValueError('another prepared scope is running')
            generation=expected_generation+1
            c.execute('UPDATE prepared_scopes SET intent=?,generation=? WHERE scope_id=?',(intent,generation,scope_id))
            self._event(c,scope_id,'intent',dict(generation=generation,intent=intent))
            return dict(row,generation=generation,intent=intent)
        result=self._write(write)
        if intent=='stopped':self.stop_path.unlink(missing_ok=True)
        return result

    def assert_running(self,scope_id,generation,*,connection=None):
        self._authority()
        if self.stop_path.exists():raise ValueError('pending stop denies new work')
        def check(c):
            row=self._scope(c,scope_id)
            if row['intent']!='running' or row['generation']!=generation:
                raise ValueError('scope stopped or generation changed')
            return row
        if connection is not None:return check(connection)
        with self.store.db() as c:return check(c)

    def refresh_observation(self,scope_id,observation):
        key=task_key(observation.as_dict()['identity'],observation.context_sha256)
        def write(c):
            self._scope(c,scope_id)
            if not c.execute('SELECT 1 FROM prepared_members WHERE scope_id=? AND task_id=?',(scope_id,key)).fetchone():
                raise ValueError('observation outside frozen scope')
            c.execute('INSERT INTO prepared_observations VALUES(?,?,?) ON CONFLICT(task_id) DO UPDATE SET data=excluded.data,updated=excluded.updated',
                (key,canonical(observation.as_dict()),time.time()))
        self._write(write)

    def observation(self,task_id):
        with self.store.db() as c:
            row=c.execute('SELECT data FROM prepared_observations WHERE task_id=?',(task_id,)).fetchone()
        return PreparedObservation.from_dict(json.loads(row['data'])) if row else None

    def begin_http(self,scope_id,generation,task_id,request_key,stage,request_sha256,*,limit,evidence=None):
        if type(limit) is not int or limit<1:raise ValueError('invalid HTTP budget')
        def write(c):
            self.assert_running(scope_id,generation,connection=c)
            if not c.execute('SELECT 1 FROM prepared_members WHERE scope_id=? AND task_id=?',(scope_id,task_id)).fetchone():
                raise ValueError('request outside frozen scope')
            if c.execute("SELECT 1 FROM prepared_http WHERE task_id=? AND stage=? AND state IN ('possible','unknown')",(task_id,stage)).fetchone():
                raise ValueError('unresolved external request must be reconciled')
            if c.execute("SELECT COUNT(*) FROM prepared_http WHERE task_id=? AND stage=? AND state!='not_sent'",(task_id,stage)).fetchone()[0]>=limit:
                raise ValueError('actual HTTP budget exhausted')
            if c.execute('SELECT 1 FROM prepared_http WHERE request_key=?',(request_key,)).fetchone():
                raise ValueError('request identity already used')
            c.execute('INSERT INTO prepared_http VALUES(?,?,?,?,?,?,?, ?,NULL,?)',
                (request_key,scope_id,task_id,stage,request_sha256,generation,'possible',time.time(),canonical(evidence or {})))
            self._event(c,scope_id,'http_started',dict(request_key=request_key,task_id=task_id,stage=stage,
                request_sha256=request_sha256,generation=generation,result=evidence or {}))
        self._write(write)

    def finish_http(self,request_key,outcome,result):
        if outcome not in ('complete','failed','unknown','not_sent'):raise ValueError('invalid HTTP outcome')
        def write(c):
            row=c.execute('SELECT * FROM prepared_http WHERE request_key=?',(request_key,)).fetchone()
            if not row:raise ValueError('unknown HTTP request identity')
            if row['state'] not in ('possible','unknown'):
                if row['state']==outcome and row['result']==canonical(result):return
                raise ValueError('HTTP result conflict')
            c.execute('UPDATE prepared_http SET state=?,finished=?,result=? WHERE request_key=?',(outcome,time.time(),canonical(result),request_key))
            self._event(c,row['scope_id'],'http_result',dict(request_key=request_key,outcome=outcome,result=result))
        self._write(write)

    def record_not_sent(self,scope_id,generation,task_id,request_key,stage,request_sha256,directory):
        """A denied permit never entered the transport; persist that late fact."""
        def write(c):
            row=self._scope(c,scope_id)
            if generation>row['generation'] or not c.execute('SELECT 1 FROM prepared_members WHERE scope_id=? AND task_id=?',(scope_id,task_id)).fetchone():
                raise ValueError('invalid denied request identity')
            result=dict(work_dir=str(directory),reason='permit_did_not_enter_transport')
            now=time.time()
            c.execute('INSERT INTO prepared_http VALUES(?,?,?,?,?,?,?,?,?,?)',
                (request_key,scope_id,task_id,stage,request_sha256,generation,'not_sent',now,now,canonical(result)))
            self._event(c,scope_id,'http_started',dict(request_key=request_key,task_id=task_id,stage=stage,
                request_sha256=request_sha256,generation=generation,result=result))
            self._event(c,scope_id,'http_result',dict(request_key=request_key,outcome='not_sent',result=result))
        self._write(write)

    def record_tombstone(self,scope_id,task_id,proof):
        def write(c):
            self._scope(c,scope_id)
            if not c.execute('SELECT 1 FROM prepared_members WHERE scope_id=? AND task_id=?',(scope_id,task_id)).fetchone():
                raise ValueError('tombstone outside scope')
            previous=c.execute('SELECT proof FROM prepared_tombstones WHERE task_id=?',(task_id,)).fetchone()
            if previous and previous['proof']!=canonical(proof):raise ValueError('tombstone conflict')
            if previous:return
            c.execute('INSERT OR IGNORE INTO prepared_tombstones VALUES(?,?,?,?)',(task_id,scope_id,canonical(proof),time.time()))
            self._event(c,scope_id,'cleaned',dict(task_id=task_id,proof=proof))
        self._write(write)

    def cleanup_tombstone(self,task_id):
        with self.store.db() as c:
            row=c.execute('SELECT proof FROM prepared_tombstones WHERE task_id=?',(task_id,)).fetchone()
        return json.loads(row['proof']) if row else None

    def exclude(self,scope_id,task_id,proof):
        def write(c):
            self._scope(c,scope_id)
            if not c.execute('SELECT 1 FROM prepared_members WHERE scope_id=? AND task_id=?',(scope_id,task_id)).fetchone():raise ValueError('exclusion outside scope')
            previous=c.execute('SELECT proof FROM prepared_exclusions WHERE scope_id=? AND task_id=?',(scope_id,task_id)).fetchone()
            if previous:
                if previous['proof']!=canonical(proof):raise ValueError('exclusion changed')
                return
            c.execute('INSERT INTO prepared_exclusions VALUES(?,?,?,?)',(scope_id,task_id,canonical(proof),time.time()))
            self._event(c,scope_id,'excluded',dict(task_id=task_id,proof=proof))
        self._write(write)

    def exclusion(self,scope_id,task_id):
        with self.store.db() as c:
            self._scope(c,scope_id)
            row=c.execute('SELECT proof FROM prepared_exclusions WHERE scope_id=? AND task_id=?',(scope_id,task_id)).fetchone()
        return json.loads(row['proof']) if row else None

    def authorize_cleanup(self,scope_id,generation,evidence_sha256):
        def write(c):
            self.assert_running(scope_id,generation,connection=c)
            value=dict(generation=generation,evidence_sha256=evidence_sha256)
            c.execute('INSERT OR REPLACE INTO prepared_cleanup VALUES(?,?,?)',(scope_id,generation,evidence_sha256))
            self._event(c,scope_id,'cleanup_admitted',value)
        self._write(write)

    def cleanup_authorized(self,scope_id,generation,evidence_sha256):
        self._authority()
        if self.stop_path.exists():return False
        with self.store.db() as c:
            scope=self._scope(c,scope_id)
            if scope['intent'] not in ('running','completed') or scope['generation']!=generation:return False
            if scope['intent']=='completed':
                completed=c.execute("SELECT data FROM prepared_events WHERE scope_id=? AND event='completed' ORDER BY sequence DESC LIMIT 1",(scope_id,)).fetchone()
                if not completed or json.loads(completed['data'])!=dict(generation=generation,evidence_sha256=evidence_sha256):return False
            row=c.execute('SELECT generation,evidence_sha256 FROM prepared_cleanup WHERE scope_id=?',(scope_id,)).fetchone()
            event=c.execute("SELECT data FROM prepared_events WHERE scope_id=? AND event='cleanup_admitted' ORDER BY sequence DESC LIMIT 1",(scope_id,)).fetchone()
        return bool(row and event and dict(row)==dict(generation=generation,evidence_sha256=evidence_sha256)==json.loads(event['data']))

    def complete_scope(self,scope_id,generation,evidence_sha256):
        """Natural delivery completion is not a user stop and does not wait for deletion."""
        if not evidence_sha256:raise ValueError('completion evidence required')
        def write(c):
            scope=self.assert_running(scope_id,generation,connection=c)
            rows=c.execute('SELECT o.data FROM prepared_members m LEFT JOIN prepared_observations o ON o.task_id=m.task_id WHERE m.scope_id=?',(scope_id,)).fetchall()
            if not rows or any(not r['data'] or json.loads(r['data'])['phase'] not in ('delivery_verified','cleaned','retained','excluded') for r in rows):
                raise ValueError('scope is incomplete')
            if c.execute("SELECT 1 FROM prepared_http h JOIN prepared_members m ON h.task_id=m.task_id WHERE m.scope_id=? AND h.state IN ('possible','unknown')",(scope_id,)).fetchone():
                raise ValueError('unresolved request prevents completion')
            value=dict(generation=generation,evidence_sha256=evidence_sha256)
            c.execute('INSERT OR REPLACE INTO prepared_cleanup VALUES(?,?,?)',(scope_id,generation,evidence_sha256))
            self._event(c,scope_id,'cleanup_admitted',value)
            self._event(c,scope_id,'completed',value)
            c.execute("UPDATE prepared_scopes SET intent='completed' WHERE scope_id=?",(scope_id,))
            self._event(c,scope_id,'intent',dict(generation=generation,intent='completed'))
            return dict(scope,intent='completed')
        return self._write(write)

    def scope_snapshot(self,scope_id):
        self._authority()
        with self.store.db() as c:
            scope=self._scope(c,scope_id)
            rows=c.execute('SELECT o.data FROM prepared_members m LEFT JOIN prepared_observations o ON o.task_id=m.task_id WHERE m.scope_id=?',(scope_id,)).fetchall()
            phases=Counter(json.loads(r['data'])['phase'] if r['data'] else 'observation_missing' for r in rows)
            counts=c.execute('SELECT state,COUNT(*) AS n FROM prepared_http WHERE scope_id=? GROUP BY state',(scope_id,)).fetchall()
        return dict(scope,phases=dict(phases),http_attempts=sum(r['n'] for r in counts if r['state']!='not_sent'),
                    http_states={r['state']:r['n'] for r in counts},stop_pending=self.stop_path.exists())

    def backup_control(self,path):
        """SQLite backup retains all authoritative scope and existing scheduler facts."""
        self._authority()
        target=Path(path)
        if self.stop_path.exists():raise ValueError('pending stop must be persisted before control backup')
        if target.exists():raise FileExistsError(target)
        with self.store.db() as source,sqlite3.connect(target) as destination:source.backup(destination)
        _durable_json(target.with_suffix('.prepared-authority.json'),json.loads(self.authority_path.read_text(encoding='utf8')),exclusive=True)
        if self.stop_path.exists():
            _durable_json(target.with_suffix('.prepared-stop-pending.json'),json.loads(self.stop_path.read_text(encoding='utf8')),exclusive=True)
