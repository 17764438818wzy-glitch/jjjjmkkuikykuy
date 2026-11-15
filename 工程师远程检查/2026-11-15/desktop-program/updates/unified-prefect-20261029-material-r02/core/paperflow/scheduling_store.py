"""Durable queue, service incidents and network budgets. Never grants permission."""
import hashlib
import json
import time
import uuid

from .handoff_store import HandoffLedger,canonical
from .scheduling_policy import backoff


SCHEMA=(
    '''CREATE TABLE IF NOT EXISTS scheduler_services (
        service_id TEXT PRIMARY KEY,kind TEXT NOT NULL,descriptor TEXT NOT NULL,state TEXT NOT NULL,
        event_id TEXT,next_probe REAL NOT NULL DEFAULT 0,probe_job TEXT,failures INTEGER NOT NULL DEFAULT 0)''',
    '''CREATE TABLE IF NOT EXISTS scheduler_events (
        event_id TEXT PRIMARY KEY,service_id TEXT NOT NULL REFERENCES scheduler_services(service_id),
        started REAL NOT NULL,ended REAL,category TEXT NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS scheduler_observations (
        action_id TEXT PRIMARY KEY,service_id TEXT NOT NULL,article TEXT NOT NULL,at REAL NOT NULL,
        category TEXT NOT NULL,family TEXT NOT NULL,attempt_id TEXT,event_id TEXT,data TEXT NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS scheduler_jobs (
        job_id TEXT PRIMARY KEY,kind TEXT NOT NULL,article TEXT NOT NULL,unit_key TEXT NOT NULL,
        service_id TEXT NOT NULL REFERENCES scheduler_services(service_id),data TEXT NOT NULL,
        state TEXT NOT NULL,due REAL NOT NULL,created REAL NOT NULL,turn INTEGER NOT NULL DEFAULT 0,
        max_rounds INTEGER NOT NULL DEFAULT 20,max_seconds REAL NOT NULL DEFAULT 86400)''',
    '''CREATE UNIQUE INDEX IF NOT EXISTS scheduler_single_unit ON scheduler_jobs(unit_key) WHERE state='running' ''',
    '''CREATE INDEX IF NOT EXISTS scheduler_due ON scheduler_jobs(state,due)''',
    '''CREATE TABLE IF NOT EXISTS scheduler_attempts (
        attempt_id TEXT PRIMARY KEY,job_id TEXT NOT NULL REFERENCES scheduler_jobs(job_id),
        execution_id TEXT UNIQUE,state TEXT NOT NULL,started REAL NOT NULL,finished REAL,outcome TEXT,data TEXT)''',
    '''CREATE TABLE IF NOT EXISTS scheduler_rounds (
        attempt_id TEXT PRIMARY KEY REFERENCES scheduler_attempts(attempt_id),job_id TEXT NOT NULL,
        service_id TEXT NOT NULL,at REAL NOT NULL,event_id TEXT)''',
    '''CREATE TABLE IF NOT EXISTS scheduler_recoveries (
        attempt_id TEXT PRIMARY KEY,unit_key TEXT NOT NULL,service_id TEXT NOT NULL,at REAL NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS scheduler_articles (article TEXT PRIMARY KEY,turn INTEGER NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS scheduler_kinds (kind TEXT PRIMARY KEY,turn INTEGER NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS scheduler_counter (id INTEGER PRIMARY KEY CHECK(id=1),turn INTEGER NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS scheduler_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,epoch TEXT NOT NULL,started REAL NOT NULL,last_seen REAL NOT NULL,ended REAL)''',
)
RUNNABLE=('queued','retry','blocked_service')


def selection_filter(task_ids):
    """Exact ownership across graph, delivery, attachment and cleanup queue units."""
    if task_ids is None:return '',()
    ids=tuple(task_ids)
    if not ids:return ' AND 0',()
    marks='SELECT value FROM json_each(?)'
    return f''' AND ((json_extract(j.data,'$.execution_engine')='paper_preextract' AND j.unit_key IN ({marks}))
        OR (j.kind IN ('graph','repair') AND j.unit_key IN ({marks}))
        OR (j.kind='delivery' AND (json_extract(j.data,'$.manifest_sha256') IN
            (SELECT manifest_sha256 FROM delivery_packages WHERE task_id IN ({marks}))
            OR json_extract(j.data,'$.attachment_id') IN
            (SELECT attachment_id FROM attachment_tasks WHERE task_id IN ({marks}))))
        OR (j.kind='cleanup' AND json_extract(j.data,'$.item_id') IN
            (SELECT item_id FROM cleanup_items WHERE task_id IN ({marks}))))''',(canonical(ids),)*5


def selected_task_sql(c,job):
    if json.loads(job['data']).get('execution_engine')=='paper_preextract':return job['unit_key']
    if job['kind'] in ('graph','repair'):return job['unit_key']
    data=json.loads(job['data'])
    if job['kind']=='delivery' and data.get('attachment_id'):
        row=c.execute('SELECT task_id FROM attachment_tasks WHERE attachment_id=?',(data['attachment_id'],)).fetchone()
    elif job['kind']=='delivery':
        row=c.execute('SELECT task_id FROM delivery_packages WHERE manifest_sha256=?',(data.get('manifest_sha256'),)).fetchone()
    elif job['kind']=='cleanup':
        row=c.execute('SELECT task_id FROM cleanup_items WHERE item_id=?',(data.get('item_id'),)).fetchone()
    else:return None
    return row['task_id'] if row else None


class SchedulingLedger(HandoffLedger):
    def __init__(self,store):
        super().__init__(store)
        self._write(lambda c:[c.execute(sql) for sql in SCHEMA])
        self._write(lambda c:c.execute('INSERT OR IGNORE INTO scheduler_counter VALUES(1,0)'))

    def rows(self,query,args=()):
        with self.store.db(timeout=.1) as c:return [dict(row) for row in c.execute(query,args)]

    def ensure_service(self,kind,descriptor):
        if kind not in ('llm','rag','local'):raise ValueError('unknown service kind')
        serialized=canonical(descriptor)
        identity=kind+':'+hashlib.sha256(serialized.encode()).hexdigest()
        self._write(lambda c:c.execute('INSERT OR IGNORE INTO scheduler_services(service_id,kind,descriptor,state) VALUES(?,?,?,?)',
            (identity,kind,serialized,'closed')))
        return self.service(identity)

    def service(self,identity):return self.rows('SELECT * FROM scheduler_services WHERE service_id=?',(identity,))[0]

    def boot(self,epoch,*,now=None):
        # Offline wall time is not effective failure time. A new manager still starts paused.
        self._write(lambda c:c.execute('UPDATE scheduler_sessions SET ended=last_seen WHERE ended IS NULL AND epoch!=?',(epoch,)))

    def session(self,epoch,active,*,now=None):
        now=time.time() if now is None else now
        def write(c):
            row=c.execute('SELECT * FROM scheduler_sessions WHERE epoch=? AND ended IS NULL',(epoch,)).fetchone()
            if row:
                c.execute('UPDATE scheduler_sessions SET last_seen=?,ended=? WHERE id=?',
                    (max(now,row['last_seen']),None if active else max(now,row['last_seen']),row['id']))
            elif active:c.execute('INSERT INTO scheduler_sessions(epoch,started,last_seen) VALUES(?,?,?)',(epoch,now,now))
        self._write(write)

    def enqueue(self,kind,article,unit_key,data,service_id,*,now=None):
        now=time.time() if now is None else now
        if kind not in ('graph','repair','delivery','cleanup','prepared_local') or not article or not unit_key:raise ValueError('invalid queue unit')
        return self._write(lambda c:self.enqueue_sql(c,kind,article,unit_key,data,service_id,now))

    @staticmethod
    def enqueue_sql(c,kind,article,unit_key,data,service_id,now):
        identity=hashlib.sha256(canonical([kind,unit_key,data]).encode()).hexdigest()
        turn=c.execute('SELECT turn FROM scheduler_counter WHERE id=1').fetchone()[0]
        c.execute('INSERT OR IGNORE INTO scheduler_articles VALUES(?,?)',(article,turn))
        c.execute('INSERT OR IGNORE INTO scheduler_kinds VALUES(?,0)',(kind,))
        c.execute('INSERT OR IGNORE INTO scheduler_jobs(job_id,kind,article,unit_key,service_id,data,state,due,created) '
            'VALUES(?,?,?,?,?,?,?,0,?)',(identity,kind,article,unit_key,service_id,canonical(data),'queued',now))
        return identity

    def enqueue_safety(self,article,unit_key,data,service_id):
        def write(c):
            previous=c.execute("SELECT job_id FROM scheduler_jobs WHERE kind='delivery' AND unit_key=? "
                "AND json_extract(data,'$.safety_manifest')=? AND json_extract(data,'$.manifest_sha256')=? "
                "AND json_extract(data,'$.generation')=? AND state!='complete'",
                (unit_key,data['safety_manifest'],data['manifest_sha256'],data['generation'])).fetchone()
            if previous:return previous['job_id']
            sequence=c.execute("SELECT COALESCE(MAX(json_extract(data,'$.safety_sequence')),0)+1 FROM scheduler_jobs "
                "WHERE kind='delivery' AND unit_key=?",(unit_key,)).fetchone()[0]
            return self.enqueue_sql(c,'delivery',article,unit_key,dict(data,safety_sequence=sequence),service_id,time.time())
        return self._write(write)

    def job(self,identity):return self.rows('SELECT * FROM scheduler_jobs WHERE job_id=?',(identity,))[0]

    def rebind_services(self,services):
        """Explicit configuration takeover, only after outstanding attempts have been reaped."""
        now=time.time()
        def write(c):
            for kind,lane in (('graph','llm'),('repair','llm'),('delivery','rag'),('cleanup','rag')):
                target=services[lane]['service_id']
                rows=c.execute("SELECT * FROM scheduler_jobs WHERE kind=? AND service_id!=? "
                    "AND COALESCE(json_extract(data,'$.prepared_local'),0)=0 "
                    "AND state IN ('queued','retry','blocked_service') AND (kind!='cleanup' OR json_extract(data,'$.resource_kind')='remote')",(kind,target)).fetchall()
                for job in rows:
                    # Stop the old configuration's clock; outstanding personal rounds are retained.
                    c.execute('INSERT INTO scheduler_recoveries VALUES(?,?,?,?)',
                        ('configuration:'+uuid.uuid4().hex,job['unit_key'],job['service_id'],now))
                    c.execute("UPDATE scheduler_jobs SET service_id=?,state='queued',due=0 WHERE job_id=?",(target,job['job_id']))
                    c.execute('INSERT INTO events(at,task_id,kind,data) VALUES(?,NULL,?,?)',
                        (now,'queue_service_rebound',canonical(dict(job_id=job['job_id'],previous=job['service_id'],current=target))))
        self._write(write)

    def claim(self,*,now=None,kinds=('graph','repair','delivery','cleanup'),task_ids=None,
              job_ids=None,still_valid=None,before_commit=None,_initialize=None):
        now=time.time() if now is None else now
        if not kinds:return None
        def write(c):
            marks=','.join('?' for _ in kinds)
            selected,parameters=selection_filter(task_ids)
            exact=(_initialize(c),) if _initialize is not None else job_ids
            if exact is None:
                selected+=" AND COALESCE(json_extract(j.data,'$.manual_local_only'),0)=0"
            else:
                selected+=' AND j.job_id IN (SELECT value FROM json_each(?))'
                parameters=(*parameters,canonical(tuple(exact)))
            while True:
                row=c.execute(f'''SELECT j.* FROM scheduler_jobs j JOIN scheduler_articles a ON a.article=j.article
                JOIN scheduler_kinds k ON k.kind=j.kind JOIN scheduler_services s ON s.service_id=j.service_id
                WHERE j.state IN ('queued','retry','blocked_service') AND j.due<=? AND j.kind IN ({marks})
                AND (s.state='closed' OR (s.state='half_open' AND (s.probe_job IS NULL OR s.probe_job=j.job_id)))
                AND NOT EXISTS(SELECT 1 FROM scheduler_jobs owned WHERE owned.unit_key=j.unit_key AND owned.state='running')
                {selected}
                ORDER BY (j.kind='cleanup'),k.turn,a.turn,j.turn,j.created,j.rowid LIMIT 1''',(now,*kinds,*parameters)).fetchone()
                if not row:return None
                budget=self.budget_sql(c,row['job_id'],now)
                if budget['rounds']<row['max_rounds'] and budget['seconds']<row['max_seconds']:break
                c.execute("UPDATE scheduler_jobs SET state='held_budget' WHERE job_id=?",(row['job_id'],))
            turn=c.execute('UPDATE scheduler_counter SET turn=turn+1 WHERE id=1 RETURNING turn').fetchone()[0]
            c.execute('UPDATE scheduler_articles SET turn=? WHERE article=?',(turn,row['article']))
            c.execute('UPDATE scheduler_kinds SET turn=? WHERE kind=?',(turn,row['kind']))
            c.execute("UPDATE scheduler_jobs SET state='running',turn=? WHERE job_id=?",(turn,row['job_id']))
            c.execute("UPDATE scheduler_services SET probe_job=? WHERE service_id=? AND state='half_open'",
                (row['job_id'],row['service_id']))
            attempt=uuid.uuid4().hex
            c.execute('INSERT INTO scheduler_attempts(attempt_id,job_id,state,started) VALUES(?,?,?,?)',
                (attempt,row['job_id'],'starting',now))
            return dict(row,attempt_id=attempt,payload=json.loads(row['data']))
        return self._write(write,still_valid=still_valid,before_commit=before_commit)

    def claim_local_recovery(self,task_id,data,service_id,*,still_valid=None,before_commit=None):
        """Insert and claim one explicit local action in the same transaction.

        Ordinary dispatch never selects these jobs; no enqueue/claim race can
        let a manager pick up a manual recovery without its local-only guard.
        """
        if not task_id or data.get('execution_engine')!='paper_preextract':
            raise ValueError('local recovery requires a prepared task identity')
        payload=dict(data,manual_local_only=True,local_only=True)
        def initialize(c):
            if c.execute("SELECT 1 FROM scheduler_jobs WHERE unit_key=? AND state='running'",(task_id,)).fetchone():return None
            return self.enqueue_sql(c,'prepared_local',task_id,task_id,payload,service_id,time.time())
        return self.claim(kinds=('prepared_local',),still_valid=still_valid,before_commit=before_commit,
                          _initialize=initialize)

    def attach_sql(self,c,attempt_id,execution_id,kind,key):
        row=c.execute('SELECT a.*,j.kind,j.unit_key FROM scheduler_attempts a JOIN scheduler_jobs j ON a.job_id=j.job_id '
                      'WHERE a.attempt_id=?',(attempt_id,)).fetchone()
        expected_key=row['unit_key'].removeprefix('cleanup:') if row and row['kind']=='cleanup' else row['unit_key'] if row else None
        if not row or row['state']!='starting' or row['execution_id'] or expected_key!=key:
            raise ValueError('queue ownership binding conflict')
        if ('graph' if row['kind'] in ('graph','repair') else row['kind'])!=kind:raise ValueError('queue kind mismatch')
        c.execute("UPDATE scheduler_attempts SET state='running',execution_id=? WHERE attempt_id=?",(execution_id,attempt_id))

    def observe(self,service_id,action_id,article,failure,*,now=None,attempt_id=None,job_id=None):
        return self._write(lambda c:self.observe_sql(c,service_id,action_id,article,failure,now=now,attempt_id=attempt_id,job_id=job_id))

    def observe_sql(self,c,service_id,action_id,article,failure,*,now=None,attempt_id=None,job_id=None):
        now=time.time() if now is None else now
        service=dict(c.execute('SELECT * FROM scheduler_services WHERE service_id=?',(service_id,)).fetchone())
        if c.execute('SELECT 1 FROM scheduler_observations WHERE action_id=?',(action_id,)).fetchone():return service
        category=('request_rejected' if failure.get('model_rejection') is True else failure['category']) if failure else 'success'
        if category not in ('transient','configuration','request_rejected','success'):return service
        family=failure.get('family','unavailable') if failure else 'success'
        event_id=service['event_id'] if service['state']!='closed' else None
        c.execute('INSERT INTO scheduler_observations VALUES(?,?,?,?,?,?,?,?,?)',
            (action_id,service_id,article,now,category,family,attempt_id,event_id,canonical(failure)))
        if category=='request_rejected':
            # Three consecutive equivalent model refusals block this model
            # service. A single bad document or a local validation error does not.
            recent=list(c.execute("SELECT category,family FROM scheduler_observations WHERE service_id=? "
                "AND at>=? ORDER BY rowid DESC LIMIT 3",(service_id,now-300)))
            confirmed=(service['state']!='closed' or
                len(recent)==3 and all(r['category']==category and r['family']==family for r in recent))
            if confirmed:
                if not event_id:
                    event_id='incident:'+hashlib.sha256((service_id+action_id).encode()).hexdigest()
                    c.execute('INSERT INTO scheduler_events VALUES(?,?,?,?,?)',(event_id,service_id,now,None,category))
                else:c.execute('UPDATE scheduler_events SET category=? WHERE event_id=?',(category,event_id))
                c.execute('UPDATE scheduler_observations SET event_id=? WHERE action_id=?',(event_id,action_id))
                c.execute("UPDATE scheduler_services SET state='open',event_id=?,next_probe=?,probe_job=NULL,failures=? WHERE service_id=?",
                    (event_id,now,service['failures']+(service['state'] in ('closed','half_open')),service_id))
        elif category=='success':
            if service['state']=='half_open' and job_id==service['probe_job']:
                c.execute('UPDATE scheduler_events SET ended=? WHERE event_id=? AND ended IS NULL',(now,event_id))
                c.execute("UPDATE scheduler_services SET state='closed',event_id=NULL,next_probe=0,probe_job=NULL,failures=0 WHERE service_id=?",(service_id,))
        else:
            observations=list(c.execute("SELECT * FROM scheduler_observations WHERE service_id=? AND family=? AND at>=? "
                "AND at<=? AND (event_id IS NULL OR event_id=?) AND category IN ('transient','configuration')",
                (service_id,family,now-60,now,event_id)))
            confirmed=(category=='configuration' or service['state']!='closed' or len({r['article'] for r in observations})>=2)
            if confirmed:
                if not event_id:
                    started=min(r['at'] for r in observations)
                    event_id='incident:'+hashlib.sha256((service_id+min(observations,key=lambda r:r['at'])['action_id']).encode()).hexdigest()
                    c.execute('INSERT INTO scheduler_events VALUES(?,?,?,?,?)',(event_id,service_id,started,None,category))
                    c.execute("UPDATE scheduler_observations SET event_id=? WHERE service_id=? AND family=? AND at>=? AND at<=? "
                        "AND event_id IS NULL AND category IN ('transient','configuration')",(event_id,service_id,family,started,now))
                failures=service['failures']+(service['state'] in ('closed','half_open'))
                due=max(service['next_probe'],now+backoff(failures,retry_after=failure.get('retry_after',0)))
                c.execute("UPDATE scheduler_services SET state='open',event_id=?,next_probe=?,probe_job=NULL,failures=? WHERE service_id=?",
                    (event_id,due,failures,service_id))
                c.execute('UPDATE scheduler_rounds SET event_id=? WHERE service_id=? AND attempt_id IN '
                    '(SELECT attempt_id FROM scheduler_observations WHERE event_id=? AND attempt_id IS NOT NULL)',(event_id,service_id,event_id))
                for job in c.execute("SELECT * FROM scheduler_jobs WHERE service_id=? AND state='held_budget'",(service_id,)).fetchall():
                    budget=self.budget_sql(c,job['job_id'],now)
                    if budget['rounds']<job['max_rounds'] and budget['seconds']<job['max_seconds']:
                        c.execute("UPDATE scheduler_jobs SET state='blocked_service',due=? WHERE job_id=?",(due,job['job_id']))
        return dict(c.execute('SELECT * FROM scheduler_services WHERE service_id=?',(service_id,)).fetchone())

    def open_probe(self,service_id,*,now=None):
        return self._write(lambda c:self.open_probe_sql(c,service_id,now=now))

    def open_probe_sql(self,c,service_id,*,now=None):
        now=time.time() if now is None else now
        row=c.execute('SELECT * FROM scheduler_services WHERE service_id=?',(service_id,)).fetchone()
        if row['state']!='open' or row['next_probe']>now:raise ValueError('probe not due')
        c.execute("UPDATE scheduler_services SET state='half_open',probe_job=NULL WHERE service_id=?",(service_id,))
        return dict(c.execute('SELECT * FROM scheduler_services WHERE service_id=?',(service_id,)).fetchone())

    def budget(self,job_id,*,now=None):
        now=time.time() if now is None else now
        with self.store.db(timeout=.1) as c:return self.budget_sql(c,job_id,now)

    @staticmethod
    def budget_sql(c,job_id,now):
        job=c.execute('SELECT * FROM scheduler_jobs WHERE job_id=?',(job_id,)).fetchone()
        rows=list(c.execute('SELECT r.at,r.service_id,COALESCE((SELECT MIN(at) FROM scheduler_recoveries x WHERE '
            'x.unit_key=j.unit_key AND x.service_id=r.service_id AND x.at>=r.at),?) AS until '
            'FROM scheduler_rounds r JOIN scheduler_jobs j ON j.job_id=r.job_id '
            'WHERE j.unit_key=? AND r.event_id IS NULL ORDER BY r.service_id,r.at',
            (now,job['unit_key'])))
        if not rows:return dict(rounds=0,seconds=0)
        intervals=[];seconds=0
        for row in rows:
            if intervals and row['service_id']==intervals[-1][2] and row['at']<=intervals[-1][1]:intervals[-1][1]=max(intervals[-1][1],min(now,row['until']))
            else:intervals.append([row['at'],min(now,row['until']),row['service_id']])
        for left,right,service_id in intervals:
            seconds+=c.execute('SELECT COALESCE(SUM(MAX(0,MIN(?,COALESCE(ended,last_seen))-MAX(?,started))),0) '
                'FROM scheduler_sessions WHERE started<? AND COALESCE(ended,last_seen)>?',(right,left,right,left)).fetchone()[0]
            seconds-=c.execute('SELECT COALESCE(SUM(MAX(0,MIN(?,COALESCE(s.ended,s.last_seen),COALESCE(e.ended,?)) '
                '-MAX(?,s.started,e.started))),0) FROM scheduler_sessions s JOIN scheduler_events e ON e.service_id=? '
                'WHERE s.started<? AND COALESCE(s.ended,s.last_seen)>? AND e.started<? AND COALESCE(e.ended,?)>?',
                (right,now,left,service_id,right,left,right,now,left)).fetchone()[0]
        return dict(rounds=len(rows),seconds=max(0,seconds))

    def finish(self,attempt_id,outcome,data,*,now=None,max_rounds=20,max_seconds=86400):
        now=time.time() if now is None else now
        def write(c):
            attempt=c.execute('SELECT * FROM scheduler_attempts WHERE attempt_id=?',(attempt_id,)).fetchone()
            if not attempt:raise ValueError('unknown attempt')
            if attempt['state']=='finished':
                if attempt['outcome']!=outcome or attempt['data']!=canonical(data):raise ValueError('attempt result conflict')
                return dict(c.execute('SELECT * FROM scheduler_jobs WHERE job_id=?',(attempt['job_id'],)).fetchone())
            job=c.execute('SELECT * FROM scheduler_jobs WHERE job_id=?',(attempt['job_id'],)).fetchone()
            service=c.execute('SELECT * FROM scheduler_services WHERE service_id=?',(job['service_id'],)).fetchone()
            category=(data.get('fault') or {}).get('category')
            due=now
            if outcome=='complete':state='complete'
            elif outcome=='blocked':state='blocked'
            elif outcome in ('deferred','stopped'):
                state='retry';due=now+max(1,(data.get('fault') or {}).get('retry_after',0))
            elif category in ('transient','configuration'):
                observation=c.execute('SELECT event_id FROM scheduler_observations WHERE attempt_id=? AND event_id IS NOT NULL LIMIT 1',(attempt_id,)).fetchone()
                event=observation['event_id'] if observation else service['event_id']
                c.execute('INSERT OR IGNORE INTO scheduler_rounds VALUES(?,?,?,?,?)',(attempt_id,job['job_id'],job['service_id'],now,event))
                budget=self.budget_sql(c,job['job_id'],now)
                state=('blocked_service' if event else 'held_budget' if budget['rounds']>=max_rounds or budget['seconds']>=max_seconds else 'retry')
                due=max(service['next_probe'],now+backoff(max(1,budget['rounds']),retry_after=data['fault'].get('retry_after',0)))
            else:state='held'
            c.execute('UPDATE scheduler_jobs SET state=?,due=?,max_rounds=?,max_seconds=? WHERE job_id=?',
                (state,due,max_rounds,max_seconds,job['job_id']))
            c.execute("UPDATE scheduler_attempts SET state='finished',finished=?,outcome=?,data=? WHERE attempt_id=?",
                (now,outcome,canonical(data),attempt_id))
            if state in ('complete','held','blocked'):
                c.execute('INSERT OR IGNORE INTO scheduler_recoveries VALUES(?,?,?,?)',(attempt_id,job['unit_key'],job['service_id'],now))
            return dict(c.execute('SELECT * FROM scheduler_jobs WHERE job_id=?',(job['job_id'],)).fetchone())
        return self._write(write)
