"""Per-specialty progress, separate from graph stage and whole-article receipts."""
import json
import time

from .handoff_store import HandoffLedger,canonical


SCHEMA=(
    '''CREATE TABLE IF NOT EXISTS specialty_retirements (
        source_id TEXT NOT NULL,specialty_code TEXT NOT NULL,manifest_sha256 TEXT NOT NULL,
        task_id TEXT NOT NULL,category TEXT NOT NULL,data TEXT NOT NULL,retired_at REAL NOT NULL,
        PRIMARY KEY(source_id,specialty_code,manifest_sha256))''',
    '''CREATE TABLE IF NOT EXISTS specialty_progress (
        manifest_sha256 TEXT NOT NULL REFERENCES delivery_packages(manifest_sha256),
        specialty_code TEXT NOT NULL,seq INTEGER NOT NULL,state TEXT NOT NULL,
        identity_json TEXT NOT NULL,receipts_json TEXT NOT NULL,updated REAL NOT NULL,
        PRIMARY KEY(manifest_sha256,specialty_code))''',
    '''CREATE TABLE IF NOT EXISTS specialty_active (
        source_id TEXT NOT NULL,specialty_code TEXT NOT NULL,manifest_sha256 TEXT NOT NULL
        REFERENCES delivery_packages(manifest_sha256),data TEXT NOT NULL,
        PRIMARY KEY(source_id,specialty_code))''',
    '''CREATE TABLE IF NOT EXISTS specialty_switches (
        source_id TEXT NOT NULL,specialty_code TEXT NOT NULL,switch_id TEXT NOT NULL UNIQUE,
        candidate TEXT NOT NULL REFERENCES delivery_packages(manifest_sha256),
        previous_json TEXT,phase TEXT NOT NULL,updated REAL NOT NULL,
        PRIMARY KEY(source_id,specialty_code))''',
    '''CREATE TABLE IF NOT EXISTS delivery_reconciliations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,source_id TEXT NOT NULL,specialty_code TEXT NOT NULL,
        observed REAL NOT NULL,evidence_json TEXT NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS specialty_safety (
        source_id TEXT NOT NULL,specialty_code TEXT NOT NULL,
        manifest_sha256 TEXT NOT NULL REFERENCES delivery_packages(manifest_sha256),
        state TEXT NOT NULL,data TEXT NOT NULL,reason TEXT NOT NULL,last_error TEXT NOT NULL,
        updated REAL NOT NULL,PRIMARY KEY(source_id,specialty_code,manifest_sha256))''',
)


def target_key(identity):return canonical([identity['source_id'],identity['specialty_code']])


class DeliveryLedger(HandoffLedger):
    def __init__(self,store):
        super().__init__(store)
        self._write(lambda c:[c.execute(sql) for sql in SCHEMA])

    def context(self,digest,code):
        with self.store.db(timeout=.1) as c:
            c.execute('BEGIN')
            return self.context_sql(c,digest,code)

    def context_sql(self,c,digest,code):
        row=c.execute('SELECT * FROM delivery_packages WHERE manifest_sha256=?',(digest,)).fetchone()
        target=c.execute('SELECT * FROM specialty_tasks WHERE manifest_sha256=? AND specialty_code=?',(digest,code)).fetchone()
        if not row or not target or row['lifecycle']!='retained':raise ValueError('registered retained specialty required')
        identity=json.loads(row['identity_json'])
        expected=self._expectation(c,identity['task_id'],code)
        if expected['manifest_sha256']!=digest:raise ValueError('expected manifest identity changed')
        identity.update(manifest_sha256=digest,specialty_code=code,generation=expected['generation'],mapping_version=target['mapping_version'])
        return dict(identity=identity,route=json.loads(target['mapping_json']),package=dict(row),target=dict(target))

    def check_identity(self,c,identity,*,allow_frozen=False,allow_legacy=False):
        ctx=self.context_sql(c,identity['manifest_sha256'],identity['specialty_code'])
        if ctx['identity']!=identity:raise ValueError('delivery identity changed')
        if not allow_legacy and c.execute('SELECT 1 FROM legacy_targets WHERE manifest_sha256=? AND specialty_code=?',
                (identity['manifest_sha256'],identity['specialty_code'])).fetchone():
            raise ValueError('legacy target requires explicit read-only reconciliation')
        if not c.execute('SELECT 1 FROM sources WHERE source_id=? AND task_id=?',
                         (identity['source_id'],identity['task_id'])).fetchone():raise ValueError('source identity superseded')
        if ctx['target']['status'] not in ('pending','frozen') or ctx['target']['mapping_issue']:
            raise ValueError('specialty mapping or status not eligible')
        frozen=ctx['target']['status']=='frozen' or c.execute('SELECT 1 FROM package_freezes WHERE manifest_sha256=? AND specialty_code IN (?,?)',
            (identity['manifest_sha256'],identity['specialty_code'],'*')).fetchone()
        if frozen and not allow_frozen:raise ValueError('package frozen')
        return ctx

    def legacy_safety_allowed_sql(self,c,identity):
        """A verified historical actor may only finish its frozen safety plan."""
        digest,code=identity['manifest_sha256'],identity['specialty_code']
        legacy=c.execute('SELECT state FROM legacy_targets WHERE manifest_sha256=? AND specialty_code=?',
                         (digest,code)).fetchone()
        if not legacy or legacy['state'] not in ('active','reconciled'):return False
        if c.execute('SELECT 1 FROM legacy_pending_scopes WHERE source_id=? AND specialty_code=?',
                     (identity['source_id'],code)).fetchone():return False
        plan=self.safety_sql(c,identity['source_id'],code,digest)
        return bool(plan and plan['state'] in ('pending','disabled') and self.frozen_sql(c,identity))

    @staticmethod
    def report_source_sql(c,identity,source_manifest):
        """An old package is evidence provenance, never an execution identity.

        The caller must separately qualify the current actor in this same
        transaction. Historical generation/status cannot authorize any writes.
        """
        if (not isinstance(source_manifest,str) or len(source_manifest)!=64
                or any(ch not in '0123456789abcdef' for ch in source_manifest)):
            raise ValueError('invalid report source manifest')
        package=c.execute('SELECT * FROM delivery_packages WHERE manifest_sha256=?',(source_manifest,)).fetchone()
        if not package or package['lifecycle']!='retained':raise ValueError('retained report source required')
        origin=json.loads(package['identity_json'])
        if any(origin[k]!=identity[k] for k in ('task_id','source_id','source_sha256')):
            raise ValueError('report source belongs to another article')
        if not c.execute('SELECT 1 FROM specialty_tasks WHERE manifest_sha256=? AND specialty_code=?',
                         (source_manifest,identity['specialty_code'])).fetchone():
            raise ValueError('report source outside owned specialty')
        return dict(origin,manifest_sha256=source_manifest)

    def progress(self,digest,code):
        with self.store.db(timeout=.1) as c:return self.progress_sql(c,digest,code)

    @staticmethod
    def progress_sql(c,digest,code):
        row=c.execute('SELECT * FROM specialty_progress WHERE manifest_sha256=? AND specialty_code=?',(digest,code)).fetchone()
        return dict(row,identity=json.loads(row['identity_json']),receipts=json.loads(row['receipts_json'])) if row else None

    def active(self,source_id,code):
        with self.store.db(timeout=.1) as c:
            row=c.execute('SELECT data FROM specialty_active WHERE source_id=? AND specialty_code=?',(source_id,code)).fetchone()
            return json.loads(row['data']) if row else None

    def switching(self,source_id,code):
        with self.store.db(timeout=.1) as c:
            row=c.execute('SELECT * FROM specialty_switches WHERE source_id=? AND specialty_code=?',(source_id,code)).fetchone()
            return dict(row,previous=json.loads(row['previous_json']) if row['previous_json'] else None) if row else None

    @staticmethod
    def validate_receipts(ctx,receipts):
        code=ctx['identity']['specialty_code']
        if set(receipts)-{code}:raise ValueError('receipt specialty mismatch')
        for role,key in (('body','paper_dataset_id'),('images','image_dataset_id')):
            obj=receipts.get(code,{}).get(role)
            if obj and obj.get('dataset_id')!=ctx['route'].get(key):raise ValueError('receipt dataset mismatch')
        if receipts.get(code,{}).get('legacy_images'):raise ValueError('legacy import belongs to stage 8')

    def save_sql(self,c,payload):
        identity=payload['identity'];ctx=self.check_identity(c,identity,allow_frozen=payload.get('state') in ('blocked','withdrawn'))
        state=payload['state'];receipts=payload['receipts'];self.validate_receipts(ctx,receipts)
        if state not in ('preparing','verified','switching','active','blocked','withdrawn'):raise ValueError('invalid delivery progress')
        digest,code=identity['manifest_sha256'],identity['specialty_code']
        current=self.progress_sql(c,digest,code);seq=current['seq'] if current else 0
        if payload.get('expected_seq',0)!=seq:raise ValueError('delivery progress compare failed')
        c.execute('INSERT INTO specialty_progress VALUES(?,?,?,?,?,?,?) ON CONFLICT(manifest_sha256,specialty_code) '
            'DO UPDATE SET seq=excluded.seq,state=excluded.state,identity_json=excluded.identity_json,receipts_json=excluded.receipts_json,updated=excluded.updated',
            (digest,code,seq+1,state,canonical(identity),canonical(receipts),time.time()))
        return dict(seq=seq+1,state=state)

    def unresolved(self,source_id,code):
        scope='unit:'+canonical(['target',canonical([source_id,code])])
        with self.store.db(timeout=.1) as c:
            return [dict(row) for row in c.execute("SELECT * FROM control_actions WHERE state='unresolved' AND kind IN ('rag_write','cleanup_write')")
                    if scope in json.loads(row['snapshot'])['scopes']]

    def frozen(self,identity):
        with self.store.db(timeout=.1) as c:return self.frozen_sql(c,identity)

    @staticmethod
    def frozen_sql(c,identity):
        scope='unit:'+canonical(['target',target_key(identity)])
        row=c.execute('SELECT state FROM admission_scopes WHERE scope=?',(scope,)).fetchone()
        key=identity['manifest_sha256'],identity['specialty_code']
        return bool((row and json.loads(row['state']).get('frozen')) or c.execute(
            "SELECT 1 FROM package_freezes WHERE manifest_sha256=? AND specialty_code IN (?, '*')",key).fetchone()
            or c.execute("SELECT 1 FROM specialty_tasks WHERE manifest_sha256=? AND specialty_code=? AND status='frozen'",key).fetchone())

    def safety(self,source,code,digest):
        with self.store.db(timeout=.1) as c:return self.safety_sql(c,source,code,digest)

    @staticmethod
    def safety_sql(c,source,code,digest):
        row=c.execute('SELECT * FROM specialty_safety WHERE source_id=? AND specialty_code=? AND manifest_sha256=?',
                      (source,code,digest)).fetchone()
        return dict(row,data=json.loads(row['data'])) if row else None

    def pending_safety(self,source,code):
        with self.store.db(timeout=.1) as c:
            return [dict(row,data=json.loads(row['data'])) for row in c.execute(
                "SELECT * FROM specialty_safety WHERE source_id=? AND specialty_code=? AND state='pending' ORDER BY updated",
                (source,code))]

    def plan_safety_sql(self,c,active,reason):
        identity=active['identity'];source,code,digest=identity['source_id'],identity['specialty_code'],identity['manifest_sha256']
        current=self.safety_sql(c,source,code,digest)
        if not current:
            c.execute('INSERT INTO specialty_safety VALUES(?,?,?,?,?,?,?,?)',
                      (source,code,digest,'pending',canonical(active),reason,'',time.time()))
        elif current['state']=='disabled':
            # An active pointer has reappeared; old completion cannot certify this exposure.
            c.execute("UPDATE specialty_safety SET state='pending',data=?,last_error='',updated=? WHERE source_id=? AND specialty_code=? AND manifest_sha256=?",
                      (canonical(active),time.time(),source,code,digest))
        # Preserve the receipts and package until the target has a verified disposition.
        self._reference(c,digest,'safety_specialty',canonical([source,code,digest]),'package',digest)
        marked=dict(active,state='frozen_pending')
        c.execute('UPDATE specialty_active SET data=? WHERE source_id=? AND specialty_code=? AND manifest_sha256=?',
                  (canonical(marked),source,code,digest))
        c.execute("UPDATE specialty_progress SET state='frozen_pending' WHERE manifest_sha256=? AND specialty_code=?",(digest,code))
        return self.safety_sql(c,source,code,digest)

    def safety_commit_sql(self,c,payload):
        identity=payload['identity']
        self.check_identity(c,identity,allow_frozen=True,allow_legacy=self.legacy_safety_allowed_sql(c,identity))
        source,code=identity['source_id'],identity['specialty_code'];digest=payload['object_manifest'];phase=payload['phase']
        if phase=='disabled':
            from .handoff_store import legacy_write_guard
            legacy_write_guard(c,source,code)
        if phase=='intent':
            row=c.execute('SELECT data FROM specialty_active WHERE source_id=? AND specialty_code=? AND manifest_sha256=?',
                          (source,code,digest)).fetchone()
            if not row:raise ValueError('frozen active version changed')
            active=json.loads(row['data'])
            if not self.frozen_sql(c,active['identity']):raise ValueError('active version is not frozen')
            return self.plan_safety_sql(c,active,'frozen active version')
        plan=self.safety_sql(c,source,code,digest)
        if not plan or phase not in ('pending','disabled'):raise ValueError('recorded safety plan required')
        if plan['state']=='disabled':return plan
        if phase=='disabled':
            # Only the exact stopped version loses its active pointer/reference.
            c.execute('DELETE FROM specialty_active WHERE source_id=? AND specialty_code=? AND manifest_sha256=?',(source,code,digest))
            c.execute("DELETE FROM package_references WHERE owner_kind='active_specialty' AND owner_id=? AND package_sha256=?",
                      (canonical([source,code]),digest))
            self._reference(c,digest,'retained_specialty',canonical([source,code,digest]),'package',digest)
            c.execute("DELETE FROM package_references WHERE owner_kind='safety_specialty' AND owner_id=? AND package_sha256=?",
                      (canonical([source,code,digest]),digest))
            c.execute("UPDATE specialty_progress SET state='disabled' WHERE manifest_sha256=? AND specialty_code=?",(digest,code))
        c.execute('UPDATE specialty_safety SET state=?,last_error=?,updated=? WHERE source_id=? AND specialty_code=? AND manifest_sha256=?',
                  (phase,str(payload.get('error',''))[:600],time.time(),source,code,digest))
        return self.safety_sql(c,source,code,digest)

    def withdraw_sql(self,c,payload):
        identity=payload['identity'];self.check_identity(c,identity,allow_frozen=True)
        row=c.execute('SELECT * FROM specialty_switches WHERE source_id=? AND specialty_code=?',
            (identity['source_id'],identity['specialty_code'])).fetchone()
        if not row or row['switch_id']!=payload['switch_id'] or row['phase']=='committed':raise ValueError('cannot withdraw committed or changed switch')
        phase=payload.get('phase','withdrawn')
        if phase not in ('withdraw_pending','withdrawn'):raise ValueError('invalid withdrawal phase')
        c.execute('UPDATE specialty_switches SET phase=?,updated=? WHERE switch_id=?',(phase,time.time(),row['switch_id']))
        c.execute('UPDATE specialty_progress SET state=? WHERE manifest_sha256=? AND specialty_code=?',
            (phase,row['candidate'],identity['specialty_code']))
        if phase=='withdrawn':
            progress=self.progress_sql(c,row['candidate'],identity['specialty_code'])
            self.retire_sql(c,dict(identity=identity,receipts=progress['receipts']),category='closeout')
        if row['previous_json']:
            previous=json.loads(row['previous_json']);digest=previous['identity']['manifest_sha256']
            previous['state']=('frozen_pending' if self.frozen_sql(c,previous['identity'])
                               else 'active' if phase=='withdrawn' else 'restore_pending')
            c.execute('UPDATE specialty_active SET data=? WHERE source_id=? AND specialty_code=? AND manifest_sha256=?',
                      (canonical(previous),identity['source_id'],identity['specialty_code'],digest))
        return dict(state=phase)

    def reconcile_sql(self,c,payload,boundary):
        identity=payload['identity'];self.check_identity(c,identity,allow_frozen=True)
        source,code=identity['source_id'],identity['specialty_code']
        scope='unit:'+canonical(['target',target_key(identity)])
        unresolved=[dict(row) for row in c.execute("SELECT * FROM control_actions WHERE state='unresolved' AND kind IN ('rag_write','cleanup_write')")
            if scope in json.loads(row['snapshot'])['scopes']]
        evidence=payload['evidence']
        if evidence.get('complete_inventory') is not True or not isinstance(evidence.get('inventory'),list):raise ValueError('full inventory required')
        if set(evidence['action_ids'])!={a['action_id'] for a in unresolved}:raise ValueError('unresolved action set changed')
        observed=time.time()
        c.execute('INSERT INTO delivery_reconciliations(source_id,specialty_code,observed,evidence_json) VALUES(?,?,?,?)',
            (source,code,observed,canonical(evidence)))
        if evidence.get('clock_reliable') is not True:return dict(released=False,reason='clock evidence unreliable',wait_seconds=0)
        reads=c.execute('SELECT * FROM delivery_reconciliations WHERE source_id=? AND specialty_code=? ORDER BY id DESC LIMIT 2',
            (source,code)).fetchall()
        if len(reads)!=2:return dict(released=False,reason='second complete read required',wait_seconds=0)
        latest,first=reads
        earlier=json.loads(first['evidence_json'])
        comparable=lambda value:{k:v for k,v in value.items() if k!='read_window'}
        if comparable(evidence)!=comparable(earlier):return dict(released=False,reason='inventory or evidence changed',wait_seconds=0)
        first_window,last_window=earlier['read_window'],evidence['read_window']
        if (first_window['epoch']!=evidence['clock_epoch'] or last_window['epoch']!=evidence['clock_epoch']
            or not first_window['started']<=first_window['finished']<=last_window['started']<=last_window['finished']):
            return dict(released=False,reason='read window sequence invalid',wait_seconds=0)
        wait=0
        for action in unresolved:
            # HTTP success (including historical terminal=True facts) does not prove
            # downstream writes ended. This deployment has no verified terminal protocol.
            if not (boundary and boundary.get('verified_end_to_end') is True and boundary.get('evidence')
                    and boundary.get('endpoint')==evidence['endpoint'] and boundary.get('configuration')==evidence['configuration']
                    and json.loads(action['request'])['resource'].get('write_operation') in boundary.get('operations',[])):
                return dict(released=False,reason='backend execution bound unverified',wait_seconds=0)
            values=[boundary.get(k) for k in ('end_to_end_seconds','safety_margin','visibility_seconds')]
            if any(type(v) not in (int,float) or not 0<v<86400 for v in values):raise ValueError('invalid verified boundary')
            execution=c.execute('SELECT * FROM control_executions WHERE execution_id=?',(action['execution_id'],)).fetchone()
            exit_fact=json.loads(execution['exit_evidence'] or '{}')
            if execution['state']!='exited' or not exit_fact.get('all_recorded_processes_exited') or not exit_fact.get('confirmed_at'):
                return dict(released=False,reason='last possible send not bounded by confirmed tree exit',wait_seconds=0)
            # A previous epoch's wall clock cannot prove elapsed time. Restart the conservative
            # bound after this manager recovered process trees, unless this epoch owns the exit.
            last_send=(exit_fact['confirmed_monotonic'] if action['epoch']==evidence['clock_epoch']
                       and exit_fact.get('confirmed_monotonic') else evidence['epoch_started'])
            cutoff=last_send+values[0]+values[1]
            wait=max(wait,values[2],cutoff-time.monotonic())
            if first_window['started']<cutoff or last_window['started']-first_window['finished']<values[2]:
                return dict(released=False,reason='silence or visibility interval incomplete',wait_seconds=wait)
        for action in unresolved:
            c.execute("UPDATE control_actions SET state='reconciled' WHERE action_id=? AND state='unresolved'",(action['action_id'],))
        return dict(released=True,actions=[a['action_id'] for a in unresolved],wait_seconds=wait)

    def switch_sql(self,c,payload):
        identity=payload['identity'];self.check_identity(c,identity)
        key=(identity['source_id'],identity['specialty_code']);digest=identity['manifest_sha256']
        row=c.execute('SELECT * FROM specialty_switches WHERE source_id=? AND specialty_code=?',key).fetchone()
        phase=payload['phase']
        if phase=='intent':
            if row and row['phase'] not in ('committed','withdrawn'):
                if row['candidate']!=digest:raise ValueError('unfinished specialty switch requires recovery')
                return dict(switch_id=row['switch_id'],phase=row['phase'])
            active=c.execute('SELECT data FROM specialty_active WHERE source_id=? AND specialty_code=?',key).fetchone()
            progress=self.progress_sql(c,digest,identity['specialty_code'])
            if not progress or progress['state']!='verified':raise ValueError('switch requires full verified readback')
            switch_id=payload['switch_id']
            c.execute('INSERT INTO specialty_switches VALUES(?,?,?,?,?,?,?) ON CONFLICT(source_id,specialty_code) '
                'DO UPDATE SET switch_id=excluded.switch_id,candidate=excluded.candidate,previous_json=excluded.previous_json,phase=excluded.phase,updated=excluded.updated',
                (*key,switch_id,digest,active['data'] if active else None,'intent',time.time()))
        else:
            if not row or row['switch_id']!=payload['switch_id'] or row['candidate']!=digest:raise ValueError('switch identity changed')
            if phase not in ('enabled','retrieved','old_disabled','committed'):raise ValueError('invalid switch phase')
            order=('intent','enabled','retrieved','old_disabled','committed')
            if order.index(phase)>order.index(row['phase'])+1:raise ValueError('missing switch proof')
            # A replay may re-read earlier steps but cannot regress the durable journal.
            if order.index(phase)<order.index(row['phase']):return dict(switch_id=row['switch_id'],phase=row['phase'])
            if phase=='committed':
                if row['phase'] not in ('old_disabled','committed'):raise ValueError('old version status unconfirmed')
                active=c.execute('SELECT data FROM specialty_active WHERE source_id=? AND specialty_code=?',key).fetchone()
                if active and active['data']!=row['previous_json'] and json.loads(active['data'])['identity']!=identity:
                    raise ValueError('active version compare failed')
                progress=self.progress_sql(c,digest,identity['specialty_code'])
                result=dict(identity=identity,receipts=progress['receipts'],activated_at=time.time(),switch_id=row['switch_id'])
                c.execute('INSERT INTO specialty_active VALUES(?,?,?,?) ON CONFLICT(source_id,specialty_code) DO UPDATE '
                    'SET manifest_sha256=excluded.manifest_sha256,data=excluded.data',(*key,digest,canonical(result)))
                c.execute("UPDATE specialty_progress SET state='active' WHERE manifest_sha256=? AND specialty_code=?",(digest,key[1]))
                owner=canonical(list(key))
                c.execute("DELETE FROM package_references WHERE owner_kind='active_specialty' AND owner_id=?",(owner,))
                self._reference(c,digest,'active_specialty',owner,'package',digest)
                if row['previous_json']:
                    old=json.loads(row['previous_json'])['identity']['manifest_sha256']
                    self._reference(c,old,'retained_specialty',canonical([*key,old]),'package',old)
                    self.retire_sql(c,json.loads(row['previous_json']))
            c.execute('UPDATE specialty_switches SET phase=?,updated=? WHERE source_id=? AND specialty_code=?',(phase,time.time(),*key))
            switch_id=row['switch_id']
        return dict(switch_id=switch_id,phase=phase)

    @staticmethod
    def retire_sql(c,previous,*,category='delayed'):
        identity=previous['identity']
        c.execute('INSERT OR IGNORE INTO specialty_retirements VALUES(?,?,?,?,?,?,?)',
            (identity['source_id'],identity['specialty_code'],identity['manifest_sha256'],identity['task_id'],
             category,canonical(previous),time.time()))
