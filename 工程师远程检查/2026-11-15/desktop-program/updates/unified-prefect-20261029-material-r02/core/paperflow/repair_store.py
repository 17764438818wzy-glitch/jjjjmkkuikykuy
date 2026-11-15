"""Repair facts and the two shared correction slots; SQL never authorizes execution."""
import hashlib
import json
import re
import time

from .handoff_store import HandoffLedger,canonical
from .phase_cache import CACHE_STAGES
from .producer import MAX_DELIVERY_CORRECTIONS,SyntaxRepairExhausted
from .cleanup_paths import defer_local_cleanup

SCHEMA=(
    '''CREATE TABLE IF NOT EXISTS repair_report_followups (
        repair_request_id TEXT NOT NULL,specialty_code TEXT NOT NULL,evidence_hash TEXT NOT NULL,
        evidence TEXT NOT NULL,followup_request_id TEXT,resolved_manifest TEXT,
        PRIMARY KEY(repair_request_id,specialty_code,evidence_hash))''',
    '''CREATE TABLE IF NOT EXISTS defective_cache (
        task_id TEXT NOT NULL,stage TEXT NOT NULL,fingerprint TEXT NOT NULL,
        repair_request_id TEXT NOT NULL,PRIMARY KEY(task_id,stage,fingerprint))''',
    '''CREATE TABLE IF NOT EXISTS repair_targets (
        repair_request_id TEXT NOT NULL,specialty_code TEXT NOT NULL,
        desired_manifest TEXT,generation INTEGER NOT NULL,outcome TEXT NOT NULL,
        PRIMARY KEY(repair_request_id,specialty_code))''',
    '''CREATE TABLE IF NOT EXISTS correction_budgets (
        source_id TEXT NOT NULL,source_sha256 TEXT NOT NULL,used INTEGER NOT NULL,
        PRIMARY KEY(source_id,source_sha256))''',
    '''CREATE TABLE IF NOT EXISTS correction_attempts (
        source_id TEXT NOT NULL,source_sha256 TEXT NOT NULL,attempt_id TEXT NOT NULL,
        kind TEXT NOT NULL,details TEXT NOT NULL,ordinal INTEGER NOT NULL,created REAL NOT NULL,
        PRIMARY KEY(source_id,source_sha256,attempt_id))''',
    '''CREATE TABLE IF NOT EXISTS repair_requests (
        repair_request_id TEXT PRIMARY KEY,task_id TEXT NOT NULL,source_id TEXT NOT NULL,source_sha256 TEXT NOT NULL,
        source_manifest TEXT NOT NULL REFERENCES delivery_packages(manifest_sha256),
        stage TEXT NOT NULL,problem_type TEXT NOT NULL,state TEXT NOT NULL,
        run_data TEXT,result TEXT,created REAL NOT NULL,
        UNIQUE(source_id,source_sha256,source_manifest,stage,problem_type))''',
    '''CREATE UNIQUE INDEX IF NOT EXISTS one_shared_repair ON repair_requests(source_id,source_sha256)
        WHERE state='running' ''',
    '''CREATE TABLE IF NOT EXISTS repair_reports (
        repair_request_id TEXT NOT NULL REFERENCES repair_requests(repair_request_id),
        specialty_code TEXT NOT NULL,evidence TEXT NOT NULL,
        PRIMARY KEY(repair_request_id,specialty_code))''',
)


def normalized_problem(stage,problem):
    key=re.sub(r'[\s-]+','_',str(problem).strip().casefold())
    key={'missing_chunks':'chunks_missing','chunk_missing':'chunks_missing',
         'missing_assets':'assets_missing','missing_body':'body_missing'}.get(key,key)
    allowed={'chunks_missing':('round3_chunk',),'body_loss':('round3_chunk',),
             'assets_missing':('round3_assets',),'privacy':('round3_redact',),
             'body_missing':('round1','round2'),'classification':('round2',),
             'json_syntax':CACHE_STAGES}
    allowed['embedding_length']=('round3_chunk','round3_assets')
    if stage not in allowed.get(key,()):raise ValueError('explicit content defect and business stage required')
    return key


class RepairLedger(HandoffLedger):
    def __init__(self,store,*,initialize=True):
        self.store=store
        if initialize:
            super().__init__(store);self._write(lambda c:[c.execute(sql) for sql in SCHEMA])

    def rows(self,sql,args=()):
        with self.store.db(timeout=.1) as c:return [dict(r) for r in c.execute(sql,args)]

    @staticmethod
    def source_key(identity):return identity['source_id'],identity['source_sha256']

    @staticmethod
    def request_row(c,request_id):
        row=c.execute('SELECT * FROM repair_requests WHERE repair_request_id=?',(request_id,)).fetchone()
        if not row:raise ValueError('unknown repair request')
        return dict(row)

    def get(self,request_id):
        with self.store.db(timeout=.1) as c:return self.request_row(c,request_id)

    def seed_sql(self,c,identity,legacy_count=0):
        if type(legacy_count) is not int or legacy_count<0:raise ValueError('invalid legacy count')
        key=self.source_key(identity);used=legacy_count
        for row in c.execute('SELECT data FROM tasks WHERE source_sha256=?',(identity['source_sha256'],)):
            task=json.loads(row['data'])
            if task.get('source_id')==identity['source_id']:
                for name in ('correction_count','repair_count'):
                    count=task.get(name,0)
                    if type(count) is not int or count<0:raise ValueError('invalid task correction mirror')
                    used=max(used,count)
        c.execute('INSERT INTO correction_budgets VALUES(?,?,?) ON CONFLICT(source_id,source_sha256) '
                  'DO UPDATE SET used=MAX(used,excluded.used)',(*key,used))
        return c.execute('SELECT used FROM correction_budgets WHERE source_id=? AND source_sha256=?',key).fetchone()['used']

    def used(self,identity):
        rows=self.rows('SELECT used FROM correction_budgets WHERE source_id=? AND source_sha256=?',self.source_key(identity))
        return rows[0]['used'] if rows else 0

    def reserve_sql(self,c,identity,attempt_id,kind,details,*,legacy_count=0):
        if not attempt_id or kind not in ('syntax','material'):raise ValueError('stable correction attempt required')
        key=self.source_key(identity);used=self.seed_sql(c,identity,legacy_count)
        old=c.execute('SELECT * FROM correction_attempts WHERE source_id=? AND source_sha256=? AND attempt_id=?',(*key,attempt_id)).fetchone()
        if old:
            if old['kind']!=kind or old['details']!=canonical(details):raise ValueError('correction attempt identity conflict')
            return used
        if used>=MAX_DELIVERY_CORRECTIONS:raise SyntaxRepairExhausted('two cumulative corrections exhausted')
        c.execute('INSERT INTO correction_attempts VALUES(?,?,?,?,?,?,?)',(*key,attempt_id,kind,canonical(details),used+1,time.time()))
        c.execute('UPDATE correction_budgets SET used=used+1 WHERE source_id=? AND source_sha256=?',key)
        return used+1

    def request_sql(self,c,identity,stage,problem,reporter,evidence,*,legacy_count=0):
        problem=normalized_problem(stage,problem)
        if not isinstance(evidence,dict) or not evidence or len(canonical(evidence))>16000:raise ValueError('bounded structured defect evidence required')
        digest=identity['manifest_sha256']
        package=c.execute('SELECT * FROM delivery_packages WHERE manifest_sha256=?',(digest,)).fetchone()
        if not package or package['lifecycle']!='retained':raise ValueError('retained source package required')
        origin=json.loads(package['identity_json'])
        if any(origin[k]!=identity[k] for k in ('task_id','source_id','source_sha256')):raise ValueError('repair source identity mismatch')
        if not c.execute('SELECT 1 FROM sources WHERE source_id=? AND task_id=?',(identity['source_id'],identity['task_id'])).fetchone():
            raise ValueError('source superseded')
        if not c.execute('SELECT 1 FROM specialty_tasks WHERE manifest_sha256=? AND specialty_code=?',(digest,reporter)).fetchone():
            raise ValueError('reporter outside source specialties')
        key=(*self.source_key(identity),digest,stage,problem)
        request_id='repair_'+hashlib.sha256(canonical(key).encode()).hexdigest()
        row=c.execute('SELECT * FROM repair_requests WHERE repair_request_id=?',(request_id,)).fetchone()
        if not row:
            # Same transaction as registration/budget/reference: whichever of
            # repair responsibility and local deletion commits first goes first.
            defer_local_cleanup(c,identity['task_id'])
            used=self.seed_sql(c,identity,legacy_count)
            state='held_budget' if used>=MAX_DELIVERY_CORRECTIONS else 'queued'
            c.execute('INSERT INTO repair_requests VALUES(?,?,?,?,?,?,?,?,NULL,NULL,?)',
                (request_id,identity['task_id'],*self.source_key(identity),digest,stage,problem,state,time.time()))
            if state=='queued':self.reserve_sql(c,identity,'material:'+request_id,'material',dict(repair_request_id=request_id,stage=stage),legacy_count=legacy_count)
            self._reference(c,digest,'repair',request_id,'package',digest)
        saved=c.execute('SELECT evidence FROM repair_reports WHERE repair_request_id=? AND specialty_code=?',(request_id,reporter)).fetchone()
        items=json.loads(saved['evidence']) if saved else []
        if isinstance(items,dict):items=[items]  # Previously recorded single-evidence rows.
        if evidence not in items:items.append(evidence)
        c.execute('INSERT INTO repair_reports VALUES(?,?,?) ON CONFLICT(repair_request_id,specialty_code) '
            'DO UPDATE SET evidence=excluded.evidence',(request_id,reporter,canonical(items)))
        affected=CACHE_STAGES[CACHE_STAGES.index(stage):]
        for row in c.execute('SELECT * FROM package_stage_dependencies WHERE manifest_sha256=?',(digest,)):
            dep=json.loads(row['dependency'])
            if row['stage'] in affected and dep.get('cache_key'):
                c.execute('INSERT OR IGNORE INTO defective_cache VALUES(?,?,?,?)',
                    (identity['task_id'],dep['cache_stage'],dep['cache_key'],request_id))
        result=self.request_row(c,request_id)
        followup=None
        if result['state']=='complete':followup=self.forward_late_sql(c,result,reporter,evidence)
        return dict(repair_request_id=request_id,state=result['state'],**(dict(followup_request_id=followup) if followup else {}))

    @staticmethod
    def reports_sql(c,request_id):
        reports=[]
        for row in c.execute('SELECT * FROM repair_reports WHERE repair_request_id=? ORDER BY specialty_code',(request_id,)):
            items=json.loads(row['evidence'])
            for evidence in ([items] if isinstance(items,dict) else items):
                reports.append(dict(specialty_code=row['specialty_code'],evidence=evidence))
        return reports

    def forward_late_sql(self,c,request,reporter,evidence):
        """Record new requirements separately; never change the running model's input."""
        run=json.loads(request['run_data']);rid=request['repair_request_id']
        covered=run['repair_problem']['requirements']
        if evidence in covered:return None
        evidence_hash=hashlib.sha256(canonical(evidence).encode()).hexdigest()
        saved=c.execute('SELECT * FROM repair_report_followups WHERE repair_request_id=? AND specialty_code=? AND evidence_hash=?',
            (rid,reporter,evidence_hash)).fetchone()
        if saved:
            if saved['followup_request_id']:
                self.settle_followups_sql(c,self.request_row(c,saved['followup_request_id']))
            return saved['followup_request_id']
        # Even a missing result specialty (child=None) creates a new unresolved
        # responsibility. Covered evidence and existing links above are replays.
        defer_local_cleanup(c,request['task_id'])
        digest=json.loads(request['result'])['manifest_sha256'];child=None
        if c.execute('SELECT 1 FROM specialty_tasks WHERE manifest_sha256=? AND specialty_code=?',(digest,reporter)).fetchone():
            identity=dict(task_id=request['task_id'],source_id=request['source_id'],source_sha256=request['source_sha256'],manifest_sha256=digest)
            child=self.request_sql(c,identity,request['stage'],request['problem_type'],reporter,evidence)['repair_request_id']
        c.execute('INSERT INTO repair_report_followups VALUES(?,?,?,?,?,NULL)',(rid,reporter,evidence_hash,canonical(evidence),child))
        c.execute("UPDATE repair_targets SET outcome='reports_pending' WHERE repair_request_id=? AND outcome IN ('advanced','followup_registered')",(rid,))
        if child:self.settle_followups_sql(c,self.request_row(c,child))
        return child

    @staticmethod
    def settle_followups_sql(c,request):
        # A completed run covers only its immutable requirements, not reports
        # received while it was running or after it had already handed off.
        if request['state']!='complete':return
        covered=json.loads(request['run_data'])['repair_problem']['requirements']
        digest=json.loads(request['result'])['manifest_sha256'];rid=request['repair_request_id']
        for link in c.execute('SELECT * FROM repair_report_followups WHERE followup_request_id=? AND resolved_manifest IS NULL',(rid,)).fetchall():
            if json.loads(link['evidence']) in covered:
                c.execute('UPDATE repair_report_followups SET resolved_manifest=? '
                    'WHERE repair_request_id=? AND specialty_code=? AND evidence_hash=?',
                    (digest,link['repair_request_id'],link['specialty_code'],link['evidence_hash']))
        c.execute("UPDATE repair_targets SET outcome='followup_registered' WHERE outcome='reports_pending' "
            'AND repair_request_id IN (SELECT repair_request_id FROM repair_report_followups WHERE followup_request_id=?) '
            'AND NOT EXISTS (SELECT 1 FROM repair_report_followups f WHERE f.repair_request_id=repair_targets.repair_request_id AND f.resolved_manifest IS NULL)',(rid,))

    def start_sql(self,c,request_id):
        request=self.request_row(c,request_id)
        if request['state'] not in ('queued','running','complete'):raise ValueError('repair held; no automatic new run')
        defer_local_cleanup(c,request['task_id'])
        if request['run_data']:return json.loads(request['run_data'])
        if c.execute("SELECT 1 FROM repair_requests WHERE source_id=? AND source_sha256=? AND state='running'",self.source_key(request)).fetchone():
            raise ValueError('another shared repair is running')
        row=c.execute('SELECT * FROM tasks WHERE task_id=?',(request['task_id'],)).fetchone()
        task=self.store.decode(row)
        if not task or not c.execute('SELECT 1 FROM sources WHERE source_id=? AND task_id=?',(request['source_id'],request['task_id'])).fetchone():
            raise ValueError('source superseded')
        source=c.execute('SELECT * FROM delivery_packages WHERE manifest_sha256=?',(request['source_manifest'],)).fetchone()
        if source['lifecycle']!='retained':raise ValueError('source package deleting')
        from .revisions import next_revision_sql
        revision=next_revision_sql(c,request['task_id'])
        codes=[r['code'] for r in json.loads(source['targets_json'])['targets']]
        # Source-bound conditions: never adopt a later decision merely because it is current at launch.
        expected={code:dict(manifest_sha256=request['source_manifest'],generation=0) for code in codes}
        for code in codes:
            current=self._expectation(c,request['task_id'],code)
            expected[code]=current if current['manifest_sha256']==request['source_manifest'] else dict(manifest_sha256=request['source_manifest'],generation=1)
        run={k:task[k] for k in ('task_id','source_id','source_sha256','source_path','contract_version')}
        run.update(thread_id=request_id,repair_request_id=request_id,stage='repair',failed_stage=request['stage'],
            revision=revision,repair_source=dict(repair_request_id=request_id,manifest_sha256=request['source_manifest']),
            repair_problem=dict(stage=request['stage'],problem_type=request['problem_type']),
            draft_dir=source['package_dir'],package_dir=source['package_dir'],stage_outputs={},
            expected_targets=expected,repair_catalog=dict(majors=[t['mapping'] for t in json.loads(source['targets_json'])['targets']]),
            mapping_version=source['mapping_version'])
        dependencies={r['stage']:json.loads(r['dependency']) for r in c.execute(
            'SELECT * FROM package_stage_dependencies WHERE manifest_sha256=?',(request['source_manifest'],))}
        run['repair_dependencies']=dependencies
        run['stage_dependencies']={stage:dep for stage,dep in dependencies.items()
            if CACHE_STAGES.index(stage)<CACHE_STAGES.index(request['stage'])}
        run['repair_reports']=self.reports_sql(c,request_id)
        run['repair_problem']['requirements']=[json.loads(value) for value in sorted({canonical(r['evidence']) for r in run['repair_reports']})]
        c.execute("UPDATE repair_requests SET state='running',run_data=? WHERE repair_request_id=?",(canonical(run),request_id))
        return run

    def complete_sql(self,c,intent,result):
        source=intent['repair_source'];rid=source['repair_request_id']
        request=RepairLedger.request_row(c,rid)
        if request['source_manifest']!=source['manifest_sha256'] or request['task_id']!=intent['identity']['task_id']:
            raise ValueError('repair handoff identity conflict')
        if request['state']=='complete':
            if json.loads(request['result'])['manifest_sha256']!=result['manifest_sha256']:raise ValueError('repair already has another result')
            self.settle_followups_sql(c,request)
            return
        if request['state']!='running' or not request['run_data']:raise ValueError('registered running repair required')
        run=json.loads(request['run_data'])
        if intent['revision']!=run['revision']:raise ValueError('repair revision conflict')
        source_codes={r['specialty_code'] for r in c.execute('SELECT specialty_code FROM specialty_tasks WHERE manifest_sha256=?',(request['source_manifest'],))}
        new_codes={r['code'] for r in intent['target_snapshot']['targets']}
        if request['stage']!='round2' and new_codes!=source_codes:raise ValueError('ordinary repair changed specialty set')
        # The same transaction reads all current references and decides each responsibility.
        observed={r['specialty_code']:dict(manifest_sha256=r['manifest_sha256'],generation=r['generation'])
            for r in c.execute('SELECT * FROM target_versions WHERE task_id=?',(request['task_id'],))}
        outcomes={r['code']:r['expectation_outcome'] for r in result['targets']}
        def dependency(manifest):
            row=c.execute('SELECT dependency FROM package_stage_dependencies WHERE manifest_sha256=? AND stage=?',
                (manifest,request['stage'])).fetchone()
            return json.loads(row['dependency']) if row else {}
        bad=dependency(request['source_manifest']).get('token')
        for code in source_codes|new_codes|set(observed):
            current=observed.get(code,dict(manifest_sha256=None,generation=0))
            desired=current['manifest_sha256']
            dep=dependency(desired)
            if outcomes.get(code)=='advanced':outcome='advanced'
            elif outcomes.get(code)=='frozen':outcome='frozen_pending'
            elif desired==request['source_manifest']:outcome='classification_pending' if code not in new_codes else 'repair_pending'
            elif (rid in dep.get('resolved_requests',[]) or
                  (bad and dep.get('lineage_complete') is True and dep.get('token')!=bad
                   and dep.get('lineage') and bad not in dep['lineage'])):outcome='superseded_independent'
            else:outcome='dependency_pending'
            c.execute('INSERT INTO repair_targets VALUES(?,?,?,?,?)',(rid,code,desired,current['generation'],outcome))
            if outcome.endswith('_pending') and desired:
                HandoffLedger._reference(c,desired,'repair_target',canonical([rid,code]),'package',desired)
        c.execute("UPDATE repair_requests SET state='complete',result=? WHERE repair_request_id=?",(canonical(result),rid))
        HandoffLedger._reference(c,result['manifest_sha256'],'repair_result',rid,'package',result['manifest_sha256'])
        completed=self.request_row(c,rid)
        for report in self.reports_sql(c,rid):self.forward_late_sql(c,completed,report['specialty_code'],report['evidence'])
        self.settle_followups_sql(c,completed)
