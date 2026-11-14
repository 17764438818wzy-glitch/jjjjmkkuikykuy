"""Cleanup facts are independent of delivery success. Transactions never perform I/O."""
import hashlib
import json
import math
import time
from pathlib import Path

from .cleanup_paths import ASSET_SCHEMA,inventory,overlaps,check_path_reference
from .delivery_store import DeliveryLedger,target_key
from .handoff_store import canonical,protects_remote,legacy_write_guard
from .repair_store import RepairLedger
from .scheduling_policy import ActionDeferred

SCHEMA=(ASSET_SCHEMA,
    '''CREATE TABLE IF NOT EXISTS cleanup_runs (
        thread_id TEXT PRIMARY KEY,task_id TEXT NOT NULL,disposition TEXT NOT NULL,closed_at REAL NOT NULL,reason TEXT NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS cleanup_items (
        item_id TEXT PRIMARY KEY,kind TEXT NOT NULL,category TEXT NOT NULL,task_id TEXT NOT NULL,
        source_id TEXT NOT NULL,manifest_sha256 TEXT,specialty_code TEXT,resource TEXT NOT NULL,
        eligible_at REAL NOT NULL,state TEXT NOT NULL,created REAL NOT NULL,updated REAL NOT NULL,
        error TEXT NOT NULL DEFAULT '',receipt TEXT NOT NULL DEFAULT '{}')''',
)


class CleanupLedger(DeliveryLedger):
    def __init__(self,store,config):
        super().__init__(store);RepairLedger(store)
        self.config=config
        self.scan_cursors={}
        self._write(lambda c:[c.execute(sql) for sql in SCHEMA])

    def rows(self,query,args=()):
        with self.store.db(timeout=.1) as c:return [dict(r) for r in c.execute(query,args)]

    def items(self):return self.rows('SELECT * FROM cleanup_items ORDER BY created,item_id')

    def item(self,item_id):
        rows=self.rows('SELECT * FROM cleanup_items WHERE item_id=?',(item_id,))
        if not rows:raise ValueError('unknown cleanup item')
        return rows[0]

    @staticmethod
    def add_sql(c,kind,category,identity,resource,created,eligible_at):
        item_id=hashlib.sha256(canonical([kind,identity,resource]).encode()).hexdigest()
        if kind=='local':
            previous=c.execute("SELECT * FROM cleanup_items WHERE kind='local' AND json_extract(resource,'$.path')=?",(resource['path'],)).fetchone()
            if previous:
                if previous['item_id']!=item_id:raise ValueError('existing cleanup inventory changed')
                return dict(previous)
        c.execute('INSERT OR IGNORE INTO cleanup_items(item_id,kind,category,task_id,source_id,manifest_sha256,'
            'specialty_code,resource,eligible_at,state,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
            (item_id,kind,category,identity['task_id'],identity['source_id'],identity.get('manifest_sha256'),
             identity.get('specialty_code'),canonical(resource),eligible_at,'waiting',created,created))
        return dict(c.execute('SELECT * FROM cleanup_items WHERE item_id=?',(item_id,)).fetchone())

    def discover_remote(self):
        retention=self.config.old_version_retention_seconds
        rows=self.scan_page('remote','SELECT rowid AS scan_row,* FROM specialty_retirements')
        def write(c):
            for row in rows:
                value=json.loads(row['data']);identity=value['identity'];code=identity['specialty_code']
                replacement=value.get('replacement')
                # Only a committed replacement receives the immediate policy. Age
                # or a changed config cannot manufacture acceptance for legacy rows.
                delay=0
                if row['category']=='delayed' and replacement is None:
                    if not math.isfinite(retention) or retention<7*86400:
                        raise ValueError('legacy old version retention must be at least seven days')
                    delay=retention
                for role,obj in value['receipts'].get(code,{}).items():
                    if role not in ('body','images') or not obj:continue
                    resource=dict(identity=identity,role=role,object=obj)
                    if replacement is not None:resource['replacement']=replacement
                    self.add_sql(c,'remote',row['category'],identity,resource,row['retired_at'],row['retired_at']+delay)
        self._write(write)

    def scan_page(self,key,query):
        cursor=self.scan_cursors.get(key,0)
        rows=self.rows('SELECT * FROM ('+query+') WHERE scan_row>? ORDER BY scan_row LIMIT 64',(cursor,))
        if not rows and cursor:rows=self.rows('SELECT * FROM ('+query+') ORDER BY scan_row LIMIT 64')
        self.scan_cursors[key]=rows[-1]['scan_row'] if rows else 0
        return rows

    @staticmethod
    def trees_exited(c,task_id):
        # Every known graph tree, including repairs, must have positive exit evidence.
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name='control_executions'").fetchone():return True
        for row in c.execute("SELECT state,exit_evidence FROM control_executions WHERE unit_kind='graph' AND unit_key=?",(task_id,)):
            if row['state']!='exited' or not json.loads(row['exit_evidence'] or '{}').get('all_recorded_processes_exited'):return False
        return True

    def close_run(self,thread_id,task_id,disposition,*,reason=''):
        if disposition not in ('complete','abandoned','superseded'):raise ValueError('explicit closure required')
        if disposition!='complete' and not reason:raise ValueError('abandonment reason required')
        def write(c):
            if not self.trees_exited(c,task_id):raise ValueError('run tree exit not confirmed')
            intents=c.execute('SELECT * FROM publication_intents WHERE thread_id=? AND task_id=?',(thread_id,task_id)).fetchall()
            if not intents and not c.execute('SELECT 1 FROM workflow_artifacts WHERE thread_id=? AND task_id=?',(thread_id,task_id)).fetchone():
                raise ValueError('unknown workflow run')
            if disposition=='complete' and (not intents or any(r['lifecycle']!='handed_off' for r in intents)):
                raise ValueError('run handoff incomplete')
            if c.execute("SELECT 1 FROM repair_requests WHERE repair_request_id=? AND state!='complete'",(thread_id,)).fetchone():
                raise ValueError('repair input remains recoverable')
            prior=c.execute('SELECT * FROM cleanup_runs WHERE thread_id=?',(thread_id,)).fetchone()
            if prior and (prior['task_id']!=task_id or prior['disposition']!=disposition):raise ValueError('run closure conflict')
            c.execute('INSERT OR IGNORE INTO cleanup_runs VALUES(?,?,?,?,?)',(thread_id,task_id,disposition,time.time(),reason))
        self._write(write)

    @staticmethod
    def run_closed(c,thread_id,task_id):
        row=c.execute('SELECT * FROM cleanup_runs WHERE thread_id=? AND task_id=?',(thread_id,task_id)).fetchone()
        if not row or not CleanupLedger.trees_exited(c,task_id):raise ValueError('run not closed or still recoverable')
        return dict(row)

    @staticmethod
    def repair_settled(c,rid):
        row=c.execute('SELECT state FROM repair_requests WHERE repair_request_id=?',(rid,)).fetchone()
        return bool(row and row['state']=='complete'
            and not c.execute('SELECT 1 FROM repair_report_followups WHERE repair_request_id=? AND resolved_manifest IS NULL',(rid,)).fetchone()
            and not c.execute("SELECT 1 FROM repair_targets WHERE repair_request_id=? AND outcome LIKE '%_pending'",(rid,)).fetchone())

    def release_sql(self,c):
        # This method only deletes references. Exit evidence cannot change
        # during its writer transaction; do not decode the same large process
        # trees once per reference. The memo dies with this single invocation.
        exit_proofs={}
        for ref in c.execute('SELECT * FROM package_references').fetchall():
            kind,owner,digest=ref['owner_kind'],ref['owner_id'],ref['package_sha256'];release=False
            if kind=='recovery':
                thread,fingerprint=json.loads(owner)
                row=c.execute('SELECT * FROM cleanup_runs WHERE thread_id=?',(thread,)).fetchone()
                if row:
                    task_id=row['task_id']
                    if task_id not in exit_proofs:exit_proofs[task_id]=self.trees_exited(c,task_id)
                    release=bool(exit_proofs[task_id])
            elif kind=='delivery':
                _,code=json.loads(owner)
                row=c.execute('SELECT task_id FROM delivery_packages WHERE manifest_sha256=?',(digest,)).fetchone()
                target=self._expectation(c,row['task_id'],code)
                progress=self.progress_sql(c,digest,code)
                release=bool((progress and progress['state'] in ('active','withdrawn')) or (target['manifest_sha256']!=digest and not progress))
            elif kind in ('repair','repair_result','repair_target'):
                rid=json.loads(owner)[0] if kind=='repair_target' else owner
                release=self.repair_settled(c,rid)
            elif kind=='retained_specialty':
                source,code,old=json.loads(owner)
                rows=c.execute('SELECT state FROM cleanup_items WHERE kind=? AND source_id=? AND specialty_code=? AND manifest_sha256=?',
                    ('remote',source,code,old)).fetchall()
                release=bool(rows and all(r['state']=='deleted' for r in rows))
            if release:c.execute('DELETE FROM package_references WHERE owner_kind=? AND owner_id=? AND resource_kind=? AND resource_id=?',
                (kind,owner,ref['resource_kind'],ref['resource_id']))

    def package_guard(self,c,digest):
        if c.execute("SELECT 1 FROM legacy_targets WHERE manifest_sha256=? AND state NOT IN ('active','reconciled')",(digest,)).fetchone():
            raise ValueError('legacy package pending reconciliation')
        if c.execute('SELECT 1 FROM target_versions WHERE manifest_sha256=?',(digest,)).fetchone():raise ValueError('current expected package retained')
        if c.execute('SELECT 1 FROM specialty_active WHERE manifest_sha256=?',(digest,)).fetchone():raise ValueError('active package retained')
        for row in c.execute("SELECT * FROM specialty_switches WHERE phase NOT IN ('committed','withdrawn')"):
            if row['candidate']==digest or (row['previous_json'] and json.loads(row['previous_json'])['identity']['manifest_sha256']==digest):
                raise ValueError('switch reference retained')
        for ref in c.execute('SELECT * FROM package_references WHERE package_sha256=? OR (resource_kind=? AND resource_id=?)',(digest,'package',digest)):
            if ref['owner_kind']!='phase_dependency':raise ValueError('package reference retained')

    def local_guard(self,c,resource,digest=None):
        if self.config.preserve_sealed_packages:
            protected=[str(Path(self.config.handoff_root)/'sealed')]
            protected += [r['package_dir'] for r in c.execute('SELECT package_dir FROM delivery_packages')]
            if any(overlaps(resource['path'],path) for path in protected):
                raise ValueError('sealed package retained by policy')
        task_id=resource['task_id'];self.run_closed(c,resource['thread_id'],task_id)
        if not self.trees_exited(c,task_id):raise ValueError('graph still using material')
        for ref in c.execute("SELECT * FROM package_references WHERE resource_kind!='package'"):
            if overlaps(resource['path'],ref['resource_id']):raise ValueError('material reference retained')
        for row in c.execute("SELECT * FROM publication_intents WHERE lifecycle!='deleting'"):
            intent=json.loads(row['data'])
            if any(overlaps(resource['path'],intent[key]) for key in ('material_dir','package_dir')):
                self.run_closed(c,row['thread_id'],row['task_id'])
        # A live cache marker also depends on its reading view. Keep those views;
        # output artifacts are pinned by every retained package's phase_dependency.
        if resource.get('artifact_kind')=='reading':
            if c.execute("SELECT 1 FROM delivery_packages WHERE task_id=? AND lifecycle='retained'",(task_id,)).fetchone():
                raise ValueError('reading view cache dependency retained')
        for r in c.execute('SELECT * FROM repair_requests WHERE task_id=?',(task_id,)):
            if not self.repair_settled(c,r['repair_request_id']):raise ActionDeferred('repair dependency retained')
        if resource['type']=='package':self.package_guard(c,digest)
        if resource['type']=='orphan':
            intent=self._intent(c,resource['thread_id'],resource['fingerprint'])
            if intent['lifecycle']=='handed_off' or c.execute('SELECT 1 FROM delivery_packages WHERE manifest_sha256=?',(digest,)).fetchone():
                raise ValueError('orphan acquired a registration')
            closed=self.run_closed(c,resource['thread_id'],task_id)
            if closed['disposition'] not in ('abandoned','superseded'):raise ValueError('orphan still recoverable')

    def roots(self):return [Path(self.config.handoff_root)/'sealed',Path(self.config.handoff_root)/'staging',self.config.root/'work']

    def originals(self):return [r['path'] for r in self.rows('SELECT path FROM source_versions')]

    def plan_publication(self,thread_id,fingerprint):
        intent=self.intent(thread_id,fingerprint)
        with self.store.db(timeout=.1) as c:closed=self.run_closed(c,thread_id,intent['identity']['task_id'])
        if intent['lifecycle']=='handed_off':return self.plan_package(intent['manifest_sha256'])
        retention=getattr(self.config,'orphan_package_retention_seconds',None)
        if retention is None or not math.isfinite(retention) or retention<0:raise ValueError('explicit orphan retention required')
        from .handoff import PackageHandoff
        PackageHandoff(self.store,self.config.handoff_root)._published(intent)
        resource=dict(inventory(intent['package_dir'],self.roots(),self.originals()),type='orphan',thread_id=thread_id,
            task_id=intent['identity']['task_id'],fingerprint=fingerprint)
        def write(c):
            self.release_sql(c);self.local_guard(c,resource,intent['manifest_sha256'])
            # Start only when the complete orphan qualification first succeeds.
            # INSERT OR IGNORE keeps this clock stable across repeated discovery.
            candidate_at=time.time()
            return self.add_sql(c,'local','delayed',dict(intent['identity'],manifest_sha256=intent['manifest_sha256']),resource,
                candidate_at,candidate_at+retention)
        return self._write(write)

    def plan_package(self,digest):
        row=self.registration(digest)
        if not row or row['lifecycle']!='retained':raise ValueError('retained package required')
        intents=self.rows('SELECT * FROM publication_intents WHERE manifest_sha256=?',(digest,))
        if not intents:raise ValueError('package publication missing')
        intent=json.loads(intents[0]['data'])
        resource=dict(inventory(row['package_dir'],self.roots(),self.originals()),type='package',thread_id=intent['thread_id'],
            task_id=row['task_id'],fingerprint=intent['input_fingerprint'])
        def write(c):
            self.release_sql(c);self.local_guard(c,resource,digest)
            return self.add_sql(c,'local','delayed',dict(json.loads(row['identity_json']),manifest_sha256=digest),resource,time.time(),0)
        return self._write(write)

    def plan_artifact(self,path):
        rows=self.rows('SELECT * FROM workflow_artifacts WHERE path=?',(str(Path(path).resolve()),))
        if not rows:raise ValueError('unregistered workflow artifact')
        row=rows[0];task=self.store.get(row['task_id'])
        # This read can only reject. Retained cache inputs otherwise repeatedly
        # take BEGIN IMMEDIATE, scan/release every reference, then roll it all
        # back when local_guard rejects them. Keep the authoritative checks in
        # the write transaction below, including references added after this read.
        with self.store.db(timeout=.1) as c:
            if row['kind']=='reading' and c.execute(
                    "SELECT 1 FROM delivery_packages WHERE task_id=? AND lifecycle='retained'",(row['task_id'],)).fetchone():
                raise ValueError('reading view cache dependency retained')
            for ref in c.execute("SELECT resource_id FROM package_references WHERE owner_kind='phase_dependency' AND resource_kind!='package'"):
                if overlaps(path,ref['resource_id']):raise ValueError('material reference retained')
        resource=dict(inventory(path,self.roots(),self.originals()),type='artifact',thread_id=row['thread_id'],task_id=row['task_id'],artifact_kind=row['kind'])
        def write(c):
            self.release_sql(c);self.local_guard(c,resource)
            return self.add_sql(c,'local','closeout',task,resource,time.time(),0)
        return self._write(write)

    def remote_guard(self,c,row):
        digest,code,source=row['manifest_sha256'],row['specialty_code'],row['source_id']
        legacy_write_guard(c,source,code)
        # Unknown effects are a reconciliation barrier, never ordinary dependency
        # backoff, even when a temporary repair reference exists at the same time.
        if row['state']=='pending_reconcile':raise ValueError('unresolved remote write protected')
        if c.execute("SELECT 1 FROM sqlite_master WHERE name='control_actions'").fetchone():
            scope='unit:'+canonical(['target',canonical([source,code])])
            for action in c.execute("SELECT snapshot FROM control_actions WHERE kind IN ('rag_write','cleanup_write') AND state='unresolved'"):
                if scope in json.loads(action['snapshot'])['scopes']:raise ValueError('unresolved remote write protected')
        if c.execute('SELECT 1 FROM specialty_active WHERE source_id=? AND specialty_code=? AND manifest_sha256=?',(source,code,digest)).fetchone():
            raise ValueError('active document protected')
        switching=c.execute('SELECT * FROM specialty_switches WHERE source_id=? AND specialty_code=?',(source,code)).fetchone()
        if switching and switching['phase'] not in ('committed','withdrawn'):raise ActionDeferred('specialty switch protected')
        resource=json.loads(row['resource'])
        if row['category']=='delayed' and 'replacement' not in resource:
            raise ValueError('historical replacement acceptance requires verification')
        if 'replacement' in resource:self.replacement_guard(c,row,resource['replacement'])
        # Recoveries cannot need a document after it is marked deleting.
        for ref in c.execute("SELECT * FROM package_references WHERE (package_sha256=? OR (resource_kind='package' AND resource_id=?)) AND owner_kind IN ('repair','repair_target','safety_specialty')",(digest,digest)):
            if protects_remote(ref['owner_kind'],ref['owner_id'],code):raise ActionDeferred('repair or safety reference retained')

    def replacement_guard(self,c,row,proof):
        """Durable switch evidence plus current usability, rechecked at DELETE admission."""
        identity=proof['identity'];code=row['specialty_code'];source=row['source_id']
        if (identity['source_id']!=source or identity['specialty_code']!=code
                or identity['manifest_sha256']==row['manifest_sha256'] or not proof.get('switch_id')):
            raise ValueError('replacement identity mismatch')
        progress=self.progress_sql(c,identity['manifest_sha256'],code)
        if (not progress or progress['identity']!=identity
                or hashlib.sha256(canonical(progress['receipts']).encode()).hexdigest()!=proof.get('receipts_sha256')):
            raise ValueError('replacement acceptance receipt changed')
        target=progress['receipts'].get(code,{})
        if target.get('verified') is not True or target.get('retrieval_verified') is not True:
            raise ValueError('replacement full readback, image association and retrieval required')
        active=c.execute('SELECT data FROM specialty_active WHERE source_id=? AND specialty_code=?',(source,code)).fetchone()
        if not active:raise ActionDeferred('usable replacement required')
        active=json.loads(active['data']);current=active['identity']
        try:self.check_identity(c,current)
        except ValueError as exc:raise ActionDeferred('current replacement update pending') from exc
        if self.frozen_sql(c,current) or c.execute(
                "SELECT 1 FROM specialty_safety WHERE source_id=? AND specialty_code=? AND state='pending'",(source,code)).fetchone():
            raise ActionDeferred('usable replacement frozen or pending safety')
        current_progress=self.progress_sql(c,current['manifest_sha256'],code)
        current_target=active['receipts'].get(code,{})
        if (not current_progress or current_progress['state']!='active' or current_progress['receipts']!=active['receipts']
                or current_target.get('verified') is not True or current_target.get('retrieval_verified') is not True):
            raise ValueError('current replacement acceptance unconfirmed')

    def begin_sql(self,c,item_id,*,now=None):
        now=time.time() if now is None else now
        row=dict(c.execute('SELECT * FROM cleanup_items WHERE item_id=?',(item_id,)).fetchone())
        if row['state']=='deleted':return row
        if now<row['eligible_at']:raise ValueError('retention not expired')
        self.release_sql(c)
        if row['kind']=='remote':self.remote_guard(c,row)
        else:
            resource=json.loads(row['resource']);self.local_guard(c,resource,row['manifest_sha256'])
            if resource['type'] in ('orphan','package'):
                c.execute("UPDATE publication_intents SET lifecycle='deleting' WHERE thread_id=? AND input_fingerprint=?",(resource['thread_id'],resource['fingerprint']))
            if resource['type']=='package':
                c.execute("UPDATE delivery_packages SET lifecycle='deleting' WHERE manifest_sha256=?",(row['manifest_sha256'],))
                c.execute("DELETE FROM package_references WHERE package_sha256=? AND owner_kind='phase_dependency'",(row['manifest_sha256'],))
        c.execute("UPDATE cleanup_items SET state='deleting',updated=? WHERE item_id=?",(now,item_id))
        return dict(c.execute('SELECT * FROM cleanup_items WHERE item_id=?',(item_id,)).fetchone())

    @staticmethod
    def result_sql(c,item_id,outcome,evidence):
        row=c.execute('SELECT * FROM cleanup_items WHERE item_id=?',(item_id,)).fetchone()
        if not row:raise ValueError('unknown cleanup result')
        if row['state']=='deleted':return dict(row)
        if outcome=='deleted':
            if row['state'] not in ('deleting','pending_reconcile') or evidence.get('absent') is not True:raise ValueError('deletion readback required')
            state='deleted'
        elif outcome=='unknown':state='pending_reconcile'
        elif outcome=='error':state=row['state']
        else:raise ValueError('invalid cleanup outcome')
        c.execute('UPDATE cleanup_items SET state=?,updated=?,error=?,receipt=? WHERE item_id=?',
            (state,time.time(),str(evidence.get('error',''))[:400],canonical(evidence),item_id))
        return dict(c.execute('SELECT * FROM cleanup_items WHERE item_id=?',(item_id,)).fetchone())

    def status(self,task_id):
        rows=self.rows("SELECT category,state,error FROM cleanup_items WHERE task_id=? AND state!='deleted'",(task_id,))
        return dict(closeout_pending=sum(r['category']=='closeout' for r in rows),delayed=sum(r['category']=='delayed' for r in rows),
            pending_reconcile=sum(r['state']=='pending_reconcile' for r in rows),errors=sum(bool(r['error']) for r in rows))

    def discover_local(self):
        # Only explicit workflow provenance, never recursive source-folder discovery.
        runs=self.scan_page('runs','''SELECT MIN(p.rowid) AS scan_row,p.thread_id,p.task_id FROM publication_intents p
            JOIN tasks t ON t.task_id=p.task_id WHERE p.lifecycle='handed_off'
            AND (t.stage='handed_off' OR EXISTS(SELECT 1 FROM repair_requests r WHERE r.repair_request_id=p.thread_id AND r.state='complete'))
            AND NOT EXISTS(SELECT 1 FROM cleanup_runs r WHERE r.thread_id=p.thread_id) GROUP BY p.thread_id,p.task_id''')
        for row in runs:
            try:self.close_run(row['thread_id'],row['task_id'],'complete')
            except ValueError:pass
        artifacts=self.scan_page('artifacts','''SELECT a.rowid AS scan_row,a.path FROM workflow_artifacts a JOIN cleanup_runs r ON r.thread_id=a.thread_id
            WHERE NOT EXISTS(SELECT 1 FROM cleanup_items i WHERE i.kind='local' AND json_extract(i.resource,'$.path')=a.path)''')
        for row in artifacts:
            if not Path(row['path']).exists():continue
            try:self.plan_artifact(row['path'])
            except ValueError:pass  # Referenced or unverified materials remain retained.
        packages=[] if self.config.preserve_sealed_packages else self.scan_page('packages','''SELECT p.rowid AS scan_row,p.manifest_sha256 FROM delivery_packages p WHERE p.lifecycle='retained'
            AND NOT EXISTS(SELECT 1 FROM target_versions v WHERE v.manifest_sha256=p.manifest_sha256)
            AND NOT EXISTS(SELECT 1 FROM cleanup_items i WHERE i.kind='local' AND i.manifest_sha256=p.manifest_sha256)''')
        for row in packages:
            try:self.plan_package(row['manifest_sha256'])
            except (ValueError,FileNotFoundError):pass
        if not self.config.preserve_sealed_packages and self.config.orphan_package_retention_seconds is not None:
            orphans=self.scan_page('orphans','''SELECT p.rowid AS scan_row,p.thread_id,p.input_fingerprint FROM publication_intents p
                JOIN cleanup_runs r ON r.thread_id=p.thread_id WHERE p.lifecycle='open' AND r.disposition IN ('abandoned','superseded')
                AND p.manifest_sha256 IS NOT NULL AND NOT EXISTS(SELECT 1 FROM cleanup_items i WHERE i.kind='local'
                    AND json_extract(i.resource,'$.thread_id')=p.thread_id AND json_extract(i.resource,'$.fingerprint')=p.input_fingerprint)''')
            for row in orphans:
                try:self.plan_publication(row['thread_id'],row['input_fingerprint'])
                except (ValueError,FileNotFoundError):pass
