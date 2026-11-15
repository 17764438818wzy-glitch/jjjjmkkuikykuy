"""Explicit local structure remapping while the controller remains paused.

No scheduler, LLM, remote writes, pause changes or content-repair budget calls.
Reservation and final registration share the existing SQLite transaction and
handoff boundaries. This API is deliberately not an automatic startup action.
"""
import hashlib
import json
import copy
from datetime import datetime, timezone
from pathlib import Path

from .handoff import PackageHandoff
from .handoff_store import canonical
from .repair_store import RepairLedger
from .package import inside, load, save, seal_package, validate_package
from .structure_plan import clone_new_revision, build_plan, PLAN_FILE
from .revisions import next_revision_sql


SCHEMA='''CREATE TABLE IF NOT EXISTS structure_migrations (
    migration_id TEXT PRIMARY KEY,task_id TEXT NOT NULL,source_manifest TEXT NOT NULL,
    data TEXT NOT NULL,state TEXT NOT NULL,fingerprint TEXT,result TEXT)'''


class StructureMigration:
    def __init__(self,store,handoff_root,work_root,*,admission,counter_endpoint=None):
        from .embedding_admission import ConfiguredAdmission
        if isinstance(admission,ConfiguredAdmission) and counter_endpoint is None:
            raise ValueError('configured migration requires maintenance counter endpoint')
        if counter_endpoint is not None and counter_endpoint.policy!=admission.policy:
            raise ValueError('maintenance counter policy mismatch')
        self.counter_endpoint=counter_endpoint
        self.ledger=RepairLedger(store)
        self.ledger._write(lambda c:c.execute(SCHEMA))
        self.handoff=PackageHandoff(store,handoff_root,admission=admission)
        self.root=Path(work_root).resolve()

    def get(self,migration_id):
        rows=self.ledger.rows('SELECT * FROM structure_migrations WHERE migration_id=?',(migration_id,))
        if not rows:raise ValueError('unknown migration')
        row=rows[0]
        return dict(json.loads(row['data']),state=row['state'],fingerprint=row['fingerprint'],
                    result=json.loads(row['result']) if row['result'] else None)

    @staticmethod
    def _paused(c,expected=None):
        row=c.execute('SELECT * FROM controller_state WHERE id=1').fetchone()
        if (not row or row['paused']!=1 or
                c.execute("SELECT 1 FROM control_executions WHERE state!='exited'").fetchone()):
            raise ValueError('paused maintenance requires all local execution trees exited')
        stamp=dict(epoch=row['epoch'],generation=row['generation'])
        if expected is not None and stamp!=expected:raise ValueError('paused maintenance generation changed')
        return stamp

    @staticmethod
    def _dependencies(c,manifest):
        return {r['stage']:json.loads(r['dependency']) for r in c.execute(
            'SELECT stage,dependency FROM package_stage_dependencies WHERE manifest_sha256=?',(manifest,))}

    @staticmethod
    def _source(c,run):
        row=c.execute('SELECT * FROM delivery_packages WHERE manifest_sha256=?',(run['source_manifest'],)).fetchone()
        if not row or row['lifecycle']!='retained':raise ValueError('retained migration source required')
        origin=json.loads(row['identity_json'])
        if not c.execute('SELECT 1 FROM sources WHERE source_id=? AND task_id=?',
                         (origin['source_id'],origin['task_id'])).fetchone():raise ValueError('source superseded')
        if ('stage_dependencies' in run and
                StructureMigration._dependencies(c,run['source_manifest'])!=run['stage_dependencies']):
            raise ValueError('migration source lineage changed')
        for code,expected in run['expected_targets'].items():
            if RepairLedger._expectation(c,origin['task_id'],code)!=expected:raise ValueError('migration target changed')
            from .handoff_store import legacy_write_guard
            legacy_write_guard(c,origin['source_id'],code)
            if c.execute("SELECT 1 FROM package_freezes WHERE manifest_sha256=? AND specialty_code IN (?, '*')",
                         (run['source_manifest'],code)).fetchone():raise ValueError('migration package frozen')
            scope='unit:'+canonical(['target',canonical([origin['source_id'],code])])
            state=c.execute('SELECT state FROM admission_scopes WHERE scope=?',(scope,)).fetchone()
            if state:
                state=json.loads(state['state'])
                if state.get('frozen') or not state.get('enabled',True):raise ValueError('migration target frozen or closed')
            for action in c.execute("SELECT snapshot FROM control_actions WHERE state='unresolved' AND kind IN ('rag_write','cleanup_write')"):
                if scope in json.loads(action['snapshot'])['scopes']:raise ValueError('migration unresolved target barrier')
        return dict(row)

    def reserve(self,source_manifest,*,body_scope=None):
        policy=self.handoff.admission.policy.fingerprint
        binding=[source_manifest,policy]
        if body_scope is not None:
            from .embedding_admission import digest
            body_scope=copy.deepcopy(body_scope)
            if (body_scope.get('schema')!='paperflow-body-split-scope/1'
                    or body_scope.get('source_manifest_sha256')!=source_manifest
                    or body_scope.get('scope_sha256')!=digest({k:v for k,v in body_scope.items() if k!='scope_sha256'})):
                raise ValueError('body scope does not match source')
            binding.append(body_scope['scope_sha256'])
        migration_id=('body_' if body_scope else 'structure_')+hashlib.sha256(canonical(binding).encode()).hexdigest()
        def write(c):
            old=c.execute('SELECT data FROM structure_migrations WHERE migration_id=?',(migration_id,)).fetchone()
            if old:return json.loads(old['data'])
            stamp=self._paused(c)
            row=c.execute('SELECT * FROM delivery_packages WHERE manifest_sha256=?',(source_manifest,)).fetchone()
            if not row or row['lifecycle']!='retained':raise ValueError('retained migration source required')
            origin=json.loads(row['identity_json']);targets=json.loads(row['targets_json'])['targets']
            expected={t['code']:self.ledger._expectation(c,origin['task_id'],t['code']) for t in targets}
            if not expected or any(v['manifest_sha256']!=source_manifest for v in expected.values()):
                raise ValueError('migration source is not current target')
            run=dict(migration_id=migration_id,task_id=origin['task_id'],identity=origin,
                     source_manifest=source_manifest,source_root=row['package_dir'],policy_id=policy,
                     expected_targets=expected,control=stamp,revision=next_revision_sql(c,origin['task_id']),
                     created_at=datetime.now(timezone.utc).isoformat(),mapping_version=row['mapping_version'],
                     stage_dependencies=self._dependencies(c,source_manifest),
                     processing_history=json.loads(row['manifest_bytes']).get('processing_history',[]),
                     catalog=dict(majors=[t['mapping'] for t in targets]))
            if body_scope is not None:run['body_scope']=body_scope
            self._source(c,run)
            from .cleanup_paths import defer_local_cleanup
            defer_local_cleanup(c,origin['task_id'])
            c.execute('INSERT INTO structure_migrations VALUES(?,?,?,?,\'reserved\',NULL,NULL)',
                      (migration_id,origin['task_id'],source_manifest,canonical(run)))
            self.ledger._reference(c,source_manifest,'structure_migration',migration_id,'package',source_manifest)
            return run
        return self.ledger._write(write)

    def _guard(self,c,run):
        row=c.execute('SELECT state,data FROM structure_migrations WHERE migration_id=?',(run['migration_id'],)).fetchone()
        if not row or row['state']!='reserved' or json.loads(row['data'])['control']!=run['control']:
            raise ValueError('migration reservation changed')
        self._paused(c,run['control']);self._source(c,run)
        if self.handoff.admission.policy.fingerprint!=run['policy_id']:raise ValueError('migration policy changed')

    def rebind_paused(self,migration_id):
        """Explicit recovery after Manager restart; preserve the reserved revision."""
        def write(c):
            row=c.execute('SELECT * FROM structure_migrations WHERE migration_id=?',(migration_id,)).fetchone()
            if not row or row['state']!='reserved':raise ValueError('reserved migration required')
            run=json.loads(row['data']);stamp=self._paused(c)
            self._source(c,run)
            if self.handoff.admission.policy.fingerprint!=run['policy_id']:raise ValueError('migration policy changed')
            if stamp!=run['control']:
                run.setdefault('prior_controls',[]).append(run['control']);run['control']=stamp
                c.execute('UPDATE structure_migrations SET data=? WHERE migration_id=?',(canonical(run),migration_id))
            return run
        return self.ledger._write(write)

    def cancel(self,migration_id):
        """Explicit local cancellation; keep files and revision history for audit."""
        def write(c):
            self._paused(c)
            row=c.execute('SELECT * FROM structure_migrations WHERE migration_id=?',(migration_id,)).fetchone()
            if not row or row['state'] not in ('reserved','cancelled'):raise ValueError('uncompleted migration required')
            c.execute("UPDATE structure_migrations SET state='cancelled' WHERE migration_id=?",(migration_id,))
            c.execute("DELETE FROM package_references WHERE owner_kind='structure_migration' AND owner_id=?",(migration_id,))
        self.ledger._write(write)

    def run(self,migration_id):
        from .instance_lock import InstanceLock
        run=self.get(migration_id)
        if run['state']=='complete':return run['result']  # Historical receipt, never a fresh permit.
        if run['state']!='reserved':raise ValueError('reserved migration required')
        source=Path(run['source_root']).resolve();output=(self.root/migration_id).resolve()
        if output.is_relative_to(source) or source.is_relative_to(output) or self.root.is_relative_to(source):
            raise ValueError('private migration directory required')
        # Reuse the existing OS-owned lock; process death releases it. Do not
        # materialize the same immutable revision concurrently in two callers.
        with InstanceLock(self.root/'.locks'/migration_id):
            if self.counter_endpoint is None:return self._run_locked(migration_id,self.handoff)
            def valid_scope():
                with self.ledger.store.db(timeout=.1) as c:self._guard(c,run)
            with self.counter_endpoint.maintenance_counter(run,valid_scope) as counter:
                # This invocation owns the maintenance adapter. Do not lend it to
                # a shared manager admission or another migration/handoff caller.
                admission=copy.copy(self.handoff.admission)
                admission.counter=counter;admission._memo=None
                handoff=copy.copy(self.handoff);handoff.admission=admission
                return self._run_locked(migration_id,handoff)

    def _run_locked(self,migration_id,handoff):
        run=self.get(migration_id)
        if run['state']=='complete':return run['result']
        with self.ledger.store.db(timeout=.1) as c:self._guard(c,run)
        origin=validate_package(run['source_root'],run['identity'],run['catalog'],strict_seal=True)
        if origin['manifest_sha256']!=run['source_manifest']:raise ValueError('migration source manifest changed')
        clone=clone_new_revision(origin,run['revision'],expected_targets=run['expected_targets'])
        task=dict(clone['task'],stage_dependencies=run['stage_dependencies'],processing_history=run['processing_history'])
        if run.get('body_scope'):
            task['processing_history']=[*task['processing_history'],dict(
                operation='deterministic_body_line_split',source_manifest=run['source_manifest'],
                body_scope_sha256=run['body_scope']['scope_sha256'],llm_actions=0)]
        output=self.root/migration_id
        if output.resolve()==Path(run['source_root']).resolve() or output.is_symlink():raise ValueError('private migration directory required')
        if run['fingerprint'] is None:
            output.mkdir(parents=True,exist_ok=True)
            for entry in clone['copy_files']:
                data=inside(Path(clone['source_root']),entry['path']).read_bytes()
                if hashlib.sha256(data).hexdigest()!=entry['sha256']:raise ValueError('migration source bytes changed')
                target=inside(output,entry['path']);target.parent.mkdir(parents=True,exist_ok=True)
                # Only this reservation owns the unpublished chunks draft. An
                # interrupted body transformation restarts from immutable input.
                if run.get('body_scope') and entry['path']=='chunks.json':target.write_bytes(data)
                if target.exists() and target.read_bytes()!=data:raise ValueError('migration draft changed')
                if not target.exists():target.write_bytes(data)
            if run.get('body_scope'):(output/PLAN_FILE).unlink(missing_ok=True)
            def packet():
                seal_package(output,task,run['revision'],created_at=run['created_at'])
                return validate_package(output,task,run['catalog'],strict_seal=True)
            current=packet();admission=handoff.admission
            from .embedding_admission import AdmissionRejected
            body_evidence=None
            for attempt in range(6 if run.get('body_scope') else 3):
                if run.get('body_scope'):
                    from .body_split import build_body_chunks
                    with (output/'article.redacted.md').open(encoding='utf-8-sig',newline='') as stream:
                        article=stream.read()
                    transformed=build_body_chunks(task,origin,current,article,run['body_scope'],
                        policy=admission.policy,counter=admission.counter,
                        previous_parts=body_evidence['parts'] if body_evidence else None)
                    body_evidence=transformed['evidence']
                    obj=load(output/'chunks.json');obj['chunks']=transformed['chunks'];save(output/'chunks.json',obj)
                    (output/PLAN_FILE).unlink(missing_ok=True)
                    current=packet()
                save(output/PLAN_FILE,build_plan(task,current,admission.policy,admission.counter))
                current=packet()
                try:handoff.check_embedding(task,current);break
                except AdmissionRejected as exc:
                    bad=[r for r in exc.evidence['records'] if r['issues']]
                    allowed=('tree','profile','body') if run.get('body_scope') else ('tree','profile')
                    if attempt==(5 if run.get('body_scope') else 2) or not bad or any(r['record_type'] not in allowed for r in bad):raise
            # All source projections remain exact; only split-plan/identity seal is new.
            for key in (('profile','tree','assets') if run.get('body_scope') else ('profile','tree','chunks','assets')):
                if current[key]!=origin[key]:raise ValueError('migration changed source projection')
            if body_evidence:
                body_evidence=dict(body_evidence,final_manifest_sha256=current['manifest_sha256'],
                    final_admission_verified=True,body_scope_sha256=run['body_scope']['scope_sha256'])
                save(self.root/'evidence'/(migration_id+'.json'),body_evidence)
                from .phase_cache import content_token
                prior=run['stage_dependencies'].get('round3_chunk',{})
                token=content_token(output,'round3_chunk')
                ancestry=set(prior.get('lineage',[]))|{token}
                if prior.get('token'):ancestry.add(prior['token'])
                task['stage_dependencies']=dict(run['stage_dependencies'],round3_chunk=dict(
                    token=token,lineage=sorted(ancestry),lineage_complete=prior.get('lineage_complete',False),
                    resolved_requests=prior.get('resolved_requests',[]),directory=str(output),
                    transformation='deterministic_body_line_split',source_manifest=run['source_manifest'],
                    body_scope_sha256=run['body_scope']['scope_sha256']))
            with self.ledger.store.db(timeout=.1) as c:self._guard(c,run)
            intent=handoff.prepare(migration_id,task,output,run['catalog'],
                mapping_version=run['mapping_version'],expected_targets=run['expected_targets'])
            fingerprint=intent['input_fingerprint']
            def remember(c):
                self._guard(c,run)
                old=c.execute('SELECT fingerprint FROM structure_migrations WHERE migration_id=?',(migration_id,)).fetchone()['fingerprint']
                if old is not None and old!=fingerprint:raise ValueError('migration publication conflict')
                c.execute('UPDATE structure_migrations SET fingerprint=? WHERE migration_id=?',(fingerprint,migration_id))
            self.ledger._write(remember)
        else:fingerprint=run['fingerprint']
        handoff.publish(migration_id,fingerprint)
        _,validated,manifest_bytes,registered=handoff.verify_registration(migration_id,fingerprint)
        def register(c):
            saved=c.execute('SELECT result FROM structure_migrations WHERE migration_id=?',(migration_id,)).fetchone()['result']
            if saved:return json.loads(saved)
            self._guard(c,run)
            result=self.ledger.register_sql(c,migration_id,fingerprint,validated,manifest_bytes,
                verified_registration=registered,maintenance_migration=migration_id)
            c.execute("UPDATE structure_migrations SET state='complete',result=? WHERE migration_id=?",(canonical(result),migration_id))
            c.execute("DELETE FROM package_references WHERE owner_kind='structure_migration' AND owner_id=?",(migration_id,))
            return result
        return self.ledger._write(register)
