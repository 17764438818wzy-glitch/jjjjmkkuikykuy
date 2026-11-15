"""Prepared workflow hooks for the original Scheduler and SchedulingLedger."""
from dataclasses import replace
import json
from pathlib import Path
import sys
import time

from .prepared_protocol import fingerprint


class PreparedAdapter:
    def __init__(self,registry,scope_id,generation,*,data_root,settings_path):
        self.registry,self.scope_id,self.generation=registry,scope_id,generation
        self.data_root,self.settings_path=Path(data_root).resolve(),Path(settings_path).resolve()
        self.cursor=0
        self._observed={}

    def attach(self,scheduler):
        if scheduler.manager.store.path!=self.registry.store.path:raise ValueError('prepared and scheduler stores differ')
        self.scheduler=scheduler;scheduler.prepared=self;scheduler.manager.prepared=self
        scheduler.manager.unit_limits.update(prepared_local=scheduler.config.local_workers)
        members=self.registry.members(self.scope_id)
        scheduler.service.select_tasks([m['task_id'] for m in members],time.time()+10*365*86400)

    def members(self):return self.registry.members(self.scope_id)

    def member(self,task_id):
        return next(m for m in self.members() if m['task_id']==task_id)

    def record(self,member):
        task=self.registry.store.get(member['task_id'])
        record=dict(member['record'])
        if task.get('prepared_checkpoint'):record['prepared_checkpoint']=task['prepared_checkpoint']
        return record

    def _stamp(self,member):
        record=self.record(member);work=Path(record['prepared_checkpoint']['path']).parent
        paths=[Path(record['path']),work/'material.json',work/'private/prepared-publication.json',work/'delivery-receipt.json',
            work/'model/partition-state.json',work/'model/assembled-candidate.json',work/'cleanup-receipt.json',work/'prepared-cleanup-retained.json',
            work/'status.json',work/'validation.json',work/'private/error.json',work/'candidate-import.json',work/'prepared-scope.json',
            work/'content-recovery.json',work/'content-recovery-rejected.json']
        task=self.registry.store.get(member['task_id'])
        directories=[work/'model']
        if task.get('prepared_purpose_directory'):directories.append(Path(task['prepared_purpose_directory']))
        for directory in directories:
            if directory.is_dir():
                paths.extend(directory.rglob('request-state.json'))
                paths.extend(directory.rglob('response.txt'))
                paths.extend(directory.rglob('adopted-response.json'))
        stamp=[]
        for path in paths:
            try:
                stat=path.stat();stamp.append((str(path),stat.st_size,stat.st_mtime_ns))
            except OSError:stamp.append((str(path),None))
        if task.get('prepared_delivery_id'):
            from preextract_delivery_queue import DeliveryQueue,queue_path
            job=DeliveryQueue(queue_path(self.data_root)).get(task['prepared_delivery_id'])
            stamp.append((job['id'],job['status'],job['updated_at']))
        return stamp

    def observe(self,member):
        key=member['task_id'];stamp=self._stamp(member);cached=self._observed.get(key)
        if cached and cached[0]==stamp and time.monotonic()-cached[1]<60:return cached[2]
        result=self._observe_live(member)
        self._observed[key]=(stamp,time.monotonic(),result)
        return result

    def _observe_live(self,member):
        from preextract_scheduler_adapter import observe_prepared
        from .prepared_protocol import PreparedIdentity,PreparedObservation
        original=self.registry.observation(member['task_id'])
        if original is None:
            checkpoint=self.record(member).get('prepared_checkpoint') or {}
            original=PreparedObservation(PreparedIdentity(**member['identity']),checkpoint,'material_review','review',
                'observation_index_missing',dict(work_dir=str(Path(checkpoint['path']).parent)) if checkpoint.get('path') else {},
                (),member['context_sha256'])
        from preextract_request_control import reconcile_saved_requests
        reconcile_saved_requests(self.registry,member['task_id'])
        from preextract_scope_guard import retired_delivery
        retired=retired_delivery(self.registry,self.scope_id,member)
        if retired:
            return replace(original,phase='cleaned' if retired['status']=='complete' else 'delivery_verified',
                next_step='none' if retired['status']=='complete' else 'cleanup',blocking_reason=None)
        tombstone=self.registry.cleanup_tombstone(member['task_id'])
        if tombstone:
            from preextract_scope_guard import member_delivery_proof
            if not member_delivery_proof(self.registry,self.scope_id,member):
                return replace(original,phase='material_review',next_step='review',blocking_reason='cleanup_proof_changed')
            return replace(original,phase='cleaned' if tombstone.get('disposition')!='retained' else 'retained',next_step='none',blocking_reason=None)
        exclusion=self.registry.exclusion(self.scope_id,member['task_id'])
        if exclusion:return replace(original,phase='excluded',next_step='none',blocking_reason=None)
        from preextract_scope_guard import binding
        work=Path(self.record(member)['prepared_checkpoint']['path']).parent
        expected=dict(schema='prepared-scope-binding/1',database=str(self.registry.store.path),scope_id=self.scope_id,
            generation=self.generation,task_id=member['task_id'])
        try:bound=binding(work)
        except (OSError,ValueError,KeyError,TypeError):bound=None
        if bound!=expected:
            return replace(original,phase='material_review',next_step='review',blocking_reason='prepared_binding_conflict')
        result=observe_prepared(self.record(member),data_root=self.data_root,now=time.time())
        if result.identity!=PreparedIdentity(**member['identity']):
            # A broken observation cannot replace the frozen identity or erase its hold.
            result=replace(result,identity=PreparedIdentity(**member['identity']),context_sha256=member['context_sha256'],
                           phase='material_review',next_step='review',blocking_reason='frozen_material_identity_changed')
        if result.phase=='publication_ready':
            from preextract_scope_guard import member_delivery_proof
            task=self.registry.store.get(member['task_id'])
            if task.get('prepared_delivery_id'):
                from preextract_delivery_queue import DeliveryQueue,queue_path
                job=DeliveryQueue(queue_path(self.data_root)).get(task['prepared_delivery_id'])
                from preextract_scope_guard import closure_path
                path=closure_path(result.evidence['work_dir'],self.scope_id)
                if job['status']=='complete' and path.exists():
                    if member_delivery_proof(self.registry,self.scope_id,member):
                        result=replace(result,phase='delivery_verified',next_step='cleanup',blocking_reason=None)
                elif job['status']=='needs_review':result=replace(result,phase='material_review',next_step='reconcile',blocking_reason='delivery_requires_reconciliation')
        return result

    def permit(self,member,observation):
        if observation.next_step=='cleanup':
            from preextract_scope_guard import retired_delivery
            if retired_delivery(self.registry,self.scope_id,member):
                return dict(allowed=True,action='allowed',reason='verified_retirement_only')
        from auto_config import load_config
        from preextract_scheduler_adapter import body_permit
        return body_permit(member['record'],observation,load_config(self.settings_path).get('purpose_review',{}))

    @staticmethod
    def signature(observation):
        evidence={k:v for k,v in observation.evidence.items() if k!='observed_at'}
        return fingerprint(dict(phase=observation.phase,step=observation.next_step,evidence=evidence))

    def discover(self):
        self.registry._authority()
        snapshot=self.registry.scope_snapshot(self.scope_id)
        if snapshot['intent']!='running' or snapshot['generation']!=self.generation:return
        members=self.members()
        for offset in range(min(16,len(members))):
            member=members[(self.cursor+offset)%len(members)];task_id=member['task_id']
            if self.scheduler.ledger.rows("SELECT 1 FROM scheduler_jobs WHERE unit_key=? AND state='running'",(task_id,)):continue
            observation=self.observe(member)
            self.registry.refresh_observation(self.scope_id,observation)
            if observation.next_step in ('review','reconcile','none'):continue
            if observation.next_step=='cleanup':
                from preextract_cleanup import policy
                if not policy(self.data_root).get('enabled'):continue
                from preextract_scope_guard import retirement_admission,binding,retired_delivery
                retired=retired_delivery(self.registry,self.scope_id,member)
                marker=retired['scope_proof']['binding'] if retired else binding(observation.evidence['work_dir'])
                if marker is None or not retirement_admission(marker)():continue
            permit=self.permit(member,observation)
            if not permit.get('allowed'):
                if permit.get('reason')=='confirmed_nonbody':
                    self.registry.refresh_observation(self.scope_id,replace(observation,phase='material_review',next_step='review',
                        blocking_reason='nonbody_disposition_verification_required'))
                    continue
                if permit.get('reason')!='purpose_evidence_required':continue
                step='purpose'
            else:step=('model' if observation.next_step in ('llm','recover','recover_parts','resume_parts') else
                       'adopt' if observation.next_step in ('adopt_saved_response','adopt_parts') else observation.next_step)
            if step=='delivery':
                # Native queue registration is part of the worker, not a new uploader.
                task=self.registry.store.get(task_id)
                kind='delivery' if task.get('prepared_delivery_id') else 'graph'
                if kind=='graph':step='register'
            else:kind='graph'
            local=step in ('adopt','postprocess','register','cleanup','repair_local')
            if local:kind='prepared_local'
            data=dict(task_id=task_id,scope_id=self.scope_id,scope_generation=self.generation,
                execution_engine='paper_preextract',step=step,
                prepared_local=local)
            if local or step=='repair_model':
                # A derived candidate needs its own local pass; completed jobs
                # for its parent must remain immutable and cannot swallow it.
                data['candidate_checkpoint']=self.record(member)['prepared_checkpoint']
            if step=='adopt':data['adoption_evidence']=self.signature(observation)
            service=(self.scheduler.local_service if local else self.scheduler.service.services['rag' if kind=='delivery' else 'llm'])
            self.scheduler.ledger.enqueue(kind,task_id,task_id,data,service['service_id'])
        self.cursor=(self.cursor+min(16,len(members)))%len(members)

    def current(self,job):
        self.registry.assert_running(self.scope_id,self.generation)
        member=self.member(job['unit_key'])
        self._observed.pop(member['task_id'],None)
        observation=self.observe(member)
        if observation.next_step in ('review','reconcile'):raise ValueError('prepared evidence requires review')
        if job['payload']['step']=='model' and observation.next_step=='adopt_parts':
            from .scheduling_policy import ActionDeferred
            raise ActionDeferred('partition_response_adoption_pending')
        if job['payload']['step']!='purpose' and not self.permit(member,observation).get('allowed'):
            raise ValueError('prepared purpose permission changed')

    def verified_complete(self,job):
        data=json.loads(job['data'])
        if data['scope_id']!=self.scope_id or data['scope_generation']!=self.generation:return True
        if data.get('candidate_checkpoint') and data['candidate_checkpoint']!=self.record(self.member(job['unit_key']))['prepared_checkpoint']:
            return True
        observation=self.registry.observation(job['unit_key'])
        if not observation:return False
        if data['step']=='purpose':return self.permit(self.member(job['unit_key']),observation).get('action') in ('allowed','associated','delete')
        if observation.phase in ('cleaned','retained','excluded'):return True
        if observation.phase=='delivery_verified' and data['step']!='cleanup':return True
        if data['step']=='model':return observation.phase in ('response_complete','assembled','publication_ready')
        if data['step']=='adopt':return observation.next_step not in ('adopt_saved_response','adopt_parts')
        if data['step']=='postprocess':return observation.phase=='publication_ready'
        if data['step']=='repair_local':return observation.next_step!='repair_local'
        if data['step']=='repair_model':return observation.next_step=='postprocess'
        if data['step']=='register':return bool(self.registry.store.get(job['unit_key']).get('prepared_delivery_id'))
        return False

    def settle(self,job,outcome,data):
        payload=json.loads(job['data'])
        if payload['scope_id']!=self.scope_id or payload['scope_generation']!=self.generation:
            from preextract_request_control import reconcile_saved_requests
            reconcile_saved_requests(self.registry,job['unit_key'])
            return 'complete',dict(superseded_authorization=True)
        member=self.member(job['unit_key']);self._observed.pop(member['task_id'],None);observation=self.observe(member)
        self.registry.refresh_observation(self.scope_id,observation)
        if observation.next_step in ('review','reconcile'):
            return 'held',dict(reason=observation.blocking_reason or observation.next_step)
        if self.verified_complete(job):return 'complete',dict(verified=True)
        if payload['step']=='postprocess' and observation.next_step=='repair_local':
            return 'complete',dict(content_recovery_pending=True)
        if payload['step']=='model' and observation.next_step=='adopt_parts':
            return 'deferred',dict(reason='partition_response_adoption_pending')
        if outcome in ('deferred','stopped'):return outcome,data
        if payload['step']=='purpose':
            from preextract_scheduler_adapter import request_observation
            directory=self.registry.store.get(job['unit_key']).get('prepared_purpose_directory')
            if directory:
                phase,step,evidence=request_observation(directory)
                if phase=='failed_ended':
                    from preextract_scheduler_recovery import request_fault
                    return 'failed',dict(fault=request_fault(evidence))
                if step=='reconcile':return 'held',dict(reason='purpose_outcome_unknown')
            return 'held',dict(reason='purpose_unconfirmed')
        if observation.phase=='failed_ended' or observation.next_step=='recover_parts':
            from preextract_scheduler_recovery import request_fault
            return 'failed',dict(fault=request_fault(observation.evidence))
        return outcome,data

    def launch(self,job):
        from .prepared_worker import launch_prepared
        return launch_prepared(self.scheduler.manager,self.scheduler.config,job['unit_key'],schedule_attempt=job['attempt_id'])

    def backlog(self):
        from preextract_delivery_queue import DeliveryQueue,queue_path
        path=queue_path(self.data_root)
        if not path.exists():return dict(count=0,bytes=0)
        queue=DeliveryQueue(path);count=0;size=0;seen=set()
        for member in self.members():
            task=self.registry.store.get(member['task_id']);key=task.get('prepared_delivery_id')
            if not key or key in seen:continue
            seen.add(key);job=queue.get(key)
            if job['status']=='complete':continue
            count+=1;package=Path(job['payload']['item']['package_path'])
            if package.is_dir():size+=sum(p.stat().st_size for p in package.rglob('*') if p.is_file())
        return dict(count=count,bytes=size)

    def descriptor(self,task_id,attempt_id):
        member=self.member(task_id)
        with self.registry.store.db() as c:
            row=c.execute('SELECT j.data,j.kind FROM scheduler_jobs j JOIN scheduler_attempts a ON a.job_id=j.job_id WHERE a.attempt_id=?',(attempt_id,)).fetchone()
        if not row:raise ValueError('missing scheduling attempt')
        return dict(schema='prepared-worker/1',database=str(self.registry.store.path),data_root=str(self.data_root),
            settings_path=str(self.settings_path),scope_id=self.scope_id,generation=self.generation,
            task_id=task_id,record=self.record(member),observation=self.registry.observation(task_id).as_dict(),
            step=json.loads(row['data'])['step'],kind=row['kind'])

    def stop(self):
        gate=self.scheduler.global_control
        if gate.path.exists() or gate.marker.exists():
            import uuid
            return gate.request_stop('prepared-adapter-'+uuid.uuid4().hex)
        # Immediate in-memory invalidation precedes potentially blocked persistence.
        self.scheduler.manager.pause('prepared scope user stop')
        row=self.registry.set_intent(self.scope_id,expected_generation=self.generation,intent='stopped')
        self.generation=row['generation']
        return row

    def check_action(self,c,execution_id,kind,resource):
        execution=self.scheduler.manager.executions[execution_id]
        identity=resource.get('identity')
        task=self.registry.store.get(execution.key)
        if not task or identity!=task.get('prepared_delivery_identity'):
            raise ValueError('prepared delivery identity required')
        self.registry.assert_running(self.scope_id,self.generation,connection=c)
        if identity.get('scope_id')!=self.scope_id:raise ValueError('prepared delivery outside scope')
        if kind not in ('rag_read','rag_write','local'):raise ValueError('invalid prepared delivery action')
        return False
