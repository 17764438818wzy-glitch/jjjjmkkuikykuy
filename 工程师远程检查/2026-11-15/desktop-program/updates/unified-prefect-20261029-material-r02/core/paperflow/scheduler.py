"""Explicit isolated queue consumer. Never scans source folders or resumes a manager."""
import json
from pathlib import Path
import shutil
import sqlite3
import threading
import time

import psutil

from .delivery_control import DeliveryCommits
from .delivery_store import target_key
from .delivery_worker import launch_delivery
from .graph_worker import launch_graph
from .handoff_store import canonical
from .processing_control import ProcessingCommits
from .repair_control import launch_repair
from .scheduling_policy import ActionDeferred,fault
from .service_control import ServiceControl
from .cleanup_control import CleanupCommits
from .cleanup_worker import launch_cleanup
from .attachment_worker import launch_attachment
from .instrumentation import best_effort


class TickBusy(ValueError):
    """An existing native control operation temporarily owns the tick lock."""


class Scheduler:
    def __init__(self,manager,config,*,isolation_root):
        self.isolation_root=Path(isolation_root).resolve()
        for path in (manager.root,manager.store.path,config.root,config.handoff_root):self.inside(path)
        if (min(config.max_live_units,config.llm_workers,config.rag_workers,config.local_workers,
                config.max_ingest_backlog,config.max_pending_package_bytes,config.max_infrastructure_retries,
                config.max_infrastructure_seconds)<=0 or config.short_retry_interval<0):raise ValueError('invalid scheduler limits')
        if config.article_workers is not None and (type(config.article_workers) is not int or config.article_workers<1
                or config.llm_workers!=config.article_workers
                or config.max_live_units<config.article_workers+config.rag_workers+config.local_workers):
            raise ValueError('article pool requires matching LLM slots and reserved delivery/cleanup units')
        self.manager,self.config=manager,config
        from global_task_control import GlobalTaskControl
        self.global_control=GlobalTaskControl(self.isolation_root)
        self._global_tokens={}
        self.tick_lock=threading.Lock()
        self._downstream_paused=False
        self.service=ServiceControl(manager,config);self.ledger=self.service.ledger
        if manager.processing is None:ProcessingCommits(manager,config)
        if manager.delivery is None:DeliveryCommits(manager,config)
        if manager.cleanup is None:CleanupCommits(manager,config,isolation_root=self.isolation_root)
        if manager.processing.config!=config or manager.delivery.config!=config:raise ValueError('controller configuration mismatch')
        with manager.gate_lock:
            manager.resource_limits=dict(llm=config.llm_workers,local=config.local_workers,rag=config.rag_workers)
            manager.max_live_units=config.max_live_units
            if config.article_workers is not None:
                manager.unit_limits=dict(graph=config.article_workers,delivery=config.rag_workers,cleanup=config.local_workers)
                manager.wait_resources=True
                manager.graph_slots.capacity=config.article_workers
            manager.llm_slots.capacity=config.llm_workers
            manager.peer_limit=max(manager.peer_limit,config.max_live_units+8)
            manager.scope_limit=max(manager.scope_limit,4*config.max_live_units)
        self.launchers=dict(graph=launch_graph,repair=launch_repair,delivery=launch_delivery,cleanup=launch_cleanup,attachment=launch_attachment)
        self.local_service=self.ledger.ensure_service('local',dict(purpose='cleanup'))

    def inside(self,path):
        path=Path(path).resolve()
        if not path.is_relative_to(self.isolation_root):raise ValueError('path outside explicit isolation root')
        return path

    def discover(self):
        if getattr(self,'prepared',None) is not None:return self.prepared.discover()
        if self.config.cleanup:self.discover_cleanup()
        # Bounded SQL discovery excludes already queued identities before LIMIT.
        graph=self.ledger.rows('''SELECT t.* FROM tasks t JOIN sources s ON s.task_id=t.task_id
            WHERE t.kind='new' AND t.stage NOT IN ('handed_off','attachment_ready','held','parked','superseded','complete','code_error')
            AND NOT EXISTS(SELECT 1 FROM scheduler_jobs j WHERE j.kind='graph' AND j.unit_key=t.task_id) LIMIT 256''')
        for row in graph:
            task=self.manager.store.decode(row)
            job=self.ledger.enqueue('graph',task['source_id'],task['task_id'],dict(task_id=task['task_id']),self.service.services['llm']['service_id'])
            try:self.inside(task['source_path'])
            except ValueError:self.hold(job,'source outside isolation')
        repairs=self.ledger.rows('''SELECT r.* FROM repair_requests r JOIN sources s ON s.task_id=r.task_id
            WHERE r.state IN ('queued','running') AND NOT EXISTS(SELECT 1 FROM scheduler_jobs j
            WHERE j.kind='repair' AND json_extract(j.data,'$.request_id')=r.repair_request_id) LIMIT 256''')
        for row in repairs:
            self.ledger.enqueue('repair',row['source_id'],row['task_id'],dict(request_id=row['repair_request_id']),self.service.services['llm']['service_id'])
        targets=self.ledger.rows('''SELECT v.*,s.source_id,p.package_dir FROM target_versions v
            JOIN sources s ON s.task_id=v.task_id JOIN specialty_tasks t
              ON t.manifest_sha256=v.manifest_sha256 AND t.specialty_code=v.specialty_code
            JOIN delivery_packages p ON p.manifest_sha256=v.manifest_sha256
            WHERE t.mapping_issue IS NULL AND t.status IN ('pending','frozen') AND p.lifecycle='retained'
            AND NOT EXISTS(SELECT 1 FROM legacy_targets l WHERE l.manifest_sha256=v.manifest_sha256 AND l.specialty_code=v.specialty_code)
            AND NOT EXISTS(SELECT 1 FROM legacy_pending_scopes l WHERE l.source_id=s.source_id AND l.specialty_code=v.specialty_code)
            AND (t.status='pending' OR EXISTS(SELECT 1 FROM specialty_safety f WHERE f.source_id=s.source_id
                AND f.specialty_code=v.specialty_code AND f.state='pending'))
            AND NOT EXISTS(SELECT 1 FROM specialty_active a WHERE a.source_id=s.source_id
                AND a.specialty_code=v.specialty_code AND a.manifest_sha256=v.manifest_sha256 AND t.status='pending')
            AND NOT EXISTS(SELECT 1 FROM scheduler_jobs j WHERE j.kind='delivery'
                AND json_extract(j.data,'$.manifest_sha256')=v.manifest_sha256
                AND json_extract(j.data,'$.specialty_code')=v.specialty_code
                AND json_extract(j.data,'$.generation')=v.generation) LIMIT 256''')
        for row in targets:
            data={key:row[key] for key in ('manifest_sha256','specialty_code','generation')}
            job=self.ledger.enqueue('delivery',row['source_id'],target_key(row),data,self.service.services['rag']['service_id'])
            try:self.inside(row['package_dir'])
            except ValueError:self.hold(job,'package outside isolation')
        safety=self.ledger.rows('''SELECT v.*,f.source_id,f.manifest_sha256 AS safety_manifest,p.package_dir
            FROM specialty_safety f JOIN sources s ON s.source_id=f.source_id
            JOIN target_versions v ON v.task_id=s.task_id AND v.specialty_code=f.specialty_code
            JOIN delivery_packages p ON p.manifest_sha256=v.manifest_sha256 WHERE f.state='pending'
            AND NOT EXISTS(SELECT 1 FROM legacy_pending_scopes l WHERE l.source_id=f.source_id AND l.specialty_code=f.specialty_code)
            AND NOT EXISTS(SELECT 1 FROM scheduler_jobs j WHERE j.kind='delivery'
                AND json_extract(j.data,'$.safety_manifest')=f.manifest_sha256
                AND json_extract(j.data,'$.manifest_sha256')=v.manifest_sha256
                AND json_extract(j.data,'$.specialty_code')=v.specialty_code
                AND json_extract(j.data,'$.generation')=v.generation AND j.state!='complete') LIMIT 256''')
        for row in safety:
            data={key:row[key] for key in ('manifest_sha256','specialty_code','generation','safety_manifest')}
            job=self.ledger.enqueue_safety(row['source_id'],target_key(row),data,self.service.services['rag']['service_id'])
            try:self.inside(row['package_dir'])
            except ValueError:self.hold(job,'package outside isolation')
        attachments=self.ledger.rows('''SELECT a.* FROM attachment_tasks a JOIN sources s ON s.task_id=a.task_id
            WHERE a.state IN ('pending','raw_saved','note_saved','configuration_pending')
            AND NOT EXISTS(SELECT 1 FROM scheduler_jobs j WHERE j.kind='delivery'
                AND json_extract(j.data,'$.attachment_id')=a.attachment_id) LIMIT 256''')
        for row in attachments:
            target=self.config.attachment_targets.get(row['category'],{})
            if row['state']=='configuration_pending' and not (target.get('parent_id') and target.get('dataset_id')):continue
            job=self.ledger.enqueue('delivery',row['source_id'],'attachment:'+row['attachment_id'],
                dict(attachment_id=row['attachment_id']),self.service.services['rag']['service_id'])
            try:self.inside(row['package_dir'])
            except ValueError:self.hold(job,'attachment outside isolation')

    def hold(self,job_id,reason):
        self.ledger._write(lambda c:c.execute("UPDATE scheduler_jobs SET state='held' WHERE job_id=? AND state!='running'",(job_id,)))
        self.manager.store.event(None,'queue_held',dict(job_id=job_id,reason=reason))

    def discover_cleanup(self):
        ledger=self.manager.cleanup.ledger
        ledger.discover_remote();ledger.discover_local()
        rows=ledger.rows("SELECT * FROM cleanup_items WHERE state!='deleted' AND eligible_at<=? AND NOT EXISTS "
            "(SELECT 1 FROM scheduler_jobs j WHERE j.kind='cleanup' AND json_extract(j.data,'$.item_id')=cleanup_items.item_id) LIMIT 64",(time.time(),))
        for row in rows:
            service=self.local_service if row['kind']=='local' else self.service.services['rag']
            self.ledger.enqueue('cleanup',row['source_id'],'cleanup:'+self.manager.cleanup.key(row),
                dict(item_id=row['item_id'],resource_kind=row['kind']),service['service_id'])

    def pressure(self,*,advance=False):
        current='''SELECT DISTINCT p.manifest_sha256,p.manifest_bytes,j.job_id,j.state FROM scheduler_jobs j
            JOIN delivery_packages p ON p.manifest_sha256=json_extract(j.data,'$.manifest_sha256')
            JOIN target_versions v ON v.manifest_sha256=p.manifest_sha256 AND v.specialty_code=json_extract(j.data,'$.specialty_code')
                AND v.generation=json_extract(j.data,'$.generation')
            WHERE j.kind='delivery' AND j.state NOT IN ('complete','superseded')'''
        allowed="state IN ('queued','retry','running','blocked_service')"
        counts=self.ledger.rows(f'WITH current AS ({current}) SELECT COUNT(*) AS total,COALESCE(SUM({allowed}),0) AS pending FROM current')[0]
        size=self.ledger.rows(f'''WITH current AS ({current}),packages AS
            (SELECT DISTINCT manifest_sha256,manifest_bytes FROM current WHERE {allowed})
            SELECT COALESCE(SUM(json_extract(f.value,'$.bytes')),0) AS size FROM packages p,json_each(p.manifest_bytes,'$.files') f''')[0]['size']
        retained=self.ledger.rows("SELECT COALESCE(SUM(json_extract(f.value,'$.bytes')),0) AS size "
            "FROM delivery_packages p,json_each(p.manifest_bytes,'$.files') f WHERE p.lifecycle='retained'")[0]['size']
        mapping=self.ledger.rows("SELECT COUNT(*) AS count FROM specialty_tasks t JOIN target_versions v ON "
            "v.manifest_sha256=t.manifest_sha256 AND v.specialty_code=t.specialty_code WHERE t.status='mapping_pending'")[0]['count']
        attachments=self.ledger.rows('''SELECT a.state,a.data,j.state AS queue_state FROM attachment_tasks a
            JOIN sources s ON s.task_id=a.task_id LEFT JOIN scheduler_jobs j ON j.kind='delivery'
            AND json_extract(j.data,'$.attachment_id')=a.attachment_id''')
        pending=[a for a in attachments if a['queue_state'] in ('queued','retry','running','blocked_service')]
        size+=sum(json.loads(a['data'])['bytes'] for a in pending)
        retained+=sum(json.loads(a['data'])['bytes'] for a in attachments)
        counts=dict(pending=counts['pending']+len(pending),total=counts['total']+sum(a['state']!='complete' for a in attachments))
        mapping+=sum(a['state']=='configuration_pending' for a in attachments)
        if getattr(self,'prepared',None) is not None:
            extra=self.prepared.backlog();counts['pending']+=extra['count'];counts['total']+=extra['count']
            size+=extra['bytes'];retained+=extra['bytes']
        reasons=[]
        memory=psutil.virtual_memory().percent
        # A fresh handoff folder does not exist until the first seal; measure its volume.
        locations=[]
        for path in (self.config.root,self.config.handoff_root):
            parent=Path(path)
            while not parent.exists():parent=parent.parent
            locations.append(parent)
        free=min(shutil.disk_usage(path).free for path in locations)
        if memory>=self.config.max_memory_percent:reasons.append('memory')
        if free<self.config.min_free_gib*1024**3:reasons.append('disk')
        # Only tick owns the latch. A dashboard read can observe recovered
        # values but cannot clear a previously admitted backpressure cycle.
        count_low=self.config.max_ingest_backlog*4//5
        bytes_low=self.config.max_pending_package_bytes*4//5
        high=(counts['pending']>=self.config.max_ingest_backlog
              or size>=self.config.max_pending_package_bytes)
        recovered=counts['pending']<=count_low and size<=bytes_low
        if advance:self._downstream_paused=high or (self._downstream_paused and not recovered)
        draining=self._downstream_paused and not recovered
        if counts['pending']>=self.config.max_ingest_backlog or (draining and counts['pending']>count_low):
            reasons.append('target_backlog')
        if size>=self.config.max_pending_package_bytes or (draining and size>bytes_low):
            reasons.append('package_bytes')
        return dict(reasons=reasons,pending_targets=counts['pending'],held_targets=counts['total']-counts['pending'],
                    pending_package_bytes=size,retained_package_bytes=retained,mapping_pending_targets=mapping,
                    memory_percent=memory,free_bytes=free,
                    backpressure=dict(active=bool(high or draining),resume_pending_targets=count_low,
                                      resume_package_bytes=bytes_low))

    def verified_complete(self,job):
        data=json.loads(job['data'])
        if data.get('execution_engine')=='paper_preextract':return self.prepared.verified_complete(job)
        if job['kind']=='cleanup':return self.manager.cleanup.ledger.item(data['item_id'])['state']=='deleted'
        if job['kind']=='graph':
            task=self.manager.store.get(data['task_id'])
            if task and task['stage']=='attachment_ready':
                return bool(self.ledger.rows('SELECT 1 FROM attachment_tasks WHERE task_id=?',(task['task_id'],)))
            return bool(task and task['stage']=='handed_off' and self.ledger.rows(
                'SELECT 1 FROM delivery_packages WHERE task_id=?',(data['task_id'],)))
        if job['kind']=='repair':
            return self.manager.processing.repairs.ledger.get(data['request_id'])['state']=='complete'
        if data.get('attachment_id'):
            return self.manager.delivery.attachments.ledger.item(data['attachment_id'])['state']=='complete'
        try:
            ctx=self.manager.delivery.ledger.context(data['manifest_sha256'],data['specialty_code'])
            active=self.manager.delivery.ledger.active(job['article'],data['specialty_code'])
            if data.get('safety_manifest'):
                plan=self.manager.delivery.ledger.safety(job['article'],data['specialty_code'],data['safety_manifest'])
                return bool(plan and plan['state']=='disabled' and (not active or active['identity']['manifest_sha256']!=data['safety_manifest']))
            return bool(active and active['identity']==ctx['identity'] and active.get('state','active')=='active'
                        and not self.manager.delivery.ledger.frozen(active['identity'])
                        and not self.manager.delivery.ledger.pending_safety(job['article'],data['specialty_code']))
        except ValueError:return False

    def native_result_pending(self,result,task_id):
        # A verified download proves transfer, not phase acceptance. Reuse the
        # existing qualified/prepared cache path before allowing a graph retry.
        # Provider errors remain governed by their original failure policy.
        if not isinstance(result,dict) or result.get('collection_pending') is not False:return True
        if result.get('finish_reason')=='error':return False
        from .package import load,sha
        from .phase_cache import CACHE_STAGES,input_fingerprint,read_cache,read_prepared_cache
        try:
            stage=result['cache_stage'];fingerprint=result['stage_input_sha256']
            output=Path(result['output']).resolve()
            if (stage not in (*CACHE_STAGES,'repair') or output.name!=stage
                    or not output.is_relative_to((self.config.root/'work'/task_id/'attempts').resolve())):return True
            path=output.parent/'phase-result.json'
            if path.stat().st_size>8*1024*1024:return True
            entry=load(path);inputs=entry['input_task']
            if (result.get('task_id')!=task_id or entry['stage']!=stage or entry['fingerprint']!=fingerprint
                    or Path(entry['result']['draft_dir']).resolve()!=output
                    or any(inputs[k]!=result.get(k) for k in ('task_id','source_id','source_sha256'))
                    or sha(inputs['source_path'])!=inputs['source_sha256']
                    or input_fingerprint(self.config,inputs,stage)!=fingerprint):return True
            cached=read_cache(self.config,inputs,stage,fingerprint)
            if cached is None:cached=read_prepared_cache(self.config,inputs,stage,fingerprint)
            return cached is None
        except sqlite3.OperationalError as exc:
            # Keep a temporary read lock on Scheduler.run's existing backoff
            # path; it cannot turn a recoverable phase into a permanent hold.
            if getattr(exc,'sqlite_errorcode',0)&255==sqlite3.SQLITE_BUSY:raise
            return True
        except (OSError,ValueError,KeyError,TypeError,sqlite3.Error):return True

    def reap(self):
        rows=self.ledger.rows('''SELECT a.*,j.kind,j.article,j.unit_key,j.data AS job_data,e.state AS execution_state,
            e.exit_evidence,r.outcome AS reported,r.data AS report_data FROM scheduler_attempts a
            JOIN scheduler_jobs j ON j.job_id=a.job_id LEFT JOIN control_executions e ON e.execution_id=a.execution_id
            LEFT JOIN control_execution_results r ON r.execution_id=a.execution_id WHERE a.state!='finished' LIMIT 256''')
        for attempt in rows:
            if attempt['execution_id']:
                if attempt['execution_state']!='exited' or not attempt['exit_evidence']:continue
                with self.manager.gate_lock:
                    ex=self.manager.executions.get(attempt['execution_id'])
                    if attempt['execution_id'] in self.manager.publications or (ex and ex.peer and ex.peer.graph_commit_lock.locked()):continue
            job=self.ledger.job(attempt['job_id'])
            if self.verified_complete(job):outcome,data='complete',dict(verified=True)
            elif not attempt['execution_id']:outcome,data='deferred',dict(reason='unlaunched claim recovered')
            else:
                outcome=attempt['reported'] or 'held';data=json.loads(attempt['report_data'] or '{}')
                evidence=json.loads(attempt['exit_evidence'])
                if not attempt['reported'] and (evidence.get('stop_at') is not None or evidence.get('reason')=='startup reconciliation'):
                    outcome='stopped';data=dict(reason='control stopped or reconciled old tree')
                if outcome=='complete':outcome='held';data=dict(reason='completion not verified')
                # The remote model has finished, but its files may still be
                # retained. An ordinary stopped/deferred retry would dispatch
                # that model work again. Keep the existing durable proof and
                # require reconciliation after this execution has exited.
                retained=[]
                if job['kind'] in ('graph','repair'):
                    for child in (evidence.get('remote') or {}).get('native_children',[]):
                        result=(child.get('proof') or {}).get('retained_result')
                        if result is not None and self.native_result_pending(result,job['article']):
                            retained.append(dict(actor_id=child.get('actor_id'),retained_result=result))
                if retained and (outcome not in ('held','blocked') or not attempt['reported']):
                    outcome,data='held',dict(data,reason='native_result_collection_pending',retained_results=retained)
                if retained and outcome=='held' and (data.get('fault') or {}).get('category') in ('transient','configuration'):
                    # finish() classifies transient faults before its generic
                    # held branch. Preserve the old diagnosis without allowing
                    # it to turn this retained-result hold back into a retry.
                    data=dict(data,prior_fault=data['fault'])
                    data.pop('fault')
                # Only fill in an absent report or a yielded short attempt. Never
                # replace an explicit content/controller hold or reconciliation block.
                if not retained and ((not attempt['reported'] and outcome=='held') or outcome=='deferred'):
                    transient=self.unrecovered_failure(attempt['execution_id'])
                    if transient:outcome,data='failed',dict(fault=transient)
            if json.loads(job['data']).get('execution_engine')=='paper_preextract':
                outcome,data=self.prepared.settle(job,outcome,data)
            report=json.loads(attempt['report_data'] or '{}')
            best_effort(self.manager.graph_slots.mark,attempt['execution_id'] or attempt['attempt_id'],'reap')
            metrics=best_effort(self.slot_snapshot)
            data=dict(data,_scheduler_timing=metrics or dict(metrics_complete=False),
                _worker_timing=report.get('_timing') or dict(metrics_complete=False,reason='final_worker_report_missing'))
            if attempt['execution_id']:
                data['_execution_boundaries']=self.manager.graph_slots.boundaries.get(attempt['execution_id'],
                    dict(metrics_complete=False,reason='prior_epoch_or_missing_grant'))
                data['_manager_permits']=[dict(record,metrics_complete=record.get('released_at') is not None)
                    for record in self.manager.resource_holds.records.values() if record['execution_id']==attempt['execution_id']]
            self.ledger.finish(attempt['attempt_id'],outcome,data,max_rounds=self.config.max_infrastructure_retries,
                               max_seconds=self.config.max_infrastructure_seconds)
            token=self._global_tokens.get(attempt['attempt_id'])
            self.global_control.finish(token,outcome)
            self._global_tokens.pop(attempt['attempt_id'],None)
            best_effort(self.retire_timing,attempt)
            self.service.abandon_probe(attempt['job_id'])

    def unrecovered_failure(self,execution_id):
        facts=self.ledger.rows('SELECT kind,state,request,result FROM control_actions WHERE execution_id=? '
            'ORDER BY rowid DESC',(execution_id,))
        # An ordinary successful response cannot reconcile a previous unknown write.
        if any(row['state'] in ('unresolved','unknown') for row in facts):return None
        recovered=set()
        for row in facts:
            if row['result'] is None:continue
            request=json.loads(row['request']);result=json.loads(row['result'])
            resource=request.get('resource') if isinstance(request,dict) else None
            key=(canonical([row['kind'],resource,request['expected_generation']])
                 if isinstance(resource,dict) and resource.get('operation')
                 and type(request.get('expected_generation')) is int else None)
            failure=result.get('fault') if isinstance(result,dict) else None
            if row['state']=='complete' and not failure:
                if key is not None:recovered.add(key)
            elif failure:
                if failure.get('category') in ('transient','configuration'):
                    if key is None or key not in recovered:return failure
                elif failure.get('category') not in ('deferred','stopped'):
                    # A later content/program error remains authoritative even if
                    # the process lost its final IPC report.
                    return None
        return None

    def current(self,job):
        data=job['payload']
        if data.get('execution_engine')=='paper_preextract':return self.prepared.current(job)
        if job['kind']=='cleanup':
            item=self.manager.cleanup.ledger.item(data['item_id'])
            if item['kind']=='local':self.inside(json.loads(item['resource'])['path'])
            return
        if job['kind']=='delivery':
            if data.get('attachment_id'):
                row=self.manager.delivery.attachments.ledger.item(data['attachment_id'])
                self.inside(row['package_dir'])
                if not self.ledger.rows('SELECT 1 FROM sources WHERE source_id=? AND task_id=?',(row['source_id'],row['task_id'])):
                    raise ValueError('attachment source superseded')
                return
            ctx=self.manager.delivery.ledger.context(data['manifest_sha256'],data['specialty_code'])
            if ctx['identity']['generation']!=data['generation']:raise ValueError('expected generation changed')
            self.inside(ctx['package']['package_dir'])
        else:
            task=self.manager.store.get(job['unit_key']);self.inside(task['source_path'])
            if not self.ledger.rows('SELECT 1 FROM sources WHERE source_id=? AND task_id=?',(job['article'],job['unit_key'])):
                raise ValueError('source superseded')

    def tick(self):
        if not self.tick_lock.acquire(blocking=False):raise TickBusy('scheduler tick already running')
        try:
            self.service.flush_clock();self.reap();self.ledger.rebind_services(self.service.services);self.discover();pressure=self.pressure(advance=True)
            best_effort(self.observe_slots,pressure)
            prepared=getattr(self,'prepared',None) is not None
            if (not self.global_control.snapshot()['admission_open'] or self.manager.stopped.is_set() or 'memory' in pressure['reasons']
                    or ('disk' in pressure['reasons'] and not prepared)):
                return dict(launched=0,pressure=pressure)
            self.service.open_due()
            kinds=('delivery','cleanup') if pressure['reasons'] else ('graph','repair','delivery','cleanup')
            if prepared:kinds=(*kinds,'prepared_local')
            launched=0
            for _ in range(self.manager.max_live_units):
                if self.manager.stopped.is_set() or self.manager.status()['live_units']>=self.manager.max_live_units:break
                eligible=tuple(k for k in kinds if self.manager.unit_available('graph' if k=='repair' else k))
                from global_task_control import GlobalPaused
                try:
                    with self.global_control.admission() as admission:
                        job=self.ledger.claim(kinds=eligible,task_ids=self.service.selected_tasks)
                        if not job:break
                        step=job['payload'].get('step',job['kind'])
                        stage=('model' if step in ('graph','repair','model','purpose','repair_model') else
                               'delivery' if step=='delivery' else 'cleanup' if step=='cleanup' else 'postprocess')
                        token=admission.begin(stage,batch_id=job['payload'].get('scope_id',''),paper_id=job['unit_key'],token=job['attempt_id'])
                        self._global_tokens[job['attempt_id']]=token
                        best_effort(self.manager.graph_slots.mark,job['attempt_id'],'claim')
                        self.service.refresh()
                        try:
                            if self.verified_complete(job):
                                self.ledger.finish(job['attempt_id'],'complete',dict(verified=True))
                                self.service.abandon_probe(job['job_id'])
                                admission.connection.execute('UPDATE stages SET finished_at=?,outcome=? WHERE token=?',(time.time(),'complete',token)) if token else None
                                self._global_tokens.pop(job['attempt_id'],None)
                                continue
                            self.current(job)
                            data=job['payload'];kwargs=dict(schedule_attempt=job['attempt_id'])
                            launcher=job['kind']
                            if data.get('execution_engine')=='paper_preextract':
                                execution=self.prepared.launch(job);launched+=1
                                best_effort(self.bind_timing,job,execution)
                                continue
                            if job['kind']=='delivery' and data.get('attachment_id'):launcher='attachment';args=(data['attachment_id'],)
                            elif job['kind']=='delivery':args=(data['manifest_sha256'],data['specialty_code'])
                            elif job['kind']=='cleanup':args=(data['item_id'],)
                            else:args=(data['request_id'] if job['kind']=='repair' else data['task_id'],)
                            execution=self.launchers[launcher](self.manager,self.config,*args,**kwargs);launched+=1
                            best_effort(self.bind_timing,job,execution)
                        except Exception as exc:
                            # A partially created tree retains its queue ownership until confirmed exit.
                            bound=self.ledger.rows('SELECT execution_id FROM scheduler_attempts WHERE attempt_id=?',(job['attempt_id'],))[0]
                            if not bound['execution_id']:
                                self.ledger.finish(job['attempt_id'],'deferred' if isinstance(exc,ActionDeferred) or self.manager.stopped.is_set() else 'held',
                                                   dict(fault=fault(exc),error=type(exc).__name__))
                                self.service.abandon_probe(job['job_id'])
                                if token:admission.connection.execute('UPDATE stages SET finished_at=?,outcome=?,error=? WHERE token=?',(time.time(),'failed',type(exc).__name__,token))
                                self._global_tokens.pop(job['attempt_id'],None)
                except GlobalPaused:break
            return dict(launched=launched,pressure=pressure)
        finally:self.tick_lock.release()

    def bind_timing(self,job,execution):
        if execution is not None and getattr(execution,'execution_id',None):
            ledger=self.manager.graph_slots
            claim=ledger.boundaries.pop(job['attempt_id'],{})
            ledger.boundaries.setdefault(execution.execution_id,{}).update(claim)

    def retire_timing(self,attempt):
        # Existing settlement has saved the complete facts. Retire only private
        # details after that write succeeds; no additional write or lock.
        execution_id=attempt.get('execution_id')
        self.manager.graph_slots.boundaries.pop(execution_id,None)
        self.manager.graph_slots.boundaries.pop(attempt['attempt_id'],None)
        records=self.manager.resource_holds.records
        for key in [key for key,row in records.items() if row['execution_id']==execution_id]:records.pop(key,None)

    def observe_slots(self,pressure):
        # Read-only supply before memory/backpressure gates, restricted to the
        # scheduler's original due/service/unit/budget eligibility. No claim,
        # order, service transition or admission change.
        now=time.time()
        query="""SELECT DISTINCT j.job_id,j.unit_key,j.service_id,j.max_rounds,j.max_seconds,
            json_extract(t.data,'$.source_path') AS source_path,s.state AS service_state,
            (SELECT COUNT(*) FROM scheduler_rounds r JOIN scheduler_jobs history ON history.job_id=r.job_id
             WHERE history.unit_key=j.unit_key AND r.event_id IS NULL) AS budget_round_count
            FROM scheduler_jobs j JOIN tasks t ON t.task_id=j.unit_key JOIN sources source ON source.task_id=t.task_id
            JOIN scheduler_articles a ON a.article=j.article JOIN scheduler_kinds k ON k.kind=j.kind
            JOIN scheduler_services s ON s.service_id=j.service_id
            WHERE j.kind IN ('graph','repair') AND j.state IN ('queued','retry','blocked_service') AND j.due<=?
            AND (s.state='closed' OR (s.state='half_open' AND (s.probe_job IS NULL OR s.probe_job=j.job_id)))
            AND NOT EXISTS(SELECT 1 FROM scheduler_jobs owned WHERE owned.unit_key=j.unit_key AND owned.state='running')
            AND (j.kind='repair' OR t.stage NOT IN ('handed_off','attachment_ready','complete','superseded','parked','held','code_error'))
            ORDER BY k.turn,a.turn,j.turn,j.created,j.rowid"""
        def collect(connection=None):
            rows=([dict(row) for row in connection.execute(query,(now,))] if connection is not None
                else self.ledger.rows(query,(now,)))
            eligible=[]
            for row in rows:
                budget=(self.ledger.budget_sql(connection,row['job_id'],now) if connection is not None
                    else self.ledger.budget(row['job_id'],now)) if row['budget_round_count'] else dict(rounds=0,seconds=0)
                if budget['rounds']<row['max_rounds'] and budget['seconds']<row['max_seconds']:eligible.append(row)
            return eligible
        try:
            if getattr(self.ledger,'store',None) is not None:
                with self.ledger.store.db(timeout=.1) as connection:rows=collect(connection)
            else:rows=collect()
        except Exception:
            # A failed observation cannot keep a prior sufficient-supply state
            # alive. This private event adds no business transition or write.
            self.manager.graph_slots.complete=False
            self.manager.graph_slots.observe(reasons=[*pressure['reasons'],'supply_observation_failed'],
                paused=self.manager.stopped.is_set(),supply_demand=None)
            self.manager.llm_slots.observe(paused=self.manager.stopped.is_set(),supply_demand=None)
            self.manager.actor_slots.observe(paused=self.manager.stopped.is_set(),supply_demand=None)
            return
        selected=self.service.selected_tasks
        ready=set();probes=set()
        for row in rows:
            if (selected is not None and row['unit_key'] not in selected) or not Path(row['source_path']).is_file():continue
            if row['unit_key'] in ready:continue
            if row['service_state']=='half_open':
                if row['service_id'] in probes:continue
                probes.add(row['service_id'])
            ready.add(row['unit_key'])
        active={ex.key for ex in tuple(self.manager.executions.values()) if ex.kind=='graph' and not ex.exit_saved}
        # A capacity reservation also counts while process/ledger creation is in flight.
        occupied=len(self.manager.graph_slots.active)
        demand=occupied+len(ready-active)
        reasons=list(pressure['reasons'])
        if rows and not ready:reasons.append('source_not_ready')
        self.manager.graph_slots.observe(reasons=reasons,paused=self.manager.stopped.is_set(),supply_demand=demand)
        self.manager.llm_slots.observe(paused=self.manager.stopped.is_set(),supply_demand=None)
        self.manager.actor_slots.observe(paused=self.manager.stopped.is_set(),supply_demand=None)

    def slot_snapshot(self):
        # Replay immutable event copies; no additional lock acquisition.
        return dict(article_slots=self.manager.graph_slots.snapshot(),llm_permits=self.manager.llm_slots.snapshot(),
            harness_actors=self.manager.actor_slots.snapshot(),
            persistence='existing_attempt_settlement; terminal_without_settlement_may_be_incomplete')

    def run(self,stop,*,interval=.5):
        busy_since=None
        failure=None
        try:
            while not stop.is_set():
                try:
                    self.tick()
                    busy_since=None
                except TickBusy:
                    # Completion checks use this same native lock. They must
                    # not turn a healthy scheduler into a stopped controller.
                    pass
                except sqlite3.OperationalError as exc:
                    if getattr(exc,'sqlite_errorcode',0)&255 != sqlite3.SQLITE_BUSY:raise
                    # Each tick already retries short transactions. A temporary
                    # external writer must not permanently stop unrelated work;
                    # a persistently unavailable ledger still fails closed.
                    now=time.monotonic()
                    if busy_since is None:busy_since=now
                    if now-busy_since>=30:raise
                stop.wait(interval)
        except BaseException as exc:
            failure=exc
            raise
        finally:
            self.manager.pause('scheduler stopped')
            try:self.service.flush_clock()
            except Exception as exc:
                from .remote_diagnostics import record_control_failure
                record_control_failure(self.manager.root,'scheduler_final_flush',exc,epoch=self.manager.epoch)
                if failure is None:raise
