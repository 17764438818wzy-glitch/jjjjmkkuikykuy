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


class Scheduler:
    def __init__(self,manager,config,*,isolation_root):
        self.isolation_root=Path(isolation_root).resolve()
        for path in (manager.root,manager.store.path,config.root,config.handoff_root):self.inside(path)
        if (min(config.max_live_units,config.llm_workers,config.rag_workers,config.local_workers,
                config.max_ingest_backlog,config.max_pending_package_bytes,config.max_infrastructure_retries,
                config.max_infrastructure_seconds)<=0 or config.short_retry_interval<0):raise ValueError('invalid scheduler limits')
        self.manager,self.config=manager,config
        self.tick_lock=threading.Lock()
        self.service=ServiceControl(manager,config);self.ledger=self.service.ledger
        if manager.processing is None:ProcessingCommits(manager,config)
        if manager.delivery is None:DeliveryCommits(manager,config)
        if manager.cleanup is None:CleanupCommits(manager,config,isolation_root=self.isolation_root)
        if manager.processing.config!=config or manager.delivery.config!=config:raise ValueError('controller configuration mismatch')
        with manager.gate_lock:
            manager.resource_limits=dict(llm=config.llm_workers,local=config.local_workers,rag=config.rag_workers)
            manager.max_live_units=config.max_live_units
        self.launchers=dict(graph=launch_graph,repair=launch_repair,delivery=launch_delivery,cleanup=launch_cleanup,attachment=launch_attachment)
        self.local_service=self.ledger.ensure_service('local',dict(purpose='cleanup'))

    def inside(self,path):
        path=Path(path).resolve()
        if not path.is_relative_to(self.isolation_root):raise ValueError('path outside explicit isolation root')
        return path

    def discover(self):
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

    def pressure(self):
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
        if counts['pending']>=self.config.max_ingest_backlog:reasons.append('target_backlog')
        if size>=self.config.max_pending_package_bytes:reasons.append('package_bytes')
        return dict(reasons=reasons,pending_targets=counts['pending'],held_targets=counts['total']-counts['pending'],
                    pending_package_bytes=size,retained_package_bytes=retained,mapping_pending_targets=mapping,
                    memory_percent=memory,free_bytes=free)

    def verified_complete(self,job):
        data=json.loads(job['data'])
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
                # Only fill in an absent report or a yielded short attempt. Never
                # replace an explicit content/controller hold or reconciliation block.
                if (not attempt['reported'] and outcome=='held') or outcome=='deferred':
                    transient=self.unrecovered_failure(attempt['execution_id'])
                    if transient:outcome,data='failed',dict(fault=transient)
            self.ledger.finish(attempt['attempt_id'],outcome,data,max_rounds=self.config.max_infrastructure_retries,
                               max_seconds=self.config.max_infrastructure_seconds)
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
        if not self.tick_lock.acquire(blocking=False):raise ValueError('scheduler tick already running')
        try:
            self.service.flush_clock();self.reap();self.ledger.rebind_services(self.service.services);self.discover();pressure=self.pressure()
            if self.manager.stopped.is_set() or any(reason in pressure['reasons'] for reason in ('memory','disk')):
                return dict(launched=0,pressure=pressure)
            self.service.open_due()
            kinds=('delivery','cleanup') if pressure['reasons'] else ('graph','repair','delivery','cleanup')
            launched=0
            for _ in range(self.manager.max_live_units):
                if self.manager.stopped.is_set() or self.manager.status()['live_units']>=self.manager.max_live_units:break
                job=self.ledger.claim(kinds=kinds,task_ids=self.service.selected_tasks)
                if not job:break
                self.service.refresh()
                try:
                    if self.verified_complete(job):
                        self.ledger.finish(job['attempt_id'],'complete',dict(verified=True))
                        self.service.abandon_probe(job['job_id']);continue
                    self.current(job)
                    data=job['payload'];kwargs=dict(schedule_attempt=job['attempt_id'])
                    launcher=job['kind']
                    if job['kind']=='delivery' and data.get('attachment_id'):launcher='attachment';args=(data['attachment_id'],)
                    elif job['kind']=='delivery':args=(data['manifest_sha256'],data['specialty_code'])
                    elif job['kind']=='cleanup':args=(data['item_id'],)
                    else:args=(data['request_id'] if job['kind']=='repair' else data['task_id'],)
                    self.launchers[launcher](self.manager,self.config,*args,**kwargs);launched+=1
                except Exception as exc:
                    # A partially created tree retains its queue ownership until confirmed exit.
                    bound=self.ledger.rows('SELECT execution_id FROM scheduler_attempts WHERE attempt_id=?',(job['attempt_id'],))[0]
                    if not bound['execution_id']:
                        self.ledger.finish(job['attempt_id'],'deferred' if isinstance(exc,ActionDeferred) or self.manager.stopped.is_set() else 'held',
                                           dict(fault=fault(exc),error=type(exc).__name__))
                        self.service.abandon_probe(job['job_id'])
            return dict(launched=launched,pressure=pressure)
        finally:self.tick_lock.release()

    def run(self,stop,*,interval=.5):
        busy_since=None
        try:
            while not stop.is_set():
                try:
                    self.tick()
                    busy_since=None
                except sqlite3.OperationalError as exc:
                    if getattr(exc,'sqlite_errorcode',0)&255 != sqlite3.SQLITE_BUSY:raise
                    # Each tick already retries short transactions. A temporary
                    # external writer must not permanently stop unrelated work;
                    # a persistently unavailable ledger still fails closed.
                    now=time.monotonic()
                    if busy_since is None:busy_since=now
                    if now-busy_since>=30:raise
                stop.wait(interval)
        finally:
            self.manager.pause('scheduler stopped');self.service.flush_clock()
