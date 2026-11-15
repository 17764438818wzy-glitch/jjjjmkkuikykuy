"""Desktop entry for a frozen prepared scope, using the existing core controller."""
import copy
import json
from pathlib import Path
import sqlite3
import threading
import time

from preextract_material import write_json,file_sha

LABELS={'no_model_request':'等待模型','failed_ended':'等待恢复','response_complete':'回答已保存',
    'parts_pending':'分区待续跑','synthesis_pending':'等待综合','assembled':'等待后处理',
    'publication_ready':'等待交付','delivery_verified':'交付已核验','cleaned':'已清理',
    'retained':'已完成，恢复证据保留','excluded':'已确认非正文','material_review':'材料待核对',
    'outcome_unknown':'请求结果待对账'}
LABELS['content_failed']='局部内容待恢复'


def control_path(root):return Path(root)/'private/prepared-control/control.sqlite3'


def execution_limits(config):
    limits=config.get('concurrency',{});enabled=limits.get('enabled',True)
    body=int(limits.get('papers',1)) if enabled else 1
    return dict(body=body,global_limit=int(limits.get('global_llm',body)) if enabled else 1,
        purpose=int(config.get('purpose_review',{}).get('workers',1)) if enabled else 1,
        local=int(config.get('preextract_preparation',{}).get('workers',2)) if enabled else 1,
        rag=int(config.get('preextract_delivery_workers',2)) if enabled else 1)


def persisted_scope(root):
    """Startup inventory is read-only and never recreates missing authority."""
    pointer=Path(root)/'prepared-current-scope.json'
    if not pointer.exists():return None
    saved=json.loads(pointer.read_text(encoding='utf8'));database=control_path(root)
    if not database.is_file() or not database.with_suffix('.prepared-authority.json').is_file():
        raise ValueError('持续待办控制记录缺失；保持停止，不能由历史文件重建授权')
    with sqlite3.connect(database.as_uri()+'?mode=ro',uri=True) as connection:
        connection.row_factory=sqlite3.Row
        row=connection.execute('SELECT * FROM prepared_scopes WHERE scope_id=?',(saved['scope_id'],)).fetchone()
        if not row:raise ValueError('持续待办范围记录缺失；保持停止')
        records=[json.loads(r[0]) for r in connection.execute('SELECT record FROM prepared_members WHERE scope_id=? ORDER BY rowid',(saved['scope_id'],))]
    return {**saved,**dict(row),'records':records,'stop_pending':database.with_suffix('.prepared-stop-pending.json').exists()}


def freeze_records(selected,root,config):
    from preextract_scheduler_adapter import observe_prepared
    rows=[]
    for source in selected:
        record=copy.deepcopy(source)
        if (record.get('prepared_checkpoint') or {}).get('format')!='paper-preextract-prepared/1':
            raise ValueError('持续范围只接管已保存的单请求解析材料')
        if config.get('preextract_template'):record['template_path']=config['preextract_template']
        observation=observe_prepared(record,data_root=root,now=time.time())
        if not observation.identity.material_sha256:
            raise ValueError('无法冻结材料身份：'+Path(record['path']).name)
        rows.append(dict(record=record,observation=observation.as_dict()))
    return rows


def bind_member(registry,scope_id,generation,member):
    from filelock import FileLock
    registry.assert_running(scope_id,generation)
    task=registry.store.get(member['task_id'])
    checkpoint=task.get('prepared_checkpoint') or member['record']['prepared_checkpoint']
    work=Path(checkpoint['path']).parent
    marker=dict(schema='prepared-scope-binding/1',database=str(registry.store.path),scope_id=scope_id,
        generation=generation,task_id=member['task_id'])
    from preextract_scope_guard import retired_delivery
    if retired_delivery(registry,scope_id,member):return marker
    with FileLock(str(work/'operation.lock'),timeout=0):
        path=work/'prepared-scope.json'
        if path.exists():
            prior=json.loads(path.read_text(encoding='utf8'))
            if prior.get('database')!=marker['database'] or prior.get('task_id')!=member['task_id']:
                raise ValueError('材料已有其他控制身份，待核对')
        write_json(path,marker)
    return marker


class Coordinator:
    def __init__(self,processor,registry,scope_id,callback,*,startup=False):
        self.processor,self.registry,self.scope_id,self.callback=processor,registry,scope_id,callback
        self.manager=None;self.adapter=None;self.runner_stop=threading.Event();self.failure=None;self.startup=startup

    def stop(self):
        from global_task_control import GlobalTaskControl
        gate=GlobalTaskControl(self.processor.store.root)
        if gate.path.exists() or gate.marker.exists():
            import uuid
            gate.request_stop('prepared-'+self.scope_id+'-'+uuid.uuid4().hex)
            # Leave the Manager and scope generation valid while admitted
            # stages finish their internal requests and save observations.
            return
        self.processor.stop.set();self.runner_stop.set()
        from paperflow.prepared_store import mark_stop_pending
        mark_stop_pending(self.registry.store.path,self.scope_id)
        if self.manager:self.manager.pause('prepared user stop')
        row=self.registry.scope_snapshot(self.scope_id)
        if row['intent']=='running' or row['stop_pending']:self.registry.set_intent(self.scope_id,expected_generation=row['generation'],intent='stopped')

    def projection(self,status='running'):
        snapshot=self.registry.scope_snapshot(self.scope_id);items=[]
        for member in self.registry.members(self.scope_id):
            task=self.registry.store.get(member['task_id']);observation=self.registry.observation(member['task_id'])
            record=member['record'];phase=observation.phase if observation else 'material_review';done=phase in ('delivery_verified','cleaned','retained','excluded')
            jobs=self.adapter.scheduler.ledger.rows("SELECT state,due,data FROM scheduler_jobs WHERE unit_key=? AND state!='complete' ORDER BY created DESC LIMIT 1",(member['task_id'],)) if self.adapter else []
            runtime=jobs[0]['state'] if jobs else 'ready'
            label=LABELS.get(phase,phase)
            reason=observation.blocking_reason if observation else 'observation_index_missing'
            if reason:label+='；'+reason
            if status=='stopped' and not done:label+='；用户已停止'
            elif runtime in ('retry','blocked_service'):label+='；退避等待'
            item=dict(id=member['task_id'],title=Path(record['path']).stem,source_path=record['path'],
                source_hash=member['identity']['source_sha256'],status='pass' if done else 'pending',reason=label,
                prepared_checkpoint=task.get('prepared_checkpoint') or record['prepared_checkpoint'],
                prepared_phase=phase,prepared_runtime=runtime,prepared_due=jobs[0]['due'] if jobs else 0,
                ai_result=dict(status='passed' if phase in ('publication_ready','delivery_verified','cleaned','retained') else 'pending'),
                automatic_approval=phase in ('delivery_verified','cleaned','retained'),content_format='paper_preextract')
            if task.get('prepared_delivery_id'):
                from preextract_delivery_queue import DeliveryQueue,queue_path
                job=DeliveryQueue(queue_path(self.processor.store.root)).get(task['prepared_delivery_id'])
                item.update(job['payload']['item']);item.update(job.get('projection') or {})
                item.update(id=member['task_id'],prepared_phase=phase,prepared_runtime=runtime,reason=label)
            item.update(status='pass' if done else 'pending',automatic_approval=phase in ('delivery_verified','cleaned','retained'))
            if not done:item['handoff_status']='pending'
            items.append(item)
        phases=snapshot['phases'];complete=sum(phases.get(k,0) for k in ('delivery_verified','cleaned','retained','excluded'))
        counts={}
        if self.manager:
            with self.manager.gate_lock:
                holds=list(self.manager.resource_holds.values())
                counts=dict(llm_active=sum(h['lane']=='llm' for h in holds),
                    body_active=sum(h.get('operation')=='prepared_body' for h in holds),
                    purpose_active=sum(h.get('operation')=='prepared_purpose' for h in holds),
                    llm_limit=self.manager.resource_limits.get('llm',0))
        return dict(schema_version=2,pipeline='automatic',operation='retry_ai',run_id='PREPARED_SCOPE_'+self.scope_id,
            status=status,source_count=len(items),processed_sources=complete,items=items,inputs=[m['record'] for m in self.registry.members(self.scope_id)],
            events=[],prepared_scope=snapshot,task_settings=copy.deepcopy(self.processor.config.get('task_settings',{})),
            message=f'本范围交付核验 {complete}/{len(items)}；未知结果单独保留待对账',
            progress=dict(stage='持续待办',completed=complete,total=len(items),message=f'交付核验 {complete}/{len(items)}'),
            pipeline_concurrency=counts,billing=self.processor.agent.ledger.summary(run_id='PREPARED_SCOPE_'+self.scope_id))

    def run(self):
        from paperflow.config import Config
        from paperflow.control import Manager
        from paperflow.scheduler import Scheduler
        from paperflow.prepared_scheduler import PreparedAdapter
        from preextract_scope_guard import scope_delivery_closure
        config=self.processor.config;root=self.processor.store.root;runtime=control_path(root).parent
        from global_task_control import GlobalTaskControl
        global_control=GlobalTaskControl(root)
        initial_control=global_control.snapshot()
        if not initial_control['admission_open']:
            return self.projection('unknown' if initial_control['state']=='unknown' else 'stopped')
        limits=execution_limits(config);body=limits['body'];global_limit=limits['global_limit']
        purpose=limits['purpose'];local=limits['local'];rag=limits['rag'];max_units=body+purpose+local+rag
        saved=json.loads((root/'settings.json').read_text(encoding='utf8'))
        core=Config(home=str(runtime/'runtime'),handoff_root=str(runtime/'handoff'),source_roots=[],
            legacy_app=str(Path(__file__).parent),provider=config.get('base_url',''),model=config.get('text_model',''),
            rag_url=saved['ragflow_url'],llm_workers=global_limit,local_workers=local,rag_workers=rag,
            max_live_units=max_units,cleanup=False,max_ingest_backlog=int(config.get('max_ingest_backlog',30)),
            max_pending_package_bytes=int(config.get('max_pending_package_bytes',2*1024**3)))
        core.prepare();manager=Manager(self.registry.store,runtime/'manager').start();self.manager=manager
        runner=None;status='stopped'
        try:
            if not global_control.snapshot()['admission_open']:return self.projection('stopped')
            row=self.registry.scope_snapshot(self.scope_id)
            if row['intent']=='completed' and not row['stop_pending']:return self.projection('complete')
            if self.startup and (row['intent']!='running' or row['stop_pending']):return self.projection('stopped')
            if row['stop_pending']:
                row=self.registry.set_intent(self.scope_id,expected_generation=row['generation'],intent='stopped')
            if self.processor.stop.is_set():return self.projection('stopped')
            if row['intent']!='running':row=self.registry.set_intent(self.scope_id,expected_generation=row['generation'],intent='running')
            scheduler=Scheduler(manager,core,isolation_root=root)
            adapter=PreparedAdapter(self.registry,self.scope_id,row['generation'],data_root=root,settings_path=root)
            self.adapter=adapter;adapter.attach(scheduler)
            manager.wait_resources=True
            manager.unit_limits.update(graph=body+purpose,delivery=rag,prepared_local=local)
            manager.llm_operation_limits.update(prepared_body=body,prepared_purpose=purpose)
            for member in self.registry.members(self.scope_id):
                try:bind_member(self.registry,self.scope_id,row['generation'],member)
                except (OSError,ValueError) as exc:
                    self.registry.store.event(member['task_id'],'prepared_binding_held',dict(error=type(exc).__name__))
                    from dataclasses import replace
                    observation=self.registry.observation(member['task_id'])
                    if observation:self.registry.refresh_observation(self.scope_id,replace(observation,
                        phase='material_review',next_step='review',blocking_reason='prepared_binding_conflict'))
            if self.processor.stop.is_set():self.stop();return self.projection('stopped')
            manager.resume(expected_generation=manager.generation)
            def schedule():
                try:scheduler.run(self.runner_stop,interval=.5)
                except BaseException as exc:self.failure=exc;self.runner_stop.set()
            runner=threading.Thread(target=schedule,name='prepared-existing-scheduler');runner.start()
            status='running';next_save=0;next_closure=0
            while not self.runner_stop.wait(.1):
                if self.processor.stop.is_set():
                    self.stop()
                    if not global_control.path.exists():status='stopped';break
                global_state=global_control.snapshot()
                if not global_state['admission_open']:
                    status='unknown' if global_state['state']=='unknown' else 'stopping'
                    with scheduler.tick_lock:
                        scheduler.reap()
                        active=scheduler.ledger.rows("SELECT 1 FROM scheduler_jobs j JOIN prepared_members m ON m.task_id=j.unit_key WHERE m.scope_id=? AND j.state='running'",(self.scope_id,))
                        if not active and not manager.status()['live_units']:
                            row=self.registry.scope_snapshot(self.scope_id)
                            if row['intent']=='running':self.registry.set_intent(self.scope_id,expected_generation=row['generation'],intent='stopped')
                            status='unknown' if global_state['unknown'] else 'stopped'
                            self.runner_stop.set();break
                if time.monotonic()>=next_closure:
                    phases=self.registry.scope_snapshot(self.scope_id)['phases']
                    if set(phases)<=set(('delivery_verified','cleaned','retained','excluded')):
                        closure=scope_delivery_closure(self.registry,self.scope_id)
                        if closure['complete']:
                            # Settle the last worker before completing; cleanup remains
                            # governed by the existing policy and native admission.
                            with scheduler.tick_lock:
                                active=scheduler.ledger.rows("SELECT 1 FROM scheduler_jobs j JOIN prepared_members m ON m.task_id=j.unit_key WHERE m.scope_id=? AND j.state='running'",(self.scope_id,))
                                closure=scope_delivery_closure(self.registry,self.scope_id)
                                if not active and closure['complete']:
                                    self.registry.complete_scope(self.scope_id,adapter.generation,closure['evidence_sha256'])
                                    status='complete';self.runner_stop.set();break
                    next_closure=time.monotonic()+5
                if time.monotonic()>=next_save:
                    manifest=self.projection(status);self.processor.store.save(manifest);self.callback(manifest)
                    next_save=time.monotonic()+2
            if self.failure:raise self.failure
            return self.projection(status if status!='running' else 'stopped')
        finally:
            self.runner_stop.set()
            if runner:runner.join(timeout=35)
            # The global stop path reaches this point only after native reap.
            # On a scheduler failure, keep admitted execution authority until
            # its workers exit; a failure is never converted to a false pause.
            if global_control.path.exists() and manager.status()['live_units']:
                import uuid
                global_control.request_stop('prepared-finalize-'+uuid.uuid4().hex)
                while manager.status()['live_units']:
                    if self.adapter:self.adapter.scheduler.reap()
                    time.sleep(.1)
                if self.adapter:self.adapter.scheduler.reap()
            manager.pause('prepared coordinator finished')
            manager.shutdown()
            # A normal exit is a persisted stop. A hard crash retains the last
            # running intent for the same frozen scope's startup reconciliation.
            row=self.registry.scope_snapshot(self.scope_id)
            if row['intent']=='running':self.registry.set_intent(self.scope_id,expected_generation=row['generation'],intent='stopped')


def run_scope(processor,selected,callback,*,resume_scope_id=None,startup=False,expected_manifest=None):
    from paperflow.store import Store
    from paperflow.prepared_store import PreparedRegistry,scope_manifest
    root=processor.store.root;existing=persisted_scope(root)
    database=control_path(root)
    if resume_scope_id:
        if not existing or existing['scope_id']!=resume_scope_id:raise ValueError('待继续的冻结范围已改变')
        registry=PreparedRegistry(Store(database));registry.scope_snapshot(resume_scope_id)
        coordinator=Coordinator(processor,registry,resume_scope_id,callback,startup=startup)
        processor._prepared_coordinator=coordinator
        try:
            manifest=coordinator.run();processor.store.save(manifest);callback(manifest);return manifest
        finally:processor._prepared_coordinator=None
    if startup:raise ValueError('启动恢复不能创建新的授权范围')
    rows=freeze_records(selected,root,processor.config);manifest_sha=scope_manifest(rows);scope_id=manifest_sha[:32]
    if expected_manifest is not None and expected_manifest!=manifest_sha:raise ValueError('automatic recovery scope changed before dispatch')
    if not database.exists() and any((Path(r['record']['prepared_checkpoint']['path']).parent/'prepared-scope.json').exists() for r in rows):
        raise ValueError('材料有控制绑定但账本丢失；只允许核对')
    registry=PreparedRegistry(Store(database))
    registry.create_scope(rows,scope_id=scope_id,manifest_sha256=manifest_sha)
    write_json(root/'prepared-current-scope.json',dict(schema='prepared-current-scope/1',scope_id=scope_id,manifest_sha256=manifest_sha))
    coordinator=Coordinator(processor,registry,scope_id,callback);processor._prepared_coordinator=coordinator
    try:
        manifest=coordinator.run();processor.store.save(manifest);callback(manifest);return manifest
    finally:processor._prepared_coordinator=None
