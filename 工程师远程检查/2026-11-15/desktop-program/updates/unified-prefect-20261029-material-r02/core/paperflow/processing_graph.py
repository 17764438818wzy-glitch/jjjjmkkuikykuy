"""Stage-3 graph: three business rounds, immutable seal, ledger handoff, END.

The historical graph.py executor remains disconnected from this entry point.
No delivery, repair-run scheduling, activation or cleanup node is compiled here.
"""
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import uuid
from typing import TypedDict

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import START, END, StateGraph

from .handoff import PackageHandoff
from .package import inside, load, save, sha, normalize_delivery, materialize_body_chunks, verify_delivery_identity, final_routing_candidates
from .phase_cache import CACHE_STAGES, input_fingerprint, read_cache, content_token, phase_dependency
from .producer import HarnessProducer, PUBLIC_NAMES, SyntaxRepairExhausted
from .instrumentation import timed,current_recorder,best_effort,safe_now


class ProcessingState(TypedDict):
    task: dict


class ProcessingWorkflow:
    def __init__(self, config, store, actions, *, producer=None):
        if actions is None:raise ValueError('managed actions required')
        self.config,self.store,self.actions=config,store,actions
        config.processing_ledger_path=str(store.path)
        self.catalog=config.catalog
        config.prepare()
        self.connection=sqlite3.connect(config.root/'checkpoints/processing.sqlite3',timeout=1,check_same_thread=False)
        self.connection.execute('PRAGMA journal_mode=WAL')
        self.connection.execute('PRAGMA synchronous=FULL')
        self.connection.execute('PRAGMA busy_timeout=1000')
        self.saver=SqliteSaver(self.connection)
        from .embedding_admission import admission_from_config,bind_worker_counter
        admission=bind_worker_counter(admission_from_config(config),getattr(actions,'client',None),config.root)
        self.producer=producer or HarnessProducer(config,control=actions,admission=admission)
        self.handoff_service=PackageHandoff(store,config.handoff_root,admission=admission)
        stages=(*CACHE_STAGES,'repair','seal','handoff','attachment_handoff')
        graph=StateGraph(ProcessingState)
        for stage in stages:graph.add_node(stage,self._node(stage))
        graph.add_node('control',lambda state:state)
        destinations={s:s for s in stages}|{'handed_off':END,'held':END,'attachment_ready':END}
        graph.add_conditional_edges(START,lambda state:state['task']['stage'],destinations)
        for stage in (*stages,'control'):
            graph.add_conditional_edges(stage,lambda state:state['task']['stage'],destinations)
        self.graph=graph.compile(checkpointer=self.saver,interrupt_after=list(stages))

    def close(self):self.connection.close()

    @staticmethod
    def graph_config(task):
        return {'configurable':{'thread_id':task.get('thread_id',task['task_id'])},'recursion_limit':30}

    @staticmethod
    def phases(task):
        if not task.get('repair_request_id'):return CACHE_STAGES
        return ('repair',*CACHE_STAGES[CACHE_STAGES.index(task['failed_stage'])+1:])

    def _node(self,stage):
        def execute(state):
            task=dict(state['task'])
            if not self.store.is_current(task['task_id']):raise ValueError('source superseded')
            if stage in (*CACHE_STAGES,'repair'):
                result=self.producer.run_stage(task,stage)
                if result.get('article_title'):
                    task.setdefault('original_display_name',task.get('display_name') or Path(task['source_path']).name)
                task.update(result)
                task['stage_timings']=dict(task.get('stage_timings',{}),**{stage:result.get('_stage_timing',dict(metrics_complete=False))})
                outputs=dict(task.get('stage_outputs',{}))
                outputs[stage]=dict(input_sha256=result['stage_input_sha256'],directory=result['draft_dir'])
                task['stage_outputs']=outputs
                business=task['failed_stage'] if stage=='repair' else stage
                dependencies=dict(task.get('stage_dependencies',{}))
                dependencies[business]=phase_dependency(task,business,stage,result)
                task['stage_dependencies']=dependencies
                if stage=='round1' and result.get('disposition') in ('exclude','uncertain'):
                    task.update(stage='held',hold_reason=result['disposition'])
                elif stage=='round1' and result.get('attachment'):
                    task['stage']='attachment_handoff'
                elif stage=='round2' and not final_routing_candidates(task['draft_dir']):
                    task.update(stage='held',hold_reason='classification_pending')
                else:
                    phases=self.phases(task)
                    task['stage']=dict(zip(phases,(*phases[1:],'seal')))[stage]
            elif stage=='attachment_handoff':
                from .attachments import prepare_package
                root=self.actions.run('local',dict(operation='seal_attachment'),lambda cancel:prepare_package(self.config,task),total_timeout=300)
                task.update(self.actions.attachment_handoff(root),stage='attachment_ready')
            elif stage=='seal':
                from .embedding_preparation import seal_with_length_repair
                task=seal_with_length_repair(self,task)
            else:
                self.prime_handoff(task)
                receipt=self.actions.handoff(task['publication_fingerprint'])
                task.update(receipt,stage='handed_off')
            return dict(task=task)
        return execute

    def prepare_seal_material(self,task):
        # Always copy verified stage bytes; never normalize an immutable cache directory in place.
        if sha(task['source_path'])!=task['source_sha256']:raise ValueError('original source changed')
        draft=Path(task['draft_dir'])
        output=self.config.root/'work'/task['task_id']/'sealing'/uuid.uuid4().hex
        from .cleanup_paths import track_artifact
        track_artifact(self.config,task,output,'sealing')
        output.mkdir(parents=True)
        for name in PUBLIC_NAMES:
            if (draft/name).is_file():shutil.copy2(draft/name,output/name)
        if (draft/'assets').is_dir():shutil.copytree(draft/'assets',output/'assets')
        catalog=task.get('repair_catalog',self.catalog) if task.get('failed_stage')!='round2' else self.catalog
        normalize_delivery(output,catalog,task=task)
        materialize_body_chunks(output)
        assets=load(output/'assets.json')
        if any(a.get('storage')=='image' and a.get('source_image_id') for a in assets.get('items',[])):
            if not task.get('reading_dir'):raise ValueError('selected images missing source view')
            result=subprocess.run([self.config.reader_python or sys.executable,'-X','utf8','-B','-m','paperflow.reading',
                task['reading_dir'],str(output),'--asset-spec',str(output/'assets.json')],
                cwd=Path(__file__).parent.parent,capture_output=True,timeout=120,
                creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            if result.returncode:raise ValueError('image materialization failed')
            assets=load(output/'assets.json')
        for asset in assets.get('items',[]):
            if asset.get('storage')=='image':asset['sha256']=sha(inside(output,asset['file']))
        save(output/'assets.json',assets)
        if task.get('repair_source'):
            origin=self.verify_repair_input(task)
            stage=task['failed_stage']
            if content_token(output,stage)==content_token(origin['package_dir'],stage):
                raise ValueError('repair produced no affected content change')
        expected=task.get('expected_targets')
        if task.get('failed_stage')=='round2':
            codes={r['code'] for r in load(output/'profile.json').get('routing_candidates',[])}
            expected={code:expected.get(code,dict(manifest_sha256=None,generation=0)) for code in codes}
        return output,catalog,expected

    def seal(self,task,prepared=None):
        output,catalog,expected=prepared or self.prepare_seal_material(task)
        thread=task.get('thread_id',task['task_id'])
        intent=self.handoff_service.prepare(thread,task,output,catalog,
            mapping_version=sha(self.config.catalog_path) if task.get('failed_stage')=='round2' else task.get('mapping_version',sha(self.config.catalog_path)),
            expected_targets=expected,repair_source=task.get('repair_source'))
        package=self.handoff_service.publish(thread,intent['input_fingerprint'])
        digest=sha(package/'manifest.json')
        return dict(publication_fingerprint=intent['input_fingerprint'],package_dir=str(package),
                    manifest_sha256=digest,
                    embedding_qualification=self.handoff_service.qualifications[digest])

    def seal_managed(self,task):
        from .embedding_admission import ConfiguredAdmission,AdmissionRejected,renderer_fingerprint,evaluate,final_inputs
        from .package import seal_package,validate_package
        from .structure_plan import build_plan,PLAN_FILE
        from .structure_split import SplitHold
        admission=self.handoff_service.admission
        if not isinstance(admission,ConfiguredAdmission):
            return self.actions.run('local',dict(operation='seal'),lambda cancel:self.seal(task),total_timeout=300)
        prepared=self.actions.run('local',dict(operation='seal_prepare'),
            lambda cancel:self.prepare_seal_material(task),total_timeout=300)
        output,catalog,_=prepared
        def stage(plan=None):
            if plan is not None:save(output/PLAN_FILE,plan)
            seal_package(output,task,task.get('revision',1))
            return validate_package(output,task,catalog,strict_seal=True)
        packet=self.actions.run('local',dict(operation='seal_preview'),lambda cancel:stage(),total_timeout=120)
        if (admission.policy.renderer_fingerprint!=renderer_fingerprint() or
                admission.observed_path!=admission.policy.path_fingerprint or admission.counter is None):
            admission.check(task,packet)  # Fails closed before planning with unaccepted rules.
        for attempt in range(3):
            if packet['routes']:
                try:
                    plan=build_plan(task,packet,admission.policy,admission.counter)
                except SplitHold as exc:
                    evidence=evaluate(final_inputs(task,packet,native=admission.native),admission.policy,admission.counter,
                        observed_path=admission.observed_path,observed_renderer=renderer_fingerprint())
                    evidence.update(admission_verified=False,structure_hold=exc.reason)
                    evidence['records'].append(dict(record_type='tree',record_id='__structure',
                        issues=['embedding_structure'],reason=exc.reason))
                    raise AdmissionRejected(evidence) from exc
                packet=self.actions.run('local',dict(operation='seal_plan'),
                    lambda cancel:stage(plan),total_timeout=120)
            try:
                # Actual final manifest prefixes, complete records and all targets.
                self.handoff_service.check_embedding(task,packet)
                break
            except AdmissionRejected as exc:
                bad=[row for row in exc.evidence['records'] if row['issues']]
                if (attempt==2 or not bad or any(row['record_type'] not in ('profile','tree') for row in bad)):
                    raise
                # Counts/plan are provisional until this real-manifest pass.
        admission.prepared_only=True
        try:
            return self.actions.run('local',dict(operation='seal'),
                lambda cancel:self.seal(task,prepared),total_timeout=300)
        finally:admission.prepared_only=False

    def prime_handoff(self,task):
        """A resumed graph may have lost its epoch's count cache. Rewarm outside local."""
        from .embedding_admission import ConfiguredAdmission
        admission=self.handoff_service.admission
        if not isinstance(admission,ConfiguredAdmission):return
        thread=task.get('thread_id',task['task_id'])
        def read(cancel):
            intent=self.handoff_service.ledger.intent(thread,task['publication_fingerprint'])
            return self.handoff_service._published(intent)[0]
        packet=self.actions.run('local',dict(operation='handoff_prepare'),read,total_timeout=120)
        self.handoff_service.check_embedding(task,packet)

    def _mirror(self,task):
        # Projection is a fact only; never used to pick a pending graph node.
        if task.get('repair_request_id'):return
        fields={k:task[k] for k in ('stage','hold_reason','package_dir','manifest_sha256','attachment_id',
            'display_name','article_title','original_display_name') if k in task}
        self.store.update(task['task_id'],**fields)

    def verify_repair_input(self,task):
        source=self.handoff_service.ledger.registration(task['repair_source']['manifest_sha256'])
        if not source or source['lifecycle']!='retained':raise ValueError('retained repair source required')
        manifest,digest,_,_=verify_delivery_identity(source['package_dir'],task,strict_seal=True)
        if (digest!=source['manifest_sha256'] or (Path(source['package_dir'])/'manifest.json').read_bytes()!=source['manifest_bytes']
                or any(manifest[k]!=task[k] for k in ('task_id','source_id','source_sha256'))):
            raise ValueError('repair source changed')
        return source

    def run(self,registered_task):
        if registered_task.get('repair_source'):
            self.actions.run('local',dict(operation='verify_repair_input'),
                lambda cancel:self.verify_repair_input(registered_task),total_timeout=120)
        config=self.graph_config(registered_task)
        snapshot=self.graph.get_state(config)
        if snapshot.values:
            task=snapshot.values['task']
            self._mirror(dict(task,stage=snapshot.next[0] if snapshot.next else task['stage']))
            # Clearing stale pending tasks and routing the verified prefix use two
            # native commits. Empty pending alone is therefore not a terminal fact:
            # the native task still records unfinished work if we exited in between.
            if not snapshot.next and task['stage'] in ('handed_off','held','attachment_ready'):return task
            if snapshot.next!=('handoff',):
                rebuilt=self.actions.run('local',dict(operation='verify_checkpoint_outputs'),
                    lambda cancel:self.reusable_prefix(registered_task,task),total_timeout=120)
                if rebuilt is not None or not snapshot.next:
                    # Native control update, derived from verified checkpoint outputs, never SQL stage.
                    if snapshot.next:self.graph.update_state(config,None,as_node=END)
                    self.graph.update_state(config,dict(task=rebuilt or task),as_node='control')
            inputs=None
        else:
            # This is a newly registered initial graph, not migration of an old workflow.
            if registered_task['stage'] not in ('round1','repair'):raise ValueError('legacy graph migration is not stage 3')
            task=dict(registered_task)
            inputs=dict(task=task)
        while True:
            try:result=self.graph.invoke(inputs,config)
            except SyntaxRepairExhausted:
                if registered_task.get('repair_request_id'):self.actions.hold_repair()
                raise
            inputs=None
            task=result['task']
            self._mirror(task)
            if not self.graph.get_state(config).next:return task

    def reusable_prefix(self,registered,task):
        base=dict(registered) if registered.get('repair_request_id') else {k:registered[k] for k in
            ('task_id','source_id','source_sha256','source_path','revision','contract_version',
             'display_name','article_title','original_display_name') if k in registered}
        phases=self.phases(base)
        base.update(stage=phases[0],stage_outputs={})
        if sha(base['source_path'])!=base['source_sha256']:raise ValueError('original source changed')
        for stage in phases:
            recorder=getattr(getattr(self.actions,'client',None),'metrics',None)
            measured_started=safe_now(recorder) if recorder else None
            output=task.get('stage_outputs',{}).get(stage)
            if output is None:return None
            fingerprint=input_fingerprint(self.config,base,stage)
            cached=read_cache(self.config,base,stage,fingerprint)
            if (fingerprint!=output['input_sha256'] or cached is None
                    or cached['draft_dir']!=output['directory']):
                return dict(base,stage=stage)
            if cached.get('article_title'):
                base.setdefault('original_display_name',base.get('display_name') or Path(base['source_path']).name)
            base.update(cached)
            if recorder:
                best_effort(recorder.stages.append,dict(stage=stage,disposition='checkpoint_skip',
                    elapsed_seconds=max(0,safe_now(recorder)-measured_started),metrics_complete=recorder.complete,
                    input_fingerprint=fingerprint,outcome='complete'))
            base['stage_outputs'][stage]=output
            business=base['failed_stage'] if stage=='repair' else stage
            base.setdefault('stage_dependencies',{})[business]=phase_dependency(base,business,stage,cached)
            if stage=='round1' and base.get('attachment'):return dict(base,stage='attachment_handoff')
        return None
