"""单篇 LangGraph：每次推进一个阶段，调度层负责持续运行和公平限流。"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import copy
import uuid
from typing import TypedDict,Annotated

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import RetryPolicy,Command,Send,Overwrite
from langgraph.errors import NodeError

from .package import (PackageError, inside, load, publish_package, save, seal_package, sha,
                      validate_package, normalize_delivery, materialize_body_chunks, verify_delivery_identity)
from .producer import (HarnessProducer,PUBLIC_NAMES,SyntaxRepairExhausted,MAX_DELIVERY_CORRECTIONS,
                       delivery_correction_count,reserve_delivery_correction)
from .rag import RagPublisher, RemoteConflict, native_client
from .store import StaleSourceVersion,UpdateConflict,TERMINAL
from .config import rules_fingerprint
from .errors import controller_fields,is_infrastructure


LLM_STAGES=('round1','round2','round3_assets','round3_redact','round3_chunk','repair')
RAG_STAGES=('reconcile','ingest','ingest_target','verify','activate')
LOCAL_STAGES=('round3','publish','publish_repaired','validate','cleanup','classify_error','join_targets')
# 只串行化版本可见性切换；读取、模型、切片入库仍使用各自并发池。
ACTIVATION_LOCK=threading.RLock()


def merge_targets(left,right):return dict(left or {},**(right or {}))


class TaskState(TypedDict, total=False):
    task_id: str
    stage: str
    task: dict
    code: str
    target_results: Annotated[dict,merge_targets]


class Workflow:
    def __init__(self, config, store, producer=None, publisher=None, catalog=None):
        self.config, self.store = config, store
        self.catalog = config.catalog if catalog is None else catalog
        self.producer = producer or HarnessProducer(config)
        self.publisher = publisher
        self.rag_slots=threading.BoundedSemaphore(config.rag_workers)
        config.prepare()
        self.connection = sqlite3.connect(config.root/'checkpoints/graph.sqlite3', check_same_thread=False)
        self.saver = SqliteSaver(self.connection)
        builder = StateGraph(TaskState)
        stages = (*LLM_STAGES, *RAG_STAGES, *LOCAL_STAGES)
        for stage in stages:
            options={} if stage=='classify_error' else dict(
                retry_policy=RetryPolicy(max_attempts=3,initial_interval=config.short_retry_interval,
                    max_interval=max(config.short_retry_interval*2,.001),jitter=False,retry_on=is_infrastructure),
                error_handler=self._node_error_handler(stage))
            action=self._ingest_target if stage=='ingest_target' else self._node(stage)
            builder.add_node(stage,action,defer=stage=='join_targets',**options)
        builder.add_node('control',lambda state:state)
        builder.add_conditional_edges('control',self._next,
            {s:(END if s in TERMINAL else s) for s in (*stages,*TERMINAL)})
        builder.add_conditional_edges(START, lambda state: state['stage'], {s:s for s in stages})
        normal={'round1':['round2','held'],'round2':['round3','held'],'round3':['round3_assets'],
                'round3_assets':['round3_redact'],'round3_redact':['round3_chunk'],'round3_chunk':['publish'],
                'repair':list((*LLM_STAGES,*RAG_STAGES,*LOCAL_STAGES,'held')),'publish':['validate'],'validate':['ingest'],
                'ingest':['ingest_target','join_targets'],'ingest_target':['join_targets'],
                'publish_repaired':['validate','ingest','ingest_target','verify','activate'],
                'join_targets':['activate','verify','classify_error'],'verify':['activate'],'activate':['cleanup'],
                'cleanup':['complete'],'reconcile':['round3','complete'],
                'classify_error':list((*LLM_STAGES,*RAG_STAGES,*LOCAL_STAGES,*TERMINAL))}
        for stage in stages:
            if stage=='ingest_target':
                builder.add_edge(stage,'join_targets');continue
            targets=set(normal[stage]+[stage,'repair','parked','superseded','code_error','classify_error'])
            builder.add_conditional_edges(stage,self._next,
                {s:(END if s in ('complete','held','parked','superseded','code_error') else s) for s in targets})
        # 原生图保存后继边；每阶段暂停，把执行名额交还给所属并发池。
        self.graph = builder.compile(checkpointer=self.saver,interrupt_after=list(stages))

    def close(self):
        self.connection.close()

    def graph_config(self,task):
        return {'configurable':{'thread_id':task['task_id']},'recursion_limit':30,'max_concurrency':self.config.rag_workers}

    def _next(self,state):
        if state['stage']!='ingest_target':return state['stage']
        task=state['task'];receipts=self._receipts(task)
        codes=task.get('route_codes') or [r['code'] for r in self._packet(task)['routes']]
        pending=[code for code in codes if not receipts.get(code,{}).get('verified')]
        return [Send('ingest_target',dict(task=task,code=code)) for code in pending] or 'join_targets'

    def step(self, task):
        checkpoint = self.graph.get_state(self.graph_config(task))
        saved = checkpoint.values.get('task', {})
        if task.get('control_request'):
            return self.apply_control(task)
        if saved:
            next_stage=checkpoint.next[0] if checkpoint.next else saved['stage']
            if task['stage']!=next_stage or not checkpoint.next:
                return self._commit_node(task,dict(saved,stage=next_stage))
            # 原生 None 恢复 pending 后继节点；不以台账里的内部字段重新启动 START。
            result=self._invoke(None,self.graph_config(task))
            return self._commit_node(task,result['task'])
        if task['stage'] in TERMINAL:return task
        if task['stage']=='repair':
            task=dict(task,failed_stage=self.business_stage(task))
        result=self._invoke(dict(task_id=task['task_id'],stage=task['stage'],task=task),self.graph_config(task))
        data = result['task']
        return self._commit_node(task,data)

    def _invoke(self,inputs,config):
        before=self.graph.get_state(config)
        seen={v.get('handled_error_token') for v in before.values.get('target_results',{}).values()}
        try:result=self.graph.invoke(inputs,config)
        except Exception as exc:
            # 1.2.15 的 Send error_handler 已提交结果后，executor 仍可能重抛原异常。
            # 只接受本次新产生、类型吻合、全部分支已落盘且pending为汇合节点的处理证明。
            snapshot=self.graph.get_state(config)
            results=snapshot.values.get('target_results',{})
            task=snapshot.values.get('task',{})
            handled=any(v.get('handled_error_token') not in seen and v.get('handled_error_token') and
                        v.get('type')==type(exc).__name__ for v in results.values())
            if not (handled and snapshot.next==('join_targets',) and
                    set(task.get('route_codes',[])).issubset(results)):
                raise
            result=snapshot.values
        # 分类只做本地台账处理，不占用新的模型调用；中间检查点仍可单独恢复。
        if result.get('task',{}).get('stage')=='classify_error' and self.graph.get_state(config).next:
            result=self.graph.invoke(None,config)
        return result

    def apply_control(self,task):
        request=task['control_request']
        state=self.graph.get_state(self.graph_config(task))
        internal=dict(state.values.get('task') or task)
        internal.update(request['fields']);internal.pop('control_request',None)
        internal.pop('step_seq',None)
        # 原生 END 更新清空旧 pending Send/汇合任务，再声明控制入口。
        # 否则 update_state 只增加控制边，旧分支续跑会和新入口同时写共享状态。
        config=self.graph.update_state(self.graph_config(task),None,as_node=END)
        self.graph.update_state(config,dict(task_id=task['task_id'],stage=internal['stage'],task=internal,
            target_results=Overwrite({})),as_node='control')
        return self._commit_node(task,dict(internal,control_request=None))

    def apply_pending_requests(self):
        for task in self.store.all_tasks():
            if task.get('control_request') and not task.get('owner'):self.apply_control(task)

    def _commit_node(self,original,data):
        snapshot=self.graph.get_state(self.graph_config(original))
        if snapshot.next:data=dict(data,stage=snapshot.next[0])
        # 台账只同步调度副本及远端交付身份；产物/失败位置仅由检查点决定。
        mirror={'stage','revision','kind','due','error_kind','error','retry_count','infra_started_at','repair_count',
                'stagnant_count','completed_at','alert','rules_fingerprint','package_dir','manifest_sha256',
                'failure_packet','traceback_private','control_request','resume_stage','disposition',
                'correction_count','delivery_check','pending_routes','route_codes'}
        fields={k:v for k,v in data.items() if k in mirror and (k not in original or original[k]!=v)}
        fields.update(owner=None, lease_until=0)
        try:
            stored=self.store.update(original['task_id'],expected_version=original.get('version',0),
                drop_fields=('step_seq','draft_dir','reading_dir','failed_stage','processing_history'),**fields)
            return dict(stored,**{k:v for k,v in data.items() if k not in stored})
        except UpdateConflict:
            self.store.event(original['task_id'],'node_result_discarded',dict(reason='optimistic_version_conflict',input_version=original.get('version',0)))
            self.store.release_lease(original['task_id'],original.get('owner'))
            return self.store.get(original['task_id'])

    def _node(self, stage):
        def execute(state):
            started=time.monotonic()
            task = dict(state['task'])
            if stage=='join_targets':task['target_results']=state.get('target_results',{})
            task.pop('step_seq',None)
            try:
                if stage=='activate':
                    journal=self.store.receipt(task['task_id'],task['revision'],'activation')
                    if journal.get('status')=='pending':
                        self._rollback_activation(task,journal,self._publisher())
                if not self.store.is_current(task['task_id']):
                    raise StaleSourceVersion('source has a newer version')
                task = self._execute(stage, task)
                if stage!='classify_error' and task['stage']!='classify_error':
                    task.update(error_kind=None,error=None,retry_count=0,infra_started_at=None,due=0)
            except Exception as exc:
                kind='content' if isinstance(exc,PackageError) else ('infrastructure' if is_infrastructure(exc) else 'controller')
                self.store.event(task['task_id'],'stage',dict(stage=stage,next_stage='classify_error',error_kind=kind,
                    revision=task['revision'],seconds=round(time.monotonic()-started,3)))
                raise
            self.store.event(task['task_id'], 'stage', dict(stage=stage, next_stage=task['stage'],
                revision=task['revision'],error_kind=task.get('error_kind'),seconds=round(time.monotonic()-started,3)))
            result=dict(task_id=task['task_id'], stage=task['stage'], task=task)
            if stage=='repair':result['target_results']=Overwrite({})
            return result
        if stage=='activate':
            def serialized(state):
                with ACTIVATION_LOCK:
                    return execute(state)
            execute_fn=serialized
        else:execute_fn=execute
        if stage in RAG_STAGES:
            def limited(state):
                with self.rag_slots:return execute_fn(state)
            return limited
        return execute_fn

    def _node_error_handler(self,stage):
        def handler(state,error:NodeError):
            exc=error.error
            if not isinstance(exc,Exception):raise exc
            task=dict(state['task'])
            if isinstance(exc,StaleSourceVersion):
                if stage=='ingest_target':
                    return Command(update=dict(target_results={state['code']:dict(category='superseded',
                        type=type(exc).__name__,handled_error_token=uuid.uuid4().hex)}),goto='join_targets')
                task.update(stage='superseded',error_kind=None,error=None)
                return Command(update=dict(task=task,stage='superseded'),goto=END)
            if stage in ('publish','publish_repaired','validate') and isinstance(exc,(FileNotFoundError,json.JSONDecodeError)):
                exc=PackageError('invalid delivery artifact: '+str(exc)[:200])
            if getattr(exc,'draft_dir',None):task['draft_dir']=exc.draft_dir
            kind='content' if isinstance(exc,PackageError) else ('infrastructure' if is_infrastructure(exc) else 'controller')
            info=dict(stage='publish' if stage=='publish_repaired' else stage,type=type(exc).__name__,category=kind,
                      reason=str(exc)[:350] if kind=='content' else None,
                      delivery_check=getattr(exc,'check',None),correction_count=getattr(exc,'correction_count',None))
            if kind=='controller':info['controller_fields']=controller_fields(self.config,task,stage,exc)
            if getattr(exc,'privacy_hits',None):
                private=self.config.root/'private/privacy'/task['task_id']/f'{time.time_ns()}.json'
                save(private,dict(hits=exc.privacy_hits));info['private_diagnostics']=str(private)
            if stage=='ingest_target':
                info['target_code']=state['code']
                info['handled_error_token']=uuid.uuid4().hex
                return Command(update=dict(target_results={state['code']:info}),goto='join_targets')
            task.update(stage='classify_error',failure_info=info)
            return Command(update=dict(task=task,stage='classify_error'),goto='classify_error')
        return handler

    def _packet(self, task):
        packet = validate_package(task['package_dir'], task, self.catalog)
        if task.get('manifest_sha256') and packet['manifest_sha256'] != task['manifest_sha256']:
            raise PackageError('immutable published revision changed','identity')
        return packet

    def _publisher(self):
        return self.publisher or RagPublisher(native_client(self.config))

    def _receipts(self, task):
        return self.store.target_receipts(task['task_id'],task['revision'])

    def _save_receipts(self, task, receipts):
        for code,value in receipts.items():
            if isinstance(value,dict):self.store.receipt(task['task_id'],task['revision'],code,value)
        save(self.config.root/'receipts'/task['task_id']/f'r{task["revision"]:03d}.json', receipts)

    def _rollback_activation(self,task,journal,publisher):
        errors=[]
        # 重启后后继版本可能已先提交；以当前活动台账恢复，不能复活日志中的旧版。
        active_versions=self.store.active_versions_for(task['task_id'])
        for active in active_versions:
            old_receipts=self.store.target_receipts(active['task_id'],active['revision'])
            try:publisher.set_enabled(old_receipts,True)
            except Exception as exc:errors.append(exc)
        current_active=any(a['task_id']==task['task_id'] and a['revision']==task['revision'] for a in active_versions)
        if not current_active:
            try:publisher.set_enabled(journal['current'],False)
            except Exception as exc:errors.append(exc)
        if errors:raise ConnectionError('availability rollback not confirmed') from errors[0]
        journal=dict(journal,status='committed' if current_active else 'rolled_back')
        self.store.receipt(task['task_id'],task['revision'],'activation',journal)

    def _ingest_target(self,state):
        task,code=state['task'],state['code']
        with self.rag_slots:
            if not self.store.is_current(task['task_id']):raise StaleSourceVersion('source changed before target ingestion')
            packet=self._packet(task);packet=dict(packet,routes=[r for r in packet['routes'] if r['code']==code])
            if not packet['routes']:raise PackageError('unknown target branch','wrong_library')
            target=self.store.receipt(task['task_id'],task['revision'],code)
            if target.get('verified'):return dict(target_results={code:dict(status='verified')})
            receipts={code:target}
            publisher=copy.copy(self.publisher) if self.publisher is not None else self._publisher()
            def save_target(value):
                self.store.receipt(task['task_id'],task['revision'],code,value[code])
                save(self.config.root/'receipts'/task['task_id']/f'r{task["revision"]:03d}'/(code+'.json'),value[code])
            publisher.on_change=save_target
            start=time.monotonic()
            try:publisher.publish(task,packet,receipts);publisher.verify(task,packet,receipts)
            except Exception as exc:
                kind='content' if isinstance(exc,PackageError) else ('infrastructure' if is_infrastructure(exc) else 'controller')
                self.store.event(task['task_id'],'stage',dict(stage='ingest_target',code=code,revision=task['revision'],
                    seconds=round(time.monotonic()-start,3),error_kind=kind))
                raise
            self.store.event(task['task_id'],'stage',dict(stage='ingest_target',code=code,revision=task['revision'],
                seconds=round(time.monotonic()-start,3),error_kind=None))
            return dict(target_results={code:dict(status='verified')})

    def _execute(self, stage, task):
        if stage=='classify_error':
            info=task['failure_info']
            task['stage']=info['stage']
            if info['category']=='controller':task.update(info['controller_fields'])
            else:
                exc=(SyntaxRepairExhausted(info['reason']) if info['type']=='SyntaxRepairExhausted' else
                     PackageError(info['reason'],info.get('delivery_check') or 'missing_files')) if info['category']=='content' else ConnectionError(info['type'])
                if info.get('correction_count') is not None:exc.correction_count=info['correction_count']
                if info.get('delivery_check'):exc.check=info['delivery_check']
                task=self._failure(task,info['stage'],exc)
                if info['category']=='infrastructure':task['error']=info['type']
                if info.get('private_diagnostics') and task.get('failure_packet'):
                    p=Path(task['failure_packet'])/'failure.json';value=load(p);value['private_diagnostics']=info['private_diagnostics'];save(p,value)
        elif stage=='join_targets':
            receipts=self._receipts(task)
            pending=[code for code in task['route_codes'] if not receipts.get(code,{}).get('verified')]
            if pending:
                failures=[state for code,state in task.get('target_results',{}).items() if code in pending and state.get('category')]
                if failures and all(f.get('category')=='superseded' for f in failures):
                    task['stage']='ingest_target';return task
                if not failures:raise ConnectionError('target branch has not produced a durable result')
                rank={'controller':0,'content':1,'infrastructure':2}
                info=min(failures,key=lambda r:rank[r['category']])
                task.update(stage='classify_error',failure_info=info)
            else:
                if self._hold_unmapped_targets(task,self._packet(task)):return task
                task['stage']=task.pop('resume_consumer','activate')
        elif stage=='round3':
            task['stage']='round3_assets'
        elif stage in LLM_STAGES:
            result = self.producer.run_stage(task, stage)
            task.update(result)
            if result.get('disposition') in ('exclude', 'uncertain'):
                task['stage'] = 'held'
            else:
                task['stage']={'round1':'round2','round2':'round3','round3_assets':'round3_redact',
                    'round3_redact':'round3_chunk','round3_chunk':'publish','repair':task.get('failed_stage') or 'round1'}[stage]
                logical=task.get('failed_stage') if stage=='repair' else stage
                if stage!='repair':task['progress_level']=max(task.get('progress_level',0),
                    {'round1':1,'round2':2,'round3_assets':3,'round3_redact':4,'round3_chunk':5}.get(logical,0))
                if stage=='repair':
                    task['repaired_stage']=task.get('failed_stage')
                    # 补正已产生该阶段成果，不能再执行一次相同业务LLM节点。
                    next_after={'round1':'round2','round2':'round3','round3_assets':'round3_redact',
                                'round3_redact':'round3_chunk','round3_chunk':'publish'}
                    if task['stage'] in next_after:
                        repaired=task['stage']
                        order=list(next_after)
                        outputs={k:v for k,v in task.get('stage_outputs',{}).items()
                                 if k not in order[order.index(repaired):]}
                        outputs[repaired]=dict(input_sha256=result.get('stage_input_sha256'),directory=result['draft_dir'])
                        task['stage_outputs']=outputs
                        task['progress_level']=max(task.get('progress_level',0),
                            {'round1':1,'round2':2,'round3_assets':3,'round3_redact':4,'round3_chunk':5}[task['stage']])
                        task['stage']=next_after[task['stage']]
                    if task['stage'] in ('validate','ingest','ingest_target','verify','activate'):
                        task['stage']='publish_repaired'
                else:
                    task.pop('repaired_stage',None)
                    outputs=dict(task.get('stage_outputs',{}));outputs[stage]=dict(input_sha256=result.get('stage_input_sha256'),directory=result['draft_dir'])
                    task['stage_outputs']=outputs
        elif stage=='publish_repaired':
            # 消费者必须读取修复后新修订的不可变包，然后重做原失败阶段。
            origin=self.business_stage(task)
            task=self._execute('publish',task)
            task['route_codes']=[r['code'] for r in self._packet(task)['routes']]
            if origin in ('verify','activate'):
                # 新修订的回执不能继承旧版；先写入新包，再重做失败的消费者。
                task.update(stage='ingest',resume_consumer=origin)
            else:task['stage']=origin
        elif stage == 'publish':
            draft=Path(task['draft_dir'])
            if (draft/'READY.json').exists() or (draft/'manifest.json').exists():
                verify_delivery_identity(draft,task)
            staging = self.config.root/'work'/task['task_id']/f'r{task["revision"]:03d}'/'delivery'
            staging.mkdir(parents=True, exist_ok=True)
            for name in PUBLIC_NAMES:
                p = Path(task['draft_dir']) / name
                if p.is_file():
                    shutil.copy2(p, staging/name)
            normalize_delivery(staging,self.catalog,task=task)
            materialize_body_chunks(staging)
            assets = load(staging/'assets.json')
            if any(a.get('storage')=='image' and a.get('source_image_id') for a in assets.get('items',[])):
                if not task.get('reading_dir'):raise PackageError('selected images missing source view')
                result=subprocess.run([self.config.reader_python or sys.executable,'-X','utf8','-B','-m','paperflow.reading',
                    task['reading_dir'],str(staging),'--asset-spec',str(staging/'assets.json')],
                    cwd=Path(__file__).parent.parent,capture_output=True,timeout=120,
                    creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
                if result.returncode:
                    detail=result.stderr.decode('utf-8',errors='replace').splitlines()
                    raise PackageError('image materialization failed: '+(detail[-1] if detail else str(result.returncode)))
                assets=load(staging/'assets.json')
            for a in assets.get('items', []):
                if a.get('storage', 'image' if a.get('file') else 'description') == 'image':
                    dest = inside(staging, a['file'])
                    if not dest.is_file():
                        src = inside(task['draft_dir'], a['file'])
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(src, dest)
                    a['sha256'] = sha(dest)
            save(staging/'assets.json', assets)
            seal_package(staging, task, task['revision'])
            validate_package(staging, task, self.catalog)
            destination = Path(self.config.handoff_root)/'Updated manuscript'/task['task_id']/f'r{task["revision"]:03d}'
            if destination.exists():
                if sha(destination/'manifest.json') != sha(staging/'manifest.json'):
                    raise PackageError('immutable revision already exists with different content','identity')
            else:
                publish_package(staging, destination)
            task.update(package_dir=str(destination), manifest_sha256=sha(destination/'manifest.json'), stage='validate')
        elif stage == 'validate':
            packet = self._packet(task)
            if packet.get('compatibility_notes'):
                self.store.event(task['task_id'],'delivery_notes',dict(notes=packet['compatibility_notes']))
            task.update(manifest_sha256=packet['manifest_sha256'],stage='ingest',progress_level=6)
        elif stage in RAG_STAGES:
            if stage == 'reconcile':
                return self._reconcile(task)
            packet = self._packet(task)
            if stage=='activate' and self._hold_unmapped_targets(task,packet):return task
            receipts = self._receipts(task)
            publisher = self._publisher()
            publisher.on_change = lambda value: self._save_receipts(task, value)
            if stage == 'ingest':
                task.update(route_codes=[r['code'] for r in packet['routes']],
                            pending_routes=packet.get('pending_routes',[]),stage='ingest_target')
            elif stage == 'verify':
                publisher.verify(task, packet, receipts)
                task['stage'] = 'activate'
            else:
                journal=self.store.receipt(task['task_id'],task['revision'],'activation')
                if journal.get('status')=='committed':
                    task.update(stage='cleanup',completed_at=time.time())
                    return task
                previous_receipts=[];previous_versions=[]
                # 仅恢复切换前实际活动的版本，绝不启用失败/未验证的历史修订。
                for active in self.store.active_versions_for(task['task_id']):
                    if active['task_id']==task['task_id'] and active['revision']==task['revision']:continue
                    old_receipts=self.store.target_receipts(active['task_id'],active['revision'])
                    if old_receipts:
                        previous_receipts.append(old_receipts)
                        previous_versions.append(dict(**active,targets=old_receipts))
                journal=dict(status='pending',previous=previous_receipts,previous_versions=previous_versions,current=receipts)
                self.store.receipt(task['task_id'],task['revision'],'activation',journal)
                try:
                    publisher.set_enabled(receipts, True)
                    publisher.verify_retrieval(task, packet, receipts)
                    if not self.store.is_current(task['task_id']):
                        raise StaleSourceVersion('source changed during activation')
                    # 源版本检查和本地激活之间的远端调用也必须纳入补偿范围。
                    for old_receipts in previous_receipts:
                        publisher.set_enabled(old_receipts, False)
                    self.store.mark_active(task['task_id'],activation_revision=task['revision'])
                except Exception:
                    self._rollback_activation(task,journal,publisher)
                    raise
                task.update(stage='cleanup', completed_at=time.time())
            self._save_receipts(task, receipts)
        elif stage == 'cleanup':
            if self.config.cleanup:
                work = self.config.root/'work'/task['task_id']
                if work.exists():
                    if not work.resolve().is_relative_to((self.config.root/'work').resolve()):
                        raise ValueError('cleanup outside owned work directory')
                    shutil.rmtree(work)
            if task.get('failure_packet'):
                packets=(Path(self.config.handoff_root)/'failed manuscript'/task['task_id']).glob('*/failure.json')
                for packet in packets:
                    if load(packet).get('source_sha256')==task['source_sha256']:
                        save(packet.parent/'RESOLVED.json', dict(task_id=task['task_id'],
                             revision=task['revision'], manifest_sha256=task['manifest_sha256'], at=time.time()))
            task['stage'] = 'complete'
        return task

    def _hold_unmapped_targets(self,task,packet):
        pending=packet.get('pending_routes',[])
        if not pending:return False
        task.update(stage='parked',error_kind='delivery',delivery_check='wrong_library',pending_routes=pending,
                    error='selected specialties have no valid target mapping',alert='target_mapping_pending',due=0)
        self.store.event(task['task_id'],'targets_held',dict(pending_routes=pending,revision=task['revision']))
        return True

    def _reconcile(self, task):
        """旧成功回执必须与实际上传字节和远端切片吻合，不能只查状态。"""
        folder=Path(task['legacy_package_dir'])
        manifest=load(folder/'manifest.json')
        if sha(folder/'manifest.json')!=task['adopted_manifest_sha256']:
            raise PackageError('legacy published revision changed')
        for row in manifest.get('files',[]):
            if not isinstance(row,dict) or not row.get('sha256'):
                raise PackageError('legacy manifest lacks verifiable file hash')
            if sha(inside(folder,row['path']))!=row['sha256']:
                raise PackageError('legacy file hash mismatch')
        receipt=load(task['legacy_receipt'])
        if receipt.get('manifest_sha256')!=task['adopted_manifest_sha256']:
            task.update(stage='round3')
            return task
        publisher=self._publisher()
        profile=load(folder/'profile.json'); assets=load(folder/'assets.json')
        routes=profile.get('routing_candidates',[])
        mapped={r['code']:r for r in self.catalog['majors']}
        converted={}
        confirmed=True
        for route in routes:
            code=route['code']
            target=receipt.get('targets',{}).get(code,{})
            body=target.get('body',{})
            ds=mapped[code]['paper_dataset_id']; did=body.get('document_id')
            if not did:
                confirmed=False; break
            docs=publisher.documents(ds,id=did)
            if len(docs)!=1:
                confirmed=False; break
            meta=docs[0].get('meta_fields') or {}
            if (meta.get('trial_source_sha256')!=task['source_sha256'] or
                meta.get('trial_profile_sha256')!=sha(folder/'profile.json') or
                meta.get('trial_tree_sha256')!=sha(folder/'tree.json') or
                publisher._download(ds,did)!=(folder/'article.redacted.md').read_bytes()):
                confirmed=False; break
            chunks=publisher.chunks(ds,did)
            expected=''.join((folder/'article.redacted.md').read_text(encoding='utf-8-sig').split())
            observed=''.join(''.join(c.get('content','').split()) for c in chunks)
            if not chunks or observed!=expected:
                confirmed=False; break
            converted[code]=dict(body=dict(dataset_id=ds,document_id=did),verified=True,legacy=True)
            for a in assets.get('items',[]):
                if a.get('kind')!='image' or a.get('file') not in manifest.get('publication_files',[]):
                    continue
                item=target.get('image_'+a['id'],{})
                ids=mapped[code]['image_dataset_id']; image_id=item.get('document_id')
                if not image_id or hashlib.sha256(publisher._download(ids,image_id)).hexdigest()!=a['sha256']:
                    confirmed=False; break
                if not publisher.chunks(ids,image_id):
                    confirmed=False; break
                converted[code].setdefault('legacy_images',[]).append(dict(dataset_id=ids,document_id=image_id))
        if confirmed and routes:
            question=profile.get('article',{}).get('title') or '研究方法'
            for code, target in converted.items():
                obj=target['body']
                data=publisher.client.request('POST','/retrieval',dict(question=question,dataset_ids=[obj['dataset_id']],
                    document_ids=[obj['document_id']],similarity_threshold=0.0,vector_similarity_weight=1.0,page_size=10))
                if not any(c.get('document_id',c.get('doc_id'))==obj['document_id'] for c in data.get('chunks',[])):
                    confirmed=False; break
        if confirmed and routes:
            self._save_receipts(task,converted)
            self.store.mark_active(task['task_id'])
            task.update(stage='complete',adopted_verified=True,completed_at=time.time(),
                        package_dir=str(folder),manifest_sha256=task['adopted_manifest_sha256'])
        else:
            task.update(stage='round3')
        return task

    def _artifact_fingerprint(self, task):
        folder = task.get('draft_dir') or task.get('repair_input_dir') or task.get('package_dir')
        if not folder or not Path(folder).is_dir():
            return 'no-artifacts'
        values = [(p.relative_to(folder).as_posix(), sha(p)) for p in Path(folder).rglob('*')
                  if p.is_file() and p.name in PUBLIC_NAMES and p.name not in ('review.json', 'corrections.json')]
        return hashlib.sha256(json.dumps(sorted(values)).encode()).hexdigest()

    def business_stage(self,task):
        valid=set((*LLM_STAGES,*RAG_STAGES,*LOCAL_STAGES))-{'repair'}
        if task.get('failed_stage') in valid:return task['failed_stage']
        packets=list((Path(self.config.handoff_root)/'failed manuscript'/task['task_id']).glob('*/failure.json'))
        if task.get('failure_packet'):packets.append(Path(task['failure_packet'])/'failure.json')
        for packet in sorted(set(packets),key=lambda p:p.parent.name):
            try:details=load(packet)
            except (OSError,ValueError):continue
            if not isinstance(details,dict):continue
            if details.get('source_sha256') not in (None,task['source_sha256']):continue
            origin=details.get('business_stage') or details.get('stage')
            if origin in valid:return origin
        return 'round1' # 未知来源保守补做 round2/round3，不能直接发布。

    def _failure(self, task, stage, exc):
        used=max(delivery_correction_count(self.config,task),getattr(exc,'correction_count',0) or 0)
        task['correction_count']=used
        check=getattr(exc,'check',None)
        if check:task['delivery_check']=check
        if check in ('wrong_library','identity'):
            task.update(stage='parked',error_kind='delivery',error=str(exc),resume_stage=stage,due=0)
            return task
        if isinstance(exc,SyntaxRepairExhausted):
            task.update(stage='parked',error_kind='content',error=str(exc),resume_stage=stage,
                        failed_stage=self.business_stage(task) if stage=='repair' else stage,
                        rules_fingerprint=rules_fingerprint(self.config),due=0)
            return task
        is_content = isinstance(exc, PackageError)
        is_infra=is_infrastructure(exc)
        if not is_content and not is_infra:
            task.update(controller_fields(self.config,task,stage,exc))
            return task
        if is_infra:
            # SDK/RAG 异常可能包含密钥或内部诊断：公开台账仅保留类型。
            count = task.get('retry_count', 0) + 1
            started=task.get('infra_started_at') or time.time()
            exhausted=count>=self.config.max_infrastructure_retries or time.time()-started>=self.config.max_infrastructure_seconds
            task.update(error_kind='infrastructure', error=type(exc).__name__, retry_count=count,
                        infra_started_at=started,due=time.time()+min(900,self.config.retry_seconds*2**min(count-1,6)))
            if exhausted:task.update(stage='parked',alert='infrastructure_retry_budget_exhausted')
            return task
        if used>=MAX_DELIVERY_CORRECTIONS:
            task.update(stage='parked',error_kind='content',error=str(exc)[:350],resume_stage=stage,
                        failed_stage=self.business_stage(task) if stage=='repair' else stage,
                        alert='delivery_correction_exhausted',due=0)
            return task
        reason = str(exc)[:350]
        artifact = self._artifact_fingerprint(task)
        category=re.sub(r'(line|column) \d+',r'\1 #',reason)
        business_stage=self.business_stage(task) if stage=='repair' else stage
        fingerprint=hashlib.sha256((business_stage+'|content|'+category).encode()).hexdigest()
        stagnant = task.get('stagnant_count', 0)
        # 改了字节仍重复同一故障不算有效进展；以验证通过的业务阶段为准。
        progress=task.get('progress_level',0)
        stagnant = stagnant+1 if (fingerprint == task.get('error_fingerprint') and
                                  progress == task.get('error_progress_level',0)) else 0
        previous_revision = task['revision']
        failure = Path(self.config.handoff_root)/'failed manuscript'/task['task_id']/f'r{previous_revision:03d}-{fingerprint[:10]}'
        failure.mkdir(parents=True, exist_ok=True)
        private_diagnostics=None
        if getattr(exc,'privacy_hits',None):
            private_diagnostics=self.config.root/'private/privacy'/task['task_id']/f'{time.time_ns()}.json'
            save(private_diagnostics,dict(hits=exc.privacy_hits))
        business_stage=self.business_stage(task) if stage=='repair' else stage
        save(failure/'failure.json', dict(origin='paperflow',business_stage=business_stage,task_id=task['task_id'], source_sha256=task['source_sha256'],
             original_path_private=task['source_path'], stage=stage, error=reason,
             artifact_directory=task.get('draft_dir') or task.get('package_dir'),
             private_diagnostics=str(private_diagnostics) if private_diagnostics else None,
             remote_receipts=self._receipts(task),delivery_check=check or 'missing_files',
             repair_policy='two_cumulative_targeted_delivery_corrections'))
        count=reserve_delivery_correction(self.config,task,business_stage)
        task.update(stage='repair', kind='repair',correction_count=count,
                    revision=previous_revision+1, repair_count=task.get('repair_count', 0)+1,
                    stagnant_count=stagnant, error_kind='content', error=reason,
                    error_fingerprint=fingerprint, artifact_fingerprint=artifact, failure_packet=str(failure),
                    repair_input_dir=task.get('draft_dir') or task.get('package_dir'), due=0,
                    rules_fingerprint=rules_fingerprint(self.config),
                    failed_stage=business_stage,error_progress_level=progress)
        task.pop('manifest_sha256', None)
        return task
