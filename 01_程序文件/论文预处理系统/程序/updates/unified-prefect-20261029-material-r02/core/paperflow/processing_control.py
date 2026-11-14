"""Narrow graph publication protocol on the existing local controller."""
from pathlib import Path
import json
import threading
import time

from .handoff import PackageHandoff
from .handoff_store import canonical
from .package import load, sha
from .phase_cache import (CACHE_STAGES, input_fingerprint, prepare_cache, publish_cache,
                          read_cache, verify_entry)
from .instrumentation import timed


class ProcessingStopped(RuntimeError):
    """Leave the pending LangGraph node intact; never classify this as content repair."""


class GraphActions:
    def __init__(self, client, config):
        self.client, self.config = client, config
        self.lock = threading.Lock()
        self.cancellation = None
        client.on_cancel = self.cancel

    def cancel(self):
        with self.lock:
            if self.cancellation:self.cancellation.request('controller pause/disconnect')

    def bind(self, cancellation):
        with self.lock:
            self.cancellation = cancellation
            if self.client.stopped.is_set() or self.client.closed.is_set():cancellation.request('already stopped')

    def unbind(self, cancellation):
        with self.lock:
            if self.cancellation is cancellation:self.cancellation = None

    def run(self, kind, resource, function, *, total_timeout):
        result = []
        requested = time.perf_counter()
        def call(cancel):
            if cancel.is_set():raise ProcessingStopped('processing stopped')
            entered = time.perf_counter()
            operation=resource.get('operation','')
            label=('preparation' if operation.startswith(('cache_lookup:','prepare:','harness_setup:')) else
                'local_check' if operation.startswith(('syntax_check:','validate:')) else
                'remote_runtime_or_wait' if kind=='llm' else 'other_processing')
            with timed(label):result.append(function(cancel))
            # RPC records a small fact, never the manuscript or an SDK object.
            fact = dict(completed=not cancel.is_set(), operation=resource.get('operation'))
            if kind=='local' and resource.get('operation') in ('seal','handoff_commit'):
                value=result[0]
                if isinstance(value,dict) and value.get('embedding_qualification'):
                    fact['embedding_qualification']=value['embedding_qualification']
            if kind=='local' and resource.get('operation') in ('seal','seal_attachment'):
                fact.update(elapsed_seconds=time.perf_counter()-entered,
                            admission_seconds=entered-requested)
            return fact
        try:
            self.client.perform(kind, resource, call, total_timeout=total_timeout)
        except Exception as exc:
            if self.client.stopped.is_set() or self.client.closed.is_set():
                raise ProcessingStopped('controller stopped or disconnected') from exc
            raise
        if self.client.stopped.is_set() or self.client.closed.is_set():raise ProcessingStopped('processing stopped')
        return result[0]

    def commit_cache(self, task, stage, fingerprint, result):
        manifest=self.run('local',dict(operation='prepare_cache'),
            lambda cancel:prepare_cache(self.config,task,stage,fingerprint,result),total_timeout=120)
        def commit(cancel):
            self.client.rpc('graph_commit',dict(operation='cache_commit',descriptor=dict(
                manifest=str(manifest),sha256=sha(manifest))))
            cached=read_cache(self.config,task,stage,fingerprint)
            if cached is None:raise ValueError('qualified cache unavailable')
            return cached
        return self.run('local',dict(operation='cache_commit'),commit,total_timeout=120)

    def handoff(self, fingerprint):
        return self.run('local',dict(operation='handoff_commit'),
            lambda cancel:self.client.rpc('graph_commit',dict(operation='handoff_commit',
                descriptor=dict(fingerprint=fingerprint))),total_timeout=120)

    def attachment_handoff(self,root):
        return self.run('local',dict(operation='attachment_handoff'),lambda cancel:self.client.rpc('graph_commit',dict(
            operation='attachment_handoff',descriptor=dict(path=str(root),sha256=sha(Path(root)/'manifest.json')))),total_timeout=120)

    def correction_count(self):
        return self.run('local',dict(operation='correction_count'),lambda cancel:self.client.rpc(
            'graph_commit',dict(operation='correction_count',descriptor={})),total_timeout=120)['used']

    def hold_repair(self):
        return self.run('local',dict(operation='repair_hold'),lambda cancel:self.client.rpc(
            'graph_commit',dict(operation='repair_hold',descriptor={})),total_timeout=120)

    def reserve_correction(self,stage,fingerprint,ordinal):
        result=self.run('local',dict(operation='correction_reserve'),lambda cancel:self.client.rpc(
            'graph_commit',dict(operation='correction_reserve',descriptor=dict(stage=stage,fingerprint=fingerprint,ordinal=ordinal))),total_timeout=120)
        if result.get('exhausted'):
            from .producer import SyntaxRepairExhausted
            raise SyntaxRepairExhausted('two cumulative corrections exhausted')
        return result['used']


class ProcessingCommits:
    """Manager owns completion qualification and cache/desired publication.

    Final qualification (only memory under gate_lock) precedes persistence. A pause
    that wins rejects and rolls back; a qualification that wins saves an already
    verified complete fact. No database, file I/O or pipe writes hold gate_lock.
    """
    def __init__(self, manager, config):
        self.manager, self.config = manager, config
        config.processing_ledger_path=str(manager.store.path)
        from .embedding_admission import admission_from_config,bind_manager_counter
        admission=bind_manager_counter(admission_from_config(config),manager,config)
        self.handoff_service=PackageHandoff(manager.store,config.handoff_root,admission=admission)
        from .attachments import AttachmentLedger
        self.attachments=AttachmentLedger(manager.store)
        from .repair_control import RepairControl
        self.repairs=RepairControl(self)
        manager.processing=self
        if config.remote_harness and config.remote_harness_groups:
            from .remote_groups import GroupPool
            GroupPool(manager,config)

    def guard(self, execution_id, operation):
        m=self.manager
        with m.gate_lock:
            ex=m.executions.get(execution_id)
            action=ex.active_action if ex else None
            if not ex or ex.kind!='graph' or not action or action['kind']!='local':
                raise ValueError('graph publication needs active local action')
            ticket=m.tickets[action['action_id']]
            if ticket['request']['resource'] != dict(operation=operation) or ticket.get('qualified'):
                raise ValueError('publication action mismatch or consumed')
            snapshot=ticket['snapshot']
            if not m._valid(snapshot,execution_id) or time.monotonic()>=action['deadline']:
                raise ValueError('publication qualification lost')
        return ex,action,ticket

    def qualify(self, execution_id, action, ticket, targets=None):
        m=self.manager
        with m.gate_lock:
            ex=m.executions[execution_id]
            if (not m._valid(ticket['snapshot'],execution_id) or ex.active_action is not action
                    or time.monotonic()>=action['deadline'] or ticket.get('qualified')
                    or any(m.scopes[s]['generation']!=g or m.scopes[s]['ready'] for s,g in (targets or {}).items())):
                raise ValueError('final publication qualification denied')
            ticket['qualified']=True
            m._event('graph_result_qualified',execution_id=execution_id,action_id=action['action_id'])
            return dict(epoch=m.epoch,execution_id=execution_id,action_id=action['action_id'],generation=m.generation)

    def _check(self,c,execution_id,ticket):
        self.manager.ledger.check(c,ticket['snapshot'],execution_id,'local',ticket['request']['expected_generation'])

    def target_scopes(self,intent):
        m=self.manager
        scopes={target['code']:'unit:'+canonical(['target',canonical([intent['identity']['source_id'],target['code']])])
                for target in intent['target_snapshot']['targets']}
        for scope in scopes.values():
            m.load_scope(scope)
        return scopes

    def handoff(self,execution_id,fingerprint):
        m=self.manager
        ex,action,ticket=self.guard(execution_id,'handoff_commit')
        h=self.handoff_service
        task=self.repairs.task(execution_id)
        thread=task.get('thread_id',ex.key)
        intent,validated,manifest,registered=h.verify_registration(thread,fingerprint)
        if intent['identity']['task_id']!=ex.key:raise ValueError('handoff belongs to another graph')
        if intent.get('repair_source')!=task.get('repair_source'):raise ValueError('repair source binding mismatch')
        if sha(task['source_path'])!=task['source_sha256']:raise ValueError('original source changed')
        scopes=self.target_scopes(intent)
        with m.gate_lock:
            self.guard(execution_id,'handoff_commit')
            if any(not m.scopes[s]['ready'] for s in scopes.values()):raise ValueError('target scope pending')
            before={s:m.scopes[s]['generation'] for s in scopes.values()}
            states={s:dict(m.scopes[s]['state']) for s in scopes.values()}
            for s,g in before.items():m.scopes[s].update(generation=g+1,ready=False)
            m._event('scopes_invalidated',execution_id=execution_id,scopes=list(before))
        aligned={}
        def write(c):
            self._check(c,execution_id,ticket)
            source=c.execute('SELECT task_id FROM sources WHERE path=?',
                             (task['source_path'],)).fetchone()
            if not source or source['task_id']!=ex.key:raise ValueError('source superseded')
            result=h.ledger.register_sql(c,thread,fingerprint,validated,manifest,verified_registration=registered,
                frozen_codes={code for code,s in scopes.items() if states[s].get('frozen')})
            for code,s in scopes.items():
                expected=h.ledger._expectation(c,ex.key,code)
                state=dict(states[s],expected_generation=expected['generation'])
                changed=c.execute('UPDATE admission_scopes SET generation=?,change_id=?,state=? WHERE scope=? AND generation=?',
                    (before[s]+1,fingerprint,canonical(state),s,before[s])).rowcount
                if changed!=1:raise ValueError('target scope compare failed')
                aligned[s]=state
            return result
        result=m.ledger._write(write,still_valid=lambda:m._valid(ticket['snapshot'],execution_id),
            before_commit=lambda:self.qualify(execution_id,action,ticket,{s:g+1 for s,g in before.items()}))
        with m.gate_lock:
            for s,state in aligned.items():
                if m.scopes[s]['generation']==before[s]+1:m.scopes[s].update(state=state,ready=True)
        if validated.get('embedding_admission',{}).get('reason')=='mapping_pending':
            result=dict(result,admission_verified=False,admission_reason='mapping_pending')
        return result

    def cache(self,execution_id,descriptor):
        ex,action,ticket=self.guard(execution_id,'cache_commit')
        task=self.repairs.task(execution_id)
        path=Path(descriptor['manifest']).resolve()
        if not path.is_relative_to((self.config.root/'work'/ex.key/'attempts').resolve()):
            raise ValueError('phase manifest outside owned attempts')
        if sha(path)!=descriptor['sha256']:raise ValueError('phase manifest changed')
        entry=load(path);stage=entry['stage'];fingerprint=entry['fingerprint']
        if stage not in (*CACHE_STAGES,'repair'):raise ValueError('unknown phase')
        inputs=entry['input_task']
        if any(inputs[k]!=task[k] for k in ('task_id','source_id','source_sha256','source_path')):
            raise ValueError('phase input identity mismatch')
        for key in ('repair_source','repair_problem','failed_stage'):
            if inputs.get(key)!=task.get(key):raise ValueError('repair phase binding mismatch')
        if sha(task['source_path'])!=task['source_sha256'] or input_fingerprint(self.config,inputs,stage)!=fingerprint:
            raise ValueError('phase input changed')
        verify_entry(self.config,task,stage,fingerprint,entry)
        qualification=[]
        self.manager.ledger._write(lambda c:self._check(c,execution_id,ticket),
            still_valid=lambda:self.manager._valid(ticket['snapshot'],execution_id),
            before_commit=lambda:qualification.append(self.qualify(execution_id,action,ticket)))
        publish_cache(self.config,inputs,stage,fingerprint,entry,dict(qualification[0],manifest_sha256=descriptor['sha256']))
        return dict(qualified=True,fingerprint=fingerprint)

    def handle(self,execution_id,operation,descriptor):
        m=self.manager
        with m.gate_lock:
            self.guard(execution_id,operation)
            if execution_id in m.publications:raise ValueError('graph publication already pending')
            m.publications.add(execution_id)
        try:
            if operation in ('correction_count','correction_reserve','repair_hold'):
                return self.repairs.budget(execution_id,operation,descriptor)
            if operation=='cache_commit' and set(descriptor)=={'manifest','sha256'}:
                return self.cache(execution_id,descriptor)
            if operation=='handoff_commit' and set(descriptor)=={'fingerprint'}:
                return self.handoff(execution_id,descriptor['fingerprint'])
            if operation=='attachment_handoff' and set(descriptor)=={'path','sha256'}:
                from .attachments import verify_package
                task=self.repairs.task(execution_id)
                manifest=verify_package(self.config,task,descriptor['path'],descriptor['sha256'])
                ex,action,ticket=self.guard(execution_id,operation)
                def write(c):
                    self._check(c,execution_id,ticket)
                    return self.attachments.register_sql(c,task,descriptor['path'],manifest,self.config.attachment_targets,
                        descriptor['sha256'],self.config.rag_url)
                return m.ledger._write(write,still_valid=lambda:m._valid(ticket['snapshot'],execution_id),
                    before_commit=lambda:self.qualify(execution_id,action,ticket))
            raise ValueError('unknown graph publication operation')
        finally:
            with m.gate_lock:m.publications.discard(execution_id)
