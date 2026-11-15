"""Service incidents reuse the manager's invalidate / commit / align protocol."""
from collections import deque
import json
import threading
import time
import uuid
from urllib.parse import urlsplit

from .scheduling_policy import ActionDeferred
from .scheduling_store import SchedulingLedger


class ServiceControl:
    def __init__(self,manager,config):
        self.manager=manager;self.config=config
        self.ledger=SchedulingLedger(manager.store)
        self.services={};self.locks={kind:threading.Lock() for kind in ('llm','rag')}
        self.selected_tasks=None
        self.selection_deadline=None
        self.clock_lock=threading.Lock();self.clock_events=deque(maxlen=256);self.clock_lost=False
        endpoint=urlsplit(config.rag_url)
        if endpoint.username or endpoint.password or endpoint.query or endpoint.fragment:
            raise ValueError('RAG endpoint must not contain credentials or query')
        descriptors={'llm':dict(provider=config.provider,model=config.model,access=config.model_access_fingerprint),
                     'rag':dict(endpoint=config.rag_url.rstrip('/'))}
        if manager.scheduling is not None:raise ValueError('service controller already attached')
        with manager.gate_lock:
            if manager.starting or any(not ex.exit_saved for ex in manager.executions.values()):
                raise ValueError('attach service control before workers')
        self.ledger.boot(manager.epoch)
        for kind,descriptor in descriptors.items():
            row=self.ledger.ensure_service(kind,descriptor)
            # A previous epoch's probe is never evidence of recovery.
            if row['state']=='half_open':
                self.ledger._write(lambda c,sid=row['service_id']:c.execute(
                    "UPDATE scheduler_services SET state='open',probe_job=NULL WHERE service_id=?",(sid,)))
                row=self.ledger.service(row['service_id'])
            self.services[kind]=row
            manager.change_admission_state('service:'+kind,uuid.uuid4().hex,self.scope_state(row))
        manager.scheduling=self
        self.control_changed(manager.stopped.is_set())

    @staticmethod
    def scope_state(row):
        return dict(enabled=row['state']!='open',circuit=row['state'],service_id=row['service_id'])

    def control_changed(self,paused):
        # Called by immediate pause: no SQLite, pipe or waiting on a worker.
        with self.clock_lock:
            if len(self.clock_events)==self.clock_events.maxlen:self.clock_lost=True
            self.clock_events.append((time.time(),not paused))

    def flush_clock(self):
        with self.clock_lock:
            events=list(self.clock_events);self.clock_events.clear()
            lost=self.clock_lost;self.clock_lost=False
        if lost:
            self.ledger._write(lambda c:c.execute('UPDATE scheduler_sessions SET ended=last_seen WHERE ended IS NULL'))
            events=events[-1:]
        for timestamp,active in events:self.ledger.session(self.manager.epoch,active,now=timestamp)
        self.ledger.session(self.manager.epoch,not self.manager.stopped.is_set())

    def check_action(self,execution_id,kind):
        if not self.selection_valid(self.manager.executions.get(execution_id)):
            raise ActionDeferred('operator_scope_closed')
        lane=self.manager.resource_lane(kind)
        if lane=='local':return
        row=self.services[lane];ex=self.manager.executions.get(execution_id)
        if row['state']=='open':raise ActionDeferred('service_open')
        if not ex:raise ActionDeferred('execution_expired')
        if getattr(ex,'network_failures',0)>=3:raise ActionDeferred('short_attempts_exhausted')
        if row['state']=='half_open' and (not row['probe_job'] or getattr(ex,'scheduler_job',{}).get('job_id')!=row['probe_job']):
            raise ActionDeferred('single_probe_owned')

    def select_tasks(self,task_ids,deadline):
        with self.manager.gate_lock:
            if not self.manager.stopped.is_set() or self.manager.starting or any(not e.exit_saved for e in self.manager.executions.values()):
                raise ValueError('selection change requires paused and exited process trees')
            self.selected_tasks=frozenset(task_ids)
            self.selection_deadline=deadline

    def selection_valid(self,execution=None):
        if self.selected_tasks is None:return True
        if not self.selected_tasks or not self.selection_deadline or time.time()>=self.selection_deadline:return False
        return execution is None or execution.scheduler_job.get('selected_task_id') in self.selected_tasks

    def extend_selected_tasks(self,task_ids,deadline,epoch,generation):
        """Only extend the current explicitly authorized batch; never resume."""
        with self.manager.gate_lock:
            m=self.manager
            if (m.epoch!=epoch or m.generation!=generation or m.stopped.is_set() or m.pause_pending
                    or m.closing or self.selected_tasks is None or deadline!=self.selection_deadline
                    or not self.selection_valid() or not self.selected_tasks.issubset(task_ids)):
                raise ValueError('stale or non-monotonic selection extension')
            self.selected_tasks=frozenset(task_ids)

    def bind_selection_sql(self,c,binding):
        if self.selected_tasks is None:return
        from .scheduling_store import selected_task_sql
        task_id=selected_task_sql(c,binding) if binding else None
        if task_id not in self.selected_tasks or not self.selection_valid():
            raise ActionDeferred('outside_operator_selection')
        binding['selected_task_id']=task_id

    def refresh(self):
        # Called by the one scheduler after claiming a probe, before any process resumes.
        for kind in self.services:
            with self.locks[kind]:
                row=self.ledger.service(self.services[kind]['service_id'])
                with self.manager.gate_lock:self.services[kind]=row

    def abandon_probe(self,job_id):
        from .scheduling_policy import backoff
        for kind in self.services:
            with self.locks[kind]:
                row=self.services[kind]
                if row['state']!='half_open' or row['probe_job']!=job_id:continue
                updated={}
                def mutate(c):
                    c.execute("UPDATE scheduler_services SET state='open',probe_job=NULL,failures=failures+1,next_probe=? WHERE service_id=?",
                              (time.time()+backoff(row['failures']+1),row['service_id']))
                    updated.update(dict(c.execute('SELECT * FROM scheduler_services WHERE service_id=?',(row['service_id'],)).fetchone()))
                    return self.scope_state(updated)
                self.manager.change_admission_state('service:'+kind,uuid.uuid4().hex,dict(enabled=False),mutate=mutate)
                with self.manager.gate_lock:self.services[kind]=updated

    def record_result(self,execution_id,action_id,outcome,result,write_fact):
        m=self.manager
        with m.gate_lock:
            ex=m.executions.get(execution_id)
            action=ex.active_action if ex else None
            if not action or action['action_id']!=action_id:
                lane=None
            else:lane=m.resource_lane(action['kind'])
            failure=result.get('fault') if isinstance(result,dict) else None
            if lane in self.services and failure and failure.get('category')=='transient':
                ex.network_failures=getattr(ex,'network_failures',0)+1
        if failure and (failure.get('category')=='conflict' or failure.get('halt_required') is True):
            m.pause('remote integrity conflict' if failure.get('category')=='conflict' else 'ledger failure')
            m.ledger._write(write_fact)
            return
        if lane not in self.services:
            m.ledger._write(write_fact);return
        with self.locks[lane]:
            row=self.services[lane]
            relevant=(failure and (failure.get('category') in ('transient','configuration') or lane=='llm' and failure.get('model_rejection') is True)) or (outcome=='complete' and (row['state']=='half_open' or lane=='llm'))
            if not relevant:
                m.ledger._write(write_fact);return
            binding=getattr(ex,'scheduler_job',{})
            article=binding.get('article',ex.key)
            updated={}
            def mutate(c):
                write_fact(c)
                observed=self.observed_failure(failure)
                updated.update(self.ledger.observe_sql(c,row['service_id'],action_id,article,observed,
                    attempt_id=binding.get('attempt_id'),job_id=binding.get('job_id')))
                return self.scope_state(updated)
            if outcome=='complete' and row['state']=='closed':
                # A successful result breaks a refusal streak; it does not need
                # to invalidate permits or reopen any service.
                m.ledger._write(mutate)
                with m.gate_lock:self.services[lane]=updated
                return
            # Invalidates immediately, including while a database lock blocks fact saving.
            m.change_admission_state('service:'+lane,action_id,dict(enabled=False),mutate=mutate)
            with m.gate_lock:self.services[lane]=updated

    def record_execution(self,execution_id,data,write_fact):
        # Transport/configuration failures can precede any HTTP/LLM action.
        m=self.manager
        with m.gate_lock:
            ex=m.executions.get(execution_id)
            binding=getattr(ex,'scheduler_job',{})
        failure=data.get('fault') or {}
        if failure.get('category')=='conflict' or failure.get('halt_required') is True:
            m.pause('remote integrity conflict' if failure.get('category')=='conflict' else 'ledger failure')
            m.ledger._write(write_fact)
            return
        if not binding or not (failure.get('category') in ('transient','configuration') or failure.get('model_rejection') is True):
            m.ledger._write(write_fact);return
        if binding['kind']=='cleanup' and json.loads(binding['data']).get('resource_kind')=='local':
            m.ledger._write(write_fact);return
        lane='rag' if binding['kind'] in ('delivery','cleanup') else 'llm'
        with self.locks[lane]:
            updated={};row=self.services[lane]
            # The final worker report commonly repeats its last failed action.
            # That immutable observation already updated this circuit; only a
            # pre-action failure needs the execution-level fallback observation.
            from .handoff_store import canonical
            with m.store.db(timeout=.1) as c:
                recorded=c.execute('SELECT data FROM scheduler_observations o WHERE o.service_id=? '
                    'AND o.action_id=(SELECT action_id FROM control_actions WHERE execution_id=? '
                    "AND kind IN ('llm','rag_read','rag_write','cleanup_write') ORDER BY rowid DESC LIMIT 1)",
                    (row['service_id'],execution_id)).fetchone()
            previous=json.loads(recorded['data']) if recorded else None
            if isinstance(previous,dict) and canonical({k:v for k,v in previous.items() if not k.startswith('_control_')})==canonical(failure):
                m.ledger._write(write_fact);return
            def mutate(c):
                write_fact(c)
                updated.update(self.ledger.observe_sql(c,row['service_id'],'execution:'+execution_id,binding['article'],self.observed_failure(failure),
                    attempt_id=binding['attempt_id'],job_id=binding['job_id']))
                return self.scope_state(updated)
            m.change_admission_state('service:'+lane,'execution:'+execution_id,dict(enabled=False),mutate=mutate)
            with m.gate_lock:self.services[lane]=updated

    def observed_failure(self,failure):
        if failure and failure.get('model_rejection') is True:
            return dict(failure,_control_epoch=self.manager.epoch,_control_generation=self.manager.generation)
        return failure

    def rejection_waits_for_operator(self,row):
        if not row['event_id']:return False
        events=self.ledger.rows('SELECT category FROM scheduler_events WHERE event_id=?',(row['event_id'],))
        if not events or events[0]['category']!='request_rejected':return False
        observations=self.ledger.rows("SELECT data FROM scheduler_observations WHERE event_id=? "
            "AND category='request_rejected' ORDER BY rowid DESC LIMIT 1",(row['event_id'],))
        if not observations:return True
        failure=json.loads(observations[0]['data'])
        return (failure.get('_control_epoch')==self.manager.epoch and
                failure.get('_control_generation')==self.manager.generation)

    def open_due(self,*,now=None):
        now=time.time() if now is None else now
        if self.manager.stopped.is_set():return []
        opened=[]
        for kind in self.services:
            with self.locks[kind]:
                row=self.services[kind]
                if row['state']!='open' or row['next_probe']>now:continue
                if self.rejection_waits_for_operator(row):continue
                updated={}
                def mutate(c):
                    if self.manager.stopped.is_set():raise ActionDeferred('paused')
                    updated.update(self.ledger.open_probe_sql(c,row['service_id'],now=now))
                    return self.scope_state(updated)
                self.manager.change_admission_state('service:'+kind,uuid.uuid4().hex,dict(enabled=False),mutate=mutate)
                with self.manager.gate_lock:self.services[kind]=updated
                opened.append(kind)
        return opened
