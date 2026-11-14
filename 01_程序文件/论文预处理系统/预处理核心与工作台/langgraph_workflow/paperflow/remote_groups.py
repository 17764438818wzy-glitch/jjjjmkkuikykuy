"""Manager-owned SDK groups. The existing scheduler still chooses all work.

An actor is one original HarnessProducer stage, including its existing narrow
syntax continuations. No new article queue, retry budget or content decisions.
"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import re
import shutil
import sqlite3
import threading
import time
from types import SimpleNamespace
import uuid

from .package import save,sha,load
from .remote_files import file_map,manifest,map_text,publish_output
from .remote_harness import RemoteHarness,current_permit,namespace
from .remote_transport import request,endpoint_identity
from .handoff_store import canonical
from .scheduling_policy import ActionDeferred
from .remote_diagnostics import describe,RemoteQueueBusy,record_control_failure
from .instrumentation import Recorder,safe_span,timed,best_effort,slot_event,new_recorder,using,current_recorder,safe_now


SCHEMA=(
    '''CREATE TABLE IF NOT EXISTS remote_sdk_groups (
       group_id TEXT PRIMARY KEY,epoch TEXT NOT NULL,identity_json TEXT NOT NULL,
       endpoint_json TEXT NOT NULL,state TEXT NOT NULL,proof TEXT)''',
    '''CREATE TABLE IF NOT EXISTS remote_sdk_actors (
       actor_id TEXT PRIMARY KEY,execution_id TEXT NOT NULL REFERENCES control_executions(execution_id),
       epoch TEXT NOT NULL,group_id TEXT NOT NULL REFERENCES remote_sdk_groups(group_id),
       state TEXT NOT NULL,proof TEXT)''',
)

# Native frames retain replay/close evidence until owner exit (128 actions).
# Reserve the existing initial call plus two syntax continuations for all eight
# admitted children, including calls whose start reply has not arrived yet.
GROUP_NEW_ACTOR_LIMIT=128-8*3


def recover_groups(guard):
    """Before Manager boot, retire old owners by exact remote container evidence."""
    ledger=guard.manager.ledger
    ledger._write(lambda c:[c.execute(sql) for sql in SCHEMA])
    for row in ledger.rows("SELECT * FROM remote_sdk_groups WHERE state!='exited'"):
        if row['endpoint_json']!=canonical(guard.endpoint):raise ValueError('old SDK group endpoint changed')
        identity=json.loads(row['identity_json'])
        if (identity.get('namespace')!=guard.namespace or identity.get('execution_id')!=row['group_id']
                or identity.get('epoch')!=row['epoch'] or identity.get('image')!=guard.settings['image']
                or identity.get('deployment_id')!=guard.settings['deployment_id']):
            raise ValueError('old SDK group ownership changed')
        proof=request(guard.settings,dict(identity,op='reap_execution',job_id='0'*32),timeout=12,queue_timeout=60)
        if (proof.get('all_exited') is not True or proof.get('execution_closed') is not True
                or proof.get('execution_id')!=row['group_id'] or proof.get('epoch')!=row['epoch']):
            raise ValueError('old SDK group exit unconfirmed')
        def commit(c):
            c.execute("UPDATE remote_sdk_groups SET state='exited',proof=? WHERE group_id=? AND epoch=?",
                      (canonical(proof),row['group_id'],row['epoch']))
            for actor in c.execute('SELECT actor_id,proof FROM remote_sdk_actors WHERE group_id=?',(row['group_id'],)).fetchall():
                previous=json.loads(actor['proof'] or '{}')
                c.execute("UPDATE remote_sdk_actors SET state='closed',proof=? WHERE actor_id=?",
                    (canonical(dict(previous,group_exited=proof)),actor['actor_id']))
        ledger._write(commit)


class Parent(RemoteHarness):
    def __init__(self,pool,identity,home):
        self.pool=pool
        client=SimpleNamespace(epoch=pool.manager.epoch,execution_id=identity,active_kind='llm',
            active_permit=dict(epoch=pool.manager.epoch,action_id=identity,deadline=time.monotonic()+86400),
            stopped=pool.manager.stopped,closed=pool.manager.closed)
        # This client keeps only the empty SDK owner alive. Every child prompt
        # separately requires a live article action in GroupPool.authorize().
        output=pool.config.root/'private/sdk-groups'/identity/'owner'
        output.mkdir(parents=True,exist_ok=True)
        tools=[dict(id=name,disabled=True) for name in ('tool-subagent','tool-subagent-fork',
            'tool-subagent-control','tool-subagent-list-agents','tool-bash','tool-pwsh','tool-workflow')]
        tools += [dict(id='sdk-jsonrpc-server',disabled=True),
            dict(insert=[dict(id='paperflow-group-native',name='/job/config/native-group.mjs')])]
        super().__init__(pool.config,SimpleNamespace(client=client),output,home,tools)
        self.task={'source_path':str(output/'owner.txt'),'source_sha256':'','task_id':identity}
        (output/'owner.txt').write_text('Empty SDK lifecycle owner. No business prompt.',encoding='utf-8')
        self.task['source_sha256']=sha(output/'owner.txt')
        self.init_lock=threading.Lock();self.members={};self.actions=0;self.retiring=False
    def _files(self):
        files=super()._files()
        value=load(self.folder/'job.json')
        value.update(mode='group',group_env=dict(PF_GROUP_ID=self.identity['execution_id'],PF_CONTROLLER_EPOCH=self.client.epoch))
        save(self.folder/'job.json',value)
        files['config/native-group.mjs']=Path(__file__).with_name('native_group.mjs')
        return files
    def ensure_started(self):
        with self.init_lock:
            if self.retiring:raise ActionDeferred('resource_busy')
            self.start()


class GroupPool:
    def __init__(self,manager,config):
        if type(config.remote_harness_groups) is not int or not 1<=config.remote_harness_groups<=8:
            raise ValueError('native SDK groups must be between one and eight')
        self.manager,self.config=manager,config;self.count=config.remote_harness_groups
        self.lock=threading.RLock();self.allocation_lock=threading.Lock();self.pending_groups=set()
        self.parents={};self.actors={};self.results={};self.closing=threading.Event()
        self.work=ThreadPoolExecutor(self.count*8,thread_name_prefix='sdk-child')
        self.cleanup=ThreadPoolExecutor(2,thread_name_prefix='sdk-group-exit')
        self.retiring={};self.error=None;self.error_events=[]
        manager.ledger._write(lambda c:[c.execute(sql) for sql in SCHEMA])
        manager.native_groups=self
        threading.Thread(target=self.watch,daemon=True,name='sdk-group-watch').start()
    def persist(self,operation):
        try:return self.manager.ledger._write(operation)
        except sqlite3.OperationalError as exc:
            if getattr(exc,'sqlite_errorcode',0)&255==sqlite3.SQLITE_BUSY:
                raise ActionDeferred('remote_group_persistence_busy') from None
            raise
    def authorize(self,execution_id,action_id):
        m=self.manager
        with m.gate_lock:
            ex=m.executions.get(execution_id);action=ex.active_action if ex else None
            if (m.stopped.is_set() or m.closed.is_set() or m.closing or not ex or not ex.eligible
                    or ex.kind!='graph' or not action or action['kind']!='llm' or action['action_id']!=action_id
                    or time.monotonic()>=action['deadline']):raise ValueError('native child admission closed')
            ticket=m.tickets.get(action_id)
            if not ticket or not m._valid(ticket['snapshot'],execution_id):raise ValueError('native child control generation expired')
            return dict(action)
    @contextmanager
    def allocator(self,execution_id,action_id):
        while not self.allocation_lock.acquire(timeout=.05):self.authorize(execution_id,action_id)
        try:
            self.authorize(execution_id,action_id);yield
        finally:self.allocation_lock.release()
    def handle(self,execution_id,operation,descriptor):
        if operation=='submit':
            if set(descriptor)!={'path','sha256','action_id'}:raise ValueError('invalid native submission')
            action=self.authorize(execution_id,descriptor['action_id'])
            with self.lock:
                known=self.results.get(action['action_id'])
                if known:
                    if known['digest']!=descriptor['sha256'] or known['execution_id']!=execution_id:raise ValueError('native action conflict')
                    return dict(accepted=True,action_id=action['action_id'])
            path=Path(descriptor['path']).resolve();base=(self.config.root/'private/sdk-submissions').resolve()
            if not path.is_relative_to(base) or path.is_symlink() or path.stat().st_size>2*1024**2:
                raise ValueError('native submission outside private scope')
            if sha(path)!=descriptor['sha256']:raise ValueError('native submission changed')
            value=load(path)
            if value['epoch']!=self.manager.epoch or value['execution_id']!=execution_id or value['action_id']!=action['action_id']:
                raise ValueError('native submission identity mismatch')
            actor_id=value['actor_id']
            if len(actor_id)!=32 or any(c not in '0123456789abcdef' for c in actor_id):raise ValueError('invalid actor identity')
            ex=self.manager.executions[execution_id]
            if value['task']['task_id']!=ex.key:raise ValueError('native task identity mismatch')
            output=Path(value['output']).resolve()
            if not output.is_relative_to((self.config.root/'work'/ex.key/'attempts').resolve()):
                raise ValueError('native output outside article attempts')
            stage=Path(value['stage']).resolve()
            if stage.parent!=output.parent or not stage.name.startswith(output.name+'.native-'):
                raise ValueError('native result staging scope invalid')
            if not Path(value['home']).resolve().is_relative_to((self.config.root/'private').resolve()):
                raise ValueError('native credentials outside private home')
            with self.lock:
                known=self.results.get(action['action_id'])
                if known:
                    if known['digest']!=descriptor['sha256'] or known['execution_id']!=execution_id:raise ValueError('native action conflict')
                    return dict(accepted=True,action_id=action['action_id'])
                actor=self.actors.get(actor_id)
                if actor and (actor['execution_id']!=execution_id or actor['closing'] or actor.get('future') and not actor['future'].done()):
                    raise ValueError('actor unavailable')
                if actor is None:
                    actor=dict(actor_id=actor_id,task_id=ex.key,started_at=time.time(),execution_id=execution_id,epoch=value['epoch'],closing=False,closed=False,parent=None,child_id=None,action_id=None,
                        remote_action_id=None,dispatched_action_id=None,lock=threading.Lock())
                    self.actors[actor_id]=actor
                actor['action_id']=action['action_id']
                record=dict(execution_id=execution_id,digest=descriptor['sha256'],actor_id=actor_id,state='pending')
                self.results[action['action_id']]=record
                actor['future']=self.work.submit(self.run,actor,value,action,record)
            path.unlink(missing_ok=True)
            return dict(accepted=True,action_id=action['action_id'])
        if operation=='poll':
            with self.lock:
                record=self.results.get(descriptor.get('action_id'))
                if not record or record['execution_id']!=execution_id:raise ValueError('unknown native result')
                return {k:v for k,v in record.items() if k not in ('digest','execution_id')}
        if operation=='close':
            actor_id=descriptor.get('actor_id')
            with self.lock:
                actor=self.actors.get(actor_id)
                if actor is None:return dict(closed=True,never_submitted=True)
                if actor['execution_id']!=execution_id:raise ValueError('actor close owner mismatch')
                actor['closing']=True
                if not actor.get('close_future'):actor['close_future']=self.cleanup.submit(self.close_actor,actor)
                return dict(closed=actor['closed'])
        raise ValueError('unknown native group operation')
    def allocate(self,actor,value):
        while True:
            self.authorize(actor['execution_id'],value['action_id'])
            # Only allocators serialize SQL registration. UI/status, pause,
            # teardown and completed actors never wait on this lock.
            with self.allocator(actor['execution_id'],value['action_id']):
                new_identity=None
                with self.lock:
                    if actor['closing']:raise ValueError('actor closing')
                    parent=actor['parent']
                    if parent:
                        if parent.retiring:raise ActionDeferred('resource_busy')
                        return parent
                    candidates=[p for p in self.parents.values() if not p.retiring and p.actions<GROUP_NEW_ACTOR_LIMIT and len(p.members)<8 and not p.stopped.is_set()]
                    if candidates:parent=min(candidates,key=lambda p:(len(p.members),p.identity['execution_id']))
                    elif len(self.parents)+len(self.pending_groups)<self.count:
                        new_identity=uuid.uuid4().hex;self.pending_groups.add(new_identity)
                    else:parent=None
                if new_identity:
                    try:
                        parent=Parent(self,new_identity,value['home'])
                        self.persist(lambda c:c.execute('INSERT INTO remote_sdk_groups VALUES(?,?,?,?,?,NULL)',
                            (new_identity,self.manager.epoch,canonical(parent._header('run')),canonical(endpoint_identity(self.config.remote_harness)),'owned')))
                        with self.lock:self.parents[new_identity]=parent
                    finally:
                        with self.lock:self.pending_groups.discard(new_identity)
                if parent:
                    context=dict(output=value.get('output'),source_sha256=value.get('task',{}).get('source_sha256'))
                    try:
                        def register(c):
                            changed=c.execute('''INSERT INTO remote_sdk_actors SELECT ?,?,?,?,?,?
                                FROM remote_sdk_groups WHERE group_id=? AND epoch=? AND state='owned' ''',
                                (actor['actor_id'],actor['execution_id'],actor['epoch'],parent.identity['execution_id'],'owned',canonical(dict(context=context)),
                                 parent.identity['execution_id'],actor['epoch'])).rowcount
                            if changed!=1:raise ActionDeferred('resource_busy')
                        self.persist(register)
                    except ActionDeferred:
                        with self.lock:
                            if not parent.started and not parent.members:self.retire(parent)
                        raise
                    with self.lock:
                        parent.members[actor['actor_id']]=actor;actor.update(parent=parent,registered=True)
                        slot_event(self.manager,'actor_slots','grant',actor['actor_id'])
                    self.authorize(actor['execution_id'],value['action_id'])
                    return parent
            if self.closing.wait(.05):raise ValueError('SDK group pool closed')
    def run(self,actor,value,action,record):
        metrics=new_recorder(factory=Recorder,epoch=self.manager.epoch)
        with using(metrics),safe_span(metrics,'stage',stage=value['task'].get('stage')):
            return self._run(actor,value,action,record)

    @staticmethod
    def terminal_timing():
        # Snapshot the still-open outer span before publishing any terminal.
        # Observational failure stays explicit and cannot replace the outcome.
        def snapshot():
            recorder=current_recorder()
            return recorder.partition(dict(recorder.stack[0],end=safe_now(recorder)))
        detail=best_effort(snapshot)
        return detail if isinstance(detail,dict) else dict(metrics_complete=False,reason='remote_timing_unavailable')

    def persist_retained(self,actor):
        # A transfer fact survives actor teardown; it grants no action or cache qualification.
        retained=dict(actor['retained_result'])
        def commit(c):
            row=c.execute('SELECT proof FROM remote_sdk_actors WHERE actor_id=? AND execution_id=? AND epoch=?',
                (actor['actor_id'],actor['execution_id'],actor['epoch'])).fetchone()
            if not row:raise ValueError('native retained result owner missing')
            previous=json.loads(row['proof'] or '{}')
            c.execute('UPDATE remote_sdk_actors SET proof=? WHERE actor_id=? AND execution_id=? AND epoch=?',
                (canonical(dict(previous,retained_result=retained)),actor['actor_id'],actor['execution_id'],actor['epoch']))
        self.persist(commit)

    def _run(self,actor,value,action,record):
        phase='allocate'
        try:
            with actor['lock']:
                with timed('resource_wait'):parent=self.allocate(actor,value)
                phase='initialize'
                with timed('preparation'):parent.ensure_started()
                phase='preparation'
                self.authorize(actor['execution_id'],action['action_id'])
                task=value['task'];source=Path(task['source_path'])
                if sha(source)!=task['source_sha256']:raise ValueError('source changed before native transfer')
                files={'input/original/'+source.name:source}
                if task.get('reading_dir'):files.update(file_map(task['reading_dir'],'input/reading'))
                files.update(file_map(value['output'],'work'))
                cancelled=lambda:self.manager.stopped.is_set() or actor['closing']
                phase='upload'
                with timed('upload'):
                    request(self.config.remote_harness,parent._header('update_child',actor_id=actor['actor_id'],files=manifest(files)),
                        files=files,timeout=120,queue_timeout=60,cancelled=cancelled)
                self.authorize(actor['execution_id'],action['action_id'])
                if actor['closing']:raise ValueError('actor close won dispatch')
                cwd='/job/output/actors/'+actor['actor_id']
                mapping={str(value['output']):cwd+'/work',str(source):'/job/input/actors/'+actor['actor_id']+'/original/'+source.name,
                    str(self.config.catalog_path):'/job/input/catalog.json'}
                if task.get('reading_dir'):mapping[str(task['reading_dir'])]='/job/input/actors/'+actor['actor_id']+'/reading'
                params=dict(epoch=self.manager.epoch,action_id=action['action_id'],prompt=map_text(value['prompt'],mapping),
                    seconds=min(self.config.model_timeout,action['deadline']-time.monotonic()))
                self.authorize(actor['execution_id'],action['action_id'])
                method='group/continue' if actor['child_id'] else 'group/start'
                params.update(child_id=actor['child_id']) if actor['child_id'] else params.update(cwd=cwd)
                # A lost start/wait reply is not proof that no model work ran.
                # Use the existing retained-result gate before any possible send;
                # scoped cancellation can then never become a blind graph retry.
                previous_retained=actor.get('retained_result')
                actor['retained_result']=dict(group_identity=parent.identity,actor_id=actor['actor_id'],action_id=action['action_id'],
                    output=value['output'],stage=value['stage'],source_sha256=task['source_sha256'],collection_pending=True,
                    task_id=task.get('task_id'),source_id=task.get('source_id'),revision=task.get('revision'),
                    cache_stage=Path(value['output']).name,
                    business_stage=task.get('failed_stage') if Path(value['output']).name=='repair' else Path(value['output']).name,
                    stage_input_sha256=task.get('stage_input_sha256'),finish_reason='unconfirmed')
                try:
                    self.persist_retained(actor)
                    self.authorize(actor['execution_id'],action['action_id'])
                    with self.lock:
                        if parent.retiring or actor['closing']:raise ValueError('group retired before dispatch')
                        phase='dispatch';actor['dispatched_action_id']=action['action_id'];parent.actions+=1
                except BaseException:
                    # Still under actor.lock, before _rpc: definitely not sent.
                    # Teardown's existing proof transaction restores the prior
                    # fact (or removes this unused intent), even after SQL busy.
                    actor['retained_result']=previous_retained
                    raise
                child=parent._rpc(method,**params);actor['remote_action_id']=action['action_id']
                if actor['child_id'] and child['child_id']!=actor['child_id']:raise ValueError('native child continuation mismatch')
                actor['child_id']=child['child_id']
                phase='wait'
                with timed('remote_runtime_or_wait'):result=parent._rpc('group/wait',epoch=self.manager.epoch,action_id=action['action_id'])
                if result['child_id']!=actor['child_id'] or result['action_id']!=action['action_id']:raise ValueError('native result identity mismatch')
                stage=Path(value['stage'])
                phase='collect'
                actor['retained_result']['finish_reason']=result['finish_reason']
                self.persist_retained(actor)
                # The model action is already complete. Capacity waits may
                # retry only this readback, within the original action budget.
                # Expiry stays retained/held, never a replayable graph retry.
                collection_cancelled=lambda:(cancelled() or self.closing.is_set()
                    or time.monotonic()>=action['deadline'])
                while True:
                    self.authorize(actor['execution_id'],action['action_id'])
                    if collection_cancelled():raise ValueError('remote result collection stopped')
                    remaining=action['deadline']-time.monotonic()
                    if remaining<=0:raise ValueError('remote result collection deadline expired')
                    queue_seconds=min(60,remaining/3)
                    try:
                        with timed('download'):
                            received=request(self.config.remote_harness,parent._header('collect_child',actor_id=actor['actor_id']),destination=stage,
                                timeout=min(120,remaining-queue_seconds),queue_timeout=queue_seconds,cancelled=collection_cancelled)
                        break
                    except RemoteQueueBusy:
                        if self.closing.wait(min(.25,max(0,action['deadline']-time.monotonic()))):
                            raise ValueError('group pool closed')
                if (received['actor_id']!=actor['actor_id'] or received.get('job_id')!=parent.identity.get('job_id')):
                    raise ValueError('native result files owner mismatch')
                # request() returns only after receive_archive has verified these hashes.
                # Persist the readback even if its original business permit just expired.
                items=received['files']
                actor['retained_result'].update(collection_pending=False,
                    files_manifest_sha256=hashlib.sha256(canonical(items).encode('utf-8')).hexdigest(),
                    files_count=len(items),files_bytes=sum(item['size'] for item in items))
                self.persist_retained(actor)
                self.authorize(actor['execution_id'],action['action_id'])
                detail=self.terminal_timing()
                with self.lock:record.update(state='done',result={k:result[k] for k in ('finish_reason','events')},
                    _remote_timing=detail)
        except (ActionDeferred,RemoteQueueBusy):
            detail=self.terminal_timing()
            with self.lock:
                if phase=='collect':record.update(state='error',error='retained_result',phase='collect',_remote_timing=detail)
                else:record.update(state='deferred',reason='resource_busy',_remote_timing=detail)
            parent=actor.get('parent')
            if parent and phase=='initialize' and not parent.started:
                with self.lock:self.retire(parent)
        except BaseException as exc:
            detail=self.terminal_timing()
            ex=getattr(self.manager,'executions',{}).get(actor['execution_id'])
            if self.manager.stopped.is_set() or actor['closing'] or (ex is not None and not ex.eligible):
                with self.lock:record.update(state='stopped',_remote_timing=detail)
                return
            with self.lock:
                record.update(state='error',error=type(exc).__name__,phase=phase,_remote_timing=detail)
                self.error_events.append(dict(phase=phase,detail=describe(exc),actor_id=actor['actor_id']))
                del self.error_events[:-32]
            # On uncertain start/transport state, retire the whole owner and
            # retain its files. Never issue the model prompt a second time.
            parent=actor.get('parent')
            if isinstance(exc,sqlite3.DatabaseError):
                self.manager.pause('native group ledger failure')
            elif parent and phase in ('initialize','dispatch','wait'):
                self.quarantine(parent,exc,phase)

    def quarantine(self,parent,exc,phase):
        # Reserve the whole failed owner until exact exit + durable proof. The
        # other owner retains its generation, permits, actors and result work.
        with self.lock:
            first=not getattr(parent,'fault',None)
            parent.retiring=True
            if first:parent.fault=dict(phase=phase,error=describe(exc),at=time.time())
            members=list(parent.members.values())
        if first:
            root=getattr(self.manager,'root',None)
            if root:
                exc.remote_context=dict(getattr(exc,'remote_context',{}),execution_id=parent.identity['execution_id'])
                record_control_failure(root,'native_group_'+phase,exc,epoch=self.manager.epoch)
        # No pool->manager gate lock inversion; cancellation cannot change the
        # proof/retained facts already written before dispatch or collection.
        for actor in members:
            execution_id=actor.get('execution_id')
            if execution_id in getattr(self.manager,'executions',{}):
                self.manager.cancel_unit(execution_id,'native SDK group quarantined')
        with self.lock:self.retire(parent)
    def close_actor(self,actor):
        # Cancel an admitted turn before waiting for its run/transfer thread.
        # The native close operation itself drains the Agent inbox and tools.
        parent=actor['parent'];proof=actor.get('close_proof')
        if (proof is None and parent and (actor['child_id'] or actor.get('dispatched_action_id'))
                and not parent.stopped.is_set() and not parent.retiring and not self.manager.stopped.is_set()):
            deadline=actor.setdefault('close_deadline',time.monotonic()+GroupProxy.shutdown_wait_seconds)
            try:
                remaining=deadline-time.monotonic()
                # Two cleanup workers may have queued this retry behind other
                # actors. Even after the budget, ask once for an already-ready
                # proof before escalating; queue time is not proof of failure.
                proof=parent._rpc('group/close',timeout=min(2,remaining) if remaining>0 else 2,epoch=self.manager.epoch,
                    action_id=actor.get('dispatched_action_id') or actor.get('remote_action_id') or actor['action_id'])
                if (actor['child_id'] and proof.get('child_id')!=actor['child_id']) or proof.get('child_closed') is not True:
                    raise ValueError('native child exit unknown')
                actor['child_id']=proof['child_id']
            except Exception as exc:
                proof=None  # A rejected child reply is never an exit proof.
                # closeChild caches its disposal task. A lost/late reply can be
                # confirmed for the same action without replaying model work.
                # Keep ownership until both exit and persistence are proven.
                if not self.manager.stopped.is_set():
                    actor['close_error']=dict(at=time.time(),actor_id=actor['actor_id'],
                        action_id=actor.get('dispatched_action_id'),error=describe(exc))
                    root=getattr(self.manager,'root',None)
                    if root:record_control_failure(root,'native_child_close',exc,epoch=self.manager.epoch)
                    if isinstance(exc,TimeoutError) and time.monotonic()<deadline and not parent.stopped.is_set():
                        raise ActionDeferred('remote_child_exit_pending') from None
                    if not self.manager.stopped.is_set():self.quarantine(parent,exc,'child_close')
                # A concurrent operator/protective pause already owns shutdown.
                # Never overwrite its first reason with normal RPC cancellation.
                if not parent.retiring:parent.close()
        elif parent and not parent.started and not parent.stopped.is_set():
            # No article can have started in an uninitialized parent. Interrupt
            # that shared bootstrap instead of waiting behind its full timeout.
            with self.lock:self.retire(parent)
        with actor['lock']:
            parent=actor['parent']
            if not parent:proof=dict(no_remote_child=True)
            elif parent.proof:proof=dict(group_exited=parent.proof)
            elif not actor.get('dispatched_action_id'):proof=dict(no_remote_child=True)
            elif proof is None:
                if not parent.retiring:parent.close()
                if not parent.proof:raise ActionDeferred('remote_group_exit_pending')
                proof=dict(group_exited=parent.proof)
            if actor.get('retained_result'):proof=dict(proof,retained_result=actor['retained_result'])
            if actor.get('close_error'):proof=dict(proof,close_error=actor['close_error'])
            actor['close_proof']=proof
            def commit(c):
                row=c.execute('SELECT proof FROM remote_sdk_actors WHERE actor_id=? AND epoch=?',(actor['actor_id'],actor['epoch'])).fetchone()
                previous=json.loads(row['proof'] or '{}') if row else {}
                if actor.get('retained_result') is None:previous.pop('retained_result',None)
                changed=c.execute("UPDATE remote_sdk_actors SET state='closed',proof=? WHERE actor_id=? AND epoch=?",
                    (canonical(dict(previous,**proof)),actor['actor_id'],actor['epoch'])).rowcount
                if changed!=1 and (actor.get('registered') or actor.get('dispatched_action_id')):raise ValueError('native actor registration missing')
            self.persist(commit)
            with self.lock:
                actor['closed']=True
                if actor.get('registered') and not actor.get('_metrics_slot_released'):
                    slot_event(self.manager,'actor_slots','release',actor['actor_id']);actor['_metrics_slot_released']=True
                if parent:parent.members.pop(actor['actor_id'],None)
                for key,record in list(self.results.items()):
                    if record['actor_id']==actor['actor_id']:self.results.pop(key,None)
                self.actors.pop(actor['actor_id'],None)
                if parent and (parent.actions>=GROUP_NEW_ACTOR_LIMIT or parent.retiring or not parent.started) and not parent.members:self.retire(parent)
            self.save_view(actor,exited=True)
            return proof
    def save_view(self,actor,*,exited=False):
        if not actor.get('task_id'):return
        parent=actor.get('parent');record=getattr(parent,'record',{})
        value=dict(task_id=actor['task_id'],job_id=actor['actor_id'],execution_id=actor['execution_id'],epoch=actor['epoch'],
            execution_kind='native_child',group_id=parent.identity['execution_id'] if parent else None,child_id=actor.get('child_id'),
            host=self.config.remote_harness['host'],remote_pid=record.get('remote_pid'),container=record.get('container'),
            state='exited' if exited else 'running' if actor.get('future') and not actor['future'].done() else 'idle',
            exit_confirmed=exited,started_at=actor.get('started_at'),last_remote_heartbeat=record.get('last_remote_heartbeat'))
        if exited:value['ended_at']=time.time()
        try:save(self.config.root/'private/remote-executions'/actor['actor_id']/'status.json',value)
        except OSError:pass
    def retire(self,parent):
        identity=parent.identity['execution_id'];parent.retiring=True
        if identity not in self.retiring and time.monotonic()>=getattr(parent,'retry_at',0):
            self.retiring[identity]=self.cleanup.submit(self.close_parent,parent)
    def close_parent(self,parent):
        if not parent.proof:parent.close()
        if not parent.proof:
            parent.retry_at=time.monotonic()+2
            raise ActionDeferred('remote_group_exit_pending')
        with self.lock:
            actors={identity:actor for identity,actor in self.actors.items() if actor['parent'] is parent}
            # Remote exit can race with consuming a completed reply locally.
            if any(actor.get('future') is not None and not actor['future'].done() for actor in actors.values()):
                raise ActionDeferred('remote_group_result_pending')
            retained={identity:actor.get('retained_result') for identity,actor in actors.items()}
            close_errors={identity:actor.get('close_error') for identity,actor in actors.items()}
        def commit(c):
            c.execute("UPDATE remote_sdk_groups SET state='exited',proof=? WHERE group_id=? AND epoch=?",
                (canonical(parent.proof),parent.identity['execution_id'],self.manager.epoch))
            for actor in c.execute('SELECT actor_id,proof FROM remote_sdk_actors WHERE group_id=?',(parent.identity['execution_id'],)).fetchall():
                previous=json.loads(actor['proof'] or '{}')
                if actor['actor_id'] in retained:
                    if retained[actor['actor_id']]:previous['retained_result']=retained[actor['actor_id']]
                    else:previous.pop('retained_result',None)
                if close_errors.get(actor['actor_id']):previous['close_error']=close_errors[actor['actor_id']]
                c.execute("UPDATE remote_sdk_actors SET state='closed',proof=? WHERE actor_id=?",
                    (canonical(dict(previous,group_exited=parent.proof)),actor['actor_id']))
        self.persist(commit)
        with self.lock:
            self.parents.pop(parent.identity['execution_id'],None)
            closed=[]
            for identity,actor in list(self.actors.items()):
                if actor['parent'] is parent:
                    actor['closed']=True;self.actors.pop(identity,None);closed.append(actor)
                    if actor.get('registered') and not actor.get('_metrics_slot_released'):
                        slot_event(self.manager,'actor_slots','release',identity);actor['_metrics_slot_released']=True
        for actor in closed:self.save_view(actor,exited=True)
    def exit_proof(self,execution_id,epoch):
        rows=self.manager.ledger.rows('SELECT * FROM remote_sdk_actors WHERE execution_id=? AND epoch=?',(execution_id,epoch))
        with self.lock:
            pending=[actor for actor in self.actors.values() if actor['execution_id']==execution_id and actor['epoch']==epoch and not actor['closed']]
            if not rows and not pending:return None
            if pending:
                for actor in pending:
                    actor['closing']=True
                    if not actor.get('close_future'):actor['close_future']=self.cleanup.submit(self.close_actor,actor)
                return False
            for row in rows:
                actor=self.actors.get(row['actor_id'])
                if row['state']!='closed' and actor:
                    actor['closing']=True
                    if not actor.get('close_future'):actor['close_future']=self.cleanup.submit(self.close_actor,actor)
            if any(row['state']!='closed' for row in rows):return False
        return dict(execution_id=execution_id,epoch=epoch,execution_closed=True,all_exited=True,
            at=time.time(),native_children=[dict(actor_id=row['actor_id'],proof=json.loads(row['proof'])) for row in rows])
    def watch(self):
        while not self.closing.wait(.1):
            pause_reason=None;views=[];faults=[]
            try:
                with self.lock:
                    if self.manager.stopped.is_set():
                        for parent in list(self.parents.values()):self.retire(parent)
                    else:
                        for parent in list(self.parents.values()):
                            if parent.retiring:self.retire(parent)
                            elif parent.stopped.is_set():faults.append((parent,ConnectionError('native group channel closed'),'channel'))
                    for identity,future in list(self.retiring.items()):
                        if future.done():
                            self.retiring.pop(identity)
                            try:future.result()
                            except ActionDeferred:
                                if identity in self.parents:self.retire(self.parents[identity])
                            except Exception as exc:
                                # Shared persistence/identity defects still fail closed.
                                self.error='SDK group exit unconfirmed';pause_reason=self.error
                    for actor in list(self.actors.values()):
                        if time.monotonic()-actor.get('last_view',0)>=2:
                            actor['last_view']=time.monotonic();views.append(actor)
                        ex=self.manager.executions.get(actor['execution_id'])
                        if not ex or not ex.eligible:
                            actor['closing']=True
                            if not actor.get('close_future'):actor['close_future']=self.cleanup.submit(self.close_actor,actor)
                        future=actor.get('close_future')
                        if future and future.done() and future.exception():
                            if isinstance(future.exception(),ActionDeferred):
                                actor['close_future']=self.cleanup.submit(self.close_actor,actor);continue
                            exc=future.exception()
                            if isinstance(exc,sqlite3.DatabaseError):
                                self.error='SDK child ledger failure';pause_reason=self.error
                            elif actor['parent']:
                                faults.append((actor['parent'],exc,'child_close'))
                                actor['close_future']=None
                    if not self.parents and not self.retiring and not self.actors:self.error=None
                    for identity,record in list(self.results.items()):
                        ex=self.manager.executions.get(record['execution_id'])
                        if record['state']!='pending' and (not ex or not ex.eligible):self.results.pop(identity,None)
            except Exception:self.error='SDK group supervision failed';pause_reason=self.error
            # Manager status takes gate_lock before this pool lock. Never take
            # the opposite lock order while sending a protective pause.
            if pause_reason:self.manager.pause(pause_reason)
            for parent,exc,phase in faults:self.quarantine(parent,exc,phase)
            for actor in views:
                try:self.save_view(actor,exited=actor['closed'])
                except OSError:pass  # View failure cannot alter exit/admission evidence.
    def status(self):
        with self.lock:return dict(groups=len(self.parents),pending_groups=len(self.pending_groups),group_limit=self.count,children=len(self.actors),child_limit=self.count*8,
            pending_group_exits=sum(p.retiring for p in self.parents.values()),error=self.error,
            quarantined_groups=sum(bool(getattr(p,'fault',None)) for p in self.parents.values()))
    def ready(self):
        with self.lock:return not self.parents and not self.pending_groups and not self.actors and not self.retiring and not self.error
    def close(self):
        self.closing.set();self.work.shutdown(wait=False,cancel_futures=True);self.cleanup.shutdown(wait=False,cancel_futures=False)


class GroupProxy:
    shutdown_wait_seconds=10
    def __init__(self,config,control,output,home,tools):
        self.config,self.client,self.output,self.home,self.tools=config,control.client,Path(output),Path(home),tools
        self.actor_id=uuid.uuid4().hex;self.task=None;self.started=False;self.closed=threading.Event()
    def configure(self,task):
        if not re.fullmatch('[A-Za-z0-9_-]{1,128}',task['task_id']):raise ValueError('invalid remote task identity')
        self.task=task
        value=dict(task_id=task['task_id'],job_id=self.actor_id,execution_id=self.client.execution_id,epoch=self.client.epoch,
            execution_kind='native_child',state='prepared_local',exit_confirmed=False,started_at=time.time(),host=self.config.remote_harness['host'])
        save(self.config.root/'private/remote-executions'/self.actor_id/'status.json',value)
        save(self.config.root/'private/remote-current'/(task['task_id']+'.json'),value)
    def start(self):
        current_permit(self.client);self.started=True
    def run(self,prompt,*,session_id):
        permit=current_permit(self.client);self.start()
        folder=self.config.root/'private/sdk-submissions'/self.client.execution_id;folder.mkdir(parents=True,exist_ok=True)
        path=folder/(permit['action_id']+'.json')
        stage=self.output.with_name(self.output.name+'.native-'+uuid.uuid4().hex)
        body=dict(epoch=self.client.epoch,execution_id=self.client.execution_id,action_id=permit['action_id'],actor_id=self.actor_id,
            output=str(self.output),home=str(self.home),task=self.task,prompt=prompt,stage=str(stage),tools=self.tools)
        save(path,body)
        try:
            self.client.rpc('remote_group',dict(operation='submit',descriptor=dict(path=str(path),sha256=sha(path),action_id=permit['action_id'])))
            while True:
                current_permit(self.client)
                if self.closed.is_set():raise ValueError('native actor closed')
                value=self.client.rpc('remote_group',dict(operation='poll',descriptor=dict(action_id=permit['action_id'])))
                if value['state'] in ('error','deferred','stopped','done'):
                    metrics=getattr(self.client,'metrics',None)
                    if metrics is not None:
                        detail=value.get('_remote_timing')
                        if not isinstance(detail,dict):detail=dict(metrics_complete=False,reason='remote_timing_unavailable')
                        if detail.get('metrics_complete') is not True:best_effort(setattr,metrics,'complete',False)
                        best_effort(metrics.observations.append,dict(source='manager_group_action',timing=detail))
                if value['state']=='error':
                    if value.get('phase')=='collect':raise ValueError('remote result retained; result transfer needs reconciliation')
                    raise ConnectionError('native child operation failed: '+value['error'])
                if value['state']=='deferred':raise ActionDeferred(value['reason'])
                if value['state']=='stopped':raise ValueError('native child stopped')
                if value['state']=='done':
                    current_permit(self.client);publish_output(stage,self.output)
                    return SimpleNamespace(**value['result'])
                self.closed.wait(.25)
        finally:
            path.unlink(missing_ok=True)
            # A late collector still owns stage if cancellation won; no model
            # replay or removal of its pending remote artifact occurs here.
    def close(self):
        self.closed.set()
        if self.client.closed.is_set():return
        deadline=time.monotonic()+8
        while time.monotonic()<deadline:
            value=self.client.rpc('remote_group',dict(operation='close',descriptor=dict(actor_id=self.actor_id)))
            if value['closed']:return
            time.sleep(.05)

