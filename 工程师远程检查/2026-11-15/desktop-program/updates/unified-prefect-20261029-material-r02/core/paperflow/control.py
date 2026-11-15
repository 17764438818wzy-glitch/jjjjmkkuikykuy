"""Isolated single-manager control plane. No graph or delivery executor wiring."""
from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
import secrets
import sqlite3
import threading
import time
import uuid

from .control_store import ControlLedger
from .handoff_store import canonical
from .instance_lock import InstanceLock
from .winjob import Job, identity_alive, process_identity
from .scheduling_policy import ActionDeferred, fault
from .instrumentation import SlotLedger,ObservedHolds,best_effort


ALLOWED = {'graph': {'llm', 'local'}, 'delivery': {'rag_read', 'rag_write', 'local'},
           'prepared_local': {'local'},
           'cleanup': {'rag_read', 'cleanup_write', 'local'}}
REMOTE_WRITES = {'rag_write', 'cleanup_write'}


@dataclass
class Execution:
    execution_id: str
    kind: str
    key: str
    scope: str
    job: Job
    process: object
    deadline: float
    last_heartbeat: float
    identities: dict = field(default_factory=dict)
    peer: object = None
    worker_identity: dict | None = None
    worker_identity_saved: bool = False
    eligible: bool = True
    active_action: dict | None = None
    stop_at: float | None = None
    kill_at: float | None = None
    kill_sent: float | None = None
    exit_evidence: dict | None = None
    exit_saved: bool = False
    exit_times: dict = field(default_factory=dict)
    job_empty_at: float | None = None

    @property
    def pid(self):
        return self.process.pid


class Manager:
    def __init__(self, store, root, *, heartbeat_interval=5, lease_seconds=60,
                 grace_seconds=10, rag_grace_seconds=30, ticket_seconds=2,
                 resource_limits=None,max_live_units=8,history_limit=256,peer_limit=32,remote_harness=None):
        if not (0 < heartbeat_interval < lease_seconds and 0 < grace_seconds <= 10
                and 0 < rag_grace_seconds <= 30 and 0 < ticket_seconds <= 5):
            raise ValueError('invalid control timing bounds')
        self.store, self.root = store, Path(root).resolve()
        self.heartbeat_interval, self.lease_seconds = heartbeat_interval, lease_seconds
        self.grace_seconds, self.rag_grace_seconds, self.ticket_seconds = grace_seconds, rag_grace_seconds, ticket_seconds
        self.gate_lock = threading.RLock()
        self.launch_lock = threading.Lock()
        self.stopped = threading.Event()
        self.stopped.set()
        self.closed = threading.Event()
        self.wake = threading.Event()
        self.epoch = uuid.uuid4().hex
        self.authkey = secrets.token_bytes(32)
        self.executions, self.scopes, self.tickets = {}, {}, {}
        self.starting = {}
        self.publications = set()
        self.pending_control_changes = 0
        self.closing = False
        self.generation, self.saved_generation = 0, -1
        self.pause_pending = True
        self.reason = 'startup'
        self.resource_limits=dict(resource_limits or {'llm':2,'local':2,'rag':1})
        if (set(self.resource_limits)!={'llm','local','rag'} or any(type(v) is not int or v<1 for v in self.resource_limits.values())
                or any(type(v) is not int or v<1 for v in (max_live_units,history_limit,peer_limit))):
            raise ValueError('invalid control resource bounds')
        self.max_live_units,self.history_limit,self.peer_limit=max_live_units,history_limit,peer_limit
        self.unit_limits={}
        self.llm_operation_limits={}
        self.wait_resources=False
        self.scope_limit=max(32,4*history_limit,4*max_live_units)
        self.graph_slots=SlotLedger(max_live_units,epoch=self.epoch)
        self.llm_slots=SlotLedger(self.resource_limits['llm'],epoch=self.epoch)
        self.actor_slots=SlotLedger(64,epoch=self.epoch)
        self.resource_holds=ObservedHolds(self.llm_slots)
        self.events = []
        self.errors = []
        self.event_sequence=0
        self.scheduling=None
        self.started = False
        self.processing = None  # Optional stage-3 committer; no scheduler or remote executor.
        self.delivery = None  # Explicit stage-4 launcher installs the narrow specialty handler.
        self.cleanup = None
        self.remote_harness=remote_harness
        self.external=None
        self.native_groups=None
        self.embedding=None

    def _event(self, event, **fields):
        # Called inside gate_lock; proves the order of pause and final grants.
        self.event_sequence+=1
        self.events.append(dict(sequence=self.event_sequence, monotonic=time.monotonic(),
                                event=event, epoch=self.epoch, generation=self.generation, **fields))
        del self.events[:-self.history_limit]
        execution_id=fields.get('execution_id')
        if event in ('pause','resumed'):
            for ledger in (self.graph_slots,self.llm_slots,self.actor_slots):
                best_effort(ledger.observe,reasons=ledger.reasons,paused=event=='pause',supply_demand=ledger.supply)
        elif execution_id and event in ('worker_resumed','tree_exited'):
            best_effort(self.graph_slots.mark,execution_id,'register' if event=='worker_resumed' else 'exit_confirmed')

    def _error(self,error):
        self.errors.append(error)
        del self.errors[:-self.history_limit]

    def start(self):
        self.root.mkdir(parents=True, exist_ok=True)
        # The ledger is the ownership domain, even when callers choose different runtime folders.
        self.lock = InstanceLock(self.store.path.resolve().with_suffix(self.store.path.suffix + '.controller.lock'))
        self.lock.__enter__()
        try:
            self.ledger = ControlLedger(self.store)
            if self.remote_harness:
                from .remote_control import RemoteExecutionGuard
                self.external=RemoteExecutionGuard(self,self.remote_harness)
            elif self.ledger.rows("SELECT 1 FROM sqlite_master WHERE name='remote_harness_executions'") and self.ledger.rows(
                    "SELECT 1 FROM remote_harness_executions WHERE state!='exited' LIMIT 1"):
                raise ValueError('unresolved remote Harness owners require their remote configuration')
            self._recover()
            self.generation = self.ledger.boot(self.epoch)
            self.saved_generation, self.pause_pending = self.generation, False
            for row in self.ledger.rows("SELECT * FROM admission_scopes WHERE scope LIKE 'service:%'"):
                self.scopes[row['scope']] = dict(generation=row['generation'], state=json.loads(row['state']), ready=True)
            from .control_ipc import PipeServer
            self.server = PipeServer(self)
            self.server.start()
            self.started = True
            self.monitor = threading.Thread(target=self._supervise, daemon=True, name='paperflow-supervisor')
            self.writer = threading.Thread(target=self._persist, daemon=True, name='paperflow-control-writer')
            self.monitor.start()
            self.writer.start()
            return self
        except BaseException:
            if self.external:self.external.close()
            self.lock.__exit__()
            raise

    def _recover(self):
        for row in self.ledger.rows("SELECT * FROM control_executions WHERE state!='exited'"):
            identities = json.loads(row['members'])
            root = json.loads(row['root_identity'])
            if root and root not in identities:
                identities.append(root)
            job = Job.open(row['job_name'])
            started = time.monotonic()
            try:
                if job:
                    for pid in job.pids():
                        try:
                            identity = process_identity(pid)
                        except OSError as exc:
                            if exc.winerror == 87:
                                continue
                            raise
                        if identity not in identities:
                            identities.append(identity)
                    job.terminate()
                # All execution members were created inside this non-breakaway
                # Job. Its empty accounting proves those original processes
                # exited; querying their old PIDs can hit unrelated new owners.
                while (job.active_count() if job else any(identity_alive(item) for item in identities)):
                    if time.monotonic() - started >= 5:
                        raise RuntimeError('old process tree exit unconfirmed; takeover denied')
                    time.sleep(.02)
                remote=self.external.reap_sync(row['execution_id'],row['epoch']) if self.external and row['unit_kind']=='graph' else None
                self.ledger.confirm_exit(row['execution_id'], dict(reason='startup reconciliation',remote=remote,
                    old_epoch=row['epoch'], job_absent=job is None, active_processes=0,exit_basis='job_accounting' if job else 'recorded_identities',
                    identities=identities, all_recorded_processes_exited=True))
            finally:
                if job:
                    job.close()

    def resume(self, *, expected_generation=None):
        if self.native_groups and not self.native_groups.ready():raise ValueError('old SDK group exit pending')
        with self.gate_lock:
            if self.embedding is not None and self.embedding.maintenance_active():
                raise ValueError('paused counter maintenance session active')
            if expected_generation is not None and expected_generation != self.generation:
                raise ValueError('stale control generation')
            if self.scheduling and not self.scheduling.selection_valid():
                self.pause('operator selection expired or absent')
                raise ValueError('operator selection expired or absent')
            if self.closing or self.starting or self.publications or self.pending_control_changes or self.pause_pending or any(ex.stop_at is not None and not ex.exit_saved for ex in self.executions.values()):
                raise ValueError('pause persistence or old tree exit pending')
            self.stopped.set()  # stay closed throughout persistent resume
            self.generation += 1
            generation = self.generation
        try:
            self.ledger.persist_control(self.epoch, generation, False, 'explicit resume')
        except BaseException:
            with self.gate_lock:
                self.pause_pending = True
                self.reason = 'resume persistence failed'
            self.wake.set()
            raise
        with self.gate_lock:
            if self.generation != generation:
                raise ValueError('resume superseded by stop')
            if self.scheduling and not self.scheduling.selection_valid():
                self.pause('operator selection expired')
                raise ValueError('operator selection expired')
            self.saved_generation = generation
            self.stopped.clear()
            self._event('resumed')
        if self.scheduling:self.scheduling.control_changed(False)

    def pause(self, reason='user pause'):
        now = time.monotonic()
        with self.gate_lock:
            self.stopped.set()
            self.generation += 1
            self.pause_pending, self.reason = True, reason
            self._event('pause', reason=reason)
            executions = list(self.executions.values())
            for execution in executions:
                self._stop_unit_locked(execution, reason, now)
            for launch in self.starting.values():
                if launch['kill_at'] is None:
                    launch['kill_at'] = now + self.grace_seconds
        # No SQL, blocking pipe writes, joins, or shutdown calls on this path.
        self.wake.set()
        if self.scheduling:self.scheduling.control_changed(True)
        return self.status()

    def _stop_unit_locked(self, execution, reason, now):
        if execution.exit_evidence is not None:
            return
        execution.eligible = False
        action = execution.active_action
        deadline = now + self.grace_seconds
        if action and action['kind'] in REMOTE_WRITES:
            deadline = min(action['deadline'], now + self.rag_grace_seconds)
        if execution.stop_at is None:
            execution.stop_at, execution.kill_at = now, deadline
            self._event('cancel_requested', execution_id=execution.execution_id, reason=reason, kill_at=deadline)
            if execution.peer:
                execution.peer.send(dict(event='cancel', reason=reason, deadline=deadline, generation=self.generation))
        else:
            execution.kill_at = min(execution.kill_at, deadline)

    def cancel_unit(self, execution_id, reason):
        with self.gate_lock:
            self._stop_unit_locked(self.executions[execution_id], reason, time.monotonic())
        self.wake.set()

    def change_admission_state(self, scope, change_id, changes, *, mutate=None):
        """Invalidate first, persist SQL changes together, then align. No external work in mutate."""
        def still_valid():
            with self.gate_lock:
                return self.started and not self.closing and not self.closed.is_set()

        with self.gate_lock:
            if not still_valid():
                raise ValueError('controller is closing or inactive')
            current = self.scopes.get(scope)
            initializing = current is None
            if initializing:
                self._scope_capacity_locked()
                current = dict(generation=0, state=dict(enabled=True, frozen=False, expected_generation=0), ready=False)
                self.scopes[scope] = current
            elif not current['ready']:
                raise ValueError('scope change already pending or failed')
            current['ready'] = False
            # Includes first-scope loading, SQL commit/rollback and memory alignment.
            # Shutdown must retain the instance lock until this entire call leaves.
            self.pending_control_changes += 1
        try:
            if initializing:
                rows = self.ledger.rows('SELECT * FROM admission_scopes WHERE scope=?', (scope,))
                if rows:
                    with self.gate_lock:
                        current.update(generation=rows[0]['generation'], state=json.loads(rows[0]['state']))
            with self.gate_lock:
                if not still_valid():
                    raise ValueError('controller is closing or inactive')
                before = current['generation']
                state = dict(current['state'], **changes)
                generation = before + 1
                current['generation'] = generation
                self._event('scope_invalidated', scope=scope, admission_generation=generation, change_id=change_id)
            def write(c):
                # This read runs under BEGIN IMMEDIATE, not in a preflight check.
                control = c.execute('SELECT epoch FROM controller_state WHERE id=1').fetchone()
                if not control or control['epoch'] != self.epoch:
                    raise ValueError('stale controller scope mutation')
                if not still_valid():
                    raise ValueError('controller is closing or inactive')
                if initializing:
                    c.execute('INSERT OR IGNORE INTO admission_scopes VALUES(?,0,?,?)',
                              (scope, 'initial', canonical(dict(enabled=True, frozen=False, expected_generation=0))))
                changed = c.execute('UPDATE admission_scopes SET generation=?,change_id=?,state=? '
                                    'WHERE scope=? AND generation=?', (generation, change_id, canonical(state), scope, before)).rowcount
                if changed != 1:
                    raise ValueError('scope compare failed')
                if mutate:
                    result=mutate(c)
                    if isinstance(result,dict):
                        state.update(result)
                        c.execute('UPDATE admission_scopes SET state=? WHERE scope=? AND generation=?',
                                  (canonical(state),scope,generation))
                if not still_valid():
                    raise ValueError('controller is closing or inactive')
            self.ledger._write(write, still_valid=still_valid)
            with self.gate_lock:
                if not still_valid() or current['generation'] != generation:
                    raise ValueError('scope alignment superseded or controller closing')
                current.update(state=state, ready=True)
            return generation
        finally:
            with self.gate_lock:
                self.pending_control_changes -= 1
            self.wake.set()

    def unit_available(self,kind):
        with self.gate_lock:
            count=sum(p['kind']==kind for p in self.starting.values())+sum(ex.kind==kind and not ex.exit_saved for ex in self.executions.values())
            return count<self.unit_limits.get(kind,self.max_live_units)

    def launch(self, kind, key, argv, *, cwd, env=None, total_timeout=3600,run_id=None,schedule_attempt=None,expected_generation=None):
        if kind not in ALLOWED or not key or not (0 < total_timeout <= 86400):
            raise ValueError('invalid ownership unit or total timeout')
        with self.launch_lock:
            execution_id = uuid.uuid4().hex
            domain = 'target' if kind in ('delivery', 'cleanup') else kind
            scope = 'unit:' + canonical([domain, key])
            with self.gate_lock:
                if self.closing or not self.started or self.stopped.is_set():
                    raise ValueError('paused')
                if self.scheduling and not self.scheduling.selection_valid():
                    raise ActionDeferred('operator_scope_closed')
                if (len(self.starting)+sum(not ex.exit_saved for ex in self.executions.values())>=self.max_live_units
                        or not self.unit_available(kind)):
                    raise ActionDeferred('unit_capacity')
                if any(ex.scope == scope and (not ex.exit_saved or ex.execution_id in self.publications)
                       for ex in self.executions.values()):
                    raise ActionDeferred('unit already owned')
                generation = self.generation
                # Count the whole creation interval, including a blocked CreateProcess/DB write.
                pending = dict(scope=scope,kind=kind,job=None, kill_at=None, kill_sent=None, closing_tree=False, exit_evidence=None)
                self.starting[execution_id] = pending
                if kind=='graph':best_effort(self.graph_slots.grant,execution_id)
            job, process = None, None
            try:
                if self.ledger.rows("SELECT execution_id FROM control_executions WHERE "
                    "(CASE WHEN unit_kind IN ('delivery','cleanup') THEN 'target' ELSE unit_kind END)=? "
                    "AND unit_key=? AND state!='exited'", (domain, key)):
                    raise ActionDeferred('unit already owned')
                changes=dict(owner=execution_id)
                if expected_generation is not None:changes['expected_generation']=expected_generation
                self.change_admission_state(scope, execution_id, changes)
                name = 'Local\\paperflow-' + self.epoch + '-' + execution_id
                now = time.time()
                binding={}
                def register(c):
                    if kind=='graph' and c.execute("SELECT 1 FROM sqlite_master WHERE name='cleanup_runs'").fetchone():
                        if c.execute('SELECT 1 FROM cleanup_runs WHERE thread_id=?',(run_id or key,)).fetchone():
                            raise ValueError('graph run already closed for cleanup')
                        from .cleanup_paths import defer_local_cleanup
                        defer_local_cleanup(c,key)
                    c.execute('INSERT INTO control_executions '
                    '(execution_id,epoch,unit_kind,unit_key,job_name,state,lease_until,last_heartbeat,deadline) VALUES(?,?,?,?,?,?,?,?,?)',
                    (execution_id, self.epoch, kind, key, name, 'starting', now + self.lease_seconds, now, now + total_timeout))
                    if self.external and kind=='graph':self.external.register_sql(c,execution_id,self.epoch)
                    if schedule_attempt:
                        if not self.scheduling:raise ValueError('queue controller missing')
                        self.scheduling.ledger.attach_sql(c,schedule_attempt,execution_id,kind,key)
                        binding.update(dict(c.execute('SELECT j.*,a.attempt_id FROM scheduler_jobs j JOIN scheduler_attempts a '
                            'ON a.job_id=j.job_id WHERE a.attempt_id=?',(schedule_attempt,)).fetchone()))
                    if self.scheduling:self.scheduling.bind_selection_sql(c,binding)
                self.ledger._write(register)
                temporary = self.root / execution_id / 'tmp'
                temporary.mkdir(parents=True)
                job = Job(name)
                with self.gate_lock:
                    pending['job'] = job
                child_env = dict(env or {}, PAPERFLOW_PIPE=self.server.address, PAPERFLOW_EPOCH=self.epoch,
                                 PAPERFLOW_EXECUTION=execution_id, PAPERFLOW_AUTH=self.authkey.hex(),
                                 PAPERFLOW_WAIT_RESOURCES='1' if self.wait_resources else '0',
                                 TEMP=str(temporary), TMP=str(temporary))
                child_env['PAPERFLOW_TIMING_CONTEXT']=canonical(dict(schema_version=1,epoch=self.epoch,
                    execution_id=execution_id,attempt_id=binding.get('attempt_id'),job_id=binding.get('job_id'),
                    unit_key=key,execution_kind=kind,
                    attempt_binding='scheduled' if schedule_attempt else 'unscheduled'))
                process = job.spawn(argv, cwd=cwd, env=child_env)
                self.ledger._write(lambda c: c.execute("UPDATE control_executions SET root_identity=?,state='active' WHERE execution_id=?",
                                                      (canonical(process.identity), execution_id)))
                execution = Execution(execution_id, kind, key, scope, job, process,
                                      time.monotonic() + total_timeout, time.monotonic())
                execution.run_id=run_id  # Manager-selected graph binding, never supplied by a worker RPC.
                execution.scheduler_job=binding
                execution.identities[(process.pid, process.identity['created_at'])] = process.identity
                with self.gate_lock:
                    if (self.stopped.is_set() or self.generation != generation
                            or (self.scheduling and not self.scheduling.selection_valid(execution))):
                        self._stop_unit_locked(execution, 'launch invalidated', time.monotonic())
                        job.terminate()
                        raise ValueError('launch invalidated by pause')
                    # The process is already in the Job; this only releases its initial suspension.
                    process.resume()
                    self.executions[execution_id] = execution
                    self.starting.pop(execution_id)
                    self._event('worker_resumed', execution_id=execution_id, root=process.identity)
                return execution
            except BaseException:
                with self.gate_lock:
                    pending['closing_tree'] = True
                if job:
                    job.close()
                if process:
                    end = time.monotonic() + 5
                    while identity_alive(process.identity) and time.monotonic() < end:
                        time.sleep(.01)
                    if identity_alive(process.identity):
                        raise RuntimeError('failed launch tree not confirmed exited')
                    process.close()
                evidence = dict(reason='launch failed', no_running_tree=True,
                                identities=[process.identity] if process else [])
                with self.gate_lock:
                    pending['exit_evidence'] = evidence
                self.wake.set()  # Save/retire asynchronously if SQLite is still busy.
                raise

    def hello(self, peer, execution_id, identity, epoch):
        with self.gate_lock:
            execution = self.executions.get(execution_id)
            generation = self.generation
            if (epoch != self.epoch or not execution or execution.peer is not None
                    or self.stopped.is_set() or self.pause_pending or self.closing or not execution.eligible):
                raise ValueError('unknown execution identity or epoch')
        if identity['pid'] not in execution.job.pids() or process_identity(identity['pid']) != identity:
            raise ValueError('peer not in owned process tree')
        with self.gate_lock:
            if (epoch != self.epoch or generation != self.generation
                    or self.executions.get(execution_id) is not execution
                    or self.stopped.is_set() or self.pause_pending or self.closing
                    or execution.peer is not None or peer.closed.is_set() or not execution.eligible
                    or time.monotonic() >= execution.deadline):
                raise ValueError('hello invalidated')
            # Connection identity is not business admission. The root was already
            # registered before spawn; batch this display/member fact with the
            # existing writer instead of making every startup contend for SQLite.
            execution.worker_identity = dict(identity)
            execution.identities[(identity['pid'], identity['created_at'])] = dict(identity)
            execution.peer = peer
            peer.execution_id = execution_id
            execution.last_heartbeat = time.monotonic()
            reply = dict(epoch=self.epoch, paused=False,
                        expected_generation=self.scopes[execution.scope]['state'].get('expected_generation', 0),
                        heartbeat_seconds=self.heartbeat_interval)
        self.wake.set()
        return reply

    def _valid(self, snapshot, execution_id):
        with self.gate_lock:
            execution = self.executions.get(execution_id)
            return bool(not self.stopped.is_set() and not self.pause_pending and snapshot['epoch'] == self.epoch
                and (not self.scheduling or self.scheduling.selection_valid(execution))
                and snapshot['control_generation'] == self.generation and execution and execution.eligible
                and time.monotonic() < execution.deadline
                and time.monotonic() - execution.last_heartbeat < self.lease_seconds
                and execution.peer and not execution.peer.closed.is_set()
                and all(self.scopes.get(scope, {}).get('ready') and self.scopes[scope]['generation'] == generation
                        for scope, generation in snapshot['scopes'].items()))

    def snapshot(self, execution_id, kind):
        with self.gate_lock:
            execution = self.executions[execution_id]
            if kind not in ALLOWED[execution.kind]:
                raise ValueError('action outside ownership unit')
            scopes = [execution.scope]
            if kind == 'llm':
                scopes.append('service:llm')
            if kind.startswith('rag_') or kind == 'cleanup_write':
                scopes.append('service:rag')
            result = dict(epoch=self.epoch, control_generation=self.generation,
                          scopes={scope: self.scopes[scope]['generation'] for scope in scopes})
            if not self._valid(result, execution_id):
                raise ActionDeferred('immediate admission closed')
            return result

    def request_action(self, execution_id, request_id, kind, resource, expected_generation, total_timeout, snapshot=None):
        with self.gate_lock:
            if self.scheduling:self.scheduling.check_action(execution_id,kind)
        snapshot = snapshot or self.snapshot(execution_id, kind)
        with self.gate_lock:
            self._trim_tickets_locked()
            if sum(t['execution_id']==execution_id for t in self.tickets.values())>=16:
                raise ActionDeferred('ticket_capacity')
        if not (0 < total_timeout <= 86400) or not isinstance(request_id, str) or not request_id:
            raise ValueError('invalid action bounds')
        request = dict(execution_id=execution_id, kind=kind, resource=resource,
                       expected_generation=expected_generation, total_timeout=total_timeout)
        action_id = uuid.uuid4().hex
        deadline = min(self.executions[execution_id].deadline, time.monotonic() + total_timeout)
        def write(c):
            allow_frozen=False
            if self.delivery is not None and self.executions[execution_id].kind == 'delivery':
                allow_frozen=self.delivery.check_action(c,execution_id,kind,resource)
            if self.cleanup is not None and self.executions[execution_id].kind == 'cleanup':
                allow_frozen=self.cleanup.check_action(c,execution_id,kind,resource)
            self.ledger.check(c, snapshot, execution_id, kind, expected_generation,allow_frozen=allow_frozen)
            row = c.execute('SELECT * FROM control_actions WHERE epoch=? AND request_id=?', (self.epoch, request_id)).fetchone()
            if row:
                if row['request'] != canonical(request) or row['snapshot'] != canonical(snapshot) or row['state'] != 'prepared':
                    raise ValueError('request replay conflict')
                return row['action_id']
            c.execute('INSERT INTO control_actions VALUES(?,?,?,?,?,?,?,?,?,NULL)',
                      (action_id, self.epoch, request_id, execution_id, kind, canonical(request), canonical(snapshot),
                       'prepared', time.time() + max(0, deadline - time.monotonic())))
            return action_id
        try:action_id = self.ledger._write(write, still_valid=lambda: self._valid(snapshot, execution_id))
        except ValueError:
            if not self._valid(snapshot,execution_id):raise ActionDeferred('admission changed during SQLite wait') from None
            raise
        with self.gate_lock:
            if not self._valid(snapshot, execution_id):
                raise ActionDeferred('request invalidated while waiting for SQLite')
            self.tickets.setdefault(action_id, dict(snapshot=snapshot, request=request, expires=time.monotonic() + self.ticket_seconds,
                execution_id=execution_id, peer=self.executions[execution_id].peer, checked=False, consumed=False, deadline=deadline))
        return dict(ticket=action_id, epoch=self.epoch)

    def check_begin(self, execution_id, ticket):
        with self.gate_lock:
            value = self.tickets.get(ticket)
            if (not value or value['execution_id'] != execution_id or value['consumed'] or value['expires'] < time.monotonic()
                    or value['deadline'] <= time.monotonic() or not self._valid(value['snapshot'], execution_id)):
                raise ActionDeferred('stale ticket')
            snapshot, request = value['snapshot'], value['request']
            lane=self.resource_lane(request['kind'])
            operation=request['resource'].get('operation')
            limit=self.llm_operation_limits.get(operation) if lane=='llm' else None
            if limit is not None and sum(v.get('operation')==operation for v in self.resource_holds.values())>=limit:
                if self.wait_resources:value['expires']=min(value['deadline'],time.monotonic()+self.ticket_seconds)
                raise ActionDeferred('resource_busy')
            if sum(v['lane']==lane for v in self.resource_holds.values())>=self.resource_limits[lane]:
                # Before writing possible: no permit exists and no remote barrier is warranted.
                if self.wait_resources:
                    # Preserve the original snapshot and absolute deadline. A pause,
                    # expired ticket or changed scope is rejected by the check above.
                    value['expires']=min(value['deadline'],time.monotonic()+self.ticket_seconds)
                raise ActionDeferred('resource_busy')
            if self.scheduling:self.scheduling.check_action(execution_id,request['kind'])
            self.resource_holds[ticket]=dict(lane=lane,execution_id=execution_id,operation=operation)
            value['checking']=True
        def write(c):
            allow_frozen=False
            if self.delivery is not None and self.executions[execution_id].kind == 'delivery':
                allow_frozen=self.delivery.check_action(c,execution_id,request['kind'],request['resource'])
            if self.cleanup is not None and self.executions[execution_id].kind == 'cleanup':
                allow_frozen=self.cleanup.check_action(c,execution_id,request['kind'],request['resource'])
            self.ledger.check(c, snapshot, execution_id, request['kind'], request['expected_generation'],allow_frozen=allow_frozen)
            if c.execute("UPDATE control_actions SET state='possible' WHERE action_id=? AND state='prepared'", (ticket,)).rowcount != 1:
                raise ValueError('ticket already consumed or unknown')
        try:self.ledger._write(write, still_valid=lambda: self._valid(snapshot, execution_id))
        except BaseException as exc:
            with self.gate_lock:self.resource_holds.pop(ticket,None)
            if isinstance(exc,ValueError) and not self._valid(snapshot,execution_id):
                raise ActionDeferred('admission changed during SQLite wait') from None
            raise
        finally:
            with self.gate_lock:value['checking']=False
        with self.gate_lock:
            if not self._valid(snapshot,execution_id):
                self.resource_holds.pop(ticket,None)
                raise ActionDeferred('admission changed after SQLite write')
            value['checked'] = True
        return snapshot

    def grant_begin(self, execution_id, ticket, snapshot):
        with self.gate_lock:
            value = self.tickets.get(ticket)
            execution = self.executions.get(execution_id)
            if (not execution or not value or not value['checked'] or value['consumed'] or value['execution_id'] != execution_id
                    or value['snapshot'] != snapshot or value['peer'] is not execution.peer
                    or value['expires'] < time.monotonic() or value['deadline'] <= time.monotonic() or execution.active_action
                    or not self._valid(snapshot, execution_id)):
                self.resource_holds.pop(ticket,None)
                raise ActionDeferred('final admission denied')
            if ticket not in self.resource_holds:raise ActionDeferred('resource_reservation_expired')
            if self.scheduling:self.scheduling.check_action(execution_id,value['request']['kind'])
            value['consumed'] = True
            deadline = value['deadline']
            execution.active_action = dict(action_id=ticket, kind=value['request']['kind'], deadline=deadline)
            best_effort(self.resource_holds.granted,ticket)
            self._event('granted', execution_id=execution_id, action_id=ticket, scopes=snapshot['scopes'])
            return dict(action_id=ticket, epoch=self.epoch, generation=self.generation,
                        expires=min(time.monotonic() + .5, deadline), deadline=deadline)

    def record_result(self, execution_id, action_id, epoch, outcome, result):
        if outcome not in ('complete', 'diagnostic', 'unknown'):
            raise ValueError('unknown result outcome')
        with self.gate_lock:
            execution = self.executions.get(execution_id)
            # Reports are evidence only, never reusable authority to publish active/cache/desired.
            current = False
        def write(c):
            row = c.execute('SELECT * FROM control_actions WHERE action_id=? AND execution_id=?', (action_id, execution_id)).fetchone()
            if not row or row['epoch'] != epoch:
                raise ValueError('unknown result identity')
            c.execute('INSERT INTO control_reports(action_id,received,report_epoch,current_qualification,result) VALUES(?,?,?,?,?)',
                      (action_id, time.time(), epoch, int(current), canonical(result)))
            if epoch == self.epoch and row['state'] == 'possible':
                state = 'unresolved' if outcome == 'unknown' else outcome
                c.execute('UPDATE control_actions SET state=?,result=? WHERE action_id=?', (state, canonical(result), action_id))
        if self.scheduling and epoch==self.epoch:
            self.scheduling.record_result(execution_id,action_id,outcome,result,write)
        else:self.ledger._write(write)
        with self.gate_lock:
            if execution and execution.active_action and execution.active_action['action_id'] == action_id:
                execution.active_action = None
            self.resource_holds.pop(action_id,None)
            self.tickets.pop(action_id,None)
        return dict(saved_fact=True, current_qualification=current)

    @staticmethod
    def resource_lane(kind):return 'rag' if kind.startswith('rag_') or kind=='cleanup_write' else kind

    def execution_result(self,execution_id,outcome,data):
        if outcome not in ('complete','held','failed','deferred','stopped','blocked') or not isinstance(data,dict) or len(canonical(data))>4096:
            raise ValueError('invalid execution result')
        if outcome=='held':best_effort(self.graph_slots.held,execution_id,True)
        def write(c):
            row=c.execute('SELECT epoch FROM control_executions WHERE execution_id=?',(execution_id,)).fetchone()
            if not row or row['epoch']!=self.epoch:raise ValueError('execution epoch expired')
            old=c.execute('SELECT outcome,data FROM control_execution_results WHERE execution_id=?',(execution_id,)).fetchone()
            if old and (old['outcome']!=outcome or old['data']!=canonical(data)):raise ValueError('execution result replay conflict')
            c.execute('INSERT OR IGNORE INTO control_execution_results VALUES(?,?,?,?,?)',
                (execution_id,self.epoch,outcome,canonical(data),time.time()))
        if self.scheduling:self.scheduling.record_execution(execution_id,data,write)
        else:self.ledger._write(write)
        return dict(saved_fact=True)

    def _trim_tickets_locked(self):
        now=time.monotonic()
        for identity,ticket in list(self.tickets.items()):
            ex=self.executions.get(ticket['execution_id'])
            if ticket.get('checking') or (ex and ex.active_action and ex.active_action['action_id']==identity):continue
            if not ex or ex.exit_saved or ticket['expires']<now:
                self.tickets.pop(identity,None);self.resource_holds.pop(identity,None)

    def _trim_history(self):
        with self.gate_lock:
            self._trim_tickets_locked()
            for action,hold in list(self.resource_holds.items()):
                ex=self.executions.get(hold['execution_id'])
                if ex and ex.exit_saved and ex.execution_id not in self.publications and (not ex.peer or not ex.peer.graph_commit_lock.locked()):
                    self.resource_holds.pop(action,None)
            retired=[ex for ex in self.executions.values() if ex.exit_saved and ex.execution_id not in self.publications
                and (not ex.peer or (ex.peer.closed.is_set() and not ex.peer.inflight and not ex.peer.graph_commit_lock.locked()))]
            if self.delivery:
                for ex in retired:self.delivery.read_windows.pop(ex.execution_id,None)
            for ex in retired[:-self.history_limit]:self.executions.pop(ex.execution_id,None)
            # A publication loads its target scopes before invalidating them; a
            # launch owns a scope before its Execution object exists.
            handoff_permit=any(not ex.exit_saved and ex.active_action and
                self.tickets.get(ex.active_action['action_id'],{}).get('request',{}).get('resource',{}).get('operation')=='handoff_commit'
                for ex in self.executions.values())
            if self.publications or self.pending_control_changes or handoff_permit:return
            protected={ex.scope for ex in self.executions.values()}
            protected.update(item['scope'] for item in self.starting.values())
            protected.update(scope for ticket in self.tickets.values() for scope in ticket['snapshot']['scopes'])
            for scope,value in list(self.scopes.items()):
                if scope.startswith('unit:') and scope not in protected and value['ready']:self.scopes.pop(scope,None)

    def _scope_capacity_locked(self):
        if len(self.scopes)>=self.scope_limit:self._trim_history()
        if len(self.scopes)>=self.scope_limit:raise ActionDeferred('scope_capacity')

    def load_scope(self,scope):
        with self.gate_lock:
            if scope in self.scopes:return
        self.ledger.ensure_scope(scope)
        row=self.ledger.rows('SELECT * FROM admission_scopes WHERE scope=?',(scope,))[0]
        with self.gate_lock:
            if scope not in self.scopes:
                self._scope_capacity_locked()
                self.scopes[scope]=dict(generation=row['generation'],state=json.loads(row['state']),ready=True)

    def disconnected(self, execution_id):
        with self.gate_lock:
            if execution_id in self.executions:
                self._stop_unit_locked(self.executions[execution_id], 'IPC disconnected', time.monotonic())

    def handle(self, peer, request):
        execution_id = peer.execution_id
        if request.get('epoch') != self.epoch:
            raise ValueError('epoch expired')
        method, args = request['method'], request.get('args', {})
        fields = {'heartbeat': set(), 'request_action': {'request_id', 'kind', 'resource', 'expected_generation', 'total_timeout'},
                  'embedding_count': {'descriptor'},
                  'embedding_epoch': {'counter_epoch'},
                  'graph_commit': {'operation', 'descriptor'},
                  'delivery_commit': {'operation', 'descriptor'},
                  'cleanup_commit': {'operation','descriptor'},
                  'remote_group': {'operation','descriptor'},
                  'execution_result': {'outcome','data'},
                  'begin_action': {'ticket'}, 'finish_action': {'action_id', 'outcome', 'result'}}
        if method not in fields or not isinstance(args, dict) or set(args) != fields[method]:
            raise ValueError('unknown method or arguments')
        if method == 'heartbeat':
            with self.gate_lock:
                execution = self.executions[execution_id]
                execution.last_heartbeat = time.monotonic()
            return dict(alive=True)
        if method == 'request_action':
            return self.request_action(execution_id, **args)
        if method == 'begin_action':
            snapshot = self.check_begin(execution_id, args['ticket'])
            return self.grant_begin(execution_id, args['ticket'], snapshot)
        if method == 'finish_action':
            return self.record_result(execution_id, epoch=self.epoch, **args)
        if method == 'execution_result':return self.execution_result(execution_id,**args)
        if method == 'embedding_count' and self.embedding is not None:
            return self.embedding.handle(execution_id,**args)
        if method == 'embedding_epoch' and self.embedding is not None:
            return self.embedding.validate_epoch(execution_id,**args)
        if method == 'remote_group' and self.native_groups is not None:
            return self.native_groups.handle(execution_id,**args)
        if method == 'graph_commit' and self.processing is not None:
            return self.processing.handle(execution_id, **args)
        if method == 'delivery_commit' and self.delivery is not None:
            return self.delivery.handle(execution_id, **args)
        if method == 'cleanup_commit' and self.cleanup is not None:
            return self.cleanup.handle(execution_id, **args)
        raise ValueError('unknown control method')

    def _supervise(self):
        while not self.closed.wait(.02):
            if self.embedding:self.embedding.poll_health()
            with self.gate_lock:
                executions = list(self.executions.values())
                for pending in self.starting.values():
                    if (pending['job'] and not pending['closing_tree'] and pending['kill_at'] is not None
                            and time.monotonic() >= pending['kill_at'] and pending['kill_sent'] is None):
                        try:
                            pending['job'].terminate()
                            pending['kill_sent'] = time.monotonic()
                        except OSError:
                            self._error('starting Job termination not confirmed')
            for execution in executions:
                if execution.exit_evidence:
                    continue
                try:
                    for pid in execution.job.pids():
                        try:
                            item = process_identity(pid)
                        except OSError as exc:
                            if exc.winerror == 87:
                                continue
                            raise
                        with self.gate_lock:
                            execution.identities[(pid, item['created_at'])] = item
                    active = execution.job.active_count()
                    now = time.monotonic()
                    if active == 0 and execution.job_empty_at is None:
                        execution.job_empty_at = now
                    for identity_key, item in list(execution.identities.items()):
                        if identity_key not in execution.exit_times and (active==0 or not identity_alive(item)):
                            execution.exit_times[identity_key] = dict(**item, observed_exited_at=time.monotonic(),exit_basis='job_accounting' if active==0 else 'recorded_identity')
                    with self.gate_lock:
                        action = execution.active_action
                        if (now >= execution.deadline or (action and now >= action['deadline'])
                                or now - execution.last_heartbeat >= self.lease_seconds):
                            self._stop_unit_locked(execution, 'deadline or lease expired', now)
                        if execution.kill_at is not None and now >= execution.kill_at and execution.kill_sent is None:
                            execution.job.terminate()
                            execution.kill_sent = now
                            self._event('job_terminated', execution_id=execution.execution_id)
                    if active == 0 and all(key in execution.exit_times for key in execution.identities):
                        remote=self.external.exit_proof(execution.execution_id,self.epoch) if self.external and execution.kind=='graph' else None
                        if self.external and execution.kind=='graph' and remote is None:continue
                        with self.gate_lock:
                            execution.eligible = False
                            execution.exit_evidence = dict(active_processes=0, all_recorded_processes_exited=True,exit_basis='job_accounting',
                                identities=list(execution.identities.values()), observed_at=time.monotonic(), stop_at=execution.stop_at,
                                worker_identity=execution.worker_identity,
                                kill_at=execution.kill_at, terminate_called_at=execution.kill_sent,
                                job_empty_at=execution.job_empty_at, process_exits=list(execution.exit_times.values()),remote=remote)
                            self._event('tree_exited', execution_id=execution.execution_id)
                        self.wake.set()
                except Exception as exc:
                    with self.gate_lock:
                        execution.eligible = False
                        self._error(type(exc).__name__)
                    # Unknown exit remains owned. Fail closed; keep trying to observe/terminate.
                    try:
                        execution.job.terminate()
                    except OSError:
                        pass

    def _persist_error(self, exc):
        if fault(exc).get('halt_required') and not self.stopped.is_set():
            self.pause('ledger failure')
        with self.gate_lock:
            self._error(type(exc).__name__)
        from .remote_diagnostics import record_control_failure
        record_control_failure(self.root,'manager_persist',exc,epoch=self.epoch)

    def _persist(self):
        last_heartbeat = 0
        last_exit = ''
        while not self.closed.is_set():
            self.wake.wait(.05)
            self.wake.clear()
            # All waits share one sweep budget. Queue behind local writers first;
            # only SQL contention gets the short slice for each independent row.
            deadline = time.monotonic() + .7
            def persist(operation, *args):
                now = time.monotonic()
                if now >= deadline:return False
                try:
                    if not self.store.write_lock.acquire(timeout=max(0,deadline-now)):
                        # No SQL was entered. Keep the pending work for the next
                        # sweep and expose capacity waiting without inventing a
                        # database failure.
                        with self.gate_lock:
                            self._event('persistence_deferred',operation=operation.__name__,
                                        reason='writer_queue_budget_exhausted')
                        return False
                    try:
                        operation(*args,deadline=min(deadline,time.monotonic()+.1))
                    finally:
                        # Ledger calls reenter this same RLock. Release our outer
                        # hold before error reporting can acquire the gate lock.
                        self.store.write_lock.release()
                    return True
                except sqlite3.DatabaseError as exc:
                    if getattr(exc,'sqlite_errorcode',0)&255 not in (sqlite3.SQLITE_BUSY,sqlite3.SQLITE_LOCKED):
                        raise
                    self._persist_error(exc)
                    return False
            try:
                with self.gate_lock:
                    pending, generation, reason = self.pause_pending, self.generation, self.reason
                    executions = list(self.executions.values())
                    starting = list(self.starting.items())
                if pending and persist(self.ledger.persist_control,self.epoch,generation,True,reason):
                    with self.gate_lock:
                        if generation == self.generation:
                            self.pause_pending = False
                            self.saved_generation = generation
                # Give live workers a heartbeat opportunity before exit backlog.
                now = time.monotonic()
                if (now - last_heartbeat >= self.heartbeat_interval
                        or any(ex.worker_identity and not ex.worker_identity_saved for ex in executions
                               if ex.eligible and not ex.exit_evidence)):
                    with self.gate_lock:
                        updates = [(ex.execution_id, time.time() - (now - ex.last_heartbeat),
                                    list(ex.identities.values()), ex.worker_identity)
                                   for ex in executions if ex.eligible and not ex.exit_evidence and now < ex.deadline
                                   and now - ex.last_heartbeat < self.lease_seconds]
                    # No execution lease needs a write when the batch is empty.
                    if not updates or persist(self.ledger.heartbeat_batch,self.epoch,updates,self.lease_seconds):
                        with self.gate_lock:
                            for execution_id, _, _, identity in updates:
                                if identity and execution_id in self.executions:
                                    self.executions[execution_id].worker_identity_saved = True
                        last_heartbeat = now
                exits = [(ex.execution_id,ex,None) for ex in executions if ex.exit_evidence and not ex.exit_saved]
                exits += [(identity,None,value) for identity,value in starting if value['exit_evidence']]
                exits.sort(key=lambda item:item[0])
                # An identity cursor survives removals and also rotates failed
                # launches; a repeatedly busy first owner cannot monopolize us.
                pivot = next((i for i,item in enumerate(exits) if item[0]>last_exit),len(exits))
                for execution_id,execution,pending_launch in exits[pivot:]+exits[:pivot]:
                    if time.monotonic() >= deadline:break
                    last_exit = execution_id
                    evidence = execution.exit_evidence if execution else pending_launch['exit_evidence']
                    if pending_launch is not None and self.external and pending_launch.get('kind')=='graph':
                        remote=self.external.exit_proof(execution_id,self.epoch)
                        if remote is None:continue
                        evidence=dict(evidence,remote=remote)
                        pending_launch['exit_evidence']=evidence
                    if not persist(self.ledger.confirm_exit,execution_id,evidence):continue
                    with self.gate_lock:
                        if execution:
                            execution.exit_saved = True
                            if execution.kind=='graph':best_effort(self.graph_slots.release,execution_id)
                        else:
                            self.starting.pop(execution_id, None)
                            if pending_launch.get('kind')=='graph':best_effort(self.graph_slots.release,execution_id)
                    if execution:
                        execution.job.close()
                        execution.process.close()
                        if execution.peer:execution.peer.close()
                self._trim_history()
            except Exception as exc:
                # Pause remains closed; independent supervisor keeps the absolute deadlines.
                self._persist_error(exc)

    def status(self):
        from .workbench_resources import counter_health
        with self.gate_lock:
            live = len(self.starting) + len({ex.execution_id for ex in self.executions.values() if not ex.exit_saved}
                                           | self.publications)
            return dict(epoch=self.epoch, admission_open=not self.stopped.is_set() and not self.pause_pending,
                        pause_persisted=not self.pause_pending and self.saved_generation == self.generation,
                        state=('running' if not self.stopped.is_set() else 'pausing' if live or self.pending_control_changes or self.pause_pending else 'paused'),
                        pending_control_changes=self.pending_control_changes,
                        live_units=live, generation=self.generation, errors=list(self.errors)[-10:],
                        harness=self.external.status() if self.external else dict(location='local',state='ready'),
                        embedding_counter=counter_health(self.embedding),
                        resources={lane:dict(limit=limit,used=sum(v['lane']==lane for v in self.resource_holds.values()))
                                   for lane,limit in self.resource_limits.items()})

    def shutdown(self, timeout=35):
        if not self.started:
            return
        with self.gate_lock:
            self.closing = True
        self.pause('normal shutdown')
        deadline = time.monotonic() + timeout
        while True:
            status = self.status()
            if (not status['live_units'] and not status['pending_control_changes'] and status['pause_persisted']
                    and (not self.native_groups or self.native_groups.ready())):
                break
            if time.monotonic() >= deadline:
                raise TimeoutError('shutdown incomplete: tree exit or persistence pending')
            time.sleep(.02)
        self.closed.set()
        self.wake.set()
        self.server.close()
        self.monitor.join(timeout=1)
        self.writer.join(timeout=1)
        if self.external:self.external.close()
        if self.native_groups:self.native_groups.close()
        if self.embedding:self.embedding.close()
        self.lock.__exit__()
        self.started = False
