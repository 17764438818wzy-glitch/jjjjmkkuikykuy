"""Authenticated, bounded local IPC. SQLite state never grants worker permission."""
from __future__ import annotations

import json
from multiprocessing.connection import Listener, Client, deliver_challenge, answer_challenge
import os
import queue
import sqlite3
import threading
import time
import uuid

from .timing_artifacts import execution_observation
from .winjob import named_pipe_peer, process_identity
from .scheduling_policy import ActionDeferred,fault
from .instrumentation import Recorder, safe_span, best_effort, diagnostic, safe_now, using,new_recorder,compact_stage_timing

MAX_MESSAGE = 65536


def encode(value):
    data = json.dumps(value, ensure_ascii=False, allow_nan=False).encode('utf-8')
    if len(data) > MAX_MESSAGE:
        raise ValueError('control message too large')
    return data


def decode(connection):
    value = json.loads(connection.recv_bytes(MAX_MESSAGE))
    if not isinstance(value, dict):
        raise ValueError('control message must be an object')
    return value


def error_reply(request, exc):
    reply = dict(id=request.get('id'), error=type(exc).__name__ + ': ' + str(exc),
                 deferred=exc.reason if isinstance(exc, ActionDeferred) else None)
    # Transport actual database failure facts, never infer them from message text.
    # The worker's existing classifier keeps busy retryable and corruption fatal.
    if isinstance(exc, sqlite3.DatabaseError):
        code = getattr(exc, 'sqlite_errorcode', None)
        reply.update(database_error=True, sqlite_errorcode=code if type(code) is int else None)
    return reply


class Peer:
    def __init__(self, server, connection):
        self.server, self.connection = server, connection
        self.execution_id = None
        self.closed = threading.Event()
        self.outgoing = queue.Queue(32)
        self.graph_commit_lock = threading.Lock()
        self.inflight=0

    def send(self, value):
        try:
            self.outgoing.put_nowait(encode(value))
        except (queue.Full, ValueError):
            self.close()

    def close(self):
        if not self.closed.is_set():
            self.closed.set()
            self.connection.close()
            if self.execution_id:
                self.server.manager.disconnected(self.execution_id)
            self.server.discard(self)
            while True:
                try:self.outgoing.get_nowait()
                except queue.Empty:break

    def write(self):
        try:
            while not self.closed.is_set():
                try:
                    data = self.outgoing.get(timeout=.1)
                except queue.Empty:
                    continue
                self.connection.send_bytes(data)
        except (OSError, EOFError):
            self.close()

    def run(self):
        timer = threading.Timer(2, self.close)
        timer.daemon = True
        timer.start()
        try:
            deliver_challenge(self.connection, self.server.manager.authkey)
            answer_challenge(self.connection, self.server.manager.authkey)
            actual_identity = named_pipe_peer(self.connection)
            request = decode(self.connection)
            if request.get('method') != 'hello' or request.get('args', {}).get('identity') != actual_identity:
                raise ValueError('peer identity mismatch')
            reply = self.server.manager.hello(self, request['args']['execution_id'], actual_identity, request.get('epoch'))
            timer.cancel()
            if self.closed.is_set():
                return
            threading.Thread(target=self.write, daemon=True).start()
            self.send(dict(id=request['id'], result=reply))
            while not self.closed.is_set():
                request = decode(self.connection)
                if request.get('method') in ('graph_commit','delivery_commit','cleanup_commit','embedding_count'):
                    if not self.graph_commit_lock.acquire(blocking=False):
                        self.send(dict(id=request.get('id'),error='graph publication already pending'))
                        continue
                    # One bounded publication handler per peer; heartbeats continue on the reader.
                    threading.Thread(target=self.graph_commit,args=(request,),daemon=True).start()
                    continue
                try:
                    self.inflight+=1
                    reply = self.server.manager.handle(self, request)
                    self.send(dict(id=request['id'], result=reply))
                except Exception as exc:
                    self.send(error_reply(request, exc))
                finally:self.inflight-=1
        except (OSError, EOFError, ValueError, KeyError):
            pass
        finally:
            timer.cancel()
            self.close()

    def graph_commit(self,request):
        try:
            reply=self.server.manager.handle(self,request)
            self.send(dict(id=request['id'],result=reply))
        except Exception as exc:
            self.send(error_reply(request, exc))
        finally:self.graph_commit_lock.release()


class PipeServer:
    def __init__(self, manager):
        self.manager = manager
        self.address = r'\\.\pipe\paperflow-' + manager.epoch
        # Authenticate in per-peer threads: a stalled handshake cannot block accept or pause.
        self.listener = Listener(self.address, family='AF_PIPE', authkey=None)
        self.closed = threading.Event()
        self.peers = set()
        self.peers_lock=threading.Lock()

    def start(self):
        threading.Thread(target=self.accept, daemon=True, name='paperflow-pipe').start()

    def accept(self):
        while not self.closed.is_set():
            try:
                connection = self.listener.accept()
            except (OSError, EOFError):
                return
            if self.closed.is_set():
                connection.close()
                return
            peer = Peer(self, connection)
            with self.peers_lock:
                if len(self.peers)>=self.manager.peer_limit:
                    connection.close();continue
                self.peers.add(peer)
            threading.Thread(target=peer.run, daemon=True).start()

    def discard(self,peer):
        with self.peers_lock:self.peers.discard(peer)

    def close(self):
        self.closed.set()
        self.listener.close()
        with self.peers_lock:peers=list(self.peers)
        for peer in peers:
            peer.close()


class WorkerClient:
    def __init__(self, address, authkey, execution_id, epoch, *, timeout=2, on_cancel=None):
        self.timeout, self.execution_id, self.epoch = timeout, execution_id, epoch
        self.metrics=new_recorder(factory=Recorder,epoch=epoch)
        self.closed, self.stopped = threading.Event(), threading.Event()
        self.local_cancel = threading.Event()
        self.on_cancel = on_cancel
        self.pending, self.pending_lock = {}, threading.Lock()
        self.outgoing = queue.Queue(32)
        self.action_lock = threading.Lock()
        self.state_lock = threading.Lock()
        self.active_kind = None
        self.active_permit = None
        connected = queue.Queue(1)
        abandoned = threading.Event()
        def connect():
            try:
                result = Client(address, family='AF_PIPE', authkey=authkey)
                if abandoned.is_set():
                    result.close()
                else:
                    connected.put(result)
            except Exception as exc:
                connected.put(exc)
        threading.Thread(target=connect, daemon=True).start()
        try:
            result = connected.get(timeout=timeout)
        except queue.Empty:
            abandoned.set()
            raise TimeoutError('controller connect/authentication timed out') from None
        if isinstance(result, Exception):
            raise ConnectionError('controller connection failed') from result
        self.connection = result
        threading.Thread(target=self.read, daemon=True).start()
        threading.Thread(target=self.write, daemon=True).start()
        try:
            reply = self.rpc('hello', dict(execution_id=execution_id, identity=process_identity(os.getpid())))
            if reply['epoch'] != epoch or reply['paused']:
                raise ValueError('controller is paused or epoch expired')
            self.expected_generation = reply['expected_generation']
            self.heartbeat_seconds = reply['heartbeat_seconds']
            threading.Thread(target=self.heartbeat, daemon=True).start()
        except BaseException:
            self.close()
            raise

    @classmethod
    def from_environment(cls, **kwargs):
        return cls(os.environ['PAPERFLOW_PIPE'], bytes.fromhex(os.environ['PAPERFLOW_AUTH']),
                   os.environ['PAPERFLOW_EXECUTION'], os.environ['PAPERFLOW_EPOCH'], **kwargs)

    def stop(self):
        with self.state_lock:
            first = not self.stopped.is_set()
            self.stopped.set()
            self.local_cancel.set()
            local = self.active_kind not in ('rag_write', 'cleanup_write')
        if first and local and self.on_cancel:
            threading.Thread(target=self.on_cancel, daemon=True).start()

    def close(self):
        if not self.closed.is_set():
            self.closed.set()
            self.stop()
            self.connection.close()
            with self.pending_lock:
                for response in self.pending.values():
                    try:
                        response.put_nowait(dict(error='IPC disconnected'))
                    except queue.Full:
                        pass

    def read(self):
        try:
            while not self.closed.is_set():
                message = decode(self.connection)
                if message.get('event') == 'cancel':
                    self.stop()
                    continue
                with self.pending_lock:
                    response = self.pending.get(message.get('id'))
                    if response:
                        response.put_nowait(message)
        except (OSError, EOFError, ValueError, queue.Full):
            pass
        finally:
            self.close()

    def write(self):
        try:
            while not self.closed.is_set():
                try:
                    data = self.outgoing.get(timeout=.1)
                except queue.Empty:
                    continue
                self.connection.send_bytes(data)
        except (OSError, EOFError):
            self.close()

    def rpc(self, method, args=None):
        if self.closed.is_set():
            raise ConnectionError('IPC disconnected')
        identity = uuid.uuid4().hex
        response = queue.Queue(1)
        with self.pending_lock:
            self.pending[identity] = response
        try:
            self.outgoing.put_nowait(encode(dict(id=identity, epoch=self.epoch, method=method, args=args or {})))
            if method in ('graph_commit','delivery_commit','embedding_count'):
                # File validation has its own admitted local-action deadline. Ordinary RPCs remain 2 s.
                deadline=time.monotonic()+120
                while True:
                    if self.stopped.is_set() or self.closed.is_set():raise ValueError('graph publication stopped')
                    try:
                        reply=response.get(timeout=min(.1,max(.001,deadline-time.monotonic())))
                        break
                    except queue.Empty:
                        if time.monotonic()>=deadline:raise
            else:reply = response.get(timeout=self.timeout)
            if 'error' in reply:
                if reply.get('deferred'):raise ActionDeferred(reply['deferred'])
                if reply.get('database_error') is True:
                    exc = sqlite3.DatabaseError(reply['error'])
                    if type(reply.get('sqlite_errorcode')) is int:
                        exc.sqlite_errorcode = reply['sqlite_errorcode']
                    raise exc
                raise ValueError(reply['error'])
            return reply['result']
        except (queue.Empty, queue.Full):
            self.close()
            raise TimeoutError('controller response timed out; new actions denied') from None
        finally:
            with self.pending_lock:
                self.pending.pop(identity, None)

    def heartbeat(self):
        while not self.closed.wait(self.heartbeat_seconds):
            try:
                self.rpc('heartbeat')
            except Exception:
                self.close()
                return

    def report_execution(self,outcome,data):
        metrics=getattr(self,'metrics',None)
        if metrics is not None and not getattr(metrics,'enabled',True) and not getattr(metrics,'disabled_by_failure',False):
            return self.rpc('execution_result',dict(outcome=outcome,data=data))
        timing=best_effort(metrics.summary) if metrics else None
        projection=best_effort(execution_observation,self,outcome,data,timing)
        if hasattr(self,'timing_context') and projection is None:
            projection=dict(metrics_complete=False,details_status='artifact_observer_failed')
        if projection is not None:
            timing=projection
            if projection.get('details_status')=='available':
                data={key:value for key,value in data.items() if key!='_failed_stage_timing'}
        observed=dict(data,_timing=timing or dict(metrics_complete=False))
        def fits(value):
            try:
                if len(json.dumps(value,ensure_ascii=False,allow_nan=False,separators=(',',':')))>4096:return False
                encode(dict(id='0'*32,epoch=self.epoch,method='execution_result',args=dict(outcome=outcome,data=value)))
                return True
            except BaseException:return False
        if not fits(observed) and isinstance(timing,dict) and timing.get('stages') and not any(
                span.get('kind','').startswith('rag_') for span in getattr(metrics,'spans',())):
            # Graph workers do not need the separate delivery-attempt projection.
            # Keep all stage leaves/boundaries and graph totals in the same cap.
            lean={key:value for key,value in timing.items() if key not in
                ('attempt_exclusive_seconds','attempt_residual_seconds','server_observations_available','stages_truncated')}
            lean['summary_compacted']=True
            candidate=dict(data,_timing=lean)
            if fits(candidate):observed=candidate
        if not fits(observed) and isinstance(timing,dict) and timing.get('details_ref'):
            compact={key:timing[key] for key in ('metrics_complete','details_status','details_ref','stage_visit_count') if key in timing}
            candidate=dict({key:value for key,value in data.items() if key!='diagnostic'},_timing=compact)
            if fits(candidate):observed=candidate
        if not fits(observed):
            best_effort(setattr,metrics,'complete',False)
            marker=dict(metrics_complete=False,details_truncated=True)
            # Diagnostic and stage facts are observability fields. Keep the
            # failed-stage full ledger ahead of optional traceback detail.
            options=[dict(data,_timing=marker),
                dict({k:v for k,v in data.items() if k!='diagnostic'},_timing=marker),
                dict({k:v for k,v in data.items() if k not in ('diagnostic','_failed_stage_timing')},_timing=marker)]
            original={k:v for k,v in data.items() if k not in ('diagnostic','_failed_stage_timing')}
            observed=next((value for value in options if fits(value)),original)
        return self.rpc('execution_result',dict(outcome=outcome,data=observed))

    def perform(self, kind, resource, function, *, total_timeout, expected_generation=None):
        with self.action_lock:
            with self.state_lock:
                if self.stopped.is_set() or self.closed.is_set():
                    raise ValueError('new actions stopped')
                self.active_kind = kind
            try:
                return self._perform(kind, resource, function, total_timeout, expected_generation)
            finally:
                with self.state_lock:
                    self.active_kind = None
                    self.active_permit = None

    def _perform(self, kind, resource, function, total_timeout, expected_generation):
            metrics=getattr(self,'metrics',None)
            if metrics is None:
                metrics=self.metrics=new_recorder(factory=Recorder,epoch=self.epoch)
            requested=safe_now(metrics)
            prepared = self.rpc('request_action', dict(request_id=uuid.uuid4().hex, kind=kind,
                resource=resource, total_timeout=total_timeout,
                expected_generation=self.expected_generation if expected_generation is None else expected_generation))
            while True:
                try:
                    permit = self.rpc('begin_action', dict(ticket=prepared['ticket']))
                    break
                except ActionDeferred as exc:
                    if exc.reason!='resource_busy' or os.environ.get('PAPERFLOW_WAIT_RESOURCES')!='1':raise
                    if self.stopped.wait(.15) or self.closed.is_set():
                        raise ActionDeferred('new actions stopped during resource wait') from None
            if (permit['epoch'] != self.epoch or time.monotonic() > permit['expires']
                    or time.monotonic() >= permit['deadline']
                    or self.stopped.is_set() or self.closed.is_set()):
                raise ValueError('permission expired before immediate use')
            remote = kind in ('rag_write', 'cleanup_write')
            # Read-only by adapters. It is cleared at the end of this one action.
            self.active_permit = permit
            granted=safe_now(metrics)
            best_effort(metrics.interval,'permit_wait',requested,granted,kind=kind)
            # A sent remote write keeps its original deadline; pause cancels local/LLM work immediately.
            cancel = threading.Event() if remote else self.local_cancel
            action_span=None
            try:
                with using(metrics),safe_span(metrics,'permit_hold',kind=kind,operation=resource.get('operation'),
                    default_label='local_processing' if kind=='local' else 'remote_runtime_or_wait' if kind=='llm' else 'residual',
                    observation_start=len(metrics.observations),poll_start=len(metrics.polls)) as action_span:
                    result = function(cancel)
                outcome = 'diagnostic' if cancel.is_set() else 'complete'
            except BaseException as exc:
                if remote:exc.unresolved_write=True
                try:
                    timing=best_effort(self._action_timing,metrics,requested,granted,action_span) or dict(metrics_complete=False)
                    if not isinstance(timing,dict) or timing.get('metrics_complete') is not True:best_effort(setattr,metrics,'complete',False)
                    base=dict(error=type(exc).__name__,fault=fault(exc))
                    fact=self._bounded_action_result(permit['action_id'],'unknown' if remote else 'diagnostic',base,
                        dict(diagnostic=diagnostic(exc,stage=resource.get('operation')),_timing=timing))
                    self.rpc('finish_action', dict(action_id=permit['action_id'],
                        outcome='unknown' if remote else 'diagnostic', result=fact))
                except Exception:
                    pass
                raise
            fact=result
            if isinstance(result,dict) and metrics.enabled:
                timing=best_effort(self._action_timing,metrics,requested,granted,action_span)
                if not isinstance(timing,dict) or timing.get('metrics_complete') is not True:best_effort(setattr,metrics,'complete',False)
                fact=self._bounded_action_result(permit['action_id'],outcome,result,
                    dict(_timing=timing or dict(metrics_complete=False)))
            self.rpc('finish_action', dict(action_id=permit['action_id'], outcome=outcome, result=fact))
            return result

    def _bounded_action_result(self,action_id,outcome,base,observations):
        """Fit observation fields into the ORIGINAL complete RPC byte budget.

        Never trim business fields or retry a finish RPC. If even an incomplete
        marker cannot fit, the original result is sent and execution summary
        records the missing coverage through metrics.complete=False.
        """
        def fits(value):
            try:
                encode(dict(id='0'*32,epoch=self.epoch,method='finish_action',
                    args=dict(action_id=action_id,outcome=outcome,result=value)))
                return True
            except BaseException:return False
        try:observed=dict(base,**observations)
        except Exception:observed=None
        if observed is not None and fits(observed):return observed
        best_effort(setattr,self.metrics,'complete',False)
        compact=dict(base,_timing=dict(metrics_complete=False,details_truncated=True,
            reason='observation_payload_exceeds_ipc_or_is_not_serializable'))
        return compact if fits(compact) else base

    @staticmethod
    def _action_timing(metrics,requested,granted,span):
        fact=metrics.partition(span)
        parent=next((entry for entry in reversed(metrics.stack) if entry['label']=='stage'),None)
        if parent:
            fact.update(stage=parent.get('stage'),input_fingerprint=parent.get('input_fingerprint'),task_id=parent.get('task_id'))
            stage_snapshot=metrics.partition(dict(parent,end=span.get('end') if span else safe_now(metrics)))
            stage_snapshot.update(stage=parent.get('stage'),stage_invocation_id=parent.get('stage_invocation_id'),
                business_stage=parent.get('business_stage'),started_at=parent.get('start'),ended_at=span.get('end') if span else None,
                input_fingerprint=parent.get('input_fingerprint'),coverage='through_action_end',
                prefix_metrics_complete=stage_snapshot.get('metrics_complete'),metrics_complete=False)
            fact['stage_snapshot']=compact_stage_timing(stage_snapshot)
        business_parent=next((entry for entry in reversed(metrics.stack) if entry['label'].startswith('business:')),None)
        if business_parent:
            business=fact.setdefault('business_seconds',{})
            name=business_parent['label'].removeprefix('business:')
            business[name]=business.get(name,0)+max(0,fact.get('elapsed_seconds',0)-sum(business.values()))
            fact['business_view_scope']='within_client_function; nonadditive_to_attempt'
        fact.update(permit_wait_seconds=max(0,granted-requested),permit_hold_seconds=fact.get('elapsed_seconds'),
            client_function_seconds=fact.get('elapsed_seconds'),hold_boundary='client_function_lower_bound; manager exact occupancy in settlement',
            request_to_result_seconds=max(0,metrics.clock()-requested))
        observations=metrics.observations[span.get('observation_start',0):] if span else []
        remote=[dict(source='manager_group_action',timing=entry.get('timing'),additive_to_client_seconds=False)
            for entry in observations if entry.get('source')=='manager_group_action']
        if remote:
            fact['remote_action_timings']=remote[-8:]
            fact['remote_action_timings_truncated']=len(remote)>8
            if len(remote)>8 or any(not isinstance(entry['timing'],dict) or entry['timing'].get('metrics_complete') is not True for entry in remote):
                fact['metrics_complete']=False
        if span and span.get('kind','').startswith('rag_'):
            leaves=fact.get('exclusive_seconds',{})
            fact['rag_hold_seconds']=fact['elapsed_seconds']
            fact['http_inside_seconds']=leaves.get('http',0)
            fact['explicit_sleep_inside_seconds']=leaves.get('explicit_sleep',0)
            fact['local_inside_seconds']=leaves.get('local_processing',0)
            fact['permit_residual_seconds']=fact['elapsed_seconds']-sum(fact[k] for k in
                ('http_inside_seconds','explicit_sleep_inside_seconds','local_inside_seconds'))
            fact['server_observations']=observations[-8:]
            fact['server_observations_truncated']=len(observations)>8
            fact['polling']=metrics.polls[span.get('poll_start',0):]
        return fact
