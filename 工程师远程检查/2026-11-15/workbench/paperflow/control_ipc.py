"""Authenticated, bounded local IPC. SQLite state never grants worker permission."""
from __future__ import annotations

import json
from multiprocessing.connection import Listener, Client, deliver_challenge, answer_challenge
import os
import queue
import threading
import time
import uuid

from .winjob import named_pipe_peer, process_identity
from .scheduling_policy import ActionDeferred,fault

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
                if request.get('method') in ('graph_commit','delivery_commit','cleanup_commit'):
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
                    self.send(dict(id=request.get('id'), error=type(exc).__name__ + ': ' + str(exc),
                        deferred=exc.reason if isinstance(exc,ActionDeferred) else None))
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
            self.send(dict(id=request.get('id'),error=type(exc).__name__+': '+str(exc),
                deferred=exc.reason if isinstance(exc,ActionDeferred) else None))
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
            if method in ('graph_commit','delivery_commit'):
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

    def report_execution(self,outcome,data):return self.rpc('execution_result',dict(outcome=outcome,data=data))

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
            prepared = self.rpc('request_action', dict(request_id=uuid.uuid4().hex, kind=kind,
                resource=resource, total_timeout=total_timeout,
                expected_generation=self.expected_generation if expected_generation is None else expected_generation))
            permit = self.rpc('begin_action', dict(ticket=prepared['ticket']))
            if (permit['epoch'] != self.epoch or time.monotonic() > permit['expires']
                    or time.monotonic() >= permit['deadline']
                    or self.stopped.is_set() or self.closed.is_set()):
                raise ValueError('permission expired before immediate use')
            remote = kind in ('rag_write', 'cleanup_write')
            # Read-only by adapters. It is cleared at the end of this one action.
            self.active_permit = permit
            # A sent remote write keeps its original deadline; pause cancels local/LLM work immediately.
            cancel = threading.Event() if remote else self.local_cancel
            try:
                result = function(cancel)
                outcome = 'diagnostic' if cancel.is_set() else 'complete'
            except BaseException as exc:
                if remote:exc.unresolved_write=True
                try:
                    self.rpc('finish_action', dict(action_id=permit['action_id'],
                        outcome='unknown' if remote else 'diagnostic', result=dict(error=type(exc).__name__,fault=fault(exc))))
                except Exception:
                    pass
                raise
            self.rpc('finish_action', dict(action_id=permit['action_id'], outcome=outcome, result=result))
            return result
