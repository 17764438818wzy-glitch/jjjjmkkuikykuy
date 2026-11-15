import sys
import threading
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest

from paperflow.delivery_control import ControlledRagClient
from paperflow.scheduling_policy import fault,ActionDeferred


@pytest.fixture
def failing_http():
    sys.path.insert(0,r'E:\uv\ragflow_upstream')
    from ragflow_client import RagflowClient
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*a):pass
        def do_GET(self):self.reply()
        def do_POST(self):self.reply()
        def reply(self):
            self.server.calls+=1
            self.send_response(503)
            if self.server.retry_after:self.send_header('Retry-After',self.server.retry_after)
            self.end_headers()
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.calls=0
    server.retry_after='45'
    thread=threading.Thread(target=lambda:server.serve_forever(poll_interval=.05),daemon=True);thread.start()
    native=RagflowClient(f'http://127.0.0.1:{server.server_port}','synthetic-key')
    yield native,server
    server.shutdown();server.server_close();thread.join()


class Actions:
    identity={'manifest_sha256':'a'*64}
    def __init__(self):self.permits=[]
    def run(self,kind,resource,call,**kw):
        self.permits.append(kind);return call(threading.Event())


def test_one_native_get_per_permit_and_retry_after_survives(failing_http):
    native,server=failing_http;actions=Actions();client=ControlledRagClient(native,actions)
    with pytest.raises(Exception) as error:client.request('GET','/datasets')
    assert server.calls==1 and actions.permits==['rag_read']
    assert fault(error.value)['retry_after']==45


def test_three_short_attempts_have_fresh_permits_and_stop_interrupts_wait(failing_http):
    from paperflow.worker_retry import short_attempts
    native,server=failing_http;actions=Actions();client=ControlledRagClient(native,actions)
    worker=SimpleNamespace(stopped=threading.Event())
    with pytest.raises(Exception):short_attempts(worker,lambda:client.request('GET','/datasets'),interval=.001)
    # Retry-After yields immediately to the outer due queue, never sleeps a worker for 45 seconds.
    assert server.calls==1
    calls=[]
    def connection_failure():calls.append(1);raise ConnectionError('synthetic')
    with pytest.raises(ConnectionError):short_attempts(worker,connection_failure,interval=.001)
    assert len(calls)==3
    calls.clear()
    def stopped_failure():calls.append(1);worker.stopped.set();raise ConnectionError('synthetic')
    with pytest.raises(ActionDeferred):short_attempts(worker,stopped_failure,interval=10)
    assert len(calls)==1


def test_content_conflict_and_unknown_write_are_never_blindly_retried():
    from paperflow.worker_retry import short_attempts
    from paperflow.rag import RemoteConflict
    worker=SimpleNamespace(stopped=threading.Event());calls=[]
    def bad():calls.append(1);raise RemoteConflict('identity')
    with pytest.raises(RemoteConflict):short_attempts(worker,bad,interval=0)
    assert len(calls)==1


def test_native_http_without_contention_exhausts_three_separate_permits(failing_http):
    from paperflow.worker_retry import short_attempts
    native,server=failing_http;server.retry_after=None
    actions=Actions();client=ControlledRagClient(native,actions)
    worker=SimpleNamespace(stopped=threading.Event())
    with pytest.raises(Exception):short_attempts(worker,lambda:client.request('GET','/datasets'),interval=.001)
    assert server.calls==3 and actions.permits==['rag_read']*3


def test_resource_contention_between_short_attempts_yields_without_more_requests():
    from paperflow.worker_retry import short_attempts
    worker=SimpleNamespace(stopped=threading.Event());calls=[]
    def call():
        calls.append(1)
        if len(calls)==1:raise ConnectionError('synthetic network failure')
        raise ActionDeferred('resource_busy')
    with pytest.raises(ActionDeferred,match='resource_busy'):short_attempts(worker,call,interval=0)
    assert len(calls)==2
