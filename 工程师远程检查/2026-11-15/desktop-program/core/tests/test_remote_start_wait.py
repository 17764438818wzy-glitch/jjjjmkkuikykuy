"""Queueing is inside the existing permit, before the SDK initialization timer."""
import hashlib
import io
import json
import queue
import threading
import time
from types import SimpleNamespace

import pytest

from paperflow.remote_harness import RemoteHarness


def setup(tmp_path,monkeypatch,*,ready=True):
    source=tmp_path/'original.txt';source.write_bytes(b'ORIGINAL')
    catalog=tmp_path/'catalog.json';catalog.write_text('{}')
    output=tmp_path/'output';output.mkdir();home=tmp_path/'home';home.mkdir()
    client=SimpleNamespace(stopped=threading.Event(),closed=threading.Event(),epoch='a'*32,execution_id='b'*32,
        active_kind='llm',active_permit=dict(epoch='a'*32,action_id='c'*32,deadline=time.monotonic()+2))
    config=SimpleNamespace(root=tmp_path,processing_ledger_path=tmp_path/'ledger.sqlite3',catalog_path=catalog,
        provider='free',model='fixture',model_timeout=2,
        remote_harness=dict(host='fixture',version='fixture',image='sha256:'+'1'*64,deployment_id='fixture'))
    harness=RemoteHarness(config,SimpleNamespace(client=client),output,home,{})
    harness.configure(dict(task_id='TASK',source_path=str(source),source_sha256=hashlib.sha256(b'ORIGINAL').hexdigest()))
    messages=queue.Queue();stop=threading.Event();initialized=[]
    class Reader:
        def readline(self,*_args):return messages.get(timeout=3)
    process=SimpleNamespace(stdout=Reader(),stdin=io.BytesIO(),poll=lambda:0 if stop.is_set() else None)
    def spawn(*_args,**_kwargs):
        assert 0<_args[1].get('startup_seconds',0)<=2,'server queue must also have the original permit bound'
        def arrive():
            if ready:
                time.sleep(.1)
                if ready=='eof':messages.put(b'');return
                messages.put((json.dumps(dict(event='remote_started',job_id='f'*32 if ready=='wrong_job' else harness.job_id,
                    image='wrong-image' if ready=='wrong_image' else config.remote_harness['image'],pid=123,
                    container='paperflow-harness-'+harness.identity['namespace'][:12]+'-'+harness.job_id))+'\n').encode())
        threading.Thread(target=arrive,daemon=True).start();return process
    def rpc(method,timeout=None,**_params):
        assert method=='initialize' and timeout==60
        assert getattr(harness,'launch_ready',threading.Event()).is_set(),'initialization timer must start after queueing'
        initialized.append(True);return dict(initialized=True)
    monkeypatch.setattr('paperflow.remote_harness.spawn',spawn)
    monkeypatch.setattr('paperflow.remote_harness.request',lambda _s,h,**_k:dict(pid=123,container='fixture') if h['op']=='status_job' else dict(prepared=True))
    monkeypatch.setattr(harness,'_rpc',rpc)
    def cleanup():
        harness.stopped.set();stop.set();messages.put(b'')
    return harness,client,initialized,cleanup


def test_sdk_initialization_starts_only_after_the_container_leaves_the_queue(tmp_path,monkeypatch):
    harness,client,initialized,cleanup=setup(tmp_path,monkeypatch)
    try:
        harness.start()
        assert harness.started and initialized==[True]
    finally:cleanup()


def test_pause_cancels_a_queued_start_without_initializing_the_sdk(tmp_path,monkeypatch):
    harness,client,initialized,cleanup=setup(tmp_path,monkeypatch,ready=False)
    timer=threading.Timer(.08,client.stopped.set);timer.start()
    try:
        with pytest.raises(ValueError):harness.start()
        assert not initialized and not harness.started
    finally:timer.cancel();cleanup()


@pytest.mark.parametrize('failure',['eof','wrong_job','wrong_image'])
def test_an_ended_or_mismatched_startup_stream_cannot_initialize(tmp_path,monkeypatch,failure):
    harness,client,initialized,cleanup=setup(tmp_path,monkeypatch,ready=failure)
    try:
        with pytest.raises(ConnectionError):harness.start()
        assert not initialized and not harness.started
    finally:cleanup()


def test_queue_wait_remains_bounded_by_the_original_model_timeout(tmp_path,monkeypatch):
    harness,client,initialized,cleanup=setup(tmp_path,monkeypatch,ready=False);harness.config.model_timeout=.04
    try:
        with pytest.raises(TimeoutError):harness.start()
        assert not initialized
    finally:cleanup()
