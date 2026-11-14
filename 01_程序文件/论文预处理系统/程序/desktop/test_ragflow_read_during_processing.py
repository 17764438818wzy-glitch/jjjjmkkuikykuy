"""A read-only library refresh must not be blocked by local preprocessing."""
import io
import json
import threading
import time
from urllib.error import HTTPError

import pytest

from ragflow_client import RagflowClient
from test_script_task_ui import ui


def test_read_library_buttons_remain_usable_while_processing(ui):
    _,app=ui
    app.busy=True
    app.task_kind='processing'
    app.update_controls()
    assert not app.task_connect_button.instate(['disabled'])
    assert not app.rag_connect_button.instate(['disabled'])
    assert not app.rag_connection_button.instate(['disabled'])
    assert app.stop_button.instate(['!disabled'])


def test_read_library_during_processing_updates_list_without_finishing_processing(ui,monkeypatch):
    root,app=ui
    app.url.set('http://localhost')
    app.key.set('synthetic-rag-key')
    app.busy=True
    app.task_kind='processing'
    token=object()
    app._active_task=token
    started=time.monotonic()
    app.task_started=started
    app.stage_started=started
    app.state.set('processing progress remains visible')
    frozen={'task_datasets':{'paper':{'id':'fixed-paper'},'image':{'id':'fixed-image'}}}
    app.processor.config=frozen
    entered=threading.Event()
    calls=[]

    class Transport:
        def open(self,request,timeout):
            calls.append(request.get_method())
            entered.set()
            return io.BytesIO(json.dumps({'code':0,'data':[{'id':'one','name':'synthetic library'}]}).encode())

    def client(base,key):
        instance=RagflowClient(base,key)
        instance.opener=Transport()
        return instance

    monkeypatch.setattr('app.RagflowClient',client)
    app.connect()
    assert entered.wait(1),'The actual dataset request is silently blocked by processing busy state'
    deadline=time.monotonic()+2
    while not app.datasets and time.monotonic()<deadline:
        root.update()
        time.sleep(.01)
    assert app.datasets==[{'id':'one','name':'synthetic library'}]
    assert calls==['GET']
    assert app.busy and app.task_kind=='processing' and app._active_task is token
    assert app.task_started==started
    assert app.stop_button.instate(['!disabled'])
    assert app.state.get()=='processing progress remains visible'
    assert app.processor.config==frozen


@pytest.mark.parametrize('change',['repeat','url','key','failure'])
def test_library_query_is_isolated_when_repeated_stale_or_failed(ui,monkeypatch,change):
    root,app=ui
    app.url.set('http://localhost')
    key='synthetic-private-key'
    app.key.set(key)
    app.busy=True
    app.task_kind='processing'
    token=object()
    app._active_task=token
    app.state.set('ongoing processing')
    entered,release=threading.Event(),threading.Event()
    calls=[]

    class Transport:
        def open(self,request,timeout):
            calls.append(request.get_method())
            entered.set()
            assert release.wait(3)
            if change=='failure':
                raise HTTPError(request.full_url,401,key,{},None)
            return io.BytesIO(json.dumps({'code':0,'data':[{'id':'one','name':'synthetic library'}]}).encode())

    def client(base,api_key):
        instance=RagflowClient(base,api_key)
        instance.opener=Transport()
        return instance

    monkeypatch.setattr('app.RagflowClient',client)
    app.connect()
    try:
        assert entered.wait(1)
        assert app.rag_connect_button.instate(['disabled'])
        assert app.stop_button.instate(['!disabled'])
        if change=='url':app.url.set('http://localhost/new')
        elif change=='key':app.key.set('different-synthetic-key')
        else:
            for _ in range(3):app.connect()
        assert calls==['GET']
    finally:
        release.set()
    deadline=time.monotonic()+2
    while app.rag_connect_button.instate(['disabled']) and time.monotonic()<deadline:
        root.update()
        time.sleep(.01)
    assert app.busy and app.task_kind=='processing' and app._active_task is token
    assert app.state.get()=='ongoing processing'
    assert app.stop_button.instate(['!disabled'])
    assert not app.rag_connect_button.instate(['disabled'])
    assert not app.processor.stop.is_set()
    if change=='repeat':assert app.datasets==[{'id':'one','name':'synthetic library'}]
    else:
        assert app.datasets==[]
        assert ('认证失败' if change=='failure' else '连接参数已变更') in app.connection.get()
    assert key not in app.connection.get()


def test_failed_query_worker_launch_does_not_permanently_disable_reading(ui,monkeypatch):
    _,app=ui
    app.url.set('http://localhost')
    app.key.set('synthetic-key')
    app.busy=True
    app.task_kind='processing'
    processing_token=object()
    app._active_task=processing_token
    errors=[]
    monkeypatch.setattr(app,'error',lambda error:errors.append(str(error)))
    def fail_start(thread):raise RuntimeError('synthetic thread launch failure')
    monkeypatch.setattr(threading.Thread,'start',fail_start)
    app.connect()
    assert errors==['synthetic thread launch failure']
    assert not app.task_connect_button.instate(['disabled'])
    assert not app.rag_connect_button.instate(['disabled'])
    assert not app.rag_connection_button.instate(['disabled'])
    assert app.busy and app.task_kind=='processing' and app._active_task is processing_token
    assert app.stop_button.instate(['!disabled'])
