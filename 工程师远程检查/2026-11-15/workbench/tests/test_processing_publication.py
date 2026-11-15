import threading
import time

import pytest

from paperflow.package import load
from test_processing_graph import graph_case,run_worker,model_calls
from test_winjob import wait_until


def markers(c,t):
    return list((c.root/'work'/t['task_id']/'phase-cache').glob('*/*.json'))


def test_pause_before_cache_final_qualification_never_publishes(graph_case,monkeypatch):
    c,s,t,m,_=graph_case
    entered=threading.Event();release=threading.Event()
    original=m.processing.qualify
    def wait_then_qualify(*args,**kwargs):
        entered.set();assert release.wait(5)
        return original(*args,**kwargs)
    monkeypatch.setattr(m.processing,'qualify',wait_then_qualify)
    ex=run_worker(graph_case,wait=False)
    try:
        wait_until(entered.is_set,seconds=20)
        m.pause('before final cache qualification');release.set()
        wait_until(lambda:ex.exit_saved)
        assert markers(c,t)==[]
    finally:release.set()


def test_qualified_complete_cache_survives_pause_before_marker_write(graph_case,monkeypatch):
    import paperflow.processing_control as module
    c,s,t,m,_=graph_case
    entered=threading.Event();release=threading.Event()
    original=module.publish_cache
    def wait_then_publish(*args,**kwargs):
        entered.set();assert release.wait(5)
        return original(*args,**kwargs)
    monkeypatch.setattr(module,'publish_cache',wait_then_publish)
    ex=run_worker(graph_case,wait=False)
    try:
        wait_until(entered.is_set,seconds=20)
        m.pause('complete fact already qualified');release.set()
        wait_until(lambda:ex.exit_saved)
        assert len(markers(c,t))==1
        assert load(markers(c,t)[0])['qualification']['execution_id']==ex.execution_id
    finally:release.set()
    monkeypatch.setattr(module,'publish_cache',original)
    m.resume();run_worker(graph_case)
    assert model_calls(c).count('round1')==1


def test_slow_file_verification_keeps_ipc_heartbeats_and_pause_responsive(graph_case,monkeypatch):
    c,s,t,m,_=graph_case
    entered=threading.Event();release=threading.Event()
    original=m.processing.cache
    def wait_then_verify(*args,**kwargs):
        entered.set();assert release.wait(8)
        return original(*args,**kwargs)
    monkeypatch.setattr(m.processing,'cache',wait_then_verify)
    ex=run_worker(graph_case,wait=False)
    try:
        wait_until(entered.is_set,seconds=20)
        before=ex.last_heartbeat
        time.sleep(2.3)
        assert ex.eligible and ex.last_heartbeat>before+1
        release.set()
        wait_until(lambda:ex.exit_saved,seconds=20)
        assert load(c.root/'worker-result.json')['stage']=='handed_off'
    finally:release.set()


def test_pending_qualified_publication_prevents_restart_and_takeover(graph_case,monkeypatch):
    import paperflow.processing_control as module
    c,s,t,m,_=graph_case
    entered=threading.Event();release=threading.Event()
    original=module.publish_cache
    def wait_then_publish(*args,**kwargs):
        entered.set();assert release.wait(6)
        return original(*args,**kwargs)
    monkeypatch.setattr(module,'publish_cache',wait_then_publish)
    ex=run_worker(graph_case,wait=False)
    try:
        wait_until(entered.is_set,seconds=20)
        m.pause('qualified fact persistence pending')
        wait_until(lambda:ex.exit_saved)
        assert m.status()['live_units']==1
        with pytest.raises(ValueError):m.resume()
        with pytest.raises(TimeoutError):m.shutdown(timeout=.1)
    finally:
        release.set();wait_until(lambda:not m.publications)
