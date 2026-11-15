import json
from pathlib import Path
import sys
import threading
import time

import pytest

from paperflow.control import Manager
from paperflow.store import Store
from paperflow.remote_runner import Lease, compact_result
from paperflow.remote_harness import current_permit
from test_winjob import wait_until


def test_lease_total_loss_bound_and_unique_turns():
    lease=Lease(now=100)
    assert not lease.expired(102.9)
    assert lease.expired(103)
    lease.heartbeat(102)
    assert not lease.expired(104)
    lease.action('one',102,20)
    with pytest.raises(ValueError):lease.action('one',103,20)
    with pytest.raises(ValueError):lease.action('two',103,0)
    assert lease.hard_exit_seconds==5


def test_result_does_not_send_transcript_to_pc():
    class Result:
        finish_reason='completed'
        events=[dict(type='assistant/message',data=dict(usage=dict(inputTokens=9),message={'content':'private manuscript'})),
                dict(type='tool/result',data={'content':'large document'}),
                dict(type='turn/end',data={'reason':{'kind':'completed'}})]
    result=compact_result(Result())
    assert result['finish_reason']=='completed'
    assert result['events'][0]['data']=={'usage':{'inputTokens':9}}
    assert 'private manuscript' not in json.dumps(result)
    assert 'large document' not in json.dumps(result)


def test_native_runtime_cache_is_in_writable_owned_home(tmp_path,monkeypatch):
    from paperflow.remote_runner import Runner
    captured={}
    class SDK:
        def __init__(self,**kwargs):captured.update(kwargs)
        def start(self):pass
    monkeypatch.setattr('deepseek_harness.DeepSeekHarness',SDK)
    (tmp_path/'job.json').write_text(json.dumps(dict(provider='free',model='test',model_timeout=30,env={})))
    runner=Runner(tmp_path);runner.emit=lambda _value:None
    runner.call(dict(id='one',method='initialize'))
    assert captured['env']['PKG_NATIVE_CACHE_PATH']=='/job/home/native-cache'
    assert captured['env']['NARB_NATIVE_CACHE_DIR']=='/job/home/native-cache'


def test_remote_call_requires_current_llm_permit():
    class Client:
        epoch='current';execution_id='execution';active_kind='llm'
        stopped=threading.Event();closed=threading.Event()
        active_permit=dict(epoch='current',action_id='one',deadline=time.monotonic()+10)
    client=Client()
    assert current_permit(client)['action_id']=='one'
    client.stopped.set()
    with pytest.raises(ValueError):current_permit(client)
    client.stopped.clear();client.active_kind='local'
    with pytest.raises(ValueError):current_permit(client)
    client.active_kind='llm';client.active_permit=dict(epoch='old',action_id='one',deadline=time.monotonic()+10)
    with pytest.raises(ValueError):current_permit(client)


def test_waiting_for_next_llm_permit_keeps_idle_session_alive():
    from paperflow.remote_harness import RemoteHarness
    class Client:
        stopped=threading.Event();closed=threading.Event()
        active_kind='llm';active_permit=None
    harness=object.__new__(RemoteHarness);harness.client=Client();harness.stopped=threading.Event()
    sent=threading.Event();harness._send=lambda _value:sent.set()
    thread=threading.Thread(target=harness._heartbeat);thread.start()
    try:
        assert sent.wait(.9),'waiting for a permit is not a cancelled remote turn'
        assert not harness.stopped.is_set()
    finally:harness.stopped.set();thread.join(1)


def test_local_tree_exit_does_not_retire_remote_owner(tmp_path):
    manager=Manager(Store(tmp_path/'state.sqlite3'),tmp_path/'manager').start()
    ready=threading.Event()
    class External:
        def register_sql(self,*_args):pass
        def exit_proof(self,*_args):return {'all_exited':True,'execution_closed':True} if ready.is_set() else None
        def status(self):return {'state':'pending'}
        def close(self):pass
    manager.external=External()
    manager.resume()
    try:
        ex=manager.launch('graph','synthetic',[sys.executable,'-c','pass'],cwd=Path.cwd())
        wait_until(lambda:ex.job.active_count()==0)
        manager.pause('synthetic remote pending')
        time.sleep(.15)
        assert not ex.exit_saved
        assert manager.status()['live_units']==1
        with pytest.raises(ValueError):manager.resume()
        ready.set();wait_until(lambda:ex.exit_saved)
        assert ex.exit_evidence['remote']['all_exited']
    finally:
        ready.set();manager.shutdown()


def test_changed_endpoint_cannot_reconcile_old_remote_owner(monkeypatch):
    from paperflow.remote_control import RemoteExecutionGuard
    from paperflow.handoff_store import canonical
    guard=object.__new__(RemoteExecutionGuard)
    guard.namespace='domain';guard.settings={'image':'same-image'};guard.endpoint={'host':'new-server'}
    row=dict(epoch='old',namespace='domain',image='same-image',state='owned',endpoint=canonical({'host':'old-server'}))
    class Ledger:
        def rows(self,*_):return [row]
        def _write(self,_):raise AssertionError('must not mark original owner exited')
    guard.manager=type('Manager',(),{'ledger':Ledger()})()
    monkeypatch.setattr('paperflow.remote_control.request',lambda *_a,**_k:dict(execution_id='execution',epoch='old',all_exited=True,execution_closed=True))
    with pytest.raises(ValueError,match='endpoint'):guard.reap_sync('execution','old')
