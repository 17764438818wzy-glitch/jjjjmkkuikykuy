import json
from multiprocessing.connection import Client
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from paperflow.control import Manager
from paperflow.store import Store
from test_winjob import wait_until


WORKER='''
import json,sys,time
from pathlib import Path
from paperflow.control_ipc import WorkerClient
client=WorkerClient.from_environment(); root=Path(sys.argv[1]);root.mkdir(exist_ok=True)
(root/'ready').touch()
while not (root/'go').exists() and not client.stopped.wait(.01):pass
try:
    def work(cancel):
        (root/'performed').touch()
        while not (root/'release').exists() and not cancel.wait(.01):pass
        return {'done':True}
    client.perform(sys.argv[2],{},work,total_timeout=30)
    client.report_execution('complete',{})
except Exception as exc:
    (root/'denied').write_text(type(exc).__name__)
finally:client.close()
'''


def worker(m,tmp_path,name,kind='llm'):
    root=tmp_path/name;root.mkdir()
    ex=m.launch('delivery' if kind.startswith('rag') else 'graph',name,
        [sys.executable,'-X','utf8','-c',WORKER,str(root),kind],cwd=Path.cwd(),total_timeout=40)
    wait_until(lambda:(root/'ready').exists())
    return ex,root


@pytest.mark.parametrize('kind',['llm','local','rag_read','rag_write'])
def test_resource_busy_exits_without_network_barrier_or_slot_leak(tmp_path,kind):
    m=Manager(Store(tmp_path/'state.sqlite3'),tmp_path/'manager',resource_limits={'llm':1,'local':1,'rag':1}).start()
    m.resume()
    try:
        first,a=worker(m,tmp_path,'a',kind);second,b=worker(m,tmp_path,'b',kind)
        (a/'go').touch();wait_until(lambda:(a/'performed').exists())
        (b/'go').touch();wait_until(lambda:(b/'denied').exists())
        assert (b/'denied').read_text()=='ActionDeferred'
        wait_until(lambda:second.exit_saved)
        assert not (b/'performed').exists()
        assert not m.ledger.rows("SELECT * FROM control_actions WHERE execution_id=? AND state='unresolved'",(second.execution_id,))
        lane='rag' if kind.startswith('rag') else kind
        assert m.status()['resources'][lane]['used']==1
        (a/'release').touch();wait_until(lambda:first.exit_saved)
        assert m.status()['resources'][lane]['used']==0
        assert m.ledger.rows('SELECT * FROM control_execution_results WHERE execution_id=?',(first.execution_id,))[0]['outcome']=='complete'
    finally:m.shutdown()


def test_pause_cancels_resource_wait_and_global_worker_cap(tmp_path):
    m=Manager(Store(tmp_path/'state.sqlite3'),tmp_path/'manager',max_live_units=1).start();m.resume()
    try:
        ex,root=worker(m,tmp_path,'first')
        from paperflow.scheduling_policy import ActionDeferred
        with pytest.raises(ActionDeferred,match='unit_capacity'):
            m.launch('graph','second',[sys.executable,'-c','pass'],cwd=Path.cwd())
        m.pause();wait_until(lambda:ex.exit_saved)
        assert not (root/'performed').exists()
        assert all(v['used']==0 for v in m.status()['resources'].values())
    finally:m.shutdown()


def test_history_and_closed_peers_are_bounded_but_sql_facts_remain(tmp_path):
    m=Manager(Store(tmp_path/'state.sqlite3'),tmp_path/'manager',history_limit=3,peer_limit=4).start();m.resume()
    try:
        ids=[]
        for n in range(12):
            ex,root=worker(m,tmp_path,str(n),'local');ids.append(ex.execution_id)
            (root/'release').touch();(root/'go').touch();wait_until(lambda:ex.exit_saved)
        wait_until(lambda:len(m.executions)<=3 and not m.server.peers)
        assert len(m.tickets)<=3 and len(m.events)<=3 and len(m.errors)<=3
        assert len(m.scopes)<=5
        events=list(m.events)
        assert len({e['sequence'] for e in events})==len(events) and events[-1]['sequence']>12
        assert len(m.ledger.rows("SELECT * FROM control_executions WHERE state='exited'"))==12
        assert len(m.ledger.rows('SELECT * FROM control_execution_results'))==12
    finally:m.shutdown()


def test_untrusted_connections_are_capped_and_released(tmp_path):
    m=Manager(Store(tmp_path/'state.sqlite3'),tmp_path/'manager',peer_limit=2).start();m.resume()
    connections=[]
    try:
        for _ in range(6):
            connections.append(Client(m.server.address,family='AF_PIPE',authkey=None))
        time.sleep(.1)
        assert len(m.server.peers)<=2
        for c in connections:c.close()
        wait_until(lambda:not m.server.peers)
        ex,root=worker(m,tmp_path,'valid','local');(root/'release').touch();(root/'go').touch()
        wait_until(lambda:ex.exit_saved)
        assert (root/'performed').exists()
    finally:
        for c in connections:c.close()
        m.shutdown()


@pytest.mark.parametrize('owner',['starting','publication','handoff_permit'])
def test_history_trim_keeps_scope_during_launch_and_publication(tmp_path,owner):
    m=Manager(Store(tmp_path/'state.sqlite3'),tmp_path/'manager')
    m.scopes['unit:pending']=dict(generation=1,state={},ready=True)
    if owner=='starting':m.starting['id']=dict(scope='unit:pending')
    elif owner=='publication':m.publications.add('id')
    else:
        m.executions['id']=SimpleNamespace(execution_id='id',scope='unit:graph',exit_saved=False,peer=None,active_action={'action_id':'a'})
        m.tickets['a']=dict(execution_id='id',snapshot={'scopes':{}},request={'resource':{'operation':'handoff_commit'}})
    m._trim_history()
    assert 'unit:pending' in m.scopes


def test_diagnostic_history_keeps_json_compatibility(tmp_path):
    m=Manager(Store(tmp_path/'state.sqlite3'),tmp_path/'manager',history_limit=2)
    assert json.loads(json.dumps(dict(events=m.events,errors=m.errors)))==dict(events=[],errors=[])


def test_publication_keeps_local_slot_until_handler_is_done(tmp_path):
    m=Manager(Store(tmp_path/'state.sqlite3'),tmp_path/'manager')
    ex=SimpleNamespace(execution_id='id',scope='unit:x',exit_saved=True,peer=None,active_action={'action_id':'action'})
    m.executions['id']=ex;m.publications.add('id')
    m.resource_holds['action']=dict(lane='local',execution_id='id')
    m._trim_history();assert m.status()['resources']['local']['used']==1
    m.publications.clear();m._trim_history()
    assert m.status()['resources']['local']['used']==0


def test_failed_scope_cache_cannot_grow_without_bound(tmp_path):
    m=Manager(Store(tmp_path/'state.sqlite3'),tmp_path/'manager',history_limit=2).start();m.resume()
    try:
        m.scopes.update({f'unit:failed:{i}':dict(generation=1,state={},ready=False) for i in range(m.scope_limit)})
        from paperflow.scheduling_policy import ActionDeferred
        with pytest.raises(ActionDeferred,match='scope_capacity'):
            m.change_admission_state('unit:new','change',dict(frozen=True))
        assert 'unit:new' not in m.scopes
    finally:m.shutdown()
