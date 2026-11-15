"""Cross-caller Docker pressure and truthful timeout recovery at the real Gateway."""
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import io
import json
import os
import subprocess
import threading
import time
import uuid

import pytest

from paperflow.remote_gateway import Gateway,save


def fixture(tmp_path,monkeypatch,count=6):
    locks=defaultdict(threading.Lock);guard=threading.Lock();state=dict(active=0,peak=0,removed=[],present={})
    headers=[];gateways=[];targets=[]
    @contextmanager
    def locked(path=None,*,blocking=True):
        lock=locks[str(path or tmp_path/'.lock')]
        if not lock.acquire(blocking=blocking):raise BlockingIOError()
        try:yield
        finally:lock.release()
    for _ in range(count):
        h=dict(namespace='a'*64,execution_id=uuid.uuid4().hex,epoch='c'*32,job_id=uuid.uuid4().hex,image='sha256:'+'1'*64)
        g=Gateway(tmp_path,dict(image=h['image'],max_live=120))
        if os.name=='nt':g.locked=locked
        target=g.validate(h)/h['job_id'];target.mkdir(parents=True)
        save(target/'intent.json',{k:h[k] for k in ('namespace','execution_id','epoch','job_id')})
        save(target/'creation.json',dict(state='done'))
        (target/'data/output').mkdir(parents=True);(target/'data/output/result').write_bytes(b'LLM_UNCHANGED')
        cid=hashlib.sha256(h['job_id'].encode()).hexdigest()
        state['present'][cid]=dict(Id=cid,Name='/'+g.name(h),Image=h['image'],
            Config=dict(Labels={'paperflow.'+k:h[k] for k in ('namespace','execution_id','epoch','job_id')}),
            State=dict(Status='running',Running=True,Pid=123))
        headers.append(h);gateways.append(g);targets.append(target)
    def docker(args,**kwargs):
        with guard:state['active']+=1;state['peak']=max(state['peak'],state['active'])
        try:
            time.sleep(.025)
            if args[:2]==['docker','inspect']:
                with guard:
                    item=next((v for k,v in state['present'].items() if args[2] in (k,v['Name'].lstrip('/'))),None)
                return subprocess.CompletedProcess(args,0,json.dumps([item]),'') if item else subprocess.CompletedProcess(args,1,'','No such object')
            if args[:3]==['docker','rm','--force']:
                with guard:
                    item=next(v for k,v in state['present'].items() if args[3] in (k,v['Name'].lstrip('/')))
                    state['removed'].append(args[3]);state['present'].pop(item['Id'])
                return subprocess.CompletedProcess(args,0,b'',b'')
            assert args[:2]==['docker','ps']
            return subprocess.CompletedProcess(args,0,'','')
        finally:
            with guard:state['active']-=1
    monkeypatch.setattr(subprocess,'run',docker)
    return gateways,headers,targets,state,docker


def test_status_burst_is_bounded_across_independent_gateway_callers(tmp_path,monkeypatch):
    gateways,headers,_,state,_=fixture(tmp_path,monkeypatch,8)
    with ThreadPoolExecutor(8) as pool:
        values=list(pool.map(lambda pair:pair[0].status_job(pair[1]),zip(gateways,headers)))
    assert len(values)==8 and state['peak']<=2,'authenticated SSH sessions must not all inspect Docker together'


def test_cleanup_burst_keeps_the_same_global_docker_bound(tmp_path,monkeypatch):
    gateways,headers,targets,state,_=fixture(tmp_path,monkeypatch,8)
    with ThreadPoolExecutor(8) as pool:
        values=list(pool.map(lambda pair:pair[0].reap_execution(pair[1]),zip(gateways,headers)))
    assert all(v['all_exited'] for v in values) and state['peak']<=2
    assert len(state['removed'])==8 and all(len(cid)==64 for cid in state['removed'])
    assert all((p/'data/output/result').read_bytes()==b'LLM_UNCHANGED' for p in targets)


def test_pause_closes_a_status_request_that_was_already_waiting(tmp_path,monkeypatch):
    gateways,headers,targets,state,original=fixture(tmp_path,monkeypatch,2)
    entered=threading.Event();release=threading.Event();queried=[]
    def docker(args,**kwargs):
        if args[:2]==['docker','inspect']:
            queried.append(args[2])
            if args[2]==gateways[0].name(headers[0]):entered.set();assert release.wait(2)
        return original(args,**kwargs)
    monkeypatch.setattr(subprocess,'run',docker)
    with ThreadPoolExecutor(2) as pool:
        first=pool.submit(gateways[0].status_job,headers[0]);assert entered.wait(1)
        second=pool.submit(gateways[1].status_job,headers[1]);time.sleep(.06)
        save(targets[1].parent/'closed.json',dict(epoch=headers[1]['epoch']))
        release.set();first.result(timeout=2)
        with pytest.raises(ValueError):second.result(timeout=2)
    assert gateways[1].name(headers[1]) not in queried


def test_cleanup_has_capacity_reserved_from_a_blocked_status_query(tmp_path,monkeypatch):
    gateways,headers,_,state,original=fixture(tmp_path,monkeypatch,2)
    entered=threading.Event();release=threading.Event()
    def docker(args,**kwargs):
        if args[:2]==['docker','inspect'] and args[2]==gateways[0].name(headers[0]):
            entered.set();assert release.wait(2)
        return original(args,**kwargs)
    monkeypatch.setattr(subprocess,'run',docker)
    with ThreadPoolExecutor(2) as pool:
        first=pool.submit(gateways[0].status_job,headers[0]);assert entered.wait(1)
        second=pool.submit(gateways[1].reap_job,headers[1])
        try:assert second.result(timeout=1)['all_exited']
        finally:release.set();first.result(timeout=2)


def test_delete_cli_timeout_is_followed_by_actual_id_and_name_readback(tmp_path,monkeypatch):
    gateways,headers,_,state,original=fixture(tmp_path,monkeypatch,1)
    def docker(args,**kwargs):
        result=original(args,**kwargs)
        if args[:2]==['docker','rm']:raise subprocess.TimeoutExpired(args,6)
        return result
    monkeypatch.setattr(subprocess,'run',docker)
    proof=gateways[0].reap_execution(headers[0])
    assert proof['all_exited'] and len(state['removed'])==1 and not state['present']


@pytest.mark.parametrize('failure',['timeout','in_progress'])
def test_delete_in_progress_is_observed_without_sending_another_delete(tmp_path,monkeypatch,failure):
    gateways,headers,_,state,original=fixture(tmp_path,monkeypatch,1);commands=[];timers=[]
    def docker(args,**kwargs):
        if args[:2]==['docker','rm']:
            commands.append(args)
            timer=threading.Timer(.08,state['present'].clear);timer.start();timers.append(timer)
            if failure=='timeout':raise subprocess.TimeoutExpired(args,6)
            raise subprocess.CalledProcessError(1,args,stderr='removal of container is already in progress')
        return original(args,**kwargs)
    monkeypatch.setattr(subprocess,'run',docker)
    try:assert gateways[0].reap_execution(headers[0])['all_exited']
    finally:
        for timer in timers:timer.join()
    assert len(commands)==1 and len(commands[0][3])==64


def test_ambiguous_delete_intent_survives_retry_and_never_reissues_rm(tmp_path,monkeypatch):
    gateways,headers,targets,state,original=fixture(tmp_path,monkeypatch,1);commands=[]
    headers[0]['control_budget']=dict(queue_seconds=.3,operation_seconds=.18)
    def docker(args,**kwargs):
        if args[:2]==['docker','rm']:
            commands.append(args);raise subprocess.TimeoutExpired(args,6)
        return original(args,**kwargs)
    monkeypatch.setattr(subprocess,'run',docker)
    with pytest.raises((TimeoutError,ConnectionError)):gateways[0].reap_job(headers[0])
    assert (targets[0]/'removal.json').exists() and not (targets[0]/'exit.json').exists()
    state['present'].clear()
    assert gateways[0].reap_job(headers[0])['all_exited'] and len(commands)==1


def test_name_disappearance_does_not_prove_the_original_id_disappeared(tmp_path,monkeypatch):
    gateways,headers,targets,state,original=fixture(tmp_path,monkeypatch,1)
    headers[0]['control_budget']=dict(queue_seconds=.3,operation_seconds=.2)
    def docker(args,**kwargs):
        if args[:2]==['docker','rm']:
            item=next(iter(state['present'].values()));item['Name']='/moved-away'
            return subprocess.CompletedProcess(args,0,b'',b'')
        return original(args,**kwargs)
    monkeypatch.setattr(subprocess,'run',docker)
    with pytest.raises((TimeoutError,ConnectionError,ValueError)):gateways[0].reap_job(headers[0])
    assert not (targets[0]/'exit.json').exists()


def test_failure_to_acquire_delete_capacity_cannot_persist_a_sent_delete(tmp_path,monkeypatch):
    gateways,headers,targets,state,_=fixture(tmp_path,monkeypatch,1);g=gateways[0]
    original=g.control_slot;attempts=0
    @contextmanager
    def slot():
        nonlocal attempts
        attempts+=1
        if attempts==2:raise TimeoutError('delete never reached command dispatch')
        with original() as budget:yield budget
    monkeypatch.setattr(g,'control_slot',slot)
    with pytest.raises(TimeoutError):g.reap_job(headers[0])
    assert state['removed']==[] and not (targets[0]/'removal.json').exists()
    assert g.reap_job(headers[0])['all_exited'] and len(state['removed'])==1


def test_startup_inspect_queue_stops_when_its_channel_closes(tmp_path,monkeypatch):
    gateways,headers,_,_,_=fixture(tmp_path,monkeypatch,1);g=gateways[0];h=headers[0]
    stopped=threading.Event();acquired=threading.Event();release=threading.Event()
    def check(*_):
        if stopped.is_set():raise ConnectionError('startup input closed')
    monkeypatch.setattr(g,'check_startup',check)
    def holder():
        with g.control_scope(),g.control_slot():acquired.set();release.wait(2)
    with ThreadPoolExecutor(2) as pool:
        held=pool.submit(holder);assert acquired.wait(1)
        inspected=pool.submit(g.startup_inspect,h,None,time.monotonic()+1,g.name(h))
        try:
            time.sleep(.06);stopped.set()
            with pytest.raises(ConnectionError):inspected.result(timeout=.5)
        finally:release.set();held.result(timeout=1)


def test_unconfirmed_daemon_removals_keep_capacity_after_their_callers_exit(tmp_path,monkeypatch):
    gateways,headers,targets,state,original=fixture(tmp_path,monkeypatch,3);commands=[]
    for h in headers:h['control_budget']=dict(queue_seconds=.18,operation_seconds=.25)
    def docker(args,**kwargs):
        if args[:2]==['docker','rm']:
            commands.append(args[3]);raise subprocess.TimeoutExpired(args,6)
        return original(args,**kwargs)
    monkeypatch.setattr(subprocess,'run',docker)
    for index in (0,1,2):
        with pytest.raises(TimeoutError):gateways[index].reap_job(headers[index])
    assert len(commands)==2,'two unknown daemon removals must prevent a third different delete'
    assert not (targets[2]/'removal.json').exists()
    state['present'].pop(commands[0]);headers[2]['control_budget']=dict(queue_seconds=.3,operation_seconds=.25)
    with pytest.raises(TimeoutError):gateways[2].reap_job(headers[2])
    assert len(commands)==3,'actual disappearance releases one persistent cleanup place'


def test_slow_intent_fsync_cannot_dispatch_an_expired_delete(tmp_path,monkeypatch):
    import paperflow.remote_gateway as module
    gateways,headers,targets,state,_=fixture(tmp_path,monkeypatch,1)
    h=dict(headers[0],control_budget=dict(queue_seconds=.3,operation_seconds=.09));original=module.save
    def slow(path,value):
        original(path,value)
        if path.name=='removal.json':time.sleep(.12)
    monkeypatch.setattr(module,'save',slow)
    with pytest.raises(TimeoutError):gateways[0].reap_job(h)
    assert not state['removed'] and not (targets[0]/'removal.json').exists()
    assert not list((tmp_path/'.cleanup').glob('*.json'))
