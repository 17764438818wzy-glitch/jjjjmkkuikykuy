"""The startup gate bounds Docker work, not the life of a model session."""
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import io
import json
import os
import subprocess
import threading
import tempfile
import time
import uuid

import pytest

from paperflow.remote_gateway import Gateway


def setup(tmp_path,monkeypatch,count=6):
    config=dict(image='sha256:'+'1'*64,max_live=160)
    locks=defaultdict(threading.Lock);guard=threading.Lock();release=threading.Event();all_live=threading.Event()
    state=dict(creating=0,peak_creating=0,live=0,peak_live=0,created=[],removed=[])
    containers={};gateways=[];headers=[];outputs=[]
    @contextmanager
    def locked(path=None,blocking=True):
        lock=locks[str(path or tmp_path/'.lock')]
        if not lock.acquire(blocking=blocking):raise BlockingIOError()
        try:yield
        finally:lock.release()
    for index in range(count):
        h=dict(namespace='a'*64,execution_id=uuid.uuid4().hex,epoch='c'*32,job_id=uuid.uuid4().hex,image=config['image'])
        g=Gateway(tmp_path,config)
        if os.name=='nt':g.locked=locked
        target=g.validate(h)/h['job_id'];target.mkdir(parents=True)
        (target/'manifest.json').write_text('[]');(target/'intent.json').write_text(json.dumps({k:h[k] for k in ('namespace','execution_id','epoch','job_id')}))
        out=target/'data/output';out.mkdir(parents=True);(out/'result.txt').write_bytes(b'EXISTING_RESULT')
        gateways.append(g);headers.append(h);outputs.append(io.BytesIO())
    def inspect(name):
        with guard:
            value=containers.get(name)
            if value is None:value=next((v for v in containers.values() if v['Id']==name),None)
            return value
    for g in gateways:
        monkeypatch.setattr(g,'inspect',inspect)
        monkeypatch.setattr(g,'inventory',lambda _h:[])
    def run(args,**_kwargs):
        if args[:2]==['docker','create']:
            name=args[args.index('--name')+1];h=next(h for h in headers if h['job_id'] in name)
            with guard:state['creating']+=1;state['peak_creating']=max(state['peak_creating'],state['creating']);state['created'].append(h['job_id'])
            time.sleep(.10)
            with guard:
                containers[name]=dict(Id=hash_id(h),Name='/'+name,Image=config['image'],Config=dict(Labels={'paperflow.'+k:h[k] for k in ('namespace','execution_id','epoch','job_id')}),State=dict(Status='created',Running=False,Pid=0))
                state['creating']-=1
            return subprocess.CompletedProcess(args,0,hash_id(h).encode()+b'\n',b'')
        assert args[:3]==['docker','rm','--force']
        with guard:
            name=next(k for k,v in containers.items() if args[3] in (k,v['Id']))
            state['removed'].append(containers[name]['Id']);del containers[name]
        return subprocess.CompletedProcess(args,0,b'',b'')
    class Process:
        def __init__(self,args,**_kwargs):
            assert args[:4]==['docker','start','--attach','--interactive']
            self.name=next(k for k,v in containers.items() if v['Id']==args[4])
            with guard:
                containers[self.name]['State']=dict(Status='running',Running=True,Pid=123)
                state['live']+=1;state['peak_live']=max(state['peak_live'],state['live'])
                if state['live']==count:all_live.set()
        def poll(self):return 0 if release.is_set() else None
        def wait(self,*_args,**_kwargs):
            assert release.wait(4),'startup gate was held for an entire model session'
            with guard:state['live']-=1
            return 0
    monkeypatch.setattr(subprocess,'run',run);monkeypatch.setattr(subprocess,'Popen',Process)
    return gateways,headers,outputs,state,release,all_live


def hash_id(h):
    import hashlib
    return hashlib.sha256(h['job_id'].encode()).hexdigest()


def test_startup_queue_releases_its_slots_before_model_sessions_finish(tmp_path,monkeypatch):
    gateways,headers,outputs,state,release,all_live=setup(tmp_path,monkeypatch)
    with ThreadPoolExecutor(6) as pool:
        futures=[pool.submit(g.run,h,tempfile.TemporaryFile(),out) for g,h,out in zip(gateways,headers,outputs)]
        try:
            assert all_live.wait(3),'independent model sessions must all be able to remain live'
            assert state['peak_creating']<=2,'the SSH ACK must not release the Docker startup gate'
            assert state['peak_live']==6
        finally:release.set()
        for future in futures:future.result(timeout=4)
    assert len(state['removed'])==6
    for h,out in zip(headers,outputs):
        values=[json.loads(line) for line in out.getvalue().splitlines()]
        assert any(v.get('event')=='remote_started' and v.get('job_id')==h['job_id'] for v in values)


def test_a_queued_request_cannot_create_after_its_exit_was_confirmed(tmp_path,monkeypatch):
    gateways,headers,outputs,state,release,all_live=setup(tmp_path,monkeypatch,count=3)
    entered=threading.Event();allow_create=threading.Event();original=subprocess.run
    def stalled(args,**kwargs):
        if args[:2]==['docker','create']:
            entered.set();assert allow_create.wait(3)
        return original(args,**kwargs)
    monkeypatch.setattr(subprocess,'run',stalled)
    with ThreadPoolExecutor(3) as pool:
        first=[pool.submit(gateways[i].run,headers[i],tempfile.TemporaryFile(),outputs[i]) for i in (0,1)]
        assert entered.wait(1)
        # Both startup slots must be occupied before the third request arrives.
        time.sleep(.05)
        third=pool.submit(gateways[2].run,headers[2],tempfile.TemporaryFile(),outputs[2])
        try:
            time.sleep(.08)
            proof=gateways[2].reap_execution(headers[2]);assert proof['all_exited']
        finally:allow_create.set();release.set()
        for future in first:future.result(timeout=4)
        try:third.result(timeout=4)
        except ValueError:pass
    assert headers[2]['job_id'] not in state['created'],'queued creation must recheck the closed epoch/job'


def test_reaping_waits_for_the_same_jobs_create_boundary(tmp_path,monkeypatch):
    gateways,headers,outputs,state,release,all_live=setup(tmp_path,monkeypatch,count=1)
    entered=threading.Event();allow_create=threading.Event();original=subprocess.run
    def stalled(args,**kwargs):
        if args[:2]==['docker','create']:
            entered.set();assert allow_create.wait(3)
        return original(args,**kwargs)
    monkeypatch.setattr(subprocess,'run',stalled)
    with ThreadPoolExecutor(2) as pool:
        running=pool.submit(gateways[0].run,headers[0],tempfile.TemporaryFile(),outputs[0]);assert entered.wait(1)
        reaping=pool.submit(gateways[0].reap_execution,headers[0])
        try:
            time.sleep(.08)
            assert not reaping.done(),'in-flight create must share the same per-job lock with reaping'
        finally:allow_create.set();release.set()
        assert reaping.result(timeout=4)['all_exited']
        try:running.result(timeout=4)
        except ValueError:pass
    assert state['created']==[headers[0]['job_id']] and state['peak_live']==0


def test_a_duplicate_run_cannot_close_the_original_live_session(tmp_path,monkeypatch):
    gateways,headers,outputs,state,release,all_live=setup(tmp_path,monkeypatch,count=1)
    g=gateways[0];h=headers[0];original=g.startup_slot;barrier=threading.Barrier(2)
    @contextmanager
    def arrive_together(header,output,input_stream=None,**kwargs):
        barrier.wait(1)
        with original(header,output,input_stream,**kwargs):yield
    monkeypatch.setattr(g,'startup_slot',arrive_together)
    with ThreadPoolExecutor(2) as pool:
        futures=[pool.submit(g.run,h,tempfile.TemporaryFile(),io.BytesIO()) for _ in range(2)]
        try:
            assert all_live.wait(2)
            deadline=time.monotonic()+1
            while not any(f.done() for f in futures) and time.monotonic()<deadline:time.sleep(.01)
            assert any(f.done() for f in futures),'the replay should be rejected independently'
            assert state['removed']==[],'rejecting a replay must not reap the original session'
        finally:release.set()
        failures=[]
        for f in futures:
            try:f.result(timeout=3)
            except ValueError:failures.append(True)
    assert failures==[True] and len(state['created'])==1 and len(state['removed'])==1


def test_the_server_queue_deadline_does_not_allow_a_late_create(tmp_path,monkeypatch):
    gateways,headers,outputs,state,release,all_live=setup(tmp_path,monkeypatch,count=3)
    entered=threading.Event();allow_create=threading.Event();original=subprocess.run
    def stalled(args,**kwargs):
        if args[:2]==['docker','create']:
            entered.set();assert allow_create.wait(3)
        return original(args,**kwargs)
    monkeypatch.setattr(subprocess,'run',stalled)
    with ThreadPoolExecutor(3) as pool:
        first=[pool.submit(gateways[i].run,headers[i],tempfile.TemporaryFile(),outputs[i]) for i in (0,1)]
        assert entered.wait(1);time.sleep(.05);headers[2]['startup_seconds']=.05
        third=pool.submit(gateways[2].run,headers[2],tempfile.TemporaryFile(),outputs[2])
        try:time.sleep(.12)
        finally:allow_create.set();release.set()
        for f in first:f.result(timeout=4)
        with pytest.raises(TimeoutError):third.result(timeout=4)
    assert headers[2]['job_id'] not in state['created']


@pytest.mark.parametrize('seconds',[True,False,float('nan'),float('inf'),0,-1,86401,'60'])
def test_an_invalid_queue_lifetime_is_rejected_before_any_creation(tmp_path,monkeypatch,seconds):
    gateways,headers,outputs,state,release,all_live=setup(tmp_path,monkeypatch,count=1)
    headers[0]['startup_seconds']=seconds
    with pytest.raises(ValueError):gateways[0].run(headers[0],tempfile.TemporaryFile(),outputs[0])
    assert state['created']==[]


def test_a_slow_inspect_cannot_start_a_container_after_the_queue_deadline(tmp_path,monkeypatch):
    gateways,headers,outputs,state,release,all_live=setup(tmp_path,monkeypatch,count=1)
    g=gateways[0];original=g.inspect;calls=0;headers[0]['startup_seconds']=.2;release.set()
    def slow(name):
        nonlocal calls
        calls+=1
        if calls==1:time.sleep(.2)
        return original(name)
    monkeypatch.setattr(g,'inspect',slow)
    with pytest.raises(TimeoutError):g.run(headers[0],tempfile.TemporaryFile(),outputs[0])
    assert state['peak_live']==0,'a blocking inspect cannot authorize a late docker start'


def test_a_slow_running_observation_cannot_emit_an_expired_start_ack(tmp_path,monkeypatch):
    gateways,headers,outputs,state,release,all_live=setup(tmp_path,monkeypatch,count=1)
    g=gateways[0];original=g.inspect;calls=0;headers[0]['startup_seconds']=.2;release.set()
    def slow(name):
        nonlocal calls
        calls+=1
        if calls==2:time.sleep(.2)
        return original(name)
    monkeypatch.setattr(g,'inspect',slow)
    with pytest.raises(TimeoutError):g.run(headers[0],tempfile.TemporaryFile(),outputs[0])
    assert not any(json.loads(line).get('event')=='remote_started' for line in outputs[0].getvalue().splitlines())
