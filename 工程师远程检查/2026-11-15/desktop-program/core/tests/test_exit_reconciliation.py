"""Duplicate owners must safely converge on one actual remote exit."""
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
import os
import subprocess
import threading

from paperflow.remote_gateway import Gateway
from test_remote_gateway import header


def setup(tmp_path,job_id=None,locks=None):
    h=header()
    if job_id:h['job_id']=job_id
    gateways=[Gateway(tmp_path,{'image':h['image']}) for _ in range(2)]
    if locks is None:locks=defaultdict(threading.Lock)
    if os.name=='nt':
        @contextmanager
        def locked(path=None,blocking=True):
            lock=locks[str(path or tmp_path/'.lock')]
            if not lock.acquire(blocking=blocking):raise BlockingIOError()
            try:yield
            finally:lock.release()
        for gateway in gateways:gateway.locked=locked
    target=gateways[0].validate(h)/h['job_id'];target.mkdir(parents=True)
    (target/'creation.json').write_text(json.dumps(dict(state='done')))
    output=target/'data/output';output.mkdir(parents=True);(output/'result.txt').write_bytes(b'LLM_RESULT_UNCHANGED')
    (target/'data/home').mkdir();(target/'data/home/session').write_bytes(b'disposable fixture')
    return h,gateways,target


def test_duplicate_reap_does_not_report_failure_after_the_container_is_gone(tmp_path,monkeypatch):
    h,gateways,target=setup(tmp_path);barrier=threading.Barrier(2);guard=threading.Lock();present=True;removals=0
    labels={'paperflow.'+k:h[k] for k in ('namespace','execution_id','epoch','job_id')}
    def inspect(_name):
        with guard:found=present
        if found:
            try:barrier.wait(.15)
            except threading.BrokenBarrierError:pass
            return dict(Id='f'*64,Name='/'+gateways[0].name(h),Image=h['image'],Config=dict(Labels=labels))
        return None
    def remove(args,**kwargs):
        nonlocal present,removals
        assert args[:3]==['docker','rm','--force']
        with guard:
            if not present:raise subprocess.CalledProcessError(1,args,stderr='No such container')
            present=False;removals+=1
        return subprocess.CompletedProcess(args,0)
    for gateway in gateways:monkeypatch.setattr(gateway,'inspect',inspect)
    monkeypatch.setattr(subprocess,'run',remove)
    errors=[];results=[]
    with ThreadPoolExecutor(2) as pool:
        futures=[pool.submit(gateway.reap_job,h) for gateway in gateways]
        for future in futures:
            try:results.append(future.result(timeout=3))
            except Exception as exc:errors.append(type(exc).__name__+': '+str(exc))
    assert not errors,errors
    assert len(results)==2 and all(r['all_exited'] for r in results) and removals==1
    assert (target/'data/output/result.txt').read_bytes()==b'LLM_RESULT_UNCHANGED'


def test_duplicate_cleanup_is_not_a_false_exit_failure(tmp_path,monkeypatch):
    import shutil
    h,gateways,target=setup(tmp_path);barrier=threading.Barrier(2);original=shutil.rmtree
    for gateway in gateways:monkeypatch.setattr(gateway,'inspect',lambda _:None)
    def remove(path):
        try:barrier.wait(.15)
        except threading.BrokenBarrierError:pass
        return original(path)
    monkeypatch.setattr(shutil,'rmtree',remove)
    errors=[]
    with ThreadPoolExecutor(2) as pool:
        futures=[pool.submit(gateway.reap_job,h) for gateway in gateways]
        for future in futures:
            try:assert future.result(timeout=3)['all_exited']
            except Exception as exc:errors.append(type(exc).__name__+': '+str(exc))
    assert not errors,errors
    assert (target/'data/output/result.txt').read_bytes()==b'LLM_RESULT_UNCHANGED'


def test_independent_job_exit_checks_still_run_in_parallel(tmp_path,monkeypatch):
    locks=defaultdict(threading.Lock)
    first,a,_=setup(tmp_path,locks=locks);second,b,_=setup(tmp_path,'e'*32,locks=locks)
    barrier=threading.Barrier(2);guard=threading.Lock();active=peak=0
    local=threading.local()
    def inspect(_):
        nonlocal active,peak
        if not getattr(local,'observed',False):
            local.observed=True
            with guard:active+=1;peak=max(peak,active)
            try:barrier.wait(1)
            except threading.BrokenBarrierError:pass
            finally:
                with guard:active-=1
        return None
    for gateway in (a[0],b[0]):monkeypatch.setattr(gateway,'inspect',inspect)
    with ThreadPoolExecutor(2) as pool:
        futures=[pool.submit(a[0].reap_job,first),pool.submit(b[0].reap_job,second)]
        assert all(f.result(timeout=3)['all_exited'] for f in futures)
    assert peak==2,'exit I/O for unrelated jobs must not share the global metadata lock'


def test_remote_error_is_persisted_without_a_transport_or_sensitive_text(tmp_path):
    h,gateways,target=setup(tmp_path);h.update(op='reap_job',request_id='f'*32)
    detail=gateways[0].record_exit_error(h,FileNotFoundError('SECRET_CONTENT_NOT_FOR_LOGGING'))
    raw=(target/'exit-error.json').read_text(encoding='utf-8');value=json.loads(raw)
    assert value['request_id']=='f'*32 and value['error']['reason']=='path_absent'
    assert 'SECRET_CONTENT_NOT_FOR_LOGGING' not in raw and detail==value['error']
