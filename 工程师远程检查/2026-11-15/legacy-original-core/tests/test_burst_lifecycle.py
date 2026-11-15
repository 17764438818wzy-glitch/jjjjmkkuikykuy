"""Whole lifecycle burst regressions, without a model or a real RAG endpoint."""
from concurrent.futures import ThreadPoolExecutor
import io
import threading
import time

import pytest

from paperflow.remote_files import file_map,manifest,write_archive
from paperflow.remote_gateway import save
from paperflow.remote_harness import RemoteHarness
from test_control_queue import fixture
from test_remote_start_wait import setup as harness_setup


def test_slow_update_hash_cannot_hold_the_global_close_barrier(tmp_path,monkeypatch):
    import paperflow.remote_gateway as module
    gateways,headers,targets,_,_=fixture(tmp_path,monkeypatch,1);g=gateways[0];h=headers[0];target=targets[0]
    source=tmp_path/'incoming';source.mkdir();(source/'result').write_bytes(b'LLM_UNCHANGED')
    files=file_map(source);h['files']=manifest(files);stream=io.BytesIO();write_archive(stream,files);stream.seek(0)
    entered=threading.Event();release=threading.Event();actual=module.manifest
    def slow(files):
        if any(str(p).startswith(str(target/'data/output')) for p in files.values()):
            entered.set();assert release.wait(2)
        return actual(files)
    monkeypatch.setattr(module,'manifest',slow)
    if not hasattr(__import__('os'),'chown'):monkeypatch.setattr(module.os,'chown',lambda *_:None,raising=False)
    with ThreadPoolExecutor(2) as pool:
        update=pool.submit(g.update,h,stream);assert entered.wait(1)
        reap=pool.submit(g.reap_execution,h)
        end=time.monotonic()+.25
        while not (target.parent/'closed.json').exists() and time.monotonic()<end:time.sleep(.01)
        closed=(target.parent/'closed.json').exists();release.set()
        try:update.result(timeout=2)
        except ValueError:pass
        assert reap.result(timeout=2)['all_exited']
    assert closed,'a slow per-job hash must not block the global closed marker'
    assert (target/'data/output/result').read_bytes()==b'LLM_UNCHANGED'


def test_repeated_execution_reap_reuses_strict_terminal_stage_proofs(tmp_path,monkeypatch):
    import subprocess
    gateways,headers,targets,_,original=fixture(tmp_path,monkeypatch,1)
    first=gateways[0].reap_execution(headers[0]);assert first['all_exited']
    def docker(args,**kwargs):
        assert args[:2]!=['docker','inspect'],'a proven immutable ID must not be inspected again for every old stage'
        return original(args,**kwargs)
    monkeypatch.setattr(subprocess,'run',docker)
    assert gateways[0].reap_execution(headers[0])['all_exited']
    assert (targets[0]/'data/output/result').read_bytes()==b'LLM_UNCHANGED'


def test_worker_close_hands_off_unconfirmed_remote_cleanup_and_always_closes_ssh(monkeypatch):
    import subprocess
    h=RemoteHarness.__new__(RemoteHarness)
    h.close_lock=threading.Lock();h.exited=threading.Event();h.exited.wait=lambda _:False;h.stopped=threading.Event()
    h.identity=dict(job_id='a'*32);h.settings=dict(image='fixture',deployment_id='fixture');h.proof=None
    h._send=lambda _:None;record={};h._record=lambda **values:record.update(values)
    class Process:
        def __init__(self):self.stdin=io.BytesIO();self.stdout=io.BytesIO();self.killed=False
        def poll(self):return 0 if self.killed else None
        def wait(self,timeout=None):
            if not self.killed:raise subprocess.TimeoutExpired(['ssh'],timeout)
            return 0
        def kill(self):self.killed=True
    h.process=Process()
    def request(*_args,**_kwargs):raise TimeoutError('queue not completed within the local close window')
    monkeypatch.setattr('paperflow.remote_harness.request',request)
    start=time.monotonic();h.close()
    assert time.monotonic()-start<1 and h.process.killed
    assert h.process.stdin.closed and h.process.stdout.closed
    assert h.proof is None and not h.exited.is_set() and record['state']=='exit_pending'


def test_initialize_reuses_the_already_verified_running_observation(tmp_path,monkeypatch):
    harness,_,initialized,cleanup=harness_setup(tmp_path,monkeypatch)
    def request(_settings,h,**_kwargs):
        assert h['op']!='status_job','do not open a redundant SSH query for every new session'
        return dict(prepared=True)
    monkeypatch.setattr('paperflow.remote_harness.request',request)
    try:
        harness.start();assert harness.started and initialized==[True]
        assert harness.record['remote_pid']==123
    finally:cleanup()


def test_completed_remote_proof_does_not_skip_local_ssh_handle_cleanup():
    h=RemoteHarness.__new__(RemoteHarness);h.close_lock=threading.Lock();h.exited=threading.Event();h.exited.set()
    h.stopped=threading.Event();h.proof={'all_exited':True};h._record=lambda **_:None
    class Process:
        def __init__(self):self.stdin=io.BytesIO();self.stdout=io.BytesIO()
        def wait(self,timeout=None):return 0
    h.process=Process();h.close()
    assert h.process.stdin.closed and h.process.stdout.closed


def test_upload_download_bursts_have_their_own_bound_not_the_docker_or_health_lane(tmp_path,monkeypatch):
    import paperflow.remote_gateway as module
    gateways,headers,_,_,_=fixture(tmp_path,monkeypatch,8)
    actual=module.file_map;guard=threading.Lock();active=peak=0
    def files(path):
        nonlocal active,peak
        with guard:active+=1;peak=max(peak,active)
        try:time.sleep(.05);return actual(path)
        finally:
            with guard:active-=1
    monkeypatch.setattr(module,'file_map',files)
    with ThreadPoolExecutor(8) as pool:
        futures=[pool.submit(g.handle,dict(h,op='collect'),io.BytesIO(),io.BytesIO()) for g,h in zip(gateways,headers)]
        for future in futures:future.result(timeout=3)
    assert peak<=2,'file transfer work must not overwhelm the host or hold Docker control capacity'


def test_guard_shutdown_cancels_queued_transport_without_inventing_exit_proof(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from paperflow.remote_control import RemoteExecutionGuard
    from paperflow.handoff_store import canonical
    entered=threading.Event();cancelled=threading.Event()
    class Ledger:
        def rows(self,*_):return [dict(epoch='epoch',namespace='ns',image='image',endpoint=canonical({}),state='owned')]
        def _write(self,*_):raise AssertionError('no proof was obtained')
    guard=RemoteExecutionGuard.__new__(RemoteExecutionGuard)
    guard.manager=type('Manager',(),dict(ledger=Ledger(),stopped=threading.Event()))();guard.manager.stopped.set()
    guard.settings=dict(image='image',deployment_id='deployment');guard.namespace='ns';guard.endpoint={}
    guard.closing=threading.Event();guard.pool=ThreadPoolExecutor(1);guard.diagnostics=type('Log',(),dict(close=lambda _:None,record=lambda *_:None))()
    def request(*_args,**kwargs):
        entered.set()
        check=kwargs.get('cancelled',lambda:False)
        if not kwargs.get('cancelled'):raise AssertionError('shutdown must reach an in-flight transport')
        end=time.monotonic()+1
        while not check() and time.monotonic()<end:time.sleep(.01)
        if check():cancelled.set();raise ValueError('closed')
        raise TimeoutError('not cancelled')
    monkeypatch.setattr('paperflow.remote_control.request',request)
    future=guard.pool.submit(guard.reap_sync,'execution','epoch');assert entered.wait(1)
    guard.close()
    with pytest.raises(ValueError):future.result(timeout=.5)
    assert cancelled.is_set()


def test_collect_capacity_retry_never_dispatches_another_model_turn(monkeypatch,tmp_path):
    from paperflow.remote_diagnostics import RemoteQueueBusy
    h=RemoteHarness.__new__(RemoteHarness);h.client=object();h.settings={};h.stopped=threading.Event()
    h._header=lambda op:dict(op=op,job_id='same-job');h._cancelled=lambda:False;h._record=lambda **_:None
    h._rpc=lambda *_a,**_k:pytest.fail('a finished model turn must not be repeated')
    monkeypatch.setattr('paperflow.remote_harness.current_permit',lambda _:True);calls=[]
    def request(_settings,header,**kwargs):
        calls.append(header)
        if len(calls)<3:raise RemoteQueueBusy('busy before transfer')
        return dict(files=[])
    monkeypatch.setattr('paperflow.remote_harness.request',request)
    assert h._collect(tmp_path/'result')==dict(files=[])
    assert calls==[dict(op='collect',job_id='same-job')]*3


def test_exhausted_collect_capacity_is_held_and_preserves_remote_result(monkeypatch,tmp_path):
    from paperflow.remote_diagnostics import RemoteQueueBusy
    from paperflow.scheduling_policy import fault
    h=RemoteHarness.__new__(RemoteHarness);h.client=object();h.settings={};h.stopped=threading.Event()
    h._header=lambda op:dict(op=op);h._cancelled=lambda:False;states=[];h._record=lambda **kw:states.append(kw)
    monkeypatch.setattr('paperflow.remote_harness.current_permit',lambda _:True)
    def request(*_a,**_k):raise RemoteQueueBusy('busy before transfer')
    monkeypatch.setattr('paperflow.remote_harness.request',request)
    with pytest.raises(ValueError) as error:h._collect(tmp_path/'result')
    assert fault(error.value)['category']=='held' and states[-1]['state']=='collection_pending'
