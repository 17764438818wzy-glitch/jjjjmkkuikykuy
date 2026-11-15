from contextlib import nullcontext
import io,json,sqlite3,threading,time,uuid
from pathlib import Path
from types import SimpleNamespace
import pytest

from paperflow.control import Manager
from paperflow.control_store import ControlLedger
from paperflow.config import Config,rules_fingerprint
from paperflow.store import Store
from paperflow.package import save,sha
from paperflow.remote_files import file_map,manifest,write_archive
from paperflow.remote_groups import GroupPool,GroupProxy
from paperflow.remote_gateway import Gateway
from paperflow.scheduling_policy import ActionDeferred

def wait(check,seconds=5):
    end=time.monotonic()+seconds
    while not check():
        assert time.monotonic()<end,'bounded wait expired';time.sleep(.01)


def test_busy_proof_persistence_does_not_requery_remote_or_pause():
    from paperflow.remote_control import RemoteExecutionGuard
    from paperflow.handoff_store import canonical
    guard=object.__new__(RemoteExecutionGuard);guard.observed={};guard.namespace='ns';guard.endpoint={};guard.settings={'image':'image','deployment_id':'deployment'}
    proof=dict(execution_id='execution',epoch='epoch',all_exited=True,execution_closed=True)
    writes=[];calls=[];pauses=[]
    def write(fn):
        writes.append(True)
        if len(writes)==1:
            exc=sqlite3.OperationalError('database is locked');exc.sqlite_errorcode=sqlite3.SQLITE_BUSY;raise exc
        fn(SimpleNamespace(execute=lambda *_:None))
    row=dict(epoch='epoch',namespace='ns',image='image',endpoint=canonical({}),state='owned')
    ledger=SimpleNamespace(rows=lambda sql,*_: [row] if 'remote_harness_executions' in sql else [],_write=write)
    guard.manager=SimpleNamespace(ledger=ledger,stopped=threading.Event(),pause=lambda why:pauses.append(why))
    import paperflow.remote_control as module
    original=module.request;module.request=lambda *_a,**_k:(calls.append(True) or proof)
    try:
        with pytest.raises(ActionDeferred,match='persistence'):guard.reap_sync('execution','epoch')
        assert not pauses
        assert guard.reap_sync('execution','epoch')==proof
        assert len(calls)==1
    finally:module.request=original


def test_group_transport_does_not_invalidate_content_cache(tmp_path):
    a=Config(home=str(tmp_path),remote_harness={'same':'route'},remote_harness_groups=0)
    b=Config(home=str(tmp_path),remote_harness={'same':'route'},remote_harness_groups=8)
    assert rules_fingerprint(a)==rules_fingerprint(b)


def test_child_update_and_collect_touch_only_its_own_subtree(tmp_path,monkeypatch):
    import os
    gateway=Gateway(tmp_path/'server',{'image':'sha256:'+'1'*64})
    gateway.locked=lambda *a,**k:nullcontext()
    monkeypatch.setattr(os,'chown',lambda *_:None,raising=False)
    h=dict(namespace='a'*64,execution_id='b'*32,epoch='c'*32,job_id='d'*32,image='sha256:'+'1'*64,actor_id='e'*32)
    output=gateway.validate(h)/h['job_id']/'data/output';output.mkdir(parents=True)
    other=output/'actors'/('f'*32)/'work';other.mkdir(parents=True);(other/'keep').write_text('immutable')
    source=tmp_path/'pc';(source/'work').mkdir(parents=True);(source/'input').mkdir()
    (source/'work/result.txt').write_text('THIS');(source/'input/original.txt').write_text('SOURCE')
    files=file_map(source);h['files']=manifest(files);stream=io.BytesIO();write_archive(stream,files);stream.seek(0)
    gateway.update_child(h,stream)
    assert (other/'keep').read_text()=='immutable'
    assert (output/'actors'/h['actor_id']/'work/result.txt').read_text()=='THIS'
    assert (gateway.validate(h)/h['job_id']/'data/input/actors'/h['actor_id']/'original.txt').read_text()=='SOURCE'
    stream=io.BytesIO();gateway.collect_child(h,stream);stream.seek(0)
    receipt=json.loads(stream.readline());assert [v['path'] for v in receipt['files']]==['result.txt']
    for bad in ('../escape','f'*33,'a/b',''):
        with pytest.raises(ValueError):gateway.child_root(dict(h,actor_id=bad))


def test_two_managed_groups_bound_and_pause_recheck(tmp_path,monkeypatch):
    import paperflow.remote_groups as module
    m=Manager(Store(tmp_path/'state.sqlite3'),tmp_path/'manager');m.ledger=ControlLedger(m.store)
    m.stopped.clear();m._valid=lambda *_:not m.stopped.is_set()
    reasons=[];m.pause=lambda why:(reasons.append(why),m.stopped.set())
    cfg=Config(home=str(tmp_path/'runtime'),remote_harness={'image':'image','host':'fake'},remote_harness_groups=2,model_timeout=10)
    cfg.prepare();gate=threading.Event();started=[];parent_objects=[]
    class Parent:
        def __init__(self,pool,identity,home):
            self.identity={'execution_id':identity,'epoch':m.epoch,'image':'image'}
            self.members={};self.actions=0;self.retiring=False;self.stopped=threading.Event();self.proof=None;self.children={}
            parent_objects.append(self)
        def _header(self,op,**kw):return dict(self.identity,op=op,**kw)
        def ensure_started(self):pass
        def _rpc(self,method,**params):
            if method=='group/start':
                child=uuid.uuid4().hex;self.children[params['action_id']]=child;started.append(params['action_id'])
                return dict(child_id=child)
            if method=='group/wait':
                assert gate.wait(5)
                return dict(child_id=self.children[params['action_id']],action_id=params['action_id'],finish_reason='completed',events=[])
            if method=='group/close':return dict(child_id=self.children.pop(params['action_id']),child_closed=True)
            raise AssertionError(method)
        def close(self):
            self.stopped.set();self.proof=dict(all_exited=True,job_closed=True)
    def transport(settings,header,**kwargs):
        if header['op']=='collect_child':
            kwargs['destination'].mkdir();(kwargs['destination']/'result.txt').write_text(header['actor_id'])
        return dict(actor_id=header['actor_id'])
    monkeypatch.setattr(module,'Parent',Parent);monkeypatch.setattr(module,'request',transport)
    monkeypatch.setattr(module,'endpoint_identity',lambda _:dict(host='fake'))
    pool=GroupPool(m,cfg)
    try:
        records=[]
        for index in range(17):
            key='task'+str(index);exid=uuid.uuid4().hex;action=uuid.uuid4().hex;actor=uuid.uuid4().hex
            m.ledger._write(lambda c,exid=exid,key=key:c.execute('INSERT INTO control_executions(execution_id,epoch,unit_kind,unit_key,job_name,state,lease_until,last_heartbeat,deadline) VALUES(?,?,?,?,?,?,?,?,?)',
                (exid,m.epoch,'graph',key,exid,'active',0,0,0)))
            m.executions[exid]=SimpleNamespace(key=key,kind='graph',eligible=True,active_action=dict(kind='llm',action_id=action,deadline=time.monotonic()+10))
            m.tickets[action]=dict(snapshot={})
            original=tmp_path/(key+'.txt');original.write_text(key)
            output=cfg.root/'work'/key/'attempts'/'one'/'stage';output.mkdir(parents=True)
            home=cfg.root/'private'/('harness-'+exid);home.mkdir()
            path=cfg.root/'private/sdk-submissions'/(action+'.json')
            save(path,dict(epoch=m.epoch,execution_id=exid,action_id=action,actor_id=actor,output=str(output),home=str(home),
                task=dict(task_id=key,source_path=str(original),source_sha256=sha(original)),prompt='fixture',stage=str(output.with_name('stage.native-'+actor))))
            pool.handle(exid,'submit',dict(path=str(path),sha256=sha(path),action_id=action));records.append((exid,action,actor))
        wait(lambda:len(started)==16)
        assert len(parent_objects)==2 and all(len(p.members)==8 for p in parent_objects)
        m.stopped.set();gate.set()  # The waiting seventeenth action must never slip through.
        wait(lambda:all(v['state']!='pending' for v in pool.results.values()))
        assert len(started)==16
        with pytest.raises(ValueError,match='admission'):pool.authorize(records[0][0],records[0][1])
        wait(lambda:not pool.parents)
    finally:
        gate.set();m.stopped.set();wait(lambda:not pool.parents);pool.close()


def test_upload_busy_never_retires_sibling_parent(tmp_path,monkeypatch):
    import paperflow.remote_groups as module
    from paperflow.remote_diagnostics import RemoteQueueBusy
    pool=object.__new__(GroupPool);pool.lock=threading.RLock();pool.error_events=[]
    pool.manager=SimpleNamespace(stopped=threading.Event(),epoch='epoch');pool.config=SimpleNamespace(remote_harness={})
    source=tmp_path/'original';source.write_text('fixture');output=tmp_path/'output';output.mkdir()
    parent=SimpleNamespace(ensure_started=lambda:None,_header=lambda op,**kw:dict(op=op,**kw),retiring=False)
    pool.allocate=lambda *_:parent;pool.authorize=lambda *_:None
    retired=[];pool.retire=lambda _:retired.append(True)
    monkeypatch.setattr(module,'request',lambda *_a,**_k:(_ for _ in ()).throw(RemoteQueueBusy('busy')))
    actor=dict(execution_id='execution',actor_id='actor',lock=threading.Lock(),closing=False,parent=parent)
    value=dict(task=dict(source_path=str(source),source_sha256=sha(source)),output=str(output))
    record={};pool.run(actor,value,dict(action_id='action'),record)
    assert record['state']=='deferred' and record['reason']=='resource_busy'
    assert not retired and not parent.retiring


def test_collect_busy_retries_readback_without_model_or_sibling_replay(tmp_path,monkeypatch):
    import paperflow.remote_groups as module
    from paperflow.remote_diagnostics import RemoteQueueBusy
    pool=object.__new__(GroupPool);pool.lock=threading.RLock();pool.error_events=[];pool.closing=threading.Event()
    pool.manager=SimpleNamespace(stopped=threading.Event(),epoch='epoch')
    pool.config=SimpleNamespace(remote_harness={},catalog_path=str(tmp_path/'catalog'),model_timeout=10)
    source=tmp_path/'original';source.write_text('fixture');output=tmp_path/'output';output.mkdir()
    model=[];collect=[];retired=[]
    def rpc(method,**kw):
        model.append(method);return dict(child_id='child',action_id='action',finish_reason='completed',events=[])
    parent=SimpleNamespace(ensure_started=lambda:None,_header=lambda op,**kw:dict(op=op,**kw),_rpc=rpc,
        retiring=False,identity={'group':'one'},actions=0)
    pool.allocate=lambda *_:parent;pool.authorize=lambda *_:None;pool.retire=lambda _:retired.append(True);pool.persist=lambda _:None
    def transfer(_settings,h,**kw):
        if h['op']=='collect_child':collect.append(True);raise RemoteQueueBusy('busy')
        return {}
    monkeypatch.setattr(module,'request',transfer)
    actor=dict(execution_id='execution',actor_id='actor',lock=threading.Lock(),closing=False,parent=parent,child_id=None)
    value=dict(task=dict(source_path=str(source),source_sha256=sha(source)),output=str(output),prompt='fixture',stage=str(tmp_path/'stage'))
    record={};pool.run(actor,value,dict(action_id='action',deadline=time.monotonic()+10),record)
    assert model==['group/start','group/wait'] and len(collect)==3 and not retired
    assert record['state']=='error' and record['phase']=='collect'
    assert actor['retained_result']['collection_pending']


def test_close_uses_last_remote_action_and_reuses_proof_after_sql_busy():
    pool=object.__new__(GroupPool);pool.lock=threading.RLock();pool.results={};pool.actors={}
    calls=[];pauses=[];writes=[]
    pool.manager=SimpleNamespace(epoch='epoch',pause=lambda why:pauses.append(why))
    def rpc(method,**kw):
        calls.append((method,kw));return dict(child_id='child',child_closed=True)
    parent=SimpleNamespace(proof=None,stopped=threading.Event(),_rpc=rpc,members={},actions=0,retiring=False,started=True)
    actor=dict(actor_id='actor',epoch='epoch',parent=parent,child_id='child',action_id='preparing-B',remote_action_id='accepted-A',
        dispatched_action_id='accepted-A',lock=threading.Lock());parent.members['actor']=actor
    pool.actors['actor']=actor
    def persist(_fn):
        writes.append(True)
        if len(writes)==1:raise ActionDeferred('remote_group_persistence_busy')
    pool.persist=persist
    with pytest.raises(ActionDeferred):pool.close_actor(actor)
    pool.close_actor(actor)
    assert len(calls)==1 and calls[0][1]['action_id']=='accepted-A' and calls[0][1]['timeout']==2
    assert not pauses and actor['closed']


def test_group_sql_wait_never_blocks_status_or_immediate_pause(tmp_path,monkeypatch):
    import paperflow.remote_groups as module
    m=Manager(Store(tmp_path/'ledger.sqlite3'),tmp_path/'manager');m.stopped.clear()
    pool=object.__new__(GroupPool);pool.manager=m;pool.lock=threading.RLock();pool.allocation_lock=threading.Lock()
    pool.parents={};pool.pending_groups=set();pool.count=1;pool.retiring={};pool.actors={};pool.error=None
    pool.config=SimpleNamespace(remote_harness={});pool.closing=threading.Event()
    m.external=SimpleNamespace(status=lambda:pool.status())
    entered=threading.Event();release=threading.Event();done=threading.Event();failures=[]
    def authorize(*_):
        if m.stopped.is_set():raise ValueError('paused')
    pool.authorize=authorize
    def persist(_):entered.set();assert release.wait(3)
    pool.persist=persist
    class Parent:
        def __init__(self,*_):self.identity={'execution_id':'group'};self.members={}
        def _header(self,_):return {}
    monkeypatch.setattr(module,'Parent',Parent);monkeypatch.setattr(module,'endpoint_identity',lambda _: {})
    actor=dict(execution_id='ex',actor_id='actor',epoch=m.epoch,closing=False,parent=None)
    def allocate():
        try:pool.allocate(actor,dict(action_id='action',home=str(tmp_path)))
        except ValueError as exc:failures.append(str(exc))
        finally:done.set()
    thread=threading.Thread(target=allocate);thread.start();assert entered.wait(1)
    status_done=threading.Event();threading.Thread(target=lambda:(m.status(),status_done.set()),daemon=True).start()
    try:
        assert status_done.wait(.2),'status waited for a database write'
        m.pause('blocked database acceptance');assert m.stopped.is_set()
    finally:release.set();assert done.wait(2);thread.join()
    assert failures==['paused']


def test_actor_registration_failure_cannot_publish_or_skip_binding():
    pool=object.__new__(GroupPool);pool.lock=threading.RLock();pool.allocation_lock=threading.Lock();pool.pending_groups=set();pool.count=1
    pool.manager=SimpleNamespace(epoch='epoch');pool.config=SimpleNamespace(remote_harness={});pool.closing=threading.Event()
    parent=SimpleNamespace(identity={'execution_id':'group'},retiring=False,started=True,actions=0,members={'sibling':{}},stopped=threading.Event())
    pool.parents={'group':parent};pool.authorize=lambda *_:None;writes=[]
    def persist(_fn):
        writes.append(True)
        if len(writes)==1:raise ActionDeferred('remote_group_persistence_busy')
    pool.persist=persist
    actor=dict(actor_id='actor',execution_id='execution',epoch='epoch',parent=None,closing=False)
    value=dict(action_id='action')
    with pytest.raises(ActionDeferred):pool.allocate(actor,value)
    assert actor['parent'] is None and 'actor' not in parent.members
    assert pool.allocate(actor,value) is parent and actor['registered']
    assert len(writes)==2


def test_prior_closed_actor_cannot_hide_a_new_unregistered_pending_actor():
    pool=object.__new__(GroupPool);pool.lock=threading.RLock();calls=[]
    pool.manager=SimpleNamespace(ledger=SimpleNamespace(rows=lambda *_:[dict(actor_id='previous',state='closed',proof='{}')]))
    actor=dict(actor_id='new',execution_id='execution',epoch='epoch',closed=False,closing=False)
    pool.actors={'new':actor};pool.cleanup=SimpleNamespace(submit=lambda *args:(calls.append(args) or object()))
    assert pool.exit_proof('execution','epoch') is False
    assert actor['closing'] and len(calls)==1


def test_registration_wait_rechecks_parent_state_in_same_transaction():
    from paperflow.remote_groups import SCHEMA
    c=sqlite3.connect(':memory:');c.row_factory=sqlite3.Row
    for sql in SCHEMA:c.execute(sql)
    c.execute("INSERT INTO remote_sdk_groups VALUES('group','epoch','{}','{}','exited','{}')")
    pool=object.__new__(GroupPool);pool.lock=threading.RLock();pool.allocation_lock=threading.Lock();pool.pending_groups=set();pool.count=1
    pool.manager=SimpleNamespace(epoch='epoch');pool.config=SimpleNamespace(remote_harness={});pool.closing=threading.Event()
    parent=SimpleNamespace(identity={'execution_id':'group'},retiring=False,started=True,actions=0,members={},stopped=threading.Event())
    pool.parents={'group':parent};pool.authorize=lambda *_:None;pool.persist=lambda fn:fn(c)
    actor=dict(actor_id='actor',execution_id='execution',epoch='epoch',parent=None,closing=False)
    with pytest.raises(ActionDeferred):pool.allocate(actor,dict(action_id='action'))
    assert actor['parent'] is None and not parent.members and not list(c.execute('SELECT * FROM remote_sdk_actors'))
    c.close()


def test_group_exit_and_restart_keep_retained_result_identity(monkeypatch):
    import paperflow.remote_groups as module
    from paperflow.handoff_store import canonical
    connection=sqlite3.connect(':memory:');connection.row_factory=sqlite3.Row
    for sql in module.SCHEMA:connection.execute(sql)
    identity=dict(namespace='ns',execution_id='group',epoch='epoch',image='image',deployment_id='deployment')
    retained=dict(action_id='accepted',source_sha256='source',output='output',collection_pending=True)
    connection.execute('INSERT INTO remote_sdk_groups VALUES(?,?,?,?,?,NULL)',('group','epoch',canonical(identity),canonical({}),'owned'))
    connection.execute('INSERT INTO remote_sdk_actors VALUES(?,?,?,?,?,?)',('actor','execution','epoch','group','closed',canonical(dict(retained_result=retained,child_closed=True))))
    pool=object.__new__(GroupPool);pool.lock=threading.RLock();pool.manager=SimpleNamespace(epoch='epoch')
    parent=SimpleNamespace(identity={'execution_id':'group'},proof={'all_exited':True},close=lambda:None)
    pool.parents={'group':parent};pool.actors={};pool.persist=lambda fn:fn(connection)
    pool.close_parent(parent)
    assert json.loads(connection.execute('SELECT proof FROM remote_sdk_actors').fetchone()[0])['retained_result']==retained
    connection.execute("UPDATE remote_sdk_groups SET state='owned'")
    calls=[]
    monkeypatch.setattr(module,'request',lambda *_a,**_k:(calls.append(True) or dict(all_exited=True,execution_closed=True,execution_id='group',epoch='epoch')))
    ledger=SimpleNamespace(_write=lambda fn:fn(connection),rows=lambda sql,*_:list(connection.execute(sql)))
    guard=SimpleNamespace(namespace='ns',manager=SimpleNamespace(ledger=ledger),endpoint={},settings=dict(image='image',deployment_id='deployment'))
    module.recover_groups(guard)
    assert len(calls)==1 and json.loads(connection.execute('SELECT proof FROM remote_sdk_actors').fetchone()[0])['retained_result']==retained
    connection.execute("UPDATE remote_sdk_groups SET state='owned',identity_json=?",(canonical(dict(identity,namespace='other')),))
    with pytest.raises(ValueError,match='ownership'):module.recover_groups(guard)
    assert len(calls)==1  # Invalid ownership must be rejected before remote reaping.
    connection.close()

