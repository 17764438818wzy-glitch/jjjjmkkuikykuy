import asyncio
from copy import deepcopy
from types import SimpleNamespace
from uuid import UUID,uuid4
import pytest
from prefect.states import Running,Pending,Crashed,Completed,Failed
from prefect.variables import Variable
from prefect.client.schemas.actions import VariableCreate
import importlib.util,importlib
recovery=importlib.import_module("paperflow_v3.control.recovery") if importlib.util.find_spec("paperflow_v3.control.recovery") else None

PARENT=UUID(int=900)

def child(number,state=None):
    return SimpleNamespace(id=UUID(int=number),parent_task_run_id=UUID(int=number+1000),
        state=state or Running(),parameters={"task_id":str(PARENT),"group_id":f"g{number}",
        "registered":{"task_id":str(PARENT),"group_id":f"g{number}","paper_id":f"p{number}","receipt_id":f"r{number}"}})

def setup(monkeypatch,children,*,unknown=False):
    assert recovery is not None,"Durable existing-child recovery scope is missing"
    variables={};events=[];artifacts={}
    async def get(name,default=None):return deepcopy(variables.get(name,default))
    async def put(name,value,**kwargs):
        VariableCreate(name=name,value=value)
        variables[name]=deepcopy(value);events.append(("variable",name,value))
    monkeypatch.setattr(Variable,"aget",staticmethod(get));monkeypatch.setattr(Variable,"aset",staticmethod(put))
    class Client:
        async def __aenter__(self):return self
        async def __aexit__(self,*a):pass
        async def read_flow_run(self,id):return next(c for c in children if c.id==id)
        async def create_artifact(self,artifact):
            saved=SimpleNamespace(id=uuid4(),data=deepcopy(artifact.data),flow_run_id=artifact.flow_run_id,type=artifact.type)
            artifacts[saved.id]=saved;return saved
        async def read_artifacts(self,*,artifact_filter,**kwargs):
            return [artifacts[id] for id in artifact_filter.id.any_ if id in artifacts]
        async def set_flow_run_state(self,id,state):
            assert state.is_crashed(),"preparation must never schedule"
            assert recovery.key(PARENT) in variables,"scope must exist before state changes"
            assert (await recovery.read_scope(PARENT))["prepared_at"] is None
            events.append(("crash",id))
            if unknown:return SimpleNamespace(state=None)
            target=next(c for c in children if c.id==id);target.state=state
            return SimpleNamespace(state=state)
    monkeypatch.setattr(recovery,"get_client",lambda:Client())
    return variables,events

def test_prepare_leaves_paused_and_preserves_original_ids_and_unknown(monkeypatch):
    children=[child(1),child(2),child(3,Pending())];variables,events=setup(monkeypatch,children)
    scope=asyncio.run(recovery.prepare_scope(PARENT,children,{str(children[0].id):None,str(children[1].id):"模型原请求结果待核对",str(children[2].id):None}))
    assert scope["prepared_at"] and len(scope["eligible"])==2 and len(scope["blocked"])==1
    assert set(scope["eligible"])=={str(children[0].id),str(children[2].id)}
    assert all(c.state.is_crashed() for c in children)
    assert all(scope["eligible"].get(str(c.id),scope["blocked"].get(str(c.id)))["prepared_state_id"]==str(c.state.id) for c in children)
    assert not any(e[0]=="variable" and e[2].get("state")=="running" for e in events)
    assert recovery.select_targets(scope,children)==[children[0],children[2]]

def test_unknown_state_proposal_keeps_scope_unprepared(monkeypatch):
    c=child(1);variables,events=setup(monkeypatch,[c],unknown=True)
    with pytest.raises(RuntimeError,match="待核对"):
        asyncio.run(recovery.prepare_scope(PARENT,[c],{str(c.id):None}))
    scope=asyncio.run(recovery.read_scope(PARENT))
    assert scope["prepared_at"] is None and c.state.is_running()
    with pytest.raises(RuntimeError,match="尚未完成"):
        recovery.select_targets(scope,[c])

def test_real_94_ticket_scope_fits_native_prefect_schema_without_large_variables(monkeypatch):
    children=[child(n) for n in range(1,95)];variables,events=setup(monkeypatch,children)
    scope=asyncio.run(recovery.prepare_scope(PARENT,children,{str(c.id):None if i<84 else "原请求待核对" for i,c in enumerate(children)}))
    import json
    assert len(json.dumps(scope))>30000
    pointer=variables[recovery.key(PARENT)]
    assert len(json.dumps(pointer))<5000 and "artifact_id" in pointer
    assert asyncio.run(recovery.read_scope(PARENT))==scope

@pytest.mark.parametrize("damage",["row","state_id","time","pointer_hash"])
def test_malformed_scope_fails_closed_as_reviewable_error(monkeypatch,damage):
    c=child(1);variables,_=setup(monkeypatch,[c])
    scope=asyncio.run(recovery.prepare_scope(PARENT,[c],{str(c.id):None}))
    if damage=="pointer_hash":
        variables[recovery.key(PARENT)]["sha256"]="0"*64
        with pytest.raises(RuntimeError,match="恢复范围"):asyncio.run(recovery.read_scope(PARENT))
        return
    if damage=="row":scope["eligible"][str(c.id)]={}
    elif damage=="state_id":scope["eligible"][str(c.id)]["prepared_state_id"]="invalid"
    else:scope["prepared_at"]="invalid"
    with pytest.raises(RuntimeError,match="恢复范围"):recovery.select_targets(scope,[c])

def test_scope_rejects_changed_identity_state_and_overlap(monkeypatch):
    c=child(1);setup(monkeypatch,[c]);scope=asyncio.run(recovery.prepare_scope(PARENT,[c],{str(c.id):None}))
    changed=deepcopy(c);changed.parameters["registered"]["receipt_id"]="other"
    with pytest.raises(RuntimeError,match="身份"):recovery.select_targets(scope,[changed])
    c.state=Crashed()
    with pytest.raises(RuntimeError,match="状态"):recovery.select_targets(scope,[c])
    scope["blocked"][str(c.id)]=scope["eligible"][str(c.id)]
    with pytest.raises(RuntimeError,match="范围"):recovery.select_targets(scope,[c])

def test_prepare_excludes_completed_and_failed(monkeypatch):
    children=[child(1,Completed()),child(2,Failed())];setup(monkeypatch,children)
    with pytest.raises(RuntimeError,match="运行中或等待"):
        asyncio.run(recovery.prepare_scope(PARENT,children,{str(c.id):None for c in children}))

def test_child_entry_blocks_unknown_and_outside_scope(monkeypatch):
    good,bad=child(1),child(2);setup(monkeypatch,[good,bad]);scope=asyncio.run(recovery.prepare_scope(PARENT,[good,bad],{str(good.id):None,str(bad.id):"请求待核对"}))
    recovery.check_child(scope,good)
    for c in (bad,child(3)):
        with pytest.raises(RuntimeError,match="恢复范围|待核对"):recovery.check_child(scope,c)

def test_parent_and_paper_entry_guards_run_before_business(monkeypatch):
    from paperflow_v3.control import parent,flows
    good,bad=child(1),child(2);setup(monkeypatch,[good,bad])
    scope=asyncio.run(recovery.prepare_scope(PARENT,[good,bad],{str(good.id):None,str(bad.id):"请求待核对"}))
    async def read_scope(_):return scope
    monkeypatch.setattr(recovery,"read_scope",read_scope)
    monkeypatch.setattr(parent.FlowRunContext,"get",lambda:SimpleNamespace(flow_run=SimpleNamespace(id=PARENT)))
    with pytest.raises(RuntimeError,match="父单剩余"):
        asyncio.run(parent.classify_and_dispatch(task_id=PARENT,source_paths=[],first_step_root=None,
           work_root=None,history_roots=[],legacy_classification_root=None,nimble_endpoint=None,
           nimble_lock_path=None,child_deployment_id=None,admission=None))
    monkeypatch.setattr(flows.FlowRunContext,"get",lambda:SimpleNamespace(flow_run=bad))
    with pytest.raises(RuntimeError,match="待核对"):
        asyncio.run(flows.paper_order.fn(task_id=str(PARENT),group_id="g2",registered=bad.parameters["registered"]))
