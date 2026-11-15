import asyncio
from copy import deepcopy
from uuid import UUID,uuid4
from types import SimpleNamespace
from contextlib import asynccontextmanager
import httpx
import pytest
from prefect.states import Running,Crashed,Completed,Failed,Scheduled
from prefect.variables import Variable
from prefect.settings import PREFECT_API_URL,temporary_settings
from paperflow_v3.control import api,recovery
from test_control_action_order import setup_control,TASK_ID,DEPLOYMENT_ID

def ticket(number,state=None):
    return SimpleNamespace(id=UUID(int=number),parent_task_run_id=UUID(int=100+number),state=state or Crashed(),
        parameters={"task_id":str(TASK_ID),"group_id":f"g{number}","registered":{"group_id":f"g{number}"}})

def scope_for(good,bad):
    return {"version":1,"mode":"existing_children","task_id":str(TASK_ID),"prepared_at":"2026-11-03T00:00:00Z",
            "eligible":{str(good.id):{**recovery.binding(good),"prepared_state_id":str(good.state.id)}},
            "blocked":{str(bad.id):{**recovery.binding(bad),"prepared_state_id":str(bad.state.id),"reason":"原请求待核对"}}}

def test_start_only_schedules_prepared_children_never_parent_or_unknown(monkeypatch):
    good,bad=ticket(1),ticket(2)
    events=setup_control(monkeypatch,running=False,state=Crashed(),children=[good,bad,ticket(3,Completed()),ticket(4,Failed())])
    scope=scope_for(good,bad)
    class Client:
        async def read_flow_run(self,id):return next(c for c in [good,bad] if c.id==id)
        async def set_flow_run_state(self,id,state):
            events.append(("schedule",id));target=next(c for c in [good,bad] if c.id==id);target.state=state
            return SimpleNamespace(state=state)
    @asynccontextmanager
    async def get_client():yield Client()
    # API read_task also uses this boundary, so preserve its original parent read.
    original_client=api.get_client
    @asynccontextmanager
    async def client_context():
        class Reads(Client):
            async def read_flow_run(self,id):
                if id==TASK_ID:
                    async with original_client() as c:return await c.read_flow_run(id)
                return await super().read_flow_run(id)
        yield Reads()
    monkeypatch.setattr(api,"get_client",client_context)
    async def read_scope(_):return deepcopy(scope)
    monkeypatch.setattr(recovery,"read_scope",read_scope)
    async def save_scope(value):scope.clear();scope.update(deepcopy(value))
    monkeypatch.setattr(recovery,"save_scope",save_scope,raising=False)
    async def test(**kwargs):events.append("gateway-test");return {"successful_count":1}
    async def apply(*args,**kwargs):events.append("capacity")
    app=api.create_app({"deployment_id":DEPLOYMENT_ID},test_start_accounts=test,apply_start_capacity=apply)
    request_id=str(uuid4())
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://test") as c:
            first=await c.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",json={"request_id":request_id,"intent_seq":1})
            second=await c.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",json={"request_id":request_id,"intent_seq":1})
        return first,second
    with temporary_settings({PREFECT_API_URL:"http://127.0.0.1:1/api"}):first,second=asyncio.run(scenario())
    assert first.status_code==second.status_code==200
    assert [(e[0],e[1]) for e in events if isinstance(e,tuple) and e[0]=="schedule"]==[("schedule",good.id)]
    assert events.count("gateway-test")==1 and bad.state.is_crashed()

def test_unprepared_scope_rejects_before_any_gateway_or_admission(monkeypatch):
    good,bad=ticket(1),ticket(2);events=setup_control(monkeypatch,running=False,children=[good,bad])
    scope=scope_for(good,bad);scope["prepared_at"]=None
    async def read_scope(_):return scope
    monkeypatch.setattr(recovery,"read_scope",read_scope)
    async def test(**kwargs):events.append("gateway-test");return {"successful_count":1}
    app=api.create_app({"deployment_id":DEPLOYMENT_ID},test_start_accounts=test)
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://test") as c:
            return await c.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",json={"request_id":str(uuid4()),"intent_seq":1})
    with temporary_settings({PREFECT_API_URL:"http://127.0.0.1:1/api"}):r=asyncio.run(scenario())
    assert r.status_code==409
    assert not any(e in events for e in ("gateway-test","start","preflight"))

def test_unknown_dispatch_pauses_and_prevents_another_start_identity(monkeypatch):
    good,bad=ticket(1),ticket(2);events=setup_control(monkeypatch,running=False,children=[good,bad])
    stored=scope_for(good,bad)
    async def read_scope(_):return deepcopy(stored)
    async def write(value):stored.clear();stored.update(deepcopy(value))
    monkeypatch.setattr(recovery,"read_scope",read_scope);monkeypatch.setattr(recovery,"save_scope",write,raising=False)
    original_client=api.get_client
    @asynccontextmanager
    async def client_context():
        class Client:
            async def read_flow_run(self,id):
                if id==TASK_ID:
                    async with original_client() as c:return await c.read_flow_run(id)
                return good
            async def set_flow_run_state(self,id,state):events.append(("dispatch_unknown",id));return SimpleNamespace(state=None)
        yield Client()
    monkeypatch.setattr(api,"get_client",client_context)
    async def test(**kwargs):events.append("gateway-test");return {"successful_count":1}
    async def apply(*args,**kwargs):pass
    app=api.create_app({"deployment_id":DEPLOYMENT_ID},test_start_accounts=test,apply_start_capacity=apply)
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://test") as c:
            first=await c.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",json={"request_id":str(uuid4()),"intent_seq":1})
            second=await c.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",json={"request_id":str(uuid4()),"intent_seq":2})
        return first,second
    with temporary_settings({PREFECT_API_URL:"http://127.0.0.1:1/api"}):first,second=asyncio.run(scenario())
    assert first.status_code==second.status_code==409
    assert stored["prepared_at"] is None and "pause" in events
    assert events.count("gateway-test")==1
    assert len([e for e in events if isinstance(e,tuple) and e[0]=="dispatch_unknown"])==1

@pytest.mark.parametrize("native_state,alive",[(Scheduled,False),(Running,True)])
@pytest.mark.parametrize("ticket_count",[1,84])
def test_pause_then_continue_reopens_admission_without_rescheduling_originals(monkeypatch,native_state,alive,ticket_count):
    goods=[ticket(n) for n in range(1,ticket_count+1)];good=goods[0];bad=ticket(95)
    events=setup_control(monkeypatch,running=False,children=[*goods,bad])
    stored=scope_for(good,bad)
    stored["eligible"]={str(c.id):{**recovery.binding(c),"prepared_state_id":str(c.state.id)} for c in goods}
    async def read_scope(_):return deepcopy(stored)
    async def save_scope(value):stored.clear();stored.update(deepcopy(value))
    monkeypatch.setattr(recovery,"read_scope",read_scope);monkeypatch.setattr(recovery,"save_scope",save_scope,raising=False)
    monkeypatch.setattr(recovery,"execution_exists",lambda child:alive,raising=False)
    original_client=api.get_client
    @asynccontextmanager
    async def client_context():
        class Client:
            async def read_flow_run(self,id):
                if id==TASK_ID:
                    async with original_client() as c:return await c.read_flow_run(id)
                return next(c for c in goods if c.id==id)
            async def set_flow_run_state(self,id,state):
                assert "start" not in events,"All original tickets must be queued while paused"
                events.append(("schedule",id));next(c for c in goods if c.id==id).state=state;return SimpleNamespace(state=state)
        yield Client()
    monkeypatch.setattr(api,"get_client",client_context)
    async def tested(**kwargs):events.append("gateway-test");return {"successful_count":1}
    async def applied(*args,**kwargs):pass
    app=api.create_app({"deployment_id":DEPLOYMENT_ID},test_start_accounts=tested,apply_start_capacity=applied)
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://test") as c:
            first=await c.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",json={"request_id":str(uuid4()),"intent_seq":1})
            paused=await c.post(f"/api/v1/prefect/tasks/{TASK_ID}/pause",json={"request_id":str(uuid4()),"intent_seq":2})
            if native_state is Running:
                for c_run in goods:c_run.state=Running()
            continued=await c.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",json={"request_id":str(uuid4()),"intent_seq":3})
        return first,paused,continued
    with temporary_settings({PREFECT_API_URL:"http://127.0.0.1:1/api"}):results=asyncio.run(scenario())
    assert [r.status_code for r in results]==[200,200,200]
    assert [e for e in events if isinstance(e,tuple) and e[0]=="schedule"]==[("schedule",c.id) for c in goods]
    assert events.count("start")==2 and events.count("gateway-test")==2 and bad.state.is_crashed()

def test_active_drift_without_confirmed_dispatch_is_rejected_before_gateway(monkeypatch):
    good,bad=ticket(1),ticket(2);events=setup_control(monkeypatch,running=False,children=[good,bad])
    scope=scope_for(good,bad);good.state=Scheduled()
    async def read_scope(_):return scope
    monkeypatch.setattr(recovery,"read_scope",read_scope)
    async def tested(**kwargs):events.append("gateway-test");return {"successful_count":1}
    app=api.create_app({"deployment_id":DEPLOYMENT_ID},test_start_accounts=tested)
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://test") as c:
            return await c.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",json={"request_id":str(uuid4()),"intent_seq":1})
    with temporary_settings({PREFECT_API_URL:"http://127.0.0.1:1/api"}):r=asyncio.run(scenario())
    assert r.status_code==409 and not any(e in events for e in ("gateway-test","preflight","start"))

@pytest.mark.parametrize("failure",["children_read","receipt_after_admission"])
def test_any_scoped_start_failure_closes_admission_and_retains_unknown(monkeypatch,failure):
    good,bad=ticket(1),ticket(2);events=setup_control(monkeypatch,running=False,children=[good,bad])
    stored=scope_for(good,bad)
    async def read_scope(_):return deepcopy(stored)
    async def save_scope(value):stored.clear();stored.update(deepcopy(value))
    monkeypatch.setattr(recovery,"read_scope",read_scope);monkeypatch.setattr(recovery,"save_scope",save_scope)
    original_client=api.get_client
    @asynccontextmanager
    async def client_context():
        class Client:
            async def read_flow_run(self,id):
                if id==TASK_ID:
                    async with original_client() as c:return await c.read_flow_run(id)
                return good
            async def set_flow_run_state(self,id,state):good.state=state;return SimpleNamespace(state=state)
        yield Client()
    monkeypatch.setattr(api,"get_client",client_context)
    async def children(_):
        if "capacity" in events:raise OSError("fake snapshot read lost")
        return [good,bad]
    async def receipt(_):raise OSError("fake receipt read lost after admission")
    if failure=="children_read":monkeypatch.setattr(api,"_children",children)
    else:monkeypatch.setattr(api,"read_receipts",receipt)
    async def tested(**kwargs):return {"successful_count":1}
    async def applied(*args,**kwargs):events.append("capacity")
    app=api.create_app({"deployment_id":DEPLOYMENT_ID},test_start_accounts=tested,apply_start_capacity=applied)
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://test") as c:
            return await c.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",json={"request_id":str(uuid4()),"intent_seq":1})
    with temporary_settings({PREFECT_API_URL:"http://127.0.0.1:1/api"}):r=asyncio.run(scenario())
    assert r.status_code==409 and stored["prepared_at"] is None
    assert events[-1]=="pause" and not asyncio.run(api.Admission(TASK_ID).running())

def test_confirmed_but_dead_execution_never_claims_continue(monkeypatch):
    good,bad=ticket(1),ticket(2);scope=scope_for(good,bad)
    scope["eligible"][str(good.id)]["dispatch"]={"request_id":str(uuid4()),"state_id":str(uuid4())}
    good.state=Running();good.infrastructure_pid="77"
    monkeypatch.setattr(recovery,"execution_exists",lambda child:False)
    with pytest.raises(RuntimeError,match="执行进程待核对"):recovery.can_start(scope,[good,bad])

def test_late_old_start_failure_cannot_pause_or_overwrite_a_new_confirmed_start(monkeypatch):
    good,bad=ticket(1),ticket(2);events=setup_control(monkeypatch,running=False,children=[good,bad])
    stored=scope_for(good,bad)
    async def read_scope(_):return deepcopy(stored)
    async def save_scope(value):stored.clear();stored.update(deepcopy(value))
    monkeypatch.setattr(recovery,"read_scope",read_scope);monkeypatch.setattr(recovery,"save_scope",save_scope)
    original_client=api.get_client
    @asynccontextmanager
    async def client_context():
        class Client:
            async def read_flow_run(self,id):
                if id==TASK_ID:
                    async with original_client() as c:return await c.read_flow_run(id)
                return good
            async def set_flow_run_state(self,id,state):good.state=state;return SimpleNamespace(state=state)
        yield Client()
    monkeypatch.setattr(api,"get_client",client_context)
    waiting=asyncio.Event();release=asyncio.Event();calls=0
    async def receipt(_):
        nonlocal calls
        calls+=1
        if calls==1:
            waiting.set();await release.wait();raise OSError("old start read failed late")
        return []
    monkeypatch.setattr(api,"read_receipts",receipt)
    async def tested(**kwargs):return {"successful_count":1}
    async def applied(*args,**kwargs):pass
    app=api.create_app({"deployment_id":DEPLOYMENT_ID},test_start_accounts=tested,apply_start_capacity=applied)
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://test") as c:
            first=asyncio.create_task(c.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",json={"request_id":str(uuid4()),"intent_seq":1}))
            await asyncio.wait_for(waiting.wait(),3)
            paused=await c.post(f"/api/v1/prefect/tasks/{TASK_ID}/pause",json={"request_id":str(uuid4()),"intent_seq":2})
            newest=await c.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",json={"request_id":str(uuid4()),"intent_seq":3})
            release.set();old=await first
            return old,paused,newest,await api.Admission(TASK_ID).running()
    with temporary_settings({PREFECT_API_URL:"http://127.0.0.1:1/api"}):old,paused,newest,running=asyncio.run(scenario())
    assert newest.status_code==paused.status_code==old.status_code==200
    assert old.json()["status"]=="superseded" and newest.json()["status"]=="confirmed"
    assert running and stored["prepared_at"] is not None and events.count("pause")==1

def test_pause_registers_while_native_proposal_is_hung_and_never_replays_unknown(monkeypatch):
    good,bad=ticket(1),ticket(2);events=setup_control(monkeypatch,running=False,children=[good,bad])
    stored=scope_for(good,bad)
    async def read_scope(_):return deepcopy(stored)
    async def save_scope(value):stored.clear();stored.update(deepcopy(value))
    monkeypatch.setattr(recovery,"read_scope",read_scope);monkeypatch.setattr(recovery,"save_scope",save_scope)
    original_client=api.get_client;waiting=asyncio.Event();release=asyncio.Event()
    @asynccontextmanager
    async def client_context():
        class Client:
            async def read_flow_run(self,id):
                if id==TASK_ID:
                    async with original_client() as c:return await c.read_flow_run(id)
                return good
            async def set_flow_run_state(self,id,state):
                assert stored["dispatch_in_flight"]["ticket_id"]==str(id),"Durable uncertainty must precede the native request"
                events.append(("proposal",id));waiting.set();await release.wait();return SimpleNamespace(state=None)
        yield Client()
    monkeypatch.setattr(api,"get_client",client_context)
    async def tested(**kwargs):return {"successful_count":1}
    async def applied(*args,**kwargs):pass
    app=api.create_app({"deployment_id":DEPLOYMENT_ID},test_start_accounts=tested,apply_start_capacity=applied)
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://test") as c:
            first=asyncio.create_task(c.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",json={"request_id":str(uuid4()),"intent_seq":1}))
            await asyncio.wait_for(waiting.wait(),3)
            try:
                paused=await asyncio.wait_for(c.post(f"/api/v1/prefect/tasks/{TASK_ID}/pause",json={"request_id":str(uuid4()),"intent_seq":2}),1)
                assert paused.status_code==200 and not await api.Admission(TASK_ID).running()
                blocked=await asyncio.wait_for(c.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",json={"request_id":str(uuid4()),"intent_seq":3}),1)
                assert blocked.status_code==409
            finally:release.set()
            old=await first
            # Recreate the API to discard the short in-process submission guard.
            restarted=api.create_app({"deployment_id":DEPLOYMENT_ID},test_start_accounts=tested,apply_start_capacity=applied)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=restarted),base_url="http://test") as after_restart:
                again=await after_restart.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",json={"request_id":str(uuid4()),"intent_seq":4})
            return old,again,await api.Admission(TASK_ID).running()
    with temporary_settings({PREFECT_API_URL:"http://127.0.0.1:1/api"}):old,again,running=asyncio.run(scenario())
    assert again.status_code==409 and not running and "start" not in events
    assert len([e for e in events if isinstance(e,tuple) and e[0]=="proposal"])==1

@pytest.mark.parametrize("pause_point",["before_native","after_readback"])
def test_confirmed_unsent_or_sent_dispatch_settles_without_reopening_admission(monkeypatch,pause_point):
    good,remaining,bad=ticket(1),ticket(2),ticket(95)
    events=setup_control(monkeypatch,running=False,children=[good,remaining,bad])
    stored=scope_for(good,bad)
    stored["eligible"][str(remaining.id)]={**recovery.binding(remaining),"prepared_state_id":str(remaining.state.id)}
    paused=False;http=None
    async def pause_once():
        nonlocal paused
        paused=True
        r=await http.post(f"/api/v1/prefect/tasks/{TASK_ID}/pause",json={"request_id":str(uuid4()),"intent_seq":2})
        assert r.status_code==200
    async def read_scope(_):return deepcopy(stored)
    async def save_scope(value):
        stored.clear();stored.update(deepcopy(value))
        if pause_point=="before_native" and not paused and value.get("dispatch_in_flight"):await pause_once()
    monkeypatch.setattr(recovery,"read_scope",read_scope);monkeypatch.setattr(recovery,"save_scope",save_scope)
    original_client=api.get_client
    @asynccontextmanager
    async def client_context():
        class Client:
            async def read_flow_run(self,id):
                if id==TASK_ID:
                    async with original_client() as c:return await c.read_flow_run(id)
                found=next(x for x in (good,remaining) if x.id==id)
                if pause_point=="after_readback" and not paused and found.state.is_scheduled():await pause_once()
                return found
            async def set_flow_run_state(self,id,state):
                events.append(("schedule",id));next(x for x in (good,remaining) if x.id==id).state=state
                return SimpleNamespace(state=state)
        yield Client()
    monkeypatch.setattr(api,"get_client",client_context)
    async def tested(**kwargs):return {"successful_count":1}
    async def applied(*args,**kwargs):pass
    app=api.create_app({"deployment_id":DEPLOYMENT_ID},test_start_accounts=tested,apply_start_capacity=applied)
    async def scenario():
        nonlocal http
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://test") as c:
            http=c
            old=await c.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",json={"request_id":str(uuid4()),"intent_seq":1})
            assert old.status_code==200 and old.json()["status"]=="superseded"
            assert not await api.Admission(TASK_ID).running()
            assert stored["prepared_at"] and "dispatch_in_flight" not in stored
            assert len([e for e in events if isinstance(e,tuple) and e[0]=="schedule"])==(0 if pause_point=="before_native" else 1)
            resumed=await c.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",json={"request_id":str(uuid4()),"intent_seq":3})
            return resumed
    with temporary_settings({PREFECT_API_URL:"http://127.0.0.1:1/api"}):resumed=asyncio.run(scenario())
    assert resumed.status_code==200
    assert [e for e in events if isinstance(e,tuple) and e[0]=="schedule"]==[("schedule",good.id),("schedule",remaining.id)]
    assert events.count("start")==1 and bad.state.is_crashed()

def test_old_crashed_state_readback_does_not_confirm_native_proposal(monkeypatch):
    good,bad=ticket(1),ticket(2);events=setup_control(monkeypatch,running=False,children=[good,bad])
    stored=scope_for(good,bad)
    async def read_scope(_):return deepcopy(stored)
    async def save_scope(value):stored.clear();stored.update(deepcopy(value))
    monkeypatch.setattr(recovery,"read_scope",read_scope);monkeypatch.setattr(recovery,"save_scope",save_scope)
    original_client=api.get_client
    @asynccontextmanager
    async def client_context():
        class Client:
            async def read_flow_run(self,id):
                if id==TASK_ID:
                    async with original_client() as c:return await c.read_flow_run(id)
                return good
            async def set_flow_run_state(self,id,state):return SimpleNamespace(state=state)
        yield Client()
    monkeypatch.setattr(api,"get_client",client_context)
    async def tested(**kwargs):return {"successful_count":1}
    async def applied(*args,**kwargs):pass
    app=api.create_app({"deployment_id":DEPLOYMENT_ID},test_start_accounts=tested,apply_start_capacity=applied)
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://test") as c:
            return await c.post(f"/api/v1/prefect/tasks/{TASK_ID}/start",json={"request_id":str(uuid4()),"intent_seq":1})
    with temporary_settings({PREFECT_API_URL:"http://127.0.0.1:1/api"}):r=asyncio.run(scenario())
    assert r.status_code==409 and "start" not in events and stored["prepared_at"] is None
