import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from uuid import UUID
import pytest
from paperflow_v3.control import parent,snapshots
from paperflow_v3.delivery import progress

def test_large_native_child_read_preserves_every_original_and_bounds_inflight(monkeypatch):
    children=[SimpleNamespace(id=UUID(int=n)) for n in range(1,2029)]
    active=0;peak=0
    class Client:
        async def count_flow_runs(self,**kwargs):return len(children)
        async def read_flow_runs(self,*,offset,limit,**kwargs):
            nonlocal active,peak
            active+=1;peak=max(peak,active)
            try:await asyncio.sleep(.002);return children[offset:offset+limit]
            finally:active-=1
    @asynccontextmanager
    async def client():
        try:yield Client()
        finally:assert active==0,"All read requests must finish before closing the client"
    monkeypatch.setattr(parent,'get_client',client)
    result=asyncio.run(parent._children(UUID(int=9000)))
    assert [c.id for c in result]==[c.id for c in children]
    assert 1<peak<=4

@pytest.mark.parametrize('fail',[False,True])
def test_snapshot_batch_reads_preserve_rows_and_drain_on_failure(fail):
    ids=[UUID(int=n) for n in range(1,1001)];active=0;peak=0
    class Client:
        async def request(self,method,path,*,json):
            assert method=='POST' and path=='/artifacts/latest/count'
            return SimpleNamespace(json=lambda:len(json['artifacts']['flow_run_id']['any_']))
        async def read_latest_artifacts(self,*,artifact_filter,offset,**kwargs):
            nonlocal active,peak
            active+=1;peak=max(peak,active)
            try:
                await asyncio.sleep(.002)
                run_ids=artifact_filter.flow_run_id.any_
                if fail and run_ids[0]==ids[200]:raise OSError('fake lost read')
                rows=[SimpleNamespace(type=snapshots.HISTORY_TYPE,flow_run_id=id) for id in run_ids[offset:offset+200]]
                return rows
            finally:active-=1
    async def run():
        try:return await snapshots._collections(Client(),ids,[snapshots.HISTORY_TYPE])
        finally:assert active==0,"Do not leave sibling reads running after a failed request"
    if fail:
        with pytest.raises(OSError,match='lost read'):asyncio.run(run())
    else:
        rows=asyncio.run(run());assert [r.flow_run_id for r in rows]==ids
    assert 1<peak<=4

@pytest.mark.parametrize('initial_count',[0,200])
def test_snapshot_growth_after_a_count_must_not_hide_new_or_tail_rows(initial_count):
    ids=[UUID(int=n) for n in range(1,202)]
    original=[SimpleNamespace(type=snapshots.HISTORY_TYPE,flow_run_id=ids[0],key=str(n)) for n in range(initial_count)]
    late=SimpleNamespace(type=snapshots.HISTORY_TYPE,flow_run_id=ids[0],key='late')
    visible=list(original);grew=False
    class Client:
        async def request(self,method,path,*,json):
            nonlocal grew
            matching=ids[0].hex in ''.join(str(x).replace('-','') for x in json['artifacts']['flow_run_id']['any_'])
            count=len(visible) if matching else 0
            if matching and not grew:visible.append(late);grew=True
            return SimpleNamespace(json=lambda:count)
        async def read_latest_artifacts(self,*,artifact_filter,offset,limit,**kwargs):
            nonlocal grew
            if ids[0] not in artifact_filter.flow_run_id.any_:return []
            if not grew:visible.append(late);grew=True
            return visible[offset:offset+limit]
    rows=asyncio.run(snapshots._collections(Client(),ids,[snapshots.HISTORY_TYPE]))
    assert {r.key for r in rows}=={r.key for r in [*original,late]}

def test_read_only_record_merge_keeps_order_and_unknown_without_copying(monkeypatch):
    local=[{'kind':'rag_effect_intent','data':{'effect_id':'same','status':'unknown'},'recorded_at':'2026-11-04T01:00:00Z'}]
    remote=[{'kind':'nas','data':{'status':'failed','files':{'x':{'status':'failed'}}},'recorded_at':'2026-11-04T00:00:00Z'}]
    copies=[];original=progress.copy.deepcopy
    def copying(value):copies.append(value);return original(value)
    monkeypatch.setattr(progress.copy,'deepcopy',copying)
    expected=progress.merge_records(local,remote)
    assert copies
    copies.clear()
    viewed=progress.merge_records(local,remote,copy_rows=False)
    assert viewed==expected and copies==[]
    expected[0]['data']['status']='changed'
    assert local[0]['data']['status']=='unknown'
