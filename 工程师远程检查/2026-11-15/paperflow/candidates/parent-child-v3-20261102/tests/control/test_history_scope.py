"""History coverage is independent from business completion and missing stages."""
import asyncio
from datetime import datetime,timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from prefect.states import Crashed
from paperflow_v3.control import snapshots


def fixture(monkeypatch, *, checked=True, gap=False, old=False):
    task=uuid4();child_id=uuid4();now=datetime.now(timezone.utc)
    child=SimpleNamespace(id=child_id,state=Crashed(),parameters={
        'task_id':str(task),'group_id':'paper-a','registered':{'task_id':str(task),
        'group_id':'paper-a','paper_id':'paper-a','receipt_id':'receipt-a','group':'synthetic'}})
    def history(run_id,version):
        return SimpleNamespace(flow_run_id=run_id,type=snapshots.HISTORY_TYPE,key='history-'+str(run_id),data={'version':version})
    rows=[history(task,snapshots.HISTORY_VERSION)]
    if checked:rows.append(history(child_id,snapshots.HISTORY_VERSION-1 if old else snapshots.HISTORY_VERSION))
    source=uuid4()
    if gap:rows.append(SimpleNamespace(flow_run_id=child_id,type=snapshots.GAP_TYPE,key='gap',
        data={'source_artifact_id':str(source),'stage':'llm'}))
    calls=[]
    class Client:
        async def __aenter__(self):return self
        async def __aexit__(self,*args):return None
        async def create_artifact(self,*args,**kwargs):raise AssertionError('No resolved evidence may be invented')
    async def collections(*args):calls.append('latest');return list(rows)
    async def pending(*args):return []
    async def originals(*args):raise AssertionError('Checked history must not be rescanned')
    async def by_id(*args):
        calls.append('gap-original')
        return [SimpleNamespace(id=source,flow_run_id=child_id,created=now,data={'kind':'child_error'})]
    monkeypatch.setattr(snapshots,'get_client',lambda:Client())
    monkeypatch.setattr(snapshots,'_collections',collections)
    monkeypatch.setattr(snapshots,'_pending_receipts',pending)
    monkeypatch.setattr(snapshots,'_originals',originals)
    monkeypatch.setattr(snapshots,'_originals_by_id',by_id)
    monkeypatch.setattr(snapshots,'_failed_snapshot',lambda *args:None)
    from paperflow_v3.delivery import progress
    monkeypatch.setattr(progress,'local_records',lambda *args:[])
    return task,child,calls


@pytest.mark.parametrize('checked,old,pending',[(True,False,False),(False,False,True),(True,True,True)])
def test_missing_stage_needs_history_only_without_current_checked_marker(monkeypatch,checked,old,pending):
    task,child,_=fixture(monkeypatch,checked=checked,old=old)
    result=asyncio.run(snapshots.load_task_snapshot(task,[child]))
    assert result['history_targets']==([str(child.id)] if pending else [])
    assert result['has_pending'] is True and result['complete'] is False
    assert result['missing'] and result['folders'][0]['stage']!='completed'


def test_unresolved_gap_keeps_its_target_without_refetching_unchanged_latest_rows(monkeypatch):
    task,child,calls=fixture(monkeypatch,gap=True)
    result=asyncio.run(snapshots.load_task_snapshot(task,[child],repair=True))
    assert result['history_targets']==[str(child.id)]
    assert calls==['latest','gap-original']
    assert result['has_pending'] is True
    assert result['folders'][0]['retryable'] is False
