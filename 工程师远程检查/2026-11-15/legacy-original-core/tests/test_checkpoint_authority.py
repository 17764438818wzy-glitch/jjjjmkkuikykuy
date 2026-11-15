from pathlib import Path
import pytest
from paperflow.config import Config
from paperflow.store import Store
from paperflow.graph import Workflow


def test_graph_committed_before_mirror_crash_is_not_executed_twice(tmp_path,monkeypatch):
    c=Config(home=str(tmp_path/'runtime'));s=Store(c.root/'state.sqlite3')
    task=s.register_source(tmp_path/'a.txt','a'*64)
    class Producer:
        calls=[]
        def run_stage(self,task,stage):self.calls.append(stage);return dict(disposition='keep',draft_dir=str(tmp_path/'draft'))
    p=Producer();flow=Workflow(c,s,producer=p,catalog={'majors':[]})
    original=s.update
    class Crash(BaseException):pass
    monkeypatch.setattr(s,'update',lambda *a,**kw:(_ for _ in ()).throw(Crash()))
    with pytest.raises(Crash):flow.step(task)
    flow.close();monkeypatch.setattr(s,'update',original)
    flow=Workflow(c,s,producer=p,catalog={'majors':[]})
    task=flow.step(s.get(task['task_id']))
    assert task['stage']=='round2' and p.calls==['round1']
    task=flow.step(s.get(task['task_id']))
    assert task['stage']=='round3' and p.calls==['round1','round2']
    flow.close()


def test_external_note_conflict_cannot_rewind_committed_internal_progress(tmp_path):
    c=Config(home=str(tmp_path/'runtime'));s=Store(c.root/'state.sqlite3');t=s.register_source(tmp_path/'a.txt','a'*64)
    class Producer:
        calls=[]
        def run_stage(self,task,stage):
            self.calls.append(stage)
            if stage=='round1':s.update(task['task_id'],external_note='keep this')
            return dict(disposition='keep',draft_dir=str(tmp_path/'draft'))
    p=Producer();flow=Workflow(c,s,producer=p,catalog={'majors':[]})
    flow.step(t);flow.close()
    flow=Workflow(c,s,producer=p,catalog={'majors':[]})
    t=flow.step(s.get(t['task_id']))
    assert t['stage']=='round2' and t['external_note']=='keep this' and p.calls==['round1']
    assert 'step_seq' not in flow.graph.get_state(flow.graph_config(t)).values['task']
    flow.close()


def test_control_request_replaces_pending_branch_continuation(tmp_path,package_factory):
    from paperflow.package import sha
    from paperflow.rag import RagPublisher
    from test_rag import MemoryRag
    c=Config(home=str(tmp_path/'run'),handoff_root=str(tmp_path/'handoff'),short_retry_interval=0)
    s=Store(c.root/'state');src=tmp_path/'s';src.write_text('test')
    t=s.register_source(src,sha(src));root,_,catalog=package_factory(task_id=t['task_id'],source_sha=t['source_sha256'])
    class Publisher(RagPublisher):
        broken=True
        def publish(self,*args):
            if self.broken:raise TypeError('test bug')
            return super().publish(*args)
    p=Publisher(MemoryRag());flow=Workflow(c,s,publisher=p,catalog=catalog)
    t=s.update(t['task_id'],stage='ingest',package_dir=str(root))
    t=flow.step(t);t=flow.step(t)
    assert t['stage']=='join_targets'
    p.broken=False;s.request(t['task_id'],stage='ingest',error=None,error_kind=None)
    flow.apply_pending_requests()
    for _ in range(10):
        t=flow.step(s.get(t['task_id']))
        if t['stage']=='complete':break
    assert t['stage']=='complete'
    flow.close()


@pytest.mark.parametrize('stage',['round1','round2','round3','round3_assets','round3_redact','round3_chunk','publish','validate','ingest','ingest_target','join_targets','activate','cleanup'])
def test_restart_after_each_committed_phase_never_replays_that_phase(tmp_path,package_factory,monkeypatch,stage):
    from paperflow.package import sha
    from paperflow.rag import RagPublisher
    from test_rag import MemoryRag
    c=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'))
    s=Store(c.root/'state.sqlite3');source=tmp_path/'paper.txt';source.write_text('original',encoding='utf-8')
    task=s.register_source(source,sha(source))
    root,_,catalog=package_factory(task_id=task['task_id'],source_sha=task['source_sha256'])
    class Producer:
        calls=[]
        def run_stage(self,task,stage):self.calls.append(stage);return dict(disposition='keep',draft_dir=str(root))
    p=Producer();client=MemoryRag();publisher=RagPublisher(client)
    flow=Workflow(c,s,producer=p,publisher=publisher,catalog=catalog)
    for _ in range(30):
        if task['stage']==stage:break
        task=flow.step(task)
    assert task['stage']==stage
    original=s.update
    class Crash(BaseException):pass
    monkeypatch.setattr(s,'update',lambda *a,**kw:(_ for _ in ()).throw(Crash()))
    with pytest.raises(Crash):flow.step(task)
    calls=list(p.calls);writes=client.writes;flow.close()
    monkeypatch.setattr(s,'update',original)
    flow=Workflow(c,s,producer=p,publisher=publisher,catalog=catalog)
    resumed=flow.step(s.get(task['task_id']))
    assert resumed['stage']!=stage and p.calls==calls and client.writes==writes
    flow.close()
