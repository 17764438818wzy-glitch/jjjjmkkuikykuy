import shutil
import pytest
from paperflow.config import Config
from paperflow.graph import Workflow
from paperflow.package import load,save,sha,seal_package
from paperflow.store import Store
from paperflow.rag import RagPublisher
from test_rag import MemoryRag


@pytest.mark.parametrize('failed_stage',['validate','ingest_target','verify','activate'])
def test_repaired_consumers_use_a_new_published_revision(tmp_path,package_factory,failed_stage):
    c=Config(home=str(tmp_path/'run'),handoff_root=str(tmp_path/'handoff'),source_roots=[],cleanup=False)
    s=Store(c.root/'state.sqlite3');source=tmp_path/'s.txt';source.write_text('source')
    t=s.register_source(source,sha(source));good,_,catalog=package_factory('good',task_id=t['task_id'],source_sha=t['source_sha256'])
    bad=tmp_path/'bad';shutil.copytree(good,bad)
    # 关联别名现在仅作备注；以真正丢失正文的交付故障验证消费者返修边。
    obj=load(bad/'chunks.json');obj['chunks'][0]['content']='正文缺失';save(bad/'chunks.json',obj);seal_package(bad,t,1)
    class Producer:
        def run_stage(self,task,stage):
            assert stage=='repair'
            return dict(draft_dir=str(good),disposition='keep')
    t=s.update(t['task_id'],stage=failed_stage,package_dir=str(bad),draft_dir=str(bad),route_codes=['120201'])
    # Start the target through the same ingest dispatch preparation used by the service.
    if failed_stage=='ingest_target':t=s.update(t['task_id'],stage='ingest')
    flow=Workflow(c,s,producer=Producer(),publisher=RagPublisher(MemoryRag()),catalog=catalog)
    for _ in range(16):
        t=flow.step(t)
        if t['stage'] in ('complete','parked','code_error'):break
    assert t['stage']=='complete',t
    assert t['revision']==2 and 'r002' in t['package_dir']
    assert load(bad/'chunks.json')['chunks'][0]['content']=='正文缺失'
    flow.close()


def test_target_failure_budget_survives_join_and_parks(tmp_path,package_factory):
    c=Config(home=str(tmp_path/'run'),handoff_root=str(tmp_path/'handoff'),source_roots=[],
             retry_seconds=0,short_retry_interval=0,max_infrastructure_retries=2)
    s=Store(c.root/'state.sqlite3');source=tmp_path/'s';source.write_text('x')
    t=s.register_source(source,sha(source));root,_,catalog=package_factory(task_id=t['task_id'],source_sha=t['source_sha256'])
    class Publisher(RagPublisher):
        def publish(self,*args):raise TimeoutError('server unavailable')
    t=s.update(t['task_id'],stage='ingest',package_dir=str(root))
    flow=Workflow(c,s,publisher=Publisher(MemoryRag()),catalog=catalog)
    for _ in range(12):
        t=flow.step(t)
        if t['stage']=='parked':break
    assert t['stage']=='parked' and t['retry_count']==2,t
    assert t['repair_count']==0
    flow.close()
