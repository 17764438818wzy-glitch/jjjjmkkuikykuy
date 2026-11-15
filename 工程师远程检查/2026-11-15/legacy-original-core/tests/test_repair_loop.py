"""完整自动返修回流，基础设施用隔离替身，技术检查和LangGraph用真实实现。"""
import shutil
from pathlib import Path

from paperflow.config import Config
from paperflow.graph import Workflow
from paperflow.package import sha,load,save
from paperflow.store import Store


def test_broken_delivery_returns_to_model_then_new_revision_reenters_ingestion(tmp_path,package_factory):
    c=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[],cleanup=False)
    s=Store(c.root/'state.sqlite3'); original=tmp_path/'source.pdf'; original.write_bytes(b'paper')
    task=s.register_source(original,sha(original))
    good,_,catalog=package_factory('good',task_id=task['task_id'],source_sha=task['source_sha256'])
    bad=tmp_path/'bad';shutil.copytree(good,bad)
    (bad/'chunks.json').unlink()
    calls=[]
    class Producer:
        def run_stage(self,task,stage):
            calls.append(stage)
            if stage=='repair':
                assert Path(task['failure_packet'],'failure.json').is_file()
                return dict(draft_dir=str(good),disposition='keep')
            return dict(draft_dir=str(bad),disposition='keep')
    flow=Workflow(c,s,producer=Producer(),catalog=catalog)
    task=s.update(task['task_id'],stage='publish',draft_dir=str(bad))
    task=flow.step(task)
    assert task['stage']=='repair' and task['revision']==2
    for _ in range(3): task=flow.step(task)
    assert calls==['repair']
    assert task['stage']=='ingest'
    assert '/r002' in task['package_dir'].replace('\\','/')
    assert Path(task['package_dir'],'READY.json').is_file()
    flow.close()
