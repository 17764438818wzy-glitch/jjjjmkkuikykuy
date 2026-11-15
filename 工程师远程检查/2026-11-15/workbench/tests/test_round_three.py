from pathlib import Path
from types import SimpleNamespace
from paperflow.config import Config
from paperflow.graph import Workflow
from paperflow.package import PackageError,sha,save
from paperflow.producer import HarnessProducer
from paperflow.store import Store


def test_chunk_failure_replays_only_chunk_not_assets_or_redaction(tmp_path,package_factory):
    c=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[])
    s=Store(c.root/'state.sqlite3');source=tmp_path/'a.txt';source.write_text('paper',encoding='utf-8')
    task=s.register_source(source,sha(source));root,_,catalog=package_factory(task_id=task['task_id'],source_sha=task['source_sha256'])
    class Producer:
        calls=[]
        def run_stage(self,task,stage):
            self.calls.append(stage)
            if stage=='round3_chunk' and self.calls.count(stage)==1:raise PackageError('chunk links missing')
            return dict(disposition='keep',draft_dir=str(root))
    p=Producer();flow=Workflow(c,s,producer=p,catalog=catalog)
    for _ in range(20):
        task=flow.step(task)
        if task['stage']=='ingest':break
    assert task['stage']=='ingest'
    assert p.calls.count('round3_assets')==1 and p.calls.count('round3_redact')==1
    assert p.calls.count('round3_chunk')==1 and p.calls.count('repair')==1
    flow.close()


def test_unchanged_substage_input_reuses_verified_files(tmp_path,package_factory):
    import shutil
    original=tmp_path/'a.txt';original.write_text('paper',encoding='utf-8')
    root,ident,_=package_factory(source_sha=sha(original))
    class Harness:
        calls=0
        def __init__(self,**kwargs):self.output=Path(kwargs['cwd'])
        def start(self):pass
        def close(self):pass
        def run(self,*args,**kwargs):
            type(self).calls+=1
            for p in root.iterdir():
                if p.is_file():shutil.copy2(p,self.output/p.name)
            return SimpleNamespace(finish_reason='completed',events=[],final_response='',session_id='s')
    c=Config(home=str(tmp_path/'runtime'));p=HarnessProducer(c,factory=Harness)
    task=dict(ident,source_path=str(original),revision=1,draft_dir=str(root))
    first=p.run_stage(task,'round3_assets');second=p.run_stage(task,'round3_assets')
    assert second['cache_hit'] and Harness.calls==1
    (Path(first['draft_dir'])/'assets.json').write_text('{bad',encoding='utf-8')
    p.run_stage(task,'round3_assets')
    assert Harness.calls==2
