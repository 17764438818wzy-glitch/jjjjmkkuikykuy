"""Exercise stage payloads and real producer/graph handoffs without external calls."""
from pathlib import Path
from types import SimpleNamespace

import pytest

from paperflow.config import Config
from paperflow.graph import Workflow
from paperflow.package import load, save, sha
from paperflow.producer import HarnessProducer
from paperflow.store import Store


@pytest.mark.parametrize('phase,title',[
    ('round3_assets','图表处理'),('round3_redact','脱敏'),('round3_chunk','切片')])
@pytest.mark.parametrize('repair',[False,True])
def test_each_model_request_uses_only_its_approved_stage_contract(tmp_path,package_factory,phase,title,repair):
    output,ident,_=package_factory()
    task=dict(ident,source_path='DO_NOT_READ_original.pdf',reading_dir='DO_NOT_READ_source_view',revision=2)
    if repair:
        task['failed_stage']=phase
        save(output/'failure.json',dict(business_stage=phase,error='specific delivery fault'))
    prompt=HarnessProducer(Config(home=str(tmp_path/'runtime'))).prompt(task,'repair' if repair else phase,output)
    assert prompt.startswith('你负责论文处理工作流第三轮中的“'+title+'”步骤。')
    assert '【本次明确批准的任务协议，优先执行】' not in prompt
    assert '第三轮及修复轮还须生成' not in prompt
    assert '〈task_id〉' not in prompt and ident['source_sha256'] in prompt
    if phase!='round3_assets':
        assert 'DO_NOT_READ_original.pdf' not in prompt and 'DO_NOT_READ_source_view' not in prompt
    if phase=='round3_assets':assert 'chunks.json' not in prompt
    if repair:assert 'failure.json' in prompt and '累计最多两次' in prompt


def test_chunk_request_provides_final_redacted_lines_without_modifying_source(package_factory,tmp_path):
    output,ident,_=package_factory()
    article=output/'article.redacted.md'
    article.write_bytes(b'\xef\xbb\xbf'+ '# 最终正文\r\n\r\n作者A的方法。\r\n末行'.encode('utf-8'))
    before=sha(article)
    task=dict(ident,source_path='original.pdf',revision=1)
    prompt=HarnessProducer(Config(home=str(tmp_path/'runtime'))).prompt(task,'round3_chunk',output)
    view=output/'private/article.numbered.txt'
    assert view.is_file()
    assert view.read_text(encoding='utf-8').splitlines()==['1\t# 最终正文','2\t','3\t作者A的方法。','4\t末行']
    assert str(view) in prompt and '共 4 行' in prompt and before in prompt
    assert sha(article)==before
    article.write_text('新的最终正文\n第二行',encoding='utf-8')
    second=HarnessProducer(Config(home=str(tmp_path/'runtime'))).prompt(task,'round3_chunk',output)
    assert view.read_text(encoding='utf-8').splitlines()==['1\t新的最终正文','2\t第二行']
    assert '共 2 行' in second and sha(article) in second


def test_changing_stage_template_prevents_reusing_old_stage_outputs(tmp_path,package_factory,monkeypatch):
    import paperflow.producer as producer_module
    import paperflow.phase_cache as cache_module
    templates=tmp_path/'prompts';templates.mkdir()
    import shutil
    shutil.copy2(cache_module.PROMPTS/'free-route.yml', templates/'free-route.yml')
    template=templates/'round3_assets.txt';template.write_text('图表阶段版本一',encoding='utf-8')
    monkeypatch.setattr(producer_module,'PROMPTS',templates)
    monkeypatch.setattr(cache_module,'PROMPTS',templates,raising=False)
    source=tmp_path/'source.txt';source.write_text('paper',encoding='utf-8')
    draft,ident,_=package_factory(source_sha=sha(source))
    class Harness:
        calls=0
        def __init__(self,**kwargs):pass
        def start(self):pass
        def close(self):pass
        def run(self,*args,**kwargs):
            type(self).calls+=1
            return SimpleNamespace(finish_reason='completed',events=[])
    task=dict(ident,source_path=str(source),revision=1,draft_dir=str(draft))
    producer=HarnessProducer(Config(home=str(tmp_path/'runtime')),factory=Harness)
    producer.run_stage(task,'round3_assets')
    assert producer.run_stage(task,'round3_assets').get('cache_hit') and Harness.calls==1
    template.write_text('图表阶段版本二',encoding='utf-8')
    assert not producer.run_stage(task,'round3_assets').get('cache_hit')
    assert Harness.calls==2


def test_invalid_body_range_is_repaired_with_chunk_contract_and_continues_without_repeat(tmp_path,package_factory):
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[])
    store=Store(config.root/'state.sqlite3')
    source=tmp_path/'source.txt';source.write_text('paper',encoding='utf-8')
    task=store.register_source(source,sha(source))
    draft,_,catalog=package_factory(task_id=task['task_id'],source_sha=task['source_sha256'])
    for name in ('profile.json','tree.json','assets.json','chunks.json','review.json','corrections.json'):
        value=load(draft/name);value['source_id']=task['source_id'];save(draft/name,value)
    calls=[]
    class Harness:
        def __init__(self,**kwargs):self.output=Path(kwargs['cwd'])
        def start(self):pass
        def close(self):pass
        def run(self,prompt,**kwargs):
            phase=self.output.name;calls.append((phase,prompt))
            if phase in ('round3_chunk','repair'):
                view=self.output/'private/article.numbered.txt'
                assert view.is_file(), 'producer must provide accurate final line numbers before the model'
                end=int(view.read_text(encoding='utf-8').splitlines()[-1].split('\t',1)[0])
                save(self.output/'chunks.json',dict(chunks=[dict(id='b1',kind='body',line_start=1,
                    line_end=end+1 if sum(p=='round3_chunk' for p,_ in calls)==1 else end)]))
            return SimpleNamespace(finish_reason='completed',events=[])
    task=store.update(task['task_id'],stage='round3_assets',draft_dir=str(draft))
    flow=Workflow(config,store,producer=HarnessProducer(config,factory=Harness),catalog=catalog)
    try:
        for _ in range(10):
            task=flow.step(task)
            if task['stage'] in ('publish','parked','code_error'):break
        assert task['stage']=='publish'
        assert [phase for phase,_ in calls]==['round3_assets','round3_redact','round3_chunk','round3_chunk']
        assert calls[-1][1].startswith('你负责工作流的定点交付补交')
        assert '只修 chunks.json' in calls[-1][1]
        assert '第三轮及修复轮还须生成' not in calls[-1][1]
        assert task['correction_count']==1 and task['revision']==1
        output=Path(task['draft_dir'])
        assert ''.join(row['content'] for row in load(output/'chunks.json')['chunks'])==(output/'article.redacted.md').read_text(encoding='utf-8')
    finally:flow.close()


@pytest.mark.parametrize('check,reason,target',[
    ('body_loss','body coverage incomplete','chunks.json'),
    ('privacy','explicit private identity remains in profile.json','准确位置'),
    ('missing_files','missing required asset description','assets.json')])
def test_consumer_repairs_select_a_scoped_contract_without_changing_resume_stage(tmp_path,package_factory,check,reason,target):
    output,ident,_=package_factory()
    task=dict(ident,source_path='DO_NOT_READ_original.pdf',reading_dir='DO_NOT_READ_source_view',revision=2,
              failed_stage='validate',delivery_check=check,error=reason)
    save(output/'failure.json',dict(business_stage='validate',delivery_check=check,error=reason))
    prompt=HarnessProducer(Config(home=str(tmp_path/'runtime'))).prompt(task,'repair',output)
    assert prompt.startswith('你负责工作流的定点交付补交')
    assert target in prompt and 'DO_NOT_READ_original.pdf' not in prompt
    assert '第三轮及修复轮还须生成' not in prompt and 'failure.json' in prompt
    assert task['failed_stage']=='validate'


def test_unspecified_consumer_fault_does_not_reintroduce_mixed_business_instructions(tmp_path,package_factory):
    output,ident,_=package_factory()
    task=dict(ident,source_path='DO_NOT_READ_original.pdf',revision=2,failed_stage='publish',
              delivery_check='missing_files',error='missing model artifacts: profile.json')
    prompt=HarnessProducer(Config(home=str(tmp_path/'runtime'))).prompt(task,'repair',output)
    assert 'failure.json' in prompt and '累计最多两次' in prompt
    assert '第三轮及修复轮还须生成' not in prompt
    assert '分类、切片仍由你判断' not in prompt and 'DO_NOT_READ_original.pdf' not in prompt
    assert task['failed_stage']=='publish'


def test_numbered_view_uses_the_same_unicode_line_boundaries_as_materialization(package_factory,tmp_path):
    output,ident,_=package_factory()
    (output/'article.redacted.md').write_text('# 章节\n段落一\f段落二\x85段落三\u2028末行\u2029',encoding='utf-8')
    prompt=HarnessProducer(Config(home=str(tmp_path/'runtime'))).prompt(dict(ident,source_path='original',revision=1),'round3_chunk',output)
    assert (output/'private/article.numbered.txt').read_text(encoding='utf-8').splitlines()==[
        '1\t# 章节','2\t段落一','3\t段落二','4\t段落三','5\t末行']
    assert '共 5 行' in prompt
