from pathlib import Path
import pytest
from test_workbench import paper


@pytest.mark.parametrize('kind',['毕业论文任务书','本科毕业论文开题报告','个人简历','招聘启事','关于提交材料的通知'])
def test_explicit_nonpaper_heading(kind):
    from screening import assess
    raw=kind+'\n'+('请按要求填写并提交相关材料。\n'*350)
    result=assess(raw,raw,'review','missing_abstract;missing_references')
    assert result['status']=='reject' and 'non_paper' in result['rule_ids']
    assert kind in result['reason']


def test_topic_mentions_are_not_nonpaper_evidence():
    from screening import assess
    raw=paper('网络广告传播策略研究')+'\n附录\n开题报告\n本文讨论招聘启事与通知的语言。'
    assert assess(raw,raw,'pass','')['status']=='pass'
    no_refs=paper(references=False)
    assert assess(no_refs,no_refs,'review','missing_references')['status']=='review'


def test_whitespace_does_not_meet_minimum_length():
    from screening import assess
    result=assess('短正文','摘要\n第一章\n'+(' ' *4000)+'正文','review','missing_references')
    assert result['status']=='reject'
    assert 'too_short' in result['rule_ids'] and result['effective_char_count']<3000


def test_reject_before_expensive_package_and_block_upload(tmp_path,monkeypatch):
    import evidence
    from engine import Store,Processor,scan_sources,approve_items
    from ragflow_client import upload_items
    source=tmp_path/'input';source.mkdir()
    (source/'notice.txt').write_text('关于提交材料的通知\n'+('请按要求填写材料。'*500),encoding='utf-8')
    (source/'short.txt').write_text('摘要\n第一章\n很短的正文',encoding='utf-8')
    (source/'paper.txt').write_text(paper(),encoding='utf-8')
    original=evidence.build_package;called=[]
    def build(src,*args,**kwargs):
        called.append(Path(src).name)
        return original(src,*args,**kwargs)
    monkeypatch.setattr(evidence,'build_package',build)
    store=Store(tmp_path/'output');m=Processor(store).run(scan_sources(source))
    assert m['status']=='completed'
    rejected=[i for i in m['items'] if i['status']=='reject']
    assert len(rejected)==2 and called==['paper.txt']
    assert all('excluded' in Path(i['path']).parts and i['reason'] for i in rejected)
    assert (source/'notice.txt').exists()
    with pytest.raises(ValueError):approve_items(store,m['run_id'],[rejected[0]['id']])
    with pytest.raises(ValueError):upload_items(store,m['run_id'],[rejected[0]['id']],None,{'id':'unused'})
