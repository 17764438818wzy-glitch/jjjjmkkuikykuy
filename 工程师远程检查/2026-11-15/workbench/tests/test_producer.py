from pathlib import Path
from types import SimpleNamespace

import pytest

from paperflow.config import Config
from paperflow.producer import HarnessProducer, ProductionError, ModelServiceError


def test_llm_final_routes_in_corrections_are_projected_without_classifying(tmp_path,package_factory):
    from paperflow.package import load,save,synchronize_routes
    root,_,_=package_factory()
    p=load(root/'profile.json');del p['routing_candidates'];save(root/'profile.json',p)
    synchronize_routes(root)
    assert load(root/'profile.json')['routing_candidates']==load(root/'corrections.json')['final_routing_candidates']
    correction=load(root/'corrections.json');correction['final_routing_candidates']=[{'code':'other','name':'other'}]
    save(root/'corrections.json',correction)
    synchronize_routes(root)
    assert load(root/'profile.json')['routing_candidates']==correction['final_routing_candidates']


def test_model_chat_claim_does_not_replace_missing_files(tmp_path):
    class FakeHarness:
        def __init__(self, **kwargs):
            pass
        def start(self):
            pass
        def run(self, prompt, **kwargs):
            return SimpleNamespace(finish_reason='completed', final_response='全部已处理完成。', session_id='s')
        def close(self):
            pass
    config = Config(home=str(tmp_path/'runtime'), model_timeout=5)
    producer = HarnessProducer(config, factory=FakeHarness)
    original = tmp_path/'original.docx'
    original.write_bytes(b'original')
    from paperflow.package import sha
    task = dict(task_id='t1', source_id='s1', source_sha256=sha(original), source_path=str(original), revision=1)
    with pytest.raises(ProductionError, match='missing'):
        producer.run_stage(task, 'round1')


def test_stage_three_requires_relationship_description_not_old_values(tmp_path):
    config = Config(home=str(tmp_path/'runtime'))
    producer = HarnessProducer(config)
    task = dict(task_id='t',source_id='s',source_sha256='a'*64,source_path='original.pdf',revision=1)
    prompt = producer.prompt(task, 'round3', tmp_path/'output')
    assert '不要求逐项抄录' in prompt
    assert 'storage' in prompt
    assert '原稿' in prompt
    assert 'chunks.json' in prompt


def test_redaction_without_retained_images_has_no_image_tool_or_original_read_scope(tmp_path):
    from paperflow.package import save
    producer=HarnessProducer(Config(home=str(tmp_path/'runtime')))
    output=tmp_path/'round3_redact';output.mkdir()
    save(output/'assets.json',{'items':[{'id':'a','storage':'description'}, {'id':'b','storage':'omit'}]})
    task=dict(task_id='t',source_id='s',source_sha256='a'*64,source_path='original.pdf',
              revision=1,reading_dir=str(tmp_path/'private/source-view'))
    prompt=producer.prompt(task,'round3_redact',output)
    assert task['reading_dir'] not in prompt
    assert '学号、身份证号、私人联系方式' in prompt
    assert '不进行无差别删除' in prompt
    assert '不重新逐页扫描原稿' in prompt
    assert {'id':'attachment-local','disabled':True} in producer.tool_overrides(output)


def test_retained_images_remain_available_only_for_required_image_redaction(tmp_path):
    from paperflow.package import save
    producer=HarnessProducer(Config(home=str(tmp_path/'runtime')))
    view=tmp_path/'private/source-view';view.mkdir(parents=True)
    save(view/'source-index.json',{'images':[
        {'id':'keep','file':'images/keep.png'}, {'id':'omit','file':'images/omit.png'}]})
    output=tmp_path/'round3_redact';output.mkdir()
    save(output/'assets.json',{'items':[{'id':'a','storage':'image','file':'assets/a.png','source_image_id':'keep'},
                                      {'id':'b','storage':'omit','source_image_id':'omit'}]})
    task=dict(task_id='t',source_id='s',source_sha256='a'*64,source_path='original.pdf',revision=1,reading_dir=str(view))
    prompt=producer.prompt(task,'round3_redact',output)
    assert str(view/'images/keep.png') in prompt
    assert str(view/'images/omit.png') not in prompt
    assert {'id':'attachment-local','disabled':True} not in producer.tool_overrides(output)
    assert {'id':'attachment-local','disabled':True} in producer.tool_overrides(tmp_path/'round3_chunk')


@pytest.mark.parametrize('business_stage',['round3_redact','round3_chunk'])
def test_text_only_repair_inherits_failed_stage_reading_capability(tmp_path,business_stage):
    from paperflow.package import save
    producer=HarnessProducer(Config(home=str(tmp_path/'runtime')))
    output=tmp_path/'repair';output.mkdir();save(output/'assets.json',{'items':[]})
    task=dict(task_id='t',source_id='s',source_sha256='a'*64,source_path='original.pdf',revision=2,
              reading_dir=str(tmp_path/'private/source-view'),failed_stage=business_stage)
    assert task['reading_dir'] not in producer.prompt(task,'repair',output)
    assert '回到原稿核对' not in producer.prompt(task,'repair',output)
    assert {'id':'attachment-local','disabled':True} in producer.tool_overrides(output,business_stage)


def test_excluded_material_does_not_require_fake_body_or_tree(tmp_path):
    import json
    class FakeHarness:
        def __init__(self,**kwargs): self.root=Path(kwargs['cwd'])
        def start(self): pass
        def close(self): pass
        def run(self,*args,**kwargs):
            (self.root/'profile.json').write_text(json.dumps({'material_disposition':'exclude','reason':'查重报告'}),encoding='utf-8')
            return SimpleNamespace(finish_reason='completed',final_response='',session_id='s')
    from paperflow.package import sha
    original=tmp_path/'report.pdf'; original.write_bytes(b'report')
    task=dict(task_id='t',source_id='s',source_sha256=sha(original),source_path=str(original),revision=1)
    producer=HarnessProducer(Config(home=str(tmp_path/'runtime')),factory=FakeHarness)
    result=producer.run_stage(task,'round1')
    assert result['disposition']=='exclude'


def test_model_service_error_does_not_become_paper_content_repair(tmp_path):
    class FakeHarness:
        def __init__(self,**kwargs): pass
        def start(self): pass
        def close(self): pass
        def run(self,*args,**kwargs):
            return SimpleNamespace(finish_reason='error',final_response='',session_id='s')
    from paperflow.package import sha
    original=tmp_path/'report.pdf'; original.write_bytes(b'report')
    task=dict(task_id='t',source_id='s',source_sha256=sha(original),source_path=str(original),revision=1)
    producer=HarnessProducer(Config(home=str(tmp_path/'runtime')),factory=FakeHarness)
    with pytest.raises(ModelServiceError): producer.run_stage(task,'round1')


@pytest.mark.parametrize('status',[400,401,403,413])
def test_deterministic_model_request_rejection_is_controller_fault(tmp_path,status):
    class Harness:
        def __init__(self,**kwargs):pass
        def start(self):pass
        def close(self):pass
        def run(self,*args,**kwargs):
            event={'type':'turn/end','data':{'reason':{'kind':'error','error':{
                'message':f'{status}: request rejected [secret diagnostic]','code':'INVALID_REQUEST'}}}}
            return SimpleNamespace(finish_reason='error',events=[event])
    from paperflow.package import sha
    from paperflow.errors import is_infrastructure
    source=tmp_path/'source.txt';source.write_text('real input')
    task=dict(task_id='t',source_id='s',source_sha256=sha(source),source_path=str(source),revision=1)
    p=HarnessProducer(Config(home=str(tmp_path/'runtime')),factory=Harness)
    with pytest.raises(ValueError) as exc:p.run_stage(task,'round1')
    assert not is_infrastructure(exc.value)
    assert '[secret diagnostic]' not in str(exc.value)


def test_rate_limit_model_request_is_still_retryable(tmp_path):
    from paperflow.producer import model_failure
    assert isinstance(model_failure([dict(type='turn/end',data=dict(reason=dict(error=dict(message='429: rate limited'))))]),ModelServiceError)


@pytest.mark.parametrize('code,message',[
    ('AUTH','Invalid API key'),('MISSING_CREDENTIAL','provider credential unavailable'),
    ('INVALID_CREDENTIAL','invalid credential'),('INVALID_REQUEST','Request payload is too large'),
    ('INVALID_REQUEST','HTTP 413: Request payload is too large')])
def test_native_deterministic_rejection_codes_are_not_transport_retries(code,message):
    from paperflow.producer import model_failure,ModelRequestError
    error=model_failure([{'type':'turn/end','data':{'reason':{'error':{'code':code,'message':message}}}}])
    assert isinstance(error,ModelRequestError)


@pytest.mark.parametrize('status',[408,429,503])
def test_transient_http_status_takes_precedence_over_generic_invalid_request_code(status):
    from paperflow.producer import model_failure
    error=model_failure([{'type':'turn/end','data':{'reason':{'error':{'code':'INVALID_REQUEST','message':f'HTTP {status}: temporary'}}}}])
    assert isinstance(error,ModelServiceError)


def test_source_quality_notes_do_not_create_a_new_semantic_gate(tmp_path,package_factory):
    import shutil,json
    from paperflow.package import sha
    original=tmp_path/'source.pdf';original.write_bytes(b'paper')
    source,ident,_=package_factory(source_sha=sha(original))
    review=json.loads((source/'review.json').read_text(encoding='utf-8'))
    review['unresolved_issues']=[{'type':'source_defect','detail':'原论文数据自相矛盾，如实保留'}]
    (source/'review.json').write_text(json.dumps(review),encoding='utf-8')
    class Harness:
        def __init__(self,**kwargs):self.output=Path(kwargs['cwd'])
        def start(self):pass
        def close(self):pass
        def run(self,*args,**kwargs):
            for file in source.iterdir():
                if file.is_file():shutil.copy2(file,self.output/file.name)
            return SimpleNamespace(finish_reason='completed',final_response='',session_id='s')
    task=dict(ident,source_path=str(original),revision=1)
    result=HarnessProducer(Config(home=str(tmp_path/'runtime')),factory=Harness).run_stage(task,'round3')
    assert result['disposition']=='keep'


def test_invalid_json_is_fixed_in_same_session_and_same_revision(tmp_path):
    from paperflow.package import save,sha
    sessions=[]
    class Harness:
        def __init__(self,**kwargs):self.root=Path(kwargs['cwd'])
        def start(self):pass
        def close(self):pass
        def run(self,prompt,**kwargs):
            sessions.append(kwargs['session_id'])
            if len(sessions)==1:(self.root/'profile.json').write_text('{bad',encoding='utf-8')
            else:
                save(self.root/'profile.json',dict(material_disposition='keep',article={'title':'Methods'}))
                save(self.root/'tree.json',dict(nodes=[{'id':'s1','title':'Methods'}]))
                (self.root/'article.redacted.md').write_text('paper',encoding='utf-8')
            return SimpleNamespace(finish_reason='completed',events=[],final_response='',session_id='s')
    original=tmp_path/'a.txt';original.write_text('paper',encoding='utf-8')
    c=Config(home=str(tmp_path/'runtime'));task=dict(task_id='t',source_id='s',source_sha256=sha(original),source_path=str(original),revision=3)
    result=HarnessProducer(c,factory=Harness).run_stage(task,'round1')
    assert result['disposition']=='keep' and len(sessions)==2 and len(set(sessions))==1
    assert task['revision']==3 and not (Path(c.handoff_root)/'failed manuscript'/'t').exists()
