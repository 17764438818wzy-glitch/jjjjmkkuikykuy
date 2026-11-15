"""A failed privacy review can be reconstructed without retaining every image."""
import copy
import hashlib
import io
import json
from pathlib import Path

import pytest
from PIL import Image,ImageDraw

import auto_evidence
from auto_evidence import Rejected,media_privacy


SECRET='fake-credential-never-write-to-diagnostics'
RETURNED_BOX={'bbox':[10,12,24,28],'coordinate_space':'pixel','api_key':SECRET}


def local_failure_case(tmp_path,monkeypatch):
    package=tmp_path/'package';(package/'_review').mkdir(parents=True);(package/'assets').mkdir()
    source=package/'_review'/'source.png'
    pixels=Image.new('RGB',(320,320),'white');draw=ImageDraw.Draw(pixels)
    draw.line((15,285,300,250),fill='navy',width=3)
    draw.text((90,90),'Research figure',fill='black');pixels.save(source)
    resource={'id':'visual_p20_1','kind':'image','position':{'page':20,'after_id':'block_0125'},
              'local_path':'_review/source.png','privacy_status':'pending'}
    meta={'resources':[resource],'blocks':[{'id':'image','type':'image',
          'resource_id':resource['id'],'caption':'Research chart'}]}
    monkeypatch.setattr(auto_evidence,'_media_source_context',lambda *args:{'api_key':SECRET,'source_context':{'text':SECRET}})
    class Agent:
        api_key=SECRET
        def __init__(self):self.calls=[];self.accepted=[]
        def call(self,task,payload,images=()):
            assert task=='privacy_image'
            self.calls.append((copy.deepcopy(payload),tuple(images)))
            n=len(self.calls)
            assert payload['phase']=={1:'identify',2:'verify_masked',3:'verify_masked',4:'locate_privacy_regions'}[n]
            assert len(images)==(1 if n<3 else 2)
            return {'decision':'pass' if n==1 else 'reject','research':True,
                    'reason':'Identity cannot be located reliably','coordinate_space':'normalized' if n==1 else 'pixel',
                    'sensitive_boxes':[[.05,.05,.2,.25]] if n==1 else ([] if n==2 else [copy.deepcopy(RETURNED_BOX)]),
                    'text':'Research figure','description':'Research line',
                    'api_key':SECRET,'raw_response':{'headers':{'Authorization':SECRET}}}
        def accept_cached_result(self,result):self.accepted.append(result)
    return package,source,meta,Agent()


def assert_unpublished(package,source,original_bytes,meta,original_meta,agent):
    assert source.read_bytes()==original_bytes and meta==original_meta
    assert not list((package/'assets').iterdir())
    assert not agent.accepted and len(agent.calls)==4


def test_failed_scaled_tile_saves_exact_last_inputs_and_sanitized_coordinates(tmp_path,monkeypatch):
    package,source,meta,agent=local_failure_case(tmp_path,monkeypatch)
    original_bytes=source.read_bytes();original_meta=copy.deepcopy(meta)
    with pytest.raises(Rejected,match='局部脱敏复核未通过'):
        media_privacy(meta,package,agent,lambda *args:None)
    assert_unpublished(package,source,original_bytes,meta,original_meta,agent)
    directory=package/'_review'/'privacy_failure'/'visual_p20_1'
    assert (directory/'last-input-1.png').is_file(), 'The actual resized tile input must survive rejection'
    assert (directory/'last-input-2.png').is_file(), 'The exact context overview must survive rejection'
    request=json.loads((directory/'last-request.json').read_text(encoding='utf-8'))
    response=json.loads((directory/'last-response.json').read_text(encoding='utf-8'))
    events=json.loads((directory/'calls.json').read_text(encoding='utf-8'))['calls']
    assert request['payload']=={'phase':'locate_privacy_regions','review_region':[0,0,176,176],
                               'context_image_indices':[2]}
    assert [entry['size'] for entry in request['inputs']]==[[352,352],[320,320]]
    assert response['coordinate_space']=='pixel'
    assert response['sensitive_boxes']==[{'bbox':[10,12,24,28],'coordinate_space':'pixel'}]
    for index,exact in enumerate(agent.calls[-1][1],1):
        assert (directory/f'last-input-{index}.png').read_bytes()==exact
        assert request['inputs'][index-1]['sha256']==hashlib.sha256(exact).hexdigest()
    assert len(events)==len(agent.calls)==4
    assert events[-1]['response']==response and events[-1]['payload']==request['payload']
    assert [len(event['inputs']) for event in events]==[1,1,2,2]
    overview=Image.open(directory/'last-input-2.png').convert('RGB')
    masked=Image.open(directory/'masked-full.png').convert('RGB')
    assert masked.tobytes()==overview.tobytes()
    assert sorted(path.name for path in directory.glob('last-input-*.png'))==['last-input-1.png','last-input-2.png']
    diagnostic_text=''.join(path.read_text(encoding='utf-8') for path in directory.glob('*.json'))
    assert SECRET not in diagnostic_text
    for forbidden in ('source_context','previous_decision','api_key','raw_response','Authorization'):
        assert forbidden not in diagnostic_text


@pytest.mark.parametrize('failure',['input','response'])
def test_last_input_trace_io_failure_keeps_same_original_rejection(tmp_path,monkeypatch,failure):
    package,source,meta,agent=local_failure_case(tmp_path,monkeypatch)
    original_bytes=source.read_bytes();original_meta=copy.deepcopy(meta);original=[];faults=[]
    verify=auto_evidence._verify_masked_observation
    def capture(*args,**kwargs):
        try:return verify(*args,**kwargs)
        except Rejected as exc:original.append(exc);raise
    monkeypatch.setattr(auto_evidence,'_verify_masked_observation',capture)
    write_bytes=Path.write_bytes;write_text=Path.write_text
    def fail_input(path,*args,**kwargs):
        if path.name=='last-input-1.png':faults.append('input');raise OSError('diagnostic disk unavailable')
        return write_bytes(path,*args,**kwargs)
    def fail_response(path,*args,**kwargs):
        if path.name=='last-response.json':faults.append('response');raise OSError('diagnostic disk unavailable')
        return write_text(path,*args,**kwargs)
    monkeypatch.setattr(Path,'write_bytes' if failure=='input' else 'write_text',fail_input if failure=='input' else fail_response)
    with pytest.raises(Rejected,match='局部脱敏复核未通过') as caught:
        media_privacy(meta,package,agent,lambda *args:None)
    assert len(original)==1 and caught.value is original[0]
    assert faults==[failure], 'Exercise the new diagnostic write, not an unrelated failure'
    assert_unpublished(package,source,original_bytes,meta,original_meta,agent)


def test_successful_review_writes_no_failure_diagnostics(tmp_path):
    package=tmp_path/'package';(package/'_review').mkdir(parents=True)
    source=package/'_review'/'source.png'
    im=Image.new('RGB',(80,80),'white');ImageDraw.Draw(im).line((5,5,65,65),fill='navy',width=2);im.save(source)
    resource={'id':'safe','kind':'image','local_path':'_review/source.png','privacy_status':'pending'}
    meta={'resources':[resource],'blocks':[{'resource_id':'safe','type':'image','caption':'Figure'}]}
    class Agent:
        calls=0
        def call(self,task,payload,images=()):
            self.calls+=1
            return {'decision':'pass','research':True,'reason':'Research line','sensitive_boxes':[],
                    'text':'Research line','description':'Research line'}
    agent=Agent()
    job=auto_evidence._media_job(meta,package,resource,agent,lambda *args:None,None,clone=False)
    assert job['action']=='approve' and job['agent'] is agent and agent.calls==1
    assert len(agent.processing_metrics)==1
    assert not (package/'_review'/'privacy_failure').exists()


def png_bytes(size,color):
    encoded=io.BytesIO();Image.new('RGB',size,color).save(encoded,format='PNG');return encoded.getvalue()


def test_proxy_retains_only_last_image_bytes_and_preserves_agent_state(tmp_path):
    from privacy_failure_trace import PrivacyFailureTrace
    class Agent:
        def __init__(self):self.calls=0;self.accepted=[];self.last_call_cache_hit=False
        def call(self,task,payload,images=(),**kwargs):
            self.calls+=1;self.last_call_cache_hit=self.calls==3
            self.latest={'decision':'pass','research':True,'sensitive_boxes':[[.1,.2,.3,.4]],
                         'coordinate_space':'normalized','reason':SECRET,'api_key':SECRET}
            return self.latest
        def accept_cached_result(self,result):self.accepted.append(result)
    agent=Agent();trace=PrivacyFailureTrace(agent)
    trace.processing_metrics=[]
    last=None
    for index,color in enumerate(('red','green','blue'),1):
        last=png_bytes((20+index,20),color)
        result=trace.call('privacy_image',{'phase':'verify_masked','headers':{'Authorization':SECRET}},[last])
        assert result is agent.latest
        trace.accept_cached_result(result)
    assert agent.calls==3 and len(agent.accepted)==3
    assert trace.processing_metrics is agent.processing_metrics
    assert trace.last_call_cache_hit is True
    assert trace._last_inputs==(last,)
    def retained_bytes(value):
        if isinstance(value,bytes):return len(value)
        if isinstance(value,dict):return sum(retained_bytes(item) for item in value.values())
        if isinstance(value,(list,tuple)):return sum(retained_bytes(item) for item in value)
        return 0
    assert retained_bytes(trace._events)==0
    assert len(trace._events)==3
    trace.write_failure(tmp_path)
    assert SECRET not in ''.join(path.read_text(encoding='utf-8') for path in tmp_path.glob('*.json'))
    history=json.loads((tmp_path/'calls.json').read_text(encoding='utf-8'))
    assert history['image_retention']=='latest_call_only'
    assert [entry['cache_hit'] for entry in history['calls']]==[False,False,True]


def test_new_one_image_failure_removes_stale_second_context_image(tmp_path):
    from privacy_failure_trace import PrivacyFailureTrace
    class Agent:
        def call(self,*args,**kwargs):return {'decision':'reject','research':True,'sensitive_boxes':[]}
    trace=PrivacyFailureTrace(Agent())
    first=png_bytes((30,30),'white');second=png_bytes((60,60),'white')
    trace.call('privacy_image',{'phase':'verify_masked','context_image_indices':[2]},[first,second])
    trace.write_failure(tmp_path)
    assert (tmp_path/'last-input-2.png').exists()
    trace.call('privacy_image',{'phase':'verify_masked'},[second])
    trace.write_failure(tmp_path)
    assert not (tmp_path/'last-input-2.png').exists(), 'An old context image must not look like the latest exact input'
    assert (tmp_path/'last-input-1.png').read_bytes()==second


def test_trace_collection_failure_does_not_change_agent_result_or_calls(monkeypatch):
    from privacy_failure_trace import PrivacyFailureTrace
    result={'decision':'pass','research':True,'sensitive_boxes':[]}
    class Agent:
        calls=0
        def call(self,*args,**kwargs):self.calls+=1;return result
    agent=Agent();trace=PrivacyFailureTrace(agent)
    def broken_record(*args,**kwargs):raise OSError('diagnostic failure')
    monkeypatch.setattr(PrivacyFailureTrace,'_begin',broken_record)
    assert trace.call('privacy_image',{'phase':'identify'},[png_bytes((20,20),'white')]) is result
    assert agent.calls==1
