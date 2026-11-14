"""Validate actual image response boundaries, with only network I/O replaced."""
import copy
import io
import json
import pytest
from PIL import Image
from agent_client import AgentClient, AgentError

class Transport:
    def __init__(self,*results):self.results=list(results);self.requests=[]
    def open(self,request,timeout):
        self.requests.append(json.loads(request.data))
        result=self.results.pop(0)
        return io.BytesIO(json.dumps({'choices':[{'finish_reason':'stop','message':{'content':json.dumps(result)}}]}).encode())

def client(monkeypatch,*results):
    monkeypatch.setattr('agent_client.time.sleep',lambda _:None)
    c=AgentClient({'base_url':'https://example.test/v1','vision_model':'fixture','attempts':3},'fixture-key')
    c.opener=Transport(*results)
    return c

def picture():
    b=io.BytesIO();Image.new('RGB',(200,100),'white').save(b,format='PNG');return b.getvalue()

def repair(bbox):
    return {'decision':'pass','reason':'已定位','sensitive_boxes':[],
            'insertions':[{'kind':'image','after_id':'b1','page':1,'bbox':bbox,'text':'单位：万元','description':'研究图'}]}

def test_bad_crop_has_one_local_schema_retry_and_field_feedback(monkeypatch):
    c=client(monkeypatch,repair(None),repair([.1,.2,.8,.9]))
    result=c.call('repair',{'page':1},[picture()])
    assert result['insertions'][0]['bbox']==[.1,.2,.8,.9]
    assert len(c.opener.requests)==2
    feedback=json.dumps(c.opener.requests[1],ensure_ascii=False)
    assert 'insertions[0].bbox' in feedback

def test_valid_crop_is_not_scanned_twice(monkeypatch):
    c=client(monkeypatch,repair([.1,.2,.8,.9]))
    assert c.call('repair',{'page':1},[picture()])['decision']=='pass'
    assert len(c.opener.requests)==1

@pytest.mark.parametrize('bad',[None,[],[0,0,0,0],[.8,.2,.1,.9],[10,10,180,90],{'x':.1,'y':.2,'width':.7,'height':.7}])
def test_invalid_coordinate_never_passes_or_retries_forever(monkeypatch,bad):
    c=client(monkeypatch,repair(bad),repair(bad),repair(bad))
    with pytest.raises(AgentError):c.call('repair',{'page':1},[picture()])
    assert len(c.opener.requests)==3

def test_explicit_pixel_coordinates_use_input_geometry(monkeypatch):
    good=repair([20,10,180,90]);good['insertions'][0]['coordinate_space']='pixel'
    c=client(monkeypatch,good)
    assert c.call('repair',{'page':1},[picture()])==good
    assert len(c.opener.requests)==1

def test_multpage_privacy_mask_unit_uses_its_actual_page(monkeypatch):
    good=repair([20,10,180,90]);good['insertions'][0].update(page=2,coordinate_space='pixel')
    good['page_masks']=[{'page':1,'boxes':[]},{'page':2,'boxes':[[20,10,40,30]],'coordinate_space':'pixel'}]
    c=client(monkeypatch,good)
    assert c.call('repair',{'pages':[1,2]},[picture(),picture()])==good

def test_nonresearch_box_null_is_invalid_even_when_crop_is_valid(monkeypatch):
    bad={'decision':'pass','reason':'研究图','research':True,'sensitive_boxes':[None],'text':'数据','description':'研究图'}
    good={**bad,'sensitive_boxes':[]}
    c=client(monkeypatch,bad,good)
    assert c.call('privacy_image',{},[picture()])==good
    assert len(c.opener.requests)==2

def test_business_rejection_is_not_retried_as_schema_failure(monkeypatch):
    decision={'decision':'reject','reason':'关键文字确实无法读取'}
    c=client(monkeypatch,decision)
    assert c.call('repair',{'page':1},[picture()])==decision
    assert len(c.opener.requests)==1

@pytest.mark.parametrize('missing',[True,False])
def test_missing_or_null_mask_list_uses_same_bounded_repair(monkeypatch,missing):
    bad=repair([.1,.2,.8,.9])
    if missing:bad.pop('sensitive_boxes')
    else:bad['sensitive_boxes']=None
    c=client(monkeypatch,bad,bad,bad)
    with pytest.raises(AgentError):c.call('repair',{'page':1},[picture()])
    assert len(c.opener.requests)==3
    assert 'sensitive_boxes' in json.dumps(c.opener.requests[1],ensure_ascii=False)

@pytest.mark.parametrize('decision',[[],{}])
def test_invalid_decision_type_cannot_escape_bounded_response_repair(monkeypatch,decision):
    bad={**repair([.1,.2,.8,.9]),'decision':decision}
    c=client(monkeypatch,bad,bad,bad)
    with pytest.raises(AgentError):c.call('repair',{'page':1},[picture()])
    assert len(c.opener.requests)==3

def test_text_task_feedback_does_not_request_image_coordinates(monkeypatch):
    good={'decision':'pass','reason':'无身份信息','sensitive_strings':[]}
    c=client(monkeypatch,{**good,'sensitive_strings':None},good)
    c.config['text_model']='fixture'
    assert c.call('privacy_text',{'text':'示例研究文字'})==good
    feedback=c.opener.requests[1]['messages'][-1]['content']
    assert 'sensitive_strings' in feedback
    assert 'coordinate_space' not in feedback

@pytest.mark.parametrize('operation',[None,'replace','insert',[],{}])
def test_unknown_repair_operation_is_corrected_at_response_boundary(monkeypatch,operation):
    good=repair([.1,.2,.8,.9])
    bad=copy.deepcopy(good);bad['insertions'][0]['operation']=operation
    c=client(monkeypatch,bad,good)
    assert c.call('repair',{'page':1},[picture()])==good
    assert len(c.opener.requests)==2
    assert 'insertions[0].operation' in c.opener.requests[1]['messages'][-1]['content']

def test_invalid_operation_is_not_silently_changed_or_retried_indefinitely(monkeypatch):
    bad=repair([.1,.2,.8,.9]);bad['insertions'][0]['operation']='replace'
    c=client(monkeypatch,bad,bad,bad)
    with pytest.raises(AgentError):c.call('repair',{'page':1},[picture()])
    assert len(c.opener.requests)==3

def test_replace_table_operation_is_preserved_without_extra_model_call(monkeypatch):
    result={'decision':'pass','reason':'完整原表','sensitive_boxes':[],
            'insertions':[{'kind':'table','operation':'replace_table','after_id':'t1','page':1,'rows':[['项目','金额'],['收入','-12.00']]}]}
    c=client(monkeypatch,result)
    assert c.call('repair',{'page':1},[picture()])==result
    assert len(c.opener.requests)==1
