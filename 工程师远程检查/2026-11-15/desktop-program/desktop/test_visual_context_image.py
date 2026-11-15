"""A masked overview may explain one local crop, never gain its own mask target."""
import io
import json

import pytest
from PIL import Image

from agent_client import AgentClient, AgentError
from response_contracts import validate_visual_coordinates
from test_vision_failover import _cfg, _ok, _Opener


def response(**changes):
    return dict(decision='pass', reason='Visible crop checked', research=True,
                sensitive_boxes=[], text='', description='Sports scene', **changes)


def images(count=2):
    blobs=[]
    for size in [(100, 80), (600, 300), (300, 200)][:count]:
        buffer=io.BytesIO();Image.new('RGB',size,'black').save(buffer,format='PNG')
        blobs.append(buffer.getvalue())
    return blobs


def client(tmp_path, result, modern=True):
    config=_cfg() if modern else {'base_url':'https://example.test/v1','vision_model':'vision','attempts':1}
    agent=AgentClient(config,key='test-key',vision_key='test-key',state_root=tmp_path)
    agent.opener=_Opener(_ok(result=result))
    return agent


@pytest.mark.parametrize('modern',[True,False])
@pytest.mark.parametrize('phase',['verify_masked','locate_privacy_regions'])
def test_real_request_chain_accepts_one_crop_plus_masked_overview(tmp_path, modern, phase):
    result=response();result.update(coordinate_space='pixel',sensitive_boxes=[[10,10,40,50]])
    agent=client(tmp_path,result,modern)
    payload={'phase':phase,'context_image_indices':[2]}
    actual=agent.call('privacy_image',payload,images())
    assert actual['sensitive_boxes']==[[10,10,40,50]]
    sent=json.loads(agent.opener.requests[0].data)
    user=sent['messages'][1]['content']
    assert len([part for part in user if part['type']=='image_url'])==2
    geometry=json.loads(user[0]['text'])['image_geometry']
    assert [(g['width'],g['height']) for g in geometry]==[(100,80),(600,300)]
    assert '第二张图片仅为同一时刻已遮盖的全图语境' in sent['messages'][0]['content']


@pytest.mark.parametrize('bad',[None,[],[1],[1,2],[2,2],[3],['2'],[2.0],(2,),True])
def test_invalid_context_declarations_fail_before_any_request(tmp_path,bad):
    agent=client(tmp_path,response())
    with pytest.raises(AgentError,match='context_image_indices'):
        agent.call('privacy_image',{'phase':'verify_masked','context_image_indices':bad},images())
    assert not agent.opener.requests


@pytest.mark.parametrize('task,phase,count',[
    ('repair','verify_masked',2),('extract_page','verify_masked',2),('ping','verify_masked',2),
    ('privacy_image','identify',2),('privacy_image','verify_masked',1),('privacy_image','verify_masked',3),
    ('privacy_image','verify_masked',0),
])
def test_context_mode_cannot_hide_other_tasks_or_extra_images(tmp_path,task,phase,count):
    agent=client(tmp_path,response())
    with pytest.raises(AgentError,match='context_image_indices'):
        agent.call(task,{'phase':phase,'context_image_indices':[2]},images(count))
    assert not agent.opener.requests


def test_context_coordinates_are_bounded_by_crop_not_larger_overview():
    result=response();result.update(coordinate_space='pixel',sensitive_boxes=[[150,10,180,40]])
    with pytest.raises(AgentError,match='坐标越界'):
        validate_visual_coordinates('privacy_image',result,[(100,80),(600,300)],[1],
                                    {'phase':'verify_masked','context_image_indices':[2]})


def test_overview_page_masks_are_never_accepted():
    result=response();result['page_masks']=[{'page':2,'boxes':[[.1,.1,.2,.2]]}]
    with pytest.raises(AgentError,match='page_masks'):
        validate_visual_coordinates('privacy_image',result,[(100,80),(600,300)],[1],
                                    {'phase':'verify_masked','context_image_indices':[2]})


def test_unmarked_extra_image_keeps_existing_rejection():
    with pytest.raises(AgentError,match='页码与图片尺寸'):
        validate_visual_coordinates('privacy_image',response(),[(100,80),(600,300)],[1],
                                    {'phase':'verify_masked'})
