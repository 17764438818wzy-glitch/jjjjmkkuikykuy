import io

import pytest
from PIL import Image, ImageDraw

from auto_evidence import Rejected, media_privacy
from evidence_tasks import _transcribed_block


def page_fixture(tmp_path):
    (tmp_path/'assets').mkdir()
    page=Image.new('RGB',(200,120),'white')
    draw=ImageDraw.Draw(page)
    draw.rectangle((105,40,150,80),fill='blue')
    draw.text((25,25),'13800138000',fill='red')
    buffer=io.BytesIO();page.save(buffer,format='PNG')
    entry={'kind':'image','bbox':[.1,1/6,.9,5/6],
           'text':'old transcript','description':'old description'}
    return buffer.getvalue(),entry


def answer(boxes=(),**changes):
    return {'decision':'pass','reason':'已核对原裁图中的身份区域','research':True,
            'sensitive_boxes':list(boxes),'text':'研究统计图','description':'蓝色柱形图',**changes}


def test_partial_page_mask_rechecks_original_crop_and_preserves_true_phone_mask(tmp_path):
    source,entry=page_fixture(tmp_path)
    original_source=bytes(source)
    phases=[]

    class Agent:
        def call(self,task,payload,images=()):
            phases.append(payload['phase'])
            crop=Image.open(io.BytesIO(images[0])).convert('RGB')
            assert crop.getpixel((100,50))==(0,0,255)
            if payload['phase']=='verify_overbroad_page_mask':
                # Recheck receives the unmasked private source, not the broken
                # page-mask output, and selects only the actual phone region.
                assert crop.getpixel((10,10))!=(0,0,0)
                return answer([[4,4,80,19]],coordinate_space='pixel')
            assert crop.getpixel((10,10))==(0,0,0)
            return answer()

    agent=Agent()
    block,resource=_transcribed_block(entry,'crop',{'page':1},tmp_path,source,
        [[.1,.5,.9,5/6]],agent=agent)
    output=Image.open(tmp_path/resource['local_path']).convert('RGB')
    assert output.getpixel((100,50))==(0,0,255)
    assert output.getpixel((10,10))==(0,0,0)
    assert source==original_source
    assert resource['crop_provenance']['mask_application']=='focused_crop_recheck'
    # Rebuilt crops must still enter the normal post-mask review before upload.
    media_privacy({'blocks':[block],'resources':[resource]},tmp_path,agent,lambda *args:None)
    assert phases==['verify_overbroad_page_mask','locate_privacy_regions','verify_masked']


def test_partial_page_mask_review_rejection_never_publishes_unmasked_crop(tmp_path):
    source,entry=page_fixture(tmp_path)

    class Agent:
        def call(self,*args,**kwargs):
            return answer(decision='reject',reason='电话号码位置无法可靠核验',failure_kind='privacy')

    with pytest.raises(Rejected,match='无法可靠'):
        _transcribed_block(entry,'crop',{'page':1},tmp_path,source,
            [[.1,.5,.9,5/6]],agent=Agent())
    assert not (tmp_path/'assets/crop.png').exists()


def test_page_mask_outside_research_crop_does_not_call_agent(tmp_path):
    source,entry=page_fixture(tmp_path)

    class Agent:
        def call(self,*args,**kwargs):
            pytest.fail('Nonoverlapping mask must not request a new visual call')

    _,resource=_transcribed_block(entry,'crop',{'page':1},tmp_path,source,
        [[0,0,.05,.05]],agent=Agent())
    output=Image.open(tmp_path/resource['local_path']).convert('RGB')
    assert output.getpixel((100,50))==(0,0,255)
    assert 'mask_application' not in resource['crop_provenance']


def test_partial_page_mask_without_agent_keeps_mask_instead_of_unmasking(tmp_path):
    source,entry=page_fixture(tmp_path)
    _,resource=_transcribed_block(entry,'crop',{'page':1},tmp_path,source,
        [[.1,.5,.9,5/6]])
    output=Image.open(tmp_path/resource['local_path']).convert('RGB')
    assert output.getpixel((100,50))==(0,0,0)
    assert 'mask_application' not in resource['crop_provenance']


def test_focused_partial_mask_cannot_publish_uniform_black_result(tmp_path):
    source,entry=page_fixture(tmp_path)

    class Agent:
        def call(self,*args,**kwargs):
            return answer([[0,0,1,1]])

    with pytest.raises(Rejected,match='全黑'):
        _transcribed_block(entry,'crop',{'page':1},tmp_path,source,
            [[.1,.5,.9,5/6]],agent=Agent())
    assert not (tmp_path/'assets/crop.png').exists()
