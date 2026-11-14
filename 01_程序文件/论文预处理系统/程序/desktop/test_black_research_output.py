import io,json
from pathlib import Path
import pytest
from PIL import Image,ImageDraw
from auto_evidence import Rejected,media_privacy
from evidence_tasks import _transcribed_block
ROOT=Path(__file__).resolve().parent.parent

@pytest.mark.parametrize('color',['black','white'])
def test_uniform_research_crop_is_rejected_instead_of_approved(tmp_path,color):
    (tmp_path/'assets').mkdir();buf=io.BytesIO();Image.new('RGB',(200,120),color).save(buf,format='PNG')
    entry={'kind':'image','bbox':[.1,.1,.9,.9],'text':'Claimed research','description':'Claimed chart'}
    with pytest.raises(Rejected,match='全黑|全白'):
        _transcribed_block(entry,'crop',{'page':1},tmp_path,buf.getvalue(),[])
    assert not (tmp_path/'assets/crop.png').exists()

def test_partial_privacy_mask_keeps_content_and_coordinates(tmp_path):
    (tmp_path/'assets').mkdir();buf=io.BytesIO();img=Image.new('RGB',(200,100),'white')
    ImageDraw.Draw(img).rectangle((100,20,150,80),fill='blue');img.save(buf,format='PNG')
    entry={'kind':'image','bbox':[20,10,180,90],'coordinate_space':'pixel','text':'research','description':'blue chart'}
    _,r=_transcribed_block(entry,'crop',{'page':2},tmp_path,buf.getvalue(),[[.1,.1,.3,.9]])
    out=Image.open(tmp_path/r['local_path'])
    assert out.getpixel((0,0))==(0,0,0)
    assert out.getpixel((100,30))==(0,0,255)
    assert r['crop_provenance']['crop_coordinate_space']=='pixel'
    assert r['crop_provenance']['page_mask_boxes_normalized']==[[.1,.1,.3,.9]]

@pytest.mark.parametrize('already_approved',[False,True])
def test_media_approval_cannot_publish_or_skip_a_black_research_image(tmp_path,already_approved):
    (tmp_path/'assets').mkdir();(tmp_path/'_review').mkdir()
    im=Image.new('RGB',(100,100),'black' if already_approved else 'white')
    if not already_approved:ImageDraw.Draw(im).rectangle((20,20,80,80),fill='blue')
    im.save(tmp_path/'_review/source.png')
    r={'id':'r','kind':'image','local_path':'_review/source.png','position':{'page':1},'privacy_status':'approved' if already_approved else 'pending','visual_processed':already_approved}
    meta={'resources':[r],'blocks':[{'id':'b','type':'image','resource_id':'r','caption':'Fig.1 Study data'}]}
    class Agent:
        def call(self,task,payload,images=()):
            return {'decision':'pass','reason':'source read','research':True,'text':'blue data','description':'chart','sensitive_boxes':[[0,0,1,1]]}
    with pytest.raises(Rejected,match='全黑'):
        media_privacy(meta,tmp_path,Agent(),lambda *a:None)
    assert not (tmp_path/'assets/r.png').exists()


def test_overbroad_page_mask_gets_focused_privacy_recheck_before_publishing(tmp_path):
    (tmp_path/'assets').mkdir()
    page=Image.new('RGB',(200,100),'white')
    ImageDraw.Draw(page).rectangle((60,25,140,75),fill='blue')
    buf=io.BytesIO();page.save(buf,format='PNG')
    entry={'kind':'image','bbox':[.2,.1,.8,.9],'text':'student name; research table','description':'unreviewed source'}
    class Agent:
        def call(self,task,payload,images=()):
            assert task=='privacy_image' and len(images)==1
            return {'decision':'pass','reason':'仅有研究数据','research':True,
                    'sensitive_boxes':[],'text':'research table','description':'blue data'}
    _,resource=_transcribed_block(entry,'crop',{'page':1},tmp_path,buf.getvalue(),
                                  [[.2,.1,.8,.9]],agent=Agent())
    output=Image.open(tmp_path/resource['local_path']).convert('RGB')
    assert output.getpixel((60,40))==(0,0,255)
    assert resource['visual_integrity']['status']=='not_uniform_black_or_white'
    assert resource['ocr']['text']=='research table'
    assert resource['description']['text']=='blue data'


def test_focused_privacy_recheck_still_rejects_a_whole_image_mask(tmp_path):
    (tmp_path/'assets').mkdir()
    page=Image.new('RGB',(200,100),'white')
    ImageDraw.Draw(page).rectangle((60,25,140,75),fill='blue')
    buf=io.BytesIO();page.save(buf,format='PNG')
    entry={'kind':'image','bbox':[.2,.1,.8,.9],'text':'research table','description':'blue data'}
    class Agent:
        def call(self,task,payload,images=()):
            return {'decision':'pass','reason':'图片含身份','research':True,
                    'sensitive_boxes':[[0,0,1,1]],'text':'[已脱敏]','description':'身份图片'}
    with pytest.raises(Rejected,match='全黑'):
        _transcribed_block(entry,'crop',{'page':1},tmp_path,buf.getvalue(),
                           [[.2,.1,.8,.9]],agent=Agent())
    assert not (tmp_path/'assets/crop.png').exists()
