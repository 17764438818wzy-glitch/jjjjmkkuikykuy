import copy,io,json
from PIL import Image,ImageDraw
from auto_evidence import media_privacy
from evidence_tasks import _transcribed_block

IDENTITY='Author: Private Submitter'

def source(tmp_path):
    (tmp_path/'assets').mkdir();(tmp_path/'_review').mkdir()
    image=Image.new('RGB',(100,100),'white');ImageDraw.Draw(image).rectangle((50,30,80,80),fill='blue')
    image.save(tmp_path/'_review/source.png');stream=io.BytesIO();image.save(stream,format='PNG')
    return stream.getvalue()

def test_mask_coordinate_extra_identity_is_not_exported(tmp_path):
    source(tmp_path)
    resource={'id':'r','kind':'image','local_path':'_review/source.png','position':{'page':1}}
    meta={'resources':[resource],'blocks':[{'id':'b','type':'image','resource_id':'r'}]}
    class Agent:
        def call(self,task,payload,**kwargs):
            return {'decision':'pass','reason':'source read','research':True,'text':'blue data','description':'chart',
                'sensitive_boxes':[] if payload.get('phase')=='verify_masked' else
                    [{'bbox':[.1,.1,.2,.2],'coordinate_space':'normalized','label':IDENTITY}]}
    media_privacy(meta,tmp_path,Agent(),lambda *a:None)
    assert IDENTITY not in json.dumps(meta)
    assert resource['masked_regions']==[[.1,.1,.2,.2]]
    assert Image.open(tmp_path/resource['local_path']).getpixel((15,15))==(0,0,0)
    assert resource['visual_integrity']['provenance']['mask_boxes_pixels']==[[10,10,20,20]]

def test_crop_coordinate_extra_identity_is_not_exported(tmp_path):
    blob=source(tmp_path)
    entry={'kind':'image','bbox':{'bbox':[.1,.1,.9,.9],'coordinate_space':'normalized','label':IDENTITY},'text':'blue data','description':'chart'}
    block,resource=_transcribed_block(entry,'crop',{'page':1},tmp_path,blob,[])
    assert IDENTITY not in json.dumps([block,resource])
    assert resource['crop_provenance']['crop_bbox_normalized']==[.1,.1,.9,.9]
    assert Image.open(tmp_path/resource['local_path']).size==(80,80)

def test_approved_cached_metadata_drops_raw_coordinate_fields(tmp_path):
    blob=source(tmp_path)
    entry={'kind':'image','bbox':[.1,.1,.9,.9],'text':'blue data','description':'chart'}
    block,resource=_transcribed_block(entry,'crop',{'page':1},tmp_path,blob,[])
    old={'crop_bbox_input':{'bbox':[.1,.1,.9,.9],'label':IDENTITY},'mask_boxes_input':[{'bbox':[.1,.1,.2,.2],'label':IDENTITY}],
         'crop_bbox_normalized':[.1,.1,.9,.9],'mask_coordinate_space':IDENTITY}
    resource['crop_provenance']=copy.deepcopy(old)
    resource['visual_integrity']['provenance']=copy.deepcopy(old)
    resource['masked_observation_verified']=True
    meta={'resources':[resource],'blocks':[block]}
    class Agent:
        def call(self,*args,**kwargs):raise AssertionError('Previously approved pixels do not need another model call')
    media_privacy(meta,tmp_path,Agent(),lambda *a:None)
    assert IDENTITY not in json.dumps(meta)
    assert resource['crop_provenance']['crop_bbox_normalized']==[.1,.1,.9,.9]
