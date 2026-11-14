import io
import math
import pytest
from PIL import Image
from agent_client import AgentError
from auto_evidence import bounds, media_privacy, Rejected
from evidence_tasks import _locate, resolve_scoped, _transcribed_block


@pytest.mark.parametrize('value,unit',[
    ([20,10,180,90],'pixel'),([10,10,90,90],'percent'),([100,100,900,900],'0_1000'),
    (['0.1','0.1','0.9','0.9'],'normalized'),
    ({'x0':20,'y0':10,'x1':180,'y1':90,'coordinate_space':'pixel'},'normalized')])
def test_explicit_coordinate_units(value,unit):
    assert bounds(value,(200,100),unit)==pytest.approx([.1,.1,.9,.9])


@pytest.mark.parametrize('value,unit',[
    ([20,10,180,90],'normalized'),([.8,.1,.2,.9],'normalized'),
    ([0,0,0,0],'normalized'),([0,0,math.nan,1],'normalized'),
    ([0,False,1,1],'normalized'),([-1,0,200,100],'pixel'),([0,0,1,1],'unknown')])
def test_ambiguous_invalid_boxes_still_fail(value,unit):
    with pytest.raises(AgentError):bounds(value,(200,100),unit)


def test_tiny_rounding_error_only_is_clipped():
    assert bounds([-1e-8,0,1+1e-8,1])==[0,0,1,1]
    with pytest.raises(AgentError):bounds([-.02,0,1.02,1])


def test_formula_uses_own_label_not_later_figure():
    blocks=[{'id':'f','type':'paragraph','text':'$y=bx$（4.1）','position':{'body_index':1}},
            {'id':'cap','type':'paragraph','text':'图4.2 后面的经营数据图','position':{'body_index':2}}]
    assert _locate({'code':'formula_review','position':blocks[0]['position']},blocks,
                   ['公式 y=bx (4.1)','图4.2 后面的经营数据图'])=={0}


def test_figure_uses_body_page_not_toc():
    blocks=[{'id':'g','type':'paragraph','text':'','position':{'body_index':1}},
            {'id':'caption','type':'paragraph','text':'图1.1 本文研究框架','position':{'body_index':2}}]
    assert _locate({'code':'embedded_object_review','position':blocks[0]['position']},blocks,
                   ['目录\n图1.1 本文研究框架 .... 9','图1.1 本文研究框架\n完整研究图'])=={1}


def test_formula_in_table_does_not_borrow_another_cell():
    block={'id':'t','type':'table','position':{'body_index':1},'cells':[
        {'row':0,'column':0,'text':'这里的统计表说明文字足够长可以定位'},
        {'row':0,'column':1,'text':'$y=x$ (3.2)'}]}
    assert _locate({'code':'formula_review','position':{'body_index':1,'row':0,'column':1}},[block],
                  ['这里的统计表说明文字足够长可以定位','y=x (3.2)'])=={1}


def test_blank_page_is_locally_skipped_but_nonblank_scan_is_sent(tmp_path):
    import fitz
    source=tmp_path/'source.pdf'
    with fitz.open() as pdf:
        pdf.new_page(width=100,height=100)
        page=pdf.new_page(width=100,height=100);page.draw_line((10,10),(80,80))
        pdf.save(source)
    meta={'blocks':[],'resources':[],'issues':[{'code':'scanned_page','position':{'page':n},'status':'open'} for n in [1,2]]}
    class Agent:
        calls=[]
        def call(self,task,payload,images=()):
            self.calls.append(payload['page'])
            return {'decision':'pass','reason':'读取非空扫描页','blocks':[{'kind':'text','text':'恢复的研究正文'}],'sensitive_boxes':[]}
    agent=Agent();resolve_scoped(meta,tmp_path,source,tmp_path/'visual',80,agent,lambda *a:None)
    assert agent.calls==[2]
    assert meta['issues'][0]['resolution']=='uniform_white_page_omitted'
    assert meta['blocks'][0]['text']=='恢复的研究正文'


def media(tmp_path,kind='image',caption=''):
    (tmp_path/'assets').mkdir();(tmp_path/'private').mkdir()
    Image.new('RGB',(100,100),'white').save(tmp_path/'private/image.png')
    return {'blocks':[{'id':'b','type':'image','resource_id':'r','caption':caption}],
            'resources':[{'id':'r','kind':kind,'local_path':'private/image.png','position':{}}]}


def test_explicit_nonresearch_reject_discards_image_not_article(tmp_path):
    meta=media(tmp_path)
    class Agent:
        def call(self,*a,**kw):return {'decision':'reject','reason':'仅有校徽','research':False,'nonresearch_kind':'logo'}
    media_privacy(meta,tmp_path,Agent(),lambda *a:None)
    assert not meta['resources'] and not meta['blocks']
    assert meta['excluded_resources'][0]['id']=='r'


@pytest.mark.parametrize('kind,caption',[('table_screenshot',''),('image','图1 财务趋势')])
def test_research_evidence_cannot_be_discarded_as_logo(tmp_path,kind,caption):
    meta=media(tmp_path,kind,caption)
    class Agent:
        def call(self,*a,**kw):return {'decision':'reject','reason':'校徽','research':False,'nonresearch_kind':'logo'}
    with pytest.raises(Rejected):media_privacy(meta,tmp_path,Agent(),lambda *a:None)
    assert meta['resources']


def test_tiny_captioned_connector_without_verified_coverage_is_not_discarded(tmp_path):
    meta=media(tmp_path,caption='图2 PS公司业务流程图')
    Image.new('RGB',(33,26),'white').save(tmp_path/'private/image.png')
    class Agent:
        def call(self,*a,**kw):return {'decision':'pass','reason':'流程图连接箭头','research':False,
            'nonresearch_kind':'decoration','text':'','description':'右向箭头','sensitive_boxes':[]}
    with pytest.raises(Rejected,match='研究图表不能按装饰图片忽略'):
        media_privacy(meta,tmp_path,Agent(),lambda *a:None)
    assert meta['resources'] and meta['blocks']


def test_large_captioned_decoration_remains_protected(tmp_path):
    meta=media(tmp_path,caption='图2 PS公司业务流程图')
    Image.new('RGB',(320,180),'white').save(tmp_path/'private/image.png')
    class Agent:
        def call(self,*a,**kw):return {'decision':'pass','reason':'疑似装饰图','research':False,
            'nonresearch_kind':'decoration','text':'','description':'大图','sensitive_boxes':[]}
    with pytest.raises(Rejected,match='研究图表不能按装饰图片忽略'):
        media_privacy(meta,tmp_path,Agent(),lambda *a:None)
    assert meta['resources']


def test_unreadable_formula_is_content_failure_not_privacy(tmp_path):
    meta=media(tmp_path)
    class Agent:
        def call(self,*a,**kw):return {'decision':'reject','reason':'公式关键下标无法读清','research':True,'failure_kind':'content'}
    with pytest.raises(Rejected) as error:media_privacy(meta,tmp_path,Agent(),lambda *a:None)
    assert error.value.kind=='evidence' and meta['resources']


def test_pixel_crop_and_mask_apply_to_exact_input_dimensions(tmp_path):
    (tmp_path/'assets').mkdir();buffer=io.BytesIO()
    Image.new('RGB',(200,100),'white').save(buffer,format='PNG')
    entry={'kind':'image','bbox':[20,10,180,90],'coordinate_space':'pixel','text':'研究图','description':'原图内容'}
    _,resource=_transcribed_block(entry,'crop',{'page':1},tmp_path,buffer.getvalue(),[[.1,.1,.2,.2]])
    with Image.open(tmp_path/resource['local_path']) as im:
        assert im.size==(160,80) and im.getpixel((0,0))==(0,0,0) and im.getpixel((50,50))==(255,255,255)


def test_adjacent_formula_regions_do_not_merge_into_unbounded_chapter():
    from evidence_tasks import _add_region
    groups=[]
    issues=[{'position':{'body_index':i}} for i in range(7)]
    for i,issue in enumerate(issues):_add_region(groups,{i,i+1},issue)
    assert all(len(g['pages'])<=4 for g in groups)
    assert sum(len(g['issues']) for g in groups)==len(issues)
    assert {id(i) for g in groups for i in g['issues']}=={id(i) for i in issues}
    with pytest.raises(Rejected):_add_region(groups,set(range(5)),{'position':{}})


def test_financial_table_spacing_is_not_a_contents_page():
    from evidence_tasks import _contents_page
    assert not _contents_page('年度  营业收入  利润\n2020  100  20\n2021  120  25\n2022  140  30\n2023  180  40')
    assert _contents_page('第一节 财务数据 .... 12\n第二节 风险 .... 15\n第三节 控制 .... 16\n第四节 结论 .... 18')


def test_wide_body_range_does_not_silently_drop_intermediate_pages():
    block={'id':'p','type':'paragraph','position':{'body_index':1},
           'text':'这是正文开始段落的唯一完整定位锚点 $x=y$ 这是正文结束段落的另一完整定位锚点'}
    pages=['这是正文开始段落的唯一完整定位锚点','中间第一页','中间第二页','中间第三页',
           '这是正文结束段落的另一完整定位锚点']
    with pytest.raises(Rejected,match='未省略中间页'):
        _locate({'code':'formula_review','position':block['position']},[block],pages)


def test_image_dimensions_sent_with_actual_image_without_extra_call():
    import json
    from agent_client import AgentClient
    agent=AgentClient({'base_url':'https://example.test/v1','vision_model':'fake','attempts':1},'fake-key')
    calls=[]
    class Opener:
        def open(self,request,timeout):
            calls.append(json.loads(request.data))
            return io.BytesIO(json.dumps({'choices':[{'message':{'content':json.dumps({'decision':'pass','reason':'mock'})}}]}).encode())
    agent.opener=Opener()
    blobs=[]
    for size in [(200,100),(100,300)]:
        buffer=io.BytesIO();Image.new('RGB',size,'white').save(buffer,format='PNG');blobs.append(buffer.getvalue())
    agent.call('ping',{},images=blobs)
    assert len(calls)==1
    payload=json.loads(calls[0]['messages'][1]['content'][0]['text'])
    assert payload['image_geometry']==[{'image_index':1,'width':200,'height':100},{'image_index':2,'width':100,'height':300}]
