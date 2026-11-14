import pytest
from evidence import attach_context

def paragraph(n, text):
    return {'type':'paragraph','id':f'p{n}','text':text,'position':{'body_index':n}}

@pytest.mark.parametrize('inline', [True,False])
def test_consecutive_figures_use_their_own_nearest_caption(inline):
    first={'type':'image','id':'im1','caption':'','position':{'body_index':1}}
    second={'type':'image','id':'im2','caption':'','position':{'body_index':4}}
    blocks=[paragraph(0,'本章比较指标'),paragraph(1,'[[IMAGE:im1]]') if inline else first,
            paragraph(2,'图3.1 产量'),paragraph(3,'对消费量的分析'),
            paragraph(4,'[[IMAGE:im2]]') if inline else second,paragraph(5,'图3.2 消费量')]
    attach_context(blocks,[first,second] if inline else [])
    assert first['caption']=='图3.1 产量'
    assert second['caption']=='图3.2 消费量'

def test_figure_accepts_nearest_caption_above():
    image={'type':'image','id':'im','caption':'','position':{'body_index':2}}
    attach_context([paragraph(0,'说明'),paragraph(1,'图2.1 结构'),image],[])
    assert image['caption']=='图2.1 结构'

def test_caption_is_not_taken_across_another_figure():
    first={'type':'image','id':'im1','caption':'','position':{'body_index':0}}
    second={'type':'image','id':'im2','caption':'','position':{'body_index':1}}
    attach_context([first,second,paragraph(2,'图1.2 第二张图')],[])
    assert first['caption']==''
    assert second['caption']=='图1.2 第二张图'

@pytest.mark.parametrize('inline', [True,False])
def test_consecutive_captions_above_keep_their_original_ownership(inline):
    first={'type':'image','id':'im1','caption':'','position':{'body_index':1}}
    second={'type':'image','id':'im2','caption':'','position':{'body_index':3}}
    blocks=[paragraph(0,'Figure 1 First'),paragraph(1,'[[IMAGE:im1]]') if inline else first,
            paragraph(2,'Figure 2 Second'),paragraph(3,'[[IMAGE:im2]]') if inline else second]
    attach_context(blocks,[first,second] if inline else [])
    assert first['caption']=='Figure 1 First'
    assert second['caption']=='Figure 2 Second'
