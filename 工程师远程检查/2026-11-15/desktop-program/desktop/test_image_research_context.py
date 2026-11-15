"""Research purpose comes from the image's actual source neighbours."""
import copy
import io

import pytest
from PIL import Image

from auto_evidence import media_privacy
from test_visual_knowledge import answer, visual_meta


def image_document(tmp_path, *, scoped=False, caption='图21 奥运五环图'):
    meta=visual_meta(tmp_path)
    image=meta['blocks'][0]
    source_position={'part':'word/document.xml','body_index':226}
    def paragraph(id,text,body_index):
        return {'id':id,'type':'paragraph','text':text,
                'position':{'part':'word/document.xml','body_index':body_index}}
    anchor=paragraph('source_image','[[IMAGE:r]]',226)
    if scoped:
        anchor.update(text='[解析说明：此位置的原文图形需定向识别。]',graphic_object=True)
        position={'page':12,'after_id':anchor['id']}
    else:
        position=source_position
    image.update(position=position,caption=caption)
    meta['resources'][0].update(position=copy.deepcopy(position),
        privacy_method='single_page_recognition_and_local_mask')
    meta['blocks']=[
        paragraph('before_1','4.扩展提升',224),
        paragraph('before_2','思考如何实现绘制奥运五环图形，用流程图把设计过程表达出来。',225),
        anchor,
        paragraph('after_1','图21 奥运五环图',227),
        paragraph('after_2','5.课堂小结',228),
        paragraph('unrelated','参考文献后的无关文字，不能当作图片旁的教学解释。',900),
        image,
    ]
    return meta


def check_context(payload,meta):
    context=payload.get('source_context',{})
    assert [entry['id'] for entry in context.get('before',[])]==['before_1','before_2']
    assert [entry['id'] for entry in context.get('after',[])]==['after_1','after_2']
    by_id={block['id']:block for block in meta['blocks']}
    for entry in context['before']+context['after']:
        assert entry['text']==by_id[entry['id']]['text']
        assert entry['position']==by_id[entry['id']]['position']
    assert payload['caption']==meta['blocks'][-1]['caption']
    assert payload['position']==meta['resources'][0]['position']


@pytest.mark.parametrize('scoped',[False,True])
@pytest.mark.parametrize('already_processed',[False,True])
def test_all_privacy_phases_receive_real_image_neighbours_without_changing_pixels(tmp_path,scoped,already_processed):
    meta=image_document(tmp_path,scoped=scoped)
    meta['resources'][0]['visual_processed']=already_processed
    original=(tmp_path/'_review/figure.png').read_bytes()
    original_pixels=Image.open(io.BytesIO(original)).convert('RGB').tobytes()
    original_paragraphs=copy.deepcopy(meta['blocks'][:-1])
    phases=[]
    class Agent:
        def call(self,task,payload,images=()):
            assert task=='privacy_image'
            phases.append(payload['phase'])
            check_context(payload,meta)
            assert len(images)==1
            assert Image.open(io.BytesIO(images[0])).convert('RGB').tobytes()==original_pixels
            if payload['phase']=='identify':
                return {'decision':'reject','reason':'需要重新定位可见身份字段','failure_kind':'privacy'}
            return answer()
    media_privacy(meta,tmp_path,Agent(),lambda *args:None)
    assert phases==(['locate_privacy_regions','verify_masked'] if already_processed else
                    ['identify','locate_privacy_regions','verify_masked'])
    assert meta['blocks'][:-1]==original_paragraphs
    assert (tmp_path/'_review/figure.png').read_bytes()==original


def test_research_neighbours_do_not_force_an_unrelated_decoration_to_pass(tmp_path):
    meta=image_document(tmp_path,caption='')
    calls=[]
    class Agent:
        def call(self,task,payload,images=()):
            calls.append(payload['phase'])
            check_context(payload,meta)
            return {'decision':'pass','reason':'实际像素为不相关的纯装饰图',
                    'research':False,'nonresearch_kind':'decoration',
                    'sensitive_boxes':[],'text':'','description':''}
    media_privacy(meta,tmp_path,Agent(),lambda *args:None)
    assert calls==['identify']
    assert meta['resources']==[]
    assert not any(block.get('resource_id')=='r' for block in meta['blocks'])


def test_missing_source_anchor_never_borrows_neighbours_of_appended_image(tmp_path):
    meta=image_document(tmp_path)
    meta['blocks']=[block for block in meta['blocks'] if block['id']!='source_image']
    calls=[]
    class Agent:
        def call(self,task,payload,images=()):
            calls.append(payload['phase'])
            context=payload.get('source_context',{})
            assert not context.get('before') and not context.get('after')
            assert '参考文献后的无关文字' not in str(payload)
            return answer()
    media_privacy(meta,tmp_path,Agent(),lambda *args:None)
    assert calls==['identify','verify_masked']
