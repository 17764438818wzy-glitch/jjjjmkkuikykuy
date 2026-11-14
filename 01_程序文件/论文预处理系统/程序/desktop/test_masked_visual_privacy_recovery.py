import io
import json

import pytest
from PIL import Image, ImageDraw

from auto_evidence import Rejected, media_privacy, text_privacy
from test_visual_knowledge import visual_meta, answer


def test_page_crop_with_masked_ocr_but_no_pixel_masks_is_rechecked(tmp_path):
    meta = visual_meta(tmp_path)
    resource = meta['resources'][0]
    resource.update(privacy_status='approved', visual_processed=True,
                    privacy_method='single_page_recognition_and_local_mask',
                    ocr={'text': '电话 130****0000'},
                    crop_provenance={'page_mask_boxes_normalized': []})
    phases = []

    class Agent:
        def call(self, task, payload, images=()):
            phases.append(payload.get('phase'))
            result = answer()
            if payload.get('phase') == 'locate_privacy_regions':
                result['sensitive_boxes'] = [[.1, .05, .3, .2]]
            return result

    media_privacy(meta, tmp_path, Agent(), lambda *args: None)
    assert phases == ['locate_privacy_regions', 'verify_masked']
    assert Image.open(tmp_path / resource['local_path']).getpixel((15, 10)) == (0, 0, 0)
    assert resource['ocr']['text'] == '年度收入趋势'


def test_verified_page_crop_does_not_repeat_privacy_calls(tmp_path):
    meta = visual_meta(tmp_path)
    meta['resources'][0].update(privacy_status='approved', visual_processed=True,
        privacy_method='single_page_recognition_and_local_mask', masked_observation_verified=True)

    class Agent:
        def call(self, *args, **kwargs):
            raise AssertionError('Verified pixels should reuse their checkpoint')

    media_privacy(meta, tmp_path, Agent(), lambda *args: None)


@pytest.mark.parametrize('extra_identity', [False, True])
def test_unmasked_page_chart_has_independent_review_without_four_photo_tiles(tmp_path, extra_identity):
    meta = visual_meta(tmp_path)
    im = Image.new('RGB', (600, 400), 'white')
    ImageDraw.Draw(im).line((20, 300, 400, 50), fill='blue', width=2)
    im.save(tmp_path / '_review/figure.png')
    meta['resources'][0].update(privacy_status='approved', visual_processed=True,
        privacy_method='single_page_recognition_and_local_mask')
    phases = []

    class Agent:
        def call(self, task, payload, images=()):
            phases.append(payload.get('phase'))
            assert not payload.get('review_region')
            result = answer()
            if extra_identity and payload.get('phase') == 'verify_masked':
                result['sensitive_boxes'] = [[.1, .1, .2, .2]]
            return result

    if extra_identity:
        with pytest.raises(Rejected):
            media_privacy(meta, tmp_path, Agent(), lambda *args: None)
        assert not (tmp_path / 'assets/r.png').exists()
    else:
        media_privacy(meta, tmp_path, Agent(), lambda *args: None)
    assert phases == ['locate_privacy_regions', 'verify_masked']


def test_masked_image_observation_is_reread_from_masked_pixels(tmp_path):
    meta = visual_meta(tmp_path)
    calls = []

    class Agent:
        def call(self, task, payload, images=()):
            calls.append((task, payload.get('phase')))
            if task == 'privacy_text':
                return {'decision': 'pass', 'reason': '无身份信息', 'sensitive_strings': []}
            result = answer()
            if payload.get('phase') == 'verify_masked':
                assert Image.open(io.BytesIO(images[0])).getpixel((15, 10)) == (0, 0, 0)
                result['text'] = '年度收入趋势'
            else:
                result['text'] = 'Author: Private Student'
                result['sensitive_boxes'] = [[.1, .05, .3, .2]]
            return result

    agent = Agent()
    media_privacy(meta, tmp_path, agent, lambda *args: None)
    text_privacy(meta, agent, lambda *args: None)
    assert ('privacy_image', 'verify_masked') in calls
    assert 'Private Student' not in json.dumps(meta)
    assert meta['resources'][0]['masked_observation_verified'] is True
    assert meta['text_privacy_complete']


def test_photo_privacy_rejection_gets_bounded_region_location_then_verification(tmp_path):
    meta = visual_meta(tmp_path)
    phases = []

    class Agent:
        def call(self, task, payload, images=()):
            phase = payload.get('phase'); phases.append(phase)
            if phase == 'identify':
                return {'decision': 'reject', 'reason': '多人面部，需定位遮盖', 'failure_kind': 'privacy'}
            result = answer()
            if phase == 'locate_privacy_regions':
                result['sensitive_boxes'] = [[.1, .05, .3, .2]]
            return result

    media_privacy(meta, tmp_path, Agent(), lambda *args: None)
    assert phases == ['identify', 'locate_privacy_regions', 'verify_masked']
    assert meta['resources'][0]['privacy_status'] == 'approved'


def test_unresolved_photo_privacy_is_never_published(tmp_path):
    meta = visual_meta(tmp_path)
    phases = []

    class Agent:
        def call(self, task, payload, images=()):
            phases.append(payload.get('phase'))
            return {'decision': 'reject', 'reason': '仍有无法定位的真实身份', 'failure_kind': 'privacy'}

    with pytest.raises(Rejected):
        media_privacy(meta, tmp_path, Agent(), lambda *args: None)
    assert phases == ['identify', 'locate_privacy_regions']
    assert not (tmp_path / 'assets/r.png').exists()


@pytest.mark.parametrize('size', [(100, 80), (600, 400)])
def test_mask_recheck_content_failure_is_distinct_from_privacy_failure(tmp_path, size):
    meta = visual_meta(tmp_path)
    im = Image.new('RGB', size, 'white')
    ImageDraw.Draw(im).line((10, 60, 70, 10), fill='blue', width=2)
    im.save(tmp_path / '_review/figure.png')

    class Agent:
        def call(self, task, payload, images=()):
            if payload.get('phase') == 'verify_masked':
                return {'decision': 'reject', 'reason': '研究坐标刻度不可辨认', 'failure_kind': 'content'}
            result = answer()
            result['sensitive_boxes'] = [[.01, .01, .02, .02]]
            return result

    with pytest.raises(Rejected) as caught:
        media_privacy(meta, tmp_path, Agent(), lambda *args: None)
    assert caught.value.kind == 'evidence'
    assert not (tmp_path / 'assets/r.png').exists()


def test_full_image_verification_can_repair_one_confirmed_remaining_region(tmp_path):
    meta = visual_meta(tmp_path)
    verified = 0
    located = 0

    class Agent:
        def call(self, task, payload, images=()):
            nonlocal verified, located
            result = answer()
            if payload.get('phase') == 'identify':
                result['sensitive_boxes'] = [[.1, .05, .3, .2]]
            elif payload.get('phase') == 'verify_masked':
                verified += 1
                if verified == 1:
                    result.update(decision='reject', reason='还有可定位的电话',
                                  sensitive_boxes=[[.7, .7, .9, .8]])
                else:
                    assert Image.open(io.BytesIO(images[0])).getpixel((80, 60)) == (0, 0, 0)
            elif payload.get('phase') == 'locate_privacy_regions':
                located += 1
                result['sensitive_boxes'] = [[.7, .7, .9, .8]]
            return result

    media_privacy(meta, tmp_path, Agent(), lambda *args: None)
    assert (verified, located) == (2, 1)
    assert meta['resources'][0]['masked_observation_verified']


def test_mask_verification_cannot_discard_research_or_accept_remaining_identity(tmp_path):
    meta = visual_meta(tmp_path)

    class Agent:
        def call(self, task, payload, images=()):
            result = answer()
            result['sensitive_boxes'] = [[.1, .05, .3, .2]]
            if payload.get('phase') == 'verify_masked':
                result['decision'] = 'reject'; result['reason'] = '尚有遗漏姓名'
            return result

    with pytest.raises(Rejected):
        media_privacy(meta, tmp_path, Agent(), lambda *args: None)
    assert not (tmp_path / 'assets/r.png').exists()


def test_local_mask_recheck_catches_small_identity_missed_by_whole_image(tmp_path):
    meta=visual_meta(tmp_path)
    original=Image.new('RGB',(600,400),'white')
    ImageDraw.Draw(original).rectangle((311,72,329,99),fill='red')
    ImageDraw.Draw(original).line((50,300,500,300),fill='blue',width=3)
    original.save(tmp_path/'_review/figure.png')
    regions=[];context_reviews=[]
    class Agent:
        def call(self,task,payload,images=()):
            result=answer()
            if payload.get('phase')=='identify':
                result['sensitive_boxes']=[[.05,.05,.1,.1]]
            elif payload.get('review_region'):
                regions.append(payload['review_region'])
                if payload.get('context_image_indices'):
                    context_reviews.append(payload['context_image_indices'])
                    assert payload['context_image_indices'] == [2]
                    assert len(images) == 2
                    overview = Image.open(io.BytesIO(images[1])).convert('RGB')
                    assert overview.size == (600, 400)
                    assert overview.getpixel((40, 30)) == (0, 0, 0)
                    assert overview.getpixel((400, 300)) == (0, 0, 255)
                else:
                    assert len(images) == 1
                im=Image.open(io.BytesIO(images[0])).convert('RGB')
                points=[(x,y) for y in range(im.height) for x in range(im.width) if im.getpixel((x,y))==(255,0,0)]
                if points:
                    xs,ys=zip(*points)
                    result['sensitive_boxes']=[[min(xs)/im.width,min(ys)/im.height,
                                               (max(xs)+1)/im.width,(max(ys)+1)/im.height]]
                    if payload.get('phase')=='verify_masked':
                        result['decision']='reject';result['reason']='实际身份已定位，需先遮盖'
            return result
    media_privacy(meta,tmp_path,Agent(),lambda *args:None)
    output=Image.open(tmp_path/meta['resources'][0]['local_path'])
    assert output.getpixel((320,85))==(0,0,0)
    assert output.getpixel((400,300))==(0,0,255)
    assert len(regions)>=4
    assert context_reviews
    assert meta['resources'][0]['visual_integrity']['provenance']['mask_boxes_pixels'][-1][0]>=280
