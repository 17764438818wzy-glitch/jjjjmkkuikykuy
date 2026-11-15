"""Image compatibility uses real pixels and preserves the source/request evidence."""
import base64
import copy
import hashlib
import io
import json
import random
from pathlib import Path

import pytest
from PIL import Image, ImageDraw

from preextract_images import prepare_request_images, restore_request_images
from preextract_request import build_request


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def material(tmp_path, size=(2362, 743), mode='RGB'):
    path = tmp_path / 'source.png'
    image = Image.new(mode, size, 'white' if mode == 'RGB' else (255, 255, 255, 0))
    draw = ImageDraw.Draw(image)
    draw.rectangle((0, 0, 99, 99), fill='red')
    draw.rectangle((size[0]-100, size[1]-100, size[0]-1, size[1]-1), fill='blue')
    image.save(path)
    return dict(source={'source_sha256':'a'*64}, blocks=[{'id':'B1','text':'完整学习内容'}],
                sections=[], assets=[dict(id='A1', image_path=str(path), local_path=str(path),
                sha256=sha(path), width=size[0], height=size[1])], issues=[])


def request(bundle, **extra):
    cfg = dict(model='test', context_window=300000, max_tokens=1000,
               max_image_side=2000, max_single_image_bytes=5*1024*1024,
               max_total_image_bytes=16*1024*1024)
    cfg.update(extra)
    return build_request(bundle, '完整模板', {'majors':[]}, cfg)[0]


def test_fit_whole_image_and_request_binding_without_changing_original(tmp_path):
    bundle = material(tmp_path)
    original = copy.deepcopy(bundle)
    adapted, report = prepare_request_images(bundle, tmp_path / 'work', {})
    asset = adapted['assets'][0]
    assert bundle == original and sha(bundle['assets'][0]['image_path']) == bundle['assets'][0]['sha256']
    assert adapted['blocks'] == bundle['blocks']
    assert report['image_count'] == 1 and report['adjusted_count'] == 1
    assert asset['id'] == 'A1' and (asset['width'], asset['height']) == (2000, 629)
    with Image.open(asset['image_path']) as image:
        assert image.getpixel((10, 10)) == (255, 0, 0)
        assert image.getpixel((1990, 619)) == (0, 0, 255)
    body = request(adapted)
    chunks = body['messages'][1]['content']
    data = base64.b64decode(next(x['image_url']['url'].split(',', 1)[1] for x in chunks if x['type']=='image_url'))
    assert hashlib.sha256(data).hexdigest() == asset['sha256']
    assert data == Path(asset['image_path']).read_bytes()
    assert json.loads(chunks[0]['text'])['material']['assets'][0]['width'] == 2000
    assert restore_request_images(bundle, tmp_path / 'work', body) == adapted


def test_small_image_reuses_exact_bytes_and_does_not_upscale(tmp_path):
    bundle = material(tmp_path, (300, 160))
    adapted, report = prepare_request_images(bundle, tmp_path / 'work', {})
    assert adapted == bundle and report['adjusted_count'] == 0
    assert restore_request_images(bundle, tmp_path / 'work', request(bundle)) == bundle


def test_byte_limit_uses_compression_and_keeps_transparency_readable(tmp_path):
    bundle = material(tmp_path, (1800, 1300))
    path = Path(bundle['assets'][0]['image_path'])
    pixels = random.Random(3).randbytes(1800*1300*3)
    Image.frombytes('RGB', (1800, 1300), pixels).save(path)
    bundle['assets'][0]['sha256'] = sha(path)
    assert path.stat().st_size > 5*1024*1024
    adapted, _ = prepare_request_images(bundle, tmp_path / 'work', {})
    asset = adapted['assets'][0]
    assert Path(asset['image_path']).stat().st_size <= 5*1024*1024
    assert (asset['width'], asset['height']) == (1800, 1300)
    assert sha(path) == bundle['assets'][0]['sha256']


def test_alpha_survives_request_resize(tmp_path):
    bundle = material(tmp_path, mode='RGBA')
    adapted, _ = prepare_request_images(bundle, tmp_path / 'work', {})
    with Image.open(adapted['assets'][0]['image_path']) as image:
        assert image.mode == 'RGBA'
        assert image.getpixel((1000, 300))[3] == 0
        assert image.getpixel((10, 10)) == (255, 0, 0, 255)


def test_reuse_verifies_saved_bytes_and_never_readapts_model_input(tmp_path):
    bundle = material(tmp_path)
    work = tmp_path / 'work'
    adapted, _ = prepare_request_images(bundle, work, {})
    body = request(adapted)
    (work / 'model').mkdir()
    (work / 'model/request-state.json').write_text('{}')
    with pytest.raises(ValueError, match='已有模型请求'):
        prepare_request_images(bundle, work, {'max_image_side':1000})
    assert restore_request_images(bundle, work, body) == adapted
    image = next(x for x in body['messages'][1]['content'] if x['type']=='image_url')
    image['image_url']['url'] = 'data:image/png;base64,' + base64.b64encode(Path(bundle['assets'][0]['image_path']).read_bytes()).decode()
    with pytest.raises(ValueError, match='图片.*(请求|版本)'):
        restore_request_images(bundle, work, body)
    Path(adapted['assets'][0]['image_path']).write_bytes(b'changed')
    with pytest.raises(ValueError, match='图片.*(请求|版本)'):
        restore_request_images(bundle, work, request(bundle, max_image_side=4096))


def test_legacy_saved_images_keep_the_original_large_version(tmp_path):
    bundle = material(tmp_path)
    body = request(bundle, max_image_side=4096)
    assert restore_request_images(bundle, tmp_path, body) == bundle


def test_publication_resizes_after_mask_and_manifest_uses_actual_pixels(tmp_path):
    from test_preextract_publish import fixture, prepare
    candidate, bundle, _ = fixture()
    source = material(tmp_path, (2400, 1200))['assets'][0]
    bundle['assets'] = [source]
    candidate['assets']['items'] = [dict(id='A1', source_image_id='A1', kind='figure', storage='image',
        meaning='完整结构', purpose='学习连接方式', section_ids=['S1'], unit_ids=['U1'])]
    candidate['redaction_plan'].append(dict(id='R2', scope='image', action='mask', entity_key='signature',
        location_status='located', image_ref=dict(asset_id='A1', rendition_sha256=source['sha256'], width=2400, height=1200),
        redactions=[dict(x0=600, y0=300, x1=1200, y1=600)]))
    result = prepare(tmp_path, candidate, bundle)
    asset = result['packet']['assets'][0]
    path = Path(result['package_path']) / asset['file']
    with Image.open(path) as image:
        assert image.size == (2000, 1000)
        assert image.getpixel((600, 300)) == (0, 0, 0)
        assert image.getpixel((400, 300)) == (255, 255, 255)
        assert (asset['width'], asset['height']) == image.size
    assert sha(path) == asset['sha256']
    assert path.stat().st_size <= 5*1024*1024
    assert sha(source['image_path']) == source['sha256']


def test_publication_does_not_turn_transparent_background_black(tmp_path):
    from preextract_publish import _mask_images
    bundle = material(tmp_path, (300, 160), mode='RGBA')
    candidate = {'assets':{'items':[dict(id='A1', storage='image')]}}
    published = _mask_images(candidate, bundle, [], tmp_path / 'public')
    with Image.open(tmp_path / 'public' / published[0]['file']) as image:
        assert image.convert('RGB').getpixel((150, 120)) == (255, 255, 255)

