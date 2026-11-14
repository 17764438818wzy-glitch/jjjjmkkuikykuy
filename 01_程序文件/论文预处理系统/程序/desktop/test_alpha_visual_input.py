import io

import pytest
from PIL import Image, ImageDraw

from evidence_render import load_visual_image


@pytest.mark.parametrize('mode', ['RGBA', 'LA', 'P', 'RGB'])
def test_transparent_black_background_is_rendered_as_white_document_page(tmp_path, mode):
    source = tmp_path / 'table.png'
    if mode == 'RGBA':
        image = Image.new(mode, (4, 1), (0, 0, 0, 0))
        image.putdata([(0, 0, 0, 0), (0, 0, 0, 255), (0, 0, 0, 128), (255, 0, 0, 255)])
        expected = [(255, 255, 255), (0, 0, 0), (127, 127, 127), (255, 0, 0)]
        image.save(source)
    elif mode == 'LA':
        image = Image.new(mode, (3, 1))
        image.putdata([(0, 0), (0, 255), (0, 128)])
        expected = [(255, 255, 255), (0, 0, 0), (127, 127, 127)]
        image.save(source)
    elif mode == 'P':
        image = Image.new(mode, (4, 1))
        image.putpalette([0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0] + [0] * 756)
        image.putdata([0, 1, 2, 3])
        expected = [(255, 255, 255), (0, 0, 0), (127, 127, 127), (255, 0, 0)]
        image.save(source, transparency=bytes([0, 255, 128, 255]))
    else:
        image = Image.new(mode, (3, 1))
        image.putdata([(0, 0, 0), (10, 20, 30), (255, 0, 0)])
        expected = [(255, 255, 255), (10, 20, 30), (255, 0, 0)]
        image.save(source, transparency=(0, 0, 0))
    original = source.read_bytes()

    result = load_visual_image(source)

    assert result.mode == 'RGB'
    assert list(result.getdata()) == expected
    assert result.size == image.size
    assert source.read_bytes() == original


def test_opaque_black_research_background_is_not_whitened(tmp_path):
    source = tmp_path / 'opaque.png'
    image = Image.new('RGBA', (3, 1), (0, 0, 0, 255))
    image.putpixel((1, 0), (255, 255, 255, 255))
    image.putpixel((2, 0), (12, 43, 78, 255))
    image.save(source)

    result = load_visual_image(source)

    assert list(result.getdata()) == [(0, 0, 0), (255, 255, 255), (12, 43, 78)]


def test_privacy_receives_visible_alpha_ink_and_masks_same_pixels(tmp_path):
    from auto_evidence import media_privacy
    from test_visual_knowledge import visual_meta, answer

    meta = visual_meta(tmp_path)
    source = tmp_path / '_review/figure.png'
    figure = Image.new('RGBA', (100, 80), (0, 0, 0, 0))
    draw = ImageDraw.Draw(figure)
    draw.line([(5, 40), (95, 40)], fill=(0, 0, 0, 255), width=2)
    draw.rectangle((70, 50, 90, 70), fill=(255, 0, 0, 255))
    figure.save(source)
    original = source.read_bytes()
    requests = []

    class Agent:
        def call(self, task, payload, images=()):
            with Image.open(io.BytesIO(images[0])) as image:
                requests.append(image.copy())
            result = answer()
            result['sensitive_boxes'] = [] if payload.get('phase') == 'verify_masked' else [[.1, .1, .2, .2]]
            return result

    media_privacy(meta, tmp_path, Agent(), lambda *args: None)

    assert len(requests) == 2
    assert requests[0].getpixel((0, 0)) == (255, 255, 255)
    assert requests[0].getpixel((50, 40)) == (0, 0, 0)
    assert requests[0].getpixel((80, 60)) == (255, 0, 0)
    assert requests[0].getpixel((15, 12)) == (255, 255, 255)
    assert requests[1].getpixel((15, 12)) == (0, 0, 0)
    with Image.open(tmp_path / meta['resources'][0]['local_path']) as output:
        assert output.size == (100, 80)
        assert output.getpixel((15, 12)) == (0, 0, 0)
        assert output.getpixel((50, 40)) == (0, 0, 0)
        assert output.getpixel((80, 60)) == (255, 0, 0)
    assert source.read_bytes() == original
