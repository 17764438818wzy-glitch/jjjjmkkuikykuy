import io
from pathlib import Path
from types import SimpleNamespace

import fitz
import pytest
from PIL import Image


def test_raster_input_keeps_pixels_and_never_starts_office(tmp_path, monkeypatch):
    import evidence_render
    source=tmp_path/'figure.png';Image.new('RGB',(40,30),(25,80,140)).save(source)
    def forbidden(*args,**kwargs):raise AssertionError('PNG entered Office renderer')
    monkeypatch.setattr(evidence_render.subprocess,'run',forbidden)
    assert hasattr(evidence_render,'load_visual_image')
    result=evidence_render.load_visual_image(source)
    assert result.size==(40,30) and result.getpixel((2,2))==(25,80,140)


@pytest.mark.parametrize('extension',['wmf','emf'])
def test_vector_uses_isolated_render_and_retains_all_nonwhite_marks(tmp_path,monkeypatch,extension):
    import evidence_render
    import office_wps
    monkeypatch.setattr(office_wps,'available',lambda:False)
    source=tmp_path/('vector.'+extension);source.write_bytes(b'vector-fixture')
    commands=[]
    def run(command,**kwargs):
        commands.append(command)
        assert kwargs['timeout']<=60
        out=Path(command[command.index('--outdir')+1])
        pdf=fitz.open();page=pdf.new_page(width=200,height=300)
        page.draw_rect(fitz.Rect(30,50,60,70),color=None,fill=(1,0,0))
        page.draw_rect(fitz.Rect(150,210,160,220),color=None,fill=(0,0,1))
        pdf.save(out/'source.pdf');pdf.close()
        return SimpleNamespace(returncode=0,stdout=b'',stderr=b'')
    monkeypatch.setattr(evidence_render.subprocess,'run',run)
    monkeypatch.setattr(evidence_render,'VECTOR_RENDERER',__file__,raising=False)
    assert hasattr(evidence_render,'load_visual_image')
    image=evidence_render.load_visual_image(source)
    assert len(commands)==1 and '--headless' in commands[0]
    assert any(s.startswith('-env:UserInstallation=') for s in commands[0])
    colors=image.getcolors(image.width*image.height)
    assert any(rgb==(255,0,0) for _,rgb in colors) and any(rgb==(0,0,255) for _,rgb in colors)
    assert image.width<600 and image.height<900
    assert source.read_bytes()==b'vector-fixture'


def test_vector_renderer_failure_never_falls_back_to_distorted_pillow(tmp_path,monkeypatch):
    import evidence_render
    import office_wps
    monkeypatch.setattr(office_wps,'available',lambda:False)
    from agent_client import AgentError
    source=tmp_path/'formula.wmf';source.write_bytes(b'vector')
    monkeypatch.setattr(evidence_render,'VECTOR_RENDERER',__file__,raising=False)
    monkeypatch.setattr(evidence_render.subprocess,'run',lambda *a,**k:SimpleNamespace(returncode=1,stdout=b'',stderr=b''))
    assert hasattr(evidence_render,'load_visual_image')
    with pytest.raises(AgentError,match='矢量图'):
        evidence_render.load_visual_image(source)


def test_vector_privacy_masks_apply_to_same_rendered_pixels(tmp_path,monkeypatch):
    from test_visual_knowledge import visual_meta,answer
    from auto_evidence import media_privacy
    import evidence_render
    meta=visual_meta(tmp_path)
    source=tmp_path/'_review/formula.wmf';source.write_bytes(b'vector')
    meta['resources'][0]['local_path']='_review/formula.wmf'
    seen=[]
    def render(path):
        assert Path(path)==source.resolve()
        return Image.new('RGB',(300,120),'white')
    monkeypatch.setattr(evidence_render,'load_visual_image',render,raising=False)
    class Agent:
        def call(self,task,payload,images=()):
            with Image.open(io.BytesIO(images[0])) as image:seen.append(image.size)
            result=answer();result['sensitive_boxes']=[] if payload.get('phase')=='verify_masked' else [[.1,.1,.2,.2]];return result
    media_privacy(meta,tmp_path,Agent(),lambda *args:None)
    assert seen==[(300,120),(300,120)]
    with Image.open(tmp_path/meta['resources'][0]['local_path']) as result:
        assert result.size==(300,120)
        assert result.getpixel((45,18))==(0,0,0)
        assert result.getpixel((150,60))==(255,255,255)
