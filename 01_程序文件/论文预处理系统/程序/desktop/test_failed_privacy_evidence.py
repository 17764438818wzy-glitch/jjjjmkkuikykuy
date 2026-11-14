"""A rejected visual review retains private pixels without approving output."""
import copy
import io
import json
from pathlib import Path

import pytest
from PIL import Image,ImageDraw

import auto_evidence
from auto_evidence import Rejected,media_privacy,pixel_box


INITIAL_BOX=[.05,.05,.2,.25]
REVIEW_BOX=[.4,.3,.6,.5]


def privacy_case(tmp_path,rid='visual_p20_1'):
    package=tmp_path/'package';(package/'_review').mkdir(parents=True);(package/'assets').mkdir()
    source=package/'_review'/'source.png'
    pixels=Image.new('RGB',(200,100),'white');draw=ImageDraw.Draw(pixels)
    draw.text((10,6),'Name field',fill='black');draw.line((10,80,190,70),fill='navy',width=3)
    pixels.save(source)
    resource={'id':rid,'kind':'image','position':{'page':20,'after_id':'block_0125'},
              'local_path':'_review/source.png','privacy_status':'pending'}
    meta={'resources':[resource],'blocks':[{'id':'image','type':'image','resource_id':rid,'caption':'Research chart'}]}
    class Agent:
        api_key='fake-secret-must-not-be-written'
        def __init__(self):self.calls=[];self.accepted=[]
        def call(self,task,payload,images=()):
            assert task=='privacy_image' and len(images)==1
            assert not payload.get('review_region')
            self.calls.append((copy.deepcopy(payload),images[0]))
            assert payload['phase']==('identify' if len(self.calls)==1 else 'verify_masked')
            return {'decision':'pass','research':True,'reason':'Located identity rectangle',
                    'sensitive_boxes':[INITIAL_BOX if len(self.calls)==1 else REVIEW_BOX],
                    'text':'Research chart','description':'Research line remains visible'}
        def accept_cached_result(self,result):self.accepted.append(result)
    return package,source,meta,Agent()


def assert_rejected_output_unchanged(package,source,before_bytes,meta,before_meta,agent):
    assert source.read_bytes()==before_bytes
    assert meta==before_meta
    assert not list((package/'assets').iterdir())
    assert not (package/'ingest.md').exists()
    assert not agent.accepted
    assert len(agent.calls)==3


@pytest.mark.parametrize('rid',['visual_p20_1','../AUTO:scope\\bad','CON'])
def test_repeated_full_review_boxes_save_last_submitted_pixels_privately(tmp_path,rid):
    package,source,meta,agent=privacy_case(tmp_path,rid)
    before_bytes=source.read_bytes();before_meta=copy.deepcopy(meta)
    with pytest.raises(Rejected,match='全图补遮盖后仍发现身份信息') as caught:
        media_privacy(meta,package,agent,lambda *args:None)
    assert_rejected_output_unchanged(package,source,before_bytes,meta,before_meta,agent)
    assert (package/'_review'/'privacy_failure').is_dir(), 'Rejected review must retain private diagnostic pixels'
    directories=list((package/'_review'/'privacy_failure').iterdir())
    assert len(directories)==1
    directory=directories[0]
    assert directory.resolve().is_relative_to((package/'_review'/'privacy_failure').resolve())
    assert all(char.isascii() and (char.isalnum() or char in '_-') for char in directory.name)
    saved=Image.open(directory/'masked-full.png').convert('RGB')
    final_input=Image.open(io.BytesIO(agent.calls[-1][1])).convert('RGB')
    assert saved.size==final_input.size and saved.tobytes()==final_input.tobytes()
    expected=Image.open(io.BytesIO(before_bytes)).convert('RGB')
    for box in (INITIAL_BOX,REVIEW_BOX):ImageDraw.Draw(expected).rectangle(pixel_box(box,expected.size),fill='black')
    assert saved.tobytes()==expected.tobytes()
    trace_text=(directory/'trace.json').read_text(encoding='utf-8');trace=json.loads(trace_text)
    assert trace['reason']==str(caught.value)
    assert trace['kind']==caught.value.kind=='privacy'
    assert trace['position']==caught.value.position=={'page':20,'after_id':'block_0125'}
    assert trace['image_scope']=='full_masked_image_at_failure'
    assert trace['may_differ_from_failing_tile_input'] is True
    assert agent.api_key not in trace_text


@pytest.mark.parametrize('failure',['mkdir','png','json'])
def test_diagnostic_filesystem_failure_preserves_the_original_rejection(tmp_path,monkeypatch,failure):
    package,source,meta,agent=privacy_case(tmp_path)
    before_bytes=source.read_bytes();before_meta=copy.deepcopy(meta);original=[];faults=[]
    verify=auto_evidence._verify_masked_observation
    def capture_original(*args,**kwargs):
        try:return verify(*args,**kwargs)
        except Rejected as exc:original.append(exc);raise
    monkeypatch.setattr(auto_evidence,'_verify_masked_observation',capture_original)
    mkdir=Path.mkdir;save=Image.Image.save;write_text=Path.write_text
    def fail_mkdir(path,*args,**kwargs):
        if 'privacy_failure' in path.parts:faults.append('mkdir');raise PermissionError('diagnostics unavailable')
        return mkdir(path,*args,**kwargs)
    def fail_png(image,fp,*args,**kwargs):
        if isinstance(fp,(str,Path)) and 'privacy_failure' in Path(fp).parts:faults.append('png');raise OSError('diagnostic disk full')
        return save(image,fp,*args,**kwargs)
    def fail_json(path,*args,**kwargs):
        if 'privacy_failure' in path.parts:faults.append('json');raise PermissionError('trace unavailable')
        return write_text(path,*args,**kwargs)
    if failure=='mkdir':monkeypatch.setattr(Path,'mkdir',fail_mkdir)
    elif failure=='png':monkeypatch.setattr(Image.Image,'save',fail_png)
    else:monkeypatch.setattr(Path,'write_text',fail_json)
    with pytest.raises(Rejected,match='全图补遮盖后仍发现身份信息') as caught:
        media_privacy(meta,package,agent,lambda *args:None)
    assert len(original)==1 and caught.value is original[0]
    assert faults==[failure]
    assert_rejected_output_unchanged(package,source,before_bytes,meta,before_meta,agent)
