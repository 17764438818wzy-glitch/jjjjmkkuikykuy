import json
import pytest


def test_request_contains_actual_image_and_one_copy_of_body(tmp_path):
    from PIL import Image
    from preextract_request import build_request
    from preextract_material import file_sha
    p=tmp_path/'image.png';Image.new('RGB',(20,20)).save(p)
    bundle={'source':{},'blocks':[{'id':'b1','text':'独有正文标记'}],'sections':[],
            'assets':[{'id':'a1','kind':'image','image_path':str(p),'sha256':file_sha(p),'width':20,'height':20}],
            'issues':[],'attachments':[]}
    body,report=build_request(bundle,'完整MD字段',{'majors':[]},{'model':'test','context_window':100000,'max_tokens':1000})
    encoded=json.dumps(body,ensure_ascii=False)
    assert encoded.count('独有正文标记')==1
    assert 'data:image/png;base64,' in encoded and report['image_count']==1


def test_capacity_does_not_truncate_or_send(tmp_path):
    from preextract_request import build_request
    with pytest.raises(ValueError,match='容量'):
        build_request({'source':{},'blocks':[{'id':'b','text':'正文'*1000}],'sections':[],'assets':[]},'模板',{},
                      {'model':'test','context_window':100,'max_tokens':90})


def test_http_error_is_one_request_and_persistently_blocks_retry(tmp_path):
    from preextract_request import request_once
    import urllib.error
    class Failing:
        calls=0
        def open(self,*a,**kw):
            self.calls+=1
            raise urllib.error.URLError('offline')
    opener=Failing();body={'model':'test','messages':[]};started=[]
    with pytest.raises(urllib.error.URLError):request_once(body,{'base_url':'http://127.0.0.1:9/v1','api_key':'secret'},tmp_path,opener=opener,on_request_started=lambda:started.append(True))
    with pytest.raises(RuntimeError,match='已存在'):request_once(body,{'base_url':'http://127.0.0.1:9/v1','api_key':'secret'},tmp_path,opener=opener,on_request_started=lambda:started.append(True))
    assert opener.calls==1
    assert started==[True]
    assert 'secret' not in (tmp_path/'request-state.json').read_text()


def _image_bundle(tmp_path, count=2):
    from PIL import Image
    from preextract_material import file_sha
    assets=[]
    for i in range(count):
        path=tmp_path/f'image-{i}.png'
        Image.new('RGB',(20,20),(i*10,10,20)).save(path)
        assets.append(dict(id=f'a{i}',kind='image',image_path=str(path),sha256=file_sha(path),width=20,height=20))
    return dict(source={},blocks=[dict(id='b',text='完整正文')],sections=[],assets=assets,issues=[],attachments=[])


def test_bound_cli_transport_flags_preserve_text_and_every_image_byte(tmp_path):
    import base64
    from pathlib import Path
    from preextract_request import build_request
    bundle = _image_bundle(tmp_path)
    normal, _ = build_request(bundle, '全部MD字段', {}, dict(model='ordinary'))
    cli, report = build_request(bundle, '全部MD字段', {}, dict(model='cli-vision',
        supports_json_object=False, image_detail='auto'))
    assert 'response_format' not in cli
    assert normal['response_format'] == {'type':'json_object'}
    assert cli['messages'][0] == normal['messages'][0]
    assert cli['messages'][1]['content'][0] == normal['messages'][1]['content'][0]
    images = [part['image_url'] for part in cli['messages'][1]['content'] if part['type'] == 'image_url']
    assert len(images) == report['image_count'] == len(bundle['assets'])
    for actual, asset in zip(images, bundle['assets']):
        assert actual['detail'] == 'auto'
        assert base64.b64decode(actual['url'].split(',', 1)[1]) == Path(asset['image_path']).read_bytes()
    assert all(part['image_url']['detail'] == 'high' for part in normal['messages'][1]['content'] if part['type'] == 'image_url')


def test_image_capacity_counts_aggregate_base64_payload_exactly(tmp_path):
    import base64
    from pathlib import Path
    from preextract_request import build_request
    bundle=_image_bundle(tmp_path)
    raw=sum(len(Path(a['image_path']).read_bytes()) for a in bundle['assets'])
    encoded=sum(len(base64.b64encode(Path(a['image_path']).read_bytes())) for a in bundle['assets'])
    limit=encoded-1
    assert raw<limit and all(len(base64.b64encode(Path(a['image_path']).read_bytes()))<limit for a in bundle['assets'])
    with pytest.raises(ValueError,match=rf'2.*Base64.*{encoded}.*{limit}'):
        build_request(bundle,'模板',{},dict(model='test',max_total_image_bytes=limit))
    body,report=build_request(bundle,'模板',{},dict(model='test',max_total_image_bytes=encoded))
    assert report['image_bytes']==raw
    assert report['image_base64_bytes']==encoded
    assert report['image_budget_bytes']==encoded
    assert report['image_budget_basis']=='aggregate_base64_payload'
    assert len([c for c in body['messages'][1]['content'] if c['type']=='image_url'])==2


def test_default_two_mib_limit_is_applied_to_base64_not_raw(tmp_path):
    import random
    from PIL import Image
    from preextract_material import file_sha
    from preextract_request import build_request
    path=tmp_path/'noise.png'
    Image.frombytes('RGB',(750,750),random.Random(42).randbytes(750*750*3)).save(path)
    assert path.stat().st_size<2*1024*1024
    bundle=dict(source={},blocks=[],sections=[],assets=[dict(id='a',image_path=str(path),sha256=file_sha(path),width=750,height=750)])
    with pytest.raises(ValueError,match=r'Base64.*2097152'):
        build_request(bundle,'模板',{},dict(model='test'))


@pytest.mark.parametrize('size', [(2001,30),(30,2001)])
def test_bound_image_side_limit_rejects_before_request_without_resizing(tmp_path,size):
    from PIL import Image
    from preextract_material import file_sha
    from preextract_request import build_request,RequestCapacityError
    path=tmp_path/'source.png';Image.new('RGB',size).save(path)
    sha=file_sha(path)
    bundle=dict(source={},blocks=[],sections=[],assets=[dict(id='a',image_path=str(path),
        sha256=sha,width=size[0],height=size[1])])
    with pytest.raises(RequestCapacityError,match='2000'):
        build_request(bundle,'模板',{},dict(model='bound-cli',max_image_side=2000))
    assert file_sha(path)==sha and not (tmp_path/'request-state.json').exists()
    # The restriction belongs to this transport, not every model.
    body,_=build_request(bundle,'模板',{},dict(model='ordinary'))
    assert any(p['type']=='image_url' for p in body['messages'][1]['content'])
