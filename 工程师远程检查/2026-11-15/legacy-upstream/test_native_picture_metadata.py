"""Office compression metadata must not hide an otherwise supported bitmap."""
import copy
import io
import zipfile
from xml.etree import ElementTree as ET

import pytest
from PIL import Image

from test_native_picture_crop import (
    A, FIXTURE, assert_deferred_to_rendered_page, extract, modify_fixture,
)

A14='{http://schemas.microsoft.com/office/drawing/2010/main}'
WP='{http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing}'
URI='{28A0092B-C50C-407E-A947-70E740481C1C}'


def local_dpi(root, value='0', *, keep_crop=False):
    # Match the failed source's floating, ungrouped PIC inside wp:anchor.
    frame=root.find('.//'+WP+'inline')
    if frame is not None:frame.tag=WP+'anchor'
    if not keep_crop:
        for parent in root.iter():
            for child in list(parent):
                if child.tag==A+'srcRect':parent.remove(child)
    blip=root.find('.//'+A+'blip')
    ext_list=ET.SubElement(blip,A+'extLst')
    ext=ET.SubElement(ext_list,A+'ext',{'uri':URI})
    flag=ET.SubElement(ext,A14+'useLocalDpi',{} if value is None else {'val':value})
    return blip,ext_list,ext,flag


@pytest.mark.parametrize('value',['0','1','false','true',None])
def test_floating_local_dpi_metadata_keeps_exact_embedded_pixels(tmp_path,value):
    source=modify_fixture(tmp_path,lambda root:local_dpi(root,value))
    before=source.read_bytes()
    resources,images,blocks=extract(source)
    with zipfile.ZipFile(FIXTURE) as archive:original=archive.read('word/media/image3.png')
    assert len(resources)==len(images)==1
    assert resources[0]['data']==original
    assert resources[0]['position']=={'part':'word/document.xml','body_index':0}
    assert not any(block.get('graphic_object') for block in blocks)
    assert images[0]['privacy_status']=='pending'
    assert source.read_bytes()==before


def test_local_dpi_metadata_does_not_restore_pixels_hidden_by_crop(tmp_path):
    source=modify_fixture(tmp_path,lambda root:local_dpi(root,keep_crop=True))
    resources,images,_=extract(source)
    with zipfile.ZipFile(FIXTURE) as archive:
        original=Image.open(io.BytesIO(archive.read('word/media/image3.png'))).convert('RGB')
    assert len(resources)==len(images)==1
    visible=Image.open(io.BytesIO(resources[0]['data'])).convert('RGB')
    assert visible.size==(1098,371)
    assert visible.tobytes()==original.crop((0,100,1098,471)).tobytes()
    assert resources[0]['display_crop']['pixel_bbox']==[0,100,1098,471]


@pytest.mark.parametrize('kind',[
    'unknown_uri','wrong_namespace','invalid_value','extra_flag_attribute',
    'extra_ext_attribute','extra_list_attribute','effect_sibling',
    'effect_in_ext','effect_in_flag','duplicate_flag','duplicate_ext',
    'duplicate_list','rotation','flip','unknown_crop',
])
def test_local_dpi_metadata_does_not_bypass_unknown_effects_or_transforms(tmp_path,kind):
    def change(root):
        blip,ext_list,ext,flag=local_dpi(root,keep_crop=True)
        if kind=='unknown_uri':ext.set('uri','unknown')
        elif kind=='wrong_namespace':flag.tag=A+'useLocalDpi'
        elif kind=='invalid_value':flag.set('val','yes')
        elif kind=='extra_flag_attribute':flag.set('effect','unknown')
        elif kind=='extra_ext_attribute':ext.set('effect','unknown')
        elif kind=='extra_list_attribute':ext_list.set('effect','unknown')
        elif kind=='effect_sibling':ET.SubElement(blip,A+'biLevel',{'thresh':'50000'})
        elif kind=='effect_in_ext':ET.SubElement(ext,A+'biLevel',{'thresh':'50000'})
        elif kind=='effect_in_flag':ET.SubElement(flag,A+'biLevel',{'thresh':'50000'})
        elif kind=='duplicate_flag':ext.append(copy.deepcopy(flag))
        elif kind=='duplicate_ext':ext_list.append(copy.deepcopy(ext))
        elif kind=='duplicate_list':blip.append(copy.deepcopy(ext_list))
        elif kind=='rotation':root.find('.//'+A+'xfrm').set('rot','60000')
        elif kind=='flip':root.find('.//'+A+'xfrm').set('flipH','1')
        elif kind=='unknown_crop':root.find('.//'+A+'srcRect').set('l','bad')
    assert_deferred_to_rendered_page(modify_fixture(tmp_path,change))


def test_local_dpi_picture_stays_private_pending_review_in_evidence_package(tmp_path):
    import json
    from evidence import build_package
    source=modify_fixture(tmp_path,lambda root:local_dpi(root))
    package=tmp_path/'package'
    build_package(source,package,{'id':'local-dpi-test','title':'原生图片元数据回归'},
                  redact_content=False)
    meta=json.loads((package/'metadata.json').read_text(encoding='utf-8'))
    assert len(meta['resources'])==1
    resource=meta['resources'][0]
    with zipfile.ZipFile(FIXTURE) as archive:
        assert (package/resource['local_path']).read_bytes()==archive.read('word/media/image3.png')
    assert resource['privacy_status']=='pending' and resource['upload_allowed'] is False
    assert resource['local_path'].startswith('_review/')
    assert any(issue['code']=='image_ocr_review' for issue in meta['issues'])
    assert not any(issue['code']=='embedded_object_review' for issue in meta['issues'])
