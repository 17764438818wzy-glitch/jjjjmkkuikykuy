import io
from pathlib import Path
import zipfile
from xml.etree import ElementTree as ET

import pytest
from PIL import Image
from native_docx import parse_docx
from native_picture import UnsupportedPicture,displayed_picture

FIXTURE=Path(__file__).parent/'fixtures/carbon_image3.docx'
A='{http://schemas.openxmlformats.org/drawingml/2006/main}'

def extract(source):
    resources=[]
    def resource(rid,data,ext,kind,position):
        record={'id':rid,'data':data,'ext':ext,'position':position};resources.append(record);return record
    blocks,images,_=parse_docx(source,lambda text:text,resource,lambda *args:None)
    return resources,images,blocks

def modify_fixture(tmp_path,change):
    target=tmp_path/'test.docx'
    with zipfile.ZipFile(FIXTURE) as src,zipfile.ZipFile(target,'w') as dst:
        root=ET.fromstring(src.read('word/document.xml'));change(root)
        for n in src.namelist():dst.writestr(n,ET.tostring(root) if n=='word/document.xml' else src.read(n))
    return target


def assert_deferred_to_rendered_page(source):
    with zipfile.ZipFile(source) as archive:
        root=ET.fromstring(archive.read('word/document.xml'))
        parents={child:parent for parent in root.iter() for child in parent}
        with pytest.raises(UnsupportedPicture):
            displayed_picture(archive.read('word/media/image3.png'),'.png',
                              root.find('.//'+A+'blip'),parents)
    resources=[];issues=[]
    blocks,images,_=parse_docx(source,lambda text:text,
        lambda rid,data,ext,kind,position:resources.append(rid) or {'id':rid},
        lambda code,message,position:issues.append(code))
    assert not resources and not images
    assert 'embedded_object_review' in issues
    assert any(block.get('graphic_object') for block in blocks)

def test_real_carbon_crop_excludes_hidden_desktop_and_preserves_visible_pixels():
    # Raw embedded screenshot includes another paper to the right; Word hides it.
    before=FIXTURE.read_bytes();resources,images,_=extract(FIXTURE)
    with zipfile.ZipFile(FIXTURE) as z:original=Image.open(io.BytesIO(z.read('word/media/image3.png'))).convert('RGB')
    visible=Image.open(io.BytesIO(resources[0]['data'])).convert('RGB')
    # WPS PDF physical page17 stores a 1098x371 image aligned to this box.
    assert visible.size==(1098,371)
    assert visible.tobytes()==original.crop((0,100,1098,471)).tobytes()
    assert resources[0]['ext']=='.png' and FIXTURE.read_bytes()==before
    assert images[0]['display_crop']['source_rectangle']=={'l':0,'t':9237,'r':42826,'b':56344}
    assert images[0]['display_crop']['pixel_bbox']==[0,100,1098,471]
    assert resources[0]['display_crop']==images[0]['display_crop']

def test_uncropped_image_remains_exact_original_bytes(tmp_path):
    def change(root):
        for n in root.iter():
            for child in list(n):
                if child.tag==A+'srcRect':n.remove(child)
    resources,_,_=extract(modify_fixture(tmp_path,change))
    with zipfile.ZipFile(FIXTURE) as z:assert resources[0]['data']==z.read('word/media/image3.png')


def test_subpixel_negative_crop_from_office_conversion_keeps_full_image(tmp_path):
    def change(root):
        crop=root.find('.//'+A+'srcRect')
        crop.attrib.clear()
        crop.attrib.update({'l':'-12','t':'-37','r':'-12','b':'-37'})
    resources,images,_=extract(modify_fixture(tmp_path,change))
    with zipfile.ZipFile(FIXTURE) as z:original=z.read('word/media/image3.png')
    assert resources[0]['data']==original
    assert 'display_crop' not in resources[0]
    assert 'display_crop' not in images[0]


def test_subpixel_negative_edge_does_not_restore_hidden_pixels(tmp_path):
    def change(root):
        crop=root.find('.//'+A+'srcRect')
        crop.attrib.update({'l':'-12','t':'9237','r':'42826','b':'56344'})
    resources,_,_=extract(modify_fixture(tmp_path,change))
    with zipfile.ZipFile(FIXTURE) as z:original=Image.open(io.BytesIO(z.read('word/media/image3.png'))).convert('RGB')
    visible=Image.open(io.BytesIO(resources[0]['data'])).convert('RGB')
    assert visible.size==(1098,371)
    assert visible.tobytes()==original.crop((0,100,1098,471)).tobytes()


def test_negative_crop_past_half_pixel_is_still_rejected(tmp_path):
    def change(root):
        crop=root.find('.//'+A+'srcRect')
        crop.attrib.update({'l':'-100'})
    assert_deferred_to_rendered_page(modify_fixture(tmp_path,change))

@pytest.mark.parametrize('attrs',[{'r':'100000'},{'l':'-1000'},{'t':'bogus'},{'l':'50000','r':'50000'},{'r':'NaN'}])
def test_invalid_or_expanding_source_rectangle_fails_closed(tmp_path,attrs):
    def change(root):
        crop=root.find('.//'+A+'srcRect');crop.attrib.clear();crop.attrib.update(attrs)
    assert_deferred_to_rendered_page(modify_fixture(tmp_path,change))

@pytest.mark.parametrize('change_kind',['rotation','flip','tile','fill_rect','nonrect','blip_effect'])
def test_crop_with_unhandled_display_transform_fails_closed(tmp_path,change_kind):
    def change(root):
        if change_kind=='rotation':root.find('.//'+A+'xfrm').set('rot','60000')
        elif change_kind=='flip':root.find('.//'+A+'xfrm').set('flipH','1')
        elif change_kind=='tile':
            stretch=root.find('.//'+A+'stretch');stretch.tag=A+'tile'
        elif change_kind=='fill_rect':root.find('.//'+A+'fillRect').set('l','1000')
        elif change_kind=='nonrect':root.find('.//'+A+'prstGeom').set('prst','ellipse')
        else:ET.SubElement(root.find('.//'+A+'blip'),A+'alphaModFix',{'amt':'0'})
    assert_deferred_to_rendered_page(modify_fixture(tmp_path,change))


@pytest.mark.parametrize('has_crop',[True,False])
def test_picture_with_bilevel_effect_uses_page_recovery_instead_of_raw_pixels(tmp_path,has_crop):
    def change(root):
        blip=root.find('.//'+A+'blip')
        ET.SubElement(blip,A+'biLevel',{'thresh':'50000'})
        if not has_crop:
            for parent in root.iter():
                for child in list(parent):
                    if child.tag==A+'srcRect':parent.remove(child)
    source=modify_fixture(tmp_path,change)
    resources=[];issues=[]
    def resource(rid,data,ext,kind,position):
        resources.append(rid)
        return {'id':rid}
    blocks,images,_=parse_docx(source,lambda text:text,resource,
                               lambda code,message,position:issues.append((code,position)))
    assert not resources and not images
    assert any(code=='embedded_object_review' for code,_ in issues)
    assert any(block.get('graphic_object') and block.get('preview_covers_object') is False for block in blocks)


@pytest.mark.parametrize('change_kind',['rotation','nonrect','fill_rect'])
def test_uncropped_shape_transform_uses_rendered_page(tmp_path,change_kind):
    def change(root):
        for parent in root.iter():
            for child in list(parent):
                if child.tag==A+'srcRect':parent.remove(child)
        if change_kind=='rotation':root.find('.//'+A+'xfrm').set('rot','60000')
        elif change_kind=='nonrect':root.find('.//'+A+'prstGeom').set('prst','ellipse')
        else:root.find('.//'+A+'fillRect').set('l','1000')
    assert_deferred_to_rendered_page(modify_fixture(tmp_path,change))


def test_object_with_one_supported_and_one_unsupported_picture_requires_page_recovery(tmp_path):
    import copy
    W='{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
    PIC='{http://schemas.openxmlformats.org/drawingml/2006/picture}'
    def change(root):
        drawing=root.find('.//'+W+'drawing')
        drawing.tag=W+'object'
        picture=root.find('.//'+PIC+'pic')
        parent=next(node for node in root.iter() if picture in list(node))
        second=copy.deepcopy(picture)
        ET.SubElement(second.find('.//'+A+'blip'),A+'biLevel',{'thresh':'50000'})
        parent.append(second)
    source=modify_fixture(tmp_path,change)
    resources=[];issues=[]
    blocks,images,_=parse_docx(source,lambda text:text,
        lambda rid,data,ext,kind,position:resources.append(rid) or {'id':rid},
        lambda code,message,position:issues.append(code))
    assert len(resources)==len(images)==1
    assert 'embedded_object_review' in issues
    assert any(block.get('graphic_object') and block.get('preview_covers_object') is False for block in blocks)


@pytest.mark.parametrize('overlay_kind',['vml_rect','word_shape'])
def test_object_with_image_and_opaque_overlay_requires_page_recovery(tmp_path,overlay_kind):
    W='{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
    VML='{urn:schemas-microsoft-com:vml}'
    WPS='{http://schemas.microsoft.com/office/word/2010/wordprocessingShape}'
    def change(root):
        drawing=root.find('.//'+W+'drawing')
        drawing.tag=W+'object'
        if overlay_kind=='vml_rect':
            overlay=ET.SubElement(drawing,VML+'rect')
            ET.SubElement(overlay,VML+'fill',{'color':'#ffffff'})
        else:
            overlay=ET.SubElement(drawing,WPS+'wsp')
            ET.SubElement(overlay,A+'prstGeom',{'prst':'rect'})
    source=modify_fixture(tmp_path,change)
    resources=[];issues=[]
    blocks,images,_=parse_docx(source,lambda text:text,
        lambda rid,data,ext,kind,position:resources.append(rid) or {'id':rid},
        lambda code,message,position:issues.append(code))
    assert len(resources)==len(images)==1
    assert 'embedded_object_review' in issues
    assert any(block.get('graphic_object') and block.get('preview_covers_object') is False for block in blocks)


@pytest.mark.parametrize('change_kind',['rotation','nonrect','shape_path','image_effect'])
def test_vml_picture_with_unhandled_display_transform_uses_page_recovery(tmp_path,change_kind):
    from docx import Document
    from docx.oxml import parse_xml
    doc=Document()
    picture=tmp_path/'original.png'
    Image.new('RGB',(73,41),'blue').save(picture)
    rid,_=doc.part.get_or_add_image(str(picture))
    shape_attr={'rotation':'style="rotation:90"','nonrect':'type="#custom-shape"',
                'shape_path':'path="m 0,0 l 1,1 e"',
                'image_effect':''}[change_kind]
    effect=' blacklevel="0.5"' if change_kind=='image_effect' else ''
    node=parse_xml(f'<w:pict xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" '
        f'xmlns:v="urn:schemas-microsoft-com:vml" '
        f'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<v:shape {shape_attr}><v:imagedata r:id="{rid}"{effect}/></v:shape></w:pict>')
    doc.add_paragraph().add_run()._r.append(node)
    source=tmp_path/'source.docx';doc.save(source)
    resources=[];issues=[]
    blocks,images,_=parse_docx(source,lambda text:text,
        lambda rid,data,ext,kind,position:resources.append(rid) or {'id':rid},
        lambda code,message,position:issues.append(code))
    assert not resources and not images
    assert 'embedded_object_review' in issues
    assert any(block.get('graphic_object') for block in blocks)

def test_same_media_occurrences_use_each_own_crop(tmp_path):
    import copy
    def change(root):
        body=root.find('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}body')
        second=copy.deepcopy(body[0]);second.find('.//'+A+'srcRect').attrib.clear();body.append(second)
    resources,_,_=extract(modify_fixture(tmp_path,change))
    assert [Image.open(io.BytesIO(r['data'])).size for r in resources]==[(1098,371),(1920,1080)]

@pytest.mark.parametrize('part,kind',[('footnotes','footnote'),('endnotes','endnote')])
def test_note_picture_keeps_its_own_display_crop(tmp_path,part,kind):
    w='{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
    target=tmp_path/'notes.docx'
    with zipfile.ZipFile(FIXTURE) as src,zipfile.ZipFile(target,'w') as dst:
        root=ET.fromstring(src.read('word/document.xml'));body=root.find(w+'body')
        notes=ET.Element(w+part);note=ET.SubElement(notes,w+kind,{w+'id':'1'});note.append(body[0]);body.remove(body[0])
        dst.writestr('word/document.xml',ET.tostring(root));dst.writestr('word/'+part+'.xml',ET.tostring(notes))
        dst.writestr('word/_rels/'+part+'.xml.rels',src.read('word/_rels/document.xml.rels'))
        dst.writestr('word/media/image3.png',src.read('word/media/image3.png'))
    resources,images,_=extract(target)
    assert Image.open(io.BytesIO(resources[0]['data'])).size==(1098,371)
    assert images[0]['position']=={'part':'word/'+part+'.xml','id':'1'}

def test_evidence_package_only_exposes_display_crop_to_media_jobs(tmp_path):
    from evidence import build_package
    package=tmp_path/'package'
    build_package(FIXTURE,package,{'id':'source-crop-test','title':'图片裁剪回归'},redact_content=False)
    import json
    meta=json.loads((package/'metadata.json').read_text(encoding='utf-8'))
    resource=meta['resources'][0]
    image=next(b for b in meta['blocks'] if b.get('resource_id')==resource['id'])
    assert Image.open(package/resource['local_path']).size==(1098,371)
    assert image['display_crop']==resource['display_crop']
    assert resource['display_crop']['source_sha256']=='b5e12e4f11809c2b4ce3cab8340a341554e20a6c78b2fb667a693bd3dcb42072'
    assert image['ocr']['status']=='unavailable' and resource['privacy_status']=='pending'
