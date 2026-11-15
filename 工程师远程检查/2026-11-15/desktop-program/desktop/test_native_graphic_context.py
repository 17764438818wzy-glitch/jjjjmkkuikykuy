"""Native shape facts inform repair without declaring ownership or approval."""
import copy
import json
import zipfile
from xml.etree import ElementTree as ET

import fitz
import pytest

import evidence_tasks

W='{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
A='{http://schemas.openxmlformats.org/drawingml/2006/main}'
WPS='{http://schemas.microsoft.com/office/word/2010/wordprocessingShape}'


def source_and_meta(tmp_path, *, change=None):
    namespaces='''xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
      xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main"
      xmlns:wp="http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing"
      xmlns:wps="http://schemas.microsoft.com/office/word/2010/wordprocessingShape"
      xmlns:pic="http://schemas.openxmlformats.org/drawingml/2006/picture"
      xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"
      xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006"
      xmlns:v="urn:schemas-microsoft-com:vml"'''
    shape='''<w:r><mc:AlternateContent><mc:Choice Requires="wps"><w:drawing><wp:anchor>
      <wp:docPr id="99" name="Private Student"/><a:graphic><a:graphicData><wps:wsp>
      <wps:spPr><a:xfrm/><a:prstGeom prst="rect"><a:avLst/></a:prstGeom>
      <a:noFill/><a:ln><a:solidFill><a:schemeClr val="tx1"/></a:solidFill></a:ln>
      </wps:spPr><wps:bodyPr/></wps:wsp></a:graphicData></a:graphic>
      </wp:anchor></w:drawing></mc:Choice><mc:Fallback><w:pict><v:rect/></w:pict>
      </mc:Fallback></mc:AlternateContent></w:r>'''
    picture='''<w:r><w:drawing><wp:anchor><a:graphic><a:graphicData><pic:pic>
      <pic:blipFill><a:blip r:embed="{rid}"/></pic:blipFill></pic:pic></a:graphicData>
      </a:graphic></wp:anchor></w:drawing></w:r>'''
    root=ET.fromstring(f'<w:document {namespaces}><w:body><w:p>'+picture.format(rid='rId20')+
        shape+shape+picture.format(rid='rId21')+'</w:p></w:body></w:document>')
    if change:change(root)
    source=tmp_path/'source.docx'
    # Keep namespace declarations needed by mc:Choice Requires after ET serialization.
    ET.register_namespace('wps',WPS[1:-1])
    with zipfile.ZipFile(source,'w') as archive:
        archive.writestr('word/document.xml',ET.tostring(root))
    position={'part':'word/document.xml','body_index':0}
    target={'id':'frame_region','type':'paragraph','text':'[解析说明：图形需恢复]',
            'position':{**position,'object_ref':'object_0009'},'graphic_object':True,
            'graphic_members':[{'position':{**position,'object_ref':'object_0009'}},
                               {'position':{**position,'object_ref':'object_0010'}}]}
    resources=[{'id':rid,'kind':'image','source':'original','position':copy.deepcopy(position),
                'local_path':'_review/Private Student.png'} for rid in ('image_0010','image_0011')]
    resources.extend([
        {'id':'image_0012','kind':'image','source':'original','position':{**position,'body_index':1}},
        {'id':'image_0013','kind':'image','source':'original','position':{**position,'object_ref':'object_0009'}},
        {'id':'visual_p1_1','kind':'image','source':'original_page_crop','position':copy.deepcopy(position)}])
    return source,target,resources


def test_two_outline_rectangles_and_two_independent_bitmaps_are_only_same_paragraph_facts(tmp_path):
    from native_graphic_context import source_graphic_context
    source,target,resources=source_and_meta(tmp_path)
    before=source.read_bytes();untouched=copy.deepcopy((target,resources))
    result=source_graphic_context(source,target,resources)
    facts=result['source_graphic_context']
    assert facts['scope']=='same_source_paragraph'
    assert facts['position']=={'part':'word/document.xml','body_index':0}
    assert facts['target_object_mapping']=='unavailable'
    assert len(facts['outline_rectangles'])==2
    for shape in facts['outline_rectangles']:
        assert shape['geometry']=='rect' and shape['fill']=='none'
        assert shape['has_text'] is False and shape['has_image'] is False and shape['has_chart'] is False
    assert facts['independent_image_resources']==[
        {'id':rid,'position':{'part':'word/document.xml','body_index':0}}
        for rid in ('image_0010','image_0011')]
    assert 'Private Student' not in json.dumps(result)
    assert source.read_bytes()==before and (target,resources)==untouched
    facts['position']['body_index']=999
    assert target['position']['body_index']==0
    assert not {'decision','nonresearch_objects','bbox'} & result.keys()


@pytest.mark.parametrize('kind',['unknown_geometry','filled_shape','text','image','custom_geometry','unknown_child','group'])
def test_unsupported_shapes_do_not_become_empty_frame_facts(tmp_path,kind):
    from native_graphic_context import source_graphic_context
    def change(root):
        shape=root.find('.//'+WPS+'wsp');properties=shape.find(WPS+'spPr')
        if kind=='unknown_geometry':properties.find(A+'prstGeom').set('prst','ellipse')
        elif kind=='filled_shape':properties.remove(properties.find(A+'noFill'));ET.SubElement(properties,A+'solidFill')
        elif kind=='text':ET.SubElement(shape,W+'t').text='Private Student'
        elif kind=='image':ET.SubElement(shape,A+'blip')
        elif kind=='custom_geometry':ET.SubElement(properties,A+'custGeom')
        elif kind=='unknown_child':ET.SubElement(shape,W+'sym',{W+'char':'F0E0'})
        else:shape.tag='{http://schemas.microsoft.com/office/word/2010/wordprocessingGroup}wgp'
    source,target,resources=source_and_meta(tmp_path,change=change)
    assert source_graphic_context(source,target,resources)=={}


@pytest.mark.parametrize('position',[
    {'part':'word/header1.xml','body_index':0},
    {'part':'word/document.xml','body_index':0,'row':0,'column':0},
    {'part':'word/document.xml','body_index':0,'container_path':['table']},
    {'part':'word/document.xml','body_index':False},
])
def test_complex_source_positions_do_not_borrow_body_shapes(tmp_path,position):
    from native_graphic_context import source_graphic_context
    source,target,resources=source_and_meta(tmp_path);target['position']=position
    assert source_graphic_context(source,target,resources)=={}


def test_repair_payload_receives_facts_without_persisting_them_into_metadata(tmp_path,monkeypatch):
    source,target,resources=source_and_meta(tmp_path)
    pdf_path=tmp_path/'render.pdf'
    with fitz.open() as pdf:
        page=pdf.new_page();page.insert_text((40,40),'Original image and outlines');pdf.save(pdf_path)
    monkeypatch.setattr(evidence_tasks,'_pdf',lambda *args:fitz.open(pdf_path))
    monkeypatch.setattr(evidence_tasks,'_locate',lambda *args:{0})
    monkeypatch.setattr(evidence_tasks,'_graphic_member_locations',lambda *args:(set(),[]))
    meta={'blocks':[target],'resources':resources,'issues':[{
        'code':'embedded_object_review','position':copy.deepcopy(target['position'])}]}
    before=copy.deepcopy(meta);calls=[]
    class Captured(BaseException):pass
    class Agent:
        def call(self,task,payload,images=()):
            calls.append((task,payload));raise Captured()
    with pytest.raises(Captured):
        evidence_tasks.resolve_scoped(meta,tmp_path/'package',source,tmp_path/'work',80,Agent(),lambda *args:None)
    assert calls[0][0]=='repair'
    facts=calls[0][1]['known_blocks'][0].get('source_graphic_context')
    assert facts and len(facts['outline_rectangles'])==2
    assert [r['id'] for r in facts['independent_image_resources']]==['image_0010','image_0011']
    assert meta==before
