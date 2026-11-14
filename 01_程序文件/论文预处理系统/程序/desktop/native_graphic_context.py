"""Narrow source shape facts for a private repair request, never a verdict."""
import copy
import io
from pathlib import Path
import re
import xml.etree.ElementTree as ET
import zipfile

from native_docx import select_compatibility

W='{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
A='{http://schemas.openxmlformats.org/drawingml/2006/main}'
C='{http://schemas.openxmlformats.org/drawingml/2006/chart}'
WPS='{http://schemas.microsoft.com/office/word/2010/wordprocessingShape}'


def source_graphic_context(source, target, resources):
    """Describe supported same-paragraph outlines, without mapping object_ref.

    OOXML object IDs and parser object_ref counters are different namespaces.
    These facts deliberately make no claim that a retained adjacent bitmap is
    a member of the target. No source text, drawing names or paths are returned.
    """
    position=target.get('position',{})
    if (not target.get('graphic_object') or Path(source).suffix.lower()!='.docx'
            or not isinstance(position,dict) or position.get('part')!='word/document.xml'
            or type(position.get('body_index')) is not int or position['body_index']<0
            or any(key in position for key in ('row','column','container_path'))):
        return {}
    try:
        with zipfile.ZipFile(source) as archive:
            xml=archive.read('word/document.xml')
        root=ET.fromstring(xml)
        namespaces=dict(item for _,item in ET.iterparse(io.BytesIO(xml),events=['start-ns']))
        select_compatibility(root,namespaces)
        body=root.find(W+'body')
        if body is None or position['body_index']>=len(body):return {}
        paragraph=body[position['body_index']]
        if paragraph.tag!=W+'p':return {}
        shapes=list(paragraph.iter(WPS+'wsp'))
        if not 1<=len(shapes)<=64:return {}
        # Do not infer the appearance of grouped, VML or other shape dialects.
        if any(n.tag.rsplit('}',1)[-1] in {'group','wgp','grpSp','sp','shape',
                'rect','oval','roundrect','cxnSp'} for n in paragraph.iter()):return {}
        facts=[]
        for ordinal,shape in enumerate(shapes,1):
            if any(n.tag not in {WPS+'cNvSpPr',WPS+'spPr',WPS+'style',WPS+'bodyPr'} for n in shape):
                return {}
            properties=shape.find(WPS+'spPr')
            if properties is None:return {}
            geometry=properties.find(A+'prstGeom')
            if (geometry is None or geometry.get('prst')!='rect'
                    or properties.find(A+'noFill') is None
                    or any(n.tag not in {A+'xfrm',A+'prstGeom',A+'noFill',A+'ln'} for n in properties)):
                return {}
            line=properties.find(A+'ln')
            if (line is None or len(line)!=1 or line[0].tag!=A+'solidFill'
                    or any(n.tag in {W+'t',A+'t',W+'txbxContent',A+'blip',C+'chart'}
                           or n.tag.endswith('}imagedata') for n in shape.iter())):
                return {}
            if any(n.get('rot','0')!='0' or any(n.get(key,'0') not in {'0','false','off'}
                       for key in ('flipH','flipV')) for n in shape.iter(A+'xfrm')):return {}
            facts.append({'shape_ordinal_in_paragraph':ordinal,'shape_type':'wps:wsp',
                          'geometry':'rect','fill':'none','stroke':'solid',
                          'has_text':False,'has_image':False,'has_chart':False})
    except (OSError,zipfile.BadZipFile,ET.ParseError,KeyError,ValueError):
        return {}
    source_position={'part':'word/document.xml','body_index':position['body_index']}
    independent=[]
    for resource in resources:
        if (resource.get('kind')=='image' and resource.get('source')=='original'
                and resource.get('position')==source_position
                and isinstance(resource.get('id'),str)
                and re.fullmatch(r'image_[0-9]{4,8}',resource['id'])):
            independent.append({'id':resource['id'],'position':copy.deepcopy(source_position)})
    return {'source_graphic_context':{
        'scope':'same_source_paragraph','position':source_position,
        'target_object_mapping':'unavailable',
        'outline_rectangles':facts,'independent_image_resources':independent,
        'association':'同段独立提取的原始图片；不代表图片属于目标框对象。',
        'usage':'这些是原文结构事实，仅供结合页图判断目标用途；不是非研究结论，不自动省略对象或改变裁图。'}}
