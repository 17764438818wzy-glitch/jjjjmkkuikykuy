"""Extract a DOCX picture's rectangular display crop without resampling."""
import hashlib
import io
import re

A='{http://schemas.openxmlformats.org/drawingml/2006/main}'
A14='{http://schemas.microsoft.com/office/drawing/2010/main}'
PIC='{http://schemas.openxmlformats.org/drawingml/2006/picture}'
VML='{urn:schemas-microsoft-com:vml}'
W='{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'


class UnsupportedPicture(ValueError):
    """The embedded pixels do not prove the picture's displayed appearance."""


def iter_ancestors(node, parents):
    while node in parents:
        node=parents[node]
        yield node


def only_compression_metadata(blip):
    """Allow the known Office compression flag, never unknown image effects."""
    if not len(blip):return True
    if len(blip)!=1:return False
    extensions=blip[0]
    if extensions.tag!=A+'extLst' or extensions.attrib or len(extensions)!=1:return False
    extension=extensions[0]
    if (extension.tag!=A+'ext' or set(extension.attrib)!={'uri'} or len(extension)!=1 or
            extension.get('uri','').upper()!='{28A0092B-C50C-407E-A947-70E740481C1C}'):
        return False
    flag=extension[0]
    # MS-ODRAWXML 2.3.3.42: this selects the stored BLIP compression setting;
    # it does not crop, mask or alter which pixels the picture displays.
    return (flag.tag==A14+'useLocalDpi' and not len(flag) and
            not (set(flag.attrib)-{'val'}) and flag.get('val','true') in {'0','1','false','true'} and
            not any((n.text or '').strip() or (n.tail or '').strip() for n in extensions.iter()))


def displayed_picture(data, extension, node, parents):
    """Never pass hidden source pixels through when display cropping is present.

    Only plain rectangular DrawingML crops are supported. Ambiguous crop/shape
    transformations need a rendered source page, not an uncropped raw image.
    """
    def unsupported(reason):
        raise UnsupportedPicture('DOCX 图片裁剪无法安全提取，需核对完整显示页：'+reason)

    if node.tag.endswith('}imagedata'):
        if any(node.get(k) not in (None,'','0','0f') for k in ('cropleft','croptop','cropright','cropbottom')):
            unsupported('不支持的 VML 裁剪')
        if any(k.rsplit('}',1)[-1] not in {'id','embed','link','src','relid','title','cropleft',
                                         'croptop','cropright','cropbottom'} for k in node.attrib):
            unsupported('VML 图像效果')
        shape=parents.get(node)
        if shape is None or shape.tag not in {VML+'shape',VML+'rect'}:
            unsupported('未知 VML 图片形状')
        if shape.get('type') not in (None,'#_x0000_t75'):
            unsupported('非矩形 VML 图片形状')
        if any(k.rsplit('}',1)[-1] in {'path','adj','rotation','flip','fillcolor','opacity','strokeweight'}
               for k in shape.attrib) or any(child.tag in {VML+'path',VML+'fill',VML+'stroke',
               VML+'shadow',VML+'skew'} for child in shape):
            unsupported('VML 形状效果或裁剪')
        if any(n.tag in {VML+'group',VML+'shapetype'} for n in (shape,*iter_ancestors(shape,parents))):
            unsupported('VML 组合图形变换')
        style={key.strip().lower():value.strip().lower()
               for item in shape.get('style','').split(';') if ':' in item
               for key,value in [item.split(':',1)]}
        if any(key not in {'width','height','left','top','position','z-index','visibility'}
               for key in style) or shape.get('flip') not in (None,'','0'):
            unsupported('VML 旋转、翻转或未知变换')
        return data,extension,None
    fill=parents.get(node)
    crop=fill.find(A+'srcRect') if fill is not None else None
    if not only_compression_metadata(node):unsupported('图像效果或扩展变换')
    # A few legacy/minimal DOCX files place a bare blip directly in drawing.
    # With no shape or sibling to transform it, its embedded pixels are displayed.
    if fill is not None and fill.tag==W+'drawing' and len(fill)==1 and crop is None:
        return data,extension,None
    picture=parents.get(fill) if fill is not None else None
    if fill is None or fill.tag!=PIC+'blipFill' or picture is None or picture.tag!=PIC+'pic':
        unsupported('未知图片容器')
    if any(c.tag not in {A+'blip',A+'srcRect',A+'stretch'} for c in fill):unsupported('平铺或未知填充变换')
    if len(fill.findall(A+'srcRect'))>1 or len(fill.findall(A+'blip'))!=1:
        unsupported('重复图片或裁剪定义')
    stretch=fill.find(A+'stretch')
    if stretch is not None:
        if len(stretch)!=1 or stretch[0].tag!=A+'fillRect':unsupported('未知目标矩形')
        if any(v not in ('0','0%') for v in stretch[0].attrib.values()):unsupported('非默认目标矩形')
    elif crop is not None:
        unsupported('未知目标矩形')
    shape=picture.find(PIC+'spPr')
    if shape is None:unsupported('缺少矩形形状定义')
    geometry=shape.find(A+'prstGeom')
    if geometry is None or geometry.get('prst')!='rect' or shape.find(A+'custGeom') is not None:unsupported('非矩形裁剪')
    if any(c.tag not in {A+'xfrm',A+'prstGeom',A+'noFill',A+'ln'} for c in shape):unsupported('形状效果或未知变换')
    ancestor=picture
    while ancestor is not None:
        for xfrm in [ancestor] if ancestor.tag==A+'xfrm' else list(ancestor.findall('./'+A+'xfrm')):
            if xfrm.get('rot','0')!='0' or any(xfrm.get(k,'0') not in ('0','false','off') for k in ('flipH','flipV')):
                unsupported('旋转或翻转')
        if ancestor.tag.rsplit('}',1)[-1] in {'grpSp','wgp','group','sp','wsp'}:unsupported('组合图形变换')
        ancestor=parents.get(ancestor)
    # pic:spPr is not an ancestor of pic:blipFill, so check it separately.
    xfrm=shape.find(A+'xfrm')
    if xfrm is not None and (xfrm.get('rot','0')!='0' or any(xfrm.get(k,'0') not in ('0','false','off') for k in ('flipH','flipV'))):
        unsupported('旋转或翻转')
    if crop is None:
        return data,extension,None
    offsets={}
    if set(crop.attrib)-{'l','t','r','b'} or len(crop):unsupported('未知源矩形属性')
    for side in ('l','t','r','b'):
        value=crop.get(side,'0')
        if not re.fullmatch(r'[+-]?[0-9]+',value):unsupported('无效或不支持的源矩形百分比')
        offsets[side]=int(value)
    if any(v>=100000 for v in offsets.values()) or offsets['l']+offsets['r']>=100000 or offsets['t']+offsets['b']>=100000:
        unsupported('源矩形范围为空或超出原图')
    if not any(offsets.values()):
        return data,extension,None
    from PIL import Image
    try:
        with Image.open(io.BytesIO(data)) as image:
            if image.format not in {'PNG','JPEG','BMP','TIFF','GIF'} or getattr(image,'n_frames',1)!=1:
                unsupported('非单帧位图')
            if image.getexif().get(274,1)!=1:unsupported('EXIF 方向变换')
            image.load();width,height=image.size
            if any(value<0 and -value*(width if side in 'lr' else height)>=50000
                   for side,value in offsets.items()):
                unsupported('负裁剪超出像素舍入范围')
            # Quantize each source edge to its nearest pixel, matching the
            # actual Word/WPS crop extent. Keep native pixel values; no resize.
            box=((width*offsets['l']+50000)//100000,(height*offsets['t']+50000)//100000,
                 (width*(100000-offsets['r'])+50000)//100000,
                 (height*(100000-offsets['b'])+50000)//100000)
            if box[0]>=box[2] or box[1]>=box[3]:unsupported('没有可见像素')
            if any(value<0 for value in offsets.values()) and box==(0,0,width,height):
                return data,extension,None
            cropped=image.crop(box);output=io.BytesIO();cropped.save(output,format='PNG')
    except (OSError,SyntaxError,Image.DecompressionBombError) as exc:
        unsupported('位图无法解码：'+type(exc).__name__)
    return output.getvalue(),'.png',{'source_rectangle':offsets,'source_size':[width,height],
        'pixel_bbox':list(box),'pixel_boundary_policy':'nearest_source_pixel',
        'source_sha256':hashlib.sha256(data).hexdigest(),'resampled':False}
