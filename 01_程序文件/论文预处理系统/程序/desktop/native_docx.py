"""Read native OOXML cells and document order. Never infer cells from whitespace."""
from __future__ import annotations

import posixpath
import io
import re
import zipfile
from xml.etree import ElementTree as ET
from urllib.parse import unquote, urlsplit

W = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
M = '{http://schemas.openxmlformats.org/officeDocument/2006/math}'
R = '{http://schemas.openxmlformats.org/officeDocument/2006/relationships}'
A = '{http://schemas.openxmlformats.org/drawingml/2006/main}'
MC = '{http://schemas.openxmlformats.org/markup-compatibility/2006}'


def select_compatibility(root, namespaces):
    supported={W[1:-1],A[1:-1],M[1:-1],
        'http://schemas.microsoft.com/office/word/2010/wordprocessingShape',
        'http://schemas.microsoft.com/office/word/2010/wordprocessingGroup',
        'http://schemas.microsoft.com/office/word/2010/wordprocessingDrawing',
        'http://schemas.microsoft.com/office/word/2010/wordml'}
    for node in root.iter(MC+'AlternateContent'):
        chosen=next((c for c in node.findall(MC+'Choice') if c.get('Requires')
            and all(namespaces.get(p) in supported for p in c.get('Requires').split())),None)
        if chosen is None:chosen=node.find(MC+'Fallback')
        if chosen is None:
            # Leave one unknown representation visible to the scoped object handler.
            chosen=next(iter(node),None)
        for child in list(node):
            if child is not chosen:node.remove(child)


def math_text(node):
    tag = node.tag.rsplit('}',1)[-1]
    children = lambda: ''.join(math_text(c) for c in node)
    part = lambda name: ''.join(math_text(c) for c in node if c.tag == M+name)
    if tag == 't': return node.text or ''
    if tag.endswith('Pr'): return ''
    if tag == 'f': return r'\frac{' + part('num') + '}{' + part('den') + '}'
    if tag == 'sSup': return '{'+part('e')+'}^{'+part('sup')+'}'
    if tag == 'sSub': return '{'+part('e')+'}_{'+part('sub')+'}'
    if tag == 'sSubSup': return '{'+part('e')+'}_{'+part('sub')+'}^{'+part('sup')+'}'
    if tag == 'rad': return r'\sqrt['+part('deg')+']{'+part('e')+'}' if part('deg') else r'\sqrt{'+part('e')+'}'
    if tag == 'd': return '('+part('e')+')'
    return children()


def math_needs_review(node, *, control_properties=False):
    # Run font/size/style properties contain no formula operands. Other math
    # properties can change notation (e.g. delimiters), so retain their review.
    if node.tag==W+'rPr':
        safe={W+name for name in ('rFonts','color','kern','sz','szCs','lang','noProof')}
        # Italic on an OMML control character does not change the operands.
        # Keep operand typography and unhandled notation under review.
        if control_properties:safe.add(W+'i')
        return any(child.tag not in safe or len(child)>0 for child in node)
    if node.tag==M+'rPr':
        return any(child.tag!=M+'sty' or child.get(M+'val') not in {'p','i'} or len(child)>0 for child in node)
    if node.tag==M+'ctrlPr':
        return any(math_needs_review(child,control_properties=True) for child in node)
    tag=node.tag.rsplit('}',1)[-1]
    supported={'oMath','oMathPara','r','t','f','num','den','e','sSup','sup','sSub','sub','sSubSup','rad','deg','d','ctrlPr'}
    if tag not in supported and not tag.endswith('Pr'):
        return True
    return any(math_needs_review(child) for child in node)


def parse_docx(source, redact, resource, issue):
    blocks, images, counters = [], [], {}
    report_issue=issue;pending_issues=[]
    def issue(code,message,position):pending_issues.append((code,message,position))
    with zipfile.ZipFile(source) as archive:
        xml=archive.read('word/document.xml')
        root = ET.fromstring(xml)
        namespaces=dict(n for _,n in ET.iterparse(io.BytesIO(xml),events=['start-ns']))
        select_compatibility(root,namespaces)
        parents={child:parent for parent in root.iter() for child in parent}
        relationships={};members=set(archive.namelist())
        def related_part(position, rid):
            # Relationship IDs are scoped to their owning OPC part, not the document.
            part=position['part']
            if part not in relationships:
                resolved={};folder,name=posixpath.split(part)
                relpath=posixpath.join(folder,'_rels',name+'.rels')
                if relpath in members:
                    for rel in ET.fromstring(archive.read(relpath)):
                        if rel.get('TargetMode','').lower()=='external':continue
                        raw=rel.get('Target','')
                        uri=urlsplit(raw)
                        if uri.scheme or uri.netloc or uri.query or uri.fragment:continue
                        target=unquote(uri.path)
                        if not target or target.startswith('//') or '\\' in target or ':' in target or any(ord(c)<32 for c in target):continue
                        target=posixpath.normpath(target[1:] if target.startswith('/') else posixpath.join(folder,target))
                        if target in {'.','..'} or target.startswith('../'):continue
                        if target in members:resolved[rel.get('Id')]=target
                relationships[part]=resolved
            return relationships[part].get(rid)
        styles = {};style_nodes={}
        if 'word/styles.xml' in archive.namelist():
            for style in ET.fromstring(archive.read('word/styles.xml')):
                style_nodes[style.get(W+'styleId')]=style
                level = style.find('.//'+W+'outlineLvl')
                if level is not None:
                    styles[style.get(W+'styleId')] = min(6,int(level.get(W+'val','0'))+1)
        numbering = {}
        if 'word/numbering.xml' in archive.namelist():
            nr = ET.fromstring(archive.read('word/numbering.xml'))
            abstracts = {n.get(W+'abstractNumId'):n for n in nr.findall(W+'abstractNum')}
            for n in nr.findall(W+'num'):
                ref=n.find(W+'abstractNumId')
                if ref is not None and ref.get(W+'val') in abstracts:
                    numbering[n.get(W+'numId')] = abstracts[ref.get(W+'val')]

        object_count=0;objects={};emitted=set();unsupported_objects=set();small_picture_paragraphs={};small_picture_sizes={};small_picture_nodes={}
        def connector_only(node):
            """Ignore only an unlabelled line with no directional meaning."""
            if any(n.tag.endswith('}chart') or n.tag in {A+'blip',W+'txbxContent'} or n.tag.endswith('}imagedata') for n in node.iter()):
                return False
            if any(n.tag==W+'t' and (n.text or '').strip() for n in node.iter()):return False
            if any(n.tag in {A+'headEnd',A+'tailEnd'} and n.get('type','none')!='none'
                   for n in node.iter()):return False
            # An adjacent figure caption protects even a text-free connector.
            # Follow paragraph ownership so table-cell drawings use their own
            # neighbours rather than borrowing captions from the document body.
            paragraph=node
            while paragraph in parents and paragraph.tag!=W+'p':paragraph=parents[paragraph]
            if paragraph.tag==W+'p' and paragraph in parents:
                def research_drawing(paragraph):
                    return any(drawing is not node and
                        any(n.tag in {A+'blip',W+'txbxContent'} or n.tag.endswith(('}chart','}imagedata'))
                            for n in drawing.iter())
                        for drawing in paragraph.iter() if drawing.tag in {W+'drawing',W+'pict',W+'object'})
                if research_drawing(paragraph):return False
                siblings=list(parents[paragraph]);index=siblings.index(paragraph)
                for direction in (-1,1):
                    for distance in (1,2):
                        at=index+direction*distance
                        if not 0<=at<len(siblings) or siblings[at].tag!=W+'p':break
                        if research_drawing(siblings[at]):return False
                        text=''.join(t.text or '' for t in siblings[at].iter(W+'t')).strip()
                        if re.match(r'^(?:图\s*[\d一二三四五六七八九十]|Fig(?:ure)?[.\s]*\d)',text,re.I):return False
                        if text:break
            geometries=[n.get('prst','') for n in node.iter() if n.tag==A+'prstGeom']
            connector_tags=any(n.tag.rsplit('}',1)[-1]=='cxnSp' for n in node.iter())
            return bool(geometries or connector_tags) and all(
                not value or value=='line' or 'connector' in value.lower() for value in geometries)
        def picture_needs_page(node,position):
            from native_picture import UnsupportedPicture,displayed_picture
            for picture in node.iter():
                if picture.tag!=A+'blip' and not picture.tag.endswith('}imagedata'):
                    continue
                target=related_part(position,picture.get(R+'embed') or picture.get(R+'id'))
                if not target or target not in members:continue
                try:
                    displayed_picture(archive.read(target),posixpath.splitext(target)[1],picture,parents)
                except UnsupportedPicture:
                    return True
            return False
        def inline(node, position, inside_object=False):
            nonlocal object_count
            parts=[]
            for child in node:
                if child.tag in {M+'oMath',M+'oMathPara'}:
                    formula = redact(math_text(child))
                    parts.append('$'+formula+'$')
                    # Preserve the native equation tree, but clean every visible text node.
                    clone=ET.fromstring(ET.tostring(child))
                    for t in clone.iter():
                        if t.text: t.text=redact(t.text)
                    equations.append({'latex':formula,'omml':ET.tostring(clone,encoding='unicode'),'position':position})
                    if math_needs_review(child):
                        issue('formula_review','公式保留原生 OMML，显示形式需对照原文核对',position)
                elif child.tag in {W+'pPr',W+'rPr'}: continue  # Formatting properties are not body characters.
                elif child.tag == W+'t': parts.append(child.text or '')
                elif child.tag == W+'tab': parts.append('\t')
                elif child.tag in {W+'br',W+'cr'}: parts.append('\n')
                elif child.tag in {W+'footnoteReference',W+'endnoteReference'}:
                    parts.append('[^'+child.tag.rsplit('}',1)[-1]+child.get(W+'id','')+']')
                elif child.tag == A+'blip' or child.tag.endswith('}imagedata'):
                    if inside_object and child.tag.endswith('}imagedata') and not any(
                            child.get(k) for k in [R+'embed',R+'id',R+'link','src','{urn:schemas-microsoft-com:office:office}relid']):
                        continue  # Empty VML fill metadata is not an image reference.
                    target=related_part(position,child.get(R+'embed') or child.get(R+'id'))
                    if target and target in archive.namelist():
                        rid=f'image_{len(images)+1:04d}'
                        from native_picture import UnsupportedPicture,displayed_picture
                        try:
                            data,extension,crop=displayed_picture(archive.read(target),posixpath.splitext(target)[1],child,parents)
                        except UnsupportedPicture:
                            if not inside_object:raise
                            unsupported_objects.add(position.get('object_ref'))
                            continue  # The enclosing object is recovered from its rendered source page.
                        res=resource(rid,data,extension, 'image',position)
                        # A media part name can itself contain an identity. The
                        # source hash and anonymous resource ID retain provenance.
                        if crop:res['display_crop']=crop
                        images.append({'id':rid,'type':'image','position':position,'resource_id':res['id'],
                                       'privacy_status':'pending','ocr':{'status':'unavailable','text':'','verified':False},
                                       'source':'original','caption':'','inline':True})
                        if crop:images[-1]['display_crop']=res['display_crop']
                        if not inside_object and not crop and extension.lower() in {'.png','.jpg','.jpeg','.bmp'}:
                            # Table statistics can store one bar as adjacent coloured
                            # and unfilled strips. They have no standalone meaning;
                            # retain the bytes until the complete source table is recovered.
                            try:
                                from PIL import Image
                                with Image.open(io.BytesIO(data)) as picture:
                                    width,height=picture.size
                                if 1<=width<=256 and 1<=height<=24:
                                    owner=child
                                    while owner in parents and owner.tag!=W+'p':owner=parents[owner]
                                    if owner.tag==W+'p':
                                        small_picture_paragraphs[rid]=owner;small_picture_sizes[rid]=(width,height);small_picture_nodes[rid]=child
                            except (OSError,ValueError):pass
                        parts.append('[[IMAGE:'+rid+']]')
                    else:
                        issue('image_missing','图片为外部链接或资源缺失，未访问外部地址',position)
                elif child.tag == W+'del':
                    issue('tracked_changes','文档含修订删除内容，需确认采用哪个修订版本',position)
                elif child.tag in {W+'drawing',W+'pict'} and any(n.tag==A+'blip' or
                        (n.tag.endswith('}imagedata') and (n.get(R+'id') or n.get(R+'embed'))) for n in child.iter()) and not any(
                            n.tag==W+'txbxContent' or n.tag.rsplit('}',1)[-1] in {'wgp','wsp','sp','cxnSp','group'} for n in child.iter()) and (
                            inside_object or not picture_needs_page(child,position)):
                    # A VML container with an image relationship is a supported picture.
                    parts.append(inline(child,position,inside_object))
                elif not inside_object and (child.tag.endswith('}chart') or child.tag in {W+'drawing',W+'object',W+'pict',W+'txbxContent'}):
                    if connector_only(child):
                        continue
                    object_count+=1
                    object_position={**position,'object_ref':f'object_{object_count:04d}'}
                    charts=[n for n in child.iter() if n.tag.endswith('}chart')]
                    native=[]
                    for chart in charts:
                        try:
                            from native_chart import chart_table
                            part=related_part(object_position,chart.get(R+'id'))
                            if part is None:raise KeyError(chart.get(R+'id'))
                            native.append(chart_table(archive,part,object_position,redact))
                        except (KeyError,ValueError,ET.ParseError):native=[];break
                    code='native_chart_visual' if native and len(native)==len(charts) else 'embedded_object_review'
                    issue(code,'原生数据已保留，需保留原图证据' if code=='native_chart_visual' else
                        '嵌入图形需按完整对象恢复；原生文字保留，不推断图形关系',object_position)
                    text=inline(child,object_position,True)
                    locators=[redact(''.join(t.text or '' for t in p.iter(W+'t'))) for p in child.iter(W+'p')]
                    if native:locators.extend(s['name'] for table in native for s in table['native_chart']['series'])
                    obj={'type':'paragraph','text':redact(text) or '[解析说明：此位置的原文图形需定向识别。]',
                        'position':object_position,'heading_level':0,'locator_texts':[t for t in dict.fromkeys(locators) if t],
                        'graphic_object':True,'native_data_available':bool(native),
                        'floating_graphic':not native and (child.tag==W+'pict' or any(n.tag.endswith('}anchor') for n in child.iter()))}
                    # A single extracted image only covers the whole object when
                    # there are no other pictures, shapes or opaque overlays.
                    visual_names={'pic','shape','rect','oval','roundrect','line','group',
                                  'sp','wsp','grpSp','wgp','graphicFrame','cxnSp'}
                    standalone_picture=(sum(n.tag.rsplit('}',1)[-1] in visual_names for n in child.iter())==1 and
                        sum(n.tag==A+'blip' or n.tag.endswith('}imagedata') for n in child.iter())==1 and
                        not any(n.tag in {'{urn:schemas-microsoft-com:vml}fill',A+'solidFill'}
                                for n in child.iter()))
                    obj['preview_covers_object']=(child.tag==W+'object' and
                        object_position['object_ref'] not in unsupported_objects and
                        standalone_picture and
                        len(re.findall(r'\[\[IMAGE:[^]]+\]\]',text))==1 and
                        not re.sub(r'\[\[IMAGE:[^]]+\]\]','',text).strip() and
                        not any(n.tag==W+'txbxContent' for n in child.iter()))
                    objects[object_position['object_ref']]=[obj]+native
                    parts.append(text if 'row' in position else '[[OBJECT:'+object_position['object_ref']+']]')
                elif inside_object and child.tag==W+'p':parts.append(inline(child,position,True)+'\n')
                else: parts.append(inline(child,position,inside_object))
            return ''.join(parts)

        def paragraph(node,position):
            text=redact(inline(node,position))
            style=node.find('./'+W+'pPr/'+W+'pStyle')
            style_id=style.get(W+'val','') if style is not None else ''
            level_node=node.find('./'+W+'pPr/'+W+'outlineLvl')
            level=int(level_node.get(W+'val'))+1 if level_node is not None else styles.get(style_id,0)
            block={'type':'paragraph','text':text,'position':position,'style':style_id,'heading_level':min(6,level)}
            page_before=node.find('./'+W+'pPr/'+W+'pageBreakBefore')
            visited_page_styles=set();page_style=style_id
            while page_before is None and page_style in style_nodes and page_style not in visited_page_styles:
                visited_page_styles.add(page_style);style=style_nodes[page_style]
                page_before=style.find('./'+W+'pPr/'+W+'pageBreakBefore')
                based=style.find(W+'basedOn');page_style=based.get(W+'val') if based is not None else None
            if any(n.get(W+'type')=='page' for n in node.iter(W+'br')) or (
                    page_before is not None and page_before.get(W+'val','true').lower() not in {'0','false','off'}):
                block['explicit_page_break']=True
            num=node.find('./'+W+'pPr/'+W+'numPr')
            visited=set();parent_style=style_id
            while num is None and parent_style in style_nodes and parent_style not in visited:
                visited.add(parent_style);style=style_nodes[parent_style]
                num=style.find('./'+W+'pPr/'+W+'numPr')
                based=style.find(W+'basedOn');parent_style=based.get(W+'val') if based is not None else None
            if num is not None:
                nid=num.find(W+'numId');lvl=num.find(W+'ilvl')
                nid=nid.get(W+'val','') if nid is not None else ''
                ilvl=int(lvl.get(W+'val','0')) if lvl is not None else 0
                key=(nid,ilvl);counters[key]=counters.get(key,0)+1
                definition=numbering.get(nid)
                definition=next((l for l in definition.findall(W+'lvl') if l.get(W+'ilvl')==str(ilvl)),None) if definition is not None else None
                fmt=definition.find(W+'numFmt') if definition is not None else None
                template=definition.find(W+'lvlText') if definition is not None else None
                start=definition.find(W+'start') if definition is not None else None
                block['list']={'id':nid,'level':ilvl,'format':fmt.get(W+'val') if fmt is not None else 'unknown',
                               'template':template.get(W+'val') if template is not None else '',
                               'value':counters[key]+(int(start.get(W+'val','1'))-1 if start is not None else 0)}
                if block['list']['format'] not in {'decimal','bullet'}:
                    issue('list_numbering_review','列表编号样式保存在结构记录中，显示编号需核对',position)
            return block

        def table(node,position):
            nonlocal object_count
            cells=[];grid=[];active={};header_rows=[];fragment_members=[];table_image_start=len(images);table_equation_start=len(equations)
            cols=len(node.findall('./'+W+'tblGrid/'+W+'gridCol'))
            for ri,row in enumerate(node.findall(W+'tr')):
                if row.find('./'+W+'trPr/'+W+'tblHeader') is not None: header_rows.append(ri)
                before=row.find('./'+W+'trPr/'+W+'gridBefore')
                ci=int(before.get(W+'val','0')) if before is not None else 0
                rowgrid=[None]*ci;new_active={}
                for tc in row.findall(W+'tc'):
                    span=tc.find('./'+W+'tcPr/'+W+'gridSpan')
                    span=int(span.get(W+'val','1')) if span is not None else 1
                    vm=tc.find('./'+W+'tcPr/'+W+'vMerge')
                    continuation=vm is not None and vm.get(W+'val','continue')=='continue'
                    image_start=len(images)
                    content='\n'.join(paragraph(p,{**position,'row':ri,'column':ci})['text'] for p in tc.findall(W+'p'))
                    cell_images=images[image_start:]
                    owners=[small_picture_paragraphs.get(im['id']) for im in cell_images]
                    sizes=[small_picture_sizes.get(im['id']) for im in cell_images]
                    if (len(owners)>=2 and owners[0] is not None and all(owner is owners[0] for owner in owners)
                            and max(size[1] for size in sizes)-min(size[1] for size in sizes)<=1
                            and any(width>=height*3 for width,height in sizes)):
                        members={small_picture_nodes[im['id']] for im in cell_images}
                        sequence=[n for n in owners[0].iter() if n in members or
                            (n.tag==W+'t' and (n.text or '').strip()) or
                            n.tag in {W+'tab',W+'br',W+'cr',M+'oMath',M+'oMathPara'} or
                            n.tag.endswith('}chart')]
                        indices=[index for index,n in enumerate(sequence) if n in members]
                        if max(indices)-min(indices)+1==len(indices):fragment_members.extend(cell_images)
                    if tc.find(W+'tbl') is not None:
                        issue('nested_table','嵌套表格需检查；结构另存为 XML',position)
                    if continuation and ci in active:
                        idx=active[ci];cells[idx]['rowspan']+=1
                        if content: issue('merged_cell_content','纵向合并续格含额外文字，需核对',position)
                        if content: cells[idx].setdefault('continuation_text',[]).append({'row':ri,'text':content})
                    else:
                        idx=len(cells)
                        cells.append({'row':ri,'column':ci,'rowspan':1,'colspan':span,'text':content,'position':{**position,'row':ri,'column':ci}})
                        if continuation: issue('merge_without_start','合并单元格缺少起始格',position)
                    rowgrid.extend([idx]*span)
                    if vm is not None:
                        for col in range(ci,ci+span): new_active[col]=idx
                    ci+=span
                active=new_active;cols=max(cols,len(rowgrid));grid.append(rowgrid)
            for row in grid: row.extend([None]*(cols-len(row)))
            complex_table=any(c['colspan']>1 or c['rowspan']>1 for c in cells)
            if not header_rows:
                header_rows=list(range(min(len(grid),max([c['rowspan'] for c in cells if c['row']==0] or [1]))))
                if complex_table and len(header_rows)==1 and len(grid)>1 and any(c['row']==0 and c['colspan']>1 for c in cells): header_rows=[0,1]
                issue('table_header_inferred','原文件未标注重复表头，导出的表头角色需要人工核对',position)
            result={'type':'table','position':position,'cells':cells,'grid':grid,'rows':len(grid),'columns':cols,
                    'complex':complex_table,'header_rows':header_rows,'header_verified':False,'source':'native_ooxml',
                    'native_xml':redacted_xml(node,redact),'caption':'','context_before':[],'context_after':[]}
            floating_picture=any(n.tag=='{http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing}anchor'
                or (n.tag.startswith('{urn:schemas-microsoft-com:vml}') and
                    re.search(r'(?:^|;)\s*position\s*:\s*(?:absolute|fixed)\s*(?:;|$)',n.get('style',''),re.I))
                for n in node.iter())
            if fragment_members and not floating_picture:
                object_count+=1
                object_position={**position,'object_ref':f'object_{object_count:04d}'}
                # Keep the owner's native position unchanged: visit() uses it
                # to emit other drawings/chart caches inside this same table.
                result.update(position=object_position,graphic_object=True,native_data_available=True,table_picture_fragments=True)
                for item in images[table_image_start:]+equations[table_equation_start:]:
                    if not item['position'].get('object_ref'):
                        # The replacement covers the whole table, including
                        # plain pictures/native equations. Keep their evidence
                        # attached without overwriting other embedded objects.
                        item['position']['object_ref']=object_position['object_ref']
                issue('embedded_object_review','表格内相邻位图片段需对照完整原表恢复；原生单元格和数字已保留',object_position)
            return result

        equations=[]
        body=root.find(W+'body')
        def visit(node,pos):
            if node.tag==W+'p':
                block=paragraph(node,pos)
                pieces=re.split(r'(\[\[OBJECT:object_\d+\]\])',block['text'])
                first_piece=True
                internal_break=any(n.get(W+'type')=='page' for n in node.iter(W+'br'))
                for piece in pieces:
                    match=re.fullmatch(r'\[\[OBJECT:(object_\d+)\]\]',piece)
                    if match:
                        if block.get('explicit_page_break') and (first_piece or internal_break):
                            objects[match[1]][0]['explicit_page_break']=True
                        blocks.extend(objects[match[1]]);emitted.add(match[1])
                        first_piece=False
                    elif piece or len(pieces)==1:
                        part={**block,'text':piece}
                        if not first_piece and not internal_break:part.pop('explicit_page_break',None)
                        blocks.append(part);first_piece=False
            elif node.tag==W+'tbl':
                blocks.append(table(node,pos))
                for key,value in objects.items():
                    if key not in emitted and all(value[0]['position'].get(k)==v for k,v in pos.items()):
                        # Cell text stays in its native grid; this anchor supplies visual correspondence.
                        blocks.append({**value[0],'text':'[解析说明：表格内图形需定向识别。]'})
                        blocks.extend(value[1:]);emitted.add(key)
            elif node.tag in {W+'customXmlInsRangeStart',W+'customXmlInsRangeEnd',W+'customXmlDelRangeStart',W+'customXmlDelRangeEnd',
                              W+'customXmlMoveFromRangeStart',W+'customXmlMoveFromRangeEnd',W+'customXmlMoveToRangeStart',W+'customXmlMoveToRangeEnd'} and not len(node) and not (node.text or '').strip():
                return  # Empty revision boundaries contain no visible body object.
            elif node.tag in {W+'sectPr',W+'sdtPr',W+'sdtEndPr',W+'bookmarkStart',W+'bookmarkEnd',W+'proofErr',W+'commentRangeStart',W+'commentRangeEnd'}: return
            elif node.tag in {W+'sdt',W+'sdtContent'}:
                # Supported content controls (including a TOC) keep normal block order.
                for child_index,sub in enumerate(node):
                    visit(sub,{**pos,'container_path':pos.get('container_path',[])+[child_index]})
            else:
                issue('body_container','正文含内容控件或非普通块，需核对原始顺序',pos)
                before=len(blocks)
                for child_index,sub in enumerate(node):
                    visit(sub,{**pos,'container_path':pos.get('container_path',[])+[child_index]})
                if len(blocks)==before:
                    blocks.append({'type':'paragraph','text':'[解析说明：此位置存在未解析的正文对象。]',
                                   'position':pos,'heading_level':0,'unresolved_object':True})
        for index,node in enumerate(body if body is not None else []):
            visit(node,{'part':'word/document.xml','body_index':index})
        for part,kind in [('word/footnotes.xml','footnoteReference'),('word/endnotes.xml','endnoteReference')]:
            if part in archive.namelist():
                note_xml=archive.read(part)
                note_root=ET.fromstring(note_xml)
                select_compatibility(note_root,dict(n for _,n in ET.iterparse(io.BytesIO(note_xml),events=['start-ns'])))
                parents.update({child:parent for parent in note_root.iter() for child in parent})
                for note in note_root:
                    nid=note.get(W+'id','')
                    # Word/WPS may use zero for a real note. Separators are
                    # identified by their semantic type, not their numeric ID.
                    if note.get(W+'type','normal') in {'separator','continuationSeparator','continuationNotice'}:
                        continue
                    if not re.fullmatch(r'-?\d+',nid):
                        issue('note_id_invalid','注释编号缺失或无效',{'part':part,'id':nid})
                        continue
                    position={'part':part,'id':nid};start=len(blocks)
                    children=list(note)
                    for child_index,child in enumerate(children):
                        child_position=position if len(children)==1 else {**position,'block_index':child_index}
                        visit(child,child_position)
                    note_blocks=blocks[start:]
                    # Keep each original block position. Collapsing paragraphs
                    # would detach their inline equations and images from the
                    # exported evidence and could mislocate a multi-page note.
                    label=kind+nid
                    for block in note_blocks:
                        block['note_label']=label
                    if not note_blocks or note_blocks[0]['type']!='paragraph':
                        note_blocks.insert(0,{'type':'paragraph','text':'','position':position,
                                              'heading_level':0,'note_label':label})
                    note_blocks[0]['text']=f'[^{label}]: '+note_blocks[0]['text']
                    note_blocks[0]['heading_level']=0
                    blocks[start:]=note_blocks
    # Floating connectors can belong to an inline drawing in the same region.
    # A region is not asserted to be one figure: recovery retains every actual
    # figure and all member-page evidence. Captions/prose/page breaks bound it.
    def region_member(block):
        return (block.get('graphic_object') and not block.get('native_data_available')
                and not block.get('preview_covers_object'))
    remap={};merged=[];at=0
    while at<len(blocks):
        first=blocks[at]
        if not region_member(first):
            merged.append(first);at+=1;continue
        end=at+1;members=[first]
        while end<len(blocks) and end-at<=240:
            block=blocks[end]
            if block.get('explicit_page_break'):break
            if region_member(block):members.append(block)
            elif block['type']!='paragraph' or block.get('text','').strip() or block.get('explicit_page_break'):break
            end+=1
        if len(members)>1 and any(b.get('floating_graphic') for b in members):
            first={**first,'text':'\n'.join(b['text'] for b in members),
                'locator_texts':list(dict.fromkeys(t for b in members for t in b.get('locator_texts',[]))),
                'graphic_members':[{'text':b['text'],'locator_texts':list(b.get('locator_texts',[])),
                                    'position':dict(b['position']),'floating_graphic':b.get('floating_graphic',False)} for b in members],
                'member_positions':[dict(b['position']) for b in members],'preview_covers_object':False,'graphic_region':True}
            for b in members:remap[b['position']['object_ref']]=dict(first['position'])
            merged.append(first)
            merged.extend(b for b in blocks[at+1:end] if not region_member(b))
        else:merged.extend(blocks[at:end])
        at=end
    seen=set()
    original_positions={id(item['position']):dict(item['position']) for item in images+equations}
    for item in images+equations:
        position=item['position'];original=original_positions[id(position)];target=remap.get(original.get('object_ref'))
        if target:
            item['original_position']=original
            # The resource callback shares this position, so retain both its membership and the image link.
            position.clear();position.update(target)
    for code,message,position in pending_issues:
        if code=='embedded_object_review' and position.get('object_ref') in remap:
            position=remap[position['object_ref']]
            key=position['object_ref']
            if key in seen:continue
            seen.add(key)
        report_issue(code,message,position)
    return merged,images,equations


def redacted_xml(node,redact):
    clone=ET.fromstring(ET.tostring(node))
    for item in clone.iter():
        if item.text: item.text=redact(item.text)
        if item.tail: item.tail=redact(item.tail)
    return ET.tostring(clone,encoding='unicode')
