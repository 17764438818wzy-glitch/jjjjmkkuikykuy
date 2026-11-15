"""One source representation, two exports; unchecked media stays local and quarantined."""
from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
import shutil
import sys
from pathlib import Path

from content_rules import ACK, ACK_TOC, CHAPTER, FRONT_END, NEXT_SECTION
from native_docx import parse_docx

VERSION='evidence-v2.5-native-note-integrity'
HANDOFF_BEGIN='[PAPER_PIPELINE_METADATA_V1]'
HANDOFF_END='[/PAPER_PIPELINE_METADATA_V1]'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def json_write(path,data):
    tmp=path.with_suffix(path.suffix+'.tmp')
    tmp.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
    tmp.replace(path)


def ragflow_handoff_header(meta, *, canonical_paper_id=False):
    """Return the small trusted pre-processing contract carried with ingest.md."""
    from knowledge_package import paper_identity
    classification=meta.get('classification')
    if not isinstance(classification,dict) or not meta.get('id') or not meta.get('major'):
        return ''
    safe_classification={key:classification[key] for key in ['major','method','source','confidence']
                         if isinstance(classification.get(key),str) and classification[key].strip()}
    safe_classification['major']=meta['major']
    payload={'schema':'paper-handoff/1.0',
             'paper_id':paper_identity(meta)[1] if canonical_paper_id else meta['id'],
             'document_id':meta['id'],'stable_paper_id':paper_identity(meta)[1],
             'major':meta['major'],
             'classification':safe_classification,
             'preprocessing_complete':bool(meta.get('preprocessing_complete')),
             'privacy_status':meta.get('privacy_status','')}
    if 'source_education' in meta:
        from source_education import source_education_fields
        payload.update(source_education_fields(meta['source_education']))
    return HANDOFF_BEGIN+'\n'+json.dumps(payload,ensure_ascii=False,separators=(',',':'))+'\n'+HANDOFF_END


def redactor():
    legacy=r'E:\dity\脱敏分类脚本\脱敏分类脚本\cleaning-pipeline'
    if legacy not in sys.path: sys.path.insert(0,legacy)
    rules=importlib.import_module('src.paper_cleaning.sanitize')
    fields=[('STUDENT_ID_RE','[STUDENT_ID]'),('STUDENT_NAME_FIELD_RE','[NAME]'),('TEACHER_FIELD_RE','[TEACHER]')]
    space=r'[^\S\r\n]*'
    empty_class_values=(
        r'姓名',r'数量(?:[/／]节|[（(]人[）)])',
        rf'\|{space}学历{space}\|{space}教龄{space}\|{space}专业{space}\|?',
        rf'授课教师[：:]{space}授课内容[：:]{space}观察时间[：:]{space}'
        rf'年{space}月{space}日{space}课堂性质[：:]',
    )
    def clean(text):
        for name,token in fields: text=rules.replace_field(getattr(rules,name),text,token)[0]
        def class_field(match):
            # OCR can join empty neighbouring fields or statistic column heads.
            # Exempt only a complete known label sequence, never a populated value.
            if any(re.fullmatch(pattern,match.group('value').strip()) for pattern in empty_class_values):return match[0]
            return match.group('label')+match.group('sep')+'[CLASS]'
        text=rules.CLASS_FIELD_RE.sub(class_field,text)
        def contact(match):
            # Charts commonly pair the platform label with its percentage.
            # Only an explicit percentage unit proves this is a statistic;
            # bare numbers and every other account shape retain strict masking.
            value=match.group('value')
            number=r'[+\-−]?(?:\d+(?:\.\d+)?|\.\d+)'
            percent=number+r'[%％]'
            if re.fullmatch(percent+r'(?:、'+percent+r')*',value):return match[0]
            if (re.fullmatch(number,value) and
                    re.match(r'[^\S\r\n]+[%％](?![\w%％])',text[match.end():])):
                return match[0]
            return match.group('label')+match.group('sep')+'[CONTACT]'
        text=rules.CONTACT_FIELD_RE.sub(contact,text)
        text=rules.EMAIL_RE.sub('[EMAIL]',text)
        text=rules.ID_CARD_RE.sub('[ID_CARD]',text)
        # Numeric cells and explicitly denominated monetary values are research
        # data. Match context at each occurrence: an identical phone number in
        # an explicit contact field must still be removed.
        if not re.fullmatch(r'[\s\d.,%％()（）+\-−—–]+',text):
            def phone(match):
                before=text[max(0,match.start()-40):match.start()]
                after=text[match.end():match.end()+24]
                contact=bool(re.search(r'(?:手机|电话|联系方式|微信|phone|tel(?:ephone)?|mobile)\s*[:：]?\s*$',before,re.I))
                currency=(re.match(r'\s*(?:亿?万元|亿元|元|美元|欧元|港元|日元|人民币|yuan\b|dollars?\b|CNY\b|RMB\b|USD\b)',after,re.I)
                          or re.search(r'(?:[￥¥$€]|\b(?:CNY|RMB|USD))\s*$',before,re.I))
                return match[0] if currency and not contact else '[PHONE]'
            text=rules.PHONE_RE.sub(phone,text)
        return text
    return clean


def _pdf_image_offpage(block,page):
    """Only ignore images whose real placement is wholly outside the page.

    Some report PDFs have clipped, reversed image bboxes. The original affine
    transform, not that damaged bbox, must prove that nothing is visible.
    """
    import fitz
    import math
    transform=block.get('transform')
    if not isinstance(transform,(list,tuple)) or len(transform)!=6:return False
    if not all(isinstance(n,(int,float)) and math.isfinite(n) for n in transform):return False
    placed=fitz.Rect(0,0,1,1)*fitz.Matrix(*transform)
    if placed.is_empty or placed.is_infinite:return False
    visible=page.rect*page.derotation_matrix
    return (placed & visible).is_empty


def _pdf_image_payload(block,page):
    """Keep the PDF's actual mask, placement and backdrop in a local image region."""
    data=block['image'];extension='.'+block.get('ext','png')
    if not block.get('mask'):
        return data,extension
    import fitz
    import math
    bounds=fitz.Rect(block['bbox'])
    from processing_errors import LocalDocumentError
    def geometry_error():
        return LocalDocumentError('pdf_image_geometry',
            f'PDF 第 {page.number+1} 页图片位置或变换无效，无法确认可见内容；未忽略该图片，请核对原页后重试',
            {'page':page.number+1,'bbox':list(bounds)})
    if bounds.is_empty or bounds.is_infinite:
        raise geometry_error()
    a,b,c,d,_,_=block['transform']
    horizontal=math.hypot(a,b);vertical=math.hypot(c,d)
    if not horizontal or not vertical:
        raise geometry_error()
    scale=max(block['width']/horizontal,block['height']/vertical)
    # PDF masks can have another size, premultiplied colors or a colored
    # backdrop. Let the PDF renderer compose these at native pixel density.
    image=page.get_pixmap(clip=bounds*page.rotation_matrix,matrix=fitz.Matrix(scale,scale),alpha=False)
    return image.tobytes('png'),'.png'


def parse_pdf(source,redact,resource,issue):
    import fitz
    from pdf_layout import adjacent_text_regions, cell_border_rules, continuation_fragments, repeated_margin_rules, three_line_tables, vector_regions
    blocks=[];images=[]
    with fitz.open(source) as pdf:
        drawings_by_page=[page.get_drawings() for page in pdf]
        margin_rules=repeated_margin_rules(pdf,drawings_by_page)
        for page_index,page in enumerate(pdf):
            drawings=[drawing for index,drawing in enumerate(drawings_by_page[page_index])
                      if (page_index,index) not in margin_rules]
            pos={'page':page_index+1}
            issue('pdf_layout_review','PDF 阅读顺序、跨页衔接、公式和截图需与页图核对',pos)
            found=page.find_tables(strategy='lines_strict').tables
            table_bounds=[];border_rules=[]
            for t in found:
                values=t.extract();cells=[];grid=[]
                for ri,row in enumerate(values):
                    indices=[]
                    for ci,value in enumerate(row):
                        indices.append(len(cells))
                        cells.append({'row':ri,'column':ci,'rowspan':1,'colspan':1,
                                      'text':redact(value) if value is not None else '',
                                      'missing_geometry':value is None,'position':{**pos,'bbox':t.rows[ri].cells[ci]}})
                    grid.append(indices)
                table={'type':'table','position':{**pos,'bbox':list(t.bbox)},'source':'pdf_native_lines','cells':cells,'grid':grid,
                       'rows':len(grid),'columns':max(map(len,grid),default=0),'complex':True,
                       'header_rows':[0],'header_verified':False,'caption':'','context_before':[],'context_after':[]}
                issue('pdf_cell_relationship_review','PDF 网格来自线条几何，合并格及多级表头尚未核对',table['position'])
                snap=resource(f'pdf_table_{page_index+1}_{len(table_bounds)+1}',page.get_pixmap(clip=t.bbox,matrix=fitz.Matrix(2,2)).tobytes('png'),'.png','table_screenshot',table['position'])
                table['screenshot_ids']=[snap['id']];table['screenshot_match_verified']=False
                blocks.append(table);table_bounds.append(fitz.Rect(t.bbox))
                border_rules.extend(cell_border_rules(t.cells))
            native_tables,unresolved_tables=three_line_tables(page,drawings,table_bounds)
            for native in native_tables:
                cells=[];grid=[]
                for ri,row in enumerate(native['values']):
                    indices=[]
                    for ci,value in enumerate(row):
                        indices.append(len(cells))
                        cells.append({'row':ri,'column':ci,'rowspan':1,'colspan':1,
                                      'text':redact(value) if value is not None else '',
                                      'missing_geometry':False,
                                      'position':{**pos,'bbox':native['cell_boxes'][ri][ci]}})
                    grid.append(indices)
                position={**pos,'bbox':native['bbox']}
                table={'type':'table','position':position,'source':'pdf_native_three_line','cells':cells,'grid':grid,
                       'rows':len(grid),'columns':len(grid[0]),'complex':False,'grid_validated':True,
                       'header_rows':[0],'header_verified':False,'caption':redact(native['caption']),
                       'context_before':[],'context_after':[],
                       'row_positions':[{**pos,'bbox':bbox} for bbox in native['row_boxes']]}
                snap=resource(f'pdf_table_{page_index+1}_{len(table_bounds)+1}',
                              page.get_pixmap(clip=native['bbox'],matrix=fitz.Matrix(2,2)).tobytes('png'),
                              '.png','table_screenshot',position)
                table['screenshot_ids']=[snap['id']];table['screenshot_match_verified']=False
                blocks.append(table);table_bounds.append(fitz.Rect(native['bbox']))
                border_rules.extend(fitz.Rect(rule) for rule in native['border_rules'])
            if page_index:
                previous=[b for b in blocks if b['type']=='table' and b['position'].get('page')==page_index]
                unresolved_tables.extend(continuation_fragments(page,drawings,previous,pdf[page_index-1].rect.height,
                    [*table_bounds,*[candidate['bbox'] for candidate in unresolved_tables]]))
            for candidate in unresolved_tables:
                target_pos={**pos,'bbox':candidate['bbox'],'vector_region':True}
                issue('pdf_vector_review','原文表格或续表片段的网格尚不可靠，需保留完整原表截图及可确定的数据',target_pos)
                blocks.append({'type':'paragraph','text':'[解析说明：此位置有待恢复的原文表格或续表片段。]',
                               'heading_level':0,'position':target_pos,'unresolved_object':True,
                               'table_candidate':True,'table_fragment':candidate['table_fragment'],
                               'caption':redact(candidate['caption']),
                               'locator_texts':[redact(text) for text in candidate['locator_texts']],
                               'possible_previous_table':candidate['possible_previous_table']})
            page_blocks=page.get_text('dict',sort=True)['blocks']
            for bi,b in enumerate(page_blocks):
                position={**pos,'bbox':list(b['bbox']),'block_index':bi}
                if b['type']==1:
                    if _pdf_image_offpage(b,page):continue
                    rid=f'pdf_image_{page_index+1}_{bi}'
                    image_data,image_extension=_pdf_image_payload(b,page)
                    res=resource(rid,image_data,image_extension,'image',position)
                    if b.get('mask'):
                        res['image_rendering']='pdf_masked_image_page_region'
                        res['source_image_sha256']=hashlib.sha256(b['image']).hexdigest()
                        res['source_mask_sha256']=hashlib.sha256(b['mask']).hexdigest()
                    image={'id':rid,'type':'image','position':position,'resource_id':res['id'],'privacy_status':'pending',
                           'ocr':{'status':'unavailable','text':'','verified':False},'source':'original','caption':'','inline':False}
                    images.append(image);blocks.append(image)
                else:
                    lines=[]
                    for line in b.get('lines',[]):
                        rect=fitz.Rect(line['bbox'])
                        if any((rect & area).get_area() > rect.get_area()*0.8 for area in table_bounds): continue
                        lines.append(redact(''.join(s.get('text','') for s in line['spans'])))
                    if lines: blocks.append({'type':'paragraph','text':'\n'.join(lines),'heading_level':0,'position':position})
            if not page.get_text().strip(): issue('scanned_page','页面无可靠文本层，需 OCR 和数值核对',pos)
            else:
                # Text/image extraction does not include vector charts and diagrams.
                # Keep a scoped target instead of silently treating the page as complete.
                remaining=[drawing for drawing in drawings if not any(
                    fitz.Rect(drawing['rect']) in (fitz.Rect(candidate['bbox'])+(-2,-2,2,2))
                    for candidate in unresolved_tables)]
                text_boxes=adjacent_text_regions(page_blocks)
                for area in vector_regions(remaining,border_rules,text_boxes=text_boxes):
                    vector_pos={**pos,'bbox':list(area),'vector_region':True}
                    issue('pdf_vector_review','页面有未被原生表格覆盖的矢量绘图，需定向识别其研究内容',vector_pos)
                    blocks.append({'type':'paragraph','text':'[解析说明：此位置有待识别的矢量绘图。]',
                                   'heading_level':0,'position':vector_pos,'unresolved_object':True})
        blocks.sort(key=lambda b:(b['position'].get('page',0),b['position'].get('bbox',[0,0])[1],b['position'].get('bbox',[0])[0]))
    return blocks,images,[]


def retained_blocks(blocks):
    start=next((i for i,b in enumerate(blocks) if b['type']=='paragraph' and FRONT_END.match(b['text'].strip())),0)
    ack_titles=[b.get('position',{}) for b in blocks[start:]
                if b['type']=='paragraph' and ACK.fullmatch(b.get('text','').strip().strip('#* '))
                and b.get('position',{}).get('page') and b['position'].get('bbox')]
    result=[];skip=False
    for b in blocks[start:]:
        # Notes live in separate OOXML parts and are appended after the body.
        # A trailing acknowledgement must not remove their research content.
        if b.get('position',{}).get('part') in {'word/footnotes.xml','word/endnotes.xml'}:
            result.append(b);continue
        pos=b.get('position',{});box=pos.get('bbox')
        # A duplicated outline of an acknowledgement heading sorts before its
        # native text because vector targets have 2pt padding. Do not let that
        # padding preserve a nonresearch heading as a missing graphic. Nearby
        # preceding research drawings and table candidates remain untouched.
        if (b.get('unresolved_object') and pos.get('vector_region') and box
                and not b.get('table_candidate') and any(
                    pos.get('page')==title['page']
                    and title['bbox'][0]-3<=box[0]<=box[2]<=title['bbox'][2]+3
                    and title['bbox'][1]-3<=box[1]<=box[3]<=title['bbox'][3]+3
                    for title in ack_titles)):
            continue
        text=b.get('text','').strip().strip('#* ')
        if b['type']=='paragraph' and ACK_TOC.fullmatch(text): continue
        if b['type']=='paragraph' and ACK.fullmatch(text): skip=True;continue
        if skip and b['type']=='paragraph' and (NEXT_SECTION.match(text) or CHAPTER.match(text)): skip=False
        if not skip: result.append(b)
    return result


def nearest_figure_caption(blocks,index):
    candidates=[]
    pattern=r'^\s*(图\s*[\d一二三四五六七八九十]|Fig(?:ure)?[.\s]*\d)'
    for direction in (1,-1):
        for distance in (1,2):
            at=index+direction*distance
            if not 0<=at<len(blocks):break
            block=blocks[at];text=block.get('text','')
            if block['type']!='paragraph' or '[[IMAGE:' in text:break
            if re.match(pattern,text,re.I):
                # Preserve the established preceding-caption preference on ties;
                # a closer following caption still outranks an older figure title.
                candidates.append((distance,direction>0,text));break
    own=blocks[index].get('text','')
    if re.match(pattern,own,re.I):candidates.append((0,False,own))
    return min(candidates)[2] if candidates else ''


def attach_context(blocks,images):
    headings=[]
    for i,b in enumerate(blocks):
        level=b.get('heading_level',0)
        if level:
            headings=headings[:level-1]+[b.get('text','')]
        b['section']=list(headings)
        if b['type']=='table':
            before=[p['text'] for p in blocks[max(0,i-2):i] if p['type']=='paragraph']
            after=[p['text'] for p in blocks[i+1:i+3] if p['type']=='paragraph']
            b['context_before']=before;b['context_after']=after
            if b.get('source')=='native_chart_cache':
                b['caption']=next((s for s in after+list(reversed(before)) if re.match(r'^\s*(图\s*\d|Fig)',s,re.I)),b['caption'])
                continue
            b['caption']=next((s for s in reversed(before) if re.match(r'^\s*(表\s*[\d一二三四五六七八九十]|Table\s*\d)',s,re.I)),'')
            b['units']=next((m.group(0) for s in before+after for m in [re.search(r'单位\s*[:：]\s*[^\n，。；;]+',s)] if m),'')
            # A preceding source note may belong to the previous table. Preserve that
            # context in JSON, but do not attribute it to this table without verification.
            b['notes']=[s for s in after if re.match(r'^\s*(数据来源|资料来源|来源|注[:：]|备注|Source)',s,re.I)]
        related=[im for im in images if all(im['position'].get(k)==v for k,v in b['position'].items())]
        if b['type']=='image': related=[b]
        for im in related:
            im['caption']=nearest_figure_caption(blocks,i)
            im['section']=list(headings)


def table_exports(b):
    cells=b['cells'];grid=b['grid'];n=b['columns'];hr=b.get('header_rows',[0])
    def value(r,c):
        idx=grid[r][c] if c<len(grid[r]) else None
        return cells[idx]['text'] if idx is not None else ''
    labels=[]
    for c in range(n):
        parts=[]
        for r in hr:
            if r<len(grid):
                text=value(r,c).replace('\n',' / ')
                if text and (not parts or text!=parts[-1]): parts.append(text)
        labels.append(' / '.join(parts) or f'第{c+1}列（原文表头未识别）')
    esc=lambda s:s.replace('|',r'\|').replace('\n','<br>')
    display=['| '+' | '.join(esc(v) for v in labels)+' |','| '+' | '.join(['---']*n)+' |']
    context='；'.join(x for x in [b.get('caption') or b['id'],b.get('units','')]+b.get('notes',[]) if x)
    state='原生行列已校验，保留原坐标' if b.get('grid_validated') else 'AI 已核对' if b.get('automatic_verified') else '未核对'
    origin='AI 转录结构' if b.get('source')=='ai_transcription' else '原图表缓存（原始数值及格式，未自动换算显示值）' if b.get('source')=='native_chart_cache' else '原生结构'
    ingest=[context, f"证据编号：{b['id']}。表头和数值对应关系：{state}；以下为{origin}的确定性展开。"]
    if b.get('grid_validated'):
        ingest.append('原表逐行记录（行列编号保留原始位置，表头候选不改变原数据）：')
        for r in range(len(grid)):
            ingest.append(f'原表第{r+1}行：'+'；'.join(f'第{c+1}列='+ (value(r,c) if value(r,c)!='' else '[原文空值]') for c in range(n))+'。')
    for r in range(len(grid)):
        if r in hr: continue
        values=[value(r,c) for c in range(n)]
        display.append('| '+' | '.join(esc(v) for v in values)+' |')
        ingest.append(context+'；'+'；'.join(label+'='+ (v if v!='' else '[原文空值]') for label,v in zip(labels,values))+'。')
    return '\n'.join(display),'\n\n'.join(ingest)


def public_crop_metadata(value):
    """Keep crop geometry and hashes, including when exporting old checkpoints."""
    if isinstance(value,list):return [public_crop_metadata(v) for v in value]
    if not isinstance(value,dict):return value
    result={}
    for key,child in value.items():
        if key!='display_crop':
            result[key]=public_crop_metadata(child)
            continue
        crop={}
        if isinstance(child,dict):
            rectangle=child.get('source_rectangle')
            if isinstance(rectangle,dict):
                crop['source_rectangle']={side:rectangle[side] for side in ('l','t','r','b')
                                          if type(rectangle.get(side)) is int}
            for field,length in (('source_size',2),('pixel_bbox',4)):
                coordinates=child.get(field)
                if isinstance(coordinates,list) and len(coordinates)==length and all(type(n) is int for n in coordinates):
                    crop[field]=list(coordinates)
            digest=child.get('source_sha256')
            if isinstance(digest,str) and re.fullmatch(r'[0-9a-fA-F]{64}',digest):crop['source_sha256']=digest
            if child.get('pixel_boundary_policy')=='nearest_source_pixel':
                crop['pixel_boundary_policy']='nearest_source_pixel'
            if type(child.get('resampled')) is bool:crop['resampled']=child['resampled']
        result[key]=crop
    return result


def export_package(package,meta,*,canonical_paper_id=None):
    from knowledge_outline import body_blocks,heading_level,heading_text
    from knowledge_package import outline,paper_identity,identifier,source_position
    if canonical_paper_id is None:
        canonical_paper_id=meta.get('category')=='教育教学类'
    original_meta=meta
    meta=public_crop_metadata(meta)
    # Visual recovery uses JSON null when an asset has no visible caption.
    # Normalize that optional field once for all exporters, preserving the input.
    blocks=[{**b,'caption':''} if b.get('caption','') is None else b for b in body_blocks(meta['blocks'])]
    meta={**meta,'blocks':blocks}
    if meta.get('preprocessing_complete') and meta.get('text_privacy_complete'):
        from note_integrity import assert_note_integrity
        assert_note_integrity(blocks)
    resources={r['id']:r for r in meta['resources']}
    figures={b['id']:b for b in meta['blocks'] if b['type']=='image'}
    ready=meta.get('preprocessing_complete') and meta.get('text_privacy_complete')
    _,paper=paper_identity(meta)
    sections,layout=outline(meta,paper) if ready else ([],[])
    first_blocks={s['block_ids'][0]:s['id'] for s in sections if s['block_ids']}
    def object_id(block,kind):
        return identifier(paper,kind,[block['id'],source_position(block.get('position',{}))]) if ready else block['id']
    display=['# '+meta.get('title','待确认题目')]
    ingest=['# '+meta.get('title','待确认题目')]
    handoff=ragflow_handoff_header(meta,canonical_paper_id=canonical_paper_id)
    if handoff:ingest.append(handoff)
    ingest.append(f"文档编号：{meta['id']}。本稿为研究资料候选；图表未核对或识别不完整时不得用于正式评价。")
    if ready:ingest.append('资料关联（程序生成）：'+paper)
    def figure(rid,reading):
        b=figures[rid];r=resources[b['resource_id']];caption=b.get('caption') or b['id']
        ocr=b.get('ocr',{})
        text=ocr.get('text','') if ocr.get('verified') else '图中文字、坐标轴、单位和数值尚未核对，不据此生成结论。'
        if b.get('description',{}).get('text'):
            text+='\n\n图像描述（generated，AI 生成）：'+b['description']['text']
        if reading:
            link=r['local_path'].replace('\\','/')
            link='!['+caption+']('+link+')' if r['privacy_status']=='approved' else '['+caption+'：待脱敏检查原图]('+link+')'
            return link+'\n\n'+text
        return f"{caption}。证据编号：{object_id(b,'figure')}。{text} 本地证据需单独检查，图片未随此文本上传。"
    for index,b in enumerate(meta['blocks']):
        section_id=first_blocks.get(layout[index]['id']) if ready else None
        if b['type']=='paragraph':
            level=heading_level(b)
            text=heading_text(b['text']) if level else b['text'];prefix='#'*level+' ' if level else ''
            # Word notes may inherit a list style, but Markdown note definitions
            # must start with their label so references still resolve.
            note_definition=re.match(r'^\[\^[^\]\r\n]+\]:',text)
            if b.get('list') and not level and not note_definition:
                l=b['list'];prefix='  '*l['level']+(str(l['value'])+'. ' if l['format']=='decimal' else '- ')
            d=re.sub(r'\[\[IMAGE:([^\]]+)\]\]',lambda m:figure(m[1],True),text)
            t=re.sub(r'\[\[IMAGE:([^\]]+)\]\]',lambda m:figure(m[1],False),text)
            display.append(prefix+d);ingest.append(prefix+t)
            if section_id:ingest.append('资料关联（程序生成）：'+section_id)
        elif b['type']=='table':
            d,t=table_exports(b)
            if ready:t=t.replace('证据编号：'+b['id']+'。','证据编号：'+object_id(b,'table')+'。')
            d=re.sub(r'\[\[IMAGE:([^\]]+)\]\]',lambda m:figure(m[1],True),d)
            t=re.sub(r'\[\[IMAGE:([^\]]+)\]\]',lambda m:figure(m[1],False),t)
            display.append(d);ingest.append(t)
            for rid in b.get('screenshot_ids',[]):
                display.append(f"[表格原文截图（需核对匹配与脱敏）]({resources[rid]['local_path']})")
            if b['complex']: display.append(f"合并格原始关系见 metadata.json 的 {b['id']}；此处表头展开仅供阅读。")
        elif b['type']=='image' and not b.get('inline'):
            display.append(figure(b['id'],True));ingest.append(figure(b['id'],False))
    for name,lines in [('document.md',display),('ingest.md',ingest)]:
        (package/name).write_text('\n\n'.join(lines)+'\n',encoding='utf-8')
    meta['content_hashes']={n:sha(package/n) for n in ['document.md','ingest.md']}
    if meta.get('preprocessing_complete') and meta.get('text_privacy_complete'):
        from knowledge_package import export_knowledge
        export_knowledge(package,meta,canonical_paper_id=canonical_paper_id)
    else:
        json_write(package/'metadata.json',meta)
    original_meta['content_hashes']=meta['content_hashes']
    if 'knowledge_package' in meta:original_meta['knowledge_package']=meta['knowledge_package']


def build_package(source,package,identity,render_screenshots=False,run_ocr=False,progress=lambda stage,message:None,redact_content=True):
    source=Path(source);package=Path(package)
    package.mkdir(parents=True,exist_ok=True)
    (package/'assets').mkdir(exist_ok=True)
    (package/'_review').mkdir(exist_ok=True)
    meta={**identity,'schema_version':1,'parser_version':VERSION,'source_sha256':sha(source),
          'resources':[],'issues':[],'review_status':'needs_review','evaluation_ready':False,
          'completeness_verified':False,'blocks':[],'formulas':[],
          'export_contract':{'upload':'ingest.md','display':'document.md','media_auto_upload':False}}
    # Direct native callers can annotate before cover cleaning as well. Derived
    # callers must supply the original-file-bound object through identity.
    if 'source_education' not in meta and meta.get('original_source_sha256',sha(source))==sha(source):
        from source_education import extract_source_education, source_education_fields
        meta.update(source_education_fields(extract_source_education(source)))
    def issue(code,message,position): meta['issues'].append({'code':code,'message':message,'position':position,'status':'open'})
    def resource(rid,blob,extension,kind,position):
        existing=next((r for r in meta['resources'] if r['id']==rid),None)
        if existing: return existing
        extension=extension.lower() if re.fullmatch(r'\.[a-zA-Z0-9]{1,6}',extension) else '.bin'
        path=package/'_review'/(rid+extension);path.write_bytes(blob)
        r={'id':rid,'local_path':path.relative_to(package).as_posix(),'sha256':sha(path),'kind':kind,'position':position,
           'original_sha256':sha(path),
           'privacy_status':'pending','source':'original','upload_allowed':False}
        meta['resources'].append(r);return r
    clean=redactor() if redact_content else lambda text:text
    if source.suffix.lower()=='.docx': blocks,images,equations=parse_docx(source,clean,resource,issue)
    elif source.suffix.lower()=='.pdf': blocks,images,equations=parse_pdf(source,clean,resource,issue)
    elif source.suffix.lower() in {'.txt','.md'}:
        text=source.read_text(encoding='utf-8')
        blocks=[{'type':'paragraph','text':clean(line),'heading_level':0,'position':{'line':i+1}} for i,line in enumerate(text.splitlines())]
        images=[];equations=[]
        if re.search(r'!\[|\|.*\|',text): issue('text_markup_review','文本中的表格或资源链接尚未结构化，需原始文档',{})
    else: raise ValueError('此格式需要先从原文件转换为 DOCX/PDF 后再生成证据包')
    blocks=retained_blocks(blocks)
    kept_positions=[b['position'] for b in blocks]
    def position_kept(position):
        return any(all(position.get(k)==v for k,v in p.items()) for p in kept_positions)
    # Warnings about removed frontmatter/acknowledgements must not trigger AI work.
    def issue_kept(position):
        if not position: return True
        for kept in kept_positions:
            common=set(position)&set(kept)
            if common and all(position[k]==kept[k] for k in common): return True
        return False
    meta['issues']=[i for i in meta['issues'] if i['code']=='scanned_page' or issue_kept(i.get('position',{}))]
    images=[im for im in images if position_kept(im['position'])]
    equations=[eq for eq in equations if position_kept(eq['position'])]
    attach_context(blocks,images)
    live={im['resource_id'] for im in images}|{rid for b in blocks for rid in b.get('screenshot_ids',[])}
    for r in list(meta['resources']):
        if r['id'] not in live:
            path=(package/r['local_path']).resolve()
            if path.is_relative_to(package.resolve()): path.unlink()
            meta['resources'].remove(r)
    for i,b in enumerate(blocks): b.setdefault('id',f'block_{i+1:04d}')
    count=0
    for b in blocks:
        if b['type']=='table': count+=1;b['id']=f'table_{count:04d}'
    for im in images:
        if im not in blocks: blocks.append(im)
        issue('image_ocr_review','研究图片已提取；图内脱敏和 OCR 未核对',im['position'])
    meta['blocks']=blocks;meta['formulas']=equations
    if render_screenshots and source.suffix.lower()=='.docx' and any(b.get('complex') for b in blocks):
        try:
            progress('渲染原文截图','转换原文并定位表格候选页')
            from evidence_render import capture_docx_tables
            capture_docx_tables(source,blocks,resource,issue)
        except Exception as exc: issue('screenshot_failed','原文渲染失败：'+str(exc),{})
    for b in blocks:
        if b['type']=='table' and b['complex'] and not b.get('screenshot_ids'):
            issue('complex_table_screenshot_missing','复杂表格缺少已定位的原文截图',b['position'])
    if run_ocr:
        from evidence_ocr import inspect_image
        for ri,r in enumerate(meta['resources']):
            if r['kind'] in {'source_page','table_source_page'}:
                r['ocr']={'status':'not_applicable','text':'','verified':False,'reason':'整页仅供原文核对，不进行整页 OCR'}
                continue
            progress('图片 OCR 与脱敏检查',f"资源 {ri+1}/{len(meta['resources'])}：{r['id']}；识别结果仍需人工核对")
            try:
                ocr=inspect_image(package/r['local_path'],clean)
                if ocr.get('image_path'):
                    r['local_path']=Path(ocr.pop('image_path')).relative_to(package).as_posix()
                    r['sha256']=sha(package/r['local_path'])
                r['ocr']=ocr
                for im in images:
                    if im['resource_id']==r['id']: im['ocr']=ocr
                if ocr['status']!='candidate': issue('ocr_unavailable','此资源未获得可靠 OCR 候选',r['position'])
            except Exception as exc:
                issue('ocr_failed','资源 OCR 失败：'+str(exc),r['position'])
    if meta['resources']: issue('media_privacy_review','资源保存在 _review，尚未进入 assets 或可上传内容',{})
    # Source comparison is a separate review; counts do not prove completeness.
    issue('source_comparison_required','需对照原文核对正文顺序、关键图表、数字及来源',{})
    export_package(package,meta)
    progress('证据包完成',f"正文与入库文本已保存，{len(meta['resources'])} 个资源，{len(meta['issues'])} 项待检查记录")
    return meta


def package_fingerprint(package):
    package=Path(package);meta=json.loads((package/'metadata.json').read_text(encoding='utf-8'))
    files=['document.md','ingest.md']+[r['local_path'] for r in meta.get('resources',[])]
    if meta.get('knowledge_package') or (package/'manifest.json').exists():
        from knowledge_package import knowledge_files
        files=knowledge_files(package)
    rows=[]
    for name in sorted(files):
        p=(package/name).resolve()
        if not p.is_relative_to(package.resolve()) or not p.is_file(): raise ValueError('证据资源缺失或路径越界')
        rows.append((name,sha(p)))
    rows.append(('metadata',hashlib.sha256(json.dumps(meta,ensure_ascii=False,sort_keys=True).encode()).hexdigest()))
    return hashlib.sha256(json.dumps(rows,ensure_ascii=False).encode()).hexdigest()


def approve_resource(package,resource_id,verified_text=None):
    """Human action: publish the inspected/redacted resource; OCR is a separate assertion."""
    from PIL import Image
    package=Path(package).resolve()
    meta=json.loads((package/'metadata.json').read_text(encoding='utf-8'))
    r=next(r for r in meta['resources'] if r['id']==resource_id)
    source=(package/r['local_path']).resolve()
    if not source.is_relative_to(package) or not source.is_file(): raise ValueError('图片资源路径无效')
    target=package/'assets'/(resource_id+'.png')
    with Image.open(source) as image:
        image.convert('RGB').save(target)
    r.update(local_path=target.relative_to(package).as_posix(),sha256=sha(target),privacy_status='approved',upload_allowed=False)
    if source!=target: source.unlink()
    if verified_text is not None:
        r['ocr']={'text':redactor()(verified_text),'verified':True,'status':'human_verified','source':'manual_transcription'}
    for block in meta['blocks']:
        if block.get('resource_id')==resource_id:
            block['privacy_status']='approved'
            if verified_text is not None: block['ocr']=dict(r['ocr'])
    # Image approval alone cannot verify table meaning, page matching or overall completeness.
    export_package(package,meta)
    return meta
