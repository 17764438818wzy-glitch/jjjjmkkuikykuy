"""Automated evidence processing. Source images are evidence, never synthesized."""
import copy
import hashlib
import io
import json
import re
import subprocess
import time
import math
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from PIL import Image,ImageDraw
from agent_client import AgentError,validate_result
from evidence import export_package,redactor,sha


class Rejected(Exception):
    def __init__(self, reason, kind='evidence', position=None):
        self.kind=kind;self.position=position or {}
        super().__init__(reason + ('；位置：'+json.dumps(position,ensure_ascii=False) if position else ''))


def ask(agent,task,payload,images=(),*,defer_rejection=False):
    started=time.monotonic();outcome='error'
    observer=getattr(agent,'on_ai_step',None)
    call_id=uuid.uuid4().hex
    reason='模型调用或响应校验失败'
    if observer:observer(call_id,task,'running')
    try:
        result=validate_result(task,agent.call(task,payload,images=images))
        outcome=result['decision']
        reason=result['reason']
        if outcome=='reject' and not defer_rejection:
            raise Rejected(result['reason'],'privacy' if task.startswith('privacy') else 'evidence',payload.get('position'))
        return result
    finally:
        if not hasattr(agent,'processing_metrics'): agent.processing_metrics=[]
        agent.processing_metrics.append({'task':task,'images':len(images),'seconds':round(time.monotonic()-started,3),'outcome':outcome,
                                         'cache_hit':bool(getattr(agent,'last_call_cache_hit',False))})
        if observer:observer(call_id,task,outcome,reason,bool(getattr(agent,'last_call_cache_hit',False)))


def accept_stage(agent,result):
    """Persist a response only after this consumer has validated and applied it."""
    accept=getattr(agent,'accept_cached_result',None)
    if accept:accept(result)


def bounds(value, image_size=None, coordinate_space='normalized'):
    """Normalize declared coordinates only; never guess a unit or swap corners."""
    if isinstance(value,dict):
        coordinate_space=value.get('coordinate_space',coordinate_space)
        if 'bbox' in value:value=value['bbox']
        elif all(k in value for k in ['x0','y0','x1','y1']):value=[value[k] for k in ['x0','y0','x1','y1']]
        else:raise AgentError('AI 图片坐标格式无效')
    if not isinstance(value,list) or len(value)!=4:raise AgentError('AI 图片坐标格式无效')
    numbers=[]
    for v in value:
        if type(v) not in {str,float,int}:raise AgentError('AI 图片坐标格式无效')
        try:n=float(v)
        except (TypeError,ValueError):raise AgentError('AI 图片坐标格式无效') from None
        if not math.isfinite(n):raise AgentError('AI 图片坐标不是有限数值')
        numbers.append(n)
    if coordinate_space=='pixel':
        if not image_size or len(image_size)!=2 or any(type(n) not in {int,float} or n<=0 for n in image_size):
            raise AgentError('像素坐标缺少实际输入图片尺寸')
        scales=[image_size[0],image_size[1]]*2
    elif coordinate_space in {'normalized','percent','0_1000'}:
        scales=[{'normalized':1,'percent':100,'0_1000':1000}[coordinate_space]]*4
    else:raise AgentError('AI 图片坐标单位不明确')
    values=[n/s for n,s in zip(numbers,scales)]
    if any(n < -1e-6 or n > 1+1e-6 for n in values):
        raise AgentError('AI 图片坐标越界或未声明正确单位；未猜测裁图范围')
    values=[max(0.0,min(1.0,n)) for n in values]
    x0,y0,x1,y1=values
    if x0>=x1 or y0>=y1:raise AgentError('AI 图片坐标顺序错误或区域为空')
    return values


def pixel_box(box,size):
    """Cover the entire declared region, including its boundary pixels."""
    w,h=size
    return (math.floor(box[0]*w),math.floor(box[1]*h),math.ceil(box[2]*w),math.ceil(box[3]*h))


def safe_visual_provenance(value):
    """Do not export free-form annotations attached to model coordinates.

    Validated normalized and pixel coordinates already retain the applied
    geometry. Raw response objects belong in private model history only.
    Also scrub old approved-resource metadata before its no-model fast path.
    """
    if isinstance(value,dict):
        return {k:safe_visual_provenance(v) for k,v in value.items()
                if k not in {'mask_boxes_input','crop_bbox_input'}
                and not (k in {'mask_coordinate_space','crop_coordinate_space'}
                         and (not isinstance(v,str) or v not in {'normalized','pixel','percent','0_1000'}))}
    if isinstance(value,list):return [safe_visual_provenance(v) for v in value]
    return value


def verify_research_pixels(image,package,rid,position,provenance):
    """Reject empty research outputs after masking; never undo privacy masks.

    This deliberately checks only exact all-black/all-white pixels, not a
    content heuristic or an AI approval.  Save the input coordinate trail on
    failure so a reviewer can distinguish a bad crop from an overbroad mask.
    """
    rgb=image.convert('RGB');extrema=rgb.getextrema()
    color=('black' if all(pair==(0,0) for pair in extrema) else
           'white' if all(pair==(255,255) for pair in extrema) else None)
    result={'method':'exact_rgb_extrema_v1','status':('rejected_uniform_'+color if color else 'not_uniform_black_or_white'),
            'output_size':list(rgb.size),'output_rgb_extrema':[list(pair) for pair in extrema],
            'output_rgb_sha256':hashlib.sha256(rgb.tobytes()).hexdigest(),'provenance':safe_visual_provenance(provenance)}
    if color:
        safe_id=re.sub(r'[^A-Za-z0-9_.-]','_',str(rid)).strip('.') or 'resource'
        trace=Path(package)/'_review'/'visual_integrity'/(safe_id+'.json')
        trace.parent.mkdir(parents=True,exist_ok=True)
        trace.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
        raise Rejected('研究图片处理产物为'+('全黑' if color=='black' else '全白')+'，无法保留研究内容；未批准产物',
                       position={**(position or {}),'resource_id':rid,'integrity_trace':trace.relative_to(package).as_posix()})
    return result


def source_pages(source,work,max_pages=80,*,page_filter=None,page_transform=None,return_records=False):
    """Temporary page evidence for AI only, not exported as research images."""
    import fitz
    source=Path(source);work=Path(work);work.mkdir(parents=True,exist_ok=True)
    if source.suffix=='.pdf': pdf_path=source
    elif source.suffix=='.docx':
        from evidence_render import render_office_pdf
        pdf_path=render_office_pdf(source,work/'render')
    else: return []
    pages=[]
    from contextlib import nullcontext
    with fitz.open(pdf_path) as pdf, (fitz.open(pdf_path) if page_filter is not None else nullcontext(pdf)) as render_pdf:
        if len(pdf)>max_pages: raise AgentError(f'原文超过当前 AI 页数上限 {max_pages}，未截断处理；可调高配置后重试')
        for page in pdf:
            # Keep rendering separate from analysis: some MuPDF image inspection
            # APIs alter shared decoded image caches even without saving the PDF.
            # Pixel edits, if requested, apply only to the resulting PNG copy.
            if page_filter is not None and not page_filter(page):continue
            blob=render_pdf[page.number].get_pixmap(matrix=fitz.Matrix(1.5,1.5)).tobytes('png')
            if page_transform is not None:blob=page_transform(page.number+1,blob)
            pages.append((page.number+1,blob) if return_records else blob)
    return pages


def repair(meta,package,pages,agent,progress):
    codes={'embedded_object_review','body_container','scanned_page','text_markup_review','image_missing','formula_review'}
    issues=[i for i in meta['issues'] if i['code'] in codes]
    if not issues: return
    if not pages: raise AgentError('疑难内容缺少原文视觉证据，不能假设已恢复')
    anchors={b['id'] for b in meta['blocks']};inserts=[]
    summary=[{'id':b['id'],'type':b['type'],'position':b['position'],'text':b.get('text','')[:160]} for b in meta['blocks']]
    for number,blob in enumerate(pages,1):
        progress('AI 补充疑难内容',f'原文第 {number}/{len(pages)} 页')
        result=ask(agent,'repair',{'page':number,'issues':issues,'known_blocks':summary},[blob])
        for ins in result['insertions']:
            if not isinstance(ins,dict) or ins.get('after_id') not in anchors or ins.get('page')!=number: raise AgentError('AI 补充内容缺少有效原文位置')
            kind=ins.get('kind');bid=f'ai_{len(inserts)+1:04d}'
            block={'id':bid,'position':{'page':number,'after_id':ins['after_id']},'source':'ai_transcription','section':[]}
            if kind=='text':
                if not isinstance(ins.get('text'),str): raise AgentError('AI 转录文本无效')
                block.update(type='paragraph',text=ins['text'],heading_level=0)
            elif kind=='table':
                rows=ins.get('rows')
                if not isinstance(rows,list) or not rows or not all(isinstance(r,list) and r and all(isinstance(c,str) for c in r) for r in rows): raise AgentError('AI 表格结构无效')
                if len({len(r) for r in rows})!=1: raise AgentError('AI 表格行列不对应')
                width=len(rows[0]);cells=[{'row':ri,'column':ci,'rowspan':1,'colspan':1,'text':v} for ri,row in enumerate(rows) for ci,v in enumerate(row)]
                block.update(type='table',rows=len(rows),columns=width,cells=cells,grid=[[ri*width+ci for ci in range(width)] for ri in range(len(rows))],complex=False,header_rows=[0],header_verified=False,caption=ins.get('caption',''),notes=[],units='')
            elif kind=='image':
                box=bounds(ins.get('bbox'));im=Image.open(io.BytesIO(blob));w,h=im.size
                if (box[2]-box[0])*(box[3]-box[1])>.9: raise AgentError('AI 返回整页区域，未当成研究图片')
                path=package/'_review'/(bid+'.png');im.crop(tuple(round(v*(w if i%2==0 else h)) for i,v in enumerate(box))).save(path)
                meta['resources'].append({'id':bid,'kind':'image','position':block['position'],'local_path':path.relative_to(package).as_posix(),'privacy_status':'pending','sha256':sha(path),'original_sha256':sha(path),'source':'original_page_crop','upload_allowed':False})
                block.update(type='image',resource_id=bid,inline=False,caption=ins.get('caption',''),ocr={'verified':False})
            else: raise AgentError('AI 补充内容类型无效')
            inserts.append(block)
    for original in reversed(list(meta['blocks'])):
        related=[b for b in inserts if b['position']['after_id']==original['id']]
        at=meta['blocks'].index(original)+1;meta['blocks'][at:at]=related


def clean_strings(value,clean):
    if isinstance(value,str): return clean(value)
    if isinstance(value,list): return [clean_strings(v,clean) for v in value]
    if isinstance(value,dict):
        result={}
        for k,v in value.items():
            if k in {'id','resource_id','local_path','source','sha256','original_sha256','source_sha256','original_source_sha256','source_image_sha256','source_mask_sha256','processing_version','parser_version'}: result[k]=v
            elif k in {'native_xml','omml'} and isinstance(v,str):
                from xml.etree import ElementTree as ET
                tree=ET.fromstring(v)
                for node in tree.iter():
                    for attr in list(node.attrib):
                        name=attr.rsplit('}',1)[-1].lower()
                        if name in {'author','initials','lastmodifiedby','creator'} or name.startswith('rsid'):
                            del node.attrib[attr]
                        else:
                            node.attrib[attr]=clean(node.attrib[attr])
                    if node.text: node.text=clean(node.text)
                    if node.tail: node.tail=clean(node.tail)
                result[k]=ET.tostring(tree,encoding='unicode')
            else: result[k]=clean_strings(v,clean)
        return result
    return value


def _privacy_decision_needs_review(result):
    """A contradiction triggers another decision, never an automatic approval."""
    if result.get('decision')!='reject':return False
    reason=str(result.get('reason',''))
    return bool(re.search(r'可精确定位|精确可定位|可精确遮盖|\[(?:已脱敏|NAME|TEACHER|STUDENT_ID|CLASS)\]|占位符|(?:教师|领导|受访者|学生)\s*[A-Z](?:\b|[、，及与之访])'
                          r'|空角色标签|仅有角色标签|身份槽位|无可定位的具体身份值'
                          r'|不应[^。；;\n]{0,24}(?:reject|拒绝|本文身份)',reason,re.I)
                or result.get('sensitive_strings'))


def _masked_identity_value(text):
    """Keep original field-edge whitespace, including real empty paragraphs."""
    start=len(text)-len(text.lstrip());end=len(text.rstrip())
    return text[:start]+'[已脱敏]'+text[end:]


def _masked_native_identity_cell(block,cell):
    from xml.etree import ElementTree as ET
    w='{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
    try:
        root=ET.fromstring(block['native_xml'])
        if root.tag!=w+'tbl':return None
        row=root.findall(w+'tr')[cell['row']]
        before=row.find('./'+w+'trPr/'+w+'gridBefore')
        column=int(before.get(w+'val','0')) if before is not None else 0
        for node in row.findall(w+'tc'):
            span=node.find('./'+w+'tcPr/'+w+'gridSpan')
            width=int(span.get(w+'val','1')) if span is not None else 1
            if column==cell['column']:
                unsupported={w+tag for tag in ('tab','br','cr','drawing','object','fldChar','instrText',
                                              'delText','del','ins','sdt','sym','noBreakHyphen','softHyphen',
                                              'footnoteReference','endnoteReference','moveFrom','moveTo')}
                if (width!=cell.get('colspan',1) or cell.get('continuation_text')
                        or any(child.tag not in {w+'tcPr',w+'p'} for child in node)
                        or any(child.tag in unsupported for child in node.iter())):return None
                paragraphs=node.findall(w+'p')
                values=[''.join(t.text or '' for t in p.iter(w+'t')) for p in paragraphs]
                active=[i for i,value in enumerate(values) if value.strip()]
                if '\n'.join(values)!=cell['text'] or len(active)!=1:return None
                value=values[active[0]];texts=list(paragraphs[active[0]].iter(w+'t'))
                start=len(value)-len(value.lstrip());end=len(value.rstrip())
                offset=0;inserted=False
                for text in texts:
                    original=text.text or '';stop=offset+len(original)
                    if offset<end and start<stop:
                        lo=max(0,start-offset);hi=min(len(original),end-offset)
                        text.text=original[:lo]+('' if inserted else '[已脱敏]')+original[hi:]
                        inserted=True
                    offset=stop
                # Native XML is exported too. A hyperlink tooltip or another
                # attribute in this same confirmed cell must not retain a copy.
                private=cell['text'].strip()
                for child in node.iter():
                    for key,value in child.attrib.items():
                        child.set(key,value.replace(private,'[已脱敏]'))
                    if child.tag!=w+'t' and child.text:child.text=child.text.replace(private,'[已脱敏]')
                    if child.tail:child.tail=child.tail.replace(private,'[已脱敏]')
                visible='\n'.join(''.join(t.text or '' for t in p.iter(w+'t')) for p in paragraphs)
                if visible!=_masked_identity_value(cell['text']):return None
                return ET.tostring(root,encoding='unicode')
            column+=width
    except (ET.ParseError,KeyError,IndexError,TypeError,ValueError):return None
    return None


def _adjacent_identity_values(block,label,values):
    """Exact cell text and unambiguous, contiguous geometry for one field."""
    cells=block.get('cells',[])
    row=label.get('row')
    if (type(row) is not int or row<0 or label.get('native_xml')
            or label.get('continuation_text')):return None
    if block.get('native_xml') and _masked_native_identity_cell(block,label) is None:return None
    row_cells=[cell for cell in cells if cell.get('row')==row]
    intervals=[]
    for cell in row_cells:
        column=cell.get('column');width=cell.get('colspan',1)
        if (type(column) is not int or column<0 or type(width) is not int or width<1
                or cell.get('rowspan',1)!=1):return None
        interval=(column,column+width)
        if any(lo<interval[1] and interval[0]<hi for lo,hi in intervals):return None
        intervals.append(interval)
    column=label['column']+label.get('colspan',1)
    result=[]
    for text in values:
        candidates=[cell for cell in row_cells if cell['column']==column]
        if len(candidates)!=1:return None
        cell=candidates[0]
        if cell.get('text')!=text or cell.get('native_xml') or cell.get('continuation_text'):return None
        if block.get('native_xml') and _masked_native_identity_cell(block,cell) is None:return None
        result.append(cell);column+=cell.get('colspan',1)
    return result


def _native_submission_marker(block,cell,pattern):
    """The form marker must exist in the matching native cell, not a copy."""
    from xml.etree import ElementTree as ET
    w='{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
    if not re.search(pattern,cell.get('text','')):return False
    if (cell.get('native_xml') or cell.get('continuation_text')
            or type(cell.get('row')) is not int or cell['row']<0
            or type(cell.get('column')) is not int or cell['column']<0):return False
    if sum(c.get('row')==cell['row'] and c.get('column')==cell['column'] for c in block.get('cells',[]))!=1:return False
    try:
        root=ET.fromstring(block['native_xml'])
        if root.tag!=w+'tbl':return False
        row=root.findall(w+'tr')[cell['row']]
        before=row.find('./'+w+'trPr/'+w+'gridBefore')
        column=int(before.get(w+'val','0')) if before is not None else 0
        for node in row.findall(w+'tc'):
            span=node.find('./'+w+'tcPr/'+w+'gridSpan')
            width=int(span.get(w+'val','1')) if span is not None else 1
            if column==cell['column']:
                if width!=cell.get('colspan',1) or any(n.tag not in {w+'tcPr',w+'p'} for n in node):return False
                text='\n'.join(''.join(n.text or '' for n in p.iter(w+'t')) for p in node.findall(w+'p'))
                return text==cell['text']
            column+=width
    except (ET.ParseError,KeyError,IndexError,TypeError,ValueError):return False
    return False


def _submission_identity_table(block):
    """Require the form's own evidence before disregarding a stale section."""
    from frontmatter_privacy import _name
    if not block.get('native_xml'):return False
    cells=block.get('cells',[])
    admission=False;name=False;phone=False
    for label in cells:
        match=re.fullmatch(r'\s*(准考证号|姓名|联系电话)\s*[:：]?\s*',label.get('text',''))
        if not match:continue
        role=match[1]
        for value in cells:
            text=value.get('text','')
            valid=(bool(re.fullmatch(r'(?:[0-9]{10}|[0-9]{12})',text)) if role=='准考证号'
                   else bool(re.fullmatch(r'1[3-9][0-9]{9}',text)) if role=='联系电话' else _name(text))
            if valid and _adjacent_identity_values(block,label,[text]):
                if role=='准考证号':admission=True
                elif role=='联系电话':phone=True
                else:name=True
    if not (admission and name and phone):return False
    signature=any(_native_submission_marker(block,cell,r'(?:^|[\n。；;])\s*作者签名\s*[:：]') for cell in cells)
    score=any(_native_submission_marker(block,cell,r'评分\s*[:：]') for cell in cells)
    return signature and score


def _table_identity_cells(meta,span):
    """Resolve AI-confirmed private identity fields without bare-number masks."""
    from frontmatter_privacy import _name
    parts=span.split('\n')
    if len(parts)<2:return []
    names={'观察者','记录者','记录人','教师','执教者','授课教师','作者','姓名','学生姓名','调查员','访谈者','指导教师','指导老师'}
    content=[part for part in parts if part.strip()]
    if not content:return []
    match=re.fullmatch(r'\s*('+'|'.join(sorted(names|{'准考证号','联系电话'}))+r')\s*[:：]?\s*',content[0])
    if not match:return []
    role=match[1]
    if role in names:
        if len(content)!=2 or not _name(content[1]):return []
    elif role=='准考证号':
        if len(parts)!=2 or parts[0]!=content[0] or not re.fullmatch(r'(?:[0-9]{10}|[0-9]{12})',parts[1]):return []
    elif role=='联系电话':
        if parts[0]!=content[0] or not all(re.fullmatch(r'1[3-9][0-9]{9}',value) for value in parts[1:]):return []
    else:return []
    matched=[]
    for block in meta.get('blocks',[]):
        if block.get('type')!='table':continue
        cells=block.get('cells',[])
        def matching_label(cell):
            text=cell.get('text','')
            return bool(re.fullmatch(r'\s*'+re.escape(role)+r'\s*[:：]?\s*',text)) if role in names else text==parts[0]
        if not any(matching_label(cell) for cell in cells):continue
        submission=_submission_identity_table(block)
        context=' '.join(str(block.get(key) or '') for key in
                         ('caption','context_before','context_after'))
        if not submission:context+=' '+str(block.get('section') or '')
        reference_context=bool(re.search(r'参考文献|文献综述|已有研究|引用文献|相关研究|研究现状|references|bibliography',context,re.I))
        for label in cells:
            if not matching_label(label) or label.get('native_xml'):continue
            if role in names:
                if type(label.get('column')) is not int or type(label.get('colspan',1)) is not int:continue
                candidates=[cell for cell in cells if cell.get('row')==label.get('row')
                            and cell.get('column')==label['column']+label.get('colspan',1)]
                if len(candidates)!=1:continue
                text=candidates[0].get('text','')
                if not _name(text):continue
                original=label['text']+'\n'+text;at=original.find(span)
                # Only the exact original pair may match. Omitted characters
                # can be field-edge whitespace, never internal separators.
                if at<0 or original[:at].strip() or original[at+len(span):].strip():continue
                if ('\n' in label['text'] or '\n' in text) and not block.get('native_xml'):continue
                values=_adjacent_identity_values(block,label,[text])
            else:values=_adjacent_identity_values(block,label,parts[1:])
            if values:
                # A second matching field outside a proved form is ambiguous;
                # do not silently borrow another table's submission evidence.
                if role not in names and not submission:return []
                # The flattened answer cannot distinguish a submitter from
                # a cited author with the same label/name in another table.
                if reference_context:return []
                matched.extend((block,value) for value in values)
    return matched


def _mask_native_table_spans(meta,spans):
    """Map an exact model span within one cell paragraph onto original runs."""
    if not spans:return
    from xml.etree import ElementTree as ET
    w='{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
    unsupported={w+tag for tag in ('tab','br','cr','drawing','object','fldChar','instrText',
                                  'delText','del','ins','sdt','sym','noBreakHyphen','softHyphen',
                                  'footnoteReference','endnoteReference','moveFrom','moveTo')}
    def reject():raise Rejected('原生表格脱敏位置无法精确映射到同一单元格段落，未发布正文','privacy')
    for block in meta.get('blocks',[]):
        if block.get('type')!='table' or not block.get('native_xml'):continue
        root=ET.fromstring(block['native_xml']);cells=block.get('cells',[]);changed=False
        for cell in cells:
            cell_spans=[s for s in sorted(set(spans),key=len,reverse=True) if s in cell.get('text','')]
            if not cell_spans:continue
            if (root.tag!=w+'tbl' or cell.get('native_xml') or cell.get('continuation_text')
                    or type(cell.get('row')) is not int or type(cell.get('column')) is not int
                    or cell['row']<0 or cell['column']<0 or cell.get('rowspan',1)!=1
                    or sum(c.get('row')==cell['row'] and c.get('column')==cell['column'] for c in cells)!=1):reject()
            try:
                row=root.findall(w+'tr')[cell['row']]
                before=row.find('./'+w+'trPr/'+w+'gridBefore')
                column=int(before.get(w+'val','0')) if before is not None else 0
                target=None
                for node in row.findall(w+'tc'):
                    span=node.find('./'+w+'tcPr/'+w+'gridSpan')
                    width=int(span.get(w+'val','1')) if span is not None else 1
                    if column==cell['column']:
                        if width!=cell.get('colspan',1):reject()
                        target=node;break
                    column+=width
            except (IndexError,TypeError,ValueError):reject()
            if target is None or any(child.tag not in {w+'tcPr',w+'p'} for child in target):reject()
            paragraphs=target.findall(w+'p')
            def content():return '\n'.join(''.join(t.text or '' for t in p.iter(w+'t')) for p in paragraphs)
            if content()!=cell['text']:reject()
            for sensitive in cell_spans:
                matches=[]
                for paragraph in paragraphs:
                    nodes=list(paragraph.iter(w+'t'));text=''.join(n.text or '' for n in nodes)
                    start=text.find(sensitive)
                    if start>=0:
                        if text.find(sensitive,start+1)>=0:reject()
                        matches.append((paragraph,nodes,start))
                if len(matches)!=1:reject()
                paragraph,nodes,start=matches[0];end=start+len(sensitive)
                if any(n.tag in unsupported for n in paragraph.iter()):reject()
                offset=0;inserted=False
                for node in nodes:
                    value=node.text or '';stop=offset+len(value)
                    if offset<end and start<stop:
                        lo=max(0,start-offset);hi=min(len(value),end-offset)
                        node.text=value[:lo]+('' if inserted else '[已脱敏]')+value[hi:]
                        inserted=True
                    offset=stop
            if content()!=_replace(cell['text'],cell_spans):reject()
            changed=True
        if changed:block['native_xml']=ET.tostring(root,encoding='unicode')


def _verify_native_table_spans(meta,spans):
    """Runs must not hide a confirmed exact span from the publication check."""
    if not spans:return
    from xml.etree import ElementTree as ET
    w='{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
    for block in meta.get('blocks',[]):
        if block.get('type')!='table' or not block.get('native_xml'):continue
        for paragraph in ET.fromstring(block['native_xml']).iter(w+'p'):
            text=''.join(node.text or '' for node in paragraph.iter(w+'t'))
            if any(s in text for s in spans):
                raise Rejected('原生表格仍含已确认的敏感内容，未发布正文','privacy')


def text_privacy(meta,agent,progress):
    meta['text_privacy_complete']=False
    rules=redactor()
    # A numeric cell is research data. Field identity needs context, not digit shape.
    clean=lambda text:text if re.fullmatch(r'[\d.,%％()（）+\-−—–\s]+',text) else rules(text)
    keys=['blocks','formulas','title','resources']
    image_keys=['ocr','description','chart_spec','structured_table']
    image_text={r['id']:[copy.deepcopy(r.get(k)) for k in image_keys]
                for r in meta.get('resources',[])}
    for key in keys:
        meta[key]=clean_strings(meta.get(key,[]),clean)
    from frontmatter_privacy import redact_frontmatter_blocks
    identity_fields=redact_frontmatter_blocks(meta)
    from evidence_tasks import visible_text
    fields=visible_text(meta,fields=True);encoded='\n'.join(fields);spans=[];accepted_results=[];decision_reviews=0
    ranges=[];offset=0;scoped_spans={}
    for value in fields:
        ranges.append((offset,offset+len(value)));offset+=len(value)+1
    # One pass over visible text; no raw OOXML, repeated structures or second full audit.
    for start in range(0,len(encoded),18000):
        fragment=encoded[max(0,start-256):start+18256]
        progress('AI 文本脱敏',f'单轮检查，片段 {start//18000+1}/{max(1,(len(encoded)+17999)//18000)}')
        result=ask(agent,'privacy_text',{'text':fragment},defer_rejection=True)
        if _privacy_decision_needs_review(result):
            progress('AI 脱敏决策复核','拒绝理由与脱敏规则存在疑点，限一次独立复核；仍拒绝则不发布')
            decision_reviews+=1
            result=ask(agent,'privacy_text',{'text':fragment,'phase':'review_privacy_decision',
                       'previous_decision':result},defer_rejection=True)
        if result['decision']=='reject':raise Rejected(result['reason'],'privacy')
        accepted_results.append(result)
        for s in result['sensitive_strings']:
            if not isinstance(s,str) or not s or s not in fragment: raise AgentError('AI 脱敏位置不匹配原文')
            at=encoded.find(s)
            while at>=0:
                if not any(lo<=at and at+len(s)<=hi for lo,hi in ranges):
                    cells=_table_identity_cells(meta,s)
                    if not cells:
                        raise Rejected('敏感内容跨段或跨单元格，未能移除；未发布正文','privacy')
                    scoped_spans[s]=cells
                at=encoded.find(s,at+1)
            if re.fullmatch(r'[\d.,%％()（）+\-−—–\s]+',s): raise AgentError('AI 请求删除纯数值，未自动改变研究数值')
            if s not in scoped_spans:spans.append(s)
    for cells in scoped_spans.values():
        for block,cell in cells:
            if cell['text'].strip()=='[已脱敏]':continue
            if block.get('native_xml'):
                replacement=_masked_native_identity_cell(block,cell)
                if replacement is None:raise Rejected('原生表格身份位置发生变化，未发布正文','privacy')
                block['native_xml']=replacement
            cell['text']=_masked_identity_value(cell['text'])
    _mask_native_table_spans(meta,spans)
    for key in keys: meta[key]=clean_strings(meta[key],lambda text:_replace(text,spans))
    _verify_native_table_spans(meta,spans)
    if any(s in visible_text(meta) for s in [*spans,*scoped_spans]):
        raise Rejected('敏感内容跨段或跨单元格，未能移除；未发布正文','privacy')
    for r in meta['resources']:
        if r['id'] in image_text and image_text[r['id']]!=[r.get(k) for k in image_keys]:
            # Later text checks can disprove an earlier visual privacy result.
            # Do not replay the same unsafe response on every future retry.
            invalidate=getattr(agent,'invalidate_accepted_results',None)
            if invalidate:invalidate()
            raise Rejected('文本检查发现图片中的额外敏感内容，无法证明原图对应区域已遮盖；未发布图片','privacy',r.get('position'))
    meta['privacy_check']={'method':'original_rules_and_single_visible_text_pass','redacted_spans':len(set(spans)),
                           'explicit_identity_fields':identity_fields,'decision_reviews':decision_reviews,
                           'table_identity_cells':sum(len(cells) for cells in scoped_spans.values())}
    meta['text_privacy_complete']=True
    for result in accepted_results:accept_stage(agent,result)


def _replace(text,spans):
    for s in sorted(set(spans),key=len,reverse=True): text=text.replace(s,'[已脱敏]')
    return text


def _parallel_agent(agent,gate):
    """Clone mutable per-call state while sharing cache acceptance state."""
    worker=copy.copy(agent)
    worker.stop=getattr(agent,'stop',None)
    worker.llm_gate=gate
    worker.processing_metrics=[]
    worker.last_call_cache_hit=False
    if hasattr(agent,'_pending_cache'): worker._pending_cache=agent._pending_cache
    if hasattr(agent,'_accepted_cache_keys'): worker._accepted_cache_keys=agent._accepted_cache_keys
    worker.billing_context=dict(getattr(agent,'billing_context',{}) or {})
    worker.on_cache_hit=None
    return worker


def _visual_recheck_needed(resource):
    if resource.get('masked_observation_verified'):
        return False
    # Page extraction may report redacted OCR while returning no pixel masks.
    # Only a separate observation of the actual crop can establish privacy.
    if resource.get('privacy_method')=='single_page_recognition_and_local_mask':
        return True
    provenance=resource.get('crop_provenance') or {}
    if (resource.get('masked_regions') or provenance.get('page_mask_boxes_normalized')
            or provenance.get('focused_mask_boxes_normalized')):
        return True
    observed={key:resource.get(key) for key in ('ocr','description','chart_spec','structured_table')}
    return clean_strings(observed,redactor())!=observed


def _verify_masked_observation(agent,image,resource,block,*,detail_review=True,research_context=None):
    """Read only the published pixels; never reuse pre-mask OCR as evidence."""
    extra=[];accepted=[]
    research_context=research_context or {}
    if detail_review and min(image.size)>=240:
        width,height=image.size
        regions=[(x0,y0,x1,y1) for x0,x1 in ((0,math.ceil(width*.55)),(math.floor(width*.45),width))
                 for y0,y1 in ((0,math.ceil(height*.55)),(math.floor(height*.45),height))]
        for region in regions:
            context_reviewed=False
            for attempt in range(2):
                crop=image.crop(region)
                scale=max(1,math.ceil(300/min(crop.size)))
                inspection=crop.resize((crop.width*scale,crop.height*scale),Image.Resampling.NEAREST) if scale>1 else crop
                encoded=io.BytesIO();inspection.save(encoded,format='PNG')
                review_images=[encoded.getvalue()];context_fields={}
                result=ask(agent,'privacy_image',{'id':resource['id'],'kind':resource['kind'],
                    'phase':'verify_masked','review_region':list(region),**research_context,
                    'scope':'只检查当前局部未遮盖的身份信息；坐标相对这张局部图片，不是原图。局部空白不代表整张研究图片无价值。',
                    'position':resource.get('position',{})},review_images,defer_rejection=True)
                if not context_reviewed and (result['decision']=='reject' or (attempt and result.get('sensitive_boxes'))):
                    # An isolated crop can mistake clothes or walls for a
                    # head. Use the masked overview only to resolve an actual
                    # failed local review; ordinary crops keep one image.
                    overview=io.BytesIO();image.save(overview,format='PNG')
                    review_images.append(overview.getvalue());context_fields={'context_image_indices':[2]}
                    result=ask(agent,'privacy_image',{'id':resource['id'],'kind':resource['kind'],
                        'phase':'verify_masked','review_region':list(region),**context_fields,**research_context,
                        'position':resource.get('position',{})},review_images,defer_rejection=True)
                    context_reviewed=True
                if result['decision']=='reject' and result.get('sensitive_boxes') and not attempt:
                    # Ask for a fresh actionable decision; a reject is never
                    # converted to approval merely because it included boxes.
                    result=ask(agent,'privacy_image',{'id':resource['id'],'kind':resource['kind'],
                        'phase':'locate_privacy_regions','review_region':list(region),
                        **context_fields,**research_context,
                        'previous_decision':result,'position':resource.get('position',{}),
                        'scope':'当前局部发现了尚未遮盖但可定位的身份；若能可靠返回完整遮盖框，再作新的可执行判定，否则继续拒绝。坐标仅相对本局部图。'},
                        review_images,defer_rejection=True)
                if result['decision']=='reject':
                    raise Rejected('局部脱敏复核未通过：'+result['reason'],
                                   'evidence' if result.get('failure_kind')=='content' else 'privacy',resource.get('position'))
                boxes=[bounds(b,inspection.size,result.get('coordinate_space','normalized'))
                       for b in result.get('sensitive_boxes',[])]
                if not boxes:
                    accepted.append(result);break
                if attempt:
                    raise Rejected('局部补遮盖后仍发现身份信息，未发布图片','privacy',resource.get('position'))
                for box in boxes:
                    mapped=[(region[0]+box[0]*crop.width)/width,(region[1]+box[1]*crop.height)/height,
                            (region[0]+box[2]*crop.width)/width,(region[1]+box[3]*crop.height)/height]
                    ImageDraw.Draw(image).rectangle(pixel_box(mapped,image.size),fill='black');extra.append(mapped)
    for attempt in range(2):
        encoded=io.BytesIO();image.save(encoded,format='PNG')
        result=ask(agent,'privacy_image',{'id':resource['id'],'kind':resource['kind'],
            'phase':'verify_masked','caption':block.get('caption',''),**research_context,
            'position':resource.get('position',{})},[encoded.getvalue()],defer_rejection=True)
        if result['decision']=='reject' and result.get('sensitive_boxes') and not attempt:
            result=ask(agent,'privacy_image',{'id':resource['id'],'kind':resource['kind'],
                'phase':'locate_privacy_regions','caption':block.get('caption',''),**research_context,
                'previous_decision':result,'position':resource.get('position',{})},
                [encoded.getvalue()],defer_rejection=True)
        if result['decision']=='reject':
            raise Rejected('遮盖后复核未通过：'+result['reason'],
                           'evidence' if result.get('failure_kind')=='content' else 'privacy',resource.get('position'))
        if result.get('research') is not True or not (result['text'].strip() or result['description'].strip()):
            raise Rejected('遮盖后复核未能保留可检索的研究图像内容',position=resource.get('position'))
        boxes=[bounds(b,image.size,result.get('coordinate_space','normalized')) for b in result.get('sensitive_boxes',[])]
        if not boxes:break
        if not detail_review:
            raise Rejected('图表复核发现新的身份区域，需要重新定位脱敏，未发布图片','privacy',resource.get('position'))
        if attempt:
            raise Rejected('全图补遮盖后仍发现身份信息，未发布图片','privacy',resource.get('position'))
        for box in boxes:
            ImageDraw.Draw(image).rectangle(pixel_box(box,image.size),fill='black');extra.append(box)
        accepted.append(result)
    for item in accepted:accept_stage(agent,item)
    return result,extra


def _media_source_context(meta,block,rid):
    from evidence_tasks import _repair_source_context,same_position
    blocks=meta['blocks'];after=block.get('position',{}).get('after_id')
    anchors=[b for b in blocks if after and b.get('id')==after and b.get('type')!='image']
    if not anchors:
        marker='[[IMAGE:'+rid+']]'
        anchors=[b for b in blocks if b.get('type')!='image' and
                 (marker in b.get('text','') or any(marker in c.get('text','') for c in b.get('cells',[])))]
    if not anchors:
        anchors=[b for b in blocks if b.get('type')!='image' and same_position(b.get('position',{}),block.get('position',{}))]
    if len(anchors)!=1:return {}
    context=_repair_source_context(anchors[0],blocks)['source_context']
    if not any(context.values()):return {}
    return {'source_context':context,
            'context_scope':'相邻原文仅帮助判断研究用途和公开引用语境；教学示例中的标志或示意图也可能是研究证据。'
                            '图片身份内容、遮盖位置和坐标只依据当前实际像素，不得由正文推测已遮盖区域或补造图中文字。'}


def _media_request(meta,resource):
    rid=resource['id']
    block=next((b for b in meta['blocks'] if b.get('resource_id')==rid),{})
    research_context=_media_source_context(meta,block,rid)
    payload={'id':rid,'kind':resource['kind'],
        'phase':'locate_privacy_regions' if resource.get('visual_processed') else 'identify',
        'caption':block.get('caption',''),'position':resource.get('position',{}),
        'context':block.get('context_before',[]),**research_context}
    protected=(resource.get('kind')=='table_screenshot' or block.get('native_data_available') or block.get('table_candidate') or
        bool((resource.get('crop_provenance') or {}).get('native_chart_region')) or
        any(b.get('type')=='table' and rid in b.get('screenshot_ids',[]) for b in meta['blocks']) or
        bool(re.match(r'\s*(图\s*\d|表\s*\d|公式|Fig(?:ure)?\b|Table\b)',block.get('caption',''),re.I)))
    return block,research_context,payload,protected


def _media_identity(meta,resource,source,png,payload,agent):
    # Per-paper optimisation only. The persistent StageCache retains full IDs
    # and positions. Do not normalise neighbouring source text or its identity.
    source_hash=meta.get('source_sha256')
    if not isinstance(source_hash,str) or not re.fullmatch('[0-9a-f]{64}',source_hash):return None
    if source.suffix.lower() not in {'.png','.jpg','.jpeg'}:return None
    context={k:v for k,v in payload.items() if k not in {'id','position'}}
    context['position_context']={k:v for k,v in payload.get('position',{}).items()
                                 if k not in {'page','bbox','body_index','block_index'}}
    try:
        context_hash=hashlib.sha256(json.dumps(context,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
        model_hash=hashlib.sha256(json.dumps([getattr(agent,'config',{}),getattr(agent,'cache_scope',None)],
                                            sort_keys=True,ensure_ascii=False).encode()).hexdigest()
    except (TypeError,ValueError):return None
    return {'source_sha256':source_hash,'image_input_sha256':sha(source),
            'visual_input_png_sha256':hashlib.sha256(png).hexdigest(),
            'context_sha256':context_hash,'model_context_sha256':model_hash,'phase':payload['phase']}


def _media_reuse_identity(meta,package,resource,agent):
    _,_,payload,protected=_media_request(meta,resource)
    if protected:return None
    source=(package/resource['local_path']).resolve()
    if not source.is_relative_to(package.resolve()):return None
    if source.suffix.lower() not in {'.png','.jpg','.jpeg'}:return None
    try:
        from evidence_render import load_visual_image
        image=load_visual_image(source);buffer=io.BytesIO();image.save(buffer,format='PNG')
        return _media_identity(meta,resource,source,buffer.getvalue(),payload,agent)
    except (OSError,ValueError,AgentError):return None  # Normal path owns failures.


def _media_identity_key(identity):
    return json.dumps(identity,sort_keys=True) if identity else None


def _media_job(meta,package,resource,agent,progress,gate,clone=True):
    rid=resource['id'];worker=_parallel_agent(agent,gate) if clone else agent
    from privacy_failure_trace import PrivacyFailureTrace
    traced_worker=PrivacyFailureTrace(worker)
    progress('AI 图表识别与脱敏',rid)
    source=(package/resource['local_path']).resolve()
    if not source.is_relative_to(package.resolve()): raise AgentError('图像路径越界')
    try:
        from evidence_render import load_visual_image
        im=load_visual_image(source)
    except AgentError:raise
    except Exception: raise AgentError('图片格式暂不能转换为视觉输入，未自动放行') from None
    buffer=io.BytesIO();im.save(buffer,format='PNG')
    block,research_context,payload,protected=_media_request(meta,resource)
    identity=_media_identity(meta,resource,source,buffer.getvalue(),payload,worker) if not protected else None
    result=ask(traced_worker,'privacy_image',payload,[buffer.getvalue()],defer_rejection=True)
    # Scoped recovery removes member bitmaps only after a complete replacement
    # has been checked. Size alone cannot prove that research evidence is covered.
    discard=result.get('research') is False and not protected and (
        result['decision']=='pass' or result.get('nonresearch_kind') in {'blank','logo','signature','decoration','cover'})
    if discard:
        accept_stage(worker,result)
        job={'resource':resource,'action':'discard','result':result,'agent':worker}
        if identity and result['decision']=='pass' and result.get('nonresearch_kind') in {'blank','logo','signature','decoration','cover'}:
            job['nonresearch_evidence']={**identity,'representative_id':rid,'reused':False,
                'result_sha256':hashlib.sha256(json.dumps(result,sort_keys=True,ensure_ascii=False).encode()).hexdigest(),
                'nonresearch_kind':result['nonresearch_kind']}
            job['reuse_identity']=identity
        return job
    if result['decision']=='reject' and result.get('failure_kind')!='content':
        progress('AI 图像脱敏复核',rid)
        result=ask(traced_worker,'privacy_image',{'id':rid,'kind':resource['kind'],
            'phase':'locate_privacy_regions','caption':block.get('caption',''),
            'position':resource.get('position',{}),'previous_reason':result['reason'],
            'context':block.get('context_before',[]),**research_context},[buffer.getvalue()],defer_rejection=True)
    if result['decision']=='reject':
        raise Rejected(result['reason'],'evidence' if result.get('failure_kind')=='content' else 'privacy',resource.get('position'))
    if result.get('research') is not True: raise Rejected('研究图表不能按装饰图片忽略',position=resource.get('position'))
    if not result['text'].strip() and not result['description'].strip():
        raise Rejected('研究图片没有可检索的识别信息',position=resource.get('position'))
    boxes=[bounds(b,im.size,result.get('coordinate_space','normalized')) for b in result['sensitive_boxes']]
    provenance={'source':'privacy_image','source_sha256':meta.get('source_sha256'),
                'image_input_sha256':sha(source),'image_input_size':list(im.size),
                'visual_input_png_sha256':hashlib.sha256(buffer.getvalue()).hexdigest(),
                'mask_coordinate_space':'normalized',
                'mask_boxes_normalized':copy.deepcopy(boxes),'mask_boxes_pixels':[list(pixel_box(b,im.size)) for b in boxes],
                'mask_rasterization':'pillow_rectangle_inclusive_floor_ceil','position':copy.deepcopy(resource.get('position',{}))}
    draw=ImageDraw.Draw(im)
    for box in boxes: draw.rectangle(pixel_box(box,im.size),fill='black')
    integrity=verify_research_pixels(im,package,rid,resource.get('position'),provenance)
    original_result=result
    if boxes or _visual_recheck_needed(resource):
        previous=resource.get('crop_provenance') or {}
        previous_masks=(previous.get('focused_mask_boxes_normalized') if previous.get('mask_application')=='focused_crop_recheck'
                        else previous.get('page_mask_boxes_normalized'))
        detail_review=bool(boxes or resource.get('masked_regions') or previous_masks
                           or not (result.get('chart_spec') or result.get('structured_table')))
        try:
            result,extra_boxes=_verify_masked_observation(traced_worker,im,resource,block,detail_review=detail_review,research_context=research_context)
        except Rejected as exc:
            # Keep private evidence of the current masked full image. A local
            # review may have failed on a resized tile, so this is not claimed
            # to reproduce that tile's exact model input.
            try:
                safe_rid=re.sub(r'[^A-Za-z0-9_-]','_',str(rid))[:80] or 'resource'
                if re.fullmatch(r'(?i:con|prn|aux|nul|com[1-9]|lpt[1-9])',safe_rid):safe_rid='resource_'+safe_rid
                directory=package/'_review'/'privacy_failure'/safe_rid
                if not directory.resolve().is_relative_to(package.resolve()):raise OSError('Diagnostic path leaves private package')
                directory.mkdir(parents=True,exist_ok=True)
                im.save(directory/'masked-full.png',format='PNG')
                trace={'reason':str(exc)[:2000],'kind':exc.kind,'position':exc.position,
                       'image':'masked-full.png','image_size':list(im.size),
                       'image_scope':'full_masked_image_at_failure','may_differ_from_failing_tile_input':True}
                (directory/'trace.json').write_text(json.dumps(trace,ensure_ascii=False,indent=2),encoding='utf-8')
                traced_worker.write_failure(directory)
            except Exception:
                pass  # Diagnostic I/O must never replace the original rejection.
            raise
        boxes.extend(extra_boxes)
        provenance.update(mask_boxes_normalized=copy.deepcopy(boxes),
                          mask_boxes_pixels=[list(pixel_box(b,im.size)) for b in boxes])
        integrity=verify_research_pixels(im,package,rid,resource.get('position'),provenance)
    masked=io.BytesIO();im.save(masked,format='PNG')
    from visual_knowledge import visual_fields
    observed=visual_fields(result)
    accept_stage(worker,original_result)
    accept_stage(worker,result)
    return {'resource':resource,'action':'approve','result':result,'agent':worker,
            'masked_bytes':masked.getvalue(),'boxes':boxes,'observed':observed,
            'visual_integrity':integrity,
            'rendering':'office_vector_pdf' if source.suffix.lower() in {'.wmf','.emf'} else 'raster'}


def _apply_media_job(meta,package,job):
    r=job['resource'];rid=r['id'];action=job['action'];result=job['result']
    if action=='discard':
        for b in meta['blocks']:
            if b.get('resource_id')==rid: b['excluded_nonresearch']=True
            if 'text' in b: b['text']=b['text'].replace('[[IMAGE:'+rid+']]','')
            for cell in b.get('cells',[]): cell['text']=cell['text'].replace('[[IMAGE:'+rid+']]','')
            b['screenshot_ids']=[value for value in b.get('screenshot_ids',[]) if value!=rid]
        meta['blocks']=[b for b in meta['blocks'] if not b.get('excluded_nonresearch')]
        meta['resources'].remove(r)
        record={'id':rid,'reason':'confirmed_nonresearch_image_removed','position':copy.deepcopy(r.get('position',{}))}
        if job.get('nonresearch_evidence'):record['nonresearch_evidence']=copy.deepcopy(job['nonresearch_evidence'])
        meta.setdefault('excluded_resources',[]).append(record)
        return
    target=package/'assets'/(rid+'.png');target.write_bytes(job['masked_bytes'])
    r.update(local_path=target.relative_to(package).as_posix(),sha256=sha(target),privacy_status='approved',
             privacy_method='ai_region_detection_and_local_mask',visual_processed=True,masked_regions=job['boxes'],
             masked_observation_verified=True)
    r['visual_input_renderer']=job.get('rendering','raster')
    r['visual_integrity']=copy.deepcopy(job['visual_integrity'])
    r['ocr']={'text':result['text'],'verified':True,'status':'ai_recognized','source':'ai_visual_transcription'}
    observed=copy.deepcopy(job['observed']);notes=[]
    table=next((b for b in meta['blocks'] if b.get('resource_id')==rid and b.get('table_candidate')),None)
    if table is not None:
        # Privacy recognition cannot replace the earlier native grid verdict.
        # A different grid needs another geometry check before it can be exported.
        review=table.get('structured_table_review',{})
        if review.get('status')!='image_only' and observed.get('structured_table')!=table.get('structured_table'):
            review={'status':'image_only','reason':'图片复核的网格与已核对的原生行列不一致',
                    'source':'native_geometry_validation'}
        observed.pop('chart_spec',None)
        if review.get('status')=='image_only':
            observed.pop('structured_table',None)
            table['structured_table_review']=copy.deepcopy(review)
            r['structured_table_review']=copy.deepcopy(review)
            notes.append('程序生成：未发布结构化表格（'+review['reason']+'）；原图和原生文字保留。')
        if table.get('table_fragment'):
            notes.append(f"程序生成：原文第 {table['position']['page']} 页的独立续表候选片段；跨页行列关系尚未确认。")
    r['description']={'text':'\n'.join([*notes,result['description']]),'source':'generated'}
    for key in ('chart_spec','structured_table'):r.pop(key,None)
    r.update(copy.deepcopy(observed))
    for b in meta['blocks']:
        if b.get('resource_id')==rid:
            for key in ('chart_spec','structured_table'):b.pop(key,None)
            b.update(privacy_status='approved',ocr=copy.deepcopy(r['ocr']),description=copy.deepcopy(r['description']))
            b.update(copy.deepcopy(observed))


def media_privacy(meta,package,agent,progress,concurrency=None,checkpoint=None,paper_key=''):
    options=concurrency if isinstance(concurrency,dict) else {}
    enabled=bool(options.get('enabled'))
    workers=max(1,int(options.get('images_per_paper',1))) if enabled else 1
    gate=options.get('_llm_gate')
    # Scoped page recognition already sets these flags.  Its output and old
    # checkpoints still need the same pixel gate before this fast path skips AI.
    for resource in meta['resources']:
        for key in ('crop_provenance','visual_integrity'):
            if key in resource:resource[key]=safe_visual_provenance(resource[key])
        if resource.get('privacy_status')=='approved' and resource.get('visual_processed'):
            source=(package/resource['local_path']).resolve()
            if not source.is_relative_to(package.resolve()):raise AgentError('图像路径越界')
            from evidence_render import load_visual_image
            image=load_visual_image(source)
            verify_research_pixels(image,package,resource['id'],resource.get('position'),
                {'source':'approved_resource_recheck','source_sha256':meta.get('source_sha256'),
                 'image_input_sha256':sha(source),'crop_provenance':resource.get('crop_provenance'),
                 'previous_visual_integrity':resource.get('visual_integrity'),
                 'recorded_mask_boxes_normalized':resource.get('masked_regions'),
                 'position':copy.deepcopy(resource.get('position',{}))})
    resources=[r for r in list(meta['resources']) if not (r.get('privacy_status')=='approved' and r.get('visual_processed'))
               or _visual_recheck_needed(r)]
    if checkpoint and paper_key:
        checkpoint.begin_paper(paper_key,options.get('_paper_id',''),[r.get('id') for r in resources])
    failures=[];accepted={}
    def reuse_job(resource):
        identity=_media_reuse_identity(meta,package,resource,agent)
        previous=accepted.get(_media_identity_key(identity))
        if previous is None:return None
        check_stop=getattr(agent,'_check_stop',None)
        if check_stop:check_stop()
        elif getattr(agent,'stop',None) is not None and agent.stop.is_set():raise AgentError('AI 处理已停止')
        progress('重复非研究图片复用',resource['id'])
        return {'resource':resource,'action':'discard','result':copy.deepcopy(previous['result']),
                'nonresearch_evidence':{**previous['nonresearch_evidence'],'reused':True}}
    def apply(job):
        _apply_media_job(meta,package,job)
        if checkpoint and paper_key:checkpoint.image_done(paper_key,job['resource']['id'])
        # Publish the shared decision only after this representative was applied.
        if job.get('reuse_identity'):accepted[_media_identity_key(job['reuse_identity'])]=job
    def failed(resource,exc):
        if checkpoint and paper_key:checkpoint.image_failed(paper_key,resource['id'],str(exc))
        if not enabled:raise exc
        failures.append(exc)
    if workers<=1:
        for r in resources:
            try:
                job=reuse_job(r) or _media_job(meta,package,r,agent,progress,gate,clone=False)
                apply(job)
            except Exception as exc:failed(r,exc)
    else:
        # Only one representative per identical group enters the existing pool.
        # Followers never occupy workers while waiting for that representative.
        groups={}
        for r in resources:
            key=_media_identity_key(_media_reuse_identity(meta,package,r,agent)) or ('unique',r['id'])
            groups.setdefault(key,[]).append(r)
        with ThreadPoolExecutor(max_workers=workers,thread_name_prefix='image') as pool:
            futures={pool.submit(_media_job,meta,package,group[0],agent,progress,gate):(group[0],group[1:])
                     for group in groups.values()}
            while futures:
                for future in as_completed(tuple(futures)):
                    r,followers=futures.pop(future)
                    try:
                        job=future.result();apply(job)
                        agent.processing_metrics.extend(job['agent'].processing_metrics)
                    except Exception as exc:failed(r,exc)
                    for follower in followers:
                        try:
                            reused=reuse_job(follower)
                            if reused:apply(reused)
                            else:futures[pool.submit(_media_job,meta,package,follower,agent,progress,gate)]=(follower,[])
                        except Exception as exc:failed(follower,exc)
    if failures: raise failures[0]


def finish_evidence(meta,package,agent,source,work,max_pages,progress,concurrency=None,checkpoint=None,paper_key=''):
    from evidence_tasks import check_structure, resolve_scoped, finish_issues
    check_structure(meta)
    resolve_scoped(meta,package,source,work,max_pages,agent,progress)
    if checkpoint and paper_key: checkpoint.paper_stage(paper_key,'repair')
    check_structure(meta)
    media_options=dict(concurrency or {})
    if paper_key:
        from knowledge_package import paper_identity
        media_options['_paper_id']=paper_identity(meta)[1]
    media_privacy(meta,package,agent,progress,media_options,checkpoint,paper_key)
    if checkpoint and paper_key: checkpoint.paper_stage(paper_key,'images')
    # Pipeline may defer this single pass until classification has supplied the final title.
    if not meta.pop('_defer_text_privacy',False): text_privacy(meta,agent,progress)
    for r in meta['resources']:
        block=next((b for b in meta['blocks'] if b.get('resource_id')==r['id']),None)
        if block:
            r['ocr']=copy.deepcopy(block.get('ocr',{}));r['description']=copy.deepcopy(block.get('description',{}))
    finish_issues(meta)
    meta.update(review_status='automatic_passed',completeness_verified=False,evaluation_ready=False,automatic_approval=True,
                preprocessing_complete=True,verification_method='native_structure_checks_and_scoped_ai_not_full_source_audit',privacy_status='ai_passed')
    export_package(package,meta)
    if checkpoint and paper_key:
        checkpoint.paper_stage(paper_key,'completed')
    return meta
