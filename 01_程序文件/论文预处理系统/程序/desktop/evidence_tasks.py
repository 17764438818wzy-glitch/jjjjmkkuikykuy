"""Local checks and narrowly scoped visual recovery, without whole-document audits."""
import io
import copy
import json
import math
import re
import subprocess
import shutil
import tempfile
import unicodedata
from pathlib import Path
from xml.etree import ElementTree as ET
from PIL import Image, ImageDraw
from caption_location import caption_parts, label_pages


def _normalized(text):
    return re.sub(r'[^\w\u4e00-\u9fff]','',unicodedata.normalize('NFKC',text),flags=re.UNICODE).lower()


def _contents_page(text):
    lines=[s.strip() for s in text.splitlines() if s.strip()]
    heading=any(_normalized(s) in {'目录','图目录','表目录','图表目录','contents','tableofcontents','listoffigures','listoftables'} for s in lines[:6])
    # Financial tables also contain aligned spaces and trailing numbers.
    # Only actual leader dots count as contents entries without a heading.
    entries=sum(bool(re.search(r'(?:\.{3,}|…{2,}|·{3,})\s*\d+\s*$',s)) for s in lines)
    return heading or entries>=4


def _blank_page(page):
    """Only a uniformly white rendered page is safe to skip without AI."""
    if page.get_text().strip():return False
    pix=page.get_pixmap(matrix=__import__('fitz').Matrix(1.5,1.5),alpha=False)
    return bool(pix.samples) and all(v==255 for v in pix.samples)


def visible_text(meta, *, fields=False):
    values=[];seen=set()
    def add(value):
        if isinstance(value,str) and value.strip() and value not in seen:
            values.append(value);seen.add(value)
    def observation(value):
        if isinstance(value,str):add(value)
        elif isinstance(value,list):
            for child in value:observation(child)
        elif isinstance(value,dict):
            for child in value.values():observation(child)
    def walk(value):
        if isinstance(value,list):
            for v in value: walk(v)
        elif isinstance(value,dict):
            for key,v in value.items():
                if key in {'chart_spec','structured_table'}:observation(v)
                elif key in {'native_xml','omml'}:
                    if isinstance(v,str):
                        for node in ET.fromstring(v).iter():
                            add(node.text);add(node.tail)
                            for attribute in node.attrib.values():
                                if re.search(r'[\u4e00-\u9fffA-Za-z]',attribute): add(attribute)
                elif key in {'text','caption','units','title','notes','context_before','context_after','continuation_text','description','latex',
                             'nonresearch_reason','locator_texts','name','categories','values','format_code','category_format','axes','part'}:
                    if isinstance(v,str): add(v)
                    elif isinstance(v,list):
                        for x in v: add(x) if isinstance(x,str) else walk(x)
                    else: walk(v)
                elif key in {'cells','ocr','blocks','formulas','resources','native_chart','series','graphic_members'}: walk(v)
    add(meta.get('title',''));walk(meta)
    return values if fields else '\n'.join(values)


def same_position(left,right):
    if not left or not right: return False
    keys=(set(left)&set(right))&{'body_index','part','id','page','line','container_path','bbox','row','column','block_index','object_ref'}
    return bool(keys-{'part'}) and all(left[k]==right[k] for k in keys)


def _record_unrecovered(meta, target, reason, issues=()):
    gaps=meta.setdefault('unrecovered_objects',[])
    object_id=target.get('id') or target.get('resource_id') or 'unknown'
    position={k:v for k,v in (target.get('position') or {}).items()
              if k in {'page','bbox','body_index','row','column','object_ref','part','line','block_index'}}
    entry=next((item for item in gaps if item.get('id')==object_id),None)
    if entry is None:
        entry={'id':object_id,'type':target.get('type','unknown'),'position':position,'reasons':[],'issue_codes':[]}
        gaps.append(entry)
    if reason not in entry['reasons']:entry['reasons'].append(str(reason))
    entry['reason']='；'.join(entry['reasons'])
    for issue in issues:
        if not isinstance(issue,dict):continue
        if same_position(issue.get('position',{}),target.get('position',{})):
            code=issue.get('code')
            if code and code not in entry['issue_codes']:entry['issue_codes'].append(code)
            issue.update(status='recorded',resolution='unrecovered_object_recorded')
    meta['completeness_status']='partial'


def check_structure(meta):
    from auto_evidence import Rejected
    from note_integrity import assert_note_integrity
    try:
        assert_note_integrity(meta['blocks'])
    except ValueError as exc:
        raise Rejected(str(exc),'evidence') from exc
    ids=set()
    for block in meta['blocks']:
        bid=block.get('id')
        if bid and bid in ids: raise Rejected('正文块编号重复，无法定位证据')
        ids.add(bid)
        if block['type']!='table': continue
        cells=block.get('cells',[]);grid=block.get('grid',[]);rows=block.get('rows',0);cols=block.get('columns',0)
        valid=rows>0 and cols>0 and len(grid)==rows and all(len(row)==cols for row in grid)
        for ri,row in enumerate(grid):
            for ci,index in enumerate(row):
                if index is None: continue  # Explicit absent geometry is never converted to zero.
                if type(index) is not int or not 0<=index<len(cells): valid=False;continue
                cell=cells[index]
                if not isinstance(cell.get('text'),str): valid=False
                if not (cell.get('row',-1)<=ri<cell.get('row',-1)+cell.get('rowspan',1)
                        and cell.get('column',-1)<=ci<cell.get('column',-1)+cell.get('colspan',1)): valid=False
        if not valid: raise Rejected('表格行列或单元格对应关系无效',position=block.get('position'))
        block['grid_validated']=True


def _pdf(source,work):
    from agent_client import AgentError
    import fitz
    source=Path(source)
    if source.suffix.lower()=='.pdf': return fitz.open(source)
    if source.suffix.lower()!='.docx': raise AgentError('此格式无法提供疑难对象原始页证据')
    from verified_pdf import copy_verified_pdf
    verified=copy_verified_pdf(source,work)
    if verified is not None:
        document=fitz.open(verified)
        document.verified_render=__import__('json').loads((Path(work)/'verified_render.json').read_text(encoding='utf-8'))
        return document
    from evidence_render import render_office_pdf
    path=render_office_pdf(source,work)
    return fitz.open(path)


def _graphic_body_bracket(group,blocks,page_texts):
    """Require ordered body anchors on both sides of a drawing, within four pages."""
    pages=[_normalized(p) if not _contents_page(p) else '' for p in page_texts]
    at=next(i for i,b in enumerate(blocks) if b is group)
    anchors=[];caption=False
    for direction in [-1,1]:
        useful=0
        for i in range(at+direction,len(blocks) if direction==1 else -1,direction):
            if abs(i-at)>240:break
            block=blocks[i]
            if block.get('graphic_object') or block.get('position',{}).get('part')!=group.get('position',{}).get('part'):continue
            text=re.sub(r'\[\[(?:IMAGE|OBJECT):.*?\]\]|\[解析说明：.*?\]','',block.get('text',''))
            if not text.strip():continue
            useful+=1
            if useful>16:break
            fragments=re.split(r'\$[^$]*\$',text)
            if direction<0:fragments.reverse()
            found=set()
            for fragment in fragments:
                value=_normalized(fragment)
                if len(value)<12:continue
                needle=value[-70:] if direction<0 else value[:70]
                hits={n for n,page in enumerate(pages) if needle in page}
                if len(hits)==1:found=hits;break
            if not found:continue
            anchors.append(next(iter(found)))
            caption=caption or bool(caption_parts(text) and len(label_pages(text,page_texts,pages))==1)
            break
    if len(anchors)==2 and 0<=anchors[1]-anchors[0]<=3:
        return set(range(anchors[0],anchors[1]+1)),caption
    return set(),False


def _ordinary_table_rows(block):
    """Use proven unmerged rows even when a different row has merged cells."""
    if block.get('source')=='ai_transcription':return []
    cells=block.get('cells',[]);columns=block.get('columns')
    if type(columns) is not int or columns<1:return []
    rows=[]
    for row,indices in enumerate(block.get('grid',[])):
        if not isinstance(indices,list) or len(indices)!=columns:continue
        values=[]
        for column,index in enumerate(indices):
            if type(index) is not int or not 0<=index<len(cells):break
            cell=cells[index]
            if ((cell.get('row'),cell.get('column'))!=(row,column)
                    or cell.get('rowspan')!=1 or cell.get('colspan')!=1
                    or cell.get('continuation_text') or not isinstance(cell.get('text'),str)):break
            values.append(_normalized(cell['text']))
        else:rows.append(''.join(values))
    # Merged grids repeat cell indexes. The preserved OOXML supplies original
    # text order without those repetitions, but only when it agrees with every
    # extracted nonempty cell and contains no nested/drawn content.
    if block.get('source')=='native_ooxml' and isinstance(block.get('native_xml'),str):
        try:root=ET.fromstring(block['native_xml'])
        except ET.ParseError:return rows
        w='{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
        if root.tag!=w+'tbl':return rows
        original=[]
        for cell in root.findall('./'+w+'tr/'+w+'tc'):
            if any(node.tag in {w+'tbl',w+'drawing',w+'pict',w+'object'} for node in cell.iter()):return rows
            text=re.sub(r'\s+','',''.join(node.text or '' for node in cell.iter(w+'t')))
            if text:original.append(text)
        extracted=[re.sub(r'\s+','',cell.get('text','')) for cell in cells
                   if re.sub(r'\s+','',cell.get('text',''))]
        if original and original==extracted:rows.append(_normalized(''.join(original)))
    return rows


def _ordinary_body_label_pages(label,blocks,pages,candidates,page_texts=None):
    """Account for repeated labels using actual ordinary-body text, not keywords."""
    needle=_normalized(label);contexts=set()
    for index,block in enumerate(blocks):
        if block.get('graphic_object'):
            # A longer native label in a separately bracketed, captioned drawing
            # can explain a shared fragment. An additional occurrence remains
            # uncovered below, so floating members cannot disappear this way.
            if page_texts is not None and block.get('graphic_members'):
                bracket,caption=_graphic_body_bracket(block,blocks,page_texts)
                if caption and len(bracket)==1 and bracket & candidates:
                    for member in block['graphic_members']:
                        for value in member.get('locator_texts',[]):
                            context=_normalized(value)
                            if (needle in context and len(context)>len(needle)
                                    and any(context in pages[page] for page in bracket)):
                                contexts.add(context)
            continue
        if block.get('type')=='table':
            variants=_ordinary_table_rows(block)
            if not any(needle in text for text in variants):continue
        elif block.get('type')=='paragraph':
            text=_normalized(block.get('text',''))
            if needle not in text:continue
            variants=[text]
        else:continue
        # Short unstyled headings can still be identified by the exact text of
        # their adjacent ordinary paragraph; do not guess from the heading alone.
        for direction in ([-1,1] if block.get('type')=='paragraph' else []):
            neighbor=index+direction
            if not 0<=neighbor<len(blocks):continue
            other=blocks[neighbor]
            if other.get('graphic_object') or other.get('type')!='paragraph':continue
            if other.get('position',{}).get('part')!=block.get('position',{}).get('part'):continue
            extra=_normalized(other.get('text',''))
            variants.append(extra[-35:]+text if direction<0 else text+extra[:35])
        if block.get('type')=='paragraph' and (block.get('heading_level') or caption_parts(block.get('text',''))):contexts.add(text)
        for variant in variants:
            for match in re.finditer(re.escape(needle),variant):
                # One side may cross a rendered page boundary. The surviving
                # side must supply at least twelve additional source characters.
                context=variant[max(0,match.start()-35):match.end()+35]
                if len(context)>=len(needle)+12:contexts.add(context)
                for size in [12,24,35]:
                    for context in [variant[max(0,match.start()-size):match.end()],
                                    variant[match.start():match.end()+size]]:
                        if len(context)>=len(needle)+12:contexts.add(context)
    # A context that appears twice cannot distinguish a body occurrence from
    # a second drawing occurrence (or another body paragraph).
    contexts={context for context in contexts if sum(page.count(context) for page in pages)==1}
    explained=set()
    for page in candidates:
        text=pages[page]
        occurrences={m.start() for m in re.finditer(re.escape(needle),text)}
        covered=set()
        for context in contexts:
            for match in re.finditer(re.escape(context),text):
                covered.update(at for at in occurrences if match.start()<=at and at+len(needle)<=match.end())
        if occurrences and occurrences<=covered:explained.add(page)
    return explained


def _graphic_context_locations(group,blocks,page_texts):
    bracket,caption=_graphic_body_bracket(group,blocks,page_texts)
    if not bracket:return set(),set(),[]
    local=[_normalized(text) if n in bracket and not _contents_page(text) else ''
           for n,text in enumerate(page_texts)]
    members=0;required=set();ambiguous=[]
    global_pages=[_normalized(text) if not _contents_page(text) else '' for text in page_texts]
    outside=set()
    for member in group['graphic_members']:
        labels=[label for label in member.get('locator_texts',[]) if len(_normalized(label))>=4]
        hits=[_member_label_pages(label,page_texts,local) for label in labels]
        for label in labels:
            external=label_pages(label,page_texts,global_pages)-bracket
            # Floating shapes may extend beyond their paragraph's body anchors.
            # Never drop an exact external label unless every occurrence is
            # independently explained by ordinary source prose on that page.
            outside |= external-_ordinary_body_label_pages(label,blocks,global_pages,external,page_texts)
        hits=[found for found in hits if found]
        if not hits:continue
        members+=1
        for found in hits:
            if len(found)==1:required |= found
            else:ambiguous.append(found)
    if members<2 and not caption:return set(),set(),[]
    # Clipped drawing labels can match distant chapter headings verbatim. Use
    # their independently bracketed region as the candidate, retaining all
    # members for visual checks. Body-anchor pages are not required image pages.
    required |= outside
    candidate=bracket|outside
    return set(range(min(candidate),max(candidate)+1)),required,ambiguous


def _graphic_member_locations(issue,blocks,page_texts):
    from auto_evidence import Rejected
    ref=issue.get('position',{}).get('object_ref')
    group=next((b for b in blocks if ref and b.get('position',{}).get('object_ref')==ref and b.get('graphic_members')),None)
    if not group:return set(),[]
    bracket,required,ambiguous=_graphic_context_locations(group,blocks,page_texts)
    if bracket:return required,ambiguous
    normalize=_normalized
    pages=[normalize(p) if not _contents_page(p) else '' for p in page_texts];required=set();ambiguous=[];located=[];unresolved=[]
    for member in group['graphic_members']:
        labels=list({normalize(t):t for t in member.get('locator_texts',[]) if len(normalize(t))>=4}.values())
        if not labels:continue  # Connectors/short labels have no independent text anchor.
        hits=[_member_label_pages(label,page_texts,pages) for label in labels]
        hits=[found for found in hits if found]
        if not hits:
            unresolved.append(member)
            continue
        unique=set().union(*(found for found in hits if len(found)==1))
        if unique:
            required |= unique;located.append(unique)
        else:
            together=set.intersection(*hits)
            if len(together)==1:
                required |= together;located.append(together)
            else:ambiguous.append(together or set.union(*hits))
    if unresolved:
        support={page:sum(page in found for found in located) for page in set().union(*located)} if located else {}
        winners={page for page,count in support.items() if count>=2 and count==max(support.values())}
        if len(winners)==1:
            # This only selects the candidate page for full visual inspection.
            # It does not mark the missing member recovered; output coverage is
            # still checked after the model sees the original page.
            required |= winners
        else:
            raise Rejected('图形成员文字无法定位到原文页，且没有至少两个独立成员指向同一候选页',
                           position=unresolved[0].get('position'))
    ambiguous=[choices for choices in ambiguous if not choices & required]
    # Repeated short labels can occur in the TOC. Agreement among several members
    # locates their shared graphic page without expanding to every textual mention.
    while ambiguous:
        counts={page:sum(page in choices for choices in ambiguous) for page in set.union(*ambiguous)}
        best=max(counts.values());winners={page for page,count in counts.items() if count==best}
        if len(winners)!=1 or best<min(3,len(ambiguous)) or best<2:break
        required |= winners
        ambiguous=[choices for choices in ambiguous if not choices & winners]
    return required,ambiguous


def _member_label_pages(label,page_texts,normalized_pages):
    """Locate drawing labels despite conversion-only suffix noise.

    Fallbacks are used only to choose pages for visual inspection. They never
    rewrite exported text or assert that the graphic was recovered.
    """
    found=label_pages(label,page_texts,normalized_pages)
    if found:return found
    # Caption numbers and bodies are semantic evidence. Their dedicated matcher
    # already handles broken Word fields; fuzzy fragments must not change them.
    if caption_parts(label):return set()
    source=unicodedata.normalize('NFKC',label)
    cjk=''.join(re.findall(r'[\u3400-\u9fff]',source))
    collapsed=re.sub(r'([\u3400-\u9fff])\1+',r'\1',cjk)
    for candidate in dict.fromkeys([cjk,collapsed]):
        if len(candidate)<4:continue
        exact={i for i,page in enumerate(normalized_pages) if candidate in page}
        if exact:return exact
    if len(collapsed)<4:return set()
    # LibreOffice can omit or alter one text run in a shape. Use the longest
    # surviving CJK fragment only when every equally strong match identifies
    # one unique non-contents page.
    for size in range(min(16,len(collapsed)),3,-1):
        hits=[]
        for start in range(len(collapsed)-size+1):
            fragment=collapsed[start:start+size]
            pages={i for i,page in enumerate(normalized_pages) if fragment in page}
            if pages:hits.append(pages)
        if hits:
            candidates=set().union(*hits)
            return candidates if len(candidates)==1 else set()
    return set()


def _locate(issue,blocks,page_texts):
    from auto_evidence import Rejected
    ref=issue.get('position',{}).get('object_ref')
    group=next((b for b in blocks if ref and b.get('position',{}).get('object_ref')==ref and b.get('graphic_members')),None)
    if group:
        bracket,_,_=_graphic_context_locations(group,blocks,page_texts)
        if bracket:return bracket
    required,ambiguous=_graphic_member_locations(issue,blocks,page_texts)
    try:base=_locate_single(issue,blocks,page_texts)
    except Rejected:
        if not required:raise
        base=set()
    pages=base|required
    for choices in ambiguous:
        if not choices & pages:pages |= choices
    # Retain intervening pages for connecting segments of a drawing split by pagination.
    return set(range(min(pages),max(pages)+1)) if required else pages


def _locate_single(issue,blocks,page_texts):
    from auto_evidence import Rejected
    pos=issue.get('position',{})
    if 'page' in pos:
        page=pos['page']-1
        if 0<=page<len(page_texts): return {page}
        raise Rejected('原文页码超出范围',position=pos)
    indices=[i for i,b in enumerate(blocks) if same_position(pos,b.get('position',{}))]
    if not indices: raise Rejected('疑难对象没有可定位的正文块',position=pos)
    exact=[i for i in indices if pos.get('object_ref') and blocks[i].get('position',{}).get('object_ref')==pos['object_ref']
           and blocks[i].get('type')!='table']
    fragment_tables=[i for i in indices if blocks[i].get('table_picture_fragments')
                     and pos.get('object_ref')==blocks[i].get('position',{}).get('object_ref')]
    at=(fragment_tables or exact or indices)[0]
    # Normalization is for locating evidence only; it never changes exported text or numbers.
    normalize=_normalized
    pages=[normalize(p) if not _contents_page(p) else '' for p in page_texts]
    def text_of(b):
        text=b.get('text','')
        if b['type']=='table':
            cells=b['cells']
            if b is blocks[at] and 'row' in pos and 'column' in pos:
                cells=[c for c in cells if c.get('row')==pos['row'] and c.get('column')==pos['column']]
            text=' '.join(c['text'] for c in cells)
        return re.sub(r'\[\[IMAGE:.*?\]\]|\[解析说明：.*?\]','',text)
    def nearby(direction):
        useful=0
        for i in range(at+direction,len(blocks) if direction==1 else -1,direction):
            if abs(i-at)>240:break
            b=blocks[i]
            if not text_of(b).strip():continue
            yield b
            useful+=1
            if useful>=16:break
    # A note may span several pages. Its opening paragraph cannot locate every
    # object in it; use the object's labels or bounded surrounding evidence.
    # Contents are excluded for body drawings, but an object actually anchored
    # inside a contents entry must use that exact entry, including its page label.
    owner_texts=[]
    for owner in blocks:
        owner_pos=owner.get('position',{})
        if owner_pos.get('object_ref') or not same_position(pos,owner_pos):continue
        if any(owner_pos.get(key)!=value for key,value in pos.items() if key!='object_ref'):continue
        owner_texts.append(owner.get('text',''))
    # Drawings can split one TOC paragraph into several native text blocks.
    # Rejoin only its exact owner; nearby entries and parent containers differ.
    for text in [*owner_texts,''.join(owner_texts)]:
        if not re.search(r'\t[\s.．…·]*\d+\s*$',text):continue
        needle=normalize(text)
        if len(needle)<12:continue
        found={i for i,page in enumerate(page_texts) if _contents_page(page) and needle in normalize(page)}
        if len(found)==1:return found
    labels=list({normalize(s):s for s in blocks[at].get('locator_texts',[]) if len(normalize(s))>=4}.values())
    matches_by_label=[label_pages(label,page_texts,pages) for label in labels]
    scores=[sum(i in hits for hits in matches_by_label) for i in range(len(pages))]
    if scores and max(scores)>=2:
        best=max(scores);winners={i for i,v in enumerate(scores) if v==best}
        second=max([v for v in scores if v<best],default=0)
        if len(winners)==1 and best>=second+2 and best>=min(3,len(labels)):return winners
    def matches(b,tail=False):
        if caption_parts(text_of(b)):
            found=label_pages(text_of(b),page_texts,pages)
            if len(found)==1:return found
        fragments=re.split(r'\$[^$]*\$',text_of(b))
        if tail:fragments=list(reversed(fragments))
        for segment in fragments:
            text=normalize(segment)
            if len(text)<12:continue
            fragment=text[-70:] if tail else text[:70]
            found={i for i,p in enumerate(pages) if fragment in p}
            if len(found)==1:return found
        return set()
    direct=matches(blocks[at])
    end=matches(blocks[at],tail=True)
    # Equation labels are object-specific. A later figure caption cannot locate
    # an earlier formula; include every uniquely matched equation label.
    if issue.get('code')=='formula_review':
        labels=re.findall(r'[（(]\s*(\d+[.\-]\d+)\s*[)）]',text_of(blocks[at]))
        equation_pages=set()
        for label in set(labels):
            pattern=r'\(\s*'+re.escape(label)+r'\s*\)'
            hits={i for i,p in enumerate(page_texts) if not _contents_page(p) and re.search(pattern,unicodedata.normalize('NFKC',p))}
            if len(hits)==1:equation_pages |= hits
        if equation_pages and max(equation_pages)-min(equation_pages)<=3:
            return set(range(min(equation_pages),max(equation_pages)+1))
        if equation_pages:
            raise Rejected('该公式块横跨多页，需要拆分原文定位，未只取第一页',position=pos)
    if direct and end:
        first,last=next(iter(direct)),next(iter(end))
        if 0<=last-first<=3:return set(range(first,last+1))
        if last>first+3:
            raise Rejected('该正文块横跨超过四页，需要更精确的原文定位，未省略中间页',position=pos)
    # Legacy note packages kept an explicit object token in their owner text.
    # That exact owner can be used after object-specific evidence, but another
    # paragraph sharing only the note ID cannot locate a multi-page object.
    if pos.get('part') in {'word/endnotes.xml','word/footnotes.xml'} and pos.get('object_ref'):
        marker='[[OBJECT:'+pos['object_ref']+']]'
        for owner in blocks:
            owner_pos=owner.get('position',{})
            if marker not in owner.get('text','') or any(
                    owner_pos.get(key)!=value for key,value in pos.items() if key!='object_ref'):
                continue
            text=re.sub(r'\[\^(?:endnote|footnote)Reference-?\d+\]:?|\[\[(?:IMAGE|OBJECT):.*?\]\]','',owner['text'])
            needle=normalize(text)
            found={i for i,page in enumerate(pages) if len(needle)>=12 and needle in page}
            if len(found)==1 and all(not hits or bool(found & hits) for hits in matches_by_label):
                return found
    anchors=[]
    for direction in [-1,1]:
        order=nearby(direction)
        for block in order:
            found=matches(block,tail=direction==-1)
            if len(found)==1: anchors.append(next(iter(found)));break
    bracket=set()
    if len(anchors)==2 and 0<=anchors[1]-anchors[0]<=3:
        bracket=set(range(anchors[0],anchors[1]+1))
    if issue.get('code') in {'embedded_object_review','native_chart_visual','pdf_vector_review'}:
        # Figure captions may be duplicated in contents or separated from a
        # floating image by pagination. Use surrounding body anchors as well.
        for b in nearby(1):
            if b.get('graphic_object') and b.get('position',{}).get('object_ref')!=pos.get('object_ref'):break
            if b.get('heading_level'):break
            text=text_of(b).strip()
            if re.match(r'^(图\s*\d|Fig(?:ure)?[.\s]*\d)',text,re.I):
                found=label_pages(text,page_texts,pages)
                if bracket:
                    local=found & bracket
                    if local:return bracket
                if len(found)==1:return found
                break
    if direct:return direct | (end or set())
    if bracket:return bracket
    raise Rejected('无法可靠定位疑难对象所在页，未扩大为全文扫描',position=pos)


def _transcribed_block(entry,bid,position,package,blob,page_masks,source_sha256=None,agent=None):
    from auto_evidence import Rejected,ask,bounds,pixel_box,verify_research_pixels,accept_stage
    from evidence import sha
    kind=entry.get('kind');block={'id':bid,'position':position,'source':'ai_transcription','section':[]}
    if kind in {'table','image'}:
        caption=entry.get('caption')
        if caption is None:caption=''
        if not isinstance(caption,str):raise Rejected('AI 图表标题格式无效',position=position)
    if kind=='text':
        if not isinstance(entry.get('text'),str) or not entry['text'].strip(): raise Rejected('页面转录缺少有效正文',position=position)
        block.update(type='paragraph',text=entry['text'],heading_level=max(0,min(6,int(entry.get('heading_level',0)))))
    elif kind=='table':
        rows=entry.get('rows')
        if not isinstance(rows,list) or not rows or not all(isinstance(r,list) and r and all(isinstance(v,str) for v in r) for r in rows) or len({len(r) for r in rows})!=1 or not any(v.strip() for r in rows for v in r):
            raise Rejected('AI 转录表格行列不完整',position=position)
        n=len(rows[0]);headers=entry.get('header_rows',[0])
        if not isinstance(headers,list) or any(type(v) is not int or not 0<=v<len(rows) for v in headers): raise Rejected('转录表头位置无效',position=position)
        units=entry.get('units');notes=entry.get('notes')
        if units is None:units=''
        if notes is None:notes=[]
        if not isinstance(units,str) or not isinstance(notes,list) or any(not isinstance(v,str) for v in notes):
            raise Rejected('AI 表格单位或注释格式无效',position=position)
        block.update(type='table',rows=len(rows),columns=n,grid=[[ri*n+ci for ci in range(n)] for ri in range(len(rows))],
            cells=[{'row':ri,'column':ci,'rowspan':1,'colspan':1,'text':v} for ri,row in enumerate(rows) for ci,v in enumerate(row)],
            header_rows=headers,complex=False,header_verified=True,caption=caption,units=units,notes=notes)
    elif kind=='image':
        image=Image.open(io.BytesIO(blob)).convert('RGB');w,h=image.size
        box=bounds(entry.get('bbox'),image.size,entry.get('coordinate_space','normalized'))
        if (box[2]-box[0])*(box[3]-box[1])>.9: raise Rejected('整页不能作为独立研究图片',position=position)
        if not isinstance(entry.get('text'),str) or not isinstance(entry.get('description'),str) or not (entry['text'].strip() or entry['description'].strip()): raise Rejected('研究图片缺少本次识别文字或描述',position=position)
        import copy,hashlib
        crop_unit=(entry['bbox'].get('coordinate_space',entry.get('coordinate_space','normalized'))
                   if isinstance(entry.get('bbox'),dict) else entry.get('coordinate_space','normalized'))
        provenance={'source':'scoped_page_recognition','source_sha256':source_sha256,
                    'page_input_sha256':hashlib.sha256(blob).hexdigest(),'page_input_size':list(image.size),
                    'crop_coordinate_space':crop_unit,
                    'crop_bbox_normalized':box,'crop_bbox_pixels':list(pixel_box(box,image.size)),
                    'page_mask_boxes_normalized':copy.deepcopy(page_masks),
                    'page_mask_boxes_pixels':[list(pixel_box(mask,image.size)) for mask in page_masks],
                    'mask_coordinate_source':'page_response_normalized_by_bounds',
                    'mask_rasterization':'pillow_rectangle_inclusive_floor_ceil','position':copy.deepcopy(position)}
        original_crop=image.crop(pixel_box(box,image.size))
        draw=ImageDraw.Draw(image)
        for mask in page_masks:
            draw.rectangle(pixel_box(mask,image.size),fill='black')
        image=image.crop(pixel_box(box,image.size))
        # Page-level masks can mistakenly cover all or part of a research
        # figure. Recheck the original crop whenever mask pixels intersect it;
        # only the focused privacy result may replace the page-level mask.
        # PIL crops exclude the right/bottom edge, while drawn rectangles
        # include it, so use the actual raster bounds for this intersection.
        left,top,right,bottom=provenance['crop_bbox_pixels']
        mask_intersects_crop=any(
            max(left,x0)<=min(right-1,x1) and max(top,y0)<=min(bottom-1,y1)
            for x0,y0,x1,y1 in provenance['page_mask_boxes_pixels'])
        focused_result=None
        if mask_intersects_crop and agent is not None:
            focused_png=io.BytesIO();original_crop.save(focused_png,format='PNG')
            focused_result=ask(agent,'privacy_image',{'id':bid,'kind':'image',
                'phase':'verify_overbroad_page_mask','caption':caption,'position':position},
                [focused_png.getvalue()],defer_rejection=True)
            if focused_result['decision']=='reject':
                raise Rejected(focused_result['reason'],
                               'evidence' if focused_result.get('failure_kind')=='content' else 'privacy',position)
            if focused_result.get('research') is not True:
                raise Rejected('研究图表不能按装饰图片忽略',position=position)
            if not focused_result['text'].strip() and not focused_result['description'].strip():
                raise Rejected('研究图片没有可检索的识别信息',position=position)
            image=original_crop.copy()
            focused_masks=[bounds(mask,image.size,focused_result.get('coordinate_space','normalized'))
                           for mask in focused_result['sensitive_boxes']]
            focused_draw=ImageDraw.Draw(image)
            for mask in focused_masks:
                focused_draw.rectangle(pixel_box(mask,image.size),fill='black')
            provenance['mask_application']='focused_crop_recheck'
            provenance['focused_mask_boxes_normalized']=copy.deepcopy(focused_masks)
            provenance['focused_mask_boxes_pixels']=[list(pixel_box(mask,image.size)) for mask in focused_masks]
            entry=dict(entry)
            for key in ('chart_spec','structured_table'):
                entry.pop(key,None)
                if key in focused_result:entry[key]=focused_result[key]
            entry['text']=focused_result['text'];entry['description']=focused_result['description']
        integrity=verify_research_pixels(image,package,bid,position,provenance)
        if focused_result is not None:accept_stage(agent,focused_result)
        path=package/'assets'/(bid+'.png');image.save(path)
        resource={'id':bid,'kind':'image','position':position,'local_path':path.relative_to(package).as_posix(),'sha256':sha(path),
            'source':'original_page_crop','privacy_status':'approved','visual_processed':True,'privacy_method':'single_page_recognition_and_local_mask',
            'crop_provenance':provenance,'visual_integrity':integrity,
            'ocr':{'text':entry['text'],'verified':True,'source':'ai_visual_transcription','status':'ai_recognized'},
            'description':{'text':entry['description'],'source':'generated'}}
        block.update(type='image',inline=False,resource_id=bid,caption=caption,ocr=resource['ocr'],description=resource['description'])
        from visual_knowledge import visual_fields
        observed=visual_fields(entry)
        block.update(observed);resource.update(__import__('copy').deepcopy(observed))
        return block,resource
    else: raise Rejected('AI 页面转录类型无效',position=position)
    return block,None


def _dedupe_image_entries(entries, sizes):
    """Drop connector-sized crops once a complete crop covers the same target.

    A graphic can be represented by several Office objects. The first visual
    response may return one crop for each arrow/text box, while a focused
    response returns the complete region. Keep the complete crop and publish an
    exact same-page crop only once. ``aliases`` lets completeness checks treat
    a shared crop as recovery for every source object without inserting the
    same image repeatedly into the document.
    """
    from auto_evidence import bounds

    images=[]
    for order,entry in enumerate(entries):
        if not isinstance(entry,dict) or entry.get('kind')!='image':
            continue
        page=entry.get('page')
        if type(page) is not int or page not in sizes:
            images.append((order,entry,None,0.0));continue
        box=bounds(entry.get('bbox'),sizes[page],entry.get('coordinate_space','normalized'))
        area=(box[2]-box[0])*(box[3]-box[1])
        images.append((order,entry,box,area))

    dropped=set();aliases={}
    valid=[item for item in images if item[2] is not None]
    # Prefer a larger crop for the same target/page. This removes the tiny
    # connector crop when the focused pass returns the complete figure.
    for order,entry,box,area in valid:
        after=entry.get('after_id');page=entry.get('page')
        for other_order,other,other_box,other_area in valid:
            if (other_order==order or other_order in dropped or
                    other.get('after_id')!=after or other.get('page')!=page or other_area<=area):
                continue
            # A nested subfigure can be legitimate. Only discard a clearly
            # connector-sized crop; exact duplicate regions are handled below.
            if area > other_area * 0.5:
                continue
            if (other_box[0] <= box[0]+1e-6 and other_box[1] <= box[1]+1e-6 and
                    other_box[2] >= box[2]-1e-6 and other_box[3] >= box[3]-1e-6):
                dropped.add(order);break
    # The same exact crop can be attached to several source objects in one
    # compound flowchart. Keep one physical resource and alias the other target.
    seen={}
    for order,entry,box,area in valid:
        if order in dropped:continue
        key=(entry.get('page'),tuple(round(value,6) for value in box))
        canonical=seen.get(key)
        if canonical is None:
            seen[key]=entry.get('after_id');continue
        dropped.add(order)
        after=entry.get('after_id')
        if after and canonical and after!=canonical:aliases[after]=canonical

    selected={order:entry for order,entry,box,area in images if order not in dropped}
    return [selected.get(order,entry) for order,entry in enumerate(entries) if order not in dropped],aliases


def has_complete_preview(issue,blocks):
    position=issue.get('position',{});ref=position.get('object_ref')
    if not ref:return False
    matching=[b for b in blocks if b.get('position',{}).get('object_ref')==ref and same_position(position,b.get('position',{}))]
    return (any(b.get('preview_covers_object') for b in matching) and
            any(b['type']=='image' for b in matching))


def _add_region(groups,pages,issue):
    """Do not turn a chain of nearby objects into a chapter-wide AI request."""
    from auto_evidence import Rejected
    if not pages or len(pages)>4:
        raise Rejected('单个对象的定位范围超过四页，需要更精确的原文位置',position=issue.get('position'))
    choices=[g for g in groups if pages & g['pages'] and len(pages | g['pages'])<=4]
    if choices:
        best=max(choices,key=lambda g:len(pages & g['pages']))
        best['pages'] |= pages;best['issues'].append(issue)
    else:groups.append({'pages':set(pages),'issues':[issue]})


def _pdf_table_context(block,pdf):
    if not block.get('table_candidate'):
        return {}
    position=block['position'];page=pdf[position['page']-1]
    x0,y0,x1,y1=position['bbox']
    return {'table_candidate':True,'table_fragment':block.get('table_fragment',False),
            'table_bbox':[x0/page.rect.width,y0/page.rect.height,x1/page.rect.width,y1/page.rect.height],
            'caption':block.get('caption',''),'native_text':'\n'.join(block.get('locator_texts',[])),
            'possible_previous_table':block.get('possible_previous_table')}


def _pdf_table_crop_position(target,entry,page,image_size):
    from auto_evidence import Rejected,bounds
    box=bounds(entry.get('bbox'),image_size,entry.get('coordinate_space','normalized'))
    points=[box[0]*page.rect.width,box[1]*page.rect.height,box[2]*page.rect.width,box[3]*page.rect.height]
    expected=target['position']['bbox']
    if (points[0]>expected[0]+2 or points[1]>expected[1]+2
            or points[2]<expected[2]-2 or points[3]<expected[3]-2):
        raise Rejected('PDF 表格原图区域未覆盖完整候选表格',position=target['position'])
    return points


def _check_pdf_table_recovery(target,recovered,ignored,pdf,resources):
    from auto_evidence import Rejected
    from collections import Counter
    position=target['position']
    if target['id'] in ignored:
        raise Rejected('已识别的 PDF 表格或续表片段不能作为装饰跳过',position=position)
    images=[b for b in recovered if b['type']=='image']
    if not images:
        raise Rejected('PDF 表格恢复没有保留完整原图截图',position=position)
    original='\n'.join(target.get('locator_texts',[]))
    compact=lambda text:re.sub(r'\s+','',text)
    numeric=re.compile(r'(?<![\d.])(?:[（(]\s*)?[+\-−]?\d+(?:[.,]\d+)*(?:[%％])?(?:\s*[）)])?')
    numbers=lambda text:Counter(compact(value) for value in numeric.findall(text))
    original_chars=Counter(compact(original));original_numbers=numbers(original)
    observed='\n'.join(b.get('ocr',{}).get('text','') for b in images)
    if original_chars-Counter(compact(observed)) or original_numbers-numbers(observed):
        raise Rejected('PDF 表格识别遗漏或改变原生文字、数值字符串',position=position)
    datasets=[]
    for block in recovered:
        if block.get('structured_table'):
            datasets.append((block['structured_table']['rows'],block['structured_table'].get('header_rows',[])))
        if block['type']=='table':
            datasets.append(([[block['cells'][index]['text'] for index in row] for row in block['grid']],block.get('header_rows',[])))
    reason='';native=None
    if datasets:
        import fitz
        from pdf_layout import validated_text_grid
        clip=fitz.Rect(position['bbox']);page=pdf[position['page']-1]
        previous=target.get('possible_previous_table')
        edges=None
        if target.get('table_fragment') and previous and previous.get('page')==position['page']-1:
            earlier=validated_text_grid(pdf[previous['page']-1],fitz.Rect(previous['bbox']))
            if earlier:
                edges=[clip.x0,*[box[0] for box in earlier['cell_boxes'][0][1:]],clip.x1]
        native=validated_text_grid(page,clip,column_edges=edges)
    for rows,headers in datasets:
        text='\n'.join(' '.join(row) for row in rows)
        if original_chars!=Counter(compact(text)) or numbers(text)!=original_numbers:
            reason='模型网格包含新增、遗漏或改变的原文字符或数值'
        elif target.get('table_fragment') and headers:
            reason='续表片段的表头角色尚未由原生证据确认'
        elif native is None:
            reason='原生行列几何不足以证明全部单元格的对应关系'
        elif [[compact(value) for value in row] for row in rows]!=[[compact(value) for value in row] for row in native['values']]:
            reason='模型网格与原生单元格的行列位置或文字不一致'
        if reason:break
    if reason:
        # Preserve the checked original image and native text, but never export
        # a guessed CSV merely because the same numbers occur somewhere in it.
        recovered[:]=[block for block in recovered if block['type']!='table']
        for block in images:
            block.pop('structured_table',None)
            block['structured_table_review']={'status':'image_only','reason':reason,'source':'native_geometry_validation'}
            block['description']['text']='程序生成：未发布结构化表格（'+reason+'）；原图和原生文字保留。\n'+block['description']['text']
            for resource in resources:
                if resource['id']==block.get('resource_id'):
                    resource.pop('structured_table',None)
                    resource['description']=dict(block['description'])
                    resource['structured_table_review']=dict(block['structured_table_review'])


def _repair_source_context(block,blocks):
    """Keep the original anchors that distinguish otherwise identical placeholders."""
    index=next((n for n,value in enumerate(blocks) if value is block),None)
    context={'before':[],'after':[]}
    if index is not None:
        for direction,key in ((-1,'before'),(1,'after')):
            for n in range(index+direction,-1 if direction<0 else len(blocks),direction):
                neighbor=blocks[n]
                # A different drawing is a boundary, not context for this one.
                if neighbor.get('graphic_object') or neighbor.get('unresolved_object'):
                    break
                text=neighbor.get('text')
                if not isinstance(text,str) or not text.strip():continue
                excerpt=text[-800:] if direction<0 else text[:800]
                context[key].append({'id':neighbor['id'],'text':excerpt,
                    'position':copy.deepcopy(neighbor.get('position',{})),
                    'excerpt_truncated':len(text)>800})
                if len(context[key])==2:break
            if direction<0:context[key].reverse()
    return {'position':copy.deepcopy(block.get('position',{})),
            'caption':block.get('caption',''),'source_context':context}


def _simple_table_cells(table,*,native=False):
    """Return cells in proven row/column order; merged/unknown grids are excluded."""
    rows=table.get('rows');columns=table.get('columns')
    cells=table.get('cells',[]);grid=table.get('grid',[])
    if (type(rows) is not int or type(columns) is not int or rows<1 or columns<1
            or len(cells)!=rows*columns or len(grid)!=rows):return None
    if native and (table.get('source')=='ai_transcription'
                   or type(table.get('position',{}).get('page')) is not int):return None
    ordered=[];seen=set()
    for row,indices in enumerate(grid):
        if not isinstance(indices,list) or len(indices)!=columns:return None
        for column,index in enumerate(indices):
            if type(index) is not int or not 0<=index<len(cells) or index in seen:return None
            cell=cells[index];seen.add(index)
            if ((cell.get('row'),cell.get('column'))!=(row,column)
                    or cell.get('rowspan')!=1 or cell.get('colspan')!=1
                    or cell.get('continuation_text') or not isinstance(cell.get('text'),str)):return None
            if native:
                position=cell.get('position',{});box=position.get('bbox',[])
                if (cell.get('missing_geometry') is not False
                        or position.get('page')!=table['position']['page']
                        or not isinstance(box,(list,tuple)) or len(box)!=4
                        or any(type(v) not in (int,float) or not math.isfinite(v) for v in box)
                        or box[0]>=box[2] or box[1]>=box[3]):return None
            ordered.append(cell)
    return ordered


def _preserve_replacement_cells(original,recovered):
    from auto_evidence import Rejected
    from collections import Counter
    def reject():
        raise Rejected('疑难表格转录遗漏或改变已提取单元格',position=original.get('position'))
    native=_simple_table_cells(original,native=True)
    tables=[block for block in recovered if block['type']=='table']
    if native is None:
        # Unproved or merged layouts retain the existing exact-text requirement.
        old=Counter(text for c in original['cells'] for text in [c['text']]+[v['text'] for v in c.get('continuation_text',[])] if text)
        new=Counter(c['text'] for table in tables for c in table['cells'])
        if old-new:reject()
        return
    if (len(tables)!=1 or (tables[0].get('rows'),tables[0].get('columns'))
            !=(original['rows'],original['columns'])):reject()
    observed=_simple_table_cells(tables[0])
    if observed is None:reject()
    for old,new in zip(native,observed):
        before=old['text'];after=new['text']
        if before==after:continue
        if before.replace('\r\n','\n').replace('\r','\n')==after.replace('\r\n','\n').replace('\r','\n'):continue
        # Only CR/LF layout differences qualify. Spaces/tabs remain significant,
        # and numeric line breaks cannot be assumed to mean a single number.
        if (re.sub(r'[\r\n]','',before)!=re.sub(r'[\r\n]','',after)
                or any(re.search(r'\d[\r\n]|[\r\n]\d',text) for text in (before,after))):reject()
    # Commit only after every corresponding cell has passed; never rewrite
    # native bytes with the model's spelling or whitespace normalization.
    for old,new in zip(native,observed):new['text']=old['text']


def resolve_scoped(meta,package,source,work,max_pages,agent,progress):
    from auto_evidence import Rejected,ask,bounds,accept_stage
    import fitz
    substantive={'embedded_object_review','native_chart_visual','body_container','scanned_page','text_markup_review','image_missing','formula_review',
                 'nested_table','merged_cell_content','merge_without_start','pdf_cell_relationship_review','pdf_vector_review'}
    needed=[]
    for issue in meta['issues']:
        if issue['code'] not in substantive: continue
        if issue['code']=='image_missing': raise Rejected('原图片资源缺失或为不可用外链',position=issue.get('position'))
        if issue['code']=='text_markup_review': raise Rejected('文本图表尚无可靠原生结构；需原始 Word/PDF 文件',position=issue.get('position'))
        if issue['code']=='embedded_object_review' and has_complete_preview(issue,meta['blocks']):
            issue.update(status='resolved',resolution='native_preview_will_be_recognized_once');continue
        needed.append(issue)
    if not needed: return
    with _pdf(source,work) as pdf:
        page_texts=[p.get_text() for p in pdf]
        groups=[];issue_pages={};issue_required={};issue_coverage={}
        for issue in needed:
            if issue['code']=='scanned_page':
                number=issue.get('position',{}).get('page',0)
                if type(number) is int and 1<=number<=len(pdf) and _blank_page(pdf[number-1]):
                    # Remove only the verified blank page's blocks/resources.
                    old=[b for b in meta['blocks'] if b.get('position',{}).get('page')==number]
                    removed={b.get('resource_id') for b in old}
                    meta['blocks']=[b for b in meta['blocks'] if b not in old]
                    meta['resources']=[r for r in meta['resources'] if r['id'] not in removed]
                    issue.update(status='resolved',resolution='uniform_white_page_omitted',resolved_pages=[number])
                    continue
            from verified_pdf import verified_issue_pages
            verified=verified_issue_pages(getattr(pdf,'verified_render',None),issue,meta['blocks'],len(pdf))
            pages=verified if verified is not None else set(_locate(issue,meta['blocks'],page_texts))
            issue_pages[id(issue)]={n+1 for n in pages}
            if verified is not None:
                # Every native member was inspected on these exact PDF pages.
                # Keep every verified page as required evidence for extraction.
                required,ambiguous=verified,[]
            else:
                required,ambiguous=_graphic_member_locations(issue,meta['blocks'],page_texts)
            if any(b.get('table_picture_fragments') and b.get('position')==issue.get('position') for b in meta['blocks']):
                # These are whole native tables, not individually labelled
                # drawing members. Every candidate page must be retained before
                # their original picture pieces can be superseded.
                required,ambiguous=pages,[]
            issue_required[id(issue)]={n+1 for n in required}
            issue_coverage[id(issue)]=[{n+1 for n in choices} for choices in ambiguous]
            _add_region(groups,pages,issue)
        if len(set().union(*(g['pages'] for g in groups)))>max_pages: raise Rejected(f'实际需识别的页数超过配置上限 {max_pages}，未截断内容')
        for group in sorted(groups,key=lambda g:min(g['pages'])):
            numbers=sorted(group['pages']);issues=group['issues'];number=numbers[0]
            if len(numbers)>4:raise Rejected('图形定位范围超过四页，需要更精确的原文位置；未扩大为全文扫描',position=issues[0].get('position'))
            page_ids=[n+1 for n in numbers]
            progress('AI 局部页面识别',f'原文第 {"、".join(map(str,page_ids))} 页；同一候选区域合并识别一次')
            blobs={n+1:pdf[n].get_pixmap(matrix=fitz.Matrix(1.5,1.5)).tobytes('png') for n in numbers}
            sizes={p:Image.open(io.BytesIO(blob)).size for p,blob in blobs.items()}
            scanned=any(i['code']=='scanned_page' for i in issues)
            targets=[];target_pages={};target_required={};target_coverage={}
            for i in issues:
                matched=[b for b in meta['blocks'] if same_position(i.get('position',{}),b.get('position',{})) and b.get('source')!='native_chart_cache']
                ref=i.get('position',{}).get('object_ref')
                exact=[b for b in matched if ref and b.get('position',{}).get('object_ref')==ref]
                fragment_tables=[b for b in exact if b.get('table_picture_fragments')]
                for b in fragment_tables or exact or matched:
                    if b not in targets:targets.append(b)
                    allowed=issue_pages[id(i)]
                    target_pages[b['id']]=target_pages.get(b['id'],allowed) & allowed
                    target_required.setdefault(b['id'],set()).update(issue_required[id(i)])
                    target_coverage.setdefault(b['id'],[]).extend(issue_coverage[id(i)])
            if scanned:
                if len(numbers)!=1:raise Rejected('扫描页与跨页对象范围冲突，未混合覆盖正文')
                result=ask(agent,'extract_page',{'page':number+1,'position':{'page':number+1}},[blobs[number+1]])
                entries=result['blocks']
            else:
                from native_graphic_context import source_graphic_context
                context=[{'id':b['id'],'type':b['type'],'text':b.get('text',''),
                          **_repair_source_context(b,meta['blocks']),
                          **source_graphic_context(source,b,meta.get('resources',[])),
                          'native_data_available':b.get('native_data_available',False),
                          'graphic_object':b.get('graphic_object',False),'candidate_pages':sorted(target_pages[b['id']]),
                          'graphic_members':b.get('graphic_members',[]),
                          'graphic_region':b.get('graphic_region',False),
                          'requires_table_replacement':b['type']=='table' and any(i['code'] in {'merged_cell_content','merge_without_start','pdf_cell_relationship_review'} and same_position(i.get('position',{}),b.get('position',{})) for i in issues),
                          'required_pages':sorted(target_required[b['id']]),
                          'member_page_options':[sorted(p & target_pages[b['id']]) for p in target_coverage[b['id']]],
                          'cells':[{k:v for k,v in c.items() if k in {'row','column','rowspan','colspan','text','continuation_text'}} for c in b.get('cells',[])],
                          **_pdf_table_context(b,pdf)} for b in targets]
                graphic_only=bool(targets) and all(b.get('graphic_object') for b in targets)
                result=ask(agent,'repair',{'page':number+1,'pages':page_ids,'issues':issues,'known_blocks':context,
                    'scope':'候选页合并提供；每个对象只在实际出现页恢复一次，不要求每页都有该对象。graphic_region=true 是连续图形区域，graphic_members是来源成员，不是一成员一张图。箭头、文字框和主体可以共同组成一张完整图；若区域内实际有多张图，须全部分别保留，不能遗漏或补造。native_data_available=true 时数值已原样提取，只需保留研究图及图义，不重新转录或估算数据。',
                    'position':{'pages':page_ids}},[blobs[p] for p in page_ids],defer_rejection=graphic_only)
                graphic_rejection=result['reason'] if result['decision']=='reject' else ''
                accepted_results=[] if graphic_rejection else [result]
                def missing_graphics(response):
                    entries=response.get('insertions',[]);ignored={e.get('after_id') for e in response.get('nonresearch_objects',[]) if isinstance(e,dict)}
                    missing=[]
                    for target,ctx in zip(targets,context):
                        if not target.get('graphic_object') or target['id'] in ignored:continue
                        images=[e for e in entries if isinstance(e,dict) and e.get('after_id')==target['id'] and e.get('kind')=='image']
                        covered={e.get('page',page_ids[0] if len(page_ids)==1 else None) for e in images}
                        if (not images or target_required[target['id']]-covered or
                                any(not choices & covered for choices in target_coverage[target['id']])):
                            missing.append(ctx)
                    return missing
                missing=missing_graphics(result) if not graphic_rejection else []
                if missing:
                    focused=ask(agent,'repair',{'page':number+1,'pages':page_ids,'issues':issues,'known_blocks':missing,
                        'scope':'上一轮没有完整覆盖这些研究图形。仅处理这里列出的缺失对象：每个对象必须返回原图 image；若页中确认没有该研究图，则列入 nonresearch_objects 并说明可核对的原因。不得用文字段落替代原图，不得猜测图中数值。',
                        'position':{'pages':page_ids}},[blobs[p] for p in page_ids],defer_rejection=True)
                    if focused['decision']=='pass':accepted_results.append(focused)
                responses=accepted_results
                entries=[]
                for response in responses:
                    for entry in response.get('insertions',[]):
                        if isinstance(entry,dict):
                            entry=dict(entry);entry.setdefault('coordinate_space',response.get('coordinate_space','normalized'))
                        entries.append(entry)
            if scanned:
                accepted_results=[result];responses=accepted_results
            page_masks={}
            for response in responses:
                if len(numbers)>1:
                    if response.get('sensitive_boxes'):raise Rejected('多页响应的敏感区域缺少所属页码')
                    current={}
                    for mask in response.get('page_masks',[]):
                        if not isinstance(mask,dict) or type(mask.get('page')) is not int or mask['page'] not in page_ids or mask['page'] in current or not isinstance(mask.get('boxes'),list):
                            raise Rejected('敏感区域页码无效')
                        current[mask['page']]=[bounds(b,sizes[mask['page']],mask.get('coordinate_space',response.get('coordinate_space','normalized'))) for b in mask['boxes']]
                    if set(current)!=set(page_ids):raise Rejected('多页响应没有逐页确认敏感区域')
                    for page,boxes in current.items():page_masks.setdefault(page,[]).extend(boxes)
                else:
                    page_masks.setdefault(page_ids[0],[]).extend(bounds(b,sizes[page_ids[0]],response.get('coordinate_space','normalized')) for b in response.get('sensitive_boxes',[]))
            entries,entry_aliases=_dedupe_image_entries(entries,sizes) if not scanned else (entries,{})
            additions=[];replacement={};replace_ids=set();native_regions={};native_tables={}
            for offset,entry in enumerate(entries):
                if not isinstance(entry,dict): raise Rejected('AI 页面内容格式无效',position={'page':number+1})
                entry=dict(entry)
                actual=entry.get('page',page_ids[0] if len(page_ids)==1 else None)
                if type(actual) is not int or actual not in page_ids:raise Rejected('恢复内容页码不属于已提供原文页')
                after_id=entry.get('after_id')
                if not scanned and after_id not in {b['id'] for b in targets}: raise Rejected('补充内容没有对应原文位置',position={'page':number+1})
                if not scanned and actual not in target_pages[after_id]:raise Rejected('恢复内容页码不属于该对象的候选页',position={'page':actual,'after_id':after_id})
                operation=entry.get('operation','append')
                if operation not in {'append','replace_table'}: raise Rejected('局部恢复操作类型无效',position={'page':number+1})
                if operation=='replace_table':
                    target=next((b for b in targets if b['id']==after_id),{})
                    if scanned or entry.get('kind')!='table' or target.get('type')!='table' or after_id in replace_ids:
                        raise Rejected('整表替换目标无效',position={'page':number+1})
                    replace_ids.add(after_id)
                bid=f'visual_p{actual}_{offset+1}'
                position={'page':actual,'after_id':after_id}
                target=next((b for b in targets if b['id']==after_id),{})
                native_region=None;native_table=None
                if target.get('table_picture_fragments') and entry.get('kind')=='image':
                    from native_table_region import find_native_table_region
                    if after_id not in native_tables:
                        native_tables[after_id]=find_native_table_region(target,pdf,target_pages[after_id])
                    proven_table=native_tables[after_id]
                    if not proven_table or proven_table['page']!=actual:
                        _record_unrecovered(meta,target,'完整原表的原生单元格与 PDF 行列边界无法唯一核验；保留原图片成员',issues)
                        continue
                    entry['bbox']=list(proven_table['bbox']);entry['coordinate_space']='normalized'
                    native_table=copy.deepcopy(proven_table['provenance'])
                if (entry.get('kind')=='image' and target.get('graphic_object')
                        and Path(source).suffix.lower()=='.docx'
                        and sum(e.get('kind')=='image' and e.get('after_id')==after_id for e in entries if isinstance(e,dict))==1):
                    from native_chart_region import find_native_chart_region
                    if after_id not in native_regions:
                        native_regions[after_id]=find_native_chart_region(source,target,pdf)
                    proven=native_regions[after_id]
                    if proven and proven['page_index']==actual-1:
                        # Only a unique original object boundary can supersede
                        # model geometry. Privacy and completeness still apply.
                        entry['bbox']=list(proven['bbox']);entry['coordinate_space']='normalized'
                        native_region={k:copy.deepcopy(v) for k,v in proven['provenance'].items() if k!='categories'}
                if target.get('table_candidate') and entry.get('kind')=='image':
                    position['bbox']=_pdf_table_crop_position(target,entry,pdf[actual-1],sizes[actual])
                block,resource=_transcribed_block(entry,bid,position,package,blobs[actual],page_masks[actual],meta.get('source_sha256'),agent)
                if native_region and resource:
                    resource['crop_provenance']['native_chart_region']=native_region
                if native_table and resource:
                    resource['crop_provenance']['native_table_region']=native_table
                if target.get('native_data_available') and block['type']=='image':
                    block['native_data_available']=True
                if target.get('table_candidate') and block['type']=='image':
                    block['table_candidate']=True;block['table_fragment']=target.get('table_fragment',False)
                    block['possible_previous_table']=target.get('possible_previous_table')
                    if block['table_fragment']:
                        block['description']['text']=f'程序生成：原文第 {actual} 页的独立续表候选片段；跨页行列关系尚未确认。\n'+block['description']['text']
                if resource: meta['resources'].append(resource)
                additions.append(block)
                if after_id: replacement.setdefault(after_id,[]).append(block)
            if not scanned:
                ignored={}
                for response in responses:
                    for entry in response.get('nonresearch_objects',[]):
                        if not isinstance(entry,dict) or entry.get('after_id') not in {b['id'] for b in targets} or not isinstance(entry.get('reason'),str) or not entry['reason'].strip():
                            raise Rejected('非研究对象缺少原文位置及原因')
                        ignored[entry['after_id']]=entry['reason']
                for b in targets:
                    recovered=replacement.get(b['id'],[])
                    alias=entry_aliases.get(b['id'])
                    if not recovered and alias:
                        recovered=replacement.get(alias,[])
                    if b.get('table_candidate'):
                        if not recovered and b['id'] not in ignored:
                            _record_unrecovered(meta,b,'疑难表格未返回原图或结构化恢复结果',issues)
                        else:
                            _check_pdf_table_recovery(b,recovered,ignored,pdf,meta['resources'])
                    if b.get('native_data_available') and not any(e['type']=='image' for e in recovered):
                        _record_unrecovered(meta,b,'原生数据已提取但原图证据未保留',issues)
                    if b.get('graphic_object') and b['id'] not in ignored and not any(e['type']=='image' for e in recovered):
                        reason=('研究图形无法从候选页可靠恢复：'+graphic_rejection if graphic_rejection else
                                '研究图形未返回可核对的原图证据')
                        _record_unrecovered(meta,b,reason,issues)
                    if b.get('graphic_object') and b['id'] not in ignored and any(e['type']=='image' for e in recovered):
                        covered_pages={e['position']['page'] for e in recovered if e['type']=='image'}
                        if target_required[b['id']]-covered_pages:
                            _record_unrecovered(meta,b,'跨页图形未保留所有已定位成员页的原图证据',issues)
                        if any(not choices & covered_pages for choices in target_coverage[b['id']]):
                            _record_unrecovered(meta,b,'仍有图形成员的候选页未返回对应原图证据',issues)
                    if (b.get('graphic_object') or b.get('unresolved_object')) and not recovered and b['id'] not in ignored:
                        _record_unrecovered(meta,b,'对象未恢复，也没有明确的非研究用途说明',issues)
                    if b['id'] in ignored:b['nonresearch_reason']=ignored[b['id']]
                # A member bitmap is only part of a compound drawing. Once the complete
                # original drawing was processed, keep that evidence and do not scan its pieces again.
                covered=[b for b in targets if b.get('graphic_object') and
                    (b['id'] in ignored or any(e['type']=='image' for e in replacement.get(b['id'],[]) or replacement.get(entry_aliases.get(b['id'],''),[]))) and
                    (not b.get('table_picture_fragments') or
                     (b['id'] not in ignored and not any(gap.get('id')==b['id'] for gap in meta.get('unrecovered_objects',[]))))]
                superseded={b.get('resource_id') for b in meta['blocks'] if b['type']=='image' and any(
                    g.get('position',{}).get('object_ref') and
                    b.get('position',{}).get('object_ref')==g['position']['object_ref'] and
                    same_position(g['position'],b.get('position',{})) for g in covered)}-{None}
                if superseded:
                    meta['resources']=[r for r in meta['resources'] if r['id'] not in superseded]
                    meta['blocks']=[b for b in meta['blocks'] if b.get('resource_id') not in superseded]
                    def strip_member_markers(value):
                        if isinstance(value,str):
                            for rid in superseded:value=value.replace('[[IMAGE:'+rid+']]','')
                            return value
                        if isinstance(value,list):return [strip_member_markers(v) for v in value]
                        if isinstance(value,dict):
                            for key,v in list(value.items()):value[key]=strip_member_markers(v)
                        return value
                    strip_member_markers(meta['blocks'])
            if scanned:
                old=[b for b in meta['blocks'] if b.get('position',{}).get('page')==number+1]
                old_resources={b.get('resource_id') for b in old if b['type']=='image'}
                meta['resources']=[r for r in meta['resources'] if r['id'] not in old_resources]
                at=next((i for i,b in enumerate(meta['blocks']) if b.get('position',{}).get('page',number+2)>=number+1),len(meta['blocks']))
                meta['blocks']=[b for b in meta['blocks'] if b not in old]
                meta['blocks'][at:at]=additions
            else:
                updated=[]
                for block in meta['blocks']:
                    extra=replacement.get(block['id'],[])
                    if block['id'] in replace_ids:
                        _preserve_replacement_cells(block,extra)
                    else:
                        if block['type']=='table' and any(i['code'] in {'merged_cell_content','merge_without_start','pdf_cell_relationship_review'} and same_position(i.get('position',{}),block.get('position',{})) for i in issues):
                            raise Rejected('疑难表格未返回完整的行列恢复结果',position=block.get('position'))
                        if block in targets and 'text' in block:
                            block['text']=re.sub(r'\[解析说明：[^\]]+\]','',block['text'])
                        updated.append(block)
                    updated.extend(extra)
                meta['blocks']=updated
            for issue in issues:
                if issue.get('status')!='recorded':
                    issue.update(status='resolved',resolution='single_scoped_visual_recognition',resolved_pages=page_ids)
            check_structure(meta)
            for accepted in accepted_results:accept_stage(agent,accepted)
    # References follow the final retained order; native paragraphs are not rewritten.


def finish_issues(meta):
    from auto_evidence import Rejected
    notes={'table_header_inferred':'header_role_inferred_original_coordinates_preserved',
           'list_numbering_review':'original_list_definition_preserved',
           'pdf_layout_review':'native_page_coordinates_preserved_not_independent_audit',
           'tracked_changes':'current_visible_revision_used',
           'source_comparison_required':'replaced_by_native_structure_and_scoped_processing',
           'complex_table_screenshot_missing':'native_cell_grid_preserved_no_full_page_screenshot_required'}
    for issue in meta['issues']:
        if issue.get('status') in {'resolved','recorded'}: continue
        code=issue['code']
        if code in notes: issue.update(status='recorded',resolution=notes[code])
        elif code in {'image_ocr_review','media_privacy_review'}:
            if any(r.get('privacy_status')!='approved' for r in meta['resources']): raise Rejected('图片处理未完成',position=issue.get('position'))
            issue.update(status='resolved',resolution='single_image_recognition_and_local_redaction')
        elif code in {'table_screenshot_match_review','embedded_object_page_review'}:
            issue.update(status='recorded',resolution='local_reference_only')
        else: raise Rejected('仍有未解决的解析问题：'+issue.get('message',code),position=issue.get('position'))
