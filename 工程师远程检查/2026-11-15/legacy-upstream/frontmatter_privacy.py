"""Mask explicit submitter fields locally, without deleting names globally."""
import re

ROLE=r'(?:Author|Student|Instructor|Supervisor|Tutor|Advis[eo]r|作者|学生姓名|指导教师|指导老师|导师)'
LABEL=re.compile(r'\s*'+ROLE+r'\s*[:：]\s*',re.I)
FIELD_LABEL=re.compile(r'(?<!\w)'+ROLE+r'\s*[:：]\s*',re.I)
STUDENT_NUMBER_FIELD=re.compile(
    r'(?P<label>学[^\S\r\n]*号|(?<![A-Za-z])Student[^\S\r\n]+(?:ID|Number|No\.?))'
    r'(?P<sep>[^\S\r\n]*[:：]?[^\S\r\n]*)'
    r'(?P<number>[0-9０-９]{6,20})(?![0-9０-９A-Za-z])',re.I)
COVER_END=re.compile(r'^\s*(?:#+\s*)?(?:摘要|摘\s+要|abstract|关键词|关键字|key\s*words)\s*[:：]?',re.I)
APPENDIX_HEADING=re.compile(r'^\s*附\s*录(?:\s*[A-Za-zＡ-Ｚａ-ｚ0-9一二三四五六七八九十])?')
SUBMITTER_INTRO=re.compile(r'我(?:是|系)[^，,。；;\n]{1,100}')
APPENDIX_SIGNATURE=re.compile(
    r'^\s*(?:[\u4e00-\u9fff0-9]{2,50})?(?:学生|毕业生|指导教师|指导老师|导师)\s*[:：]\s*'
    r'[\u4e00-\u9fff]{2,6}(?=\s|[，,。；;]|$)')


def _mask_appendix_preamble(text):
    """Remove explicit submitter identity only near an appendix opening."""
    intro=SUBMITTER_INTRO.search(text)
    if intro:
        identity=intro.group()
        rest=text[intro.end():]
        student=(any(word in identity for word in ('学生','毕业生','研究生','本科生'))
                 or ('级' in identity and '班' in identity))
        school=any(word in identity for word in ('学院','大学','专业','年级','届','级','班'))
        purpose=any(word in rest[:100] for word in ('为了','目前','正在','论文','问卷','调查','访谈','课题'))
        if student and school and purpose:
            text=text[:intro.start()]+'[已脱敏]'+rest
    return APPENDIX_SIGNATURE.sub('[已脱敏]',text,count=1)

def _name(value):
    value=value.strip()
    if re.search(r'[A-Za-z]{2,}\.$',value):return False
    return bool(re.fullmatch(r'[\u4e00-\u9fff]{2,6}|[A-Za-z][A-Za-z\u00c0-\u024f\s.\-\x27]{0,79}',value)) and 1<=len(value.split())<=6

def _body(value):
    value=re.sub(r'\s+','',value).lower()
    value=re.sub(r'^(?:[一二三四五六七八九十]+[、.．]|\d+[.．、])','',value)
    return bool(re.match(r'(?:参考文献|references|bibliography|绪论|引言|introduction|文献综述|literaturereview|第[一二三四五六七八九十\d]+章)',value))

def _signature(text):
    labels=list(FIELD_LABEL.finditer(text))
    if not labels or text[:labels[0].start()].strip():return False
    # A complete line must consist only of role/name pairs; never match prose
    # merely because it contains one labelled name or an incomplete last field.
    return all(_name(text[label.end():labels[n+1].start() if n+1<len(labels) else len(text)])
               for n,label in enumerate(labels))


def _terminated_english_signature(text):
    """A final period is a separator only for a complete paired cover line."""
    labels=list(FIELD_LABEL.finditer(text))
    if (len(labels)!=2 or text[:labels[0].start()].strip()
            or [re.sub(r'[\s:：]+','',label.group()).lower() for label in labels]!=['author','instructor']
            or not text.rstrip().endswith('.')):
        return False
    values=[text[labels[0].end():labels[1].start()],text[labels[1].end():].rstrip()[:-1]]
    # Keep single fields, initials, prose and less certain name forms with the
    # existing exact review. Do not relax the general _name punctuation rule.
    return all(_name(value) and 2<=len(value.split())<=4
               and all(re.fullmatch(r"[A-Z][A-Za-z'-]+",word) for word in value.split())
               for value in values)


def _mask_field(text):
    if _signature(text):return '[已脱敏]'
    # A title and a signature can share one PDF/DOCX paragraph. Keep the title.
    lines=text.splitlines(keepends=True)
    for index,line in enumerate(lines):
        if _signature(line.rstrip('\r\n')) or _terminated_english_signature(line.rstrip('\r\n')):
            lines[index]='[已脱敏]'+line[len(line.rstrip('\r\n')):]
    return ''.join(lines)


def mask_cover_student_numbers(text):
    """Only explicitly labelled identity values before the abstract/body."""
    lines=text.splitlines(keepends=True)
    for i,line in enumerate(lines):
        plain=re.sub(r'^\s*#+\s*','',line)
        if COVER_END.match(plain) or _body(plain):break
        if len(line)>350:continue
        lines[i]=mask_student_identity_line(line)
    return ''.join(lines)


def mask_student_identity_line(text):
    value=text.strip()
    field=STUDENT_NUMBER_FIELD.fullmatch(value)
    # A numeric identifier embedded in research prose is not a cover field.
    parenthetical=(value.startswith(('（','(')) and value.endswith(('）',')'))
        and any(word in value for word in ('专业','学院','大学','项目','年级'))
        and not re.search(r'样本|问卷|数据|受访|被试|调查|研究材料|[。！？!?；;]',value))
    found=STUDENT_NUMBER_FIELD.search(value)
    terminal=found and not value[found.end():].rstrip('）)').strip()
    if field or (parenthetical and terminal):
        return STUDENT_NUMBER_FIELD.sub(lambda m:m['label']+m['sep']+'[STUDENT_ID]',text)
    return text

def redact_frontmatter_blocks(meta):
    """Only short, explicitly labelled native paragraphs in front matter.

    Ambiguous fragments still go through the model and exact-span checks.
    A split label/name pair additionally needs adjacent PDF geometry.
    """
    blocks=meta.get('blocks',[]);changed=0;complete_fields={}
    in_contents=False;content_blocks=0;cover_numbers=True
    for index,block in enumerate(blocks):
        text=block.get('text','')
        if not isinstance(text,str):continue
        if not text.strip():continue
        if re.sub(r'\s+','',text).lower() in {'目录','contents','tableofcontents'}:
            in_contents=True;continue
        toc_style=re.fullmatch(r'toc\d*',str(block.get('style','')),re.I)
        if toc_style or (in_contents and re.search(r'\t\s*(?:\d+|[ivxlcdm]+)\s*$',text,re.I)):
            continue
        in_contents=False
        content_blocks+=1
        if content_blocks>60:break
        if _body(text) and len(text.strip())<60:break
        if any(_body(s) for s in block.get('section',[]) if isinstance(s,str)):break
        if block.get('type')!='paragraph' or block.get('native_xml') or block.get('graphic_object'):continue
        if COVER_END.match(text):cover_numbers=False
        masked=_mask_field(text)
        if cover_numbers and len(text)<=350:
            masked=''.join(mask_student_identity_line(line) for line in masked.splitlines(keepends=True))
        if masked!=text:
            complete_fields[text]=masked;block['text']=masked;changed+=1
            # Locator copies belonging to this paragraph must agree with it.
            if 'locator_texts' in block:
                block['locator_texts']=[masked if s==text else s for s in block['locator_texts']]
        elif LABEL.fullmatch(text) and index+1<len(blocks):
            following=blocks[index+1];value=following.get('text','')
            a=block.get('position',{});b=following.get('position',{})
            abox=a.get('bbox');bbox=b.get('bbox')
            if (following.get('type')=='paragraph' and not following.get('native_xml') and not following.get('graphic_object')
                    and isinstance(value,str) and _name(value) and a.get('page') is not None and a.get('page')==b.get('page')
                    and abox and bbox and 0<=bbox[1]-abox[3]<=28 and abs(abox[0]-bbox[0])<=36):
                block['text']='[已脱敏]';following['text']='[已脱敏]';changed+=2
    appendix_window=0
    for block in blocks:
        text=block.get('text','')
        if not isinstance(text,str):continue
        if APPENDIX_HEADING.match(text):
            appendix_window=12
            continue
        if appendix_window<=0:continue
        appendix_window-=1
        if block.get('type')!='paragraph' or block.get('native_xml') or block.get('graphic_object'):continue
        masked=_mask_appendix_preamble(text)
        if masked!=text:
            complete_fields[text]=masked;block['text']=masked;changed+=1
            if 'locator_texts' in block:
                block['locator_texts']=[masked if s==text else s for s in block['locator_texts']]
    # Some DOCX styles misclassify signature lines as headings and copy them
    # into section/context labels. Remove only the same complete role field.
    def clean_copies(value,in_context=False):
        if isinstance(value,list):return [clean_copies(v,in_context) for v in value]
        if isinstance(value,dict):
            for key,child in value.items():
                value[key]=clean_copies(child,in_context or key in {'section','context_before','context_after','locator_texts'})
        return complete_fields.get(value,value) if in_context and isinstance(value,str) else value
    clean_copies(meta)
    return changed
