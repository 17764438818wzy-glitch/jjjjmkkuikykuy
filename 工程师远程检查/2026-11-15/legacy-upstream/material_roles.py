"""Deterministic document purpose gate. No model, upload or deletion at import."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import re
import tempfile
import unicodedata
import zipfile

from engine import OFFICE_FORMATS, digest, write_json
from screening import nonpaper_heading, similarity_report

VERSION='program-material-role/1'
PROCESS_FORM=re.compile(r'(?:开题报告|任务书|答辩(?:记录(?:单|表)?|审批表|申请表|成绩(?:单|表)?|评分表)|'
    r'(?:毕业(?:论文|设计))?(?:审批表|申请表|审查表|审核表|评阅表|评审表|进度表|检查表)|毕业(?:论文|设计)(?:工作)?手册|查重报告)')
SUPPORTING=re.compile(r'^(?:外文(?:原文|翻译|译文)(?:封面)?|文献综述|附录[A-Z一二三123]*|(?:施工)?工程量清单|(?:施工计划)?横道图)(?:[\s_—\-].*)?$',re.I)
DRAWING=re.compile(r'(?:图纸(?:总和|合集|集)?|(?:厂区|车间|平面|立面|设备|管道|焦炉|场地|总)?布置图?|'
    r'装配图|总装图|零件图|剖视图|接线图|电路图|原理图|结构图|工艺流程图|管道及仪表流程图|'
    r'P[＆&]ID|CAD图(?:纸)?|决策树模型|模型图)(?:[\s_—\-（(、].*)?$',re.I)
CODE_LIST=re.compile(r'(?:程序清单|源(?:程序|代码)(?:清单)?|代码清单)(?:[\s_—\-（(].*)?$')
BODY_START=re.compile(r'^(?:摘要|abstract|目录|contents|绪论|引言|introduction|第[一二三四五六七八九十123456789]章|'
    r'\d+[.、．]?\s*(?:绪论|引言|研究|设计|系统|实验|方法|排程))',re.I)
CAD_CREATOR=re.compile(r'autocad|autodesk|solidworks|bentley|draftsight|zwcad|中望|浩辰|gstarcad|acroplot',re.I)


def _name(record):
    value=record.get('archive_member_path') or record.get('name') or record.get('path') or record.get('source_path','')
    name=Path(str(value).replace('\\','/')).stem
    # Legacy ZIP names may have been decoded as CP437 by the existing extractor.
    # Recover classification evidence only; never rename paths or identities.
    try:
        recovered=name.encode('cp437').decode('gb18030')
        if re.search(r'[\u4e00-\u9fff]',recovered):name=recovered
    except UnicodeError:pass
    return name


def _compact(value):
    return re.sub(r'\s+','',unicodedata.normalize('NFKC',str(value)))


def _front_matter(text):
    # Bound *both* reused screeners before calling them. A paper's appendix may
    # contain real form headings, even when its body starts with "1 排程研究".
    lines=[]
    for raw in text[:5000].splitlines()[:32]:
        line=_compact(raw).strip('#*|:：')
        if BODY_START.match(line) or re.match(r'^(?:附录|appendix)(?:[A-Z一二三123]|$)',line,re.I):break
        lines.append(raw)
    return '\n'.join(lines)


def _form_heading(text):
    front=_front_matter(text)
    # Reuse the existing document-heading matcher within the safe boundary.
    existing=nonpaper_heading(front)
    if (existing and PROCESS_FORM.search(_compact(existing))
            and not re.search(r'基于|面向|的研究|识别方法|分类方法|管理系统',existing)):return existing
    for raw in front.splitlines():
        line=_compact(raw).strip('#*|:：')
        if BODY_START.match(line):break
        if not line or len(line)>100 or re.search(r'[。！？;；]',line):continue
        if re.search(r'基于|面向|的研究|识别方法|分类方法|管理系统',line):continue
        match=PROCESS_FORM.search(line)
        if match and not line[match.end():].strip('（）（）()[]【】0123456789:：-—_'):
            return raw.strip()
    return ''


def _primary(text,name,*,allow_length=True):
    body=re.split(r'(?mi)^\s*(?:附录|appendix)\s*$',text,maxsplit=1)[0]
    compact=_compact(body)
    lines=[line.strip().lstrip('#').strip() for line in body.splitlines() if line.strip()]
    if len(compact)<80:return False
    headings=sum(bool(BODY_START.match(_compact(line))) for line in lines)
    if headings and any(word in compact for word in ('本研究','本文','本设计','研究方法','研究结果','实验结果','设计方案','研究比较')):return True
    if re.search(r'(?mi)^\s*(?:abstract|introduction|methodology|results)\b',body) and len(compact)>300:return True
    if any(word in name for word in ('设计说明书','论文正文','学位论文','论文终稿')) and len(compact)>100:return True
    # This is a purpose signal, not a minimum-length admission requirement.
    return allow_length and len(compact)>1800 and len(lines)>5 and any(w in compact for w in ('研究','设计','实验','仿真'))


def _read(path):
    suffix=path.suffix.lower()
    if suffix in OFFICE_FORMATS:
        from engine import convert_office
        from material_conversion import current
        if current() is None:raise ValueError('native conversion required')
        with tempfile.TemporaryDirectory(prefix='intake-') as temporary:
            return _read(convert_office(path,Path(temporary)/'converted'))
    if suffix=='.pdf':
        import fitz
        with fitz.open(path) as doc:
            if doc.needs_pass:raise ValueError('encrypted PDF')
            text='\n'.join(doc[i].get_text() for i in range(min(len(doc),6)))
            return text[:16000],' '.join(str(v) for k,v in doc.metadata.items() if k in {'creator','producer'}),len(doc)<=6 and len(text)<=16000
    if suffix=='.docx':
        import zipfile
        with zipfile.ZipFile(path) as archive:
            if archive.getinfo('word/document.xml').file_size>32*1024*1024:raise ValueError('bounded heading reader limit')
            from xml.etree import ElementTree as ET
            document=ET.fromstring(archive.read('word/document.xml'))
            count=sum(1 for _ in document.iter('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}p'))
        from classification_rules import _native_cover_lines
        text='\n'.join(_native_cover_lines(path))
        return text[:16000],'',count<=120 and len(text)<=16000
    if suffix in {'.txt','.md'}:
        with path.open('rb') as stream:raw=stream.read(64000)
        for encoding in ('utf-8-sig','utf-16','gb18030'):
            try:
                text=raw.decode(encoding)
                return text[:16000],'',path.stat().st_size<=64000 and len(text)<=16000
            except UnicodeError:continue
        raise ValueError('unreadable text')
    raise ValueError('native conversion required')


@lru_cache(maxsize=24000)
def _cached_read(path,size,mtime):
    return _read(Path(path))


def classify_document(record, *, processor=None, work=None, fresh=False):
    path=Path(record.get('path') or record.get('source_path',''))
    name=_name(record)
    result=dict(version=VERSION,kind='unknown',action='pending',confirmed=False,
        reason='用途尚未确认，保留原件待核对；未进入论文 AI')
    if SUPPORTING.fullmatch(name):
        return dict(result,kind='supporting_document',action='associated',confirmed=True,
            reason='明确的附录或参考附件，按关联资料保存，不进入论文 AI')
    if re.search(r'毕业(?:论文|设计)(?:工作)?手册',name):
        return dict(result,kind='administrative',reason='过程手册按行政材料拦截，待核对内容，不进入论文 AI')
    try:
        stat=path.stat()
        if path.suffix.lower() in OFFICE_FORMATS:
            if processor is None or work is None:
                from material_conversion import current
                if current() is None:
                    return dict(result,kind='administrative' if PROCESS_FORM.search(name) else 'document',
                        action='pending' if PROCESS_FORM.search(name) else 'review',reason='旧 Office 文种将在既有转换槽内核对')
                text,creator,complete=_read(path) if fresh else _cached_read(str(path.resolve()),stat.st_size,stat.st_mtime_ns)
            else:
                from material_conversion import preparation_context
                from contextlib import nullcontext
                from engine import convert_office
                enter=getattr(processor,'_preextract_enter_prepare',None)
                if enter:enter()
                with preparation_context(getattr(processor,'_resume_root',processor.store.root),
                        enabled=(processor.config.get('preextract_preparation') or {}).get('office_enabled',True),
                        stop=processor.stop,wait_scope=getattr(processor,'_preextract_conversion',nullcontext)):
                    with tempfile.TemporaryDirectory(prefix='intake-',dir=work) as temporary:
                        converted=convert_office(path,Path(temporary)/'converted')
                        text,creator,complete=_read(converted)
        else:
            text,creator,complete=_read(path) if fresh else _cached_read(str(path.resolve()),stat.st_size,stat.st_mtime_ns)
        heading=_form_heading(text)
        report=similarity_report(_front_matter(text),path.name)
        if heading or report:
            if not complete:
                return dict(result,reason='过程表单证据之外仍有未读内容，待核对，不自动删除整份文件')
            # A combined paper+form file is not safe to delete wholesale.
            if heading and _primary(text,name,allow_length=False):
                return dict(result,reason='同一文件同时出现过程表单与正文证据，待核对，不自动删除')
            return dict(result,kind='administrative',action='delete',confirmed=True,
                reason=report or '已确认过程表单：'+heading,evidence_sha256=digest(path))
        if any(SUPPORTING.fullmatch(_compact(line).removeprefix('本科毕业设计').removeprefix('本科毕业论文')) for line in _front_matter(text).splitlines()[:24]):
            return dict(result,kind='supporting_document',action='associated',confirmed=True,
                reason='附录或参考资料标题明确，按关联资料保存，不进入论文 AI')
        primary=_primary(text,name,allow_length=False)
        title_fields=sum(bool(re.search(word,text,re.I)) for word in (r'比例|scale',r'图号|drawing\s*(?:no|number)',r'校核|审核|checked',r'制图|drawn',r'共\s*\d+\s*张|sheet\s*\d+'))
        if (CAD_CREATOR.search(creator) or title_fields>=3 or DRAWING.search(name)) and not primary:
            return dict(result,kind='drawing',action='associated',confirmed=True,reason='图纸名称、原生 CAD 元数据或图签证据；不进入论文 AI')
        if primary:
            return dict(result,kind='document',action='review',confirmed=True,reason='已取得论文正文或设计说明内容证据')
        if CODE_LIST.search(name):
            return dict(result,kind='source_code',action='associated',confirmed=True,reason='程序清单按关联资料保存，不进入论文 AI')
        if _primary(text,name):
            return dict(result,kind='document',action='review',confirmed=True,reason='取得正文内容证据；继续既有 AI 审核')
        return result
    except (OSError,ValueError,KeyError,UnicodeError,ImportError,RuntimeError,zipfile.BadZipFile):
        if path.suffix.lower() in {'.pdf','.docx'} and DRAWING.search(name):
            return dict(result,kind='drawing',action='associated',reason='图纸文件名明确，内容待解析')
        if path.suffix.lower() in {'.pdf','.docx'} and CODE_LIST.search(name):
            return dict(result,kind='source_code',action='associated',reason='程序清单文件名明确，内容待解析')
        if not path.exists() and any(w in name for w in ('设计说明书','论文正文')):
            return dict(result,kind='document',action='review',reason='选中文件将在处理时核对')
        return result


def delete_confirmed_form(record,work,role,*,processor=None,stop=None):
    """Delete one checked regular file; callers choose selected/extracted inputs."""
    source=Path(record['path']).absolute()
    if stop is not None and stop.is_set():raise InterruptedError('已停止，未删除文件')
    for part in (source,*source.parents):
        if part.is_symlink() or part.is_junction():raise ValueError('过程表单路径包含链接，保留待核对')
    checked=classify_document(record,processor=processor,work=work,fresh=True)
    if checked['action']!='delete' or not checked['confirmed'] or checked.get('evidence_sha256')!=role.get('evidence_sha256'):
        raise ValueError('过程表单用途或源文件发生变化，未删除')
    # The final absolute target is the selected file itself; never recurse or
    # replace/delete its containing folder or original mixed archive.
    if str(source.resolve())!=str(Path(record['path']).resolve()):raise ValueError('删除目标不等于选中文件')
    if stop is not None and stop.is_set():raise InterruptedError('已停止，未删除文件')
    if digest(source)!=checked['evidence_sha256']:raise ValueError('过程表单删除前发生变化')
    receipt=dict(schema='process-form-deletion/1',source_path=str(source),source_sha256=checked['evidence_sha256'],
        reason=checked['reason'],archive_member_path=record.get('archive_member_path'),original_archive_modified=False,status='intent')
    write_json(Path(work)/'intake-deletion.json',receipt)
    source.unlink()
    receipt['status']='deleted';write_json(Path(work)/'intake-deletion.json',receipt)
    return checked


def delete_process_form(processor,record,work,item,stage,role):
    """Delete one explicitly selected regular file, retaining an audit, not bytes."""
    checked=delete_confirmed_form(record,work,role,processor=processor,stop=processor.stop)
    audit_path=str(Path(work)/'intake-deletion.json')
    item.update(status='deleted',content_format='excluded_material',title='已删除过程表单',reason=checked['reason'],
        deletion_receipt=audit_path,path=audit_path,display_path=audit_path,
        reason_kind='material_deleted',business_decision='delete',automatic_approval=False,intake_role=checked,
        script_result=dict(status='completed',reason='已按用途规则删除；未保留原件或送入 AI'),
        ai_result=dict(status='not_started',reason='过程表单在 AI 前删除',stage='前置用途分类'))
    stage('已删除过程表单',checked['reason'])
