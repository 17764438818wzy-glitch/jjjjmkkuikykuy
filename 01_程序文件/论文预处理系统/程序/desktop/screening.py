"""Basic intake screening, not academic quality grading."""
import re

MIN_CHARS=3000
LEGACY_REASONS={'too_short':'正文太短','garbled_text':'乱码比例过高','no_body':'未识别到论文正文',
                'reject_duplicate':'内容重复','missing_abstract':'缺少摘要，待复核','missing_references':'缺少参考文献，待复核'}
DOCUMENT_KIND=re.compile(r'^(?:.{0,45})?(?:开题报告|任务书|个人简历|求职简历|招聘启事|招聘公告|课程表|成绩单|申请表|登记表|审批表|会议通知|会议纪要|调查问卷|问卷调查表)$')


def similarity_report(raw, source_name=''):
    """Require report-layout evidence; a filename or research mention is not enough."""
    from pathlib import Path
    head=[]
    for line in raw[:2500].splitlines()[:60]:
        compact=re.sub(r'\s+', '', line)
        if re.match(r'^(?:摘要|Abstract|ABSTRACT|目录|第一章|第1章|绪论)',compact):break
        head.append(compact)
    joined='\n'.join(head)
    names=('原文对照报告','简洁报告')
    title=next((name for name in names if any(
        line==name or re.fullmatch(re.escape(name)+r'[-—－](?:大学生|本科|研究生|专业)版',line.removeprefix('【').removesuffix('】'))
        or re.fullmatch(r'.{0,30}[（(]'+name+r'[）)]',line) for line in head)),None)
    # Some PDF report headers are flattened onto one line; the filename then
    # corroborates an actual report title and a numeric similarity field.
    stem=Path(source_name).stem
    named=next((name for name in names if re.search(name+r'(?:[（(]\d+[）)])?$',stem)),None)
    if not title and named and named in joined[:1800]:title=named
    ratio=re.search(r'(?:总相似比|总文字复制比|文字复制比|全文总相似比)[:：]?\s*\d+(?:\.\d+)?[%％]',joined)
    if not ratio:
        # PDF columns may extract the value before its label. Require two
        # additional report fields before accepting this less local match.
        report_fields=sum(bool(re.search(label+r'[:：]',joined)) for label in ('检测字符数','报告编号','检测时间','总相似片段数'))
        ratio=(report_fields>=2 and re.search(r'(?:总相似比|总文字复制比|文字复制比)[:：]',joined)
               and re.search(r'(?m)^\d+(?:\.\d+)?[%％]$',joined))
    if title and ratio:return '查重'+title+'属于辅助材料，无需进行论文 AI 处理；请使用论文原文件'
    # These full-text report exports also contain the submitted thesis, so
    # length/abstract checks cannot distinguish them from the original. Require
    # an exact report heading and independent front-page diagnostic fields.
    cnki_title=any(re.fullmatch(
        r'(?:知网(?:个人)?查重服务报告单|文本复制检测报告单)(?:[（(](?:全文对照|全文标明引文|简洁)[）)])?',
        line) for line in head)
    cnki_fields=(re.search(r'(?m)^检测时间[:：]\d{4}-\d{1,2}-\d{1,2}', joined)
                 and re.search(r'(?m)^(?:篇名|检测文献)[:：].+', joined))
    if cnki_title and cnki_fields and ratio:
        return '查重全文检测报告属于辅助材料，无需进行论文 AI 处理；请使用论文原文件'
    paperpass_title=any(re.fullmatch(r'PaperPass(?:旗舰版|专业版|免费版)?检测报告',line,re.I) for line in head)
    if (paperpass_title and re.search(r'(?m)^报告编号[:：][A-Za-z0-9]+$',joined)
            and re.search(r'(?m)^提交时间[:：]\d{4}-\d{1,2}-\d{1,2}',joined)
            and re.search(r'(?m)^比对范围[:：].+',joined)
            and re.search(r'(?m)^总体[:：]\s*\d+(?:\.\d+)?[%％]',joined)):
        return 'PaperPass查重检测报告属于辅助材料，无需进行论文 AI 处理；请使用论文原文件'
    # PaperYY reports do not contain the two report titles above. Require its
    # actual front-page fields, not the archive filename or a research mention.
    if (ratio and re.match(r'^检测文献[:：].+', joined)
            and re.search(r'(?m)^报告编号[:：]YY\d{12,}$', joined)
            and re.search(r'(?m)^报告时间[:：]\d{4}-\d{2}-\d{2}', joined)
            and re.search(r'(?m)^检测范围[:：].+', joined)
            and re.search(r'(?m)^去除引用文献复制比[:：]\d+(?:\.\d+)?[%％]$', joined)):
        return '查重检测报告属于辅助材料，无需进行论文 AI 处理；请使用论文原文件'
    if (stem=='报告说明' and all(name in joined for name in names)
            and '检测系统' in joined):
        return '查重报告说明属于辅助材料，无需进行论文 AI 处理；请使用论文原文件'
    return ''


def nonpaper_heading(raw):
    for line in raw[:5000].splitlines()[:24]:
        text=re.sub(r'\s+','',line.strip().strip('#*'))
        if re.match(r'^(摘要|Abstract|ABSTRACT|目录|第一章|第1章|绪论)',text):break
        if not text or text.startswith('[') or len(text)>80:continue
        if re.search(r'[。！？;；]',text):continue
        if DOCUMENT_KIND.fullmatch(text) or re.fullmatch(r'关于.{2,55}的通知',text) or text in {'通知','个人履历','求职信'}:
            return line.strip()
    return ''


def assess(raw,cleaned,legacy_status,legacy_reason):
    body=re.sub(r'\[(?:PAGE_BREAK|SECTION_BREAK)\]','',cleaned)
    count=len(re.sub(r'\s+','',body))
    codes=[c for c in legacy_reason.split(';') if c]
    kind=nonpaper_heading(raw)
    if kind:codes.insert(0,'non_paper')
    if count<MIN_CHARS and 'too_short' not in codes:codes.append('too_short')
    status='reject' if kind or count<MIN_CHARS or legacy_status=='reject' else legacy_status
    reasons=[]
    for code in dict.fromkeys(codes):
        if code=='non_paper':reasons.append('明显非论文材料：'+kind)
        elif code=='too_short':reasons.append(f'正文太短：{count} 个有效字符，最低 {MIN_CHARS}（不计空白）')
        else:reasons.append(LEGACY_REASONS.get(code,code))
    return {'status':status,'reason':'；'.join(reasons),'rule_ids':list(dict.fromkeys(codes)),
            'effective_char_count':count,'min_chars':MIN_CHARS,'nonpaper_heading':kind,
            'basis':'原 quality_filter.py 的基础筛选 + 显式非论文文种/非空白字符检查','academic_rating':None}
