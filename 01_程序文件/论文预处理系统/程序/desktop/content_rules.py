"""Additional output rules requested for the RAGFlow upstream mode."""
from __future__ import annotations

import re
from pathlib import PureWindowsPath

PROCESSING_VERSION = "evidence-v3.2-private-coordinate-input"
TITLE_LABEL = re.compile(r"^(?:毕业论文|论文|设计)?题\s*目\s*[：:]?\s*")
FRONT_END = re.compile(r"^(?:摘\s*要|abstract|目\s*录|contents|第[一1]章|绪论(?:\s|[:：]|$)|(?:1|一)[.、．]?\s*(?:绪\s*论|引\s*言)(?:\s|$)|1(?:[.．]\d+)+\s+\S)", re.I)
FIELD = re.compile(r"^(?:二级学院|学\s*院|学\s*校|专\s*业|班\s*级|学\s*号|姓\s*名|学生姓名|指导教师|指导老师|导\s*师|作\s*者|日\s*期|年\s*级|完成时间)(?:\s*[:：]|\s+|$)")
ENGLISH_FIELD = re.compile(r"^(?:author|student(?:\s+name)?|instructor|supervisor|advisor|adviser)\s*[:：]", re.I)
GENERIC = re.compile(r"^(?:(?:成人|高等|继续|本科|专科|学士|硕士|博士|函授|自考|教育|学位|毕业|设计|论文|报告|开题|任务书|封面|管理学院|会计学)\s*)+$")
DATE_ONLY = re.compile(
    r"^(?:"
    r"\d{4}年\d{1,2}月(?:\d{1,2}日)?|"
    r"\d{4}[./-]\d{1,2}[./-]\d{1,2}|"
    r"\d{1,2}[./-]\d{1,2}[./-]\d{2,4}"
    r")$"
)
COVER_VALUE = re.compile(r"^(?:(?:年?级)?\d{4}[级届]|(?:职称或学位|职称|学位)(?:博士|硕士|学士|副?教授|讲师|助教))$")
# PDF covers sometimes extract a vertical label as one character per line and
# attach its last character to the value. Normalize that label only, so ordinary
# research titles beginning with 年级 or 专业 retain their original wording.
VERTICAL_FIELD = re.compile(r"(?m)^[ \t]*(?:" + "|".join(
    r"[ \t]*(?:\r?\n[ \t]*)?".join(name)
    for name in ('二级学院', '学生姓名', '指导教师', '指导老师', '完成时间',
                 '学院', '学校', '专业', '班级', '学号', '姓名', '导师', '作者', '日期', '年级')
) + r")")


def generic_title(value):
    # Normalize only for cover-label detection; preserve the actual title text.
    compact=re.sub(r'[\s()（）\[\]【】]+','',value)
    compact=re.sub(r'^\d{4}(?:届|年度|年)','',compact)
    return bool(GENERIC.fullmatch(compact))


def clean_title(value):
    value = value.strip().strip("#* ：:")
    value = TITLE_LABEL.sub("", value)
    # Cover tables can flatten a title label between the title and subtitle.
    value = re.sub(r"题\s*目(?=\s|[:：]|$)\s*[:：]?\s*", "", value)
    value = re.sub(r"\s*(——|—{1,2})\s*", r"\1", value)
    return re.sub(r"\s+", " ", value).strip()


def valid_title(value, min_length=6):
    date_candidate = re.sub(r"[\s,，]", "", value)
    # A single terminal question mark is a legitimate research-title form.
    # Interior question marks still indicate multiple sentences, not a title.
    punctuation_text = value[:-1] if value.endswith(('？', '?')) else value
    return (min_length <= len(value) <= 160 and not DATE_ONLY.fullmatch(date_candidate)
            and not COVER_VALUE.fullmatch(date_candidate)
            and not generic_title(value)
            and not FIELD.search(value) and not ENGLISH_FIELD.search(value) and not FRONT_END.search(value)
            and not re.search(r"\d{6,}|\[(?:NAME|STUDENT_ID|PHONE|EMAIL|TEACHER)\]|@|学\s*号|学生姓名|指导教师(?:\s*[:：]|\s+[\u4e00-\u9fff]{2,6}(?:\s|$))", value)
            and not re.search(r"\[(?:PAGE_BREAK|SECTION_BREAK)\]", value)
            and not value.endswith(("大学", "学院", "学校"))
            and not re.search(r"[。；;！？!?]", punctuation_text))


def detect_title(text, original_relative_path=""):
    front = []
    cover = VERTICAL_FIELD.sub(lambda match: re.sub(r'\s+', '', match[0]) + ': '
                               if '\n' in match[0] else match[0], text[:8000])
    lines = cover.splitlines()
    for index, line in enumerate(lines):
        stripped = line.strip()
        split_abstract_heading = (re.fullmatch(r"摘\s*", stripped)
                                  and index + 1 < len(lines)
                                  and re.match(r"^要\s*[:：]", lines[index + 1].strip()))
        if FRONT_END.search(stripped) or split_abstract_heading:
            break
        front.append(stripped)
    # Explicit fields first, then title-like cover lines; skip administrative fields.
    for index, line in enumerate(front):
        if TITLE_LABEL.match(line):
            title = clean_title(line)
            # A cover title is often split by a soft line break in the source.
            # Continue only this explicitly labelled field, not arbitrary prose.
            for offset, continuation in enumerate(front[index+1:index+7],start=index+1):
                # PDF text extraction may split a spaced cover field (学 院)
                # across lines. Look ahead only for an existing field pattern.
                field_context='\n'.join(front[offset:offset+4])
                if not continuation or FIELD.search(field_context) or ENGLISH_FIELD.search(field_context) or TITLE_LABEL.match(continuation) or generic_title(continuation):
                    break
                if (re.search(r'[\u4e00-\u9fff]',title)
                        and not re.search(r'[\u4e00-\u9fff]',continuation)
                        and len(re.findall(r'[A-Za-z]+',continuation)) >= 2):
                    break  # A separate translated title, not a Chinese wrap.
                # Flattened cover text has no paragraph/cell boundaries. Do not
                # absorb an ambiguous bare name after a plausible title.
                # Short research suffixes and initial fragments can still wrap.
                if (valid_title(title) and re.fullmatch(r'[\u4e00-\u9fff]{2,4}',continuation)
                        and not re.search(r'研究|分析|评价|评估|设计|策略|方法|机制|模型|体系|探讨|实践|应用|问题|影响|对策|管理|现状|优化|比较|改革|创新',continuation)):
                    return ''  # Existing classifier resolves ambiguous title boundaries.
                separator=' ' if title and re.search(r'[A-Za-z0-9]$',title) and re.match(r'[A-Za-z0-9]',continuation) else ''
                candidate=clean_title(title+separator+continuation)
                if not valid_title(candidate,min_length=1):break
                title=candidate
            if valid_title(title):
                return title
    for index, line in enumerate(front):
        if FIELD.search(line) or ENGLISH_FIELD.search(line):
            continue
        title = clean_title(line)
        if valid_title(title):
            if index + 1 < len(front) and re.match(r'^(?:——|—|--)', front[index + 1]):
                candidate = clean_title(title + front[index + 1])
                if valid_title(candidate):title = candidate
            return title
    # Never reuse a student identifier or a bare major/name as a fallback title.
    stem=PureWindowsPath(original_relative_path).stem
    # A short Chinese prefix separated from the research title may be an author.
    # Without cover evidence, do not publish that ambiguous filename as a title.
    if (re.match(r'^[\u4e00-\u9fff]{2,4}(?:[（(][^（）()]{1,12}[）)])?\s*(?:[_\-—–.．]|\s+)',stem)
            or re.match(r'^[A-Z][a-z]{1,24}(?:[ _.-][A-Z][a-z]{1,24})?[_\-.．]',stem)):
        return ""
    # Author suffixes are a filename convention, never a cover-title rule.
    filename=re.sub(r"(?:--|——|-)[\u4e00-\u9fff]{2,4}\d*稿?$", "", stem)
    # A separated leading segment can be a customer, author or project label.
    # Use the existing content classifier instead of promoting it into output.
    if re.search(r'[_\.．\-–]|(?<!—)—(?!—)',filename):
        return ""
    candidate = clean_title(filename)
    if valid_title(candidate) and re.search(r"研究|分析|探讨|设计|策略|应用|评价|优化|Study|Analysis|Research", candidate, re.I):
        return candidate
    return ""


NUMBER = r"(?:第[一二三四五六七八九十百零〇\d]+[章节]|[一二三四五六七八九十\d]+[.、．])"
PREFIX = rf"\s*(?:\#{{1,6}}\s*)?(?:{NUMBER}\s*)?"
ACK_NAME = r"(?:致\s*谢|鸣\s*谢|谢\s*辞|acknowledg(?:e)?ments?)"
ACK = re.compile(rf"^{PREFIX}{ACK_NAME}\s*(?:[:：].*)?$", re.I)
ACK_TOC = re.compile(rf"^{PREFIX}{ACK_NAME}\s*(?:[:：.．…·\t ]+\s*)[\dIVXivx]+\s*$", re.I)
NEXT_SECTION = re.compile(rf"^{PREFIX}(?:参\s*考\s*文\s*献|附\s*录|摘\s*要|abstract|references|bibliography|appendix|appendices|结\s*论|绪\s*论)(?:\s|[:：A-ZＡ-Ｚ]|$)", re.I)
CHAPTER = re.compile(rf"^\s*(?:\#{{1,6}}\s+\S|第[一二三四五六七八九十百零〇\d]+[章节]\s*\S|(?:一[、．.]|1(?:[.．]1)?[、．.]?)\s*(?:绪论|引言|研究背景)(?:\s|$))")


def remove_acknowledgements(text):
    lines, skipping, count = [], False, 0
    for line in text.splitlines(keepends=True):
        heading = line.strip().strip("*")
        if ACK_TOC.fullmatch(heading):
            continue
        if ACK.fullmatch(heading):
            skipping = True
            count += 1
            continue
        if skipping and (NEXT_SECTION.match(heading) or CHAPTER.match(heading)):
            skipping = False
        if not skipping:
            lines.append(line)
    return "".join(lines), count
